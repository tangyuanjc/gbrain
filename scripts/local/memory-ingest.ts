#!/usr/bin/env bun
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import {
  appendFileSync,
  existsSync,
  lstatSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  renameSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import { homedir } from 'node:os';
import { dirname, join, relative, resolve, sep } from 'node:path';

export type MemoryIngestSource = {
  root: string;
  slugPrefix: string;
};

type MemoryIngestState = {
  schema_version: 1;
  last_success_at: string;
  files: Record<string, { sha256: string; bytes: number }>;
};

export type ImportCommandResult = {
  exitCode: number;
  stdout: string;
  stderr: string;
  timedOut?: boolean;
};

export type ImportCommandRunner = (args: string[], options: { cwd: string; timeoutMs: number }) => ImportCommandResult;

export type MemoryIngestOptions = {
  home?: string;
  now?: Date;
  sources?: MemoryIngestSource[];
  statePath?: string;
  stagingRoot?: string;
  logPath?: string;
  lockPath?: string;
  commandTimeoutMs?: number;
  runImport?: ImportCommandRunner;
  monotonicNow?: () => number;
  sleep?: (ms: number) => void;
};

export type MemoryIngestSummary = {
  schema_version: 1;
  status: 'success' | 'skipped' | 'failed';
  generated_at: string;
  scanned_files: number;
  changed_files: number;
  imported_pages: number;
  unchanged_pages: number;
  failed_files?: number;
  timed_out_files?: number;
  lock_timeout_files?: number;
  lock_retries?: number;
  reason?: string;
};

const ignoredDirectoryNames = new Set(['.git', '.gbrain', 'node_modules']);

function isGbrainImportDisabled(path: string) {
  const content = readFileSync(path, 'utf8');
  const frontmatter = content.match(/^---\s*\r?\n([\s\S]*?)\r?\n---(?:\r?\n|$)/)?.[1];
  return frontmatter
    ? /^gbrain_import:\s*(?:false|no|0)\s*(?:#.*)?$/im.test(frontmatter)
    : false;
}

function defaultSources(home: string): MemoryIngestSource[] {
  return [
    {
      root: join(home, '.claude/projects', home.replace(/[\\/]/g, '-'), 'memory'),
      slugPrefix: 'opus-memory',
    },
    {
      root: join(home, 'agents-changelog'),
      slugPrefix: 'agents-changelog',
    },
  ];
}

function defaultImportRunner(home: string): ImportCommandRunner {
  return (args, options) => {
    const result = spawnSync(join(home, '.local/bin/gbrain'), args, {
      cwd: options.cwd,
      encoding: 'utf8',
      env: {
        ...process.env,
        HOME: home,
        PATH: `${join(home, '.bun/bin')}:${join(home, '.local/bin')}:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin`,
        NO_PROXY: '*',
        no_proxy: '*',
      },
      maxBuffer: 32 * 1024 * 1024,
      timeout: options.timeoutMs,
    });
    return {
      exitCode: result.status ?? (result.error ? 1 : 0),
      stdout: String(result.stdout ?? ''),
      // spawnSync reports ETIMEDOUT on `error` while stderr is often the
      // empty string. Preserve that diagnostic so the per-file runner can
      // classify a budget breach instead of reporting an opaque exit 1.
      stderr: [result.stderr, result.error?.message].filter(Boolean).join('\n'),
      timedOut: (result.error as NodeJS.ErrnoException | undefined)?.code === 'ETIMEDOUT',
    };
  };
}

function isPathInside(parent: string, child: string) {
  const rel = relative(resolve(parent), resolve(child));
  return rel === '' || (!rel.startsWith(`..${sep}`) && rel !== '..');
}

function validateSource(source: MemoryIngestSource) {
  if (!source.slugPrefix || source.slugPrefix.includes('/') || source.slugPrefix === '.' || source.slugPrefix === '..') {
    throw new Error(`invalid slug prefix: ${source.slugPrefix}`);
  }
  if (!existsSync(source.root) || !lstatSync(source.root).isDirectory()) {
    throw new Error(`memory source directory is missing: ${source.root}`);
  }
}

function collectMarkdownFiles(root: string): string[] {
  const files: string[] = [];
  const visit = (dir: string) => {
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      // Match GBrain's hardened walker: dotfiles and dot-directories are not
      // source pages (this excludes .pytest_cache and local secret allowlists).
      if (entry.name.startsWith('.')) continue;
      if (entry.isSymbolicLink()) continue;
      const path = join(dir, entry.name);
      if (entry.isDirectory()) {
        if (!ignoredDirectoryNames.has(entry.name)) visit(path);
        continue;
      }
      if (entry.isFile() && /\.md$/i.test(entry.name) && !isGbrainImportDisabled(path)) files.push(path);
    }
  };
  visit(root);
  return files.sort();
}

function fileDigest(path: string) {
  const content = readFileSync(path);
  return {
    sha256: createHash('sha256').update(content).digest('hex'),
    bytes: content.byteLength,
  };
}

function readState(path: string): MemoryIngestState | undefined {
  if (!existsSync(path)) return undefined;
  const parsed = JSON.parse(readFileSync(path, 'utf8')) as Partial<MemoryIngestState>;
  if (parsed.schema_version !== 1 || !parsed.files || typeof parsed.files !== 'object') {
    throw new Error(`unsupported or invalid memory ingest state: ${path}`);
  }
  return parsed as MemoryIngestState;
}

function writeState(path: string, state: MemoryIngestState) {
  mkdirSync(dirname(path), { recursive: true });
  const temporaryPath = `${path}.tmp-${process.pid}`;
  writeFileSync(temporaryPath, `${JSON.stringify(state, null, 2)}\n`, { encoding: 'utf8', mode: 0o600 });
  renameSync(temporaryPath, path);
}

function appendSummary(path: string, summary: MemoryIngestSummary) {
  mkdirSync(dirname(path), { recursive: true });
  appendFileSync(path, `${JSON.stringify(summary)}\n`, 'utf8');
}

function processIsRunning(pid: number) {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

function acquireLock(path: string) {
  mkdirSync(dirname(path), { recursive: true });
  try {
    mkdirSync(path);
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    if (code !== 'EEXIST') throw error;
    const pidPath = join(path, 'pid');
    const pid = existsSync(pidPath) ? Number(readFileSync(pidPath, 'utf8').trim()) : Number.NaN;
    if (Number.isInteger(pid) && pid > 0 && processIsRunning(pid)) return false;
    rmSync(path, { recursive: true, force: true });
    mkdirSync(path);
  }
  writeFileSync(join(path, 'pid'), `${process.pid}\n`, 'utf8');
  return true;
}

function parseImportSummary(stdout: string) {
  const lines = stdout.split(/\r?\n/).map((line) => line.trim()).filter(Boolean).reverse();
  for (const line of lines) {
    if (!line.startsWith('{')) continue;
    try {
      const value = JSON.parse(line) as Record<string, unknown>;
      if (value.status === 'success') return value;
    } catch {
      // Keep looking for the final structured envelope.
    }
  }
  return undefined;
}

function errorTail(value: string) {
  return value.trim().split(/\r?\n/).slice(-12).join('\n');
}

/**
 * Keep one pathological markdown file from holding the whole launchd run.
 * The importer is invoked once per staged file below, so this budget is a
 * real process boundary (rather than a Promise.race that leaves PGlite work
 * running in the background).  Splitting is deliberately below the normal
 * content-sanity warn threshold; this keeps the expensive chunk/parse path
 * bounded even when a page is only "warn"-sized and not hard-blocked.
 */
const SPLIT_BYTES = 80_000;

function splitMarkdownForIngest(content: string): string[] {
  if (Buffer.byteLength(content, 'utf8') <= SPLIT_BYTES) return [content];

  const frontmatter = content.match(/^---\s*\r?\n[\s\S]*?\r?\n---(?:\r?\n|$)/)?.[0] ?? '';
  const body = frontmatter ? content.slice(frontmatter.length) : content;
  const lines = body.split(/(?<=\n)/);
  const parts: string[] = [];
  let current = frontmatter;

  const flush = () => {
    if (current.length > frontmatter.length) parts.push(current);
    current = frontmatter;
  };

  for (const line of lines) {
    // A single enormous line is uncommon in memory markdown, but splitting it
    // by code points avoids silently creating another over-budget part.
    if (Buffer.byteLength(line, 'utf8') > SPLIT_BYTES) {
      flush();
      let segment = '';
      for (const char of line) {
        if (Buffer.byteLength(segment + char, 'utf8') > SPLIT_BYTES) {
          if (segment) parts.push(frontmatter + segment);
          segment = char;
        } else {
          segment += char;
        }
      }
      if (segment) current += segment;
      continue;
    }
    if (Buffer.byteLength(current + line, 'utf8') > SPLIT_BYTES && current.length > frontmatter.length) {
      flush();
    }
    current += line;
  }
  flush();
  return parts.length > 0 ? parts : [content];
}

function isTimedOutImport(result: ImportCommandResult) {
  return result.timedOut === true || /\bETIMEDOUT\b/.test(result.stderr);
}

function isLockTimeout(result: ImportCommandResult) {
  return !isTimedOutImport(result) && result.exitCode !== 0
    && /Timed out waiting for PGLite lock/i.test(result.stderr);
}

/** Retry only failures before import acquired the DB. One total per-part
 * deadline covers subprocess time AND jittered backoff; never steal a lock. */
export function importWithLockRetry(
  run: ImportCommandRunner, args: string[], options: { cwd: string; timeoutMs: number },
  now = () => performance.now(),
  sleep = (ms: number) => { Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms); },
): { result: ImportCommandResult; retries: number } {
  const deadline = now() + options.timeoutMs;
  let retries = 0;
  let result = run(args, options);
  while (isLockTimeout(result) && retries < 8) {
    const delay = Math.min(5000, 500 * 2 ** retries) * (0.8 + Math.random() * 0.4);
    if (deadline - now() <= delay + 1000) break;
    sleep(delay);
    const remaining = Math.floor(deadline - now());
    if (remaining <= 0) break;
    retries++;
    result = run(args, { ...options, timeoutMs: remaining });
  }
  return { result, retries };
}

export function runMemoryIngest(options: MemoryIngestOptions = {}): MemoryIngestSummary {
  const home = options.home ?? homedir();
  const now = options.now ?? new Date();
  const generatedAt = now.toISOString();
  const sources = options.sources ?? defaultSources(home);
  const statePath = options.statePath ?? join(home, '.gbrain/memory-ingest-state.json');
  const stagingRoot = options.stagingRoot ?? join(home, '.gbrain/.staging');
  const logPath = options.logPath ?? join(home, '.gbrain/logs/memory-ingest.jsonl');
  const lockPath = options.lockPath ?? join(home, '.gbrain/.locks/gbrain-memory-ingest.lock');
  const timeoutMs = options.commandTimeoutMs ?? 5 * 60 * 1000;
  const runImport = options.runImport ?? defaultImportRunner(home);

  if (!acquireLock(lockPath)) {
    const summary: MemoryIngestSummary = {
      schema_version: 1,
      status: 'skipped',
      generated_at: generatedAt,
      scanned_files: 0,
      changed_files: 0,
      imported_pages: 0,
      unchanged_pages: 0,
      reason: 'another memory ingest run is active',
    };
    appendSummary(logPath, summary);
    return summary;
  }

  let scannedFiles = 0;
  let changedFiles = 0;
  try {
    const previous = readState(statePath);
    const nextFiles: MemoryIngestState['files'] = {};
    const changed: Array<{ source: MemoryIngestSource; path: string; relativePath: string }> = [];

    for (const source of sources) {
      validateSource(source);
      for (const path of collectMarkdownFiles(source.root)) {
        const relativePath = relative(source.root, path);
        if (!isPathInside(source.root, path) || relativePath.startsWith('..')) {
          throw new Error(`source file escaped configured root: ${path}`);
        }
        const key = `${source.slugPrefix}/${relativePath.split(sep).join('/')}`;
        const digest = fileDigest(path);
        nextFiles[key] = digest;
        scannedFiles += 1;
        if (previous?.files[key]?.sha256 !== digest.sha256) changed.push({ source, path, relativePath });
      }
    }
    changedFiles = changed.length;

    let importedPages = 0;
    let unchangedPages = 0;
    let failedFiles = 0;
    let timedOutFiles = 0;
    let lockTimeoutFiles = 0;
    let lockRetries = 0;
    const failedKeys = new Set<string>();
    const failureReasons: string[] = [];
    if (changed.length > 0) {
      mkdirSync(stagingRoot, { recursive: true });
      for (const item of changed) {
        const key = `${item.source.slugPrefix}/${item.relativePath.split(sep).join('/')}`;
        const parts = splitMarkdownForIngest(readFileSync(item.path, 'utf8'));
        let itemImported = 0;
        let itemUnchanged = 0;
        let itemFailed = false;
        let itemTimedOut = false;
        let itemLockTimeout = false;
        let itemFailure = '';

        for (let partIndex = 0; partIndex < parts.length; partIndex += 1) {
          const partRoot = mkdtempSync(join(stagingRoot, 'memory-ingest-file-'));
          try {
            const partRelativePath = parts.length === 1
              ? item.relativePath
              : `${item.relativePath.replace(/\.md$/i, '')}.part-${String(partIndex + 1).padStart(3, '0')}.md`;
            const destination = join(partRoot, item.source.slugPrefix, partRelativePath);
            mkdirSync(dirname(destination), { recursive: true });
            writeFileSync(destination, parts[partIndex], 'utf8');

            const { result, retries } = importWithLockRetry(runImport,
              ['import', partRoot, '--no-embed', '--workers', '1', '--json'],
              { cwd: home, timeoutMs }, options.monotonicNow, options.sleep,
            );
            lockRetries += retries;
            const payload = parseImportSummary(result.stdout);
            const invalidSkips = result.stderr.split(/\r?\n/)
              .filter((line) => /^\s*(?:Warning:\s*)?skipped .+:/i.test(line));
            const imported = Number(payload?.imported ?? 0);
            const skipped = Number(payload?.skipped ?? 0);
            const errors = Number(payload?.errors ?? 0);
            const totalFiles = Number(payload?.total_files ?? Number.NaN);
            const countersComplete = totalFiles === 1 && imported + skipped + errors === 1;
            if (result.exitCode !== 0 || !payload || errors !== 0 || !countersComplete || invalidSkips.length > 0) {
              itemTimedOut = isTimedOutImport(result);
              itemLockTimeout = isLockTimeout(result);
              const detail = [
                itemTimedOut
                  ? `gbrain import timed out after ${timeoutMs}ms`
                  : itemLockTimeout ? `gbrain import lock contention exhausted retries=${retries} within ${timeoutMs}ms budget`
                  : `gbrain import failed with exit ${result.exitCode}`,
                !countersComplete
                  ? `incomplete import counters: staged=1 total=${totalFiles} imported=${imported} skipped=${skipped} errors=${errors}`
                  : '',
                invalidSkips.length ? `invalid skips: ${invalidSkips.join(' | ')}` : '',
                errorTail(result.stderr),
              ].filter(Boolean).join('\n');
              throw new Error(detail);
            }
            itemImported += imported;
            itemUnchanged += skipped;
          } catch (error) {
            itemFailed = true;
            itemFailure = error instanceof Error ? error.message : String(error);
            break;
          } finally {
            rmSync(partRoot, { recursive: true, force: true });
          }
        }

        if (itemFailed) {
          failedFiles += 1;
          if (itemTimedOut) timedOutFiles += 1;
          if (itemLockTimeout) lockTimeoutFiles += 1;
          failedKeys.add(key);
          failureReasons.push(`${key}: ${itemFailure}`);
          continue;
        }
        importedPages += itemImported;
        unchangedPages += itemUnchanged;
      }
    }

    // Persist successful files even when one pathological page timed out. A
    // failed key keeps its previous digest (or is omitted if new), so the next
    // launch retries only the unresolved source instead of replaying the whole
    // batch. `last_success_at` remains honest until every changed file lands.
    const committedFiles = { ...nextFiles };
    for (const key of failedKeys) {
      if (previous?.files[key]) committedFiles[key] = previous.files[key];
      else delete committedFiles[key];
    }
    writeState(statePath, {
      schema_version: 1,
      last_success_at: failedFiles === 0 ? generatedAt : (previous?.last_success_at ?? ''),
      files: committedFiles,
    });
    const summary: MemoryIngestSummary = {
      schema_version: 1,
      status: failedFiles === 0 ? 'success' : 'failed',
      generated_at: generatedAt,
      scanned_files: scannedFiles,
      changed_files: changedFiles,
      imported_pages: importedPages,
      unchanged_pages: unchangedPages,
      ...(failedFiles > 0 ? { failed_files: failedFiles } : {}),
      ...(timedOutFiles > 0 ? { timed_out_files: timedOutFiles } : {}),
      ...(lockTimeoutFiles > 0 ? { lock_timeout_files: lockTimeoutFiles } : {}),
      ...(lockRetries > 0 ? { lock_retries: lockRetries } : {}),
      ...(failedFiles > 0
        ? { reason: failureReasons.slice(0, 3).join('\n') }
        : changedFiles === 0 ? { reason: 'no source content changed' } : {}),
    };
    appendSummary(logPath, summary);
    return summary;
  } catch (error) {
    const summary: MemoryIngestSummary = {
      schema_version: 1,
      status: 'failed',
      generated_at: generatedAt,
      scanned_files: scannedFiles,
      changed_files: changedFiles,
      imported_pages: 0,
      unchanged_pages: 0,
      reason: error instanceof Error ? error.message : String(error),
    };
    appendSummary(logPath, summary);
    throw error;
  } finally {
    rmSync(lockPath, { recursive: true, force: true });
  }
}

if (import.meta.main) {
  try {
    const summary = runMemoryIngest();
    console.log(JSON.stringify(summary));
    if (summary.status === 'failed') process.exit(1);
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    process.exit(1);
  }
}
