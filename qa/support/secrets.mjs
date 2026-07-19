import { execFile } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import {
  lstat,
  mkdir,
  mkdtemp,
  readFile,
  readdir,
  rename,
  rm,
  writeFile
} from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { basename, dirname, isAbsolute, join } from 'node:path';
import { promisify } from 'node:util';

const REDACTED = '[REDACTED]';
const execFileAsync = promisify(execFile);
const SECRET_TOKEN_PREFIXES = ['ahs', 'ahe', 'aho', 'ahw', 'ahk', 'ahre', 'ahrc', 'ahrt', 'ahr'];
const TOKEN_PATTERN = new RegExp(
  `\\b(?:${SECRET_TOKEN_PREFIXES.join('|')})_[A-Za-z0-9._-]+\\b`,
  'g'
);
const BEARER_PATTERN = /\bBearer\s+[^\s"'`,;}]+/gi;
const HEADER_PATTERN = /(\b(?:authorization|proxy-authorization|cookie|set-cookie)\s*:\s*)[^\r\n]*/gi;
const JSON_FIELD_PATTERN = /("(?:api[_-]?key|client[_-]?secret|(?:access[_-]|refresh[_-]|session[_-]|webhook[_-])?token|secret|password|cookie|authorization)"\s*:\s*)("(?:\\.|[^"\\])*"|[^,\r\n}\]]+)/gi;
const ZIP_SIGNATURES = new Set(['504b0304', '504b0506', '504b0708']);
const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const URL_PATTERN = /(?:https?:\/\/[^\s"'<>]+|\/[^\s"'<>?]*\?[^\s"'<>]+)/gi;

function normalizedName(value) {
  return value
    .replace(/([a-z0-9])([A-Z])/g, '$1_$2')
    .replace(/[\s-]+/g, '_')
    .toLowerCase();
}

function isSensitiveName(value) {
  const name = normalizedName(value);
  return /(?:^|_)(?:api_key|client_secret|secret|token|password|cookie|cookies|authorization|set_cookie)(?:$|_)/.test(name);
}

function isSensitiveParameter(name, value) {
  return isSensitiveName(name)
    || (normalizedName(name) === 'code' && typeof value === 'string' && UUID_PATTERN.test(value));
}

function redactSearchParams(params) {
  const redacted = new URLSearchParams();
  let changed = false;
  for (const [name, value] of params) {
    const replacement = isSensitiveParameter(name, value) && value !== REDACTED
      ? REDACTED
      : value;
    changed ||= replacement !== value;
    redacted.append(name, replacement);
  }
  return { changed, value: redacted.toString() };
}

function redactUrl(value) {
  try {
    const absolute = /^https?:\/\//i.test(value);
    const url = absolute ? new URL(value) : new URL(value, 'http://artifact.invalid');
    const query = redactSearchParams(url.searchParams);
    if (!query.changed) return value;
    url.search = query.value;
    return absolute ? url.toString() : `${url.pathname}${url.search}${url.hash}`;
  } catch {
    return value;
  }
}

function redactForm(value) {
  if (!value.includes('=') || /[\r\n]/.test(value)) return value;
  const form = redactSearchParams(new URLSearchParams(value));
  return form.changed ? form.value : value;
}

function redactMarkup(value) {
  return value
    .replace(/(<input\b[^>]*\bvalue\s*=\s*)(["'])(.*?)\2/gi, `$1$2${REDACTED}$2`)
    .replace(/<(textarea|code)\b([^>]*)>[\s\S]*?<\/\1>/gi, `<$1$2>${REDACTED}</$1>`);
}

function redactPlainText(value) {
  const structured = redactForm(redactMarkup(value).replace(URL_PATTERN, redactUrl));
  return structured
    .replace(HEADER_PATTERN, `$1${REDACTED}`)
    .replace(JSON_FIELD_PATTERN, `$1"${REDACTED}"`)
    .replace(BEARER_PATTERN, REDACTED)
    .replace(TOKEN_PATTERN, REDACTED);
}

function isDomSnapshotNode(value) {
  return Array.isArray(value)
    && typeof value[0] === 'string'
    && /^[A-Z][A-Z0-9-]*$/.test(value[0])
    && value[1] !== null
    && typeof value[1] === 'object'
    && !Array.isArray(value[1]);
}

function isSensitiveDomNode(value) {
  if (!isDomSnapshotNode(value)) return false;
  const tagName = value[0].toLowerCase();
  if (['input', 'textarea', 'select', 'code'].includes(tagName)) return true;
  const attributes = value[1];
  return ['class', 'id', 'name', 'aria-label', 'data-testid', 'type']
    .some((name) => typeof attributes[name] === 'string'
      && /secret|token|password|credential|api[-_ ]?key/i.test(attributes[name]));
}

function redactContainerValues(value) {
  if (value === REDACTED) return REDACTED;
  if (Array.isArray(value)) return value.map(redactContainerValues);
  if (value !== null && typeof value === 'object' && Object.getPrototypeOf(value) === Object.prototype) {
    return Object.fromEntries(Object.entries(value).map(([name, entry]) => [
      name,
      redactContainerValues(entry)
    ]));
  }
  return REDACTED;
}

function redactStructured(value, key = '') {
  if (normalizedName(key) === 'secrets') return redactContainerValues(value);
  if (isSensitiveName(key)) return REDACTED;
  if (typeof value === 'string') return redactSecretString(value);
  if (value === null || typeof value !== 'object') return value;
  if (Buffer.isBuffer(value) || ArrayBuffer.isView(value)) return value;

  if (Array.isArray(value)) {
    const sensitiveDomNode = isSensitiveDomNode(value);
    return value.map((entry, index) => {
      if (sensitiveDomNode && index >= 2) return REDACTED;
      if (sensitiveDomNode && index === 1) {
        return Object.fromEntries(Object.entries(entry).map(([name, attribute]) => [
          name,
          name.toLowerCase().includes('value') ? REDACTED : redactStructured(attribute, name)
        ]));
      }
      return redactStructured(entry);
    });
  }

  if (Object.getPrototypeOf(value) !== Object.prototype) return value;
  const namedValueIsSensitive = typeof value.name === 'string'
    && isSensitiveParameter(value.name, value.value);
  return Object.fromEntries(Object.entries(value).map(([name, entry]) => [
    name,
    namedValueIsSensitive && name.toLowerCase() === 'value' ? REDACTED : redactStructured(entry, name)
  ]));
}

function redactJsonCandidate(candidate) {
  const parsed = JSON.parse(candidate);
  const redacted = redactStructured(parsed);
  return JSON.stringify(redacted) === JSON.stringify(parsed) ? null : JSON.stringify(redacted);
}

function redactSecretString(value) {
  const documentLeading = value.match(/^\s*/)?.[0] ?? '';
  const documentTrailing = value.match(/\s*$/)?.[0] ?? '';
  const documentEnd = value.length - documentTrailing.length;
  const documentCandidate = value.slice(documentLeading.length, documentEnd);
  if (documentCandidate.startsWith('{') || documentCandidate.startsWith('[')) {
    try {
      const redacted = redactJsonCandidate(documentCandidate);
      if (redacted !== null) return `${documentLeading}${redacted}${documentTrailing}`;
    } catch {
      // JSONL and ordinary text are handled line by line below.
    }
  }

  const parts = value.split(/(\r?\n)/);
  const redactedParts = parts.map((part) => {
    if (/^\r?\n$/.test(part)) return part;
    const leading = part.match(/^\s*/)?.[0] ?? '';
    const trailing = part.match(/\s*$/)?.[0] ?? '';
    const candidate = part.slice(leading.length, part.length - trailing.length || undefined);
    if (candidate.startsWith('{') || candidate.startsWith('[')) {
      try {
        const redacted = redactJsonCandidate(candidate);
        if (redacted !== null) return `${leading}${redacted}${trailing}`;
      } catch {
        // Trace files are JSONL; non-JSON lines still receive text redaction below.
      }
    }
    return redactPlainText(part);
  }).join('');
  return redactMarkup(redactedParts);
}

export function redactSecrets(value) {
  if (typeof value === 'string') return redactSecretString(value);
  return redactStructured(value);
}

function isZip(buffer, path) {
  return path.toLowerCase().endsWith('.zip')
    || (buffer.length >= 4 && ZIP_SIGNATURES.has(buffer.subarray(0, 4).toString('hex')));
}

function decodeText(buffer) {
  if (buffer.includes(0)) return null;
  try {
    return new TextDecoder('utf-8', { fatal: true }).decode(buffer);
  } catch {
    return null;
  }
}

function binaryContainsSecret(buffer) {
  const value = buffer.toString('latin1');
  return redactPlainText(value) !== value;
}

async function runZipCommand(command, args, options = {}) {
  return execFileAsync(command, args, {
    ...options,
    encoding: 'utf8',
    maxBuffer: 16 * 1024 * 1024,
    shell: false,
    windowsHide: true
  });
}

function validateZipEntries(output) {
  for (const entry of output.split(/\r?\n/).filter(Boolean)) {
    const normalized = entry.replaceAll('\\', '/');
    if (isAbsolute(normalized)
      || /^[A-Za-z]:\//.test(normalized)
      || normalized.split('/').includes('..')) {
      throw new Error('ZIP contains an unsafe entry path');
    }
  }
}

async function atomicWrite(path, value) {
  const temporaryPath = join(dirname(path), `.${basename(path)}.redacted-${randomUUID()}`);
  try {
    await writeFile(temporaryPath, value, { flag: 'wx' });
    await rename(temporaryPath, path);
  } finally {
    await rm(temporaryPath, { force: true }).catch(() => undefined);
  }
}

async function sanitizeZip(path) {
  const extractionRoot = await mkdtemp(join(tmpdir(), 'agent-hub-artifact-zip-'));
  const extracted = join(extractionRoot, 'contents');
  const rebuilt = join(dirname(path), `.${basename(path)}.redacted-${randomUUID()}.zip`);
  try {
    const listing = await runZipCommand('unzip', ['-Z1', path]);
    validateZipEntries(listing.stdout);
    if (listing.stdout.trim() === '') return;

    await mkdir(extracted, { recursive: true });
    await runZipCommand('unzip', ['-q', '-o', path, '-d', extracted]);
    await sanitizeTree(extracted);
    await runZipCommand('zip', ['-q', '-r', '-X', rebuilt, '.'], { cwd: extracted });
    await runZipCommand('unzip', ['-tqq', rebuilt]);
    await rename(rebuilt, path);
  } finally {
    await rm(rebuilt, { force: true }).catch(() => undefined);
    await rm(extractionRoot, { recursive: true, force: true }).catch(() => undefined);
  }
}

async function sanitizeFile(path) {
  const buffer = await readFile(path);
  if (isZip(buffer, path)) {
    await sanitizeZip(path);
    return;
  }
  const text = decodeText(buffer);
  if (text === null) {
    if (binaryContainsSecret(buffer)) throw new Error('Binary artifact contains a secret');
    return;
  }
  const redacted = redactSecrets(text);
  if (redacted !== text) await atomicWrite(path, redacted);
}

async function sanitizeTree(root) {
  const stat = await lstat(root);
  if (stat.isSymbolicLink()) throw new Error('Artifact tree contains a symbolic link');
  if (stat.isFile()) {
    await sanitizeFile(root);
    return;
  }
  if (!stat.isDirectory()) return;
  for (const entry of await readdir(root, { withFileTypes: true })) {
    await sanitizeTree(join(root, entry.name));
  }
}

async function clearArtifactRoot(root) {
  let stat;
  try {
    stat = await lstat(root);
  } catch (error) {
    if (error?.code === 'ENOENT') return;
    throw error;
  }
  if (!stat.isDirectory() || stat.isSymbolicLink()) {
    await rm(root, { recursive: true, force: true });
    return;
  }
  await Promise.all((await readdir(root)).map((entry) => (
    rm(join(root, entry), { recursive: true, force: true })
  )));
}

export async function sanitizeArtifactTree(root) {
  try {
    await sanitizeTree(root);
  } catch (error) {
    if (error?.code === 'ENOENT') return;
    await clearArtifactRoot(root).catch(() => undefined);
    throw new Error('Artifact sanitization failed; unsafe artifacts were removed');
  }
}

async function zipIsUnsafe(path) {
  const extractionRoot = await mkdtemp(join(tmpdir(), 'agent-hub-artifact-scan-'));
  const extracted = join(extractionRoot, 'contents');
  try {
    const listing = await runZipCommand('unzip', ['-Z1', path]);
    validateZipEntries(listing.stdout);
    if (listing.stdout.trim() === '') return false;
    await mkdir(extracted, { recursive: true });
    await runZipCommand('unzip', ['-q', '-o', path, '-d', extracted]);
    return await treeIsUnsafe(extracted);
  } catch {
    return true;
  } finally {
    await rm(extractionRoot, { recursive: true, force: true }).catch(() => undefined);
  }
}

async function treeIsUnsafe(root) {
  const stat = await lstat(root);
  if (stat.isSymbolicLink()) return true;
  if (stat.isFile()) {
    const buffer = await readFile(root);
    if (isZip(buffer, root)) return zipIsUnsafe(root);
    const text = decodeText(buffer);
    return text === null ? binaryContainsSecret(buffer) : redactSecrets(text) !== text;
  }
  if (!stat.isDirectory()) return false;
  for (const entry of await readdir(root, { withFileTypes: true })) {
    if (await treeIsUnsafe(join(root, entry.name))) return true;
  }
  return false;
}

export async function assertArtifactTreeSafe(root) {
  try {
    if (await treeIsUnsafe(root)) {
      await clearArtifactRoot(root);
      throw new Error('Artifact safety assertion failed; unsafe artifacts were removed');
    }
  } catch (error) {
    if (error?.code === 'ENOENT') return;
    if (error?.message === 'Artifact safety assertion failed; unsafe artifacts were removed') throw error;
    await clearArtifactRoot(root).catch(() => undefined);
    throw new Error('Artifact safety assertion failed; unreadable artifacts were removed');
  }
}
