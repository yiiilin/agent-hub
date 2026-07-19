import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

export const DEFAULT_E2E_COMPOSE_PROJECT = 'agent-hub-dev';
const DISCOVERY_BASE_URL = 'http://127.0.0.1:9';
const COMPOSE_FILE = fileURLToPath(new URL('../../compose.dev.yml', import.meta.url));

export function e2eComposeProject(env: NodeJS.ProcessEnv = process.env) {
  return env.E2E_COMPOSE_PROJECT?.trim() || DEFAULT_E2E_COMPOSE_PROJECT;
}

export function composeArgs(project = e2eComposeProject()) {
  return ['compose', '-p', project, '-f', COMPOSE_FILE] as const;
}

function canonicalHost(host: string) {
  const unwrapped = host.replace(/^\[(.*)\]$/, '$1').toLowerCase();
  if (['0.0.0.0', '127.0.0.1', '::', '::1', 'localhost'].includes(unwrapped)) {
    return 'localhost';
  }
  return unwrapped;
}

function hostForURL(host: string) {
  return host.includes(':') ? `[${host}]` : host;
}

export function normalizeBaseURL(value: string, label = 'base URL') {
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    throw new Error(`${label} must be an absolute HTTP URL: ${value}`);
  }
  if (!['http:', 'https:'].includes(url.protocol)
    || url.username
    || url.password
    || url.pathname !== '/'
    || url.search
    || url.hash) {
    throw new Error(`${label} must contain only an HTTP(S) origin: ${value}`);
  }
  const host = canonicalHost(url.hostname);
  const port = url.port ? `:${url.port}` : '';
  return `${url.protocol}//${hostForURL(host)}${port}`;
}

export function parseComposePort(output: string) {
  const origins = new Set(output.split('\n').map((line) => line.trim()).filter(Boolean).map((endpoint) => {
    const separator = endpoint.lastIndexOf(':');
    if (separator <= 0) throw new Error(`Unexpected Docker Compose port output: ${endpoint}`);
    const host = canonicalHost(endpoint.slice(0, separator));
    const port = Number(endpoint.slice(separator + 1));
    if (!Number.isInteger(port) || port < 1 || port > 65_535) {
      throw new Error(`Unexpected Docker Compose port output: ${endpoint}`);
    }
    return `http://${hostForURL(host)}:${port}`;
  }));
  if (origins.size !== 1) {
    throw new Error(`Expected one frontend port mapping, received: ${output.trim() || '<empty>'}`);
  }
  return [...origins][0];
}

function dockerOutput(args: readonly string[], failureMessage: string) {
  try {
    return execFileSync('docker', [...args], {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'pipe']
    }).trim();
  } catch {
    throw new Error(failureMessage);
  }
}

export function resolveComposeFrontendURL(project = e2eComposeProject()) {
  const output = dockerOutput(
    [...composeArgs(project), 'port', 'frontend', '5173'],
    `Unable to resolve frontend port for Compose project "${project}". Start that project before running Playwright.`
  );
  return parseComposePort(output);
}

export function validateExplicitBaseURL(explicitURL: string, composeURL: string, project: string) {
  const explicit = normalizeBaseURL(explicitURL, 'E2E_BASE_URL');
  const expected = normalizeBaseURL(composeURL, 'Docker Compose frontend URL');
  if (explicit !== expected) {
    throw new Error(
      `E2E_BASE_URL ${explicit} targets a different Compose project; project "${project}" publishes frontend at ${expected}.`
    );
  }
  return expected;
}

export function isPlaywrightDiscovery(argv: readonly string[] = process.argv) {
  return argv.includes('--list');
}

export function resolvePlaywrightBaseURL(env: NodeJS.ProcessEnv = process.env, argv: readonly string[] = process.argv) {
  if (isPlaywrightDiscovery(argv)) {
    return env.E2E_BASE_URL?.trim()
      ? normalizeBaseURL(env.E2E_BASE_URL, 'E2E_BASE_URL')
      : DISCOVERY_BASE_URL;
  }

  const project = e2eComposeProject(env);
  const composeURL = resolveComposeFrontendURL(project);
  return env.E2E_BASE_URL?.trim()
    ? validateExplicitBaseURL(env.E2E_BASE_URL, composeURL, project)
    : composeURL;
}

export function assertComposeFrontendTarget(project: string, configuredBaseURL: string) {
  const composeURL = resolveComposeFrontendURL(project);
  if (normalizeBaseURL(configuredBaseURL, 'Playwright baseURL') !== composeURL) {
    throw new Error(`Playwright baseURL does not match Compose project "${project}" at ${composeURL}.`);
  }

  const containerIDs = dockerOutput(
    [...composeArgs(project), 'ps', '-q', 'frontend'],
    `Unable to find the frontend container for Compose project "${project}".`
  ).split('\n').map((line) => line.trim()).filter(Boolean);
  if (containerIDs.length !== 1) {
    throw new Error(`Expected one running frontend container for Compose project "${project}", found ${containerIDs.length}.`);
  }

  const inspection = dockerOutput(
    ['inspect', '--format', '{{.State.Running}}|{{ index .Config.Labels "com.docker.compose.project" }}', containerIDs[0]],
    `Unable to inspect the frontend container for Compose project "${project}".`
  );
  const [running, actualProject] = inspection.split('|');
  if (running !== 'true') {
    throw new Error(`Frontend container for Compose project "${project}" is not running.`);
  }
  if (actualProject !== project) {
    throw new Error(`Frontend container belongs to Compose project "${actualProject}", expected "${project}".`);
  }
}
