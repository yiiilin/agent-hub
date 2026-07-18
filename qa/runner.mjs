import { spawnSync } from 'node:child_process';
import { readFileSync, readdirSync } from 'node:fs';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { ComposeHarness } from './support/compose.mjs';
import { poll } from './support/api.mjs';

const qaRoot = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(qaRoot, '..');
const scenariosRoot = join(qaRoot, 'scenarios');
const workerPath = join(qaRoot, 'scenario-worker.mjs');

function usage() {
  console.log(`Usage: ./qa/run-all.sh [options] [scenario ...]

Options:
  --type api|browser  Run only one scenario type.
  --list              List discovered scenarios without starting Compose.
  --keep-env          Keep the isolated QA Compose environment after the run.
  --help              Show this help.`);
}

function parseArgs(argv) {
  const options = { type: null, list: false, keepEnv: false, scenarios: [] };
  for (let index = 0; index < argv.length; index += 1) {
    const value = argv[index];
    if (value === '--help') return { ...options, help: true };
    if (value === '--list') options.list = true;
    else if (value === '--keep-env') options.keepEnv = true;
    else if (value === '--type') {
      const type = argv[index + 1];
      if (!['api', 'browser'].includes(type)) throw new Error('--type must be api or browser');
      options.type = type;
      index += 1;
    } else if (value.startsWith('-')) {
      throw new Error(`Unknown option: ${value}`);
    } else {
      options.scenarios.push(value);
    }
  }
  return options;
}

function discoverScenarios() {
  return readdirSync(scenariosRoot, { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .map((entry) => {
      const directory = join(scenariosRoot, entry.name);
      const manifest = JSON.parse(readFileSync(join(directory, 'scenario.json'), 'utf8'));
      if (!manifest.name || !['api', 'browser'].includes(manifest.type)) {
        throw new Error(`${entry.name}/scenario.json requires name and type api|browser`);
      }
      const timeoutMs = Number(manifest.timeout_ms ?? 90_000);
      if (!Number.isInteger(timeoutMs) || timeoutMs < 1_000 || timeoutMs > 10 * 60_000) {
        throw new Error(`${entry.name}/scenario.json has an invalid timeout_ms`);
      }
      return {
        id: entry.name,
        name: manifest.name,
        type: manifest.type,
        timeoutMs,
        entry: join(directory, 'scenario.mjs')
      };
    })
    .sort((left, right) => left.id.localeCompare(right.id));
}

function selectScenarios(allScenarios, options) {
  const requested = new Set(options.scenarios);
  const known = new Set(allScenarios.map((scenario) => scenario.id));
  const unknown = [...requested].filter((id) => !known.has(id));
  if (unknown.length > 0) throw new Error(`Unknown scenario: ${unknown.join(', ')}`);
  const selected = allScenarios.filter((scenario) => (
    (!options.type || scenario.type === options.type)
    && (requested.size === 0 || requested.has(scenario.id))
  ));
  if (selected.length === 0) throw new Error('No scenarios matched the requested filters');
  return selected;
}

function xmlEscape(value) {
  return String(value)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&apos;');
}

async function writeSummary(artifactsRoot, project, baseURL, results) {
  const summary = {
    project,
    base_url: baseURL,
    passed: results.filter((result) => result.status === 'passed').length,
    failed: results.filter((result) => result.status === 'failed').length,
    scenarios: results
  };
  await writeFile(join(artifactsRoot, 'summary.json'), `${JSON.stringify(summary, null, 2)}\n`);
  const failures = summary.failed;
  const durationSeconds = results.reduce((total, result) => total + result.duration_ms, 0) / 1_000;
  const cases = results.map((result) => {
    const failure = result.status === 'failed'
      ? `<failure message="${xmlEscape(result.error)}">${xmlEscape(result.error)}</failure>`
      : '';
    return `  <testcase classname="agent-hub.qa.${result.type}" name="${xmlEscape(result.id)}" time="${(result.duration_ms / 1_000).toFixed(3)}">${failure}</testcase>`;
  }).join('\n');
  const junit = `<?xml version="1.0" encoding="UTF-8"?>\n<testsuite name="agent-hub-qa" tests="${results.length}" failures="${failures}" time="${durationSeconds.toFixed(3)}">\n${cases}\n</testsuite>\n`;
  await writeFile(join(artifactsRoot, 'junit.xml'), junit);
}

async function healthCheck(baseURL) {
  await poll(async () => {
    try {
      return (await fetch(`${baseURL}/healthz`)).status;
    } catch {
      return 0;
    }
  }, (status) => status === 200, {
    timeoutMs: 30_000,
    intervalMs: 250,
    description: `${baseURL}/healthz to return 200`
  });
}

async function workerFailure(artifactsDir, fallback) {
  try {
    const failure = JSON.parse(await readFile(join(artifactsDir, 'failure.json'), 'utf8'));
    return failure.message || fallback;
  } catch {
    return fallback;
  }
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  if (options.help) {
    usage();
    return;
  }
  const allScenarios = discoverScenarios();
  if (options.list) {
    for (const scenario of allScenarios) console.log(`${scenario.id}\t${scenario.type}\t${scenario.name}`);
    return;
  }
  const scenarios = selectScenarios(allScenarios, options);
  const runId = new Date().toISOString().replaceAll(':', '-').replaceAll('.', '-');
  const artifactsRoot = join(qaRoot, 'artifacts', runId);
  await mkdir(artifactsRoot, { recursive: true });
  const project = process.env.QA_COMPOSE_PROJECT?.trim()
    || `agent-hub-qa-${Date.now().toString(36)}-${process.pid}`;
  const compose = new ComposeHarness({ repoRoot, project });
  let started = false;
  let baseURL = '';
  let interrupted = false;

  const stopForSignal = (signal) => {
    if (interrupted) return;
    interrupted = true;
    console.error(`\nReceived ${signal}; cleaning up ${project}.`);
    if (started && !options.keepEnv) compose.down();
    process.exit(signal === 'SIGINT' ? 130 : 143);
  };
  process.once('SIGINT', stopForSignal);
  process.once('SIGTERM', stopForSignal);

  const results = [];
  try {
    console.log(`Starting isolated QA environment: ${project}`);
    started = true;
    compose.start();
    baseURL = compose.frontendURL();
    await healthCheck(baseURL);
    console.log(`QA environment ready: ${baseURL}`);

    for (const scenario of scenarios) {
      const artifactsDir = join(artifactsRoot, scenario.id);
      await mkdir(artifactsDir, { recursive: true });
      console.log(`\n[RUN ] ${scenario.id} (${scenario.type}) ${scenario.name}`);
      const startedAt = Date.now();
      const worker = spawnSync(process.execPath, [workerPath], {
        cwd: repoRoot,
        env: {
          ...process.env,
          QA_REPO_ROOT: repoRoot,
          QA_COMPOSE_PROJECT: project,
          QA_BASE_URL: baseURL,
          QA_SCENARIO_ID: scenario.id,
          QA_SCENARIO_NAME: scenario.name,
          QA_SCENARIO_TYPE: scenario.type,
          QA_SCENARIO_ENTRY: scenario.entry,
          QA_ARTIFACTS_DIR: artifactsDir
        },
        stdio: 'inherit',
        timeout: scenario.timeoutMs,
        killSignal: 'SIGTERM'
      });
      const durationMs = Date.now() - startedAt;
      if (!worker.error && worker.status === 0) {
        results.push({ id: scenario.id, name: scenario.name, type: scenario.type, status: 'passed', duration_ms: durationMs });
        console.log(`[PASS] ${scenario.id} (${durationMs} ms)`);
        continue;
      }
      const fallback = worker.error?.code === 'ETIMEDOUT'
        ? `Timed out after ${scenario.timeoutMs} ms`
        : worker.error?.message || `Scenario worker exited with status ${worker.status}`;
      const error = await workerFailure(artifactsDir, fallback);
      await writeFile(join(artifactsDir, 'compose.log'), compose.logs());
      results.push({ id: scenario.id, name: scenario.name, type: scenario.type, status: 'failed', duration_ms: durationMs, error });
      console.error(`[FAIL] ${scenario.id}: ${error}`);
    }

    await writeSummary(artifactsRoot, project, baseURL, results);
    const passed = results.filter((result) => result.status === 'passed').length;
    const failed = results.length - passed;
    console.log(`\nQA summary: ${passed} passed, ${failed} failed`);
    console.log(`Artifacts: ${artifactsRoot}`);
    if (failed > 0) process.exitCode = 1;
  } finally {
    process.removeListener('SIGINT', stopForSignal);
    process.removeListener('SIGTERM', stopForSignal);
    if (started) {
      if (options.keepEnv) console.log(`Keeping QA environment ${project} at ${baseURL}`);
      else compose.down();
    }
  }
}

await main().catch((error) => {
  console.error(error instanceof Error ? error.stack : String(error));
  process.exitCode = 1;
});
