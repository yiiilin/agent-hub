import { spawnSync } from 'node:child_process';
import { resolve } from 'node:path';

const PROJECT_PATTERN = /^agent-hub-qa-[a-z0-9-]+$/;

function commandText(args) {
  return ['docker', ...args].join(' ');
}

export class ComposeHarness {
  constructor({ repoRoot, project }) {
    if (!PROJECT_PATTERN.test(project)) {
      throw new Error(`QA Compose project must match ${PROJECT_PATTERN}: ${project}`);
    }
    this.repoRoot = repoRoot;
    this.project = project;
    this.composeFile = resolve(repoRoot, 'deploy/docker-compose.yml');
    this.environment = { ...process.env, FRONTEND_PORT: '0' };
  }

  composeArgs(args) {
    return ['compose', '-p', this.project, '-f', this.composeFile, ...args];
  }

  run(args, { capture = true, allowFailure = false, timeoutMs = 60_000 } = {}) {
    const dockerArgs = this.composeArgs(args);
    const result = spawnSync('docker', dockerArgs, {
      cwd: this.repoRoot,
      env: this.environment,
      encoding: 'utf8',
      stdio: capture ? ['ignore', 'pipe', 'pipe'] : 'inherit',
      timeout: timeoutMs
    });
    if (!allowFailure && (result.error || result.status !== 0)) {
      const detail = result.error?.message || result.stderr?.trim() || result.stdout?.trim() || `exit ${result.status}`;
      throw new Error(`${commandText(dockerArgs)} failed: ${detail}`);
    }
    return result;
  }

  start() {
    this.run(['up', '-d', '--build', '--wait', '--remove-orphans'], {
      capture: false,
      timeoutMs: 10 * 60_000
    });
  }

  down() {
    this.run(['down', '--volumes', '--remove-orphans'], {
      capture: false,
      allowFailure: true,
      timeoutMs: 2 * 60_000
    });
  }

  frontendURL() {
    const output = this.run(['port', 'frontend', '5173']).stdout.trim();
    const ports = new Set(output.split('\n').map((line) => line.trim()).filter(Boolean).map((endpoint) => {
      const separator = endpoint.lastIndexOf(':');
      const port = Number(endpoint.slice(separator + 1));
      if (separator < 1 || !Number.isInteger(port) || port < 1 || port > 65_535) {
        throw new Error(`Unexpected frontend port mapping: ${endpoint}`);
      }
      return port;
    }));
    if (ports.size !== 1) {
      throw new Error(`Expected one frontend port, received: ${output || '<empty>'}`);
    }
    return `http://127.0.0.1:${[...ports][0]}`;
  }

  psql(sql) {
    return this.run([
      'exec', '-T', 'postgres',
      'psql', '-X', '-v', 'ON_ERROR_STOP=1', '-U', 'agent_hub', '-d', 'agent_hub',
      '-A', '-t', '-c', sql
    ]).stdout.trim();
  }

  logs() {
    const result = this.run(['logs', '--no-color', '--timestamps'], {
      allowFailure: true,
      timeoutMs: 30_000
    });
    return `${result.stdout ?? ''}${result.stderr ?? ''}`;
  }
}
