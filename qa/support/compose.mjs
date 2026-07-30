import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { resolve } from 'node:path';

const PROJECT_PATTERN = /^agent-hub-qa-[a-z0-9-]+$/;

function commandText(args) {
  return ['docker', ...args].join(' ');
}

function subnetForIndex(index) {
  const secondOctet = 128 + Math.floor(index / 256);
  const thirdOctet = index % 256;
  return `10.${secondOctet}.${thirdOctet}.0/24`;
}

function qaNetworkSubnets(project) {
  const slot = createHash('sha256').update(project).digest().readUInt16BE(0) % 16_384;
  return [subnetForIndex(slot * 2), subnetForIndex(slot * 2 + 1)];
}

export class ComposeHarness {
  constructor({ repoRoot, project }) {
    if (!PROJECT_PATTERN.test(project)) {
      throw new Error(`QA Compose project must match ${PROJECT_PATTERN}: ${project}`);
    }
    this.repoRoot = repoRoot;
    this.project = project;
    this.composeFile = resolve(repoRoot, 'compose.dev.yml');
    const [hubNetworkSubnet, modelNetworkSubnet] = qaNetworkSubnets(project);
    this.environment = {
      ...process.env,
      FRONTEND_PORT: '0',
      SEED_DEV_USER: 'true',
      SEED_DEV_MODEL_CONNECTION: 'true',
      DEV_MODEL_PROVIDER_BASE_URL: 'http://fake-model-provider:8080',
      DEV_MODEL_PROVIDER_MODEL_IDS: 'hub-proxy-smoke,hub-proxy-smoke-fast',
      DEV_MODEL_PROVIDER_API_KEY: 'dev-model-provider-api-key',
      COMPOSE_PROFILES: 'ldap',
      HUB_NETWORK_SUBNET: hubNetworkSubnet,
      MODEL_NETWORK_SUBNET: modelNetworkSubnet
    };
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
    return this.run(['down', '--volumes', '--remove-orphans'], {
      capture: false,
      allowFailure: true,
      timeoutMs: 2 * 60_000
    });
  }

  remainingRuntimeResources() {
    const filter = `label=com.docker.compose.project=${this.project}`;
    const commands = [
      ['container', 'ls', '--all', '--quiet', '--filter', filter],
      ['network', 'ls', '--quiet', '--filter', filter],
      ['volume', 'ls', '--quiet', '--filter', filter]
    ];
    const [containers, networks, volumes] = commands.map((args) => {
      const result = spawnSync('docker', args, {
        cwd: this.repoRoot,
        env: this.environment,
        encoding: 'utf8',
        stdio: ['ignore', 'pipe', 'pipe'],
        timeout: 30_000
      });
      if (result.error || result.status !== 0) {
        const detail = result.error?.message || result.stderr?.trim() || `exit ${result.status}`;
        throw new Error(`${commandText(args)} failed: ${detail}`);
      }
      return result.stdout.split('\n').map((value) => value.trim()).filter(Boolean);
    });
    return { containers, networks, volumes };
  }

  frontendURL() {
    const output = this.run(['port', 'backend', '8080']).stdout.trim();
    const ports = new Set(output.split('\n').map((line) => line.trim()).filter(Boolean).map((endpoint) => {
      const separator = endpoint.lastIndexOf(':');
      const port = Number(endpoint.slice(separator + 1));
      if (separator < 1 || !Number.isInteger(port) || port < 1 || port > 65_535) {
        throw new Error(`Unexpected Hub port mapping: ${endpoint}`);
      }
      return port;
    }));
    if (ports.size !== 1) {
      throw new Error(`Expected one Hub port, received: ${output || '<empty>'}`);
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
