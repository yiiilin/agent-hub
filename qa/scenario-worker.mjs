import { randomUUID } from 'node:crypto';
import { mkdir, rm, writeFile } from 'node:fs/promises';
import { pathToFileURL } from 'node:url';
import { ComposeHarness } from './support/compose.mjs';
import {
  assertArtifactTreeSafe,
  redactSecrets,
  sanitizeArtifactTree
} from './support/secrets.mjs';

const required = [
  'QA_REPO_ROOT',
  'QA_COMPOSE_PROJECT',
  'QA_BASE_URL',
  'QA_SCENARIO_ID',
  'QA_SCENARIO_NAME',
  'QA_SCENARIO_TYPE',
  'QA_SCENARIO_ENTRY',
  'QA_ARTIFACTS_DIR'
];
for (const name of required) {
  if (!process.env[name]) throw new Error(`Missing worker environment variable: ${name}`);
}

const artifactsDir = process.env.QA_ARTIFACTS_DIR;
await mkdir(artifactsDir, { recursive: true });

try {
  const scenarioModule = await import(`${pathToFileURL(process.env.QA_SCENARIO_ENTRY).href}?run=${Date.now()}`);
  if (typeof scenarioModule.default !== 'function') {
    throw new Error(`${process.env.QA_SCENARIO_ENTRY} must export a default async function`);
  }
  const compose = new ComposeHarness({
    repoRoot: process.env.QA_REPO_ROOT,
    project: process.env.QA_COMPOSE_PROJECT
  });
  await scenarioModule.default({
    id: process.env.QA_SCENARIO_ID,
    name: process.env.QA_SCENARIO_NAME,
    type: process.env.QA_SCENARIO_TYPE,
    repoRoot: process.env.QA_REPO_ROOT,
    baseURL: process.env.QA_BASE_URL,
    artifactsDir,
    compose,
    unique(prefix) {
      return `${prefix}-${Date.now()}-${randomUUID().slice(0, 8)}`;
    }
  });
} catch (error) {
  let failure = redactSecrets({
    message: error instanceof Error ? error.message : String(error),
    stack: error instanceof Error ? error.stack : undefined
  });
  try {
    await writeFile(`${artifactsDir}/failure.json`, `${JSON.stringify(failure, null, 2)}\n`);
    await sanitizeArtifactTree(artifactsDir);
    await assertArtifactTreeSafe(artifactsDir);
  } catch (artifactError) {
    await rm(artifactsDir, { recursive: true, force: true }).catch(() => undefined);
    await mkdir(artifactsDir, { recursive: true });
    failure = redactSecrets({
      ...failure,
      artifact_error: artifactError instanceof Error ? artifactError.message : String(artifactError)
    });
    try {
      await writeFile(`${artifactsDir}/failure.json`, `${JSON.stringify(failure, null, 2)}\n`);
      await assertArtifactTreeSafe(artifactsDir);
    } catch (diagnosticError) {
      await rm(artifactsDir, { recursive: true, force: true }).catch(() => undefined);
      console.error(redactSecrets(
        diagnosticError instanceof Error ? diagnosticError.message : String(diagnosticError)
      ));
    }
  }
  console.error(failure.stack ?? failure.message);
  process.exitCode = 1;
}
