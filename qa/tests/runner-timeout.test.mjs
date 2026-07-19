import assert from 'node:assert/strict';
import { mkdirSync, mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import {
  executeScenarioQueue,
  isWorkerHardTimeout,
  writeSummary
} from '../runner.mjs';

function scenario(id) {
  return { id, name: id, type: 'api', timeoutMs: 1_000 };
}

test('hard timeout stops worker launches and records every remaining scenario as not_run', async (t) => {
  const scenarios = [
    scenario('01-ordinary-failure'),
    scenario('02-hard-timeout'),
    scenario('03-never-started'),
    scenario('04-also-never-started')
  ];
  const started = [];
  const results = await executeScenarioQueue(scenarios, async (current) => {
    started.push(current.id);
    if (current.id === '01-ordinary-failure') {
      return {
        result: {
          ...current,
          status: 'failed',
          duration_ms: 10,
          error: 'ordinary scenario failure'
        }
      };
    }

    const worker = { error: Object.assign(new Error('spawnSync ETIMEDOUT'), { code: 'ETIMEDOUT' }) };
    return {
      result: {
        ...current,
        status: 'failed',
        duration_ms: 1_000,
        error: 'Timed out after 1000 ms'
      },
      hardTimeout: isWorkerHardTimeout(worker)
    };
  });

  assert.deepEqual(started, ['01-ordinary-failure', '02-hard-timeout']);
  assert.deepEqual(results.map((result) => result.status), [
    'failed',
    'failed',
    'not_run',
    'not_run'
  ]);
  for (const result of results.slice(2)) {
    assert.equal(result.duration_ms, 0);
    assert.match(result.reason, /02-hard-timeout hit its hard timeout/);
    assert.match(result.reason, /shared QA environment may be contaminated/);
  }

  const root = mkdtempSync(join(tmpdir(), 'agent-hub-runner-timeout-'));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const artifacts = join(root, 'artifacts');
  mkdirSync(artifacts);
  const coverage = { selected: { complete: true }, overall: { complete: true } };
  await writeSummary(artifacts, 'fixture', 'http://127.0.0.1:1', results, coverage);

  const summary = JSON.parse(readFileSync(join(artifacts, 'summary.json'), 'utf8'));
  assert.equal(summary.passed, 0);
  assert.equal(summary.failed, 2);
  assert.equal(summary.not_run, 2);
  assert.equal(summary.scenarios.length, 4);

  const junit = readFileSync(join(artifacts, 'junit.xml'), 'utf8');
  assert.match(junit, /tests="4" failures="2" skipped="2"/);
  assert.equal((junit.match(/<failure /g) ?? []).length, 2);
  assert.equal((junit.match(/<skipped /g) ?? []).length, 2);
  assert.match(junit, /name="03-never-started" status="not_run"/);
  assert.match(junit, /name="04-also-never-started" status="not_run"/);
});
