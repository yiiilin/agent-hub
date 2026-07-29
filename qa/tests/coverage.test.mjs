import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import {
  chmodSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync
} from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';
import {
  buildClaimRecords,
  calculateCoverage,
  CoverageValidationError,
  loadCoverageRepository,
  readCanonicalOpenApiOperations,
  readSourceIdentity,
  validateCatalog,
  validateScenarioManifest,
  writeArtifactManifest,
  writeSummary
} from '../runner.mjs';

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');

function minimalCatalog() {
  return {
    catalog_version: 1,
    layers: ['rust', 'playwright', 'qa-api', 'qa-browser'],
    domains: [{ id: 'example', name: 'Example', requires_compose: true }],
    features: [{
      id: 'EX-001',
      domain: 'example',
      title: 'Example behavior',
      required_layers: ['rust', 'qa-api', 'qa-browser'],
      operations: ['GET /api/example'],
      routes: ['/example'],
      evidence: [{ layer: 'rust', path: 'crates/example/src/lib.rs', marker: 'example_marker' }]
    }]
  };
}

function playwrightCatalog(marker) {
  const catalog = minimalCatalog();
  catalog.features[0].required_layers = ['playwright'];
  catalog.features[0].evidence = [{
    layer: 'playwright',
    path: 'frontend/tests/example.spec.ts',
    marker
  }];
  return catalog;
}

function writePlaywrightFixture(root, source, { includeTestImport = true } = {}) {
  const testImport = includeTestImport ? "import { test } from '@playwright/test';\n" : '';
  writeFileSync(join(root, 'frontend', 'tests', 'example.spec.ts'), `${testImport}${source}`);
}

function scenario(id, type, covers = ['EX-001']) {
  return { id, name: id, type, timeout_ms: 1_000, covers };
}

function fixtureRepository(t, { catalog = minimalCatalog(), scenarios = [
  scenario('01-example-api', 'api'),
  scenario('02-example-browser', 'browser')
] } = {}) {
  const root = mkdtempSync(join(tmpdir(), 'agent-hub-coverage-'));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  mkdirSync(join(root, 'qa', 'scenarios'), { recursive: true });
  mkdirSync(join(root, 'crates', 'backend', 'src'), { recursive: true });
  mkdirSync(join(root, 'crates', 'example', 'src'), { recursive: true });
  mkdirSync(join(root, 'frontend', 'src'), { recursive: true });
  mkdirSync(join(root, 'frontend', 'tests'), { recursive: true });
  writeFileSync(join(root, 'crates', 'example', 'src', 'lib.rs'), '#[test]\nfn example_marker() {}\n');
  writeFileSync(join(root, 'qa', 'features.json'), `${JSON.stringify(catalog, null, 2)}\n`);
  writeFileSync(join(root, 'crates', 'backend', 'src', 'main.rs'), `
fn openapi_document() -> Value {
    json!({
        "paths": {
            "/api/example": {
                "get": { "summary": "Example" }
            }
        }
    })
}
`);
  writeFileSync(join(root, 'frontend', 'src', 'main.tsx'), `
function parseRoute() {
  const path = window.location.pathname;
  if (path === '/example' || path === '/example/') return { name: 'example' };
  return { name: 'example' };
}
`);
  for (const manifest of scenarios) {
    const directory = join(root, 'qa', 'scenarios', manifest.id);
    mkdirSync(directory, { recursive: true });
    writeFileSync(join(directory, 'scenario.json'), `${JSON.stringify(manifest, null, 2)}\n`);
    writeFileSync(join(directory, 'scenario.mjs'), 'export default async function scenario() {}\n');
    writeFileSync(join(directory, 'README.md'), `# ${manifest.name}\n`);
  }
  return root;
}

test('catalog rejects invalid shape, duplicate IDs, unknown domain/layer, and missing marker', (t) => {
  const catalog = minimalCatalog();
  catalog.features.push({
    ...catalog.features[0],
    domain: 'unknown',
    required_layers: ['unknown-layer'],
    evidence: [{ layer: 'rust', path: 'crates/example/src/lib.rs', marker: 'missing_marker' }]
  });
  const root = fixtureRepository(t, { catalog, scenarios: [] });
  assert.throws(
    () => validateCatalog(catalog, root),
    (error) => {
      assert.ok(error instanceof CoverageValidationError);
      assert.match(error.message, /duplicate feature IDs: EX-001/);
      assert.match(error.message, /unknown domain: unknown/);
      assert.match(error.message, /unknown layer: unknown-layer/);
      assert.match(error.message, /marker must name a Rust test function/);
      return true;
    }
  );

  assert.throws(
    () => validateCatalog({ ...minimalCatalog(), features: null }, root),
    /features must be a non-empty array/
  );
});

test('catalog rejects fake Rust and Playwright evidence instead of source substrings', (t) => {
  const rustCatalog = minimalCatalog();
  const rustRoot = fixtureRepository(t, { catalog: rustCatalog });
  writeFileSync(
    join(rustRoot, 'crates', 'example', 'src', 'lib.rs'),
    'fn example_marker() {}\nconst DESCRIPTION: &str = "example_marker";\n'
  );
  assert.throws(
    () => validateCatalog(rustCatalog, rustRoot),
    /marker must name a Rust test function/
  );

  writeFileSync(join(rustRoot, 'evidence.txt'), '#[test]\nfn example_marker() {}\n');
  rustCatalog.features[0].evidence[0].path = 'evidence.txt';
  assert.throws(
    () => validateCatalog(rustCatalog, rustRoot),
    /path must match crates\/\*\*\/\*\.rs/
  );

  const playwrightCatalog = minimalCatalog();
  playwrightCatalog.features[0].required_layers = ['playwright'];
  playwrightCatalog.features[0].evidence = [{
    layer: 'playwright',
    path: 'frontend/tests/example.spec.ts',
    marker: 'fake Playwright title'
  }];
  const playwrightRoot = fixtureRepository(t, { catalog: playwrightCatalog });
  writePlaywrightFixture(
    playwrightRoot,
    "const description = 'fake Playwright title';\ntest('real Playwright title', async () => {});\n"
  );
  assert.throws(
    () => validateCatalog(playwrightCatalog, playwrightRoot),
    /marker must name a Playwright test title/
  );
});

for (const [kind, source] of [
  ['line comment', '// #[test]\n// fn example_marker() {}\n'],
  ['block comment', '/*\n#[test]\nfn example_marker() {}\n*/\n'],
  ['ordinary string', 'const FAKE: &str = "\n#[test]\nfn example_marker() {}\n";\n'],
  ['raw string', 'const FAKE: &str = r##"\n#[test]\nfn example_marker() {}\n"##;\n']
]) {
  test(`catalog rejects a Rust test declaration inside a ${kind}`, (t) => {
    const catalog = minimalCatalog();
    const root = fixtureRepository(t, { catalog });
    writeFileSync(join(root, 'crates', 'example', 'src', 'lib.rs'), source);
    assert.throws(
      () => validateCatalog(catalog, root),
      /marker must name a Rust test function/
    );
  });
}

test('Playwright evidence rejects a partial test title', (t) => {
  const marker = 'exact Playwright title';
  const partialCatalog = playwrightCatalog(marker);
  const partialRoot = fixtureRepository(t, { catalog: partialCatalog });
  writePlaywrightFixture(
    partialRoot,
    "test('exact Playwright title with a suffix', async () => {});\n"
  );
  assert.throws(
    () => validateCatalog(partialCatalog, partialRoot),
    /marker must name a Playwright test title/
  );
});

test('Playwright evidence rejects a concatenated title expression', (t) => {
  const marker = 'exact Playwright title';
  const catalog = playwrightCatalog(marker);
  const root = fixtureRepository(t, { catalog });
  writePlaywrightFixture(
    root,
    `test('${marker}' + ' suffix', async () => {});\n`
  );
  assert.throws(
    () => validateCatalog(catalog, root),
    /marker must name a Playwright test title/
  );
});

test('Playwright evidence rejects a test declaration inside nested template text', (t) => {
  const marker = 'exact Playwright title';
  const catalog = playwrightCatalog(marker);
  const root = fixtureRepository(t, { catalog });
  writePlaywrightFixture(
    root,
    [
      'const fake = `outer ${`',
      `test('${marker}', async () => {});`,
      '`} tail`;',
      ''
    ].join('\n')
  );
  assert.throws(
    () => validateCatalog(catalog, root),
    /marker must name a Playwright test title/
  );
});

test('Playwright evidence rejects a complete test declaration inside a block comment', (t) => {
  const marker = 'exact Playwright title';
  const catalog = playwrightCatalog(marker);
  const root = fixtureRepository(t, { catalog });
  writePlaywrightFixture(
    root,
    "/*\ntest('exact Playwright title', async () => {});\n*/\n"
  );
  assert.throws(
    () => validateCatalog(catalog, root),
    /marker must name a Playwright test title/
  );
});

test('Playwright evidence rejects complete test declarations inside string text', (t) => {
  const marker = 'exact Playwright title';
  const catalog = playwrightCatalog(marker);
  const root = fixtureRepository(t, { catalog });
  writePlaywrightFixture(
    root,
    "const ordinary = \"test('exact Playwright title', async () => {})\";\n"
      + "const template = `test('exact Playwright title', async () => {})`;\n"
  );
  assert.throws(
    () => validateCatalog(catalog, root),
    /marker must name a Playwright test title/
  );
});

test('Playwright evidence rejects a complete test declaration inside regex text', (t) => {
  const marker = 'exact Playwright title';
  const catalog = playwrightCatalog(marker);
  const root = fixtureRepository(t, { catalog });
  writePlaywrightFixture(
    root,
    String.raw`const fake = /test\('exact Playwright title', async \(\) => \{\}\)/;
`
  );
  assert.throws(
    () => validateCatalog(catalog, root),
    /marker must name a Playwright test title/
  );
});

for (const [kind, source] of [
  [
    'without a Playwright import',
    "const test = (...args: unknown[]) => args;\ntest('exact Playwright title', async () => {});\n"
  ],
  [
    'with a type-only import declaration',
    "import type { test } from '@playwright/test';\ntest('exact Playwright title', async () => {});\n"
  ],
  [
    'with a type-only named import',
    "import { type test } from '@playwright/test';\ntest('exact Playwright title', async () => {});\n"
  ],
  [
    'with the Playwright test import bound under another name',
    "import { test as playwrightTest } from '@playwright/test';\n"
      + "const test = (...args: unknown[]) => args;\n"
      + "test('exact Playwright title', async () => {});\n"
  ],
  [
    'with test imported from another module',
    "import { test } from './fake-playwright';\ntest('exact Playwright title', async () => {});\n"
  ]
]) {
  test(`Playwright evidence rejects a declaration ${kind}`, (t) => {
    const marker = 'exact Playwright title';
    const catalog = playwrightCatalog(marker);
    const root = fixtureRepository(t, { catalog });
    writePlaywrightFixture(root, source, { includeTestImport: false });
    assert.throws(
      () => validateCatalog(catalog, root),
      /marker must name a Playwright test title/
    );
  });
}

for (const [context, source] of [
  [
    'an uncalled function',
    "function registerNothing() {\n  test('exact Playwright title', async () => {});\n}\n"
  ],
  [
    'an if (false) branch',
    "if (false) {\n  test('exact Playwright title', async () => {});\n}\n"
  ],
  [
    'a test.skip callback',
    "test.skip('disabled parent', async () => {\n  test('exact Playwright title', async () => {});\n});\n"
  ],
  [
    'a test.fixme callback',
    "test.fixme('disabled parent', async () => {\n  test('exact Playwright title', async () => {});\n});\n"
  ],
  [
    'a test.describe callback with a shadowing const test binding',
    "test.describe('suite', () => {\n"
      + '  const test = (...args: unknown[]) => args;\n'
      + "  test('exact Playwright title', async () => {});\n"
      + '});\n'
  ],
  [
    'a test.describe callback with a shadowing test parameter',
    "test.describe('suite', (test) => {\n"
      + "  test('exact Playwright title', async () => {});\n"
      + '});\n'
  ]
]) {
  test(`Playwright evidence rejects a test declaration inside ${context}`, (t) => {
    const marker = 'exact Playwright title';
    const catalog = playwrightCatalog(marker);
    const root = fixtureRepository(t, { catalog });
    writePlaywrightFixture(root, source);
    assert.throws(
      () => validateCatalog(catalog, root),
      /marker must name a Playwright test title/
    );
  });
}

test('Playwright evidence requires a function as the second test argument', (t) => {
  const marker = 'exact Playwright title';
  const catalog = playwrightCatalog(marker);
  const root = fixtureRepository(t, { catalog });
  writePlaywrightFixture(
    root,
    `const callback = async () => {};\ntest('${marker}', callback);\n`
  );
  assert.throws(
    () => validateCatalog(catalog, root),
    /marker must name a Playwright test title/
  );
});

test('Playwright evidence rejects computed test.only declarations', (t) => {
  const marker = 'exact Playwright title';
  const catalog = playwrightCatalog(marker);
  const root = fixtureRepository(t, { catalog });
  writePlaywrightFixture(
    root,
    `test['only']('${marker}', async () => {});\n`
  );
  assert.throws(
    () => validateCatalog(catalog, root),
    /marker must name a Playwright test title/
  );
});

for (const modifier of ['skip', 'fixme']) {
  test(`Playwright evidence rejects test.${modifier}`, (t) => {
    const marker = 'exact Playwright title';
    const catalog = playwrightCatalog(marker);
    const root = fixtureRepository(t, { catalog });
    writePlaywrightFixture(
      root,
      `test.${modifier}('${marker}', async () => {});\n`
    );
    assert.throws(
      () => validateCatalog(catalog, root),
      /marker must name a Playwright test title/
    );
  });

  test(`Playwright evidence rejects tests inside test.describe.${modifier}`, (t) => {
    const marker = 'exact Playwright title';
    const catalog = playwrightCatalog(marker);
    const root = fixtureRepository(t, { catalog });
    writePlaywrightFixture(
      root,
      `test.describe.${modifier}('disabled suite', () => {\n`
        + `  test('${marker}', async () => {});\n`
        + '});\n'
    );
    assert.throws(
      () => validateCatalog(catalog, root),
      /marker must name a Playwright test title/
    );
  });

  for (const [form, invocation] of [
    ['zero arguments', `test.${modifier}()`],
    ['a literal true condition', `test.${modifier}(true, 'disabled')`],
    ['computed access and zero arguments', `test['${modifier}']()`],
    ['computed access and a literal true condition', `test['${modifier}'](true, 'disabled')`],
    [
      'concatenated computed access',
      modifier === 'skip' ? "test['sk' + 'ip']()" : "test['fix' + 'me']()"
    ]
  ]) {
    test(`Playwright evidence rejects tests with unconditional ${form} in test.${modifier}`, (t) => {
      const marker = 'exact Playwright title';
      const catalog = playwrightCatalog(marker);
      const root = fixtureRepository(t, { catalog });
      writePlaywrightFixture(
        root,
        `test('${marker}', async () => {\n  ${invocation};\n});\n`
      );
      assert.throws(
        () => validateCatalog(catalog, root),
        /marker must name a Playwright test title/
      );
    });
  }
}

test('Playwright evidence rejects a test disabled inside a nested ordinary block', (t) => {
  const marker = 'exact Playwright title';
  const catalog = playwrightCatalog(marker);
  const root = fixtureRepository(t, { catalog });
  writePlaywrightFixture(
    root,
    `test('${marker}', async () => {\n`
      + "  { test['skip'](true, 'disabled'); }\n"
      + '});\n'
  );
  assert.throws(
    () => validateCatalog(catalog, root),
    /marker must name a Playwright test title/
  );
});

for (const modifier of ['skip', 'fixme']) {
  test(`Playwright evidence rejects an expression-bodied test with test.${modifier}()`, (t) => {
    const marker = 'exact Playwright title';
    const catalog = playwrightCatalog(marker);
    const root = fixtureRepository(t, { catalog });
    writePlaywrightFixture(
      root,
      `test('${marker}', async () => test.${modifier}());\n`
    );
    assert.throws(
      () => validateCatalog(catalog, root),
      /marker must name a Playwright test title/
    );
  });
}

test('catalog accepts real Rust tests and executable Playwright test titles', (t) => {
  const rustCatalog = minimalCatalog();
  const rustRoot = fixtureRepository(t, { catalog: rustCatalog });
  writeFileSync(
    join(rustRoot, 'crates', 'example', 'src', 'lib.rs'),
    '#[test]\nfn example_marker() {}\n'
  );
  assert.doesNotThrow(() => validateCatalog(rustCatalog, rustRoot));

  const marker = 'exact executable title';
  for (const [kind, source] of [
    ['test', `test('${marker}', async () => {});\n`],
    ['test.only', `test.only('${marker}', async () => {});\n`],
    ['static template title and function expression', `test(\`${marker}\`, function () {});\n`],
    [
      'test.describe callback',
      `test.describe('suite', () => {\n  test('${marker}', async () => {});\n});\n`
    ],
    [
      'nested test.describe.only callback',
      `test.describe('suite', () => {\n`
        + `  test.describe.only('focused suite', function () {\n`
        + `    test.only('${marker}', function () {});\n`
        + '  });\n'
        + '});\n'
    ]
  ]) {
    const catalog = playwrightCatalog(marker);
    const root = fixtureRepository(t, { catalog });
    writePlaywrightFixture(root, source);
    assert.doesNotThrow(() => validateCatalog(catalog, root), kind);
  }
});

test('qa-api and qa-browser evidence must come from scenario manifests', (t) => {
  const catalog = minimalCatalog();
  catalog.features[0].evidence = [{
    layer: 'qa-api',
    path: 'crates/example/src/lib.rs',
    marker: 'example_marker'
  }];
  const root = fixtureRepository(t, { catalog });
  assert.throws(
    () => validateCatalog(catalog, root),
    /qa-api and qa-browser evidence comes from scenario manifests/
  );
});

test('manifest covers must be non-empty and contain no duplicate IDs', (t) => {
  const root = fixtureRepository(t);
  const catalog = validateCatalog(minimalCatalog(), root);
  assert.throws(
    () => validateScenarioManifest(scenario('invalid', 'api', []), 'invalid', catalog),
    /covers must not be empty/
  );
  assert.throws(
    () => validateScenarioManifest(scenario('invalid', 'api', ['EX-001', 'EX-001']), 'invalid', catalog),
    /covers contains duplicates: EX-001/
  );
});

test('manifest rejects unknown feature IDs', (t) => {
  const root = fixtureRepository(t);
  const catalog = validateCatalog(minimalCatalog(), root);
  assert.throws(
    () => validateScenarioManifest(scenario('invalid', 'api', ['UNKNOWN-001']), 'invalid', catalog),
    /references unknown feature ID: UNKNOWN-001/
  );
});

test('coverage reports a missing required layer', (t) => {
  const root = fixtureRepository(t);
  const catalog = validateCatalog(minimalCatalog(), root);
  const report = calculateCoverage(
    catalog,
    [scenario('01-example-api', 'api')],
    ['GET /api/example'],
    ['/example']
  );
  assert.equal(report.complete, false);
  assert.deepEqual(report.gaps.missing_required_layers, [{
    feature_id: 'EX-001',
    domain: 'example',
    missing_layers: ['qa-browser']
  }]);
});

test('coverage reports unmapped OpenAPI operations and UI routes', (t) => {
  const root = fixtureRepository(t);
  const catalog = validateCatalog(minimalCatalog(), root);
  const report = calculateCoverage(
    catalog,
    [scenario('01-example-api', 'api'), scenario('02-example-browser', 'browser')],
    ['GET /api/example', 'POST /api/unmapped'],
    ['/example', '/unmapped']
  );
  assert.deepEqual(report.gaps.unmapped_openapi_operations, ['POST /api/unmapped']);
  assert.deepEqual(report.gaps.unmapped_ui_routes, ['/unmapped']);
  assert.equal(report.complete, false);
});

test('coverage reports features without evidence and uncovered Compose domains', (t) => {
  const catalog = minimalCatalog();
  catalog.features[0].evidence = [];
  const root = fixtureRepository(t, { catalog, scenarios: [] });
  validateCatalog(catalog, root);
  const report = calculateCoverage(catalog, [], ['GET /api/example'], ['/example']);
  assert.deepEqual(report.gaps.features_without_evidence, ['EX-001']);
  assert.deepEqual(report.gaps.uncovered_compose_domains, ['example']);
  assert.equal(report.complete, false);
});

test('valid minimal repository fixture reaches complete coverage', (t) => {
  const root = fixtureRepository(t);
  const repository = loadCoverageRepository(root, join(root, 'qa'));
  const report = calculateCoverage(
    repository.catalog,
    repository.scenarios,
    repository.canonicalOperations,
    repository.canonicalRoutes
  );
  assert.equal(report.complete, true);
  assert.equal(report.fully_covered_feature_count, 1);
  assert.deepEqual(report.gaps, {
    features_without_evidence: [],
    missing_required_layers: [],
    uncovered_compose_domains: [],
    unmapped_openapi_operations: [],
    unmapped_ui_routes: []
  });
});

test('scenario directories require README.md alongside manifest and entrypoint', (t) => {
  const root = fixtureRepository(t);
  rmSync(join(root, 'qa', 'scenarios', '01-example-api', 'README.md'));
  assert.throws(
    () => loadCoverageRepository(root, join(root, 'qa')),
    /qa\/scenarios\/01-example-api\/README\.md does not exist/
  );
});

test('all OpenAPI methods are discovered while path metadata is excluded', (t) => {
  const root = fixtureRepository(t);
  writeFileSync(join(root, 'crates', 'backend', 'src', 'main.rs'), `
fn openapi_document() -> Value {
    json!({
        "paths": {
            "/api/example": {
                "summary": "Example path",
                "parameters": [],
                "get": {},
                "put": {},
                "post": {},
                "delete": {},
                "options": {},
                "head": {},
                "patch": {},
                "trace": {}
            }
        }
    })
}
`);
  const operations = readCanonicalOpenApiOperations(root);
  assert.deepEqual(operations, [
    'DELETE /api/example',
    'GET /api/example',
    'HEAD /api/example',
    'OPTIONS /api/example',
    'PATCH /api/example',
    'POST /api/example',
    'PUT /api/example',
    'TRACE /api/example'
  ]);

  const catalog = validateCatalog(minimalCatalog(), root);
  const report = calculateCoverage(
    catalog,
    [scenario('01-example-api', 'api'), scenario('02-example-browser', 'browser')],
    operations,
    ['/example']
  );
  assert.ok(report.gaps.unmapped_openapi_operations.includes('HEAD /api/example'));
  assert.ok(report.gaps.unmapped_openapi_operations.includes('OPTIONS /api/example'));
  assert.ok(report.gaps.unmapped_openapi_operations.includes('TRACE /api/example'));
  assert.equal(report.gaps.unmapped_openapi_operations.some((operation) => operation.includes('summary')), false);
  assert.equal(report.gaps.unmapped_openapi_operations.some((operation) => operation.includes('parameters')), false);
});

test('summary JSON and JUnit expose selected and overall coverage with gaps', async (t) => {
  const root = fixtureRepository(t);
  const artifacts = join(root, 'artifacts');
  mkdirSync(artifacts);
  const catalog = validateCatalog(minimalCatalog(), root);
  const selected = calculateCoverage(catalog, [scenario('01-example-api', 'api')], ['GET /api/example'], ['/example']);
  const overall = calculateCoverage(
    catalog,
    [scenario('01-example-api', 'api'), scenario('02-example-browser', 'browser')],
    ['GET /api/example'],
    ['/example']
  );
  await writeSummary(artifacts, 'fixture', 'http://127.0.0.1:1', [], { selected, overall });

  const summary = JSON.parse(readFileSync(join(artifacts, 'summary.json'), 'utf8'));
  assert.deepEqual(summary.coverage.selected.gaps.missing_required_layers[0].missing_layers, ['qa-browser']);
  assert.equal(summary.coverage.overall.complete, true);
  const junit = readFileSync(join(artifacts, 'junit.xml'), 'utf8');
  assert.match(junit, /name="coverage\.selected"/);
  assert.match(junit, /name="coverage\.overall"/);
  assert.match(junit, /&quot;missing_required_layers&quot;/);
});

test('claim records keep each executed scenario claim independently auditable', () => {
  const catalog = minimalCatalog();
  const records = buildClaimRecords(catalog, [{
    id: '02-example-browser',
    name: 'Browser claim',
    type: 'browser',
    covers: ['EX-001'],
    status: 'passed',
    duration_ms: 25,
    started_at: '2026-07-29T00:00:00.000Z',
    finished_at: '2026-07-29T00:00:00.025Z'
  }], {
    revision: 'a'.repeat(40),
    sourceFingerprint: 'b'.repeat(64),
    environment: {
      id: 'agent-hub-qa-fixture',
      class: 'owned_ephemeral',
      base_url: 'http://127.0.0.1:1234'
    }
  });

  assert.equal(records.length, 1);
  assert.deepEqual(records[0], {
    claim_id: 'EX-001',
    title: 'Example behavior',
    scenario_id: '02-example-browser',
    status: 'passed',
    contract_source: 'qa/features.json#EX-001',
    revision: 'a'.repeat(40),
    source_fingerprint: 'b'.repeat(64),
    environment: {
      id: 'agent-hub-qa-fixture',
      class: 'owned_ephemeral',
      base_url: 'http://127.0.0.1:1234'
    },
    action: 'qa/scenarios/02-example-browser/scenario.mjs',
    oracle: 'Example behavior',
    result: 'passed in 25 ms',
    observation: 'Scenario completed with every assertion satisfied.',
    artifacts: ['junit.xml'],
    verified_at: '2026-07-29T00:00:00.025Z',
    freshness: 'Invalidated by relevant source, working-tree fingerprint, Compose configuration, dependency mode, fixture, actor permission, or scenario oracle changes.'
  });
});

test('artifact manifest hashes retained evidence and excludes recursive report files', async (t) => {
  const root = fixtureRepository(t);
  const artifacts = join(root, 'artifacts');
  mkdirSync(join(artifacts, 'scenario'), { recursive: true });
  writeFileSync(join(artifacts, 'scenario', 'observation.json'), '{"ok":true}\n');
  writeFileSync(join(artifacts, 'summary.json'), '{}\n');

  const manifest = await writeArtifactManifest(artifacts, '2026-07-29T00:00:00.000Z');
  assert.equal(manifest.algorithm, 'sha256');
  assert.deepEqual(manifest.excluded, ['artifact-manifest.json', 'summary.json']);
  assert.deepEqual(manifest.entries.map((entry) => entry.path), ['scenario/observation.json']);
  assert.equal(manifest.entries[0].bytes, 12);
  assert.match(manifest.entries[0].sha256, /^[a-f0-9]{64}$/);
  assert.deepEqual(
    JSON.parse(readFileSync(join(artifacts, 'artifact-manifest.json'), 'utf8')),
    manifest
  );
});

test('summary records source, environment, dependency, gate, claim, and artifact evidence', async (t) => {
  const root = fixtureRepository(t);
  const artifacts = join(root, 'artifacts');
  mkdirSync(artifacts);
  const coverage = { selected: { complete: true }, overall: { complete: true } };
  await writeSummary(artifacts, 'fixture', 'http://127.0.0.1:1', [], coverage, {
    run_id: 'run-fixture',
    started_at: '2026-07-29T00:00:00.000Z',
    finished_at: '2026-07-29T00:00:01.000Z',
    source: {
      revision: 'a'.repeat(40),
      dirty: true,
      working_tree_fingerprint: 'b'.repeat(64),
      stable_during_run: true
    },
    environment: { id: 'fixture', class: 'owned_ephemeral', disposition: 'removed' },
    dependencies: [{ name: 'model-provider', mode: 'emulated' }],
    quality_gates: [{ id: 'sdk-test', status: 'passed', duration_ms: 10 }],
    claims: [{ claim_id: 'EX-001', status: 'passed' }],
    artifacts: {
      manifest: 'artifact-manifest.json',
      artifact_safety_scan: { status: 'passed', scanner: 'qa/support/secrets.mjs' }
    }
  });

  const summary = JSON.parse(readFileSync(join(artifacts, 'summary.json'), 'utf8'));
  assert.equal(summary.run_id, 'run-fixture');
  assert.equal(summary.source.stable_during_run, true);
  assert.equal(summary.environment.disposition, 'removed');
  assert.equal(summary.dependencies[0].mode, 'emulated');
  assert.equal(summary.quality_gates[0].id, 'sdk-test');
  assert.equal(summary.claims[0].claim_id, 'EX-001');
  assert.equal(summary.artifacts.artifact_safety_scan.status, 'passed');
  assert.deepEqual(summary.counts, {
    scenarios: { passed: 0, failed: 0, blocked: 0, not_run: 0, unverified: 0 },
    quality_gates: { passed: 1, failed: 0 }
  });
  const junit = readFileSync(join(artifacts, 'junit.xml'), 'utf8');
  assert.match(junit, /classname="agent-hub\.qa\.gate" name="sdk-test"/);
});

test('source fingerprint detects subsequent changes to a dirty gateway file', (t) => {
  const root = mkdtempSync(join(tmpdir(), 'agent-hub-source-fingerprint-'));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  mkdirSync(join(root, 'gateway'), { recursive: true });
  writeFileSync(join(root, 'gateway', 'server.go'), 'package gateway\n\nconst version = "base"\n');

  for (const args of [
    ['init'],
    ['config', 'user.email', 'qa@example.invalid'],
    ['config', 'user.name', 'Agent Hub QA'],
    ['add', 'gateway/server.go'],
    ['commit', '-m', 'fixture']
  ]) {
    const result = spawnSync('git', args, { cwd: root, encoding: 'utf8' });
    assert.equal(result.status, 0, result.stderr || result.stdout);
  }

  writeFileSync(join(root, 'gateway', 'server.go'), 'package gateway\n\nconst version = "dirty-one"\n');
  const first = readSourceIdentity(root);
  writeFileSync(join(root, 'gateway', 'server.go'), 'package gateway\n\nconst version = "dirty-two"\n');
  const second = readSourceIdentity(root);

  assert.equal(first.dirty, true);
  assert.equal(second.dirty, true);
  assert.notEqual(first.working_tree_fingerprint, second.working_tree_fingerprint);
});

test('--coverage reports every current feature without invoking Docker', (t) => {
  const root = mkdtempSync(join(tmpdir(), 'agent-hub-coverage-command-'));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const bin = join(root, 'bin');
  const sentinel = join(root, 'docker-was-called');
  mkdirSync(bin);
  const docker = join(bin, 'docker');
  writeFileSync(docker, `#!/usr/bin/env bash\nprintf called > "${sentinel}"\nexit 97\n`);
  chmodSync(docker, 0o755);
  const result = spawnSync('./qa/run-all.sh', ['--coverage'], {
    cwd: repoRoot,
    env: { ...process.env, PATH: `${bin}:${process.env.PATH}` },
    encoding: 'utf8'
  });
  assert.equal(result.status, 0, result.stderr || result.stdout);
  assert.equal(readFileSync(join(repoRoot, 'qa', 'runner.mjs'), 'utf8').includes("./support/browser.mjs"), false);
  const overall = result.stdout.match(/Overall repository coverage: (\d+)\/(\d+) fully covered/);
  assert.ok(overall, result.stdout);
  assert.equal(Number(overall[1]), 81);
  assert.equal(Number(overall[2]), 81);
  assert.match(result.stdout, /Coverage validation passed\./);
  assert.equal(result.stdout.includes('Unmapped OpenAPI operations'), false);
  assert.equal(result.stdout.includes('Unmapped UI routes'), false);
  assert.equal(readFileSync(docker, 'utf8').includes('docker-was-called'), true);
  assert.equal(result.error, undefined);
  assert.equal(result.signal, null);
  assert.equal(readFileSync(join(repoRoot, 'qa', 'features.json'), 'utf8').length > 0, true);
  assert.equal(result.stderr, '');
  assert.equal(exists(sentinel), false);
});

function exists(path) {
  try {
    readFileSync(path);
    return true;
  } catch {
    return false;
  }
}
