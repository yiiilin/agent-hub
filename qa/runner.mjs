import { spawnSync } from 'node:child_process';
import { existsSync, readFileSync, readdirSync } from 'node:fs';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { dirname, join, relative, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import { ComposeHarness } from './support/compose.mjs';
import { poll } from './support/api.mjs';
import {
  assertArtifactTreeSafe,
  redactSecrets,
  sanitizeArtifactTree
} from './support/secrets.mjs';

const qaRoot = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(qaRoot, '..');
const workerPath = join(qaRoot, 'scenario-worker.mjs');
const frontendRequire = createRequire(join(repoRoot, 'frontend', 'package.json'));
const ts = frontendRequire('typescript');
const OPENAPI_METHODS = new Set([
  'get',
  'put',
  'post',
  'delete',
  'options',
  'head',
  'patch',
  'trace'
]);

export class CoverageValidationError extends Error {
  constructor(errors) {
    super(`Coverage validation failed:\n${errors.map((error) => `- ${error}`).join('\n')}`);
    this.name = 'CoverageValidationError';
    this.errors = errors;
  }
}

function usage() {
  console.log(`Usage: ./qa/run-all.sh [options] [scenario ...]

Options:
  --type api|browser  Run only one scenario type.
  --list              List discovered scenarios without starting Compose.
  --coverage          Validate and report repository coverage without Compose.
  --keep-env          Keep the isolated QA Compose environment after the run.
  --help              Show this help.`);
}

function parseArgs(argv) {
  const options = { type: null, list: false, coverage: false, keepEnv: false, scenarios: [] };
  for (let index = 0; index < argv.length; index += 1) {
    const value = argv[index];
    if (value === '--help') return { ...options, help: true };
    if (value === '--list') options.list = true;
    else if (value === '--coverage') options.coverage = true;
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

function isObject(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function duplicateValues(values) {
  const seen = new Set();
  const duplicates = new Set();
  for (const value of values) {
    if (seen.has(value)) duplicates.add(value);
    seen.add(value);
  }
  return [...duplicates];
}

function validateStringArray(value, label, errors, { nonEmpty = false } = {}) {
  if (!Array.isArray(value)) {
    errors.push(`${label} must be an array`);
    return [];
  }
  if (nonEmpty && value.length === 0) errors.push(`${label} must not be empty`);
  if (value.some((entry) => typeof entry !== 'string' || entry.trim() === '')) {
    errors.push(`${label} must contain only non-empty strings`);
    return value.filter((entry) => typeof entry === 'string' && entry.trim() !== '');
  }
  const duplicates = duplicateValues(value);
  if (duplicates.length > 0) errors.push(`${label} contains duplicates: ${duplicates.join(', ')}`);
  return value;
}

function validateNonEmptyString(value, label, errors) {
  if (typeof value !== 'string' || value.trim() === '') {
    errors.push(`${label} must be a non-empty string`);
    return false;
  }
  return true;
}

function safeRepositoryPath(root, path, label, errors) {
  if (!validateNonEmptyString(path, label, errors)) return null;
  const absolute = resolve(root, path);
  const fromRoot = relative(root, absolute);
  if (fromRoot === '..' || fromRoot.startsWith(`..${sep}`)) {
    errors.push(`${label} must stay inside the repository: ${path}`);
    return null;
  }
  return absolute;
}

function escapeRegExp(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function maskCharacter(masked, index) {
  if (!['\r', '\n'].includes(masked[index])) masked[index] = ' ';
}

function rustCharLiteralEnd(source, start) {
  let index = start + 1;
  if (index >= source.length || ['\r', '\n'].includes(source[index])) return -1;
  if (source[index] === '\\') {
    index += 1;
    if (source[index] === 'u' && source[index + 1] === '{') {
      const close = source.indexOf('}', index + 2);
      if (close < 0) return -1;
      index = close + 1;
    } else if (source[index] === 'x') {
      index += 3;
    } else {
      index += 1;
    }
  } else {
    index += source.codePointAt(index) > 0xffff ? 2 : 1;
  }
  return source[index] === '\'' ? index : -1;
}

function maskRustNonCode(source) {
  const masked = source.split('');
  let index = 0;
  while (index < source.length) {
    if (source.startsWith('//', index)) {
      while (index < source.length && source[index] !== '\n') {
        maskCharacter(masked, index);
        index += 1;
      }
      continue;
    }
    if (source.startsWith('/*', index)) {
      let depth = 0;
      while (index < source.length) {
        if (source.startsWith('/*', index)) {
          depth += 1;
          maskCharacter(masked, index);
          maskCharacter(masked, index + 1);
          index += 2;
        } else if (source.startsWith('*/', index)) {
          depth -= 1;
          maskCharacter(masked, index);
          maskCharacter(masked, index + 1);
          index += 2;
          if (depth === 0) break;
        } else {
          maskCharacter(masked, index);
          index += 1;
        }
      }
      continue;
    }
    if (source[index] === 'r') {
      let quote = index + 1;
      while (source[quote] === '#') quote += 1;
      if (source[quote] === '"') {
        const terminator = `"${'#'.repeat(quote - index - 1)}`;
        const close = source.indexOf(terminator, quote + 1);
        const end = close < 0 ? source.length : close + terminator.length;
        while (index < end) {
          maskCharacter(masked, index);
          index += 1;
        }
        continue;
      }
    }
    if (source[index] === '"') {
      maskCharacter(masked, index);
      index += 1;
      while (index < source.length) {
        if (source[index] === '\\') {
          maskCharacter(masked, index);
          index += 1;
          if (index < source.length) maskCharacter(masked, index);
        } else if (source[index] === '"') {
          maskCharacter(masked, index);
          index += 1;
          break;
        } else {
          maskCharacter(masked, index);
        }
        index += 1;
      }
      continue;
    }
    if (source[index] === '\'') {
      const end = rustCharLiteralEnd(source, index);
      if (end >= 0) {
        while (index <= end) {
          maskCharacter(masked, index);
          index += 1;
        }
        continue;
      }
    }
    index += 1;
  }
  return masked.join('');
}

function hasRustTestFunction(source, marker) {
  const testAttribute = String.raw`^[ \t]*#\[[ \t]*(?:(?:sqlx|tokio)::)?test(?:[ \t]*\([^\r\n]*\))?[ \t]*\][\s]*`;
  const followingAttributes = String.raw`(?:^[ \t]*#\[[^\]\r\n]*\][\s]*)*`;
  const declaration = String.raw`^[ \t]*(?:pub(?:\([^\r\n)]*\))?[ \t]+)?(?:async[ \t]+)?fn[ \t]+${escapeRegExp(marker)}\b`;
  return new RegExp(`${testAttribute}${followingAttributes}${declaration}`, 'm')
    .test(maskRustNonCode(source));
}

function playwrightCallPath(call, checker, importedTestSymbol) {
  if (call.questionDotToken) return null;
  const path = [];
  let expression = call.expression;
  while (ts.isPropertyAccessExpression(expression)) {
    if (expression.questionDotToken) return null;
    path.unshift(expression.name.text);
    expression = expression.expression;
  }
  if (!ts.isIdentifier(expression)) return null;
  if (checker.getSymbolAtLocation(expression) !== importedTestSymbol) return null;
  path.unshift(expression.text);
  return path.join('.');
}

function isInlineFunction(node) {
  return node !== undefined && (ts.isArrowFunction(node) || ts.isFunctionExpression(node));
}

function staticStringLiteral(node) {
  if (ts.isStringLiteral(node) || ts.isNoSubstitutionTemplateLiteral(node)) return node.text;
  return null;
}

function computedPropertyName(node) {
  if (ts.isParenthesizedExpression(node)) return computedPropertyName(node.expression);
  if (ts.isStringLiteral(node) || ts.isNoSubstitutionTemplateLiteral(node)) return node.text;
  if (ts.isBinaryExpression(node) && node.operatorToken.kind === ts.SyntaxKind.PlusToken) {
    const left = computedPropertyName(node.left);
    const right = computedPropertyName(node.right);
    return left === null || right === null ? null : left + right;
  }
  return null;
}

function isPlaywrightDisableCall(call) {
  let callee = call.expression;
  while (ts.isParenthesizedExpression(callee)) callee = callee.expression;
  if (ts.isPropertyAccessExpression(callee)
    && ts.isIdentifier(callee.expression)
    && callee.expression.text === 'test') {
    return ['skip', 'fixme'].includes(callee.name.text);
  }
  if (!ts.isElementAccessExpression(callee)
    || !ts.isIdentifier(callee.expression)
    || callee.expression.text !== 'test') return false;
  const name = computedPropertyName(callee.argumentExpression);
  return name === null || ['skip', 'fixme'].includes(name);
}

function hasUnconditionalPlaywrightDisable(callback) {
  let disabled = false;
  function visit(node) {
    if (disabled) return;
    if (ts.isCallExpression(node)
      && isPlaywrightDisableCall(node)
      && (node.arguments.length === 0 || node.arguments[0].kind === ts.SyntaxKind.TrueKeyword)) {
      disabled = true;
      return;
    }
    ts.forEachChild(node, visit);
  }
  visit(callback.body);
  return disabled;
}

function isExecutablePlaywrightTest(call, marker, path) {
  if (!['test', 'test.only'].includes(path)) return false;
  const title = call.arguments[0];
  const callback = call.arguments[1];
  return title !== undefined
    && staticStringLiteral(title) === marker
    && isInlineFunction(callback)
    && !hasUnconditionalPlaywrightDisable(callback);
}

function executableDescribeStatements(call, path) {
  if (!['test.describe', 'test.describe.only'].includes(path)) return null;
  const callback = call.arguments[1];
  if (!isInlineFunction(callback) || !ts.isBlock(callback.body)) return null;
  return callback.body.statements;
}

function hasPlaywrightTestStatement(statements, marker, checker, importedTestSymbol) {
  for (const statement of statements) {
    if (!ts.isExpressionStatement(statement) || !ts.isCallExpression(statement.expression)) continue;
    const call = statement.expression;
    const path = playwrightCallPath(call, checker, importedTestSymbol);
    if (path === null) continue;
    if (isExecutablePlaywrightTest(call, marker, path)) return true;
    const describeStatements = executableDescribeStatements(call, path);
    if (describeStatements
      && hasPlaywrightTestStatement(describeStatements, marker, checker, importedTestSymbol)) return true;
  }
  return false;
}

function playwrightTestImportSpecifier(sourceFile) {
  for (const statement of sourceFile.statements) {
    if (!ts.isImportDeclaration(statement)
      || !ts.isStringLiteral(statement.moduleSpecifier)
      || statement.moduleSpecifier.text !== '@playwright/test') continue;
    const importClause = statement.importClause;
    if (!importClause
      || importClause.isTypeOnly
      || !importClause.namedBindings
      || !ts.isNamedImports(importClause.namedBindings)) continue;
    const specifier = importClause.namedBindings.elements.find((candidate) => {
      const importedName = candidate.propertyName ?? candidate.name;
      return !candidate.isTypeOnly
        && ts.isIdentifier(importedName)
        && importedName.text === 'test'
        && candidate.name.text === 'test';
    });
    if (specifier) return specifier;
  }
  return null;
}

function singleFileTypeChecker(sourceFile, source) {
  const fileName = sourceFile.fileName;
  const host = {
    fileExists: (path) => path === fileName,
    readFile: (path) => path === fileName ? source : undefined,
    getSourceFile: (path) => path === fileName ? sourceFile : undefined,
    getDefaultLibFileName: () => '/lib.d.ts',
    writeFile: () => {},
    getCurrentDirectory: () => '/',
    getDirectories: () => [],
    getCanonicalFileName: (path) => path,
    useCaseSensitiveFileNames: () => true,
    getNewLine: () => '\n'
  };
  return ts.createProgram(
    [fileName],
    { noLib: true, noResolve: true, target: ts.ScriptTarget.Latest },
    host
  ).getTypeChecker();
}

function importedPlaywrightTestSymbol(sourceFile, checker, importSpecifier) {
  const symbol = checker.getSymbolAtLocation(importSpecifier.name);
  if (!symbol) return null;
  const declarationMatches = symbol.declarations?.some((declaration) => {
    if (!ts.isImportSpecifier(declaration)) return false;
    const importedName = declaration.propertyName ?? declaration.name;
    return !declaration.isTypeOnly
      && ts.isIdentifier(importedName)
      && importedName.text === 'test'
      && declaration.name.text === 'test'
      && declaration.parent.parent.parent.moduleSpecifier.text === '@playwright/test';
  });
  return declarationMatches ? symbol : null;
}

function hasPlaywrightTestTitle(source, marker) {
  const sourceFile = ts.createSourceFile(
    '/evidence.spec.ts',
    source,
    ts.ScriptTarget.Latest,
    true,
    ts.ScriptKind.TS
  );
  if (sourceFile.parseDiagnostics.length > 0) return false;
  const importSpecifier = playwrightTestImportSpecifier(sourceFile);
  if (!importSpecifier) return false;
  const checker = singleFileTypeChecker(sourceFile, source);
  const importedTestSymbol = importedPlaywrightTestSymbol(sourceFile, checker, importSpecifier);
  return importedTestSymbol !== null
    && hasPlaywrightTestStatement(sourceFile.statements, marker, checker, importedTestSymbol);
}

export function validateCatalog(catalog, root = repoRoot) {
  const errors = [];
  if (!isObject(catalog)) throw new CoverageValidationError(['qa/features.json must contain an object']);
  if (!Number.isInteger(catalog.catalog_version) || catalog.catalog_version < 1) {
    errors.push('catalog_version must be a positive integer');
  }

  const layers = validateStringArray(catalog.layers, 'layers', errors, { nonEmpty: true });
  const layerIds = new Set(layers);
  if (!Array.isArray(catalog.domains) || catalog.domains.length === 0) {
    errors.push('domains must be a non-empty array');
  }
  const domains = Array.isArray(catalog.domains) ? catalog.domains : [];
  const domainIds = [];
  for (const [index, domain] of domains.entries()) {
    const label = `domains[${index}]`;
    if (!isObject(domain)) {
      errors.push(`${label} must be an object`);
      continue;
    }
    if (validateNonEmptyString(domain.id, `${label}.id`, errors)) domainIds.push(domain.id);
    validateNonEmptyString(domain.name, `${label}.name`, errors);
    if (typeof domain.requires_compose !== 'boolean') {
      errors.push(`${label}.requires_compose must be a boolean`);
    }
  }
  const duplicateDomains = duplicateValues(domainIds);
  if (duplicateDomains.length > 0) errors.push(`duplicate domain IDs: ${duplicateDomains.join(', ')}`);
  const knownDomains = new Set(domainIds);

  if (!Array.isArray(catalog.features) || catalog.features.length === 0) {
    errors.push('features must be a non-empty array');
  }
  const features = Array.isArray(catalog.features) ? catalog.features : [];
  const featureIds = [];
  for (const [index, feature] of features.entries()) {
    const label = `features[${index}]`;
    if (!isObject(feature)) {
      errors.push(`${label} must be an object`);
      continue;
    }
    if (validateNonEmptyString(feature.id, `${label}.id`, errors)) featureIds.push(feature.id);
    validateNonEmptyString(feature.title, `${label}.title`, errors);
    if (!validateNonEmptyString(feature.domain, `${label}.domain`, errors)
      || !knownDomains.has(feature.domain)) {
      if (typeof feature.domain === 'string' && feature.domain.trim() !== '') {
        errors.push(`${label}.domain references unknown domain: ${feature.domain}`);
      }
    }
    const requiredLayers = validateStringArray(
      feature.required_layers,
      `${label}.required_layers`,
      errors,
      { nonEmpty: true }
    );
    for (const layer of requiredLayers) {
      if (!layerIds.has(layer)) errors.push(`${label}.required_layers references unknown layer: ${layer}`);
    }
    const operations = validateStringArray(feature.operations, `${label}.operations`, errors);
    for (const operation of operations) {
      if (!/^(GET|PUT|POST|DELETE|OPTIONS|HEAD|PATCH|TRACE) \/\S+$/.test(operation)) {
        errors.push(`${label}.operations has invalid operation: ${operation}`);
      }
    }
    const routes = validateStringArray(feature.routes, `${label}.routes`, errors);
    for (const route of routes) {
      if (!route.startsWith('/')) errors.push(`${label}.routes has invalid route: ${route}`);
    }
    if (!Array.isArray(feature.evidence)) {
      errors.push(`${label}.evidence must be an array`);
      continue;
    }
    for (const [evidenceIndex, evidence] of feature.evidence.entries()) {
      const evidenceLabel = `${label}.evidence[${evidenceIndex}]`;
      if (!isObject(evidence)) {
        errors.push(`${evidenceLabel} must be an object`);
        continue;
      }
      if (!validateNonEmptyString(evidence.layer, `${evidenceLabel}.layer`, errors)
        || !layerIds.has(evidence.layer)) {
        if (typeof evidence.layer === 'string' && evidence.layer.trim() !== '') {
          errors.push(`${evidenceLabel}.layer references unknown layer: ${evidence.layer}`);
        }
      }
      if (layerIds.has(evidence.layer) && !['rust', 'playwright'].includes(evidence.layer)) {
        errors.push(`${evidenceLabel}.layer must be rust or playwright; qa-api and qa-browser evidence comes from scenario manifests`);
        continue;
      }
      const evidencePath = safeRepositoryPath(root, evidence.path, `${evidenceLabel}.path`, errors);
      const hasMarker = validateNonEmptyString(evidence.marker, `${evidenceLabel}.marker`, errors);
      if (!evidencePath) continue;
      if (!existsSync(evidencePath)) {
        errors.push(`${evidenceLabel}.path does not exist: ${evidence.path}`);
        continue;
      }
      const repositoryPath = relative(root, evidencePath).split(sep).join('/');
      const source = readFileSync(evidencePath, 'utf8');
      if (evidence.layer === 'rust') {
        if (!/^crates\/.+\.rs$/.test(repositoryPath)) {
          errors.push(`${evidenceLabel}.path must match crates/**/*.rs for rust evidence: ${evidence.path}`);
        } else if (hasMarker && !hasRustTestFunction(source, evidence.marker)) {
          errors.push(`${evidenceLabel}.marker must name a Rust test function in ${evidence.path}: ${evidence.marker}`);
        }
      } else if (evidence.layer === 'playwright') {
        if (!/^frontend\/tests\/[^/]+\.spec\.ts$/.test(repositoryPath)) {
          errors.push(`${evidenceLabel}.path must match frontend/tests/*.spec.ts for playwright evidence: ${evidence.path}`);
        } else if (hasMarker && !hasPlaywrightTestTitle(source, evidence.marker)) {
          errors.push(`${evidenceLabel}.marker must name a Playwright test title in ${evidence.path}: ${evidence.marker}`);
        }
      }
    }
  }
  const duplicateFeatures = duplicateValues(featureIds);
  if (duplicateFeatures.length > 0) errors.push(`duplicate feature IDs: ${duplicateFeatures.join(', ')}`);
  if (errors.length > 0) throw new CoverageValidationError(errors);
  return catalog;
}

export function validateScenarioManifest(manifest, id, catalog) {
  const errors = [];
  const label = `${id}/scenario.json`;
  if (!isObject(manifest)) throw new CoverageValidationError([`${label} must contain an object`]);
  validateNonEmptyString(manifest.name, `${label}.name`, errors);
  if (!['api', 'browser'].includes(manifest.type)) {
    errors.push(`${label}.type must be api or browser`);
  }
  const timeoutMs = Number(manifest.timeout_ms ?? 90_000);
  if (!Number.isInteger(timeoutMs) || timeoutMs < 1_000 || timeoutMs > 10 * 60_000) {
    errors.push(`${label}.timeout_ms must be an integer from 1000 to 600000`);
  }
  const covers = validateStringArray(manifest.covers, `${label}.covers`, errors, { nonEmpty: true });
  const knownFeatures = new Set(catalog.features.map((feature) => feature.id));
  for (const featureId of covers) {
    if (!knownFeatures.has(featureId)) {
      errors.push(`${label}.covers references unknown feature ID: ${featureId}`);
    }
  }
  const scenarioLayer = manifest.type === 'api' ? 'qa-api' : manifest.type === 'browser' ? 'qa-browser' : null;
  if (scenarioLayer && !catalog.layers.includes(scenarioLayer)) {
    errors.push(`${label}.type requires catalog layer ${scenarioLayer}`);
  }
  if (errors.length > 0) throw new CoverageValidationError(errors);
  return { ...manifest, timeout_ms: timeoutMs };
}

function matchingDelimiter(source, start, open = '{', close = '}') {
  let depth = 0;
  let inString = false;
  let escaped = false;
  for (let index = start; index < source.length; index += 1) {
    const character = source[index];
    if (inString) {
      if (escaped) escaped = false;
      else if (character === '\\') escaped = true;
      else if (character === '"') inString = false;
      continue;
    }
    if (character === '"') {
      inString = true;
      continue;
    }
    if (character === open) depth += 1;
    else if (character === close) {
      depth -= 1;
      if (depth === 0) return index;
    }
  }
  throw new CoverageValidationError([`could not find closing ${close} for canonical source block`]);
}

function parseQuotedString(source, start) {
  let escaped = false;
  for (let index = start + 1; index < source.length; index += 1) {
    const character = source[index];
    if (escaped) escaped = false;
    else if (character === '\\') escaped = true;
    else if (character === '"') {
      return { value: JSON.parse(source.slice(start, index + 1)), end: index };
    }
  }
  throw new CoverageValidationError(['unterminated string in canonical source']);
}

function objectEntries(source, openIndex) {
  const closeIndex = matchingDelimiter(source, openIndex);
  const entries = [];
  let index = openIndex + 1;
  while (index < closeIndex) {
    while (/[\s,]/.test(source[index])) index += 1;
    if (index >= closeIndex) break;
    if (source[index] !== '"') {
      throw new CoverageValidationError([`expected an object key in canonical source near offset ${index}`]);
    }
    const key = parseQuotedString(source, index);
    index = key.end + 1;
    while (/\s/.test(source[index])) index += 1;
    if (source[index] !== ':') {
      throw new CoverageValidationError([`expected ':' after canonical source key ${key.value}`]);
    }
    index += 1;
    while (/\s/.test(source[index])) index += 1;
    let valueEnd;
    if (source[index] === '{') valueEnd = matchingDelimiter(source, index);
    else if (source[index] === '[') valueEnd = matchingDelimiter(source, index, '[', ']');
    else if (source[index] === '"') valueEnd = parseQuotedString(source, index).end;
    else {
      valueEnd = index;
      while (valueEnd < closeIndex && ![',', '}'].includes(source[valueEnd])) valueEnd += 1;
      valueEnd -= 1;
    }
    entries.push({ key: key.value, valueStart: index, valueEnd });
    index = valueEnd + 1;
  }
  return entries;
}

export function readCanonicalOpenApiOperations(root = repoRoot) {
  const path = join(root, 'crates', 'backend', 'src', 'main.rs');
  const source = readFileSync(path, 'utf8');
  const functionStart = source.indexOf('fn openapi_document()');
  const pathsMarker = source.indexOf('"paths":', functionStart);
  if (functionStart < 0 || pathsMarker < 0) {
    throw new CoverageValidationError([`could not locate openapi_document().paths in ${relative(root, path)}`]);
  }
  const pathsOpen = source.indexOf('{', pathsMarker);
  const operations = [];
  for (const pathEntry of objectEntries(source, pathsOpen)) {
    if (source[pathEntry.valueStart] !== '{') {
      throw new CoverageValidationError([`expected an object value for OpenAPI path ${pathEntry.key}`]);
    }
    const methods = objectEntries(source, pathEntry.valueStart)
      .map((entry) => entry.key)
      .filter((method) => OPENAPI_METHODS.has(method));
    for (const method of methods) operations.push(`${method.toUpperCase()} ${pathEntry.key}`);
  }
  if (operations.length === 0) {
    throw new CoverageValidationError([`no OpenAPI operations found in ${relative(root, path)}`]);
  }
  return [...new Set(operations)].sort();
}

function camelToSnake(value) {
  return value.replace(/[A-Z]/g, (letter) => `_${letter.toLowerCase()}`);
}

export function readCanonicalUiRoutes(root = repoRoot) {
  const path = join(root, 'frontend', 'src', 'main.tsx');
  const source = readFileSync(path, 'utf8');
  const functionStart = source.indexOf('function parseRoute()');
  const functionOpen = source.indexOf('{', functionStart);
  if (functionStart < 0 || functionOpen < 0) {
    throw new CoverageValidationError([`could not locate parseRoute() in ${relative(root, path)}`]);
  }
  const routeSource = source.slice(functionOpen + 1, matchingDelimiter(source, functionOpen));
  const routes = new Set();
  for (const match of routeSource.matchAll(/path === '([^']+)'/g)) {
    routes.add(match[1].endsWith('/') ? match[1].slice(0, -1) : match[1]);
  }
  for (const match of routeSource.matchAll(/path\.startsWith\('([^']+)'\)/g)) {
    const route = match[1];
    if (!route.endsWith('/')) routes.add(route);
  }
  for (const match of routeSource.matchAll(/if \(path\.startsWith\('([^']+\/)'\)\) \{([\s\S]*?)\}/g)) {
    const property = match[2].match(/return \{ name: '[^']+',\s*([A-Za-z][A-Za-z0-9]*):/);
    if (property) routes.add(`${match[1]}:${camelToSnake(property[1])}`);
  }
  const lines = routeSource.split('\n');
  for (let index = 0; index < lines.length; index += 1) {
    const match = lines[index].match(/const\s+(\w+)\s*=\s*path\.match\(\/\^\\\/([A-Za-z0-9_-]+)\\\//);
    if (!match) continue;
    const following = lines.slice(index + 1, index + 4).join(' ');
    const property = following.match(new RegExp(`if \\(${match[1]}\\).*?return \\{ name: '[^']+',\\s*([A-Za-z][A-Za-z0-9]*):`));
    if (property) routes.add(`/${match[2]}/:${camelToSnake(property[1])}`);
  }
  if (routes.size === 0) {
    throw new CoverageValidationError([`no UI routes found in ${relative(root, path)}`]);
  }
  return [...routes].sort();
}

export function calculateCoverage(catalog, scenarios, canonicalOperations, canonicalRoutes) {
  const featureById = new Map(catalog.features.map((feature) => [feature.id, feature]));
  const evidenceLayers = new Map(catalog.features.map((feature) => [
    feature.id,
    new Set(feature.evidence.map((evidence) => evidence.layer))
  ]));
  const scenarioFeatureIds = new Set();
  const composeDomains = new Set();
  for (const scenario of scenarios) {
    const layer = scenario.type === 'api' ? 'qa-api' : 'qa-browser';
    for (const featureId of scenario.covers) {
      scenarioFeatureIds.add(featureId);
      evidenceLayers.get(featureId).add(layer);
      composeDomains.add(featureById.get(featureId).domain);
    }
  }

  const missingRequiredLayers = [];
  const featuresWithoutEvidence = [];
  const fullyCoveredFeatureIds = [];
  for (const feature of catalog.features) {
    const layers = evidenceLayers.get(feature.id);
    if (layers.size === 0) featuresWithoutEvidence.push(feature.id);
    const missingLayers = feature.required_layers.filter((layer) => !layers.has(layer));
    if (missingLayers.length > 0) {
      missingRequiredLayers.push({ feature_id: feature.id, domain: feature.domain, missing_layers: missingLayers });
    } else {
      fullyCoveredFeatureIds.push(feature.id);
    }
  }
  const mappedOperations = new Set(catalog.features.flatMap((feature) => feature.operations));
  const mappedRoutes = new Set(catalog.features.flatMap((feature) => feature.routes));
  const uncoveredComposeDomains = catalog.domains
    .filter((domain) => domain.requires_compose && !composeDomains.has(domain.id))
    .map((domain) => domain.id);
  const unmappedOpenApiOperations = canonicalOperations.filter((operation) => !mappedOperations.has(operation));
  const unmappedUiRoutes = canonicalRoutes.filter((route) => !mappedRoutes.has(route));
  const featureCount = catalog.features.length;
  const complete = missingRequiredLayers.length === 0
    && featuresWithoutEvidence.length === 0
    && uncoveredComposeDomains.length === 0
    && unmappedOpenApiOperations.length === 0
    && unmappedUiRoutes.length === 0;
  return {
    complete,
    scenario_ids: scenarios.map((scenario) => scenario.id),
    feature_count: featureCount,
    fully_covered_feature_count: fullyCoveredFeatureIds.length,
    fully_covered_feature_percent: Number(((fullyCoveredFeatureIds.length / featureCount) * 100).toFixed(2)),
    fully_covered_feature_ids: fullyCoveredFeatureIds,
    scenario_feature_count: scenarioFeatureIds.size,
    scenario_feature_ids: [...scenarioFeatureIds].sort(),
    gaps: {
      features_without_evidence: featuresWithoutEvidence,
      missing_required_layers: missingRequiredLayers,
      uncovered_compose_domains: uncoveredComposeDomains,
      unmapped_openapi_operations: unmappedOpenApiOperations,
      unmapped_ui_routes: unmappedUiRoutes
    }
  };
}

export function loadCoverageRepository(root = repoRoot, rootQa = qaRoot) {
  const catalogPath = join(rootQa, 'features.json');
  let catalog;
  try {
    catalog = JSON.parse(readFileSync(catalogPath, 'utf8'));
  } catch (error) {
    throw new CoverageValidationError([`${relative(root, catalogPath)} is not valid JSON: ${error.message}`]);
  }
  validateCatalog(catalog, root);
  const scenarioRoot = join(rootQa, 'scenarios');
  const scenarios = readdirSync(scenarioRoot, { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .map((entry) => {
      const directory = join(scenarioRoot, entry.name);
      const manifestPath = join(directory, 'scenario.json');
      const scenarioEntry = join(directory, 'scenario.mjs');
      const readmePath = join(directory, 'README.md');
      const missingFiles = [manifestPath, scenarioEntry, readmePath]
        .filter((path) => !existsSync(path))
        .map((path) => `${relative(root, path)} does not exist`);
      if (missingFiles.length > 0) throw new CoverageValidationError(missingFiles);
      let manifest;
      try {
        manifest = JSON.parse(readFileSync(manifestPath, 'utf8'));
      } catch (error) {
        throw new CoverageValidationError([`${relative(root, manifestPath)} is not valid JSON: ${error.message}`]);
      }
      const validated = validateScenarioManifest(manifest, entry.name, catalog);
      return {
        id: entry.name,
        name: validated.name,
        type: validated.type,
        timeoutMs: validated.timeout_ms,
        covers: validated.covers,
        entry: scenarioEntry
      };
    })
    .sort((left, right) => left.id.localeCompare(right.id));
  return {
    catalog,
    scenarios,
    canonicalOperations: readCanonicalOpenApiOperations(root),
    canonicalRoutes: readCanonicalUiRoutes(root)
  };
}

function discoverScenarios(repository) {
  return repository.scenarios;
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

function coverageLine(label, report) {
  return `${label}: ${report.fully_covered_feature_count}/${report.feature_count} fully covered (${report.fully_covered_feature_percent}%), ${report.scenario_feature_count} scenario-covered`;
}

function printCoverageGaps(report) {
  const { gaps } = report;
  if (gaps.features_without_evidence.length > 0) {
    console.log(`Features without evidence (${gaps.features_without_evidence.length}):`);
    for (const featureId of gaps.features_without_evidence) console.log(`  ${featureId}`);
  }
  if (gaps.missing_required_layers.length > 0) {
    console.log(`Missing required layers (${gaps.missing_required_layers.length} features):`);
    for (const gap of gaps.missing_required_layers) {
      console.log(`  ${gap.feature_id} [${gap.domain}]: ${gap.missing_layers.join(', ')}`);
    }
  }
  if (gaps.uncovered_compose_domains.length > 0) {
    console.log(`Compose domains without qa-api/qa-browser scenarios (${gaps.uncovered_compose_domains.length}):`);
    for (const domain of gaps.uncovered_compose_domains) console.log(`  ${domain}`);
  }
  if (gaps.unmapped_openapi_operations.length > 0) {
    console.log(`Unmapped OpenAPI operations (${gaps.unmapped_openapi_operations.length}):`);
    for (const operation of gaps.unmapped_openapi_operations) console.log(`  ${operation}`);
  }
  if (gaps.unmapped_ui_routes.length > 0) {
    console.log(`Unmapped UI routes (${gaps.unmapped_ui_routes.length}):`);
    for (const route of gaps.unmapped_ui_routes) console.log(`  ${route}`);
  }
}

function reportCoverage(coverage) {
  console.log(coverageLine('Selected scenario coverage', coverage.selected));
  console.log(coverageLine('Overall repository coverage', coverage.overall));
  if (coverage.overall.complete) {
    console.log('Coverage validation passed.');
  } else {
    console.log('Coverage validation found pending gaps:');
    printCoverageGaps(coverage.overall);
  }
}

export async function writeSummary(artifactsRoot, project, baseURL, results, coverage) {
  const summary = redactSecrets({
    project,
    base_url: baseURL,
    passed: results.filter((result) => result.status === 'passed').length,
    failed: results.filter((result) => result.status === 'failed').length,
    not_run: results.filter((result) => result.status === 'not_run').length,
    scenarios: results,
    coverage
  });
  await writeFile(join(artifactsRoot, 'summary.json'), `${JSON.stringify(summary, null, 2)}\n`);
  const safeResults = summary.scenarios;
  const failures = summary.failed;
  const skipped = summary.not_run;
  const durationSeconds = safeResults.reduce((total, result) => total + result.duration_ms, 0) / 1_000;
  const cases = safeResults.map((result) => {
    const outcome = result.status === 'failed'
      ? `<failure message="${xmlEscape(result.error)}">${xmlEscape(result.error)}</failure>`
      : result.status === 'not_run'
        ? `<skipped message="${xmlEscape(result.reason)}">${xmlEscape(result.reason)}</skipped>`
        : '';
    return `  <testcase classname="agent-hub.qa.${result.type}" name="${xmlEscape(result.id)}" status="${result.status}" time="${(result.duration_ms / 1_000).toFixed(3)}">${outcome}</testcase>`;
  }).join('\n');
  const properties = ['selected', 'overall']
    .map((scope) => `    <property name="coverage.${scope}" value="${xmlEscape(JSON.stringify(coverage[scope]))}"/>`)
    .join('\n');
  const junit = `<?xml version="1.0" encoding="UTF-8"?>\n<testsuite name="agent-hub-qa" tests="${safeResults.length}" failures="${failures}" skipped="${skipped}" time="${durationSeconds.toFixed(3)}">\n  <properties>\n${properties}\n  </properties>\n${cases}\n</testsuite>\n`;
  await writeFile(join(artifactsRoot, 'junit.xml'), junit);
}

export function isWorkerHardTimeout(worker) {
  return worker.error?.code === 'ETIMEDOUT';
}

export async function executeScenarioQueue(scenarios, executeScenario) {
  const results = [];
  for (const [index, scenario] of scenarios.entries()) {
    const { result, hardTimeout = false } = await executeScenario(scenario);
    results.push(result);
    if (!hardTimeout) continue;

    const reason = `Not run because ${scenario.id} hit its hard timeout; the shared QA environment may be contaminated.`;
    for (const remaining of scenarios.slice(index + 1)) {
      results.push({
        id: remaining.id,
        name: remaining.name,
        type: remaining.type,
        status: 'not_run',
        duration_ms: 0,
        reason
      });
    }
    break;
  }
  return results;
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
    return redactSecrets(String(failure.message || fallback));
  } catch {
    return redactSecrets(String(fallback));
  }
}

async function finalizeArtifacts(artifactsPath) {
  await sanitizeArtifactTree(artifactsPath);
  await assertArtifactTreeSafe(artifactsPath);
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  if (options.help) {
    usage();
    return;
  }
  const repository = loadCoverageRepository();
  const allScenarios = discoverScenarios(repository);
  if (options.list) {
    for (const scenario of allScenarios) console.log(`${scenario.id}\t${scenario.type}\t${scenario.name}`);
    return;
  }
  const scenarios = selectScenarios(allScenarios, options);
  const coverage = {
    selected: calculateCoverage(
      repository.catalog,
      scenarios,
      repository.canonicalOperations,
      repository.canonicalRoutes
    ),
    overall: calculateCoverage(
      repository.catalog,
      allScenarios,
      repository.canonicalOperations,
      repository.canonicalRoutes
    )
  };
  if (options.coverage) {
    reportCoverage(coverage);
    if (!coverage.overall.complete) process.exitCode = 1;
    return;
  }
  const runId = new Date().toISOString().replaceAll(':', '-').replaceAll('.', '-');
  const artifactsRoot = join(qaRoot, 'artifacts', runId);
  await mkdir(artifactsRoot, { recursive: true });
  const project = process.env.QA_COMPOSE_PROJECT?.trim()
    || `agent-hub-qa-${Date.now().toString(36)}-${process.pid}`;
  const compose = new ComposeHarness({ repoRoot, project });
  let started = false;
  let baseURL = '';
  let interrupted = false;
  let environmentContaminated = false;

  const stopForSignal = async (signal) => {
    if (interrupted) return;
    interrupted = true;
    console.error(`\nReceived ${signal}; cleaning up ${project}.`);
    if (started && (!options.keepEnv || environmentContaminated)) compose.down();
    try {
      await finalizeArtifacts(artifactsRoot);
    } catch {
      console.error('QA artifact finalization failed; unsafe artifacts were removed.');
    }
    process.exit(signal === 'SIGINT' ? 130 : 143);
  };
  process.once('SIGINT', stopForSignal);
  process.once('SIGTERM', stopForSignal);

  try {
    console.log(`Starting isolated QA environment: ${project}`);
    started = true;
    compose.start();
    baseURL = compose.frontendURL();
    await healthCheck(baseURL);
    console.log(`QA environment ready: ${baseURL}`);

    const results = await executeScenarioQueue(scenarios, async (scenario) => {
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
        try {
          await finalizeArtifacts(artifactsDir);
        } catch {
          const error = 'QA artifact finalization failed; unsafe artifacts were removed.';
          console.error(`[FAIL] ${scenario.id}: ${error}`);
          return {
            result: { id: scenario.id, name: scenario.name, type: scenario.type, status: 'failed', duration_ms: durationMs, error }
          };
        }
        console.log(`[PASS] ${scenario.id} (${durationMs} ms)`);
        return {
          result: { id: scenario.id, name: scenario.name, type: scenario.type, status: 'passed', duration_ms: durationMs }
        };
      }
      const hardTimeout = isWorkerHardTimeout(worker);
      if (hardTimeout) environmentContaminated = true;
      const fallback = hardTimeout
        ? `Timed out after ${scenario.timeoutMs} ms`
        : worker.error?.message || `Scenario worker exited with status ${worker.status}`;
      let error = await workerFailure(artifactsDir, fallback);
      let artifactFinalizationFailed = false;
      try {
        await writeFile(join(artifactsDir, 'compose.log'), redactSecrets(compose.logs()));
      } catch {
        artifactFinalizationFailed = true;
      }
      try {
        await finalizeArtifacts(artifactsDir);
      } catch {
        artifactFinalizationFailed = true;
      }
      if (artifactFinalizationFailed) {
        error = `${error}\nQA artifact finalization failed; unsafe artifacts were removed.`;
      }
      console.error(`[FAIL] ${scenario.id}: ${error}`);
      return {
        result: { id: scenario.id, name: scenario.name, type: scenario.type, status: 'failed', duration_ms: durationMs, error },
        hardTimeout
      };
    });
    for (const result of results.filter((result) => result.status === 'not_run')) {
      console.error(`[NOT RUN] ${result.id}: ${result.reason}`);
    }

    await writeSummary(artifactsRoot, project, baseURL, results, coverage);
    const passed = results.filter((result) => result.status === 'passed').length;
    const failed = results.filter((result) => result.status === 'failed').length;
    const notRun = results.filter((result) => result.status === 'not_run').length;
    console.log(`\nQA summary: ${passed} passed, ${failed} failed, ${notRun} not run`);
    console.log(`Artifacts: ${artifactsRoot}`);
    if (failed > 0) process.exitCode = 1;
  } finally {
    process.removeListener('SIGINT', stopForSignal);
    process.removeListener('SIGTERM', stopForSignal);
    try {
      await finalizeArtifacts(artifactsRoot);
    } catch {
      console.error('QA artifact finalization failed; unsafe artifacts were removed.');
      process.exitCode = 1;
    }
    if (started) {
      if (options.keepEnv && !environmentContaminated) console.log(`Keeping QA environment ${project} at ${baseURL}`);
      else compose.down();
    }
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  await main().catch((error) => {
    console.error(error instanceof Error ? error.stack : String(error));
    process.exitCode = 1;
  });
}
