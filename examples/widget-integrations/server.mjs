import { randomBytes } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { dirname, extname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const sourceDirectory = dirname(fileURLToPath(import.meta.url));
const sessionCookieName = 'agent_hub_example_session';
const sessionLifetimeMs = 24 * 60 * 60 * 1_000;

function required(environment, name) {
  const value = environment[name]?.trim();
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function normalizedBaseUrl(environment, name) {
  const value = new URL(required(environment, name));
  if (!['http:', 'https:'].includes(value.protocol)) {
    throw new Error(`${name} must use http or https`);
  }
  value.pathname = '/';
  value.search = '';
  value.hash = '';
  return value;
}

function commaSeparated(environment, name) {
  return required(environment, name)
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean);
}

function loadConfiguration(environment = process.env) {
  const port = Number(environment.PORT ?? '15179');
  if (!Number.isInteger(port) || port < 1 || port > 65_535) {
    throw new Error('PORT must be an integer between 1 and 65535');
  }

  let attributes;
  try {
    attributes = JSON.parse(environment.AUTH_WIDGET_USER_ATTRIBUTES ?? '{}');
  } catch {
    throw new Error('AUTH_WIDGET_USER_ATTRIBUTES must be valid JSON');
  }
  if (!attributes || Array.isArray(attributes) || typeof attributes !== 'object') {
    throw new Error('AUTH_WIDGET_USER_ATTRIBUTES must be a JSON object');
  }

  return {
    host: environment.HOST?.trim() || '127.0.0.1',
    port,
    hubUrl: normalizedBaseUrl(environment, 'AGENT_HUB_URL'),
    publicWidget: {
      appClientId: required(environment, 'PUBLIC_WIDGET_CLIENT_ID'),
      appName: required(environment, 'PUBLIC_WIDGET_APP_NAME'),
      agentName: required(environment, 'PUBLIC_WIDGET_AGENT_NAME'),
      tools: commaSeparated(environment, 'PUBLIC_WIDGET_TOOLS')
    },
    authenticatedWidget: {
      appClientId: required(environment, 'AUTH_WIDGET_CLIENT_ID'),
      appClientSecret: required(environment, 'AUTH_WIDGET_CLIENT_SECRET'),
      appName: required(environment, 'AUTH_WIDGET_APP_NAME'),
      agentId: required(environment, 'AUTH_WIDGET_AGENT_ID'),
      agentName: required(environment, 'AUTH_WIDGET_AGENT_NAME'),
      tools: commaSeparated(environment, 'AUTH_WIDGET_TOOLS'),
      externalUser: {
        external_user_id: required(environment, 'AUTH_WIDGET_EXTERNAL_USER_ID'),
        tenant_id: required(environment, 'AUTH_WIDGET_TENANT_ID'),
        username: required(environment, 'AUTH_WIDGET_USERNAME'),
        display_name: required(environment, 'AUTH_WIDGET_DISPLAY_NAME'),
        email: required(environment, 'AUTH_WIDGET_EMAIL'),
        attributes
      }
    }
  };
}

function parseCookies(header) {
  const result = new Map();
  for (const item of (header ?? '').split(';')) {
    const separator = item.indexOf('=');
    if (separator < 1) continue;
    result.set(item.slice(0, separator).trim(), item.slice(separator + 1).trim());
  }
  return result;
}

function jsonResponse(response, status, value, headers = {}) {
  response.writeHead(status, {
    'cache-control': 'no-store',
    'content-type': 'application/json; charset=utf-8',
    ...headers
  });
  response.end(JSON.stringify(value));
}

function textResponse(response, status, value, contentType, headers = {}) {
  response.writeHead(status, {
    'cache-control': 'no-store',
    'content-type': contentType,
    ...headers
  });
  response.end(value);
}

function sameOriginRequest(request) {
  const origin = request.headers.origin;
  if (!origin) return true;
  const forwardedProtocol = request.headers['x-forwarded-proto'];
  const protocol = typeof forwardedProtocol === 'string' ? forwardedProtocol : 'http';
  return origin === `${protocol}://${request.headers.host}`;
}

function securityHeaders(hubOrigin) {
  return {
    'content-security-policy': [
      "default-src 'self'",
      `frame-src ${hubOrigin}`,
      "connect-src 'self'",
      "img-src 'self' data:",
      "script-src 'self'",
      "style-src 'self'",
      "base-uri 'none'",
      "form-action 'self'",
      "frame-ancestors 'none'"
    ].join('; '),
    'referrer-policy': 'no-referrer',
    'x-content-type-options': 'nosniff'
  };
}

function publicConfiguration(configuration) {
  return {
    hub_origin: configuration.hubUrl.origin,
    widget_url: new URL(
      `/widget?app=${encodeURIComponent(configuration.publicWidget.appClientId)}`,
      configuration.hubUrl
    ).href,
    app_name: configuration.publicWidget.appName,
    agent_name: configuration.publicWidget.agentName,
    tools: configuration.publicWidget.tools,
    login_required: false,
    history_enabled: false
  };
}

function authenticatedConfiguration(configuration, externalUser) {
  return {
    hub_origin: configuration.hubUrl.origin,
    widget_url: new URL('/widget', configuration.hubUrl).href,
    app_name: configuration.authenticatedWidget.appName,
    agent_name: configuration.authenticatedWidget.agentName,
    tools: configuration.authenticatedWidget.tools,
    login_required: true,
    history_enabled: true,
    user: externalUser
  };
}

export function createWidgetIntegrationServer(configuration, { fetchImpl = fetch } = {}) {
  const sessions = new Map();
  const headers = securityHeaders(configuration.hubUrl.origin);

  function activeSession(request) {
    const sessionId = parseCookies(request.headers.cookie).get(sessionCookieName);
    const session = sessionId ? sessions.get(sessionId) : undefined;
    if (!session || session.expiresAt <= Date.now()) {
      if (sessionId) sessions.delete(sessionId);
      return null;
    }
    return session;
  }

  function createSession() {
    const id = randomBytes(32).toString('base64url');
    const session = {
      expiresAt: Date.now() + sessionLifetimeMs,
      externalUser: structuredClone(configuration.authenticatedWidget.externalUser)
    };
    sessions.set(id, session);
    return { id, session };
  }

  return createServer(async (request, response) => {
    try {
      const url = new URL(request.url ?? '/', `http://${request.headers.host ?? 'localhost'}`);
      if (request.method === 'GET' && url.pathname === '/healthz') {
        return jsonResponse(response, 200, { ok: true });
      }
      if (request.method === 'GET' && url.pathname === '/') {
        response.writeHead(302, { location: '/anonymous/' });
        return response.end();
      }
      if (request.method === 'GET' && url.pathname === '/anonymous/') {
        const body = await readFile(join(sourceDirectory, 'anonymous.html'), 'utf8');
        return textResponse(response, 200, body, 'text/html; charset=utf-8', headers);
      }
      if (request.method === 'GET' && url.pathname === '/authenticated/') {
        let session = activeSession(request);
        const responseHeaders = { ...headers };
        if (!session) {
          const created = createSession();
          session = created.session;
          responseHeaders['set-cookie'] = `${sessionCookieName}=${created.id}; Path=/; HttpOnly; SameSite=Strict; Max-Age=${sessionLifetimeMs / 1_000}`;
        }
        const body = await readFile(join(sourceDirectory, 'authenticated.html'), 'utf8');
        return textResponse(response, 200, body, 'text/html; charset=utf-8', responseHeaders);
      }
      if (request.method === 'GET' && url.pathname === '/api/public/config') {
        return jsonResponse(response, 200, publicConfiguration(configuration));
      }
      if (request.method === 'GET' && url.pathname === '/api/authenticated/config') {
        const session = activeSession(request);
        if (!session) return jsonResponse(response, 401, { error: 'login_required' });
        return jsonResponse(
          response,
          200,
          authenticatedConfiguration(configuration, session.externalUser)
        );
      }
      if (request.method === 'POST' && url.pathname === '/api/authenticated/widget-access') {
        if (!sameOriginRequest(request)) {
          return jsonResponse(response, 403, { error: 'origin_not_allowed' });
        }
        const session = activeSession(request);
        if (!session) return jsonResponse(response, 401, { error: 'login_required' });

        const authorization = Buffer.from(
          `${configuration.authenticatedWidget.appClientId}:${configuration.authenticatedWidget.appClientSecret}`
        ).toString('base64');
        const upstream = await fetchImpl(new URL('/api/widget/access', configuration.hubUrl), {
          method: 'POST',
          headers: {
            accept: 'application/json',
            authorization: `Basic ${authorization}`,
            'content-type': 'application/json'
          },
          body: JSON.stringify({
            agent_id: configuration.authenticatedWidget.agentId,
            ...session.externalUser
          })
        });
        if (!upstream.ok) {
          return jsonResponse(response, 502, {
            error: 'widget_access_failed',
            upstream_status: upstream.status
          });
        }
        const access = await upstream.json();
        return jsonResponse(response, 200, access);
      }
      if (request.method === 'GET' && url.pathname.startsWith('/assets/')) {
        const filename = url.pathname.slice('/assets/'.length);
        if (!['example.css', 'anonymous.js', 'authenticated.js'].includes(filename)) {
          return textResponse(response, 404, 'Not found', 'text/plain; charset=utf-8');
        }
        const body = await readFile(join(sourceDirectory, filename), 'utf8');
        const contentType = extname(filename) === '.css'
          ? 'text/css; charset=utf-8'
          : 'text/javascript; charset=utf-8';
        return textResponse(response, 200, body, contentType, headers);
      }
      return textResponse(response, 404, 'Not found', 'text/plain; charset=utf-8');
    } catch (error) {
      console.error(error instanceof Error ? error.message : 'Unexpected example server error');
      return jsonResponse(response, 500, { error: 'request_failed' });
    }
  });
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const configArgument = process.argv.indexOf('--config');
  let environment = process.env;
  if (configArgument >= 0) {
    const configPath = process.argv[configArgument + 1];
    if (!configPath) throw new Error('--config requires a JSON file path');
    environment = {
      ...process.env,
      ...JSON.parse(await readFile(configPath, 'utf8'))
    };
  }
  const configuration = loadConfiguration(environment);
  const server = createWidgetIntegrationServer(configuration);
  server.listen(configuration.port, configuration.host, () => {
    console.log(`Widget integration examples listening on http://${configuration.host}:${configuration.port}`);
  });
}

export { loadConfiguration };
