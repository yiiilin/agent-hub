import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { extname, join, normalize, resolve, sep } from 'node:path';

const JSON_CONTENT_TYPE = 'application/json; charset=utf-8';

function send(response, status, body, headers = {}) {
  response.writeHead(status, {
    'cache-control': 'no-store',
    ...headers
  });
  response.end(body);
}

async function readJson(request) {
  const chunks = [];
  let size = 0;
  for await (const chunk of request) {
    size += chunk.length;
    if (size > 16_384) throw new Error('request body is too large');
    chunks.push(chunk);
  }
  if (size === 0) return {};
  return JSON.parse(Buffer.concat(chunks).toString('utf8'));
}

function requestOrigin(request) {
  const host = request.headers.host;
  if (!host) throw new Error('request host is required');
  return new URL(`http://${host}`).origin;
}

function contentType(path) {
  if (extname(path) === '.js' || extname(path) === '.mjs') return 'text/javascript; charset=utf-8';
  if (extname(path) === '.html') return 'text/html; charset=utf-8';
  if (extname(path) === '.json' || extname(path) === '.map') return JSON_CONTENT_TYPE;
  return 'application/octet-stream';
}

function safeFile(root, requested) {
  const relative = normalize(requested).replace(/^([/\\])+/, '');
  const candidate = resolve(root, relative);
  if (candidate !== root && !candidate.startsWith(`${root}${sep}`)) return null;
  return candidate;
}

async function serveFile(response, root, requested) {
  const path = safeFile(root, requested);
  if (!path) {
    send(response, 404, 'Not found');
    return;
  }
  try {
    const body = await readFile(path);
    send(response, 200, body, { 'content-type': contentType(path) });
  } catch {
    send(response, 404, 'Not found');
  }
}

function serializeConfig(config) {
  if (!config) return null;
  return {
    hubBaseUrl: config.hubBaseUrl,
    ...(config.clientId ? { clientId: config.clientId } : {}),
    ...(config.toolName ? { toolName: config.toolName } : {})
  };
}

/**
 * Serves a tiny same-origin browser harness for the published Client SDK.
 * Client secrets stay in this Node process; the browser only calls /authorize.
 */
export async function startClientSdkHost({ repoRoot, hubBaseUrl, fixtureDir }) {
  const sdkDir = join(repoRoot, 'sdk', 'typescript', 'dist');
  const fixtureRoot = resolve(fixtureDir);
  const state = {
    authenticated: null,
    anonymous: null,
    origin: null
  };

  const server = createServer(async (request, response) => {
    const url = new URL(request.url ?? '/', state.origin ?? 'http://127.0.0.1');
    try {
      if (url.pathname === '/favicon.ico') {
        send(response, 204, '');
        return;
      }
      if (url.pathname === '/' || url.pathname === '/index.html') {
        await serveFile(response, fixtureRoot, 'index.html');
        return;
      }
      if (url.pathname === '/app.mjs') {
        await serveFile(response, fixtureRoot, 'app.mjs');
        return;
      }
      if (url.pathname.startsWith('/sdk/')) {
        await serveFile(response, sdkDir, url.pathname.slice('/sdk/'.length));
        return;
      }
      if (url.pathname === '/config' && request.method === 'GET') {
        const mode = url.searchParams.get('mode');
        const config = mode === 'anonymous' ? state.anonymous : state.authenticated;
        if (!config) {
          send(response, 409, JSON.stringify({ code: 'qa_fixture_not_configured' }), {
            'content-type': JSON_CONTENT_TYPE
          });
          return;
        }
        send(response, 200, JSON.stringify(serializeConfig(config)), {
          'content-type': JSON_CONTENT_TYPE
        });
        return;
      }
      if (url.pathname === '/authorize' && request.method === 'POST') {
        const config = state.authenticated;
        if (!config) {
          send(response, 409, JSON.stringify({ code: 'qa_fixture_not_configured' }), {
            'content-type': JSON_CONTENT_TYPE
          });
          return;
        }
        const body = await readJson(request);
        if (typeof body.client_instance_id !== 'string') {
          send(response, 400, JSON.stringify({ code: 'client_instance_id_required' }), {
            'content-type': JSON_CONTENT_TYPE
          });
          return;
        }
        const origin = url.searchParams.get('grant_origin') === 'allowed'
          ? state.origin
          : requestOrigin(request);
        const upstream = await fetch(new URL('/api/client/access', config.hubBaseUrl), {
          method: 'POST',
          headers: {
            accept: 'application/json',
            authorization: `Basic ${Buffer.from(`${config.clientId}:${config.clientSecret}`).toString('base64')}`,
            'content-type': 'application/json',
            origin
          },
          body: JSON.stringify({
            agent_id: config.agentId,
            client_instance_id: body.client_instance_id,
            external_user_id: config.externalUserId,
            tenant_id: config.tenantId,
            username: config.username,
            display_name: config.displayName,
            email: config.email,
            attributes: config.attributes,
            client_tools: config.clientTools
          })
        });
        const upstreamBody = await upstream.text();
        send(response, upstream.status, upstreamBody, { 'content-type': JSON_CONTENT_TYPE });
        return;
      }
      send(response, 404, 'Not found');
    } catch (error) {
      send(response, 500, JSON.stringify({
        code: 'qa_fixture_error',
        message: error instanceof Error ? error.message : String(error)
      }), { 'content-type': JSON_CONTENT_TYPE });
    }
  });

  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('Client SDK host did not bind a TCP port');
  const origin = `http://127.0.0.1:${address.port}`;
  state.origin = origin;

  return {
    origin,
    alternateOrigin: `http://localhost:${address.port}`,
    url(path = '/') {
      return new URL(path, origin).href;
    },
    configureAuthenticated(config) {
      state.authenticated = { ...config, hubBaseUrl };
    },
    configureAnonymous(config) {
      state.anonymous = { ...config, hubBaseUrl };
    },
    async close() {
      await new Promise((resolve, reject) => {
        server.close((error) => (error ? reject(error) : resolve()));
      });
    }
  };
}
