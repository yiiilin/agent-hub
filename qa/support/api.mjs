import { randomBytes } from 'node:crypto';
import { redactSecrets } from './secrets.mjs';

export function qaSourceIp() {
  const bytes = randomBytes(3);
  return `198.${18 + (bytes[0] & 1)}.${bytes[1]}.${(bytes[2] % 254) + 1}`;
}

function expectedStatusMatches(status, expectedStatus) {
  if (expectedStatus === undefined) return status >= 200 && status < 300;
  if (Array.isArray(expectedStatus)) return expectedStatus.includes(status);
  return status === expectedStatus;
}

export class ApiClient {
  constructor(baseURL, { sourceIp = qaSourceIp() } = {}) {
    this.baseURL = new URL(baseURL);
    this.cookies = new Map();
    this.sourceIp = sourceIp;
  }

  absorbCookies(headers) {
    const values = typeof headers.getSetCookie === 'function'
      ? headers.getSetCookie()
      : [headers.get('set-cookie')].filter(Boolean);
    for (const value of values) {
      const pair = value.split(';', 1)[0];
      const separator = pair.indexOf('=');
      if (separator <= 0) continue;
      const name = pair.slice(0, separator);
      const cookieValue = pair.slice(separator + 1);
      if (cookieValue) this.cookies.set(name, cookieValue);
      else this.cookies.delete(name);
    }
  }

  cookieHeader() {
    return [...this.cookies].map(([name, value]) => `${name}=${value}`).join('; ');
  }

  async request(path, { method = 'GET', body, headers = {}, expectedStatus, signal } = {}) {
    const requestHeaders = { accept: 'application/json', ...headers };
    const pathname = new URL(path, this.baseURL).pathname;
    const suppliedHeaders = new Set(Object.keys(requestHeaders).map((name) => name.toLowerCase()));
    if (
      ['/api/auth/login', '/api/auth/ldap/login'].includes(pathname)
      && !suppliedHeaders.has('forwarded')
      && !suppliedHeaders.has('x-forwarded-for')
    ) {
      requestHeaders['x-forwarded-for'] = this.sourceIp;
    }
    const cookie = this.cookieHeader();
    if (cookie) requestHeaders.cookie = cookie;
    let requestBody;
    if (body !== undefined) {
      requestHeaders['content-type'] = 'application/json';
      requestBody = JSON.stringify(body);
    }
    const response = await fetch(new URL(path, this.baseURL), {
      method,
      headers: requestHeaders,
      body: requestBody,
      signal
    });
    this.absorbCookies(response.headers);
    const text = await response.text();
    let data = null;
    if (text) {
      try {
        data = JSON.parse(text);
      } catch {
        data = text;
      }
    }
    if (!expectedStatusMatches(response.status, expectedStatus)) {
      const detail = redactSecrets(typeof data === 'string' ? data : JSON.stringify(data));
      throw new Error(redactSecrets(
        `${method} ${path} returned ${response.status}: ${detail.slice(0, 2_000)}`
      ));
    }
    return { status: response.status, headers: response.headers, data };
  }

  get(path, options) {
    return this.request(path, options);
  }

  post(path, body, options = {}) {
    return this.request(path, { ...options, method: 'POST', body });
  }

  delete(path, options = {}) {
    return this.request(path, { ...options, method: 'DELETE' });
  }
}

export async function loginAsAdmin(client) {
  await client.post('/api/auth/login', {
    email: 'admin@example.com',
    password: 'admin123'
  });
  return client.get('/api/auth/me');
}

export async function provisionLocalUser(adminClient, context, prefix, {
  role = 'member',
  displayName = context.unique('QA local user')
} = {}) {
  const slug = context.unique(prefix)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '');
  const email = `${slug}@example.com`;
  const password = `${context.unique('QA local password')}!Aa9`;
  const { data: detail } = await adminClient.post('/api/admin/users', {
    email,
    display_name: displayName,
    password,
    role
  });
  const client = new ApiClient(context.baseURL);
  const { data: login } = await client.post('/api/auth/login', { email, password });
  return { client, user: login.user, detail, email, password };
}

export async function poll(check, accept, {
  timeoutMs = 45_000,
  intervalMs = 250,
  description = 'condition'
} = {}) {
  const deadline = Date.now() + timeoutMs;
  let lastValue;
  while (Date.now() < deadline) {
    lastValue = await check();
    if (accept(lastValue)) return lastValue;
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
  throw new Error(redactSecrets(
    `Timed out waiting for ${description}; last value: ${JSON.stringify(lastValue)}`
  ));
}

export async function waitForRunStatus(client, agentId, runId, expectedStatuses, timeoutMs = 45_000) {
  const wanted = new Set(Array.isArray(expectedStatuses) ? expectedStatuses : [expectedStatuses]);
  return poll(async () => {
    const { data } = await client.get(`/api/agents/${agentId}/runs`);
    return data.find((run) => run.id === runId) ?? null;
  }, (run) => run !== null && wanted.has(run.status), {
    timeoutMs,
    description: `Run ${runId} to reach ${[...wanted].join(' or ')}`
  });
}
