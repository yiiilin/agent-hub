const SECRET_PATTERNS = [
  /\bahk_[A-Za-z0-9._-]+\b/g,
  /\bahr_[A-Za-z0-9._-]+\b/g,
  /\bBearer\s+[^\s"']+/gi
];

function redact(value) {
  return SECRET_PATTERNS.reduce((current, pattern) => current.replace(pattern, '[REDACTED]'), value);
}

function expectedStatusMatches(status, expectedStatus) {
  if (expectedStatus === undefined) return status >= 200 && status < 300;
  if (Array.isArray(expectedStatus)) return expectedStatus.includes(status);
  return status === expectedStatus;
}

export class ApiClient {
  constructor(baseURL) {
    this.baseURL = new URL(baseURL);
    this.cookies = new Map();
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
      const detail = redact(typeof data === 'string' ? data : JSON.stringify(data));
      throw new Error(`${method} ${path} returned ${response.status}: ${detail.slice(0, 2_000)}`);
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
  throw new Error(`Timed out waiting for ${description}; last value: ${JSON.stringify(lastValue)}`);
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
