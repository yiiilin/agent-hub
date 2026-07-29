import { connect, connectAnonymous } from '@agent-hub/client';

const params = new URLSearchParams(location.search);
const mode = params.get('mode') === 'anonymous' ? 'anonymous' : 'authenticated';
const role = params.get('role') ?? 'primary';
const configuredSessionId = params.get('session');
const configuredAfter = Number(params.get('after') ?? '0');
const initialClientInstanceId = sessionStorage.getItem('agent-hub:client-instance-id');
const status = document.querySelector('#status');
const observerButton = document.querySelector('#open-observer');

const state = {
  mode,
  role,
  initialClientInstanceId,
  connected: false,
  clientInstanceId: null,
  sessionId: configuredSessionId,
  lastSequence: 0,
  authorizedTools: [],
  handlerCalls: 0,
  handlerStarted: 0,
  handlerInputs: [],
  renewCount: 0,
  resultPostCount: 0,
  events: [],
  errors: [],
  holdHandler: params.get('hold') === '1'
};

let client;
let session;
const subscriptions = new Set();
let releaseHandler;
let handlerRelease = new Promise((resolve) => { releaseHandler = resolve; });

function updateStatus(value) {
  status.textContent = value;
}

function serializableEvent(event) {
  const raw = event.raw;
  return {
    type: event.type,
    sequence: event.sequence,
    ...(event.runId ? { runId: event.runId } : {}),
    ...(event.toolCallId ? { toolCallId: event.toolCallId } : {}),
    ...(event.toolName ? { toolName: event.toolName } : {}),
    ...(event.eventType ? { eventType: event.eventType } : {}),
    ...(event.role ? { role: event.role } : {}),
    ...(event.content !== undefined ? { content: event.content } : {}),
    ...(event.code ? { code: event.code } : {}),
    ...(typeof raw?.status === 'number' ? { status: raw.status } : {})
  };
}

function listener(event) {
  const item = serializableEvent(event);
  state.events.push(item);
  state.lastSequence = Math.max(state.lastSequence, item.sequence ?? 0);
  if (item.type === 'error') state.errors.push(item);
}

async function readConfig() {
  const response = await fetch(`/config?mode=${encodeURIComponent(mode)}`, { cache: 'no-store' });
  if (!response.ok) throw new Error(`QA host config failed with ${response.status}`);
  return response.json();
}

async function sdkFetch(input, init) {
  const url = new URL(typeof input === 'string' ? input : input.url, location.href);
  if (init?.method === 'POST' && url.pathname === '/api/client/renew') {
    state.renewCount += 1;
  }
  const response = await fetch(input, init);
  if (init?.method === 'POST' && /\/api\/client\/tool-calls\/[^/]+\/result$/.test(url.pathname)) {
    state.resultPostCount += 1;
  }
  return response;
}

function toolHandler(input) {
  state.handlerCalls += 1;
  state.handlerStarted += 1;
  state.handlerInputs.push(input);
  return (async () => {
    if (state.holdHandler) await handlerRelease;
    return { handled_by: role, echoed_input: input };
  })();
}

function selectSession() {
  if (configuredSessionId) return client.existing(configuredSessionId);
  return client.currentSession?.() ?? client.draft();
}

async function start() {
  const config = await readConfig();
  const handlers = { [config.toolName ?? 'echo']: toolHandler };
  if (mode === 'anonymous') {
    client = await connectAnonymous({
      baseUrl: config.hubBaseUrl,
      clientId: config.clientId,
      fetch: sdkFetch,
      handlers
    });
  } else {
    client = await connect({
      baseUrl: config.hubBaseUrl,
      fetch: sdkFetch,
      handlers,
      authorize: async ({ clientInstanceId, signal }) => {
        const grantOrigin = params.get('grant_origin') === 'allowed' ? '?grant_origin=allowed' : '';
        const response = await fetch(`/authorize${grantOrigin}`, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ client_instance_id: clientInstanceId }),
          signal,
          cache: 'no-store'
        });
        if (!response.ok) throw new Error(`Client access failed with ${response.status}`);
        const credential = await response.json();
        if (params.get('renew') === 'immediate') {
          credential.expires_at = new Date(Date.now() + 100).toISOString();
        }
        return credential;
      }
    });
  }
  session = selectSession();
  state.connected = true;
  state.clientInstanceId = client.clientInstanceId;
  state.sessionId = session.id;
  state.authorizedTools = [...client.authorizedToolNames];
  updateStatus('Connected');
  observerButton.disabled = !session.id || role !== 'primary' || mode !== 'authenticated';
  return snapshot();
}

function snapshot() {
  return structuredClone(state);
}

function subscribe(after = configuredAfter) {
  if (!session?.id) throw new Error('A Session is required before subscribing');
  const subscription = session.subscribe(listener, { after, reconnectDelayMs: 40 });
  subscriptions.add(subscription);
  void subscription.closed.finally(() => subscriptions.delete(subscription));
  return snapshot();
}

async function send(message) {
  if (!session) throw new Error('Client is not connected');
  const sent = await session.send(message);
  state.sessionId = sent.sessionId;
  observerButton.disabled = role !== 'primary' || mode !== 'authenticated';
  return {
    runId: sent.run.id,
    sessionId: sent.sessionId,
    clientMessageKey: sent.clientMessageKey
  };
}

function newDraft() {
  for (const subscription of subscriptions) subscription.dispose();
  subscriptions.clear();
  session = client.draft();
  state.sessionId = null;
  state.lastSequence = 0;
  state.events = [];
  state.errors = [];
  observerButton.disabled = true;
  return snapshot();
}

function observerUrl() {
  if (!session?.id) throw new Error('A Session is required before opening an observer');
  const url = new URL('/index.html', location.href);
  url.searchParams.set('mode', 'authenticated');
  url.searchParams.set('role', 'observer');
  url.searchParams.set('session', session.id);
  url.searchParams.set('after', String(state.lastSequence));
  return url.href;
}

observerButton.addEventListener('click', () => {
  window.open(observerUrl(), 'agent-hub-sdk-observer', 'popup,width=760,height=560');
});

async function messages() {
  if (!session) throw new Error('Client is not connected');
  return session.messages();
}

async function stop(runId) {
  if (!session) throw new Error('Client is not connected');
  return session.stop(runId);
}

async function listSessionsError() {
  try {
    await client.listSessions();
    return null;
  } catch (error) {
    return {
      name: error?.name ?? 'Error',
      code: error?.code ?? null,
      status: error?.status ?? null,
      message: error instanceof Error ? error.message : String(error)
    };
  }
}

function release() {
  releaseHandler();
  handlerRelease = new Promise((resolve) => { releaseHandler = resolve; });
  state.holdHandler = false;
}

function dispose() {
  for (const subscription of subscriptions) subscription.dispose();
  subscriptions.clear();
  client?.dispose();
}

window.qaSdk = {
  ready: start().catch((error) => {
    state.errors.push({
      type: 'startup',
      name: error?.name ?? 'Error',
      code: error?.code ?? null,
      status: error?.status ?? null,
      message: error instanceof Error ? error.message : String(error)
    });
    updateStatus('Failed');
    return snapshot();
  }),
  snapshot,
  send,
  newDraft,
  subscribe,
  messages,
  stop,
  listSessionsError,
  release,
  dispose
};
