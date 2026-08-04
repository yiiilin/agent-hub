import {
  connect,
  connectAnonymous,
  type AgentHubClient,
  type ClientAgent,
  type ClientSession,
  type SecretGrantRequirement,
  type SessionEvent,
  type SessionMessage,
  type SessionSubscription,
  type SessionSummary,
  isSecretGrantsRequiredError
} from '@agent-hub/client';
import { ArrowUp, Bot, History, Languages, Plus, X } from 'lucide-react';
import { type FormEvent, type TouchEvent, type WheelEvent, useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import { api, type RunEvent } from './api/client';
import { useI18n } from './i18n';
import {
  ChatActivityGroup,
  ChatMessageBubble,
  ChatRunFailure,
  ChatThinkingBubble,
  activityGroupProcessingWindow,
  mergeRunEvents,
  projectActivities,
  projectRunFailures,
  resizeComposer,
  runThinkingStage,
  runProcessingWindow,
  type ActivityEntry,
  type RunFailureEntry
} from './sessions';

const terminalStatuses = new Set(['completed', 'failed', 'cancelled', 'interrupted']);
const historyPageSize = 50;
const bottomThreshold = 24;
const historyThreshold = 64;

type PersistedWidgetState = {
  sessionId: string | null;
  draft: string;
  draftClientMessageKey: string;
};

type SecretGrantPrompt = {
  requirements: SecretGrantRequirement[];
  content: string;
  clientMessageKey: string;
};

type OptimisticMessage = {
  id: string;
  content: string;
  clientMessageKey: string;
  runId: string | null;
  acceptedAt: string;
  failed: boolean;
};

type MessageTimelineEntry = {
  kind: 'message';
  id: string;
  runId: string | null;
  role: string;
  content: string;
  occurredAt: number;
  outputEndedAt?: number;
  sequence: number;
  streaming?: boolean;
  state?: string;
};

type ActivityTimelineEntry = {
  kind: 'activity';
  id: string;
  runId: string;
  occurredAt: number;
  sequence: number;
  activity: ActivityEntry;
};

type FailureTimelineEntry = {
  kind: 'failure';
  id: string;
  runId: string;
  occurredAt: number;
  sequence: number;
  failure: RunFailureEntry;
};

type TimelineItem = MessageTimelineEntry | FailureTimelineEntry | {
  kind: 'activity-group';
  id: string;
  runId: string;
  activities: ActivityEntry[];
};

function randomUuid() {
  if (typeof crypto.randomUUID === 'function') return crypto.randomUUID();
  const bytes = crypto.getRandomValues(new Uint8Array(16));
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = [...bytes].map((byte) => byte.toString(16).padStart(2, '0')).join('');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function widgetChannelId() {
  return `widget-${randomUuid()}`;
}

function inferredHostOrigin() {
  if (window.parent === window) return null;
  const ancestorOrigins = (window.location as Location & { ancestorOrigins?: DOMStringList }).ancestorOrigins;
  const ancestorOrigin = ancestorOrigins?.item(0);
  if (ancestorOrigin) return ancestorOrigin;
  if (!document.referrer) return null;
  try {
    const origin = new URL(document.referrer).origin;
    return origin === 'null' ? null : origin;
  } catch {
    return null;
  }
}

function eventTimestamp(value?: string | null) {
  const timestamp = Date.parse(value ?? '');
  return Number.isFinite(timestamp) ? timestamp : 0;
}

function persistedStateKey(appClientId?: string) {
  return `agent-hub-widget-ui-v2:${appClientId ? encodeURIComponent(appClientId) : 'authenticated'}`;
}

function readPersistedState(key: string): PersistedWidgetState {
  const fallback = { sessionId: null, draft: '', draftClientMessageKey: `msg_${randomUuid()}` };
  try {
    const value = JSON.parse(sessionStorage.getItem(key) ?? 'null') as Partial<PersistedWidgetState> | null;
    if (!value || typeof value !== 'object') return fallback;
    return {
      sessionId: typeof value.sessionId === 'string' ? value.sessionId : null,
      draft: typeof value.draft === 'string' ? value.draft : '',
      draftClientMessageKey: typeof value.draftClientMessageKey === 'string'
        ? value.draftClientMessageKey
        : fallback.draftClientMessageKey
    };
  } catch {
    return fallback;
  }
}

function writePersistedState(key: string, value: PersistedWidgetState) {
  try {
    sessionStorage.setItem(key, JSON.stringify(value));
  } catch {
    // Draft state is best-effort; credentials are never stored here.
  }
}

function rawRunEvent(event: SessionEvent): RunEvent | null {
  const raw = event.raw;
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return null;
  const value = raw as Record<string, unknown>;
  if (typeof value.seq !== 'number'
    || typeof value.run_id !== 'string'
    || typeof value.event_type !== 'string'
    || typeof value.created_at !== 'string') return null;
  return {
    seq: value.seq,
    run_id: value.run_id,
    event_type: value.event_type,
    role: typeof value.role === 'string' ? value.role : null,
    content: typeof value.content === 'string' ? value.content : null,
    payload: value.payload && typeof value.payload === 'object' && !Array.isArray(value.payload)
      ? value.payload as Record<string, unknown>
      : {},
    created_at: value.created_at
  };
}

function mergeMessages(current: SessionMessage[], incoming: SessionMessage[]) {
  const merged = new Map(current.map((message) => [message.id, message]));
  for (const message of incoming) merged.set(message.id, message);
  return [...merged.values()].sort((left, right) => left.sequence - right.sequence);
}

function isTerminalEvent(event: RunEvent) {
  if (event.event_type !== 'status') return false;
  const status = event.content ?? (typeof event.payload.status === 'string' ? event.payload.status : null);
  return status !== null && terminalStatuses.has(status);
}

function historyUpdatedAt(summary: SessionSummary) {
  return typeof summary.updated_at === 'string' ? summary.updated_at : summary.created_at;
}

export function WidgetApp({ token, appClientId }: { token?: string; appClientId?: string }) {
  const { language, locale, setLanguage, t } = useI18n();
  const storageKey = useMemo(() => persistedStateKey(appClientId), [appClientId]);
  const initialState = useMemo(() => readPersistedState(storageKey), [storageKey]);
  const channelId = useMemo(widgetChannelId, []);
  const clientRef = useRef<AgentHubClient | null>(null);
  const sessionRef = useRef<ClientSession | null>(null);
  const subscriptionRef = useRef<SessionSubscription | null>(null);
  const subscriptionSessionIdRef = useRef<string | null>(null);
  const selectedTokenRef = useRef<string | null>(null);
  const connectionGenerationRef = useRef(0);
  const sessionGenerationRef = useRef(0);
  const messageLoadGenerationRef = useRef(0);
  const runPendingRef = useRef(false);
  const mountedRef = useRef(true);
  const initialHostOrigin = useMemo(inferredHostOrigin, []);
  const hostOriginRef = useRef<string | null>(initialHostOrigin);
  const submitFromHostRef = useRef<(content: string) => Promise<void>>(async () => undefined);
  const refreshTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const chatScrollRef = useRef<HTMLDivElement>(null);
  const composerRef = useRef<HTMLTextAreaElement>(null);
  const followBottomRef = useRef(true);
  const historyPagingReadyRef = useRef(false);
  const lastScrollTopRef = useRef(0);
  const historyAnchorRef = useRef<{ scrollHeight: number; scrollTop: number } | null>(null);
  const touchStartYRef = useRef<number | null>(null);
  const [agent, setAgent] = useState<ClientAgent | null>(null);
  const [historyEnabled, setHistoryEnabled] = useState(false);
  const [historyOpen, setHistoryOpen] = useState(false);
  const [historySessions, setHistorySessions] = useState<SessionSummary[]>([]);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [historyError, setHistoryError] = useState(false);
  const [selectedSessionId, setSelectedSessionId] = useState<string | null>(initialState.sessionId);
  const [messages, setMessages] = useState<SessionMessage[]>([]);
  const [events, setEvents] = useState<RunEvent[]>([]);
  const [optimisticMessages, setOptimisticMessages] = useState<OptimisticMessage[]>([]);
  const [hasOlderMessages, setHasOlderMessages] = useState(false);
  const [olderMessagesLoading, setOlderMessagesLoading] = useState(false);
  const [transcriptLoading, setTranscriptLoading] = useState(false);
  const [draft, setDraft] = useState(initialState.draft);
  const draftRef = useRef(initialState.draft);
  const [draftClientMessageKey, setDraftClientMessageKey] = useState(initialState.draftClientMessageKey);
  const [runPending, setRunPending] = useState(false);
  const [streamError, setStreamError] = useState(false);
  const [secretGrantPrompt, setSecretGrantPrompt] = useState<SecretGrantPrompt | null>(null);
  const [grantPending, setGrantPending] = useState(false);
  const grantPendingRef = useRef(false);
  const [ready, setReady] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hostOrigin, setHostOrigin] = useState<string | null>(initialHostOrigin);

  const widgetFetch = useCallback<typeof fetch>((input, init) => {
    const headers = new Headers(input instanceof Request ? input.headers : undefined);
    new Headers(init?.headers).forEach((value, name) => headers.set(name, value));
    const embeddedOrigin = hostOriginRef.current;
    if (embeddedOrigin) headers.set('X-Agent-Hub-Embedded-Origin', embeddedOrigin);
    return fetch(input, { ...init, headers });
  }, []);

  const postWidgetMessage = useCallback((type: string, payload: Record<string, unknown> = {}) => {
    const origin = hostOriginRef.current;
    if (!origin || window.parent === window) return;
    window.parent.postMessage({ type, channelId, protocolVersion: 1, ...payload }, origin);
  }, [channelId]);

  const refreshHistory = useCallback(async (client = clientRef.current) => {
    if (!client || !client.historyEnabled) return;
    setHistoryLoading(true);
    setHistoryError(false);
    try {
      const loaded = await client.listSessions({ limit: 100 });
      if (mountedRef.current && client === clientRef.current) setHistorySessions(loaded);
    } catch {
      if (mountedRef.current && client === clientRef.current) setHistoryError(true);
    } finally {
      if (mountedRef.current && client === clientRef.current) setHistoryLoading(false);
    }
  }, []);

  const loadMessages = useCallback(async (session: ClientSession, initial: boolean) => {
    if (!session.id) return;
    const generation = ++messageLoadGenerationRef.current;
    if (initial) setTranscriptLoading(true);
    try {
      const [page, loadedEvents] = await Promise.all([
        session.messagePage({ limit: historyPageSize }),
        initial ? session.events() : Promise.resolve([])
      ]);
      if (!mountedRef.current || generation !== messageLoadGenerationRef.current || session !== sessionRef.current) return;
      setMessages(page.items.sort((left, right) => left.sequence - right.sequence));
      setEvents((current) => mergeRunEvents(current, loadedEvents.flatMap((event) => {
        const runEvent = rawRunEvent(event);
        return runEvent ? [runEvent] : [];
      })));
      setHasOlderMessages(page.nextBeforeSequence !== null);
      const acceptedKeys = new Set(page.items.flatMap((message) => message.client_message_key ? [message.client_message_key] : []));
      setOptimisticMessages((current) => current.filter((message) => !acceptedKeys.has(message.clientMessageKey)));
    } catch {
      if (mountedRef.current && generation === messageLoadGenerationRef.current && session === sessionRef.current) {
        setError(t('genericError'));
      }
    } finally {
      if (mountedRef.current && generation === messageLoadGenerationRef.current && initial) setTranscriptLoading(false);
    }
  }, [t]);

  const scheduleMessageRefresh = useCallback((session: ClientSession) => {
    if (refreshTimerRef.current) clearTimeout(refreshTimerRef.current);
    refreshTimerRef.current = setTimeout(() => {
      refreshTimerRef.current = null;
      if (session === sessionRef.current) void loadMessages(session, false);
    }, 0);
  }, [loadMessages]);

  const subscribeToSession = useCallback((session: ClientSession) => {
    if (!session.id || subscriptionSessionIdRef.current === session.id) return;
    subscriptionRef.current?.dispose();
    subscriptionSessionIdRef.current = session.id;
    const subscription = session.subscribe((sessionEvent) => {
      if (session !== sessionRef.current) return;
      if (sessionEvent.type === 'error') {
        setStreamError(true);
        setError(t('genericError'));
        return;
      }
      const runEvent = rawRunEvent(sessionEvent);
      if (runEvent) {
        setStreamError(false);
        setError(null);
        setEvents((current) => mergeRunEvents(current, [runEvent]));
        postWidgetMessage('agent-hub:run-event', { runId: runEvent.run_id, event: runEvent });
        if (runEvent.event_type === 'message' || isTerminalEvent(runEvent)) {
          scheduleMessageRefresh(session);
          if (isTerminalEvent(runEvent)) void refreshHistory();
        }
      }
      if (sessionEvent.type === 'timeout') setError(sessionEvent.message);
    });
    subscriptionRef.current = subscription;
    void subscription.closed.catch(() => {
      if (session === sessionRef.current) setError(t('genericError'));
    });
  }, [postWidgetMessage, refreshHistory, scheduleMessageRefresh, t]);

  const activateSession = useCallback(async (session: ClientSession) => {
    sessionGenerationRef.current += 1;
    messageLoadGenerationRef.current += 1;
    subscriptionRef.current?.dispose();
    subscriptionRef.current = null;
    subscriptionSessionIdRef.current = null;
    sessionRef.current = session;
    const sessionId = session.id;
    setSelectedSessionId(sessionId);
    setMessages([]);
    setEvents([]);
    setOptimisticMessages([]);
    setHasOlderMessages(false);
    setError(null);
    setHistoryOpen(false);
    setStreamError(false);
    followBottomRef.current = true;
    historyPagingReadyRef.current = false;
    if (sessionId) {
      await loadMessages(session, true);
      if (session === sessionRef.current) subscribeToSession(session);
    } else {
      setTranscriptLoading(false);
    }
  }, [loadMessages, subscribeToSession]);

  const initializeClient = useCallback(async (nextToken: string | null, anonymousClientId?: string) => {
    if (nextToken && selectedTokenRef.current === nextToken && clientRef.current) return true;
    const generation = ++connectionGenerationRef.current;
    const previousClient = clientRef.current;
    const credentialChanged = previousClient !== null && selectedTokenRef.current !== nextToken;
    let candidateClient: AgentHubClient | null = null;
    setReady(false);
    setError(null);
    try {
      candidateClient = anonymousClientId
        ? await connectAnonymous({
            baseUrl: window.location.origin,
            clientId: anonymousClientId,
            fetch: widgetFetch
          })
        : await connect({
            baseUrl: window.location.origin,
            fetch: widgetFetch,
            authorize: async ({ signal }) => {
              if (!nextToken) throw new Error('Widget credential is required');
              const metadata = await api.widgetAgent(nextToken, signal, hostOriginRef.current ?? undefined);
              return {
                accessToken: nextToken,
                expiresAt: metadata.expires_at ?? Date.now() + 60 * 60_000,
                agent: {
                  id: metadata.id,
                  name: metadata.name,
                  instructions: metadata.instructions
                },
                history_enabled: metadata.history_enabled ?? false
              };
            }
          });
      if (!mountedRef.current || generation !== connectionGenerationRef.current) {
        candidateClient.dispose();
        return false;
      }
      const listed = candidateClient.historyEnabled
        ? await candidateClient.listSessions({ limit: 100 })
        : [];
      if (!mountedRef.current || generation !== connectionGenerationRef.current) {
        candidateClient.dispose();
        return false;
      }
      let session: ClientSession;
      const anonymousSession = candidateClient.currentSession();
      if (anonymousSession) {
        session = anonymousSession;
      } else if (!previousClient && initialState.sessionId
        && (!candidateClient.historyEnabled || listed.some((item) => item.id === initialState.sessionId))) {
        session = candidateClient.existing(initialState.sessionId);
      } else {
        session = listed[0] ? candidateClient.existing(listed[0].id) : candidateClient.draft();
      }

      clientRef.current = candidateClient;
      selectedTokenRef.current = nextToken;
      previousClient?.dispose();
      setAgent(candidateClient.agent);
      setHistoryEnabled(candidateClient.historyEnabled);
      setHistorySessions(listed);
      if (credentialChanged) {
        draftRef.current = '';
        setDraft('');
        setDraftClientMessageKey(`msg_${randomUuid()}`);
      }
      candidateClient = null;
      await activateSession(session);
      const initialized = mountedRef.current && generation === connectionGenerationRef.current;
      if (initialized) setReady(true);
      return initialized;
    } catch (cause) {
      candidateClient?.dispose();
      console.error(cause);
      if (mountedRef.current && generation === connectionGenerationRef.current) {
        setReady(clientRef.current !== null);
        setError(t('genericError'));
      }
      return false;
    }
  }, [activateSession, initialState.sessionId, t, widgetFetch]);

  const startRun = useCallback(async (requestedContent: string, requestedMessageKey?: string) => {
    const session = sessionRef.current;
    const content = requestedContent.trim();
    if (!session || !content || runPendingRef.current || !ready) return;
    const messageKey = requestedMessageKey ?? draftClientMessageKey;
    const optimistic: OptimisticMessage = {
      id: `optimistic-${messageKey}`,
      content,
      clientMessageKey: messageKey,
      runId: null,
      acceptedAt: new Date().toISOString(),
      failed: false
    };
    runPendingRef.current = true;
    setRunPending(true);
    setStreamError(false);
    setError(null);
    setSecretGrantPrompt(null);
    setOptimisticMessages((current) => [...current.filter((item) => item.clientMessageKey !== messageKey), optimistic]);
    try {
      const result = await session.send(content, { clientMessageKey: messageKey });
      if (session !== sessionRef.current) return;
      setSelectedSessionId(result.sessionId);
      setOptimisticMessages((current) => current.map((item) => item.clientMessageKey === messageKey
        ? { ...item, runId: result.run.id }
        : item));
      subscribeToSession(session);
      if (draftRef.current.trim() === content) {
        draftRef.current = '';
        setDraft('');
        setDraftClientMessageKey(`msg_${randomUuid()}`);
      }
      await loadMessages(session, false);
      void refreshHistory();
      postWidgetMessage('agent-hub:run-started', { runId: result.run.id, sessionId: result.sessionId });
    } catch (error) {
      if (session === sessionRef.current) {
        if (isSecretGrantsRequiredError(error) && selectedTokenRef.current !== null) {
          setOptimisticMessages((current) => current.filter((item) => item.clientMessageKey !== messageKey));
          setSecretGrantPrompt({ requirements: error.requirements, content, clientMessageKey: messageKey });
          return;
        }
        setOptimisticMessages((current) => current.map((item) => item.clientMessageKey === messageKey
          ? { ...item, failed: true }
          : item));
        setError(t('genericError'));
      }
    } finally {
      runPendingRef.current = false;
      if (mountedRef.current) setRunPending(false);
    }
  }, [draft, draftClientMessageKey, loadMessages, postWidgetMessage, ready, refreshHistory, subscribeToSession, t]);

  const allowSecretGrant = useCallback(async () => {
    const prompt = secretGrantPrompt;
    if (!prompt || grantPendingRef.current) return;
    const session = sessionRef.current;
    const agentId = agent?.id ?? clientRef.current?.agent?.id;
    if (!session || !agentId) return;
    grantPendingRef.current = true;
    setGrantPending(true);
    try {
      const token = selectedTokenRef.current;
      if (!token) return;
      const response = await fetch('/api/secret-grants', {
        method: 'POST',
        credentials: 'include',
        headers: {
          'Content-Type': 'application/json',
          Authorization: 'Bearer ' + token
        },
        body: JSON.stringify({
          agent_id: agentId,
          secret_names: prompt.requirements.map((requirement) => requirement.name)
        })
      });
      if (!response.ok) throw new Error('Secret grant request failed');
      if (session !== sessionRef.current) return;
      setSecretGrantPrompt(null);
      await startRun(prompt.content, prompt.clientMessageKey);
    } catch (cause) {
      console.error(cause);
      if (session === sessionRef.current) setError(t('secretGrantFailed'));
    } finally {
      grantPendingRef.current = false;
      if (mountedRef.current) setGrantPending(false);
    }
  }, [agent?.id, secretGrantPrompt, startRun, t]);

  const cancelSecretGrant = useCallback(() => {
    setSecretGrantPrompt(null);
  }, []);

  submitFromHostRef.current = startRun;

  useEffect(() => {
    mountedRef.current = true;
    if (appClientId) void initializeClient(null, appClientId);
    else if (token) void initializeClient(token);
    else if (window.parent !== window) window.parent.postMessage({ type: 'agent-hub:ready', channelId, protocolVersion: 1 }, '*');
    return () => {
      mountedRef.current = false;
      connectionGenerationRef.current += 1;
      subscriptionRef.current?.dispose();
      clientRef.current?.dispose();
      if (refreshTimerRef.current) clearTimeout(refreshTimerRef.current);
    };
  }, [appClientId, channelId, initializeClient, token]);

  useEffect(() => {
    const onMessage = async (event: MessageEvent) => {
      if (event.source !== window.parent || !event.data || typeof event.data !== 'object') return;
      if (event.data.channelId !== channelId || typeof event.data.type !== 'string') return;
      if (!hostOriginRef.current) {
        if (event.data.type !== 'agent-hub:init') return;
        hostOriginRef.current = event.origin;
        setHostOrigin(event.origin);
      } else if (event.origin !== hostOriginRef.current) {
        return;
      }
      if ((event.data.type === 'agent-hub:init' || event.data.type === 'agent-hub:session-select')
        && typeof event.data.token === 'string') {
        const sessionReady = await initializeClient(event.data.token);
        postWidgetMessage('agent-hub:ready', { bound: true, sessionReady });
        return;
      }
      if ((event.data.type === 'agent-hub:init' || event.data.type === 'agent-hub:embed-jwt')
        && typeof event.data.jwt === 'string') {
        try {
          const exchanged = await api.exchangeEmbedJwt(event.data.jwt);
          const sessionReady = await initializeClient(exchanged.token);
          postWidgetMessage('agent-hub:ready', { bound: true, sessionReady });
        } catch {
          setError(t('genericError'));
        }
        return;
      }
      if (event.data.type === 'agent-hub:message-submit') {
        const content = typeof event.data.message === 'string' ? event.data.message : draft;
        await submitFromHostRef.current(content);
      }
    };
    window.addEventListener('message', onMessage);
    return () => window.removeEventListener('message', onMessage);
  }, [channelId, draft, initializeClient, postWidgetMessage, t]);

  useEffect(() => {
    writePersistedState(storageKey, {
      sessionId: selectedSessionId,
      draft,
      draftClientMessageKey
    });
  }, [draft, draftClientMessageKey, selectedSessionId, storageKey]);

  useEffect(() => {
    resizeComposer(composerRef.current);
  }, [draft]);

  const loadOlderMessages = useCallback(async () => {
    const session = sessionRef.current;
    const beforeSequence = messages[0]?.sequence;
    if (!session?.id || beforeSequence === undefined || !hasOlderMessages || olderMessagesLoading) return;
    setOlderMessagesLoading(true);
    const generation = messageLoadGenerationRef.current;
    try {
      const page = await session.messagePage({ beforeSequence, limit: historyPageSize });
      if (!mountedRef.current || generation !== messageLoadGenerationRef.current || session !== sessionRef.current) return;
      const scroll = chatScrollRef.current;
      if (scroll) historyAnchorRef.current = { scrollHeight: scroll.scrollHeight, scrollTop: scroll.scrollTop };
      setMessages((current) => mergeMessages(current, page.items));
      setHasOlderMessages(page.nextBeforeSequence !== null);
    } catch {
      if (session === sessionRef.current) setError(t('genericError'));
    } finally {
      if (mountedRef.current) setOlderMessagesLoading(false);
    }
  }, [hasOlderMessages, messages, olderMessagesLoading, t]);

  const requestOlderMessages = useCallback(() => {
    if (!historyPagingReadyRef.current || !hasOlderMessages || olderMessagesLoading) return;
    followBottomRef.current = false;
    void loadOlderMessages();
  }, [hasOlderMessages, loadOlderMessages, olderMessagesLoading]);

  const handleScroll = useCallback(() => {
    const scroll = chatScrollRef.current;
    if (!scroll) return;
    const scrollingUp = scroll.scrollTop < lastScrollTopRef.current - 1;
    lastScrollTopRef.current = scroll.scrollTop;
    followBottomRef.current = scroll.scrollHeight - scroll.clientHeight - scroll.scrollTop <= bottomThreshold;
    if (scrollingUp && scroll.scrollTop <= historyThreshold) requestOlderMessages();
  }, [requestOlderMessages]);

  const handleWheel = useCallback((event: WheelEvent<HTMLDivElement>) => {
    if (event.deltaY < 0 && (chatScrollRef.current?.scrollTop ?? 0) <= historyThreshold) requestOlderMessages();
  }, [requestOlderMessages]);

  const handleTouchStart = useCallback((event: TouchEvent<HTMLDivElement>) => {
    touchStartYRef.current = event.touches[0]?.clientY ?? null;
  }, []);

  const handleTouchEnd = useCallback((event: TouchEvent<HTMLDivElement>) => {
    const start = touchStartYRef.current;
    touchStartYRef.current = null;
    const end = event.changedTouches[0]?.clientY;
    if (start !== null && end !== undefined && end > start + 12
      && (chatScrollRef.current?.scrollTop ?? 0) <= historyThreshold) requestOlderMessages();
  }, [requestOlderMessages]);

  const timeline = useMemo(() => {
    const entries: Array<MessageTimelineEntry | ActivityTimelineEntry | FailureTimelineEntry> = messages
      .filter((message) => Boolean(message.content) && (message.role === 'user' || message.role === 'assistant'))
      .map((message) => ({
        kind: 'message' as const,
        id: message.id,
        runId: message.run_id ?? null,
        role: message.role,
        content: message.content!,
        occurredAt: eventTimestamp(message.accepted_at),
        sequence: message.sequence * 1000,
        state: message.delivery_state
      }));
    const acceptedKeys = new Set(messages.flatMap((message) => message.client_message_key ? [message.client_message_key] : []));
    for (const message of optimisticMessages) {
      if (acceptedKeys.has(message.clientMessageKey)) continue;
      entries.push({
        kind: 'message',
        id: message.id,
        runId: message.runId,
        role: 'user',
        content: message.content,
        occurredAt: eventTimestamp(message.acceptedAt),
        sequence: Number.MAX_SAFE_INTEGER - 2,
        state: message.failed ? 'failed' : 'queued'
      });
    }
    const messageKeys = new Set(entries.flatMap((entry) => entry.kind === 'message'
      ? [`${entry.runId ?? ''}:${entry.role}:${entry.content}`]
      : []));
    for (const event of events) {
      const key = `${event.run_id}:${event.role ?? 'assistant'}:${event.content ?? ''}`;
      if (event.event_type === 'message' && event.content && !messageKeys.has(key)) {
        entries.push({
          kind: 'message',
          id: `event-message-${event.run_id}-${event.seq}`,
          runId: event.run_id,
          role: event.role ?? 'assistant',
          content: event.content,
          occurredAt: eventTimestamp(event.created_at),
          sequence: event.seq * 1000 + 1
        });
        messageKeys.add(key);
      }
    }
    const completedRuns = new Set(entries.flatMap((entry) => entry.kind === 'message' && entry.role === 'assistant' && entry.runId
      ? [entry.runId]
      : []));
    const deltasByRun = new Map<string, RunEvent[]>();
    for (const event of events) {
      if (event.event_type !== 'message_delta' || event.role !== 'assistant' || !event.content || completedRuns.has(event.run_id)) continue;
      deltasByRun.set(event.run_id, [...(deltasByRun.get(event.run_id) ?? []), event]);
    }
    for (const [runId, deltas] of deltasByRun) {
      deltas.sort((left, right) => left.seq - right.seq);
      entries.push({
        kind: 'message',
        id: `streaming-message-${runId}`,
        runId,
        role: 'assistant',
        content: deltas.map((event) => event.content).join(''),
        occurredAt: eventTimestamp(deltas[0]?.created_at),
        outputEndedAt: eventTimestamp(deltas.at(-1)?.created_at),
        sequence: (deltas[0]?.seq ?? 0) * 1000 + 1,
        streaming: true
      });
    }
    for (const activity of projectActivities(events)) {
      entries.push({
        kind: 'activity',
        id: activity.id,
        runId: activity.runId,
        occurredAt: activity.occurredAt,
        sequence: activity.sequence * 1000 + 2,
        activity
      });
    }
    for (const failure of projectRunFailures(events)) {
      entries.push({
        kind: 'failure',
        id: failure.id,
        runId: failure.runId,
        occurredAt: failure.occurredAt,
        sequence: failure.sequence * 1000 + 3,
        failure
      });
    }
    entries.sort((left, right) => left.occurredAt - right.occurredAt || left.sequence - right.sequence);
    return entries.reduce<TimelineItem[]>((items, entry) => {
      if (entry.kind !== 'activity') {
        items.push(entry);
        return items;
      }
      const previous = items.at(-1);
      if (previous?.kind === 'activity-group' && previous.runId === entry.runId) previous.activities.push(entry.activity);
      else items.push({ kind: 'activity-group', id: `activity-${entry.id}`, runId: entry.runId, activities: [entry.activity] });
      return items;
    }, []);
  }, [events, messages, optimisticMessages]);

  const runWindows = useMemo(() => {
    const runIds = new Set(timeline.flatMap((entry) => entry.kind === 'message' && entry.runId ? [entry.runId] : entry.kind === 'activity-group' ? [entry.runId] : []));
    return new Map([...runIds].map((runId) => {
      const accepted = timeline.flatMap((entry) => entry.kind === 'message' && entry.runId === runId && entry.role === 'user'
        ? [entry.occurredAt]
        : []).filter((value) => value > 0);
      return [runId, runProcessingWindow(
        events.filter((event) => event.run_id === runId),
        accepted.length > 0 ? Math.min(...accepted) : undefined
      )] as const;
    }));
  }, [events, timeline]);
  const clockOffset = useMemo(() => {
    const timestamps = [
      ...events.map((event) => eventTimestamp(event.created_at)),
      ...messages.map((message) => eventTimestamp(message.accepted_at)),
      ...optimisticMessages.map((message) => eventTimestamp(message.acceptedAt))
    ].filter((timestamp) => timestamp > 0);
    return timestamps.length > 0 ? Date.now() - Math.max(...timestamps) : undefined;
  }, [events, messages, optimisticMessages]);

  const lastUserRunId = [...timeline].reverse().find((entry) => entry.kind === 'message' && entry.role === 'user')?.runId ?? null;
  const lastRunTerminal = lastUserRunId ? events.some((event) => event.run_id === lastUserRunId && isTerminalEvent(event)) : false;
  const activeRunInProgress = !streamError && (runPending || Boolean(lastUserRunId && !lastRunTerminal));
  const activeRunLastTimelineItem = lastUserRunId
    ? [...timeline].reverse().find((entry) => entry.runId === lastUserRunId)
    : undefined;
  const showThinking = activeRunInProgress && activeRunLastTimelineItem?.kind !== 'activity-group';
  const activeThinking = useMemo(() => {
    if (!lastUserRunId) return undefined;
    const runEvents = events.filter((event) => event.run_id === lastUserRunId);
    const lastEventAt = runEvents.reduce((max, event) => Math.max(max, eventTimestamp(event.created_at)), 0) || undefined;
    return { stage: runThinkingStage(runEvents), lastEventAt };
  }, [events, lastUserRunId]);

  useLayoutEffect(() => {
    const scroll = chatScrollRef.current;
    if (!scroll) return;
    const anchor = historyAnchorRef.current;
    if (anchor) {
      scroll.scrollTop = anchor.scrollTop + scroll.scrollHeight - anchor.scrollHeight;
      historyAnchorRef.current = null;
      historyPagingReadyRef.current = true;
    } else if (followBottomRef.current) {
      scroll.scrollTop = scroll.scrollHeight;
    }
    lastScrollTopRef.current = scroll.scrollTop;
    if (!transcriptLoading) historyPagingReadyRef.current = true;
  }, [showThinking, timeline, transcriptLoading]);

  useEffect(() => {
    if (!hostOrigin) return;
    const reportSize = () => postWidgetMessage('agent-hub:resize', {
      width: Math.ceil(document.documentElement.getBoundingClientRect().width),
      height: Math.ceil(document.documentElement.scrollHeight)
    });
    const observer = new ResizeObserver(reportSize);
    observer.observe(document.documentElement);
    reportSize();
    return () => observer.disconnect();
  }, [hostOrigin, postWidgetMessage]);

  async function submit(event: FormEvent) {
    event.preventDefault();
    await startRun(draft);
  }

  function updateDraft(value: string) {
    draftRef.current = value;
    setDraft(value);
    writePersistedState(storageKey, {
      sessionId: selectedSessionId,
      draft: value,
      draftClientMessageKey
    });
  }

  return <div className="widget session-chat">
    <header className="session-detail-header session-chat-header widget-header">
      <div className="session-chat-title"><span className="widget-agent-icon" aria-hidden="true"><Bot size={17} /></span><div><h2>{agent?.name ?? t('agentWidget')}</h2><span>{t('hubNative')}</span></div></div>
      <div className="widget-header-actions">
        {historyEnabled && <button type="button" className="icon-button widget-history-toggle" aria-label={t('widgetHistory')} title={t('widgetHistory')} onClick={() => setHistoryOpen((open) => !open)}><History size={17} /></button>}
        <label className="widget-language-control"><Languages size={15} aria-hidden="true" /><span className="sr-only">{t('language')}</span><select className="widget-language" aria-label={t('language')} value={language} onChange={(event) => setLanguage(event.target.value as 'en' | 'zh-CN')}><option value="en">English</option><option value="zh-CN">简体中文</option></select></label>
      </div>
    </header>
    {historyOpen && <aside className="widget-history" role="dialog" aria-label={t('widgetHistory')}>
      <header><strong>{t('widgetHistory')}</strong><button type="button" className="icon-button" aria-label={t('widgetCloseHistory')} title={t('widgetCloseHistory')} onClick={() => setHistoryOpen(false)}><X size={16} /></button></header>
      <button type="button" className="secondary widget-history-new" onClick={() => { const client = clientRef.current; if (client) void activateSession(client.draft()); }}><Plus size={15} /> {t('newConversation')}</button>
      {historyLoading && <div className="widget-history-state">{t('loadingMessages')}</div>}
      {historyError && <div className="widget-history-state error">{t('widgetHistoryLoadFailed')} <button type="button" className="text-button" onClick={() => void refreshHistory()}>{t('retry')}</button></div>}
      {!historyLoading && !historyError && historySessions.length === 0 && <div className="widget-history-state">{t('widgetNoHistory')}</div>}
      {!historyLoading && !historyError && historySessions.length > 0 && <div className="widget-history-list">{historySessions.map((item) => <button type="button" key={item.id} className={`widget-history-item ${item.id === selectedSessionId ? 'selected' : ''}`} onClick={() => { const client = clientRef.current; if (client) void activateSession(client.existing(item.id)); }}><span>{item.preview || t('newConversation')}</span>{historyUpdatedAt(item) && <time>{new Date(historyUpdatedAt(item)!).toLocaleString(locale)}</time>}</button>)}</div>}
    </aside>}
    <div className="session-chat-scroll" ref={chatScrollRef} onScroll={handleScroll} onWheel={handleWheel} onTouchStart={handleTouchStart} onTouchEnd={handleTouchEnd}>
      {secretGrantPrompt && <div className="session-banner" role="alert">
        <strong>{t('secretGrantRequired')}</strong>
        <span>{t('secretGrantRequiredHelp')}</span>
        <ul>
          {secretGrantPrompt.requirements.map((requirement) => (
            <li key={requirement.name}><strong>{requirement.name}</strong>{requirement.description ? ` — ${requirement.description}` : null}</li>
          ))}
        </ul>
        <div className="widget-secret-grant-actions">
          <button type="button" className="primary" disabled={grantPending} onClick={() => void allowSecretGrant()}>{t('allowSecretGrant')}</button>
          <button type="button" className="secondary" disabled={grantPending} onClick={cancelSecretGrant}>{t('cancel')}</button>
        </div>
      </div>}
      {error && <div className="session-banner error" role="alert">{error}</div>}
      <div className="session-transcript widget-transcript" aria-live="polite" aria-busy={transcriptLoading || olderMessagesLoading}>
        {transcriptLoading && timeline.length === 0 && <div className="widget-transcript-state">{t('loadingMessages')}</div>}
        {timeline.map((entry, index) => {
          if (entry.kind === 'activity-group') {
            const reasoningText = entry.activities.every((activity) => activity.kind === 'reasoning')
              ? entry.activities.map((activity) => activity.summary).filter(Boolean).join('\\n')
              : '';
            if (reasoningText) {
              return <div className="session-reasoning-text" key={entry.id}>{reasoningText}</div>;
            }
            const active = activeRunInProgress && entry.runId === lastUserRunId;
            const window = activityGroupProcessingWindow(timeline, index, runWindows.get(entry.runId), active);
            return <ChatActivityGroup active={active && window.endedAt === undefined} activities={entry.activities} clockOffset={clockOffset} endedAt={window.endedAt} key={entry.id} startedAt={window.startedAt} />;
          }
          if (entry.kind === 'failure') return <ChatRunFailure failure={entry.failure} key={entry.id} />;
          return <ChatMessageBubble agentName={agent?.name ?? null} content={entry.content} key={entry.id} role={entry.role} state={entry.state} streaming={entry.streaming} />;
        })}
        {showThinking && <ChatThinkingBubble stage={activeThinking?.stage.key ?? 'runStageThinking'} detail={activeThinking?.stage.detail} lastEventAt={activeThinking?.lastEventAt} />}
      </div>
    </div>
    <form className="session-composer session-chat-composer widget-composer" onSubmit={submit}>
      <label><span className="sr-only">{t('message')}</span><textarea ref={composerRef} rows={2} aria-label={t('message')} value={draft} onChange={(event) => updateDraft(event.target.value)} onInput={(event) => resizeComposer(event.currentTarget)} onKeyDown={(event) => {
        if (event.key !== 'Enter' || event.shiftKey || event.nativeEvent.isComposing) return;
        event.preventDefault();
        event.currentTarget.form?.requestSubmit();
      }} placeholder={t('messagePlaceholder')} /></label>
      <div><span className="session-composer-actions"><button type="submit" className="icon-button session-send-button" aria-label={runPending ? t('sending') : t('send')} title={t('send')} disabled={!ready || runPending || !draft.trim()}><ArrowUp size={18} /></button></span></div>
    </form>
  </div>;
}
