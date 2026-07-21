import { ArrowUp, Bot, Brain, ChevronDown, ChevronRight, FilePenLine, ImageIcon, ListChecks, Minimize2, PanelLeft, Plus, Search, Square, Terminal, Users, Wrench, X } from 'lucide-react';
import { FormEvent, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { api, type Agent, type HubSession, type HubSessionMessage, type RunEvent } from './api/client';
import { FormDialog } from './components/form-dialog';
import { useI18n } from './i18n';
import type { TranslationKey } from './i18n';

const lifecycleKeys: Record<string, TranslationKey> = {
  waiting_for_runtime: 'sessionStatusWaitingRuntime',
  restoring: 'sessionStatusRestoring',
  online: 'statusOnline',
  saving: 'sessionStatusSaving',
  offline: 'statusOffline',
  recovery_failed: 'sessionStatusRecoveryFailed',
  historical: 'historicalSession'
};

const deliveryKeys: Record<string, TranslationKey> = {
  queued: 'messageStateQueued',
  deferred: 'messageStateDeferred',
  delivering: 'messageStateDelivering',
  delivered: 'messageStateDelivered',
  failed: 'statusFailed'
};

const terminalRunStatuses = new Set(['completed', 'failed', 'cancelled', 'interrupted']);

type ActivityKind = 'reasoning' | 'command' | 'file' | 'tool' | 'search' | 'plan' | 'image' | 'subagent' | 'compaction' | 'review' | 'wait';

type ActivityEntry = {
  id: string;
  runId: string;
  itemId: string | null;
  kind: ActivityKind;
  phase: string;
  sequence: number;
  occurredAt: number;
  endedAt: number;
  summary: string | null;
  output: string | null;
};

type TimelineEntry =
  | { kind: 'message'; id: string; sequence: number; occurredAt: number; runId: string | null; role: string; content: string; state?: string; mode?: string }
  | { kind: 'live'; id: string; sequence: number; occurredAt: number; role: string; content: string }
  | { kind: 'activity'; id: string; sequence: number; occurredAt: number; activity: ActivityEntry };

type TimelineItem = Exclude<TimelineEntry, { kind: 'activity' }>
  | { kind: 'activity-group'; id: string; runId: string; activities: ActivityEntry[] };

type ConversationDraft = {
  agentId: string;
  agentName: string;
};

function eventTimestamp(value: string) {
  const timestamp = Date.parse(value);
  return Number.isFinite(timestamp) ? timestamp : 0;
}

const activityKeys: Record<ActivityKind, TranslationKey> = {
  reasoning: 'activityReasoning',
  command: 'activityCommand',
  file: 'activityFileChange',
  tool: 'activityTool',
  search: 'activityWebSearch',
  plan: 'activityPlan',
  image: 'activityImage',
  subagent: 'activitySubagent',
  compaction: 'activityCompaction',
  review: 'activityReview',
  wait: 'activityWait'
};

function formatActivityDuration(activities: ActivityEntry[], locale: string) {
  const milliseconds = Math.max(
    0,
    Math.max(...activities.map((activity) => activity.endedAt))
      - Math.min(...activities.map((activity) => activity.occurredAt))
  );
  const seconds = milliseconds / 1000;
  if (seconds < 60) {
    return new Intl.NumberFormat(locale, {
      style: 'unit',
      unit: 'second',
      unitDisplay: 'short',
      maximumFractionDigits: seconds < 10 ? 1 : 0
    }).format(seconds);
  }
  const minutes = Math.floor(seconds / 60);
  const remainingSeconds = Math.round(seconds % 60);
  const minuteLabel = new Intl.NumberFormat(locale, { style: 'unit', unit: 'minute', unitDisplay: 'short' }).format(minutes);
  if (remainingSeconds === 0) return minuteLabel;
  const secondLabel = new Intl.NumberFormat(locale, { style: 'unit', unit: 'second', unitDisplay: 'short' }).format(remainingSeconds);
  return `${minuteLabel} ${secondLabel}`;
}

function mergeRunEvents(current: RunEvent[], incoming: RunEvent[]) {
  const merged = new Map(current.map((event) => [`${event.run_id}:${event.seq}`, event]));
  for (const event of incoming) merged.set(`${event.run_id}:${event.seq}`, event);
  return [...merged.values()].sort((left, right) => (
    eventTimestamp(left.created_at) - eventTimestamp(right.created_at)
    || left.run_id.localeCompare(right.run_id)
    || left.seq - right.seq
  ));
}

function payloadString(payload: Record<string, unknown>, key: string) {
  return typeof payload[key] === 'string' ? payload[key] as string : null;
}

function payloadNumber(payload: Record<string, unknown>, key: string) {
  return typeof payload[key] === 'number' && Number.isFinite(payload[key]) ? payload[key] as number : null;
}

function payloadTextList(payload: Record<string, unknown>, key: string) {
  const value = payload[key];
  if (typeof value === 'string') return value;
  if (!Array.isArray(value)) return null;
  const parts = value.filter((part): part is string => typeof part === 'string' && Boolean(part));
  return parts.length > 0 ? parts.join('\n') : null;
}

function fileChangeSummary(payload: Record<string, unknown>) {
  if (!Array.isArray(payload.changes)) return null;
  const paths = payload.changes.flatMap((change) => {
    if (!change || typeof change !== 'object') return [];
    const path = (change as Record<string, unknown>).path;
    return typeof path === 'string' ? [path] : [];
  });
  return paths.length > 0 ? paths.join('\n') : null;
}

function activityKind(itemType: string): ActivityKind | null {
  if (itemType === 'reasoning') return 'reasoning';
  if (itemType === 'commandExecution') return 'command';
  if (itemType === 'fileChange') return 'file';
  if (itemType === 'mcpToolCall' || itemType === 'dynamicToolCall') return 'tool';
  if (itemType === 'collabAgentToolCall' || itemType === 'subAgentActivity') return 'subagent';
  if (itemType === 'webSearch') return 'search';
  if (itemType === 'plan') return 'plan';
  if (itemType === 'imageView' || itemType === 'imageGeneration') return 'image';
  if (itemType === 'contextCompaction') return 'compaction';
  if (itemType === 'enteredReviewMode' || itemType === 'exitedReviewMode') return 'review';
  if (itemType === 'sleep') return 'wait';
  return null;
}

function activityFromEvent(event: RunEvent): ActivityEntry | null {
  if (event.event_type === 'tool_request' || event.event_type === 'tool_result') {
    const itemId = payloadString(event.payload, 'tool_request_id') ?? payloadString(event.payload, 'source_id');
    return {
      id: `activity-${event.run_id}-${itemId ?? event.seq}`,
      runId: event.run_id,
      itemId,
      kind: 'tool',
      phase: 'completed',
      sequence: event.seq,
      occurredAt: eventTimestamp(event.created_at),
      endedAt: eventTimestamp(event.created_at),
      summary: payloadString(event.payload, 'tool_name'),
      output: null
    };
  }
  if (event.event_type !== 'item') return null;
  const itemType = payloadString(event.payload, 'item_type');
  const kind = itemType ? activityKind(itemType) : null;
  if (!itemType || !kind) return null;
  const phase = payloadString(event.payload, 'phase') ?? 'completed';
  const itemId = payloadString(event.payload, 'item_id');
  const duration = payloadNumber(event.payload, 'duration_ms') ?? 0;
  const endedAt = eventTimestamp(event.created_at);
  let summary: string | null = null;
  if (kind === 'reasoning') summary = payloadTextList(event.payload, 'summary');
  else if (kind === 'command') summary = payloadString(event.payload, 'command');
  else if (kind === 'file') summary = fileChangeSummary(event.payload);
  else if (itemType === 'mcpToolCall') {
    const server = payloadString(event.payload, 'server');
    const tool = payloadString(event.payload, 'tool');
    summary = [server, tool].filter(Boolean).join(' / ') || null;
  } else if (kind === 'tool') {
    const namespace = payloadString(event.payload, 'namespace');
    const tool = payloadString(event.payload, 'tool');
    summary = [namespace, tool].filter(Boolean).join(' / ') || null;
  } else if (kind === 'subagent') {
    summary = payloadString(event.payload, 'tool')
      ?? payloadString(event.payload, 'kind')
      ?? payloadString(event.payload, 'agent_path');
  } else if (kind === 'search') summary = payloadString(event.payload, 'query');
  else if (kind === 'plan') summary = payloadString(event.payload, 'text');
  else if (kind === 'image') summary = payloadString(event.payload, 'path');
  return {
    id: `activity-${event.run_id}-${itemId ?? event.seq}`,
    runId: event.run_id,
    itemId,
    kind,
    phase,
    sequence: event.seq,
    occurredAt: Math.max(0, endedAt - duration),
    endedAt,
    summary,
    output: payloadString(event.payload, 'output')
  };
}

function mergeActivity(current: ActivityEntry, incoming: ActivityEntry): ActivityEntry {
  const summary = incoming.phase === 'summary_delta'
    ? `${current.summary ?? ''}${incoming.summary ?? ''}` || null
    : incoming.summary ?? current.summary;
  const output = incoming.phase === 'output_delta'
    ? `${current.output ?? ''}${incoming.output ?? ''}` || null
    : incoming.output ?? current.output;
  return {
    ...current,
    kind: incoming.kind,
    phase: incoming.phase,
    occurredAt: Math.min(current.occurredAt, incoming.occurredAt),
    endedAt: Math.max(current.endedAt, incoming.endedAt),
    sequence: Math.min(current.sequence, incoming.sequence),
    summary,
    output
  };
}

function projectActivities(events: RunEvent[]) {
  const activities = new Map<string, ActivityEntry>();
  for (const event of events) {
    const activity = activityFromEvent(event);
    if (!activity) continue;
    const key = activity.itemId
      ? `${activity.runId}:${activity.itemId}`
      : `${activity.runId}:${activity.sequence}`;
    const current = activities.get(key);
    activities.set(key, current ? mergeActivity(current, activity) : activity);
  }
  return [...activities.values()].sort((left, right) => (
    left.occurredAt - right.occurredAt || left.sequence - right.sequence
  ));
}

function resizeComposer(textarea: HTMLTextAreaElement | null) {
  if (!textarea) return;
  const style = window.getComputedStyle(textarea);
  const lineHeight = Number.parseFloat(style.lineHeight);
  const verticalChrome = Number.parseFloat(style.paddingTop) + Number.parseFloat(style.paddingBottom)
    + Number.parseFloat(style.borderTopWidth) + Number.parseFloat(style.borderBottomWidth);
  if (!Number.isFinite(lineHeight) || !Number.isFinite(verticalChrome)) return;
  const minimum = lineHeight * 2 + verticalChrome;
  const maximum = lineHeight * 5 + verticalChrome;
  textarea.style.height = 'auto';
  const contentHeight = textarea.scrollHeight;
  textarea.style.height = `${Math.min(maximum, Math.max(minimum, contentHeight))}px`;
  textarea.style.overflowY = contentHeight > maximum ? 'auto' : 'hidden';
}

function ActivityIcon({ kind }: { kind: ActivityKind }) {
  if (kind === 'reasoning') return <Brain size={15} />;
  if (kind === 'command') return <Terminal size={15} />;
  if (kind === 'file') return <FilePenLine size={15} />;
  if (kind === 'tool') return <Wrench size={15} />;
  if (kind === 'search') return <Search size={15} />;
  if (kind === 'plan') return <ListChecks size={15} />;
  if (kind === 'image') return <ImageIcon size={15} />;
  if (kind === 'subagent') return <Users size={15} />;
  return <Minimize2 size={15} />;
}

function eventRefreshesSession(event: RunEvent) {
  if (event.event_type === 'turn_started') return true;
  if (event.event_type !== 'status') return false;
  const status = event.content ?? (typeof event.payload.status === 'string' ? event.payload.status : null);
  return status !== null && terminalRunStatuses.has(status);
}

function NewConversationDialog({ agents, onClose, onSelected }: {
  agents: Agent[];
  onClose: () => void;
  onSelected: (draft: ConversationDraft) => void;
}) {
  const { t } = useI18n();
  const availableAgents = agents.filter((agent) => agent.can_invoke);
  const [agentId, setAgentId] = useState(availableAgents[0]?.id ?? '');
  const agentRef = useRef<HTMLSelectElement>(null);

  function submit(event: FormEvent) {
    event.preventDefault();
    const agent = availableAgents.find((candidate) => candidate.id === agentId);
    if (!agent) return;
    onSelected({ agentId: agent.id, agentName: agent.name });
  }

  return <FormDialog
    title={t('newConversation')}
    eyebrow={t('sessions')}
    onClose={onClose}
    initialFocusRef={agentRef}
    className="session-create-dialog"
    footer={<><button className="secondary" type="button" onClick={onClose}>{t('cancel')}</button><button className="primary" form="new-conversation-form" type="submit" disabled={!agentId}>{t('startConversation')}</button></>}
  >
    <form id="new-conversation-form" className="stack" onSubmit={submit}>
      <label>{t('agent')}<select ref={agentRef} aria-label={t('agent')} required value={agentId} onChange={(event) => setAgentId(event.target.value)}>{availableAgents.map((agent) => <option key={agent.id} value={agent.id}>{agent.name}</option>)}</select></label>
      {availableAgents.length === 0 && <div className="warning" role="alert">{t('noInvocableAgents')}</div>}
    </form>
  </FormDialog>;
}

export function SessionsPage() {
  const { locale, t } = useI18n();
  const mountedRef = useRef(true);
  const sessionLoadGeneration = useRef(0);
  const messageLoadGeneration = useRef(0);
  const eventLoadGeneration = useRef(0);
  const streamGeneration = useRef(0);
  const sessionRefreshGeneration = useRef(0);
  const conversationDraftGeneration = useRef(0);
  const composerRef = useRef<HTMLTextAreaElement>(null);
  const [sessions, setSessions] = useState<HubSession[]>([]);
  const [agents, setAgents] = useState<Agent[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [conversationDraft, setConversationDraft] = useState<ConversationDraft | null>(null);
  const [messages, setMessages] = useState<HubSessionMessage[]>([]);
  const [events, setEvents] = useState<RunEvent[]>([]);
  const [search, setSearch] = useState('');
  const [originFilter, setOriginFilter] = useState<'all' | 'hub_native' | 'external'>('all');
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState(false);
  const [messagesLoading, setMessagesLoading] = useState(false);
  const [messagesError, setMessagesError] = useState(false);
  const [draft, setDraft] = useState('');
  const [sending, setSending] = useState(false);
  const [stopping, setStopping] = useState(false);
  const [stopRequestedRunId, setStopRequestedRunId] = useState<string | null>(null);
  const [actionError, setActionError] = useState(false);
  const [conversationCreateError, setConversationCreateError] = useState(false);
  const [createOpen, setCreateOpen] = useState(false);
  const [sessionListOpen, setSessionListOpen] = useState(false);

  const loadSessions = useCallback(async (
    preferredId?: string,
    shouldSelectPreferred?: () => boolean
  ) => {
    const generation = ++sessionLoadGeneration.current;
    const controller = new AbortController();
    setLoading(true);
    setLoadError(false);
    try {
      const [loadedSessions, loadedAgents] = await Promise.all([
        api.sessions(controller.signal),
        api.agents(controller.signal).catch((error) => {
          if ((error as Error)?.name === 'AbortError') throw error;
          return [];
        })
      ]);
      if (!mountedRef.current || generation !== sessionLoadGeneration.current) return;
      setSessions(loadedSessions);
      setAgents(loadedAgents);
      setSelectedId((current) => {
        const requested = preferredId !== undefined
          && (!shouldSelectPreferred || shouldSelectPreferred())
          ? preferredId
          : current;
        return requested && loadedSessions.some((session) => session.id === requested)
          ? requested
          : loadedSessions[0]?.id ?? null;
      });
    } catch (error) {
      if (mountedRef.current && generation === sessionLoadGeneration.current && (error as Error)?.name !== 'AbortError') setLoadError(true);
    } finally {
      if (mountedRef.current && generation === sessionLoadGeneration.current) setLoading(false);
    }
    return () => controller.abort();
  }, []);

  useEffect(() => {
    mountedRef.current = true;
    void loadSessions();
    return () => {
      mountedRef.current = false;
      sessionLoadGeneration.current += 1;
      messageLoadGeneration.current += 1;
      eventLoadGeneration.current += 1;
      streamGeneration.current += 1;
    };
  }, [loadSessions]);

  useEffect(() => {
    const generation = ++messageLoadGeneration.current;
    const controller = new AbortController();
    setMessages([]);
    setEvents([]);
    setActionError(false);
    setConversationCreateError(false);
    setStopRequestedRunId(null);
    if (!selectedId) {
      setMessages([]);
      setMessagesLoading(false);
      return () => controller.abort();
    }
    setMessagesLoading(true);
    setMessagesError(false);
    api.sessionMessages(selectedId, controller.signal).then((response) => {
      if (mountedRef.current && generation === messageLoadGeneration.current) setMessages(response);
    }).catch((error) => {
      if (mountedRef.current && generation === messageLoadGeneration.current && (error as Error)?.name !== 'AbortError') setMessagesError(true);
    }).finally(() => {
      if (mountedRef.current && generation === messageLoadGeneration.current) setMessagesLoading(false);
    });
    return () => controller.abort();
  }, [selectedId]);

  const selectedSession = sessions.find((session) => session.id === selectedId) ?? null;
  const conversationAgentName = conversationDraft?.agentName ?? selectedSession?.agent_name ?? null;
  const sessionMessages = useMemo(
    () => messages.filter((message) => message.session_id === selectedId),
    [messages, selectedId]
  );
  const sessionRunIds = useMemo(
    () => [...new Set(sessionMessages.flatMap((message) => message.run_id ? [message.run_id] : []))],
    [sessionMessages]
  );
  const sessionRunIdsKey = sessionRunIds.join(':');
  const sessionRunIdSet = useMemo(() => new Set(sessionRunIds), [sessionRunIds]);
  const sessionEvents = useMemo(
    () => events.filter((event) => sessionRunIdSet.has(event.run_id)),
    [events, sessionRunIdSet]
  );
  const activeRunId = useMemo(() => [...sessionMessages].reverse().find((message) => message.run_id)?.run_id ?? null, [sessionMessages]);
  const activeRunUserMessage = activeRunId
    ? [...sessionMessages].reverse().find((message) => message.run_id === activeRunId && message.role === 'user') ?? null
    : null;
  const activeRunEvents = activeRunId
    ? sessionEvents.filter((event) => event.run_id === activeRunId)
    : [];
  const activeRunTerminal = activeRunEvents.some((event) => {
    if (event.event_type !== 'status') return false;
    const status = event.content ?? payloadString(event.payload, 'status');
    return status !== null && terminalRunStatuses.has(status);
  });
  const activeRunHasAssistantMessage = activeRunId !== null && (
    sessionMessages.some((message) => message.run_id === activeRunId && message.role === 'assistant' && Boolean(message.content))
    || activeRunEvents.some((event) => event.event_type === 'message' && event.role === 'assistant' && Boolean(event.content))
  );
  const activeRunStarted = activeRunEvents.some((event) => {
    if (event.event_type === 'turn_started') return true;
    if (event.event_type !== 'status') return false;
    const status = event.content ?? payloadString(event.payload, 'status');
    return status !== null && ['pending', 'running', 'waiting_tool'].includes(status);
  });
  const showThinking = activeRunId !== null
    && !activeRunTerminal
    && !activeRunHasAssistantMessage
    && (Boolean(selectedSession?.active_turn_id)
      || activeRunStarted
      || Boolean(activeRunUserMessage && ['queued', 'deferred', 'delivering'].includes(activeRunUserMessage.delivery_state)));
  const readOnly = selectedSession?.lifecycle_status === 'historical'
    || selectedSession?.lifecycle_status === 'recovery_failed'
    || Boolean(selectedSession?.agent_deleted_at);

  useEffect(() => {
    const generation = ++eventLoadGeneration.current;
    const controller = new AbortController();
    if (!selectedId || sessionRunIds.length === 0) return () => controller.abort();
    void Promise.allSettled(sessionRunIds.map((runId) => api.runEvents(runId, controller.signal))).then((results) => {
      if (!mountedRef.current || generation !== eventLoadGeneration.current) return;
      const loaded = results.flatMap((result) => result.status === 'fulfilled' ? result.value : []);
      if (loaded.length > 0) setEvents((current) => mergeRunEvents(current, loaded));
    });
    return () => controller.abort();
  }, [selectedId, sessionRunIdsKey]);

  useEffect(() => {
    const generation = ++streamGeneration.current;
    const controller = new AbortController();
    if (!activeRunId || !selectedSession || readOnly) return () => controller.abort();
    const refreshSelectedSession = () => {
      const refreshGeneration = ++sessionRefreshGeneration.current;
      void Promise.allSettled([
        api.session(selectedSession.id, controller.signal),
        api.sessionMessages(selectedSession.id, controller.signal)
      ]).then(([sessionResult, messageResult]) => {
        if (!mountedRef.current
          || generation !== streamGeneration.current
          || refreshGeneration !== sessionRefreshGeneration.current) return;
        if (sessionResult.status === 'fulfilled') {
          setSessions((current) => current.map((session) => (
            session.id === sessionResult.value.id ? sessionResult.value : session
          )));
        }
        if (messageResult.status === 'fulfilled') {
          messageLoadGeneration.current += 1;
          setMessages(messageResult.value);
          setMessagesError(false);
          setMessagesLoading(false);
        }
      });
    };
    void api.streamRunEvents(activeRunId, controller.signal, (event) => {
      if (!mountedRef.current || generation !== streamGeneration.current) return;
      setEvents((current) => mergeRunEvents(current, [event]));
      if (eventRefreshesSession(event)) refreshSelectedSession();
    }).catch((error) => {
      if ((error as Error)?.name !== 'AbortError') setActionError(true);
    });
    return () => controller.abort();
  }, [activeRunId, readOnly, selectedSession?.id]);

  useEffect(() => {
    resizeComposer(composerRef.current);
  }, [conversationDraft, draft, selectedId]);

  const filteredSessions = useMemo(() => {
    const query = search.trim().toLocaleLowerCase(locale);
    return sessions.filter((session) => {
      const originMatches = originFilter === 'all' || session.origin.kind === originFilter;
      const searchMatches = !query || [session.agent_name, session.id, session.lifecycle_status]
        .join(' ').toLocaleLowerCase(locale).includes(query);
      return originMatches && searchMatches;
    });
  }, [locale, originFilter, search, sessions]);

  const timeline = useMemo(() => {
    const entries: TimelineEntry[] = sessionMessages.filter((message) => Boolean(message.content)).map((message) => ({
      kind: 'message' as const,
      id: message.id,
      sequence: message.sequence * 1000,
      occurredAt: eventTimestamp(message.accepted_at),
      runId: message.run_id,
      role: message.role,
      content: message.content!,
      state: message.delivery_state,
      mode: message.delivery_mode
    }));
    const messageContents = new Set(entries.flatMap((entry) => entry.kind === 'message'
      ? [`${entry.runId ?? ''}:${entry.role}:${entry.content}`]
      : []));
    for (const event of sessionEvents) {
      const messageKey = `${event.run_id}:${event.role ?? 'assistant'}:${event.content ?? ''}`;
      if (event.event_type === 'message' && event.content && !messageContents.has(messageKey)) {
        entries.push({ kind: 'live', id: `event-message-${event.run_id}-${event.seq}`, sequence: event.seq * 1000 + 1, occurredAt: eventTimestamp(event.created_at), role: event.role ?? 'assistant', content: event.content });
        messageContents.add(messageKey);
      }
    }
    for (const activity of projectActivities(sessionEvents)) {
      entries.push({
        kind: 'activity',
        id: activity.id,
        sequence: activity.sequence * 1000 + 2,
        occurredAt: activity.occurredAt,
        activity
      });
    }
    return entries.sort((left, right) => left.occurredAt - right.occurredAt || left.sequence - right.sequence);
  }, [sessionEvents, sessionMessages]);
  const timelineItems = useMemo(() => {
    return timeline.reduce<TimelineItem[]>((items, entry) => {
      if (entry.kind !== 'activity') {
        items.push(entry);
        return items;
      }
      const previous = items.at(-1);
      if (previous?.kind === 'activity-group' && previous.runId === entry.activity.runId) {
        previous.activities.push(entry.activity);
      } else {
        items.push({
          kind: 'activity-group',
          id: `activity-${entry.activity.runId}-${entry.activity.id}`,
          runId: entry.activity.runId,
          activities: [entry.activity]
        });
      }
      return items;
    }, []);
  }, [timeline]);

  function lifecycleLabel(status: string) {
    return t(lifecycleKeys[status] ?? 'sessionStatusUnknown');
  }

  function deliveryLabel(status: string) {
    return t(deliveryKeys[status] ?? 'sessionStatusUnknown');
  }

  async function stopCurrentRun() {
    if (!activeRunId || stopping) return;
    setStopping(true);
    setActionError(false);
    try {
      await api.stopRun(activeRunId);
      if (mountedRef.current) setStopRequestedRunId(activeRunId);
    } catch {
      if (mountedRef.current) setActionError(true);
    } finally {
      if (mountedRef.current) setStopping(false);
    }
  }

  async function submitMessage(event: FormEvent) {
    event.preventDefault();
    const content = draft.trim();
    const pendingConversationDraft = conversationDraft;
    const pendingDraftGeneration = conversationDraftGeneration.current;
    if ((!selectedSession && !pendingConversationDraft) || !content || readOnly || sending) return;
    setSending(true);
    setActionError(false);
    setConversationCreateError(false);
    try {
      if (pendingConversationDraft) {
        const run = await api.createRun(pendingConversationDraft.agentId, content);
        if (!run.hub_session_id) throw new Error('new conversation did not return a Session id');
        if (!mountedRef.current) return;
        if (pendingDraftGeneration !== conversationDraftGeneration.current) {
          await loadSessions();
          return;
        }
        await loadSessions(
          run.hub_session_id,
          () => pendingDraftGeneration === conversationDraftGeneration.current
        );
        if (!mountedRef.current || pendingDraftGeneration !== conversationDraftGeneration.current) return;
        setDraft('');
        setConversationDraft(null);
        return;
      }
      const accepted = await api.createSessionMessage(selectedSession!.id, { content });
      if (!mountedRef.current) return;
      setMessages((current) => current.some((message) => message.id === accepted.message.id)
        ? current
        : [...current, accepted.message]);
      setDraft('');
    } catch {
      if (mountedRef.current) {
        if (pendingConversationDraft) {
          if (pendingDraftGeneration === conversationDraftGeneration.current) {
            setConversationCreateError(true);
          }
        } else {
          setActionError(true);
        }
      }
    } finally {
      if (mountedRef.current) setSending(false);
    }
  }

  return <section className="session-workspace session-chat-workspace" aria-labelledby="session-page-title">
    <h1 className="sr-only" id="session-page-title">{t('sessions')}</h1>
    {loadError && <div className="operation-alert" role="alert"><span>{t('sessionsLoadFailed')}</span><button type="button" onClick={() => void loadSessions()}>{t('retry')}</button></div>}
    <div className="session-layout">
      <aside className={`session-master${sessionListOpen ? ' open' : ''}`} aria-label={t('sessionList')}>
        <div className="session-master-header">
          <button className="session-new-conversation" type="button" disabled={loading || loadError || sending} onClick={() => setCreateOpen(true)}><Plus size={17} /> <span>{t('newConversation')}</span></button>
          <button className="icon-button session-close-list" type="button" aria-label={t('close')} title={t('close')} onClick={() => setSessionListOpen(false)}><X size={18} /></button>
        </div>
        <div className="session-list-controls">
          <label className="operation-search"><span className="sr-only">{t('searchSessions')}</span><Search size={16} aria-hidden="true" /><input value={search} onChange={(event) => setSearch(event.target.value)} placeholder={t('searchSessions')} /></label>
          <label className="session-origin-filter"><span className="sr-only">{t('sessionOrigin')}</span><select aria-label={t('sessionOrigin')} value={originFilter} onChange={(event) => setOriginFilter(event.target.value as typeof originFilter)}><option value="all">{t('allOrigins')}</option><option value="hub_native">{t('hubNative')}</option><option value="external">{t('external')}</option></select><ChevronDown size={14} aria-hidden="true" /></label>
        </div>
        <div className="session-list" aria-live="polite" aria-busy={loading}>
          {loading && <div className="operation-state" role="status">{t('loadingSessions')}</div>}
          {!loading && sessions.length === 0 && !loadError && <div className="operation-state">{t('noSessions')}</div>}
          {!loading && sessions.length > 0 && filteredSessions.length === 0 && <div className="operation-state">{t('noSessionMatches')}</div>}
          {filteredSessions.map((session) => <button className={`session-row${session.id === selectedId ? ' selected' : ''}`} type="button" key={session.id} aria-pressed={session.id === selectedId} onClick={() => {
            conversationDraftGeneration.current += 1;
            setConversationDraft(null);
            setConversationCreateError(false);
            setDraft('');
            setSelectedId(session.id);
            setSessionListOpen(false);
          }}>
            <span className="session-row-heading"><strong>{session.agent_name}</strong><span className={`session-row-status ${session.lifecycle_status}`} aria-label={lifecycleLabel(session.lifecycle_status)} title={lifecycleLabel(session.lifecycle_status)} /></span>
            <span className="session-row-preview"><span>{session.origin.kind === 'hub_native' ? t('hubNative') : t('external')}</span><time>{new Date(session.updated_at).toLocaleString(locale)}</time></span>
          </button>)}
        </div>
      </aside>
      {sessionListOpen && <button className="session-list-backdrop" type="button" aria-label={t('close')} onClick={() => setSessionListOpen(false)} />}
      <section className="session-detail session-chat" role="region" aria-label={t('sessionDetails')}>
        {!selectedSession && !conversationDraft ? <>
          <header className="session-detail-header session-chat-header session-chat-empty-header">
            <button className="icon-button session-list-toggle" type="button" aria-label={t('sessionList')} title={t('sessionList')} onClick={() => setSessionListOpen(true)}><PanelLeft size={18} /></button>
            <h2>{t('sessions')}</h2>
          </header>
          <div className="operation-state session-detail-state">{loading ? t('loadingSessions') : t('selectSession')}</div>
        </> : <>
          <header className="session-detail-header session-chat-header">
            <div className="session-chat-title">
              <button className="icon-button session-list-toggle" type="button" aria-label={t('sessionList')} title={t('sessionList')} onClick={() => setSessionListOpen(true)}><PanelLeft size={18} /></button>
              <div><h2>{conversationAgentName}</h2><span>{conversationDraft || selectedSession?.origin.kind === 'hub_native' ? t('hubNative') : t('external')}</span></div>
            </div>
            {selectedSession && <span className={`status ${selectedSession.lifecycle_status}`}>{lifecycleLabel(selectedSession.lifecycle_status)}</span>}
          </header>
          <div className="session-chat-scroll">
            {selectedSession?.lifecycle_status === 'recovery_failed' && <div className="session-banner error" role="alert"><strong>{t('sessionStatusRecoveryFailed')}</strong><span>{selectedSession.recovery_error ?? t('recoveryFailedFallback')}</span></div>}
            {(selectedSession?.lifecycle_status === 'historical' || selectedSession?.agent_deleted_at) && <div className="session-banner"><strong>{t('historicalSession')}</strong><span>{t('historicalSessionHelp')}</span></div>}
            {stopRequestedRunId && <div className="session-banner success" role="status">{t('stopRequested')}</div>}
            {actionError && <div className="session-banner error" role="alert">{t('genericError')}</div>}
            {conversationCreateError && <div className="session-banner error" role="alert">{t('conversationCreateFailed')}</div>}
            <div className="session-transcript" aria-busy={messagesLoading}>
              {!conversationDraft && messagesLoading && <div className="operation-state" role="status">{t('loadingMessages')}</div>}
              {!conversationDraft && !messagesLoading && messagesError && <div className="operation-state error" role="alert">{t('messagesLoadFailed')}</div>}
              {!conversationDraft && !messagesLoading && !messagesError && timelineItems.length === 0 && <div className="operation-state">{t('noMessages')}</div>}
              {!conversationDraft && timelineItems.map((entry) => entry.kind === 'activity-group'
                ? <details className="session-activity-events" key={entry.id}><summary><span>{t('agentActivityDuration').replace('{duration}', formatActivityDuration(entry.activities, locale))}</span><ChevronRight className="session-activity-chevron" size={16} aria-hidden="true" /></summary><div>{entry.activities.map((activity) => <div className="session-activity-row" key={activity.id}><span className="session-activity-icon" aria-hidden="true"><ActivityIcon kind={activity.kind} /></span><div className="session-activity-content"><strong>{t(activityKeys[activity.kind])}</strong>{activity.summary && (activity.kind === 'command' ? <code>{activity.summary}</code> : <span className="session-activity-summary">{activity.summary}</span>)}{activity.output && <div className="session-activity-output"><span>{t('activityOutput')}</span><pre>{activity.output}</pre></div>}</div></div>)}</div></details>
                : <article className={`session-bubble role-${entry.role}`} key={entry.id}>
                  {entry.role !== 'user' && <span className="session-message-avatar" aria-hidden="true"><Bot size={17} /></span>}
                  <div className="session-message-body">
                    <header>{entry.role !== 'user' && <strong>{entry.role === 'assistant' ? conversationAgentName : entry.role}</strong>}{entry.kind === 'message' && entry.state && entry.state !== 'delivered' && <span className={`message-state ${entry.state}`}>{deliveryLabel(entry.state)}</span>}</header>
                    <div className="session-message-text">{entry.content}</div>
                    {entry.kind === 'message' && entry.mode === 'steer' && <small>{t('guidingCurrentTurn')}</small>}
                  </div>
                </article>)}
              {!conversationDraft && showThinking && <article className="session-bubble role-assistant session-thinking">
                <span className="session-message-avatar" aria-hidden="true"><Bot size={17} /></span>
                <div className="session-message-body">
                  <div className="session-thinking-indicator" role="status" aria-label={t('agentThinking')}>
                    <span className="session-thinking-label">{t('agentThinking')}</span>
                    <span className="session-thinking-dot" aria-hidden="true" />
                    <span className="session-thinking-dot" aria-hidden="true" />
                    <span className="session-thinking-dot" aria-hidden="true" />
                  </div>
                </div>
              </article>}
            </div>
          </div>
          {!readOnly && <form className="session-composer session-chat-composer" onSubmit={submitMessage}>
            <label><span className="sr-only">{t('message')}</span><textarea ref={composerRef} rows={2} aria-label={t('message')} value={draft} onChange={(event) => setDraft(event.target.value)} onInput={(event) => resizeComposer(event.currentTarget)} onKeyDown={(event) => {
              if (event.key !== 'Enter' || event.shiftKey || event.nativeEvent.isComposing) return;
              event.preventDefault();
              event.currentTarget.form?.requestSubmit();
            }} placeholder={selectedSession?.active_turn_id ? t('guideCurrentTurnPlaceholder') : t('messagePlaceholder')} /></label>
            <div>{selectedSession?.active_turn_id && <span>{t('guidingCurrentTurn')}</span>}<span className="session-composer-actions">{selectedSession?.active_turn_id && activeRunId && <button type="button" className="icon-button session-stop-button" aria-label={t('stopCurrentRun')} title={t('stopCurrentRun')} disabled={stopping || stopRequestedRunId === activeRunId} onClick={stopCurrentRun}><Square size={14} /></button>}<button type="submit" className="icon-button session-send-button" aria-label={sending ? t('sending') : t('send')} title={t('send')} disabled={sending || !draft.trim()}><ArrowUp size={18} /></button></span></div>
          </form>}
        </>}
      </section>
    </div>
    {createOpen && <NewConversationDialog agents={agents} onClose={() => setCreateOpen(false)} onSelected={(nextDraft) => {
      setCreateOpen(false);
      conversationDraftGeneration.current += 1;
      setConversationDraft(nextDraft);
      setConversationCreateError(false);
      setActionError(false);
      setSelectedId(null);
      setDraft('');
      setSessionListOpen(false);
    }} />}
  </section>;
}
