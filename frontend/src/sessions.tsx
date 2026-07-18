import { ArrowUp, Bot, ChevronDown, ChevronRight, PanelLeft, Plus, Search, Square, X } from 'lucide-react';
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

type TimelineEntry =
  | { kind: 'message'; id: string; sequence: number; occurredAt: number; role: string; content: string; state?: string; mode?: string }
  | { kind: 'live'; id: string; sequence: number; occurredAt: number; role: string; content: string }
  | { kind: 'technical'; id: string; sequence: number; occurredAt: number; event: RunEvent };

type TimelineItem = Exclude<TimelineEntry, { kind: 'technical' }>
  | { kind: 'technical-group'; id: string; events: RunEvent[] };

function eventTimestamp(value: string) {
  const timestamp = Date.parse(value);
  return Number.isFinite(timestamp) ? timestamp : 0;
}

function formatTechnicalDuration(events: RunEvent[], locale: string) {
  const timestamps = events.map((event) => eventTimestamp(event.created_at));
  const milliseconds = Math.max(0, Math.max(...timestamps) - Math.min(...timestamps));
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
  return [...merged.values()].sort((left, right) => left.seq - right.seq);
}

function NewConversationDialog({ agents, onClose, onCreated }: {
  agents: Agent[];
  onClose: () => void;
  onCreated: (sessionId: string) => void;
}) {
  const { t } = useI18n();
  const availableAgents = agents.filter((agent) => agent.can_invoke);
  const [agentId, setAgentId] = useState(availableAgents[0]?.id ?? '');
  const [message, setMessage] = useState('');
  const [pending, setPending] = useState(false);
  const [error, setError] = useState(false);
  const pendingRef = useRef(false);
  const mountedRef = useRef(true);
  const agentRef = useRef<HTMLSelectElement>(null);

  useEffect(() => () => { mountedRef.current = false; }, []);

  async function submit(event: FormEvent) {
    event.preventDefault();
    if (pendingRef.current || !agentId || !message.trim()) return;
    pendingRef.current = true;
    setPending(true);
    setError(false);
    try {
      const run = await api.createRun(agentId, message.trim());
      if (mountedRef.current && run.hub_session_id) onCreated(run.hub_session_id);
      else if (mountedRef.current) setError(true);
    } catch {
      if (mountedRef.current) setError(true);
    } finally {
      pendingRef.current = false;
      if (mountedRef.current) setPending(false);
    }
  }

  return <FormDialog
    title={t('newConversation')}
    eyebrow={t('sessions')}
    onClose={onClose}
    busy={pending}
    initialFocusRef={agentRef}
    className="session-create-dialog"
    footer={<><button className="secondary" type="button" disabled={pending} onClick={onClose}>{t('cancel')}</button><button className="primary" form="new-conversation-form" type="submit" disabled={pending || !agentId || !message.trim()}>{pending ? t('sending') : t('startConversation')}</button></>}
  >
    <form id="new-conversation-form" className="stack" onSubmit={submit}>
      <label>{t('agent')}<select ref={agentRef} aria-label={t('agent')} required value={agentId} onChange={(event) => setAgentId(event.target.value)}>{availableAgents.map((agent) => <option key={agent.id} value={agent.id}>{agent.name}</option>)}</select></label>
      <label>{t('initialMessage')}<textarea required value={message} onChange={(event) => setMessage(event.target.value)} /></label>
      {availableAgents.length === 0 && <div className="warning" role="alert">{t('noInvocableAgents')}</div>}
      {error && <div className="error" role="alert">{t('conversationCreateFailed')}</div>}
    </form>
  </FormDialog>;
}

export function SessionsPage() {
  const { locale, t } = useI18n();
  const mountedRef = useRef(true);
  const sessionLoadGeneration = useRef(0);
  const messageLoadGeneration = useRef(0);
  const streamGeneration = useRef(0);
  const [sessions, setSessions] = useState<HubSession[]>([]);
  const [agents, setAgents] = useState<Agent[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
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
  const [createOpen, setCreateOpen] = useState(false);
  const [sessionListOpen, setSessionListOpen] = useState(false);

  const loadSessions = useCallback(async (preferredId?: string) => {
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
        const requested = preferredId ?? current;
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
      streamGeneration.current += 1;
    };
  }, [loadSessions]);

  useEffect(() => {
    const generation = ++messageLoadGeneration.current;
    const controller = new AbortController();
    setEvents([]);
    setActionError(false);
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
  const activeRunId = useMemo(() => [...messages].reverse().find((message) => message.run_id)?.run_id ?? null, [messages]);
  const readOnly = selectedSession?.lifecycle_status === 'historical'
    || selectedSession?.lifecycle_status === 'recovery_failed'
    || Boolean(selectedSession?.agent_deleted_at);

  useEffect(() => {
    const generation = ++streamGeneration.current;
    const controller = new AbortController();
    if (!activeRunId || !selectedSession || readOnly) return () => controller.abort();
    api.runEvents(activeRunId, controller.signal).then((loaded) => {
      if (mountedRef.current && generation === streamGeneration.current) {
        setEvents((current) => mergeRunEvents(current, loaded));
      }
    }).catch(() => { /* The message history remains usable without technical events. */ });
    void api.streamRunEvents(activeRunId, controller.signal, (event) => {
      if (!mountedRef.current || generation !== streamGeneration.current) return;
      setEvents((current) => mergeRunEvents(current, [event]));
    }).catch((error) => {
      if ((error as Error)?.name !== 'AbortError') setActionError(true);
    });
    return () => controller.abort();
  }, [activeRunId, readOnly, selectedSession?.id]);

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
    const entries: TimelineEntry[] = messages.filter((message) => Boolean(message.content)).map((message) => ({
      kind: 'message' as const,
      id: message.id,
      sequence: message.sequence * 1000,
      occurredAt: eventTimestamp(message.accepted_at),
      role: message.role,
      content: message.content!,
      state: message.delivery_state,
      mode: message.delivery_mode
    }));
    const messageContents = new Set(entries.flatMap((entry) => entry.kind === 'technical' ? [] : [entry.content]));
    for (const event of events) {
      if (event.event_type === 'message' && event.content && !messageContents.has(event.content)) {
        entries.push({ kind: 'live', id: `event-message-${event.run_id}-${event.seq}`, sequence: event.seq * 1000 + 1, occurredAt: eventTimestamp(event.created_at), role: event.role ?? 'assistant', content: event.content });
        messageContents.add(event.content);
      } else if (event.event_type !== 'message') {
        entries.push({ kind: 'technical', id: `event-${event.run_id}-${event.seq}`, sequence: event.seq * 1000 + 2, occurredAt: eventTimestamp(event.created_at), event });
      }
    }
    return entries.sort((left, right) => left.occurredAt - right.occurredAt || left.sequence - right.sequence);
  }, [events, messages]);
  const timelineItems = useMemo(() => {
    const eventsByRun = new Map<string, RunEvent[]>();
    for (const entry of timeline) {
      if (entry.kind !== 'technical') continue;
      const runEvents = eventsByRun.get(entry.event.run_id) ?? [];
      runEvents.push(entry.event);
      eventsByRun.set(entry.event.run_id, runEvents);
    }
    const emittedRuns = new Set<string>();
    return timeline.reduce<TimelineItem[]>((items, entry) => {
      if (entry.kind !== 'technical') {
        items.push(entry);
      } else if (!emittedRuns.has(entry.event.run_id)) {
        emittedRuns.add(entry.event.run_id);
        items.push({ kind: 'technical-group', id: `technical-${entry.event.run_id}`, events: eventsByRun.get(entry.event.run_id)! });
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
    if (!selectedSession || !content || readOnly || sending) return;
    setSending(true);
    setActionError(false);
    try {
      const accepted = await api.createSessionMessage(selectedSession.id, { content });
      if (!mountedRef.current) return;
      setMessages((current) => current.some((message) => message.id === accepted.message.id)
        ? current
        : [...current, accepted.message]);
      setDraft('');
    } catch {
      if (mountedRef.current) setActionError(true);
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
          <button className="session-new-conversation" type="button" disabled={loading || loadError} onClick={() => setCreateOpen(true)}><Plus size={17} /> <span>{t('newConversation')}</span></button>
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
          {filteredSessions.map((session) => <button className={`session-row${session.id === selectedId ? ' selected' : ''}`} type="button" key={session.id} aria-pressed={session.id === selectedId} onClick={() => { setSelectedId(session.id); setSessionListOpen(false); }}>
            <span className="session-row-heading"><strong>{session.agent_name}</strong><span className={`session-row-status ${session.lifecycle_status}`} aria-label={lifecycleLabel(session.lifecycle_status)} title={lifecycleLabel(session.lifecycle_status)} /></span>
            <span className="session-row-preview"><span>{session.origin.kind === 'hub_native' ? t('hubNative') : t('external')}</span><time>{new Date(session.updated_at).toLocaleString(locale)}</time></span>
          </button>)}
        </div>
      </aside>
      {sessionListOpen && <button className="session-list-backdrop" type="button" aria-label={t('close')} onClick={() => setSessionListOpen(false)} />}
      <section className="session-detail session-chat" role="region" aria-label={t('sessionDetails')}>
        {!selectedSession ? <>
          <header className="session-detail-header session-chat-header session-chat-empty-header">
            <button className="icon-button session-list-toggle" type="button" aria-label={t('sessionList')} title={t('sessionList')} onClick={() => setSessionListOpen(true)}><PanelLeft size={18} /></button>
            <h2>{t('sessions')}</h2>
          </header>
          <div className="operation-state session-detail-state">{loading ? t('loadingSessions') : t('selectSession')}</div>
        </> : <>
          <header className="session-detail-header session-chat-header">
            <div className="session-chat-title">
              <button className="icon-button session-list-toggle" type="button" aria-label={t('sessionList')} title={t('sessionList')} onClick={() => setSessionListOpen(true)}><PanelLeft size={18} /></button>
              <div><h2>{selectedSession.agent_name}</h2><span>{selectedSession.origin.kind === 'hub_native' ? t('hubNative') : t('external')}</span></div>
            </div>
            <span className={`status ${selectedSession.lifecycle_status}`}>{lifecycleLabel(selectedSession.lifecycle_status)}</span>
          </header>
          <div className="session-chat-scroll">
            {selectedSession.lifecycle_status === 'recovery_failed' && <div className="session-banner error" role="alert"><strong>{t('sessionStatusRecoveryFailed')}</strong><span>{selectedSession.recovery_error ?? t('recoveryFailedFallback')}</span></div>}
            {(selectedSession.lifecycle_status === 'historical' || selectedSession.agent_deleted_at) && <div className="session-banner"><strong>{t('historicalSession')}</strong><span>{t('historicalSessionHelp')}</span></div>}
            {stopRequestedRunId && <div className="session-banner success" role="status">{t('stopRequested')}</div>}
            {actionError && <div className="session-banner error" role="alert">{t('genericError')}</div>}
            <div className="session-transcript" aria-busy={messagesLoading}>
              {messagesLoading && <div className="operation-state" role="status">{t('loadingMessages')}</div>}
              {!messagesLoading && messagesError && <div className="operation-state error" role="alert">{t('messagesLoadFailed')}</div>}
              {!messagesLoading && !messagesError && timelineItems.length === 0 && <div className="operation-state">{t('noMessages')}</div>}
              {timelineItems.map((entry) => entry.kind === 'technical-group'
                ? <details className="session-technical-events" key={entry.id}><summary><span>{t('technicalEventsDuration').replace('{duration}', formatTechnicalDuration(entry.events, locale))}</span><ChevronRight className="session-technical-chevron" size={16} aria-hidden="true" /></summary><div>{entry.events.map((event) => <div className="session-technical-row" key={`${event.run_id}-${event.seq}`}><code>{event.event_type}</code><span>{event.content ?? JSON.stringify(event.payload)}</span></div>)}</div></details>
                : <article className={`session-bubble role-${entry.role}`} key={entry.id}>
                  {entry.role !== 'user' && <span className="session-message-avatar" aria-hidden="true"><Bot size={17} /></span>}
                  <div className="session-message-body">
                    <header>{entry.role !== 'user' && <strong>{entry.role === 'assistant' ? selectedSession.agent_name : entry.role}</strong>}{entry.kind === 'message' && entry.state && entry.state !== 'delivered' && <span className={`message-state ${entry.state}`}>{deliveryLabel(entry.state)}</span>}</header>
                    <div className="session-message-text">{entry.content}</div>
                    {entry.kind === 'message' && entry.mode === 'steer' && <small>{t('guidingCurrentTurn')}</small>}
                  </div>
                </article>)}
            </div>
          </div>
          {!readOnly && <form className="session-composer session-chat-composer" onSubmit={submitMessage}>
            <label><span className="sr-only">{t('message')}</span><textarea aria-label={t('message')} value={draft} onChange={(event) => setDraft(event.target.value)} placeholder={selectedSession.active_turn_id ? t('guideCurrentTurnPlaceholder') : t('messagePlaceholder')} /></label>
            <div>{selectedSession.active_turn_id && <span>{t('guidingCurrentTurn')}</span>}<span className="session-composer-actions">{selectedSession.active_turn_id && activeRunId && <button type="button" className="icon-button session-stop-button" aria-label={t('stopCurrentRun')} title={t('stopCurrentRun')} disabled={stopping || stopRequestedRunId === activeRunId} onClick={stopCurrentRun}><Square size={14} /></button>}<button type="submit" className="icon-button session-send-button" aria-label={sending ? t('sending') : t('send')} title={t('send')} disabled={sending || !draft.trim()}><ArrowUp size={18} /></button></span></div>
          </form>}
        </>}
      </section>
    </div>
    {createOpen && <NewConversationDialog agents={agents} onClose={() => setCreateOpen(false)} onCreated={async (sessionId) => {
      setCreateOpen(false);
      await loadSessions(sessionId);
    }} />}
  </section>;
}
