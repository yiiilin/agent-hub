import { ArrowUp, Bot, Brain, ChevronDown, ChevronRight, FilePenLine, ImageIcon, ListChecks, Minimize2, PanelLeft, Plus, RefreshCw, Search, Square, Terminal, Trash2, Users, Wrench, X } from 'lucide-react';
import { FormEvent, lazy, Suspense, type TouchEvent, type WheelEvent, useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import { api, ApiError, type Agent, type HubSession, type HubSessionMessage, type RunEvent, type SecretGrantRequirement } from './api/client';
import { FormDialog } from './components/form-dialog';
import { useI18n } from './i18n';
import type { TranslationKey } from './i18n';
import { discardConversationDraft, loadConversationDraft, loadSelectedSessionAgent, saveConversationDraft, saveSelectedSessionAgent } from './session-drafts';

const ChatMarkdown = lazy(() => import('./components/chat-markdown'));

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
const sessionMessagePageSize = 50;
export const sessionMessageRequestLimit = sessionMessagePageSize + 1;
const chatBottomThreshold = 24;
const chatHistoryThreshold = 64;

export function selectSessionMessagePage(messages: HubSessionMessage[]) {
  const hasMore = messages.length > sessionMessagePageSize;
  return {
    hasMore,
    items: hasMore ? messages.slice(messages.length - sessionMessagePageSize) : messages
  };
}

function mergeSessionMessages(current: HubSessionMessage[], incoming: HubSessionMessage[]) {
  const merged = new Map(current.map((message) => [message.id, message]));
  for (const message of incoming) merged.set(message.id, message);
  return [...merged.values()].sort((left, right) => left.sequence - right.sequence);
}

type ActivityKind = 'reasoning' | 'command' | 'file' | 'tool' | 'search' | 'plan' | 'image' | 'subagent' | 'compaction' | 'review' | 'wait' | 'retry';

export type ActivityEntry = {
  id: string;
  runId: string;
  itemId: string | null;
  kind: ActivityKind;
  phase: string;
  sequence: number;
  occurredAt: number;
  sortAt: number;
  endedAt: number;
  summary: string | null;
  output: string | null;
  status: string | null;
};

export type RunFailureEntry = {
  id: string;
  runId: string;
  occurredAt: number;
  sequence: number;
  errorCode: string | null;
  timeoutSeconds: number | null;
};

type TimelineEntry =
  | { kind: 'message'; id: string; sequence: number; occurredAt: number; outputEndedAt?: number; runId: string | null; role: string; content: string; state?: string; mode?: string }
  | { kind: 'live'; id: string; sequence: number; occurredAt: number; outputEndedAt?: number; runId: string; role: string; content: string }
  | { kind: 'failure'; id: string; sequence: number; occurredAt: number; runId: string; failure: RunFailureEntry }
  | { kind: 'activity'; id: string; sequence: number; occurredAt: number; activity: ActivityEntry };

type TimelineItem = Exclude<TimelineEntry, { kind: 'activity' }>
  | { kind: 'activity-group'; id: string; runId: string; activities: ActivityEntry[] };

type ConversationDraft = {
  agentId: string;
  agentName: string;
};

type PlatformFilter = 'hub_native' | 'all' | `external:${string}`;

function eventTimestamp(value: string) {
  const timestamp = Date.parse(value);
  return Number.isFinite(timestamp) ? timestamp : 0;
}

function newestSessionFirst(left: HubSession, right: HubSession) {
  return eventTimestamp(right.created_at) - eventTimestamp(left.created_at)
    || right.id.localeCompare(left.id);
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
  wait: 'activityWait',
  retry: 'activityRetry'
};

const activityStatusKeys: Record<string, TranslationKey> = {
  pending: 'clientToolPending',
  success: 'clientToolSucceeded',
  result: 'clientToolSucceeded',
  error: 'clientToolFailed',
  timeout: 'clientToolTimedOut',
  timed_out: 'clientToolTimedOut',
  unknown: 'clientToolUnknown',
  cancelled: 'statusCancelled'
};

function formatActivityDuration(
  activities: ActivityEntry[],
  locale: string,
  startedAt?: number,
  endedAt?: number
) {
  const activityStartedAt = Math.min(...activities.map((activity) => activity.occurredAt));
  const activityEndedAt = Math.max(...activities.map((activity) => activity.endedAt));
  const milliseconds = Math.max(
    0,
    (endedAt ?? activityEndedAt) - (startedAt ?? activityStartedAt)
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

export function mergeRunEvents(current: RunEvent[], incoming: RunEvent[]) {
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

export function runProcessingWindow(events: RunEvent[], acceptedAt?: number) {
  const starts = events
    .filter((event) => event.event_type === 'message' && event.role === 'user')
    .map((event) => eventTimestamp(event.created_at))
    .filter((timestamp) => timestamp > 0);
  if (acceptedAt !== undefined && acceptedAt > 0) starts.push(acceptedAt);
  const terminalEnds = events.filter((event) => {
    if (event.event_type !== 'status') return false;
    const status = event.content ?? payloadString(event.payload, 'status');
    return status !== null && terminalRunStatuses.has(status);
  }).map((event) => eventTimestamp(event.created_at)).filter((timestamp) => timestamp > 0);
  return {
    startedAt: starts.length > 0 ? Math.min(...starts) : undefined,
    endedAt: terminalEnds.length > 0 ? Math.min(...terminalEnds) : undefined
  };
}

type ProcessingTimelineItem = {
  kind: string;
  runId: string | null;
  role?: string;
  occurredAt?: number;
  outputEndedAt?: number;
  activities?: ActivityEntry[];
};

export function activityGroupProcessingWindow(
  timeline: readonly ProcessingTimelineItem[],
  index: number,
  runWindow: { startedAt?: number; endedAt?: number } | undefined,
  active: boolean
) {
  const group = timeline[index];
  const activities = group?.activities ?? [];
  const activityStartedAt = activities.length > 0
    ? Math.min(...activities.map((activity) => activity.occurredAt))
    : undefined;
  const activityEndedAt = activities.length > 0
    ? Math.max(...activities.map((activity) => activity.endedAt))
    : undefined;
  let startedAt = runWindow?.startedAt ?? activityStartedAt;
  for (let cursor = index - 1; cursor >= 0; cursor -= 1) {
    const candidate = timeline[cursor];
    if (candidate.runId !== group?.runId || (candidate.role !== 'user' && candidate.role !== 'assistant')) continue;
    startedAt = candidate.outputEndedAt ?? candidate.occurredAt ?? startedAt;
    break;
  }

  let endedAt: number | undefined;
  for (let cursor = index + 1; cursor < timeline.length; cursor += 1) {
    const candidate = timeline[cursor];
    if (candidate.runId !== group?.runId || candidate.role !== 'assistant') continue;
    endedAt = candidate.occurredAt;
    break;
  }
  endedAt ??= runWindow?.endedAt;
  if (endedAt === undefined && !active) endedAt = activityEndedAt;
  if (startedAt !== undefined && endedAt !== undefined) endedAt = Math.max(startedAt, endedAt);
  return { startedAt, endedAt };
}

export function activityGroupIsClosed(
  timeline: readonly TimelineItem[],
  index: number,
  runId: string
) {
  for (let cursor = index + 1; cursor < timeline.length; cursor += 1) {
    const candidate = timeline[cursor];
    if (candidate.runId !== runId) continue;
    if (candidate.kind === 'message' && candidate.role === 'assistant') return true;
    if (candidate.kind === 'message' && candidate.role === 'user') return false;
  }
  return false;
}

export function ActivityLiveStep({ activity }: { activity: ActivityEntry }) {
  const { t } = useI18n();
  if (activity.kind === 'reasoning' && activity.summary) {
    return <div className="session-reasoning-text">{activity.summary}</div>;
  }
  const label = t(activityKeys[activity.kind] ?? 'activityTool');
  return <div className="session-live-activity">
    <span className="session-live-activity-icon" aria-hidden="true"><ActivityIcon kind={activity.kind} /></span>
    <div className="session-live-activity-body">
      <span className="session-live-activity-heading"><strong>{label}</strong>{activity.summary && <code>{activity.summary}</code>}</span>
      {activity.output && <pre className="session-live-activity-output">{activity.output}</pre>}
    </div>
  </div>;
}

function payloadNumber(payload: Record<string, unknown>, key: string) {
  return typeof payload[key] === 'number' && Number.isFinite(payload[key]) ? payload[key] as number : null;
}

export function projectRunFailures(events: RunEvent[]) {
  const failures = new Map<string, RunFailureEntry>();
  for (const event of events) {
    if (event.event_type !== 'status') continue;
    const status = event.content ?? payloadString(event.payload, 'status');
    if (status !== 'failed') continue;
    const candidate: RunFailureEntry = {
      id: `run-failure-${event.run_id}`,
      runId: event.run_id,
      occurredAt: eventTimestamp(event.created_at),
      sequence: event.seq,
      errorCode: payloadString(event.payload, 'error_code'),
      timeoutSeconds: payloadNumber(event.payload, 'timeout_seconds')
    };
    const current = failures.get(event.run_id);
    if (!current || (!current.errorCode && candidate.errorCode)) failures.set(event.run_id, candidate);
  }
  return [...failures.values()].sort((left, right) => (
    left.occurredAt - right.occurredAt || left.sequence - right.sequence
  ));
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
  if (itemType === 'retry') return 'retry';
  return null;
}

function activityFromEvent(event: RunEvent): ActivityEntry | null {
  const clientToolEvent = event.event_type === 'tool_request'
    || event.event_type === 'tool_result'
    || event.event_type === 'client_tool_result'
    || event.event_type === 'client_tool_timeout'
    || event.event_type === 'client_tool_unknown'
    || event.event_type === 'client_tool_cancelled';
  if (clientToolEvent) {
    const itemId = payloadString(event.payload, 'tool_call_id')
      ?? payloadString(event.payload, 'tool_request_id')
      ?? payloadString(event.payload, 'source_id');
    const result = event.payload.result && typeof event.payload.result === 'object'
      ? event.payload.result as Record<string, unknown>
      : null;
    const resultStatus = result && typeof result.status === 'string' ? result.status : null;
    const status = payloadString(event.payload, 'status')
      ?? resultStatus
      ?? (event.event_type === 'tool_request' ? 'pending' : event.event_type.replace('client_tool_', ''));
    const elapsedMs = payloadNumber(event.payload, 'elapsed_ms') ?? 0;
    const endedAt = eventTimestamp(event.created_at);
    const output = result
      ? JSON.stringify(result, null, 2)
      : payloadString(event.payload, 'message') ?? event.content;
    return {
      id: `activity-${event.run_id}-${itemId ?? event.seq}`,
      runId: event.run_id,
      itemId,
      kind: 'tool',
      phase: event.event_type === 'tool_request' ? 'started' : 'completed',
      sequence: event.seq,
      occurredAt: Math.max(0, endedAt - elapsedMs),
      sortAt: endedAt,
      endedAt,
      summary: payloadString(event.payload, 'tool_name'),
      output,
      status
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
  else if (kind === 'retry' && phase === 'started') {
    const attempt = payloadNumber(event.payload, 'attempt');
    const maxAttempts = payloadNumber(event.payload, 'max_attempts');
    const delayMs = payloadNumber(event.payload, 'delay_ms');
    const progress = attempt === null ? null : maxAttempts === null ? `${attempt}` : `${attempt}/${maxAttempts}`;
    summary = [progress, delayMs === null ? null : `${delayMs} ms`].filter(Boolean).join(' · ') || null;
  }
  return {
    id: `activity-${event.run_id}-${itemId ?? event.seq}`,
    runId: event.run_id,
    itemId,
    kind,
    phase,
    sequence: event.seq,
    occurredAt: Math.max(0, endedAt - duration),
    sortAt: endedAt,
    endedAt,
    summary,
    output: payloadString(event.payload, 'output'),
    status: null
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
    sortAt: Math.min(current.sortAt, incoming.sortAt),
    endedAt: Math.max(current.endedAt, incoming.endedAt),
    sequence: Math.min(current.sequence, incoming.sequence),
    summary,
    output,
    status: incoming.status ?? current.status
  };
}

export function projectActivities(events: RunEvent[]) {
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
    left.runId.localeCompare(right.runId) || left.sequence - right.sequence
  ));
}

export function resizeComposer(textarea: HTMLTextAreaElement | null) {
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
  if (kind === 'retry') return <RefreshCw size={15} />;
  return <Minimize2 size={15} />;
}

export function ChatActivityGroup({ activities, startedAt, endedAt, active = false, clockOffset }: { activities: ActivityEntry[]; startedAt?: number; endedAt?: number; active?: boolean; clockOffset?: number }) {
  const { locale, t } = useI18n();
  const [now, setNow] = useState(() => Date.now() - (clockOffset ?? 0));
  useEffect(() => {
    if (!active) return;
    setNow(Date.now() - (clockOffset ?? 0));
    const timer = window.setInterval(() => setNow(Date.now() - (clockOffset ?? 0)), 1000);
    return () => window.clearInterval(timer);
  }, [active, clockOffset, startedAt]);
  const durationEndedAt = active ? now : endedAt;
  return <details className="session-activity-events"><summary><span>{t('agentActivityDuration').replace('{duration}', formatActivityDuration(activities, locale, startedAt, durationEndedAt))}</span><ChevronRight className="session-activity-chevron" size={16} aria-hidden="true" /></summary><div>{activities.map((activity) => <div className="session-activity-row" key={activity.id}><span className="session-activity-icon" aria-hidden="true"><ActivityIcon kind={activity.kind} /></span><div className="session-activity-content"><span className="session-activity-heading"><strong>{t(activityKeys[activity.kind])}</strong>{activity.status && <small className={`session-activity-status status-${activity.status}`}>{t(activityStatusKeys[activity.status] ?? 'statusFailed')}</small>}{activity.kind === 'tool' && activity.status && activity.status !== 'pending' && <small className="session-activity-elapsed">{formatActivityDuration([activity], locale)}</small>}</span>{activity.summary && (activity.kind === 'command' ? <code>{activity.summary}</code> : <span className="session-activity-summary">{activity.summary}</span>)}{activity.output && <div className="session-activity-output"><span>{t('activityOutput')}</span><pre>{activity.output}</pre></div>}</div></div>)}</div></details>;
}

export function ChatMessageBubble({
  agentName,
  content,
  role,
  state,
  stateLabel,
  streaming = false
}: {
  agentName: string | null;
  content: string;
  role: string;
  state?: string;
  stateLabel?: string;
  streaming?: boolean;
}) {
  const { t } = useI18n();
  const visibleState = state && state !== 'delivered' && state !== 'queued' ? state : undefined;
  return <article className={`session-bubble role-${role}`}>
    {role !== 'user' && <span className="session-message-avatar" aria-hidden="true"><Bot size={17} /></span>}
    <div className="session-message-body">
      <header>{role !== 'user' && <strong>{role === 'assistant' ? agentName ?? t('assistant') : role}</strong>}{visibleState && <span className={`message-state ${visibleState}`}>{stateLabel}</span>}</header>
      {role === 'assistant'
        ? <div className="session-message-text session-message-markdown"><Suspense fallback={<span className="session-message-markdown-loading">{content}</span>}><ChatMarkdown content={content} streaming={streaming} /></Suspense></div>
        : <div className="session-message-text">{content}</div>}
    </div>
  </article>;
}

export function runThinkingStage(events: RunEvent[]): { key: TranslationKey; detail?: string } {
  let fallback: { key: TranslationKey; detail?: string } = { key: 'runStageWaitingRuntime' };
  for (const event of events) {
    if (event.event_type === 'status') fallback = { key: 'runStageStarting' };
    if (event.event_type === 'turn_started') fallback = { key: 'runStageStartingAgent' };
    if (event.event_type === 'item') {
      const itemType = payloadString(event.payload, 'item_type');
      const tool = payloadString(event.payload, 'tool') ?? undefined;
      if (itemType === 'reasoning') return { key: 'runStageThinking' };
      if (itemType === 'dynamicToolCall' || itemType === 'mcpToolCall') return { key: 'runStageTool', detail: tool };
      if (itemType === 'commandExecution') return { key: 'runStageCommand', detail: tool };
      if (itemType === 'webSearch') return { key: 'runStageTool', detail: 'web search' };
    }
    if (event.event_type === 'tool_request') {
      return { key: 'runStageTool', detail: payloadString(event.payload, 'tool_name') ?? undefined };
    }
    if (event.event_type === 'message_delta') return { key: 'runStageGenerating' };
  }
  return fallback;
}

export function ChatThinkingBubble({ stage = 'runStageThinking', detail, lastEventAt }: { stage?: TranslationKey; detail?: string; lastEventAt?: number }) {
  const { locale, t } = useI18n();
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), 1000);
    return () => window.clearInterval(timer);
  }, []);
  const silentSeconds = lastEventAt && lastEventAt > 0
    ? Math.max(0, Math.floor((now - lastEventAt) / 1000))
    : undefined;
  const stuck = silentSeconds !== undefined && silentSeconds >= 60;
  return <article className="session-bubble role-assistant session-thinking">
    <span className="session-message-avatar" aria-hidden="true"><Bot size={17} /></span>
    <div className="session-message-body">
      <div className="session-thinking-indicator" role="status" aria-label={t('agentThinking')}>
        <span className="session-thinking-label">{t(stage)}</span>
        {detail && <span className="session-thinking-detail">{detail}</span>}
        <span className="session-thinking-dot" aria-hidden="true" />
        <span className="session-thinking-dot" aria-hidden="true" />
        <span className="session-thinking-dot" aria-hidden="true" />
      </div>
      {lastEventAt && <div className="session-thinking-meta">
        <span>{t('runStageLastEvent').replace('{time}', new Date(lastEventAt).toLocaleTimeString(locale))}</span>
        {silentSeconds !== undefined && <span>{t('runStageSilent').replace('{seconds}', String(silentSeconds))}{stuck ? t('runStageStuck') : ''}</span>}
      </div>}
    </div>
  </article>;
}

export function ChatRunFailure({ failure }: { failure: RunFailureEntry }) {
  const { t } = useI18n();
  const message = failure.errorCode === 'engine_turn_timeout'
    ? t('turnTimeoutStopped').replace(
        '{minutes}',
        String(Math.max(1, Math.ceil((failure.timeoutSeconds ?? 3600) / 60)))
      )
    : t('genericError');
  return <div className="session-banner error session-run-failure" role="alert">{message}</div>;
}

function eventRefreshesSession(event: RunEvent) {
  if (event.event_type === 'turn_started') return true;
  if (event.event_type !== 'status') return false;
  const status = event.content ?? (typeof event.payload.status === 'string' ? event.payload.status : null);
  return status !== null && terminalRunStatuses.has(status);
}

export function SessionsPage({ currentUserId }: { currentUserId: string }) {
  const { locale, t } = useI18n();
  const mountedRef = useRef(true);
  const sessionLoadGeneration = useRef(0);
  const messageLoadGeneration = useRef(0);
  const eventLoadGeneration = useRef(0);
  const streamGeneration = useRef(0);
  const sessionRefreshGeneration = useRef(0);
  const conversationDraftGeneration = useRef(0);
  const chatScrollRef = useRef<HTMLDivElement>(null);
  const followBottomRef = useRef(true);
  const historyPagingReadyRef = useRef(false);
  const lastChatScrollTopRef = useRef(0);
  const olderMessagesLoadingRef = useRef(false);
  const historyAnchorRef = useRef<{ scrollHeight: number; scrollTop: number } | null>(null);
  const chatTouchStartYRef = useRef<number | null>(null);
  const composerRef = useRef<HTMLTextAreaElement>(null);
  const [sessions, setSessions] = useState<HubSession[]>([]);
  const [agents, setAgents] = useState<Agent[]>([]);
  const [selectedAgentId, setSelectedAgentId] = useState<string | null>(() => loadSelectedSessionAgent(currentUserId));
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [conversationDraft, setConversationDraft] = useState<ConversationDraft | null>(null);
  const [messages, setMessages] = useState<HubSessionMessage[]>([]);
  const [events, setEvents] = useState<RunEvent[]>([]);
  const [search, setSearch] = useState('');
  const [platformFilter, setPlatformFilter] = useState<PlatformFilter>('hub_native');
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState(false);
  const [messagesLoading, setMessagesLoading] = useState(false);
  const [messagesError, setMessagesError] = useState(false);
  const [hasOlderMessages, setHasOlderMessages] = useState(false);
  const [olderMessagesLoading, setOlderMessagesLoading] = useState(false);
  const [draft, setDraft] = useState('');
  const [sending, setSending] = useState(false);
  const [stopping, setStopping] = useState(false);
  const [stopRequestedRunId, setStopRequestedRunId] = useState<string | null>(null);
  const [actionError, setActionError] = useState(false);
  const [conversationCreateError, setConversationCreateError] = useState(false);
  const [sessionListOpen, setSessionListOpen] = useState(false);
  const [secretGrantRequest, setSecretGrantRequest] = useState<{ agentId: string; requirements: SecretGrantRequirement[] } | null>(null);
  const [granting, setGranting] = useState(false);

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
      const invocableAgents = loadedAgents.filter((agent) => agent.can_invoke);
      const sessionAgentIds = new Set(loadedSessions.map((session) => session.agent_id));
      setSelectedAgentId((current) => {
        const next = current && (
          invocableAgents.some((agent) => agent.id === current) || sessionAgentIds.has(current)
        )
          ? current
          : invocableAgents[0]?.id ?? loadedSessions[0]?.agent_id ?? null;
        if (next) saveSelectedSessionAgent(currentUserId, next);
        return next;
      });
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
  }, [currentUserId]);

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
    followBottomRef.current = true;
    historyPagingReadyRef.current = false;
    lastChatScrollTopRef.current = 0;
    olderMessagesLoadingRef.current = false;
    historyAnchorRef.current = null;
    setMessages([]);
    setEvents([]);
    setHasOlderMessages(false);
    setOlderMessagesLoading(false);
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
    api.sessionMessagePage(selectedId, { limit: sessionMessageRequestLimit }, controller.signal).then((response) => {
      if (mountedRef.current && generation === messageLoadGeneration.current) {
        const page = selectSessionMessagePage(response);
        setMessages(page.items);
        setHasOlderMessages(page.hasMore);
      }
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
  const serverClockOffset = useMemo(() => {
    const timestamps = [
      ...sessionEvents.map((event) => eventTimestamp(event.created_at)),
      ...sessionMessages.map((message) => eventTimestamp(message.accepted_at))
    ].filter((timestamp) => timestamp > 0);
    return timestamps.length > 0 ? Date.now() - Math.max(...timestamps) : undefined;
  }, [sessionEvents, sessionMessages]);
  const runProcessingWindows = useMemo(() => new Map(sessionRunIds.map((runId) => {
    const acceptedAt = sessionMessages
      .filter((message) => message.run_id === runId && message.role === 'user')
      .map((message) => eventTimestamp(message.accepted_at))
      .filter((timestamp) => timestamp > 0);
    return [runId, runProcessingWindow(
      sessionEvents.filter((event) => event.run_id === runId),
      acceptedAt.length > 0 ? Math.min(...acceptedAt) : undefined
    )] as const;
  })), [sessionEvents, sessionMessages, sessionRunIds]);
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
  const activeRunStarted = activeRunEvents.some((event) => {
    if (event.event_type === 'turn_started') return true;
    if (event.event_type !== 'status') return false;
    const status = event.content ?? payloadString(event.payload, 'status');
    return status !== null && ['pending', 'running', 'waiting_tool'].includes(status);
  });
  const activeRunInProgress = activeRunId !== null
    && !activeRunTerminal
    && (Boolean(selectedSession?.active_turn_id)
      || activeRunStarted
      || Boolean(activeRunUserMessage && ['queued', 'deferred', 'delivering'].includes(activeRunUserMessage.delivery_state)));
  const historyReadOnly = selectedSession?.lifecycle_status === 'historical'
    || selectedSession?.lifecycle_status === 'recovery_failed'
    || Boolean(selectedSession?.agent_deleted_at);
  const canMutate = Boolean(conversationDraft) || Boolean(
    selectedSession && !historyReadOnly && selectedSession.origin.kind === 'hub_native'
  );

  const loadOlderMessages = useCallback(async () => {
    const beforeSequence = sessionMessages[0]?.sequence;
    if (!selectedId || beforeSequence === undefined || !hasOlderMessages || olderMessagesLoadingRef.current) return;
    const generation = messageLoadGeneration.current;
    olderMessagesLoadingRef.current = true;
    setOlderMessagesLoading(true);
    try {
      const response = await api.sessionMessagePage(selectedId, {
        beforeSequence,
        limit: sessionMessageRequestLimit
      });
      const page = selectSessionMessagePage(response);
      const runIds = [...new Set(page.items.flatMap((message) => message.run_id ? [message.run_id] : []))];
      const eventResults = await Promise.allSettled(runIds.map((runId) => api.runEvents(runId)));
      if (!mountedRef.current || generation !== messageLoadGeneration.current) return;
      const scroll = chatScrollRef.current;
      if (scroll) {
        historyAnchorRef.current = {
          scrollHeight: scroll.scrollHeight,
          scrollTop: scroll.scrollTop
        };
      }
      const loadedEvents = eventResults.flatMap((result) => result.status === 'fulfilled' ? result.value : []);
      if (loadedEvents.length > 0) setEvents((current) => mergeRunEvents(current, loadedEvents));
      setMessages((current) => mergeSessionMessages(current, page.items));
      setHasOlderMessages(page.hasMore);
    } catch {
      if (mountedRef.current && generation === messageLoadGeneration.current) setActionError(true);
    } finally {
      olderMessagesLoadingRef.current = false;
      if (mountedRef.current) setOlderMessagesLoading(false);
    }
  }, [hasOlderMessages, selectedId, sessionMessages]);

  const requestOlderMessages = useCallback(() => {
    if (!historyPagingReadyRef.current || !hasOlderMessages || messagesLoading) return;
    followBottomRef.current = false;
    void loadOlderMessages();
  }, [hasOlderMessages, loadOlderMessages, messagesLoading]);

  const handleChatScroll = useCallback(() => {
    const scroll = chatScrollRef.current;
    if (!scroll) return;
    const scrollingUp = scroll.scrollTop < lastChatScrollTopRef.current - 1;
    lastChatScrollTopRef.current = scroll.scrollTop;
    followBottomRef.current = scroll.scrollHeight - scroll.clientHeight - scroll.scrollTop <= chatBottomThreshold;
    if (historyPagingReadyRef.current
      && scrollingUp
      && scroll.scrollTop <= chatHistoryThreshold) {
      requestOlderMessages();
    }
  }, [requestOlderMessages]);

  const handleChatWheel = useCallback((event: WheelEvent<HTMLDivElement>) => {
    const scroll = chatScrollRef.current;
    if (event.deltaY < 0 && scroll && scroll.scrollTop <= chatHistoryThreshold) requestOlderMessages();
  }, [requestOlderMessages]);

  const handleChatTouchStart = useCallback((event: TouchEvent<HTMLDivElement>) => {
    chatTouchStartYRef.current = event.touches[0]?.clientY ?? null;
  }, []);

  const handleChatTouchEnd = useCallback((event: TouchEvent<HTMLDivElement>) => {
    const startY = chatTouchStartYRef.current;
    chatTouchStartYRef.current = null;
    const endY = event.changedTouches[0]?.clientY;
    const scroll = chatScrollRef.current;
    if (startY !== null && endY !== undefined && endY > startY + 12 && scroll && scroll.scrollTop <= chatHistoryThreshold) {
      requestOlderMessages();
    }
  }, [requestOlderMessages]);

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
    if (!activeRunId || !selectedSession || historyReadOnly) return () => controller.abort();
    const refreshSelectedSession = () => {
      const refreshGeneration = ++sessionRefreshGeneration.current;
      void Promise.allSettled([
        api.session(selectedSession.id, controller.signal),
        api.sessionMessagePage(selectedSession.id, { limit: sessionMessageRequestLimit }, controller.signal)
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
          setMessages((current) => mergeSessionMessages(current, selectSessionMessagePage(messageResult.value).items));
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
  }, [activeRunId, historyReadOnly, selectedSession?.id]);

  useEffect(() => {
    resizeComposer(composerRef.current);
  }, [conversationDraft, draft, selectedId]);

  const sessionAgentOptions = useMemo(() => {
    const options = new Map<string, string>();
    for (const agent of agents) {
      if (agent.can_invoke) options.set(agent.id, agent.name);
    }
    for (const session of sessions) {
      if (!options.has(session.agent_id)) options.set(session.agent_id, session.agent_name);
    }
    return [...options].map(([id, name]) => ({ id, name }));
  }, [agents, sessions]);
  const selectedAgentCanInvoke = agents.some((agent) => (
    agent.id === selectedAgentId && agent.can_invoke
  ));

  const externalPlatforms = useMemo(() => {
    const platforms = new Map<string, string>();
    for (const session of sessions) {
      if (session.origin.kind === 'external') {
        platforms.set(session.origin.platform_id, session.origin_platform_name ?? t('external'));
      }
    }
    return [...platforms].map(([id, name]) => ({ id, name })).sort((left, right) => (
      left.name.localeCompare(right.name, locale) || left.id.localeCompare(right.id)
    ));
  }, [locale, sessions, t]);

  const filteredSessions = useMemo(() => {
    const query = search.trim().toLocaleLowerCase(locale);
    return sessions.filter((session) => {
      const platformMatches = platformFilter === 'all'
        || (platformFilter === 'hub_native' && session.origin.kind === 'hub_native')
        || (session.origin.kind === 'external' && platformFilter === `external:${session.origin.platform_id}`);
      const agentMatches = session.agent_id === selectedAgentId;
      const platformName = session.origin.kind === 'external' ? session.origin_platform_name ?? t('external') : t('hubNative');
      const searchMatches = !query || [session.agent_name, session.id, session.lifecycle_status, platformName]
        .join(' ').toLocaleLowerCase(locale).includes(query);
      return platformMatches && agentMatches && searchMatches;
    }).sort(newestSessionFirst);
  }, [locale, platformFilter, search, selectedAgentId, sessions, t]);

  useEffect(() => {
    if (conversationDraft) return;
    if (selectedId && filteredSessions.some((session) => session.id === selectedId)) return;
    setSelectedId(filteredSessions[0]?.id ?? null);
  }, [conversationDraft, filteredSessions, selectedId]);

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
        entries.push({ kind: 'live', id: `event-message-${event.run_id}-${event.seq}`, sequence: event.seq * 1000 + 1, occurredAt: eventTimestamp(event.created_at), runId: event.run_id, role: event.role ?? 'assistant', content: event.content });
        messageContents.add(messageKey);
      }
    }
    for (const activity of projectActivities(sessionEvents)) {
      entries.push({
        kind: 'activity',
        id: activity.id,
        sequence: activity.sequence * 1000 + 2,
        occurredAt: activity.sortAt,
        activity
      });
    }
    for (const failure of projectRunFailures(sessionEvents)) {
      entries.push({
        kind: 'failure',
        id: failure.id,
        sequence: failure.sequence * 1000 + 3,
        occurredAt: failure.occurredAt,
        runId: failure.runId,
        failure
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
  const activeRunLastTimelineItem = activeRunId
    ? [...timelineItems].reverse().find((entry) => entry.runId === activeRunId)
    : undefined;
  const showThinking = activeRunInProgress && activeRunLastTimelineItem?.kind !== 'activity-group';
  const activeThinking = useMemo(() => {
    if (!activeRunId) return undefined;
    const runEvents = sessionEvents.filter((event) => event.run_id === activeRunId);
    const lastEventAt = runEvents.reduce((max, event) => Math.max(max, eventTimestamp(event.created_at)), 0) || undefined;
    return { stage: runThinkingStage(runEvents), lastEventAt };
  }, [activeRunId, sessionEvents]);

  useLayoutEffect(() => {
    const scroll = chatScrollRef.current;
    if (!scroll) return;
    const anchor = historyAnchorRef.current;
    if (anchor) {
      scroll.scrollTop = anchor.scrollTop + scroll.scrollHeight - anchor.scrollHeight;
      lastChatScrollTopRef.current = scroll.scrollTop;
      historyAnchorRef.current = null;
      historyPagingReadyRef.current = true;
      return;
    }
    if (followBottomRef.current) scroll.scrollTop = scroll.scrollHeight;
    lastChatScrollTopRef.current = scroll.scrollTop;
    if (!messagesLoading) historyPagingReadyRef.current = true;
  }, [messagesLoading, selectedId, showThinking, timelineItems]);

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

  async function sendDraft() {
    const content = draft.trim();
    const pendingConversationDraft = conversationDraft;
    const pendingDraftGeneration = conversationDraftGeneration.current;
    if ((!selectedSession && !pendingConversationDraft) || !content || !canMutate || sending) return;
    setSending(true);
    setActionError(false);
    setConversationCreateError(false);
    try {
      if (pendingConversationDraft) {
        const run = await api.createRun(pendingConversationDraft.agentId, content);
        if (!run.hub_session_id) throw new Error('new conversation did not return a Session id');
        discardConversationDraft(currentUserId, pendingConversationDraft.agentId);
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
    } catch (caught) {
      if (!mountedRef.current) return;
      if (caught instanceof ApiError && caught.code === 'secret_grants_required' && caught.details?.secret_grants_required?.length) {
        const agentId = pendingConversationDraft ? pendingConversationDraft.agentId : selectedSession?.agent_id;
        if (agentId) {
          setSecretGrantRequest({ agentId, requirements: caught.details.secret_grants_required });
          return;
        }
      }
      if (pendingConversationDraft) {
        if (pendingDraftGeneration === conversationDraftGeneration.current) {
          setConversationCreateError(true);
        }
      } else {
        setActionError(true);
      }
    } finally {
      if (mountedRef.current) setSending(false);
    }
  }

  function submitMessage(event: FormEvent) {
    event.preventDefault();
    void sendDraft();
  }

  async function allowSecretGrant() {
    if (!secretGrantRequest || granting) return;
    setGranting(true);
    try {
      await api.createSecretGrants(secretGrantRequest.agentId, secretGrantRequest.requirements.map((requirement) => requirement.name));
      if (!mountedRef.current) return;
      setSecretGrantRequest(null);
      await sendDraft();
    } catch {
      if (mountedRef.current) setActionError(true);
    } finally {
      if (mountedRef.current) setGranting(false);
    }
  }

  function cancelSecretGrant() {
    if (granting) return;
    setSecretGrantRequest(null);
  }

  function selectAgent(agentId: string) {
    saveSelectedSessionAgent(currentUserId, agentId);
    setSelectedAgentId(agentId);
    conversationDraftGeneration.current += 1;
    setConversationDraft(null);
    setConversationCreateError(false);
    setDraft('');
  }

  function openConversationDraft() {
    const agent = agents.find((candidate) => candidate.id === selectedAgentId && candidate.can_invoke);
    if (!agent) return;
    const stored = loadConversationDraft(currentUserId, agent.id);
    if (!stored) saveConversationDraft(currentUserId, agent.id, '');
    setPlatformFilter('hub_native');
    setSearch('');
    conversationDraftGeneration.current += 1;
    setConversationDraft({ agentId: agent.id, agentName: agent.name });
    setConversationCreateError(false);
    setActionError(false);
    setSelectedId(null);
    setDraft(stored?.content ?? '');
    setSessionListOpen(false);
  }

  function discardCurrentDraft() {
    if (!conversationDraft) return;
    discardConversationDraft(currentUserId, conversationDraft.agentId);
    conversationDraftGeneration.current += 1;
    setConversationDraft(null);
    setConversationCreateError(false);
    setDraft('');
  }

  return <section className="session-workspace session-chat-workspace" aria-labelledby="session-page-title">
    <h1 className="sr-only" id="session-page-title">{t('sessions')}</h1>
    {loadError && <div className="operation-alert" role="alert"><span>{t('sessionsLoadFailed')}</span><button type="button" onClick={() => void loadSessions()}>{t('retry')}</button></div>}
    <div className="session-layout">
      <aside className={`session-master${sessionListOpen ? ' open' : ''}`} aria-label={t('sessionList')}>
        <div className="session-master-header">
          <button className="session-new-conversation" type="button" disabled={loading || loadError || sending || !selectedAgentCanInvoke} onClick={openConversationDraft}><Plus size={17} /> <span>{t('newConversation')}</span></button>
          <button className="icon-button session-close-list" type="button" aria-label={t('close')} title={t('close')} onClick={() => setSessionListOpen(false)}><X size={18} /></button>
        </div>
        <div className="session-list-controls">
          <label className="session-origin-filter"><span className="sr-only">{t('sessionOrigin')}</span><select aria-label={t('sessionOrigin')} value={platformFilter} onChange={(event) => setPlatformFilter(event.target.value as PlatformFilter)}><option value="hub_native">{t('hubNative')}</option><option value="all">{t('allOrigins')}</option>{externalPlatforms.map((platform) => <option key={platform.id} value={`external:${platform.id}`}>{platform.name}</option>)}</select><ChevronDown size={14} aria-hidden="true" /></label>
          <label className="session-agent-filter"><span className="sr-only">{t('agent')}</span><select aria-label={t('agent')} value={selectedAgentId ?? ''} disabled={sessionAgentOptions.length === 0} onChange={(event) => selectAgent(event.target.value)}>{sessionAgentOptions.map((agent) => <option key={agent.id} value={agent.id}>{agent.name}</option>)}</select><ChevronDown size={14} aria-hidden="true" /></label>
          <label className="operation-search"><span className="sr-only">{t('searchSessions')}</span><Search size={16} aria-hidden="true" /><input value={search} onChange={(event) => setSearch(event.target.value)} placeholder={t('searchSessions')} /></label>
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
            <span className="session-row-preview"><span>{session.origin.kind === 'hub_native' ? t('hubNative') : session.origin_platform_name ?? t('external')}</span><time>{new Date(session.updated_at).toLocaleString(locale)}</time></span>
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
              <div><h2>{conversationAgentName}</h2><span>{conversationDraft || selectedSession?.origin.kind === 'hub_native' ? t('hubNative') : selectedSession?.origin_platform_name ?? t('external')}</span></div>
            </div>
            {conversationDraft && <button className="icon-button session-discard-draft" type="button" aria-label={t('discardDraft')} title={t('discardDraft')} onClick={discardCurrentDraft}><Trash2 size={16} /></button>}
            {selectedSession && <span className={`status ${selectedSession.lifecycle_status}`}>{lifecycleLabel(selectedSession.lifecycle_status)}</span>}
          </header>
          <div className="session-chat-scroll" ref={chatScrollRef} onScroll={handleChatScroll} onWheel={handleChatWheel} onTouchStart={handleChatTouchStart} onTouchEnd={handleChatTouchEnd}>
            {selectedSession?.lifecycle_status === 'recovery_failed' && <div className="session-banner error" role="alert"><strong>{t('sessionStatusRecoveryFailed')}</strong><span>{selectedSession.recovery_error ?? t('recoveryFailedFallback')}</span></div>}
            {(selectedSession?.lifecycle_status === 'historical' || selectedSession?.agent_deleted_at) && <div className="session-banner"><strong>{t('historicalSession')}</strong><span>{t('historicalSessionHelp')}</span></div>}
            {stopRequestedRunId && <div className="session-banner success" role="status">{t('stopRequested')}</div>}
            {actionError && <div className="session-banner error" role="alert">{t('genericError')}</div>}
            {conversationCreateError && <div className="session-banner error" role="alert">{t('conversationCreateFailed')}</div>}
            <div className="session-transcript" aria-busy={messagesLoading || olderMessagesLoading}>
              {!conversationDraft && messagesLoading && <div className="operation-state" role="status">{t('loadingMessages')}</div>}
              {!conversationDraft && !messagesLoading && messagesError && <div className="operation-state error" role="alert">{t('messagesLoadFailed')}</div>}
              {!conversationDraft && !messagesLoading && !messagesError && timelineItems.length === 0 && <div className="operation-state">{t('noMessages')}</div>}
              {!conversationDraft && timelineItems.map((entry, index) => {
                if (entry.kind === 'activity-group') {
                  const active = activeRunInProgress && entry.runId === activeRunId;
                  if (active && !activityGroupIsClosed(timelineItems, index, entry.runId)) {
                    return <div className="session-live-steps" key={entry.id}>{entry.activities.map((activity) => <ActivityLiveStep activity={activity} key={activity.id} />)}</div>;
                  }
                  const window = activityGroupProcessingWindow(timelineItems, index, runProcessingWindows.get(entry.runId), active);
                  return <ChatActivityGroup
                    active={active && window.endedAt === undefined}
                    activities={entry.activities}
                    clockOffset={serverClockOffset}
                    endedAt={window.endedAt}
                    key={entry.id}
                    startedAt={window.startedAt}
                  />;
                }
                if (entry.kind === 'failure') return <ChatRunFailure failure={entry.failure} key={entry.id} />;
                return <ChatMessageBubble
                  agentName={conversationAgentName}
                  content={entry.content}
                  key={entry.id}
                  role={entry.role}
                  state={entry.kind === 'message' ? entry.state : undefined}
                  stateLabel={entry.kind === 'message' && entry.state ? deliveryLabel(entry.state) : undefined}
                />;
              })}
              {!conversationDraft && showThinking && <ChatThinkingBubble stage={activeThinking?.stage.key ?? 'runStageThinking'} detail={activeThinking?.stage.detail} lastEventAt={activeThinking?.lastEventAt} />}
            </div>
          </div>
          {canMutate && <form className="session-composer session-chat-composer" onSubmit={submitMessage}>
            <label><span className="sr-only">{t('message')}</span><textarea ref={composerRef} rows={2} aria-label={t('message')} value={draft} onChange={(event) => {
              setDraft(event.target.value);
              if (conversationDraft) saveConversationDraft(currentUserId, conversationDraft.agentId, event.target.value);
            }} onInput={(event) => resizeComposer(event.currentTarget)} onKeyDown={(event) => {
              if (event.key !== 'Enter' || event.shiftKey || event.nativeEvent.isComposing) return;
              event.preventDefault();
              event.currentTarget.form?.requestSubmit();
            }} placeholder={selectedSession?.active_turn_id ? t('guideCurrentTurnPlaceholder') : t('messagePlaceholder')} /></label>
            <div><span className="session-composer-actions">{selectedSession?.active_turn_id && activeRunId && <button type="button" className="icon-button session-stop-button" aria-label={t('stopCurrentRun')} title={t('stopCurrentRun')} disabled={stopping || stopRequestedRunId === activeRunId} onClick={stopCurrentRun}><Square size={14} /></button>}<button type="submit" className="icon-button session-send-button" aria-label={sending ? t('sending') : t('send')} title={t('send')} disabled={sending || !draft.trim()}><ArrowUp size={18} /></button></span></div>
          </form>}
        </>}
      </section>
    </div>
    {secretGrantRequest && <FormDialog title={t('secretGrantRequired')} busy={granting} onClose={cancelSecretGrant} className="secret-grant-dialog" footer={<>
      <button type="button" className="secondary" disabled={granting} onClick={cancelSecretGrant}>{t('cancel')}</button>
      <button type="button" className="primary" disabled={granting} onClick={allowSecretGrant}>{granting ? t('saving') : t('allowSecretGrant')}</button>
    </>}>
      <p className="secret-grant-help">{t('secretGrantRequiredHelp')}</p>
      <ul className="secret-grant-requirements">{secretGrantRequest.requirements.map((requirement) => (
        <li key={requirement.name}>
          <code>{requirement.name}</code>
          <span>{requirement.kind === 'file' ? t('secretKindFile') : t('secretKindValue')}</span>
          {requirement.description && <span>{requirement.description}</span>}
        </li>
      ))}</ul>
    </FormDialog>}
  </section>;
}
