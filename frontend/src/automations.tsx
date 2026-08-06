import { Clock, Pencil, Play, Plus, Search } from 'lucide-react';
import { ComponentType, FormEvent, useCallback, useEffect, useRef, useState } from 'react';
import { Agent, api, Automation, Run } from './api/client';
import { FormDialog } from './components/form-dialog';
import { MarkdownEditor } from './components/markdown-editor';
import { useI18n } from './i18n';
import type { TranslationKey } from './i18n';
import './automations.css';

const HISTORY_PAGE_SIZE = 20;
const INTERVAL_PRESETS = ['30s', '1m', '5m', '10m', '15m', '30m', '1h', '2h', '3h', '6h', '12h', '24h'];
const DEFAULT_INTERVAL = '1h';
const DEFAULT_CRON = '0 * * * *';
const ACTIVE_RUN_STATUSES = new Set(['pending', 'running', 'waiting_tool']);

function triggerLabel(triggerType: string, t: ReturnType<typeof useI18n>['t']) {
  const keys = {
    manual: 'manual',
    webhook: 'webhook',
    interval: 'interval',
    cron: 'cron'
  } as const;
  return triggerType in keys ? t(keys[triggerType as keyof typeof keys]) : t('unknownTrigger');
}

function triggerConfiguration(automation: Automation, webhookUrl: string, t: ReturnType<typeof useI18n>['t']) {
  if (automation.trigger_type === 'webhook') return webhookUrl;
  if (automation.trigger_type === 'interval' || automation.trigger_type === 'cron') {
    return automation.schedule || t('none');
  }
  return t('none');
}

function runStatusLabel(status: string, t: ReturnType<typeof useI18n>['t']) {
  const keys = {
    pending: 'statusPending',
    running: 'statusRunning',
    completed: 'statusCompleted',
    failed: 'statusFailed',
    cancelled: 'statusCancelled',
    waiting_tool: 'statusWaitingTool'
  } as const;
  return status in keys ? t(keys[status as keyof typeof keys]) : status;
}

function runSourceLabel(source: string, t: ReturnType<typeof useI18n>['t']) {
  const keys = {
    console: 'runSourceConsole',
    widget: 'runSourceWidget',
    'automation:manual': 'runSourceAutomationManual',
    'automation:scheduler': 'runSourceAutomationScheduler',
    'automation:webhook': 'runSourceAutomationWebhook',
    'integration:message': 'runSourceIntegrationMessage',
    'integration:tool_result': 'runSourceIntegrationToolResult'
  } as const;
  return source in keys ? t(keys[source as keyof typeof keys]) : t('runSourceUnknown');
}

function AutomationFormDialog({
  agents,
  automation,
  onClose,
  onSaved
}: {
  agents: Agent[];
  automation?: Automation;
  onClose: () => void;
  onSaved: (automation: Automation) => void;
}) {
  const { t } = useI18n();
  const editing = Boolean(automation);
  const nameRef = useRef<HTMLInputElement>(null);
  const submittingRef = useRef(false);
  const mountedRef = useRef(true);
  const [agentId, setAgentId] = useState(automation?.agent_id ?? agents[0]?.id ?? '');
  const [name, setName] = useState(automation?.name ?? t('defaultAutomationName'));
  const [triggerType, setTriggerType] = useState(automation?.trigger_type ?? 'manual');
  const [prompt, setPrompt] = useState(automation?.prompt ?? t('defaultAutomationPrompt'));
  const [schedule, setSchedule] = useState(automation?.schedule ?? '');
  const [enabled, setEnabled] = useState(automation?.enabled ?? true);
  const [pending, setPending] = useState(false);
  const [error, setError] = useState(false);

  useEffect(() => () => { mountedRef.current = false; }, []);

  async function submit(event: FormEvent) {
    event.preventDefault();
    if (submittingRef.current || !agentId || !name.trim() || !prompt.trim()) return;
    submittingRef.current = true;
    setPending(true);
    setError(false);
    try {
      const saved = automation
        ? await api.updateAutomation(automation.id, name.trim(), triggerType, prompt, schedule.trim(), enabled)
        : await api.createAutomation(agentId, name.trim(), triggerType, prompt, schedule.trim(), enabled);
      if (mountedRef.current) onSaved(saved);
    } catch {
      if (mountedRef.current) setError(true);
    } finally {
      submittingRef.current = false;
      if (mountedRef.current) setPending(false);
    }
  }

  const scheduleRequired = triggerType === 'interval' || triggerType === 'cron';
  const submitDisabled = pending || !agentId || !name.trim() || !prompt.trim() || (scheduleRequired && !schedule.trim());

  return <FormDialog
    title={t(editing ? 'editAutomation' : 'createAutomation')}
    eyebrow={t('automations')}
    onClose={onClose}
    busy={pending}
    initialFocusRef={nameRef}
    className="automation-form-dialog"
    footer={<><button className="secondary" type="button" disabled={pending} onClick={onClose}>{t('cancel')}</button><button className="primary" form="automation-form" type="submit" disabled={submitDisabled}>{pending ? t('saving') : t(editing ? 'saveChanges' : 'createAutomationAction')}</button></>}
  >
    <form id="automation-form" className="automation-dialog-form" onSubmit={submit}>
      <label>{t('agent')}<select aria-label={t('agent')} disabled={editing || pending} required value={agentId} onChange={(event) => setAgentId(event.target.value)}>{agents.map((agent) => <option key={agent.id} value={agent.id}>{agent.name}</option>)}</select></label>
      <label>{t('name')}<input ref={nameRef} aria-label={t('name')} disabled={pending} required value={name} onChange={(event) => setName(event.target.value)} /></label>
      <label>{t('trigger')}<select aria-label={t('trigger')} disabled={pending} value={triggerType} onChange={(event) => { const next = event.target.value; setTriggerType(next); setSchedule(next === 'interval' ? DEFAULT_INTERVAL : next === 'cron' ? DEFAULT_CRON : ''); }}><option value="manual">{t('manual')}</option><option value="webhook">{t('webhook')}</option><option value="interval">{t('interval')}</option><option value="cron">{t('cron')}</option></select></label>
      <MarkdownEditor className="automation-prompt-field" label={t('prompt')} disabled={pending} required value={prompt} onChange={setPrompt} />
      {triggerType === 'interval' && <label>{t('schedule')}<select aria-label={t('schedule')} disabled={pending} required value={schedule} onChange={(event) => setSchedule(event.target.value)}>{INTERVAL_PRESETS.includes(schedule) ? INTERVAL_PRESETS.map((preset) => <option key={preset} value={preset}>{preset}</option>) : <><option value={schedule}>{schedule}</option>{INTERVAL_PRESETS.map((preset) => <option key={preset} value={preset}>{preset}</option>)}</>}</select></label>}
      {triggerType === 'cron' && <label>{t('schedule')}<input aria-label={t('schedule')} disabled={pending} required value={schedule} onChange={(event) => setSchedule(event.target.value)} placeholder={t('scheduleCronHint')} /></label>}
      <label className="check-row"><input aria-label={t('enabled')} disabled={pending} type="checkbox" checked={enabled} onChange={(event) => setEnabled(event.target.checked)} /> {t('enabled')}</label>
      {error && <div className="error" role="alert">{t('automationSaveFailed')}</div>}
    </form>
  </FormDialog>;
}

type AutomationDialog = { kind: 'create' } | { kind: 'edit'; automation: Automation } | null;
type WebhookSecret = { automation: Automation; token: string } | null;

export function AutomationsPage({ RunConsole }: { RunConsole: ComponentType<{ run: Run }> }) {
  const { locale, t } = useI18n();
  const [agents, setAgents] = useState<Agent[]>([]);
  const [automations, setAutomations] = useState<Automation[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [filter, setFilter] = useState('');
  const [dialog, setDialog] = useState<AutomationDialog>(null);
  const [webhookSecret, setWebhookSecret] = useState<WebhookSecret>(null);
  const [selectedRun, setSelectedRun] = useState<Run | null>(null);
  const [historyRuns, setHistoryRuns] = useState<Run[]>([]);
  const [historyPage, setHistoryPage] = useState(1);
  const [historyTotal, setHistoryTotal] = useState(0);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [historyError, setHistoryError] = useState(false);
  const [historyRefresh, setHistoryRefresh] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<TranslationKey | null>(null);
  const [notice, setNotice] = useState<TranslationKey | null>(null);
  const [triggeringId, setTriggeringId] = useState<string | null>(null);
  const loadGeneration = useRef(0);
  const selectionGeneration = useRef(0);
  const mountedRef = useRef(true);

  const load = useCallback(async () => {
    const generation = ++loadGeneration.current;
    setLoading(true);
    try {
      const [user, loadedAgents, loadedAutomations] = await Promise.all([api.me(), api.agents(), api.automations()]);
      if (generation !== loadGeneration.current) return;
      setAgents(loadedAgents.filter((agent) => agent.owner_id === user.id));
      setAutomations(loadedAutomations.map((automation) => ({ ...automation, webhook_token: null })));
      setError(null);
    } catch {
      if (generation === loadGeneration.current) setError('automationLoadFailed');
    } finally {
      if (generation === loadGeneration.current) setLoading(false);
    }
  }, []);

  useEffect(() => {
    load();
    return () => {
      mountedRef.current = false;
      loadGeneration.current += 1;
    };
  }, [load]);

  useEffect(() => {
    if (!selectedId) {
      setHistoryRuns([]);
      setHistoryTotal(0);
      setHistoryLoading(false);
      setHistoryError(false);
      return;
    }
    let active = true;
    let timer: number | undefined;
    const controller = new AbortController();
    setHistoryLoading(true);
    setHistoryError(false);
    const poll = async () => {
      try {
        const response = await api.automationRuns(selectedId, historyPage, HISTORY_PAGE_SIZE, controller.signal);
        if (!active) return;
        setHistoryRuns(response.items);
        setHistoryTotal(response.total);
        setHistoryError(false);
        setHistoryLoading(false);
        if (response.items.some((run) => ACTIVE_RUN_STATUSES.has(run.status))) {
          timer = window.setTimeout(poll, 2000);
        }
      } catch {
        if (!active || controller.signal.aborted) return;
        setHistoryLoading(false);
        setHistoryError(true);
      }
    };
    poll();
    return () => {
      active = false;
      controller.abort();
      if (timer !== undefined) window.clearTimeout(timer);
    };
  }, [historyPage, historyRefresh, selectedId]);

  function selectAutomation(automation: Automation) {
    if (selectedId === automation.id) return;
    selectionGeneration.current += 1;
    setSelectedId(automation.id);
    setHistoryPage(1);
    setHistoryRuns([]);
    setHistoryTotal(0);
    setSelectedRun(null);
    setHistoryError(false);
  }

  function saveAutomation(saved: Automation) {
    const secret = saved.webhook_token ? { automation: { ...saved, webhook_token: null }, token: saved.webhook_token } : null;
    const safe = { ...saved, webhook_token: null };
    setAutomations((current) => current.some((automation) => automation.id === safe.id)
      ? current.map((automation) => automation.id === safe.id ? safe : automation)
      : [safe, ...current]);
    selectionGeneration.current += 1;
    setSelectedId(safe.id);
    setHistoryPage(1);
    setHistoryRuns([]);
    setHistoryTotal(0);
    setSelectedRun(null);
    setHistoryRefresh((value) => value + 1);
    setDialog(null);
    setWebhookSecret(secret);
    setError(null);
    setNotice('changesSaved');
  }

  async function trigger(automation: Automation, webhookToken?: string) {
    if (triggeringId) return;
    const triggerGeneration = selectionGeneration.current;
    setTriggeringId(automation.id);
    setError(null);
    setNotice(null);
    try {
      if (webhookToken) await api.triggerAutomationWebhook(webhookToken);
      else await api.triggerAutomation(automation.id);
    } catch {
      if (mountedRef.current) setError('automationRunFailed');
      return;
    } finally {
      if (mountedRef.current) setTriggeringId(null);
    }
    if (!mountedRef.current) return;
    if (selectionGeneration.current === triggerGeneration) {
      selectionGeneration.current += 1;
      setSelectedId(automation.id);
      setHistoryPage(1);
      setHistoryRuns([]);
      setHistoryTotal(0);
      setSelectedRun(null);
      setHistoryRefresh((value) => value + 1);
    }
    try {
      const refreshed = await api.automations();
      if (mountedRef.current) setAutomations(refreshed.map((item) => ({ ...item, webhook_token: null })));
    } catch {
      if (mountedRef.current) setError('automationLoadFailed');
    }
  }

  const selected = automations.find((automation) => automation.id === selectedId) ?? null;
  const normalizedFilter = filter.trim().toLocaleLowerCase();
  const filtered = automations.filter((automation) => {
    const agentName = agents.find((agent) => agent.id === automation.agent_id)?.name ?? '';
    return `${automation.name} ${agentName}`.toLocaleLowerCase().includes(normalizedFilter);
  });
  const webhookUrl = `${window.location.origin}/api/automations/webhook`;
  const historyTotalPages = Math.max(1, Math.ceil(historyTotal / HISTORY_PAGE_SIZE));
  const historyPageLabel = t('pageOf').replace('{page}', String(historyPage)).replace('{total}', String(historyTotalPages));

  return <div className="workspace-page automations-page">
    <header className="page-header automations-header"><div><h1>{t('automations')}</h1><p>{t('automationSubtitle')}</p></div><button className="primary" type="button" onClick={() => { setNotice(null); setDialog({ kind: 'create' }); }}><Plus size={16} /> {t('newAutomation')}</button></header>

    <section className="automation-list" aria-label={t('list')}>
      <div className="automation-list-toolbar">
        <label className="search-field automation-search"><span className="sr-only">{t('filterAutomations')}</span><Search size={16} /><input aria-label={t('filterAutomations')} placeholder={t('searchAutomations')} value={filter} onChange={(event) => setFilter(event.target.value)} /></label>
        {notice && <span className="success" role="status">{t(notice)}</span>}
      </div>
      {error && <div className="error automation-page-error" role="alert">{t(error)} {error === 'automationLoadFailed' && <button type="button" className="text-button" onClick={load}>{t('retry')}</button>}</div>}
      {loading && automations.length === 0 && <div className="automation-list-state" role="status">{t('loading')}</div>}
      {!loading && filtered.length === 0 && <div className="automation-list-state">{t('noAutomations')}</div>}
      {filtered.length > 0 && <div className="automation-list-rows">{filtered.map((automation) => {
        const agent = agents.find((item) => item.id === automation.agent_id);
        const editLabel = `${t('editAutomation')} ${automation.name}`;
        const configuration = triggerConfiguration(automation, webhookUrl, t);
        return <article className={`automation-list-row${selectedId === automation.id ? ' selected' : ''}`} data-automation-id={automation.id} key={automation.id}>
          <button type="button" className="automation-select" aria-pressed={selectedId === automation.id} onClick={() => selectAutomation(automation)}>
            <span className="automation-name"><strong>{automation.name}</strong><span className={`status-dot ${automation.enabled ? 'on' : ''}`} /></span>
            <span className="automation-agent"><small>{t('agent')}</small><strong>{agent?.name ?? t('agent')}</strong></span>
            <span><small>{t('trigger')}</small><strong>{triggerLabel(automation.trigger_type, t)}</strong></span>
            <span className="automation-trigger-config"><small>{t('triggerConfiguration')}</small><strong title={configuration}>{configuration}</strong></span>
            <span><small>{t('status')}</small><strong>{automation.enabled ? t('enabledStatus') : t('disabledStatus')}</strong></span>
            <span><small>{t('lastRun')}</small><strong>{automation.last_triggered_at ? new Date(automation.last_triggered_at).toLocaleString(locale) : t('neverTriggered')}</strong></span>
          </button>
          <div className="automation-row-actions">
            {automation.enabled && automation.trigger_type === 'manual' && <button className="secondary compact-action" type="button" disabled={triggeringId === automation.id} onClick={() => trigger(automation)}><Play size={15} /> {t('runNow')}</button>}
            <button className="icon-button" type="button" aria-label={editLabel} title={editLabel} onClick={() => { setNotice(null); setDialog({ kind: 'edit', automation }); }}><Pencil size={16} /></button>
          </div>
        </article>;
      })}</div>}
    </section>

    {selected && <section className="automation-inspector" aria-label={t('details')}>
      <div className="automation-inspector-header"><div><span className="eyebrow">{t('automationWorkspace')}</span><h2>{selected.name}</h2></div><button className="secondary" type="button" onClick={() => setDialog({ kind: 'edit', automation: selected })}><Pencil size={15} /> {t('editAutomation')}</button></div>
      <div className="automation-inspector-meta"><span><small>{t('agent')}</small><strong>{agents.find((agent) => agent.id === selected.agent_id)?.name ?? t('agent')}</strong></span><span><small>{t('trigger')}</small><strong>{triggerLabel(selected.trigger_type, t)}</strong></span><span><small>{t('status')}</small><strong>{selected.enabled ? t('enabledStatus') : t('disabledStatus')}</strong></span>{selected.trigger_type === 'webhook' && <span className="automation-inspector-endpoint"><small>{t('webhookEndpoint')}</small><code>{webhookUrl}</code></span>}</div>
      <div className="automation-history" role="region" aria-label={t('runHistory')}>
        <div className="automation-history-header"><div className="section-title"><Clock size={18} /> {t('runHistory')}</div></div>
        {historyLoading && historyRuns.length === 0 && <div className="automation-history-state">{t('loadingRunHistory')}</div>}
        {historyError && <div className="automation-history-state error">{t('automationHistoryLoadFailed')} <button type="button" className="text-button" onClick={() => setHistoryRefresh((value) => value + 1)}>{t('retry')}</button></div>}
        {!historyLoading && !historyError && historyRuns.length === 0 && <div className="automation-history-state">{t('noAutomationRuns')}</div>}
        {historyRuns.length > 0 && <div className="automation-history-list">{historyRuns.map((run) => <button type="button" className={`automation-history-row${selectedRun?.id === run.id ? ' selected' : ''}`} data-run-id={run.id} key={run.id} onClick={() => setSelectedRun(run)}><span><strong>{runStatusLabel(run.status, t)}</strong><small>{t('status')}</small></span><span><strong>{runSourceLabel(run.source, t)}</strong><small>{t('source')}</small></span><span className="automation-history-message"><strong>{run.initial_message}</strong><small>{t('initialMessage')}</small></span><span><strong>{new Date(run.created_at).toLocaleString(locale)}</strong><small>{t('created')}</small></span><span><strong>{new Date(run.updated_at).toLocaleString(locale)}</strong><small>{t('updated')}</small></span></button>)}</div>}
        {historyTotal > HISTORY_PAGE_SIZE && <div className="pagination"><button type="button" className="secondary" disabled={historyPage <= 1 || historyLoading} onClick={() => { setSelectedRun(null); setHistoryPage((value) => Math.max(1, value - 1)); }}>{t('previous')}</button><span>{historyPageLabel}</span><button type="button" className="secondary" disabled={historyPage >= historyTotalPages || historyLoading} onClick={() => { setSelectedRun(null); setHistoryPage((value) => value + 1); }}>{t('next')}</button></div>}
      </div>
      <section className="console-band">{selectedRun ? <RunConsole run={selectedRun} /> : <div className="empty compact-empty">{t('noAutomationRun')}</div>}</section>
    </section>}

    {dialog && <AutomationFormDialog key={dialog.kind === 'edit' ? dialog.automation.id : 'create'} agents={agents} automation={dialog.kind === 'edit' ? dialog.automation : undefined} onClose={() => setDialog(null)} onSaved={saveAutomation} />}
    {webhookSecret && <FormDialog title={t('webhookToken')} eyebrow={webhookSecret.automation.name} onClose={() => setWebhookSecret(null)} footer={<><button className="secondary" type="button" onClick={() => setWebhookSecret(null)}>{t('close')}</button>{webhookSecret.automation.enabled && <button className="primary" type="button" disabled={triggeringId === webhookSecret.automation.id} onClick={() => trigger(webhookSecret.automation, webhookSecret.token)}><Play size={15} /> {t('triggerWebhook')}</button>}</>}><div className="automation-secret-result"><span>{t('shownOnce')}</span><span>{t('webhookEndpoint')}</span><code>{webhookUrl}</code><span>{t('webhookToken')}</span><code className="secret-token" data-testid="webhook-token">{webhookSecret.token}</code></div></FormDialog>}
  </div>;
}
