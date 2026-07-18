import {
  Bot,
  CheckCircle2,
  ChevronLeft,
  ChevronRight,
  CircleOff,
  Database,
  FlaskConical,
  Gauge,
  Globe2,
  KeyRound,
  Pencil,
  Plus,
  Power,
  PowerOff,
  RefreshCw,
  Save,
  ShieldAlert,
  Star,
  StarOff,
  Trash2,
  UserRound,
  Users,
  X
} from 'lucide-react';
import { FormEvent, KeyboardEvent, useEffect, useMemo, useState } from 'react';
import {
  api,
  type Agent,
  type CreateModelConnectionRequest,
  type ModelCallErrorPage,
  type ModelConnection,
  type ModelLedgerQuery,
  type ModelTokenUsagePage,
  type ModelUsageSummary,
  type UpdateModelConnectionRequest,
  type User
} from './api/client';
import { FormDialog } from './components/form-dialog';
import { useI18n } from './i18n';
import type { TranslationKey } from './i18n';
import './models.css';

type ModelsTab = 'personal' | 'available' | 'usage' | 'global';
type UsageRange = 'today' | 'yesterday' | '7days' | '30days' | '90days' | 'all';
type ConnectionDialog =
  | { kind: 'create'; scope: 'personal' | 'global' }
  | { kind: 'edit'; connection: ModelConnection }
  | { kind: 'test'; connection: ModelConnection }
  | { kind: 'status'; connection: ModelConnection }
  | { kind: 'default'; connection: ModelConnection }
  | { kind: 'delete'; connection: ModelConnection }
  | { kind: 'force-delete'; connection: ModelConnection };

type ModelTranslationKey =
  | 'modelsSubtitle'
  | 'myModels'
  | 'availableModels'
  | 'modelUsage'
  | 'globalModels'
  | 'createPersonalModel'
  | 'createGlobalModel'
  | 'modelConnectionList'
  | 'availableModelList'
  | 'globalModelConnectionList'
  | 'modelBaseUrl'
  | 'modelId'
  | 'modelScope'
  | 'systemDefault'
  | 'modelConnectionActions'
  | 'noPersonalModels'
  | 'noAvailableModels'
  | 'noGlobalModels'
  | 'loadingModels'
  | 'modelsLoadFailed'
  | 'createModelConnection'
  | 'editModelConnection'
  | 'modelConnectionName'
  | 'modelApiKey'
  | 'modelApiKeyCreateHelp'
  | 'modelApiKeyEditHelp'
  | 'modelSaveFailed'
  | 'editModelConnectionAria'
  | 'testModelConnectionAria'
  | 'enableModelConnectionAria'
  | 'disableModelConnectionAria'
  | 'deleteModelConnectionAria'
  | 'forceDeleteModelConnectionAria'
  | 'setSystemDefaultAria'
  | 'clearSystemDefaultAria'
  | 'testModelConnection'
  | 'runConnectionTest'
  | 'testingModelConnection'
  | 'modelTestSucceeded'
  | 'modelTestFailed'
  | 'enableModelConnection'
  | 'disableModelConnection'
  | 'confirmEnableModel'
  | 'confirmDisableModel'
  | 'setSystemDefault'
  | 'clearSystemDefault'
  | 'confirmSetSystemDefault'
  | 'confirmClearSystemDefault'
  | 'deleteModelConnection'
  | 'forceDeleteModelConnection'
  | 'confirmDeleteModel'
  | 'confirmForceDeleteModel'
  | 'modelActionFailed'
  | 'usageRange'
  | 'rangeToday'
  | 'rangeYesterday'
  | 'range7Days'
  | 'range30Days'
  | 'range90Days'
  | 'rangeAll'
  | 'filterByModel'
  | 'allModels'
  | 'filterByAgent'
  | 'allAgents'
  | 'filterByUser'
  | 'allUsers'
  | 'modelUsageLoadFailed'
  | 'modelUsageOverall'
  | 'modelUsageByModel'
  | 'modelUsageByAgent'
  | 'modelUsageByUser'
  | 'inputTokens'
  | 'outputTokens'
  | 'cachedTokens'
  | 'reasoningTokens'
  | 'totalTokens'
  | 'usageDetails'
  | 'errorDetails'
  | 'usageTime'
  | 'usageSubject'
  | 'usageModel'
  | 'usageAgent'
  | 'noModelUsage'
  | 'noModelErrors'
  | 'anonymousUser'
  | 'integrationSubject'
  | 'systemSubject'
  | 'upstreamStatus'
  | 'errorCode'
  | 'errorMessage'
  | 'previousUsagePage'
  | 'nextUsagePage'
  | 'previousErrorPage'
  | 'nextErrorPage';

function useModelI18n() {
  const i18n = useI18n();
  return {
    ...i18n,
    mt: (key: ModelTranslationKey) => i18n.t(key as TranslationKey)
  };
}

function formatMessage(template: string, values: Record<string, string>) {
  return Object.entries(values).reduce(
    (message, [key, value]) => message.replaceAll(`{${key}}`, value),
    template
  );
}

function modelRange(range: UsageRange, now = new Date()): Pick<ModelLedgerQuery, 'from_ms' | 'to_ms'> {
  if (range === 'all') return {};
  const today = new Date(now);
  today.setHours(0, 0, 0, 0);
  if (range === 'yesterday') {
    const yesterday = new Date(today);
    yesterday.setDate(yesterday.getDate() - 1);
    return { from_ms: yesterday.getTime(), to_ms: today.getTime() };
  }
  if (range === 'today') return { from_ms: today.getTime(), to_ms: now.getTime() };
  const days = Number.parseInt(range, 10);
  const start = new Date(today);
  start.setDate(start.getDate() - (days - 1));
  return { from_ms: start.getTime(), to_ms: now.getTime() };
}

function ConnectionFormDialog({
  state,
  onClose,
  onSaved
}: {
  state: Extract<ConnectionDialog, { kind: 'create' | 'edit' }>;
  onClose: () => void;
  onSaved: (connection: ModelConnection) => void;
}) {
  const { t, mt } = useModelI18n();
  const connection = state.kind === 'edit' ? state.connection : null;
  const [name, setName] = useState(connection?.name ?? '');
  const [baseUrl, setBaseUrl] = useState(connection?.base_url ?? '');
  const [modelId, setModelId] = useState(connection?.model_id ?? '');
  const [apiKey, setApiKey] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState(false);
  const valid = Boolean(name.trim() && baseUrl.trim() && modelId.trim() && (connection || apiKey.trim()));

  async function save(event: FormEvent) {
    event.preventDefault();
    if (busy || !valid) return;
    setBusy(true);
    setError(false);
    try {
      if (state.kind === 'edit') {
        const request: UpdateModelConnectionRequest = {
          name: name.trim(),
          base_url: baseUrl.trim(),
          model_id: modelId.trim(),
          ...(apiKey.trim() ? { api_key: apiKey } : {})
        };
        onSaved(await api.updateModelConnection(state.connection.id, request));
      } else {
        const request: CreateModelConnectionRequest = {
          scope: state.scope,
          name: name.trim(),
          base_url: baseUrl.trim(),
          model_id: modelId.trim(),
          api_key: apiKey
        };
        onSaved(await api.createModelConnection(request));
      }
    } catch {
      setError(true);
    } finally {
      setBusy(false);
    }
  }

  return <FormDialog
    title={connection ? mt('editModelConnection') : mt('createModelConnection')}
    eyebrow={connection?.name}
    onClose={onClose}
    busy={busy}
    className="model-connection-dialog"
    footer={<>
      <button className="secondary" type="button" disabled={busy} onClick={onClose}><X size={16} /> {t('cancel')}</button>
      <button className="primary" type="submit" form="model-connection-form" disabled={busy || !valid}>{connection ? <Save size={16} /> : <Plus size={16} />} {busy ? t('saving') : connection ? t('saveChanges') : mt('createModelConnection')}</button>
    </>}
  >
    <form id="model-connection-form" className="model-connection-form" onSubmit={save}>
      <label>{mt('modelConnectionName')}<input autoComplete="off" required value={name} onChange={(event) => setName(event.target.value)} /></label>
      <label>{mt('modelBaseUrl')}<input type="url" inputMode="url" autoComplete="url" required value={baseUrl} onChange={(event) => setBaseUrl(event.target.value)} /></label>
      <label>{mt('modelId')}<input autoComplete="off" required value={modelId} onChange={(event) => setModelId(event.target.value)} /></label>
      <label>{mt('modelApiKey')}<input type="password" autoComplete="new-password" required={!connection} value={apiKey} onChange={(event) => setApiKey(event.target.value)} /><small>{connection ? mt('modelApiKeyEditHelp') : mt('modelApiKeyCreateHelp')}</small></label>
      {error && <div className="model-alert error" role="alert">{mt('modelSaveFailed')}</div>}
    </form>
  </FormDialog>;
}

function ConnectionActionDialog({
  state,
  onClose,
  onUpdated,
  onRemoved,
  onDefaultChanged
}: {
  state: Exclude<ConnectionDialog, { kind: 'create' | 'edit' }>;
  onClose: () => void;
  onUpdated: (connection: ModelConnection) => void;
  onRemoved: (connectionId: string) => void;
  onDefaultChanged: (connectionId: string | null) => void;
}) {
  const { t, mt } = useModelI18n();
  const { connection } = state;
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState(false);
  const [testResult, setTestResult] = useState<{ success: boolean; status_code: number | null; error_code: string | null; message: string | null } | null>(null);
  const enabling = connection.status === 'disabled';
  const settingDefault = !connection.is_system_default;

  const title = state.kind === 'test'
    ? mt('testModelConnection')
    : state.kind === 'status'
      ? enabling ? mt('enableModelConnection') : mt('disableModelConnection')
      : state.kind === 'default'
        ? settingDefault ? mt('setSystemDefault') : mt('clearSystemDefault')
        : state.kind === 'delete'
          ? mt('deleteModelConnection')
          : mt('forceDeleteModelConnection');

  const message = state.kind === 'status'
    ? enabling ? mt('confirmEnableModel') : mt('confirmDisableModel')
    : state.kind === 'default'
      ? settingDefault ? mt('confirmSetSystemDefault') : mt('confirmClearSystemDefault')
      : state.kind === 'delete'
        ? mt('confirmDeleteModel')
        : state.kind === 'force-delete'
          ? mt('confirmForceDeleteModel')
          : '';

  async function perform() {
    if (busy) return;
    setBusy(true);
    setError(false);
    try {
      if (state.kind === 'test') {
        setTestResult(await api.testModelConnection(connection.id));
        return;
      }
      if (state.kind === 'status') {
        onUpdated(await api.setModelConnectionStatus(connection.id, enabling ? 'enabled' : 'disabled'));
      } else if (state.kind === 'default') {
        const nextDefault = settingDefault ? connection.id : null;
        await api.setSystemDefaultModelConnection(nextDefault);
        onDefaultChanged(nextDefault);
      } else if (state.kind === 'delete') {
        await api.deleteModelConnection(connection.id);
        onRemoved(connection.id);
      } else {
        await api.forceDeleteModelConnection(connection.id);
        onRemoved(connection.id);
      }
      onClose();
    } catch {
      setError(true);
    } finally {
      setBusy(false);
    }
  }

  const actionIcon = state.kind === 'test'
    ? <FlaskConical size={16} />
    : state.kind === 'status'
      ? enabling ? <Power size={16} /> : <PowerOff size={16} />
      : state.kind === 'default'
        ? settingDefault ? <Star size={16} /> : <StarOff size={16} />
        : state.kind === 'delete'
          ? <Trash2 size={16} />
          : <ShieldAlert size={16} />;
  const actionLabel = state.kind === 'test'
    ? busy ? mt('testingModelConnection') : mt('runConnectionTest')
    : title;

  return <FormDialog
    title={title}
    eyebrow={connection.name}
    onClose={onClose}
    busy={busy}
    className="model-action-dialog"
    footer={<>
      <button className="secondary" type="button" disabled={busy} onClick={onClose}><X size={16} /> {state.kind === 'test' && testResult ? t('close') : t('cancel')}</button>
      <button className={state.kind === 'delete' || state.kind === 'force-delete' ? 'secondary danger' : 'primary'} type="button" disabled={busy} onClick={perform}>{actionIcon} {actionLabel}</button>
    </>}
  >
    {message && <p className={state.kind === 'force-delete' ? 'model-danger-copy' : ''}>{formatMessage(message, { name: connection.name })}</p>}
    {state.kind === 'test' && <p className="model-test-copy"><code>{connection.model_id}</code><span>{connection.base_url}</span></p>}
    {testResult && <div className={`model-test-result ${testResult.success ? 'success' : 'error'}`} role="status">
      {testResult.success ? <CheckCircle2 size={18} /> : <CircleOff size={18} />}
      <div><strong>{testResult.success ? mt('modelTestSucceeded') : mt('modelTestFailed')}</strong>{testResult.status_code !== null && <span>HTTP {testResult.status_code}</span>}{testResult.error_code && <code>{testResult.error_code}</code>}{testResult.message && <span>{testResult.message}</span>}</div>
    </div>}
    {error && <div className="model-alert error" role="alert">{mt('modelActionFailed')}</div>}
  </FormDialog>;
}

function actionLabel(template: string, connection: ModelConnection) {
  return formatMessage(template, { name: connection.name });
}

function ConnectionsTable({
  title,
  tableLabel,
  emptyMessage,
  connections,
  managementScope,
  onOpen
}: {
  title: string;
  tableLabel: string;
  emptyMessage: string;
  connections: ModelConnection[];
  managementScope?: 'personal' | 'global';
  onOpen: (dialog: ConnectionDialog) => void;
}) {
  const { t, mt } = useModelI18n();
  return <section className="models-table-section" aria-label={title}>
    <div className="models-table-toolbar">
      <h2>{title}</h2>
      {managementScope && <button className="primary" type="button" onClick={() => onOpen({ kind: 'create', scope: managementScope })}><Plus size={16} /> {managementScope === 'personal' ? mt('createPersonalModel') : mt('createGlobalModel')}</button>}
    </div>
    <div className="models-table-wrap">
      <table className="models-table" aria-label={tableLabel}>
        <thead><tr><th>{t('name')}</th><th>{mt('modelId')}</th><th>{mt('modelBaseUrl')}</th><th>{mt('modelScope')}</th><th>{t('status')}</th><th>{mt('systemDefault')}</th>{managementScope && <th>{mt('modelConnectionActions')}</th>}</tr></thead>
        <tbody>{connections.length === 0 ? <tr><td className="models-empty-cell" colSpan={managementScope ? 7 : 6}>{emptyMessage}</td></tr> : connections.map((connection) => {
          const editLabel = actionLabel(mt('editModelConnectionAria'), connection);
          const testLabel = actionLabel(mt('testModelConnectionAria'), connection);
          const statusLabel = actionLabel(connection.status === 'enabled' ? mt('disableModelConnectionAria') : mt('enableModelConnectionAria'), connection);
          const defaultLabel = actionLabel(connection.is_system_default ? mt('clearSystemDefaultAria') : mt('setSystemDefaultAria'), connection);
          const deleteLabel = actionLabel(mt('deleteModelConnectionAria'), connection);
          const forceDeleteLabel = actionLabel(mt('forceDeleteModelConnectionAria'), connection);
          return <tr key={connection.id}>
            <td><strong>{connection.name}</strong></td>
            <td><code>{connection.model_id}</code></td>
            <td><code className="model-url">{connection.base_url}</code></td>
            <td><span className={`model-scope ${connection.scope}`}>{t((connection.scope === 'global' ? 'modelScopeGlobal' : 'modelScopePersonal') as TranslationKey)}</span></td>
            <td><span className={`status ${connection.status}`}>{connection.status === 'enabled' ? t('enabled') : t('disabled')}</span></td>
            <td>{connection.is_system_default ? <span className="model-default-mark"><Star size={14} /> {mt('systemDefault')}</span> : t('none')}</td>
            {managementScope && <td><div className="models-table-actions">
              <button className="icon-button" type="button" aria-label={editLabel} title={editLabel} onClick={() => onOpen({ kind: 'edit', connection })}><Pencil size={16} /></button>
              <button className="icon-button" type="button" aria-label={testLabel} title={testLabel} onClick={() => onOpen({ kind: 'test', connection })}><FlaskConical size={16} /></button>
              <button className="icon-button" type="button" aria-label={statusLabel} title={statusLabel} onClick={() => onOpen({ kind: 'status', connection })}>{connection.status === 'enabled' ? <PowerOff size={16} /> : <Power size={16} />}</button>
              {managementScope === 'global' && <button className="icon-button" type="button" aria-label={defaultLabel} title={defaultLabel} disabled={!connection.is_system_default && connection.status !== 'enabled'} onClick={() => onOpen({ kind: 'default', connection })}>{connection.is_system_default ? <StarOff size={16} /> : <Star size={16} />}</button>}
              <button className="icon-button model-delete-action" type="button" aria-label={deleteLabel} title={deleteLabel} onClick={() => onOpen({ kind: 'delete', connection })}><Trash2 size={16} /></button>
              <button className="icon-button model-force-action" type="button" aria-label={forceDeleteLabel} title={forceDeleteLabel} onClick={() => onOpen({ kind: 'force-delete', connection })}><ShieldAlert size={16} /></button>
            </div></td>}
          </tr>;
        })}</tbody>
      </table>
    </div>
  </section>;
}

type SummaryTotals = ModelUsageSummary['overall'];

function TokenHeader() {
  const { mt } = useModelI18n();
  return <><th>{mt('inputTokens')}</th><th>{mt('outputTokens')}</th><th>{mt('cachedTokens')}</th><th>{mt('reasoningTokens')}</th><th>{mt('totalTokens')}</th></>;
}

function TokenCells({ totals }: { totals: SummaryTotals }) {
  const { locale } = useModelI18n();
  return <><td>{totals.input_tokens.toLocaleString(locale)}</td><td>{totals.output_tokens.toLocaleString(locale)}</td><td>{totals.cached_tokens.toLocaleString(locale)}</td><td>{totals.reasoning_tokens.toLocaleString(locale)}</td><td><strong>{totals.total_tokens.toLocaleString(locale)}</strong></td></>;
}

function SummaryTable({
  title,
  rows
}: {
  title: string;
  rows: Array<{ id: string; label: React.ReactNode; totals: SummaryTotals }>;
}) {
  const { t } = useModelI18n();
  return <section className="model-summary-group" aria-label={title}>
    <h3>{title}</h3>
    <div className="model-summary-table-wrap"><table><thead><tr><th>{t('name')}</th><TokenHeader /></tr></thead><tbody>{rows.length === 0 ? <tr><td colSpan={6}>{t('none')}</td></tr> : rows.map((row) => <tr key={row.id}><td>{row.label}</td><TokenCells totals={row.totals} /></tr>)}</tbody></table></div>
  </section>;
}

function UsageSummary({ query }: { query: ModelLedgerQuery }) {
  const { locale, t, mt } = useModelI18n();
  const [summary, setSummary] = useState<ModelUsageSummary | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);
  const [reload, setReload] = useState(0);

  useEffect(() => {
    const controller = new AbortController();
    setLoading(true);
    setError(false);
    api.modelUsageSummary(query, controller.signal)
      .then(setSummary)
      .catch((reason) => { if (reason?.name !== 'AbortError') setError(true); })
      .finally(() => { if (!controller.signal.aborted) setLoading(false); });
    return () => controller.abort();
  }, [query, reload]);

  if (loading) return <section className="models-usage-section model-inline-state" aria-label={mt('modelUsageOverall')} role="status">{t('loadingUsage')}</section>;
  if (error || !summary) return <section className="models-usage-section model-inline-state error" aria-label={mt('modelUsageOverall')} role="alert"><p>{mt('modelUsageLoadFailed')}</p><button className="secondary" type="button" onClick={() => setReload((value) => value + 1)}><RefreshCw size={16} /> {t('retry')}</button></section>;

  const totals = [
    [mt('inputTokens'), summary.overall.input_tokens],
    [mt('outputTokens'), summary.overall.output_tokens],
    [mt('cachedTokens'), summary.overall.cached_tokens],
    [mt('reasoningTokens'), summary.overall.reasoning_tokens],
    [mt('totalTokens'), summary.overall.total_tokens]
  ] as const;
  return <section className="models-usage-section" aria-labelledby="model-usage-summary-title">
    <h2 id="model-usage-summary-title">{mt('modelUsageOverall')}</h2>
    <dl className="model-overall-totals">{totals.map(([label, value]) => <div key={label}><dt>{label}</dt><dd>{value.toLocaleString(locale)}</dd></div>)}</dl>
    <div className="model-summary-groups">
      <SummaryTable title={mt('modelUsageByModel')} rows={summary.by_model.map((row, index) => ({ id: row.model.id ?? `model-${index}`, label: <span className="model-summary-name"><strong>{row.model.name}</strong><code>{row.model.model_id}</code></span>, totals: row.totals }))} />
      <SummaryTable title={mt('modelUsageByAgent')} rows={summary.by_agent.map((row, index) => ({ id: row.agent.id ?? `agent-${index}`, label: row.agent.name, totals: row.totals }))} />
      <SummaryTable title={mt('modelUsageByUser')} rows={summary.by_user.map((row, index) => ({ id: row.user_id ?? `user-${index}`, label: row.display_name ?? mt('anonymousUser'), totals: row.totals }))} />
    </div>
  </section>;
}

function subjectName(subject: ModelTokenUsagePage['items'][number]['subject'], mt: (key: ModelTranslationKey) => string) {
  if (subject.kind === 'user') return subject.display_name ?? mt('anonymousUser');
  if (subject.kind === 'integration_app') return subject.display_name ?? mt('integrationSubject');
  return subject.display_name ?? mt('systemSubject');
}

function UsageLedger({ query }: { query: ModelLedgerQuery }) {
  const { locale, t, mt } = useModelI18n();
  const [cursors, setCursors] = useState<Array<ModelTokenUsagePage['next_cursor']>>([null]);
  const [pageIndex, setPageIndex] = useState(0);
  const [page, setPage] = useState<ModelTokenUsagePage | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);
  const [reload, setReload] = useState(0);
  const cursor = cursors[pageIndex];

  useEffect(() => {
    const controller = new AbortController();
    setLoading(true);
    setError(false);
    api.modelTokenUsage({
      ...query,
      page_size: 20,
      ...(cursor ? { cursor_occurred_at_ms: cursor.occurred_at_ms, cursor_id: cursor.id } : {})
    }, controller.signal).then(setPage)
      .catch((reason) => { if (reason?.name !== 'AbortError') setError(true); })
      .finally(() => { if (!controller.signal.aborted) setLoading(false); });
    return () => controller.abort();
  }, [cursor, query, reload]);

  function nextPage() {
    if (!page?.next_cursor) return;
    setCursors((current) => [...current.slice(0, pageIndex + 1), page.next_cursor]);
    setPageIndex((current) => current + 1);
  }

  return <section className="models-usage-section model-ledger-section" aria-labelledby="model-usage-details-title">
    <h2 id="model-usage-details-title">{mt('usageDetails')}</h2>
    {loading && <div className="model-inline-state" role="status">{t('loadingUsage')}</div>}
    {!loading && error && <div className="model-inline-state error" role="alert"><p>{mt('modelUsageLoadFailed')}</p><button className="secondary" type="button" onClick={() => setReload((value) => value + 1)}><RefreshCw size={16} /> {t('retry')}</button></div>}
    {!loading && !error && page && <>
      <div className="models-table-wrap"><table className="models-table model-usage-table" aria-label={mt('usageDetails')}>
        <thead><tr><th>{mt('usageTime')}</th><th>{mt('usageSubject')}</th><th>{mt('usageAgent')}</th><th>{mt('usageModel')}</th><th>{t('status')}</th><TokenHeader /></tr></thead>
        <tbody>{page.items.length === 0 ? <tr><td className="models-empty-cell" colSpan={10}>{mt('noModelUsage')}</td></tr> : page.items.map((item) => <tr key={item.id}>
          <td><time dateTime={item.occurred_at}>{new Date(item.occurred_at).toLocaleString(locale)}</time></td>
          <td>{subjectName(item.subject, mt)}</td><td>{item.agent.name}</td><td><span className="model-summary-name"><strong>{item.model.name}</strong><code>{item.model.model_id}</code></span></td><td>{item.response_status}</td><TokenCells totals={item} />
        </tr>)}</tbody>
      </table></div>
      {(pageIndex > 0 || page.next_cursor) && <div className="model-pagination" aria-label={mt('usageDetails')}>
        <button className="secondary" type="button" aria-label={mt('previousUsagePage')} disabled={pageIndex === 0} onClick={() => setPageIndex((current) => current - 1)}><ChevronLeft size={16} /> {t('previous')}</button>
        <span>{pageIndex + 1}</span>
        <button className="secondary" type="button" aria-label={mt('nextUsagePage')} disabled={!page.next_cursor} onClick={nextPage}>{t('next')} <ChevronRight size={16} /></button>
      </div>}
    </>}
  </section>;
}

function ErrorLedger({ query }: { query: ModelLedgerQuery }) {
  const { locale, t, mt } = useModelI18n();
  const [cursors, setCursors] = useState<Array<ModelCallErrorPage['next_cursor']>>([null]);
  const [pageIndex, setPageIndex] = useState(0);
  const [page, setPage] = useState<ModelCallErrorPage | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);
  const [reload, setReload] = useState(0);
  const cursor = cursors[pageIndex];

  useEffect(() => {
    const controller = new AbortController();
    setLoading(true);
    setError(false);
    api.modelCallErrors({
      ...query,
      page_size: 20,
      ...(cursor ? { cursor_occurred_at_ms: cursor.occurred_at_ms, cursor_id: cursor.id } : {})
    }, controller.signal).then(setPage)
      .catch((reason) => { if (reason?.name !== 'AbortError') setError(true); })
      .finally(() => { if (!controller.signal.aborted) setLoading(false); });
    return () => controller.abort();
  }, [cursor, query, reload]);

  function nextPage() {
    if (!page?.next_cursor) return;
    setCursors((current) => [...current.slice(0, pageIndex + 1), page.next_cursor]);
    setPageIndex((current) => current + 1);
  }

  return <section className="models-usage-section model-ledger-section" aria-labelledby="model-error-details-title">
    <h2 id="model-error-details-title">{mt('errorDetails')}</h2>
    {loading && <div className="model-inline-state" role="status">{t('loading')}</div>}
    {!loading && error && <div className="model-inline-state error" role="alert"><p>{mt('modelUsageLoadFailed')}</p><button className="secondary" type="button" onClick={() => setReload((value) => value + 1)}><RefreshCw size={16} /> {t('retry')}</button></div>}
    {!loading && !error && page && <>
      <div className="models-table-wrap"><table className="models-table model-error-table" aria-label={mt('errorDetails')}>
        <thead><tr><th>{mt('usageTime')}</th><th>{mt('usageSubject')}</th><th>{mt('usageAgent')}</th><th>{mt('usageModel')}</th><th>{t('status')}</th><th>{mt('upstreamStatus')}</th><th>{mt('errorCode')}</th><th>{mt('errorMessage')}</th></tr></thead>
        <tbody>{page.items.length === 0 ? <tr><td className="models-empty-cell" colSpan={8}>{mt('noModelErrors')}</td></tr> : page.items.map((item) => <tr key={item.id}>
          <td><time dateTime={item.occurred_at}>{new Date(item.occurred_at).toLocaleString(locale)}</time></td><td>{subjectName(item.subject, mt)}</td><td>{item.agent.name}</td><td><span className="model-summary-name"><strong>{item.model.name}</strong><code>{item.model.model_id}</code></span></td><td>{item.response_status}</td><td>{item.upstream_status ?? t('none')}</td><td><code>{item.error_code ?? t('none')}</code></td><td>{item.message ?? t('none')}</td>
        </tr>)}</tbody>
      </table></div>
      {(pageIndex > 0 || page.next_cursor) && <div className="model-pagination" aria-label={mt('errorDetails')}>
        <button className="secondary" type="button" aria-label={mt('previousErrorPage')} disabled={pageIndex === 0} onClick={() => setPageIndex((current) => current - 1)}><ChevronLeft size={16} /> {t('previous')}</button>
        <span>{pageIndex + 1}</span>
        <button className="secondary" type="button" aria-label={mt('nextErrorPage')} disabled={!page.next_cursor} onClick={nextPage}>{t('next')} <ChevronRight size={16} /></button>
      </div>}
    </>}
  </section>;
}

function UsageTab({ currentUser, connections }: { currentUser: User; connections: ModelConnection[] }) {
  const { t, mt } = useModelI18n();
  const administrator = currentUser.role === 'admin' || currentUser.role === 'super_admin';
  const [agents, setAgents] = useState<Agent[]>([]);
  const [users, setUsers] = useState<User[]>([]);
  const [optionsLoading, setOptionsLoading] = useState(true);
  const [optionsError, setOptionsError] = useState(false);
  const [optionsReload, setOptionsReload] = useState(0);
  const [range, setRange] = useState<UsageRange>('today');
  const [modelId, setModelId] = useState('');
  const [agentId, setAgentId] = useState('');
  const [userId, setUserId] = useState('');

  useEffect(() => {
    const controller = new AbortController();
    setOptionsLoading(true);
    setOptionsError(false);
    Promise.all([
      api.agents(controller.signal),
      administrator ? api.users(controller.signal) : Promise.resolve([currentUser])
    ])
      .then(([loadedAgents, loadedUsers]) => {
        setAgents(loadedAgents);
        setUsers(loadedUsers.some((user) => user.id === currentUser.id) ? loadedUsers : [currentUser, ...loadedUsers]);
      })
      .catch((reason) => { if (reason?.name !== 'AbortError') setOptionsError(true); })
      .finally(() => { if (!controller.signal.aborted) setOptionsLoading(false); });
    return () => controller.abort();
  }, [administrator, currentUser, optionsReload]);

  const rangeQuery = useMemo(() => modelRange(range), [range]);
  const query = useMemo<ModelLedgerQuery>(() => ({
    ...rangeQuery,
    ...(modelId ? { model_connection_id: modelId } : {}),
    ...(agentId ? { agent_id: agentId } : {}),
    ...(userId ? { user_id: userId } : {})
  }), [agentId, modelId, rangeQuery, userId]);
  const queryKey = JSON.stringify(query);

  if (optionsLoading) return <div className="panel state-panel" role="status">{t('loadingUsage')}</div>;
  if (optionsError) return <div className="panel state-panel" role="alert"><p>{mt('modelUsageLoadFailed')}</p><button className="secondary" type="button" onClick={() => setOptionsReload((value) => value + 1)}><RefreshCw size={16} /> {t('retry')}</button></div>;

  return <div className="models-usage-workspace">
    <section className="model-usage-filters" aria-label={mt('modelUsage')}>
      <label>{mt('usageRange')}<select value={range} onChange={(event) => setRange(event.target.value as UsageRange)}><option value="today">{mt('rangeToday')}</option><option value="yesterday">{mt('rangeYesterday')}</option><option value="7days">{mt('range7Days')}</option><option value="30days">{mt('range30Days')}</option><option value="90days">{mt('range90Days')}</option><option value="all">{mt('rangeAll')}</option></select></label>
      <label>{mt('filterByModel')}<select value={modelId} onChange={(event) => setModelId(event.target.value)}><option value="">{mt('allModels')}</option>{connections.map((connection) => <option key={connection.id} value={connection.id}>{connection.name} / {connection.model_id}</option>)}</select></label>
      <label>{mt('filterByAgent')}<select value={agentId} onChange={(event) => setAgentId(event.target.value)}><option value="">{mt('allAgents')}</option>{agents.map((agent) => <option key={agent.id} value={agent.id}>{agent.name}</option>)}</select></label>
      <label>{mt('filterByUser')}<select value={userId} onChange={(event) => setUserId(event.target.value)}><option value="">{mt('allUsers')}</option>{users.map((user) => <option key={user.id} value={user.id}>{user.display_name || user.username}</option>)}</select></label>
    </section>
    <UsageSummary key={`summary-${queryKey}`} query={query} />
    <UsageLedger key={`usage-${queryKey}`} query={query} />
    <ErrorLedger key={`errors-${queryKey}`} query={query} />
  </div>;
}

export function ModelsPage({ currentUser }: { currentUser: User }) {
  const { t, mt } = useModelI18n();
  const administrator = currentUser.role === 'admin' || currentUser.role === 'super_admin';
  const [activeTab, setActiveTab] = useState<ModelsTab>('personal');
  const [connections, setConnections] = useState<ModelConnection[]>([]);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState(false);
  const [reload, setReload] = useState(0);
  const [dialog, setDialog] = useState<ConnectionDialog | null>(null);

  useEffect(() => {
    const controller = new AbortController();
    setLoading(true);
    setLoadError(false);
    api.modelConnections(controller.signal)
      .then(setConnections)
      .catch((reason) => { if (reason?.name !== 'AbortError') setLoadError(true); })
      .finally(() => { if (!controller.signal.aborted) setLoading(false); });
    return () => controller.abort();
  }, [reload]);

  const tabs = [
    { id: 'personal' as const, label: mt('myModels'), icon: UserRound },
    { id: 'available' as const, label: mt('availableModels'), icon: Database },
    { id: 'usage' as const, label: mt('modelUsage'), icon: Gauge },
    ...(administrator ? [{ id: 'global' as const, label: mt('globalModels'), icon: Globe2 }] : [])
  ];
  const personal = connections.filter((connection) => connection.scope === 'personal' && connection.owner_id === currentUser.id);
  const available = connections.filter((connection) => connection.status === 'enabled');
  const global = connections.filter((connection) => connection.scope === 'global');

  function handleTabKeyDown(event: KeyboardEvent<HTMLButtonElement>, index: number) {
    if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) return;
    event.preventDefault();
    const nextIndex = event.key === 'Home'
      ? 0
      : event.key === 'End'
        ? tabs.length - 1
        : (index + (event.key === 'ArrowRight' ? 1 : -1) + tabs.length) % tabs.length;
    setActiveTab(tabs[nextIndex].id);
    document.getElementById(`models-tab-${tabs[nextIndex].id}`)?.focus();
  }

  function updateConnection(connection: ModelConnection) {
    setConnections((current) => current.some((item) => item.id === connection.id)
      ? current.map((item) => item.id === connection.id ? connection : item)
      : [...current, connection]);
    setDialog(null);
  }

  function removeConnection(connectionId: string) {
    setConnections((current) => current.filter((connection) => connection.id !== connectionId));
  }

  function updateDefault(connectionId: string | null) {
    setConnections((current) => current.map((connection) => ({ ...connection, is_system_default: connection.id === connectionId })));
  }

  return <div className="workspace-page models-page" aria-labelledby="models-title">
    <header className="page-header models-header"><div><h1 id="models-title"><KeyRound size={22} /> {t('models' as TranslationKey)}</h1><p>{mt('modelsSubtitle')}</p></div></header>
    <div className="models-tabs" role="tablist" aria-label={t('models' as TranslationKey)}>
      {tabs.map((tab, index) => {
        const Icon = tab.icon;
        const selected = activeTab === tab.id;
        return <button id={`models-tab-${tab.id}`} key={tab.id} type="button" role="tab" aria-selected={selected} aria-controls={`models-panel-${tab.id}`} tabIndex={selected ? 0 : -1} onClick={() => setActiveTab(tab.id)} onKeyDown={(event) => handleTabKeyDown(event, index)}><Icon size={16} /> <span>{tab.label}</span></button>;
      })}
    </div>
    <div id={`models-panel-${activeTab}`} className="models-tab-panel" role="tabpanel" aria-labelledby={`models-tab-${activeTab}`}>
      {loading && <div className="panel state-panel" role="status">{mt('loadingModels')}</div>}
      {!loading && loadError && <div className="panel state-panel" role="alert"><p>{mt('modelsLoadFailed')}</p><button className="secondary" type="button" onClick={() => setReload((value) => value + 1)}><RefreshCw size={16} /> {t('retry')}</button></div>}
      {!loading && !loadError && activeTab === 'personal' && <ConnectionsTable title={mt('myModels')} tableLabel={mt('modelConnectionList')} emptyMessage={mt('noPersonalModels')} connections={personal} managementScope="personal" onOpen={setDialog} />}
      {!loading && !loadError && activeTab === 'available' && <ConnectionsTable title={mt('availableModels')} tableLabel={mt('availableModelList')} emptyMessage={mt('noAvailableModels')} connections={available} onOpen={setDialog} />}
      {!loading && !loadError && activeTab === 'usage' && <UsageTab currentUser={currentUser} connections={connections} />}
      {!loading && !loadError && activeTab === 'global' && administrator && <ConnectionsTable title={mt('globalModels')} tableLabel={mt('globalModelConnectionList')} emptyMessage={mt('noGlobalModels')} connections={global} managementScope="global" onOpen={setDialog} />}
    </div>
    {dialog?.kind === 'create' && <ConnectionFormDialog state={dialog} onClose={() => setDialog(null)} onSaved={updateConnection} />}
    {dialog?.kind === 'edit' && <ConnectionFormDialog state={dialog} onClose={() => setDialog(null)} onSaved={updateConnection} />}
    {dialog && !['create', 'edit'].includes(dialog.kind) && <ConnectionActionDialog state={dialog as Exclude<ConnectionDialog, { kind: 'create' | 'edit' }>} onClose={() => setDialog(null)} onUpdated={updateConnection} onRemoved={removeConnection} onDefaultChanged={updateDefault} />}
  </div>;
}
