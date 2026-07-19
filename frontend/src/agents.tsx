import { ArrowDown, ArrowUp, Bot, Pencil, Plus, Save, Search, Send, Trash2 } from 'lucide-react';
import { ComponentType, FormEvent, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  Agent,
  api,
  ApiError,
  type CodexSubagentDefinition,
  type ModelConnectionOption,
  type ModelConnectionOptions,
  type ReasoningEffort,
  Run,
  Runtime,
  Skill,
  User
} from './api/client';
import { FormDialog } from './components/form-dialog';
import { MarkdownEditor } from './components/markdown-editor';
import { useI18n } from './i18n';

type Navigate = (path: string, force?: boolean) => void;
type SortField = 'name' | 'availability' | 'runtime' | 'visibility' | 'skills' | 'created';
type SortDirection = 'asc' | 'desc';
type Availability = 'automatic' | 'online' | 'offline' | 'unbound';

function availabilityFor(agent: Agent, runtimesById: Map<string, Runtime>): Availability {
  if (!agent.runtime_id) return 'automatic';
  const runtime = runtimesById.get(agent.runtime_id);
  if (!runtime) return 'unbound';
  return runtime.status === 'online' ? 'online' : 'offline';
}

function compareText(left: string, right: string) {
  return left.localeCompare(right, undefined, { sensitivity: 'base' });
}

function visibilityLabel(visibility: string, t: ReturnType<typeof useI18n>['t']) {
  if (visibility === 'private') return t('private');
  if (visibility === 'public_to') return t('specificUsers');
  if (visibility === 'public') return t('public');
  return t('unknownVisibility');
}

function availabilityLabel(availability: Availability, t: ReturnType<typeof useI18n>['t']) {
  const keys = {
    automatic: 'agentAvailabilityAutomatic',
    online: 'agentAvailabilityOnline',
    offline: 'agentAvailabilityOffline',
    unbound: 'agentAvailabilityUnbound'
  } as const;
  return t(keys[availability]);
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

function runtimeStatusLabel(status: string, t: ReturnType<typeof useI18n>['t']) {
  if (status === 'online') return t('statusOnline');
  if (status === 'offline') return t('statusOffline');
  return status;
}

const reasoningEfforts: ReasoningEffort[] = [
  'default',
  'none',
  'minimal',
  'low',
  'medium',
  'high',
  'xhigh',
  'max',
  'ultra'
];

function reasoningEffortLabel(value: ReasoningEffort, t: ReturnType<typeof useI18n>['t']) {
  const keys = {
    default: 'reasoningDefault',
    none: 'reasoningNone',
    minimal: 'reasoningMinimal',
    low: 'reasoningLow',
    medium: 'reasoningMedium',
    high: 'reasoningHigh',
    xhigh: 'reasoningXhigh',
    max: 'reasoningMax',
    ultra: 'reasoningUltra'
  } as const;
  return t(keys[value]);
}

function modelOptionLabel(option: ModelConnectionOption, t: ReturnType<typeof useI18n>['t']) {
  const scope = option.scope === 'global' ? t('modelScopeGlobal') : t('modelScopePersonal');
  const status = option.status === 'disabled' ? ` · ${t('disabled')}` : '';
  return `${option.name} · ${option.model_id} · ${scope}${status}`;
}

function modelName(
  modelConnectionId: string | null,
  options: ModelConnectionOption[],
  fallback: string
) {
  if (!modelConnectionId) return fallback;
  const option = options.find((candidate) => candidate.id === modelConnectionId);
  return option ? `${option.name} · ${option.model_id}` : fallback;
}

type SubagentDialogState = {
  index: number | null;
  draft: CodexSubagentDefinition;
  error: string;
};

function emptySubagent(): CodexSubagentDefinition {
  return {
    name: '',
    description: '',
    developer_instructions: '',
    model_connection_id: null,
    reasoning_effort: null
  };
}

function editableSubagent(definition?: CodexSubagentDefinition): CodexSubagentDefinition {
  return definition ? {
    ...definition,
    enabled: true,
    disabled_reason: null
  } : emptySubagent();
}

function SubagentTable({
  definitions,
  modelOptions,
  canManage,
  disabled,
  onEdit,
  onDelete
}: {
  definitions: CodexSubagentDefinition[];
  modelOptions: ModelConnectionOption[];
  canManage: boolean;
  disabled: boolean;
  onEdit: (index: number) => void;
  onDelete: (index: number) => void;
}) {
  const { t } = useI18n();
  return <div className="agents-table-wrap agent-subagent-table-wrap">
    <table className="agents-table agent-subagent-table" aria-label={t('codexSubagents')}>
      <thead><tr><th>{t('subagentName')}</th><th>{t('subagentDescription')}</th><th>{t('subagentModelOverride')}</th><th>{t('subagentReasoningOverride')}</th><th>{t('status')}</th>{canManage && <th className="agent-subagent-actions-column">{t('actions')}</th>}</tr></thead>
      <tbody>{definitions.map((definition, index) => <tr key={`${definition.name}-${index}`}>
        <td><strong>{definition.name}</strong></td>
        <td>{definition.description}</td>
        <td>{modelName(definition.model_connection_id, modelOptions, t('inheritAgentModel'))}</td>
        <td>{definition.reasoning_effort ? reasoningEffortLabel(definition.reasoning_effort, t) : t('inheritAgentReasoning')}</td>
        <td>{definition.enabled === false ? <span title={definition.disabled_reason ?? undefined}>{t('disabled')}</span> : t('enabled')}</td>
        {canManage && <td className="agent-subagent-actions-column"><div className="button-row agent-subagent-actions"><button type="button" className="icon-button" disabled={disabled} aria-label={`${t('editCodexSubagent')}: ${definition.name}`} title={`${t('editCodexSubagent')}: ${definition.name}`} onClick={() => onEdit(index)}><Pencil size={16} /></button><button type="button" className="icon-button" disabled={disabled} aria-label={`${t('delete')} ${definition.name}`} title={`${t('delete')} ${definition.name}`} onClick={() => onDelete(index)}><Trash2 size={16} /></button></div></td>}
      </tr>)}{definitions.length === 0 && <tr><td colSpan={canManage ? 6 : 5}><div className="compact-empty">{t('noCodexSubagents')}</div></td></tr>}</tbody>
    </table>
  </div>;
}

function SubagentDialog({
  dialog,
  definitions,
  modelOptions,
  formId,
  busy,
  onChange,
  onCommit,
  onClose
}: {
  dialog: SubagentDialogState;
  definitions: CodexSubagentDefinition[];
  modelOptions: ModelConnectionOption[];
  formId: string;
  busy: boolean;
  onChange: (dialog: SubagentDialogState) => void;
  onCommit: (index: number | null, definition: CodexSubagentDefinition) => void;
  onClose: () => void;
}) {
  const { t } = useI18n();
  const nameRef = useRef<HTMLInputElement>(null);

  function update(update: Partial<CodexSubagentDefinition>) {
    onChange({ ...dialog, draft: { ...dialog.draft, ...update }, error: '' });
  }

  function submit(event: FormEvent) {
    event.preventDefault();
    const definition = {
      ...dialog.draft,
      name: dialog.draft.name.trim(),
      description: dialog.draft.description.trim(),
      developer_instructions: dialog.draft.developer_instructions.trim(),
      enabled: true,
      disabled_reason: null
    };
    if (!definition.name || !definition.description || !definition.developer_instructions) {
      onChange({ ...dialog, error: t('subagentInvalid') });
      return;
    }
    if (definitions.some((candidate, index) => index !== dialog.index
      && candidate.name.trim().toLocaleLowerCase() === definition.name.toLocaleLowerCase())) {
      onChange({ ...dialog, error: t('subagentNameDuplicate') });
      return;
    }
    onCommit(dialog.index, definition);
  }

  return <FormDialog
    title={dialog.index === null ? t('addCodexSubagent') : `${t('editCodexSubagent')}: ${dialog.draft.name}`}
    busy={busy}
    onClose={onClose}
    initialFocusRef={nameRef}
    className="agent-subagent-dialog"
    footer={<><button type="button" className="secondary" disabled={busy} onClick={onClose}>{t('cancel')}</button><button type="submit" form={formId} className="primary" disabled={busy}>{t('saveChanges')}</button></>}
  >
    <form id={formId} className="stack" onSubmit={submit}>
      <label>{t('subagentName')}<input ref={nameRef} required maxLength={64} disabled={busy} value={dialog.draft.name} onChange={(event) => update({ name: event.target.value })} /></label>
      <label>{t('subagentDescription')}<textarea required maxLength={512} disabled={busy} value={dialog.draft.description} onChange={(event) => update({ description: event.target.value })} /></label>
      <MarkdownEditor label={t('developerInstructions')} required disabled={busy} value={dialog.draft.developer_instructions} onChange={(developerInstructions) => update({ developer_instructions: developerInstructions })} />
      <label>{t('subagentModelOverride')}<select disabled={busy} value={dialog.draft.model_connection_id ?? ''} onChange={(event) => update({ model_connection_id: event.target.value || null })}><option value="">{t('inheritAgentModel')}</option>{modelOptions.map((option) => <option key={option.id} value={option.id} disabled={option.status === 'disabled' && option.id !== dialog.draft.model_connection_id}>{modelOptionLabel(option, t)}</option>)}</select></label>
      <label>{t('subagentReasoningOverride')}<select disabled={busy} value={dialog.draft.reasoning_effort ?? ''} onChange={(event) => update({ reasoning_effort: event.target.value ? event.target.value as ReasoningEffort : null })}><option value="">{t('inheritAgentReasoning')}</option>{reasoningEfforts.map((effort) => <option key={effort} value={effort}>{reasoningEffortLabel(effort, t)}</option>)}</select></label>
      {dialog.error && <div className="error" role="alert">{dialog.error}</div>}
    </form>
  </FormDialog>;
}

export function AgentsPage({ currentUser, navigate }: { currentUser: User; navigate: Navigate }) {
  const { locale, t } = useI18n();
  const [agents, setAgents] = useState<Agent[]>([]);
  const [runtimes, setRuntimes] = useState<Runtime[]>([]);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState(false);
  const [retry, setRetry] = useState(0);
  const [search, setSearch] = useState('');
  const [runtimeFilter, setRuntimeFilter] = useState('all');
  const [availabilityFilter, setAvailabilityFilter] = useState('all');
  const [visibilityFilter, setVisibilityFilter] = useState('all');
  const [sortField, setSortField] = useState<SortField>('created');
  const [sortDirection, setSortDirection] = useState<SortDirection>('desc');
  const [createOpen, setCreateOpen] = useState(false);
  const loadGeneration = useRef(0);
  const createButtonRef = useRef<HTMLButtonElement>(null);

  useEffect(() => {
    const controller = new AbortController();
    const generation = ++loadGeneration.current;
    setLoading(true);
    setLoadError(false);
    Promise.all([api.agents(controller.signal), api.runtimes(controller.signal)])
      .then(([agentResponse, runtimeResponse]) => {
        if (controller.signal.aborted || generation !== loadGeneration.current) return;
        setAgents(agentResponse);
        setRuntimes(runtimeResponse);
        setLoading(false);
      })
      .catch(() => {
        if (controller.signal.aborted || generation !== loadGeneration.current) return;
        setLoadError(true);
        setLoading(false);
      });
    return () => controller.abort();
  }, [retry]);

  const runtimesById = useMemo(() => new Map(runtimes.map((runtime) => [runtime.id, runtime])), [runtimes]);

  const rows = useMemo(() => {
    const query = search.trim().toLocaleLowerCase(locale);
    const filtered = agents.filter((agent) => {
      const availability = availabilityFor(agent, runtimesById);
      const runtime = agent.runtime_id ? runtimesById.get(agent.runtime_id) : null;
      if (query && !agent.name.toLocaleLowerCase(locale).includes(query)) return false;
      if (availabilityFilter !== 'all' && availability !== availabilityFilter) return false;
      if (runtimeFilter !== 'all' && agent.runtime_id !== runtimeFilter) return false;
      if (visibilityFilter !== 'all' && agent.visibility !== visibilityFilter) return false;
      return Boolean(runtime || availability === 'automatic' || availability === 'unbound');
    });
    const direction = sortDirection === 'asc' ? 1 : -1;
    return filtered.sort((left, right) => {
      const leftRuntime = left.runtime_id ? runtimesById.get(left.runtime_id)?.hostname ?? '' : '';
      const rightRuntime = right.runtime_id ? runtimesById.get(right.runtime_id)?.hostname ?? '' : '';
      const leftAvailability = availabilityFor(left, runtimesById);
      const rightAvailability = availabilityFor(right, runtimesById);
      let result = 0;
      if (sortField === 'name') result = compareText(left.name, right.name);
      if (sortField === 'availability') result = compareText(leftAvailability, rightAvailability);
      if (sortField === 'runtime') result = compareText(leftRuntime, rightRuntime);
      if (sortField === 'visibility') result = compareText(left.visibility, right.visibility);
      if (sortField === 'skills') result = left.managed_skill_ids.length - right.managed_skill_ids.length;
      if (sortField === 'created') result = Date.parse(left.created_at) - Date.parse(right.created_at);
      return result * direction || compareText(left.id, right.id) * direction;
    });
  }, [agents, availabilityFilter, locale, runtimeFilter, runtimesById, search, sortDirection, sortField, visibilityFilter]);

  const toggleSort = useCallback((field: SortField) => {
    if (sortField === field) setSortDirection((current) => current === 'asc' ? 'desc' : 'asc');
    else {
      setSortField(field);
      setSortDirection('asc');
    }
  }, [sortField]);

  const columns: Array<{ field: SortField; label: string }> = [
    { field: 'name', label: t('name') },
    { field: 'availability', label: t('agentAvailability') },
    { field: 'runtime', label: t('agentRuntimeHostname') },
    { field: 'visibility', label: t('visibility') },
    { field: 'skills', label: t('agentManagedSkillCount') },
    { field: 'created', label: t('created') }
  ];

  return (
    <div className="agents-page">
      <header className="agents-header">
        <div><h1><Bot size={19} /> {t('agents')}</h1><span>{agents.length}</span></div>
        <button ref={createButtonRef} type="button" className="primary" onClick={() => setCreateOpen(true)}><Plus size={16} /> {t('createAgent')}</button>
      </header>

      {loading ? <section className="agents-state" aria-live="polite">{t('loadingAgents')}</section>
        : loadError ? <section className="agents-state" role="alert"><p>{t('agentsLoadFailed')}</p><button type="button" className="secondary" onClick={() => setRetry((value) => value + 1)}>{t('retry')}</button></section>
          : agents.length === 0 ? <section className="agents-state"><h2>{t('noAgents')}</h2><button type="button" className="primary" onClick={() => setCreateOpen(true)}><Plus size={16} /> {t('createAgent')}</button></section>
            : <>
              <section className="agents-toolbar" aria-label={t('agentListControls')}>
                <label className="agents-search"><Search size={16} /><span className="sr-only">{t('searchAgents')}</span><input aria-label={t('searchAgents')} value={search} onChange={(event) => setSearch(event.target.value)} /></label>
                <label><span>{t('agentAvailability')}</span><select aria-label={t('agentAvailability')} value={availabilityFilter} onChange={(event) => setAvailabilityFilter(event.target.value)}><option value="all">{t('filterAll')}</option><option value="automatic">{t('agentAvailabilityAutomatic')}</option><option value="online">{t('agentAvailabilityOnline')}</option><option value="offline">{t('agentAvailabilityOffline')}</option><option value="unbound">{t('agentAvailabilityUnbound')}</option></select></label>
                <label><span>{t('agentRuntimeFilter')}</span><select aria-label={t('agentRuntimeFilter')} value={runtimeFilter} onChange={(event) => setRuntimeFilter(event.target.value)}><option value="all">{t('filterAll')}</option>{runtimes.map((runtime) => <option key={runtime.id} value={runtime.id}>{runtime.hostname}</option>)}</select></label>
                <label><span>{t('agentVisibilityFilter')}</span><select aria-label={t('agentVisibilityFilter')} value={visibilityFilter} onChange={(event) => setVisibilityFilter(event.target.value)}><option value="all">{t('filterAll')}</option><option value="private">{t('private')}</option><option value="public_to">{t('specificUsers')}</option><option value="public">{t('public')}</option></select></label>
              </section>
              <div className="agents-table-wrap">
                <table className="agents-table" aria-label={t('agentList')}>
                  <thead><tr>{columns.map((column) => <th key={column.field} aria-sort={sortField === column.field ? (sortDirection === 'asc' ? 'ascending' : 'descending') : 'none'}><button type="button" aria-label={`${t('agentSortBy')} ${column.label}`} onClick={() => toggleSort(column.field)}>{column.label}{sortField === column.field ? sortDirection === 'asc' ? <ArrowUp size={13} /> : <ArrowDown size={13} /> : null}</button></th>)}</tr></thead>
                  <tbody>{rows.map((agent) => {
                    const availability = availabilityFor(agent, runtimesById);
                    const runtime = agent.runtime_id ? runtimesById.get(agent.runtime_id) : null;
                    return <tr key={agent.id} data-agent-id={agent.id} onClick={() => navigate(`/agents/${agent.id}`)}>
                      <td><button type="button" className="agent-row-button" onClick={(event) => { event.stopPropagation(); navigate(`/agents/${agent.id}`); }}>{agent.name}</button></td>
                      <td><span className={`agent-availability ${availability}`}>{availabilityLabel(availability, t)}</span></td>
                      <td>{runtime?.hostname ?? (availability === 'automatic' ? t('agentAvailabilityAutomatic') : t('agentAvailabilityUnbound'))}</td>
                      <td>{visibilityLabel(agent.visibility, t)}</td>
                      <td>{agent.managed_skill_ids.length}</td>
                      <td><time dateTime={agent.created_at}>{new Date(agent.created_at).toLocaleString(locale)}</time></td>
                    </tr>;
                  })}</tbody>
                </table>
                {rows.length === 0 && <div className="agents-filter-empty">{t('noAgentMatches')}</div>}
              </div>
            </>}
      {createOpen && <CreateAgentModal currentUser={currentUser} navigate={navigate} onClose={() => {
        setCreateOpen(false);
        window.requestAnimationFrame(() => createButtonRef.current?.focus());
      }} />}
    </div>
  );
}

function CreateAgentModal({ currentUser, navigate, onClose }: { currentUser: User; navigate: Navigate; onClose: () => void }) {
  const { t } = useI18n();
  const canCreatePublic = currentUser.role === 'admin' || currentUser.role === 'super_admin';
  const [name, setName] = useState(() => t('defaultAgentName'));
  const [instructions, setInstructions] = useState(() => t('defaultAgentInstructions'));
  const [visibility, setVisibility] = useState('private');
  const [publicTo, setPublicTo] = useState<string[]>([]);
  const [users, setUsers] = useState<User[]>([]);
  const [usersLoading, setUsersLoading] = useState(true);
  const [usersError, setUsersError] = useState(false);
  const [modelOptions, setModelOptions] = useState<ModelConnectionOptions>({ items: [], system_default_model_connection_id: null });
  const [modelsLoading, setModelsLoading] = useState(true);
  const [modelsError, setModelsError] = useState(false);
  const [defaultModelConnectionId, setDefaultModelConnectionId] = useState<string | null>(null);
  const [reasoningEffort, setReasoningEffort] = useState<ReasoningEffort>('default');
  const [codexSubagents, setCodexSubagents] = useState<CodexSubagentDefinition[]>([]);
  const [subagentDialog, setSubagentDialog] = useState<SubagentDialogState | null>(null);
  const [pending, setPending] = useState(false);
  const [error, setError] = useState(false);
  const nameRef = useRef<HTMLInputElement>(null);
  const pendingRef = useRef(false);
  const mountedRef = useRef(true);
  const usersControllerRef = useRef<AbortController | null>(null);
  const modelsControllerRef = useRef<AbortController | null>(null);
  const mutationControllerRef = useRef<AbortController | null>(null);

  const loadUsers = useCallback(() => {
    usersControllerRef.current?.abort();
    const controller = new AbortController();
    usersControllerRef.current = controller;
    setUsersLoading(true);
    setUsersError(false);
    api.users(controller.signal).then((loaded) => {
      if (controller.signal.aborted || !mountedRef.current) return;
      setUsers(loaded.filter((user) => user.id !== currentUser.id));
      setUsersLoading(false);
    }).catch(() => {
      if (controller.signal.aborted || !mountedRef.current) return;
      setUsersError(true);
      setUsersLoading(false);
    });
  }, [currentUser.id]);

  const loadModels = useCallback(() => {
    modelsControllerRef.current?.abort();
    const controller = new AbortController();
    modelsControllerRef.current = controller;
    setModelsLoading(true);
    setModelsError(false);
    api.modelConnectionOptions(controller.signal).then((loaded) => {
      if (controller.signal.aborted || !mountedRef.current) return;
      setModelOptions(loaded);
      setDefaultModelConnectionId(loaded.system_default_model_connection_id);
      setModelsLoading(false);
    }).catch(() => {
      if (controller.signal.aborted || !mountedRef.current) return;
      setModelsError(true);
      setModelsLoading(false);
    });
  }, []);

  useEffect(() => {
    loadUsers();
    loadModels();
    return () => {
      mountedRef.current = false;
      usersControllerRef.current?.abort();
      modelsControllerRef.current?.abort();
      mutationControllerRef.current?.abort();
    };
  }, [loadModels, loadUsers]);

  function requestClose() {
    if (pendingRef.current) return;
    onClose();
  }

  async function create(event: FormEvent) {
    event.preventDefault();
    if (pendingRef.current) return;
    pendingRef.current = true;
    setPending(true);
    setError(false);
    const controller = new AbortController();
    mutationControllerRef.current = controller;
    try {
      const agent = await api.createConfiguredAgent({
        name,
        instructions,
        visibility,
        public_to: visibility === 'public_to' ? publicTo : [],
        default_model_connection_id: defaultModelConnectionId,
        reasoning_effort: reasoningEffort,
        codex_subagents: codexSubagents
      }, controller.signal);
      if (controller.signal.aborted || !mountedRef.current) return;
      navigate(`/agents/${agent.id}`);
    } catch {
      if (!controller.signal.aborted && mountedRef.current) setError(true);
    } finally {
      if (mountedRef.current && mutationControllerRef.current === controller) {
        pendingRef.current = false;
        setPending(false);
      }
    }
  }

  function openSubagent(index: number | null) {
    const definition = index === null ? undefined : codexSubagents[index];
    if (index !== null && !definition) return;
    setSubagentDialog({ index, draft: editableSubagent(definition), error: '' });
  }

  function commitSubagent(index: number | null, definition: CodexSubagentDefinition) {
    setCodexSubagents((current) => index === null
      ? [...current, definition]
      : current.map((candidate, candidateIndex) => candidateIndex === index ? definition : candidate));
    setSubagentDialog(null);
  }

  function deleteSubagent(index: number) {
    const definition = codexSubagents[index];
    if (!definition || !window.confirm(t('confirmDeleteCodexSubagent').replace('{name}', definition.name))) return;
    setCodexSubagents((current) => current.filter((_, candidateIndex) => candidateIndex !== index));
  }

  return <><FormDialog
    title={t('createAgent')}
    busy={pending}
    onClose={requestClose}
    initialFocusRef={nameRef}
    className="agent-create-modal"
    footer={<><button type="button" className="secondary" disabled={pending} onClick={requestClose}>{t('cancel')}</button><button form="create-agent-form" type="submit" className="primary" disabled={pending || usersLoading || usersError || modelsLoading || modelsError}>{pending ? t('creating') : t('createAgentAction')}</button></>}
  >
      <form id="create-agent-form" className="stack" onSubmit={create}>
        <label>{t('name')}<input ref={nameRef} required value={name} onChange={(event) => setName(event.target.value)} /></label>
        <MarkdownEditor label={t('instructions')} required value={instructions} onChange={setInstructions} />
        {modelsLoading ? <div className="compact-empty" aria-live="polite">{t('modelOptionsLoading')}</div>
          : modelsError ? <div className="error agent-model-options-error" role="alert"><span>{t('modelOptionsLoadFailed')}</span><button type="button" className="text-button" onClick={loadModels}>{t('retry')}</button></div>
            : <div className="agent-model-fields"><label>{t('defaultModelConnection')}<select value={defaultModelConnectionId ?? ''} onChange={(event) => setDefaultModelConnectionId(event.target.value || null)}><option value="">{t('modelNotConfigured')}</option>{modelOptions.items.map((option) => <option key={option.id} value={option.id} disabled={option.status === 'disabled' && option.id !== defaultModelConnectionId}>{modelOptionLabel(option, t)}</option>)}</select></label><label>{t('reasoningEffort')}<select value={reasoningEffort} onChange={(event) => setReasoningEffort(event.target.value as ReasoningEffort)}>{reasoningEfforts.map((effort) => <option key={effort} value={effort}>{reasoningEffortLabel(effort, t)}</option>)}</select></label></div>}
        <section className="agent-subagent-section">
          <div className="agent-subagent-heading"><span className="field-label">{t('codexSubagents')}</span><button type="button" className="secondary" disabled={pending || modelsLoading || modelsError || codexSubagents.length >= 32} onClick={() => openSubagent(null)}><Plus size={16} /> {t('addCodexSubagent')}</button></div>
          <SubagentTable definitions={codexSubagents} modelOptions={modelOptions.items} canManage disabled={pending} onEdit={openSubagent} onDelete={deleteSubagent} />
        </section>
        <label>{t('visibility')}<select value={visibility} onChange={(event) => { setVisibility(event.target.value); if (event.target.value !== 'public_to') setPublicTo([]); }}><option value="private">{t('private')}</option><option value="public_to">{t('specificUsers')}</option>{canCreatePublic && <option value="public">{t('public')}</option>}</select></label>
        {visibility === 'public_to' && <fieldset className="agent-user-picker" disabled={pending || usersLoading}><legend>{t('agentPublicTo')}</legend>
          {usersLoading ? <span>{t('loadingUsers')}</span> : usersError ? <div role="alert"><span>{t('usersLoadFailed')}</span><button type="button" className="text-button" onClick={loadUsers}>{t('retry')}</button></div> : users.map((user) => <label className="check-row" key={user.id}><input type="checkbox" checked={publicTo.includes(user.id)} onChange={(event) => setPublicTo((current) => event.target.checked ? [...current, user.id] : current.filter((id) => id !== user.id))} /> {user.display_name} ({user.email ?? user.username})</label>)}</fieldset>}
        {error && <div className="error" role="alert">{t('agentCreateFailed')}</div>}
      </form>
  </FormDialog>
  {subagentDialog && <SubagentDialog dialog={subagentDialog} definitions={codexSubagents} modelOptions={modelOptions.items} formId="create-agent-subagent-form" busy={pending} onChange={setSubagentDialog} onCommit={commitSubagent} onClose={() => setSubagentDialog(null)} />}
  </>;
}

type DetailTab = 'activity' | 'instructions' | 'models' | 'skills' | 'mcp' | 'access';
type NavigationBlockerSetter = (blocker: (() => boolean) | null) => void;
type RunConsoleComponent = ComponentType<{ run: Run }>;

const detailTabs: Array<{ id: DetailTab; key: 'tabActivity' | 'tabInstructions' | 'tabModels' | 'tabSkills' | 'tabMcp' | 'tabAccess' }> = [
  { id: 'activity', key: 'tabActivity' },
  { id: 'instructions', key: 'tabInstructions' },
  { id: 'models', key: 'tabModels' },
  { id: 'skills', key: 'tabSkills' },
  { id: 'mcp', key: 'tabMcp' },
  { id: 'access', key: 'tabAccess' }
];

const REDACTED_MCP_SECRET = '********';

type McpEntry = {
  name: string;
  command?: string;
  args?: string[];
  secrets?: Record<string, string>;
};

type McpEntryDraft = {
  name: string;
  command: string;
  args: string;
  secrets: Array<{ key: string; value: string }>;
};

type McpDialogState = {
  index: number | null;
  draft: McpEntryDraft;
  base: McpEntryDraft;
  error: string;
};

function normalizeMcpEntries(value: unknown[]): McpEntry[] {
  return value.flatMap((candidate) => {
    if (!candidate || typeof candidate !== 'object' || Array.isArray(candidate)) return [];
    const source = candidate as Record<string, unknown>;
    if (typeof source.name !== 'string' || !source.name.trim()) return [];
    const command = typeof source.command === 'string' && source.command ? source.command : undefined;
    const args = Array.isArray(source.args)
      ? source.args.filter((arg): arg is string => typeof arg === 'string')
      : undefined;
    const sourceSecrets = source.secrets && typeof source.secrets === 'object' && !Array.isArray(source.secrets)
      ? source.secrets as Record<string, unknown>
      : null;
    const secrets = sourceSecrets
      ? Object.fromEntries(Object.entries(sourceSecrets)
        .filter((entry): entry is [string, string] => typeof entry[1] === 'string')
        .map(([key]) => [key, REDACTED_MCP_SECRET]))
      : undefined;
    return [{
      name: source.name,
      ...(command ? { command } : {}),
      ...(args?.length ? { args } : {}),
      ...(secrets && Object.keys(secrets).length ? { secrets } : {})
    }];
  });
}

function mcpDraftFor(entry?: McpEntry): McpEntryDraft {
  return {
    name: entry?.name ?? '',
    command: entry?.command ?? '',
    args: entry?.args?.join('\n') ?? '',
    secrets: Object.entries(entry?.secrets ?? {}).map(([key, value]) => ({ key, value }))
  };
}

function mcpEntryFor(draft: McpEntryDraft): McpEntry {
  const command = draft.command.trim();
  const args = draft.args.split('\n').map((arg) => arg.trim()).filter(Boolean);
  const secrets = Object.fromEntries(draft.secrets.map(({ key, value }) => [key.trim(), value]));
  return {
    name: draft.name.trim(),
    ...(command ? { command } : {}),
    ...(args.length ? { args } : {}),
    ...(Object.keys(secrets).length ? { secrets } : {})
  };
}

function sameMcpDraft(left: McpEntryDraft, right: McpEntryDraft) {
  return JSON.stringify(left) === JSON.stringify(right);
}

function sameIds(left: string[], right: string[]) {
  return left.length === right.length && left.every((id, index) => id === right[index]);
}

type AgentModelDraft = {
  defaultModelConnectionId: string | null;
  reasoningEffort: ReasoningEffort;
  codexSubagents: CodexSubagentDefinition[];
};

function agentModelDraft(agent: Agent): AgentModelDraft {
  return {
    defaultModelConnectionId: agent.default_model_connection_id,
    reasoningEffort: agent.reasoning_effort,
    codexSubagents: agent.codex_subagents
  };
}

function sameAgentModelDraft(left: AgentModelDraft, right: AgentModelDraft) {
  return JSON.stringify(left) === JSON.stringify(right);
}

export function AgentPage({
  agentId,
  currentUser,
  navigate,
  setNavigationBlocker,
  RunConsole
}: {
  agentId: string;
  currentUser: User;
  navigate: Navigate;
  setNavigationBlocker: NavigationBlockerSetter;
  RunConsole: RunConsoleComponent;
}) {
  const { locale, t } = useI18n();
  const [agent, setAgent] = useState<Agent | null>(null);
  const [runs, setRuns] = useState<Run[]>([]);
  const [runtimes, setRuntimes] = useState<Runtime[]>([]);
  const [skills, setSkills] = useState<Skill[]>([]);
  const [users, setUsers] = useState<User[]>([]);
  const [modelOptions, setModelOptions] = useState<ModelConnectionOptions>({ items: [], system_default_model_connection_id: null });
  const [selectedRun, setSelectedRun] = useState<Run | null>(null);
  const [activeTab, setActiveTab] = useState<DetailTab>('activity');
  const [loadError, setLoadError] = useState(false);
  const [loadRetry, setLoadRetry] = useState(0);
  const [error, setError] = useState('');
  const [configPending, setConfigPending] = useState(false);
  const [runPending, setRunPending] = useState(false);
  const [skillsDialogOpen, setSkillsDialogOpen] = useState(false);
  const [mcpDialog, setMcpDialog] = useState<McpDialogState | null>(null);
  const [subagentDialog, setSubagentDialog] = useState<SubagentDialogState | null>(null);

  const [instructionDraft, setInstructionDraft] = useState({ name: '', instructions: '' });
  const [instructionBase, setInstructionBase] = useState({ name: '', instructions: '' });
  const [managedSkillDraft, setManagedSkillDraft] = useState<string[]>([]);
  const [managedSkillBase, setManagedSkillBase] = useState<string[]>([]);
  const [modelDraft, setModelDraft] = useState<AgentModelDraft>({ defaultModelConnectionId: null, reasoningEffort: 'default', codexSubagents: [] });
  const [modelBase, setModelBase] = useState<AgentModelDraft>({ defaultModelConnectionId: null, reasoningEffort: 'default', codexSubagents: [] });
  const [accessDraft, setAccessDraft] = useState({ visibility: 'private', publicTo: [] as string[], runtimeId: null as string | null });
  const [accessBase, setAccessBase] = useState({ visibility: 'private', publicTo: [] as string[], runtimeId: null as string | null });
  const [message, setMessage] = useState(() => t('defaultRunMessage'));
  const [continueThread, setContinueThread] = useState(false);

  const mounted = useRef(true);
  const loadGeneration = useRef(0);
  const mutationGeneration = useRef(0);
  const runGeneration = useRef(0);
  const loadController = useRef<AbortController | null>(null);
  const mutationController = useRef<AbortController | null>(null);
  const runController = useRef<AbortController | null>(null);
  const refreshController = useRef<AbortController | null>(null);
  const refreshPromise = useRef<Promise<void> | null>(null);
  const configPendingRef = useRef(false);
  const runPendingRef = useRef(false);
  const firstSkillRef = useRef<HTMLInputElement>(null);
  const mcpNameRef = useRef<HTMLInputElement>(null);

  const instructionDirty = instructionDraft.name !== instructionBase.name || instructionDraft.instructions !== instructionBase.instructions;
  const skillsDirty = skillsDialogOpen && !sameIds(managedSkillDraft, managedSkillBase);
  const mcpDirty = Boolean(mcpDialog && !sameMcpDraft(mcpDialog.draft, mcpDialog.base));
  const modelDirty = !sameAgentModelDraft(modelDraft, modelBase);
  const subagentDialogDirty = Boolean(subagentDialog && !sameAgentModelDraft(
    { ...modelDraft, codexSubagents: [subagentDialog.draft] },
    { ...modelDraft, codexSubagents: [editableSubagent(subagentDialog.index === null ? undefined : modelDraft.codexSubagents[subagentDialog.index])] }
  ));
  const accessDirty = accessDraft.visibility !== accessBase.visibility
    || !sameIds(accessDraft.publicTo, accessBase.publicTo)
    || accessDraft.runtimeId !== accessBase.runtimeId;
  const dirty = instructionDirty || skillsDirty || mcpDirty || modelDirty || subagentDialogDirty || accessDirty;

  const applyLoadedAgent = useCallback((loaded: Agent) => {
    setAgent(loaded);
    const instructions = { name: loaded.name, instructions: loaded.instructions };
    setInstructionDraft(instructions);
    setInstructionBase(instructions);
    setManagedSkillDraft(loaded.managed_skill_ids);
    setManagedSkillBase(loaded.managed_skill_ids);
    const models = agentModelDraft(loaded);
    setModelDraft(models);
    setModelBase(models);
    const access = {
      visibility: loaded.visibility,
      publicTo: loaded.public_to,
      runtimeId: loaded.runtime_id
    };
    setAccessDraft(access);
    setAccessBase(access);
  }, []);

  useEffect(() => {
    mounted.current = true;
    const controller = new AbortController();
    loadController.current?.abort();
    loadController.current = controller;
    const generation = ++loadGeneration.current;
    setAgent(null);
    setRuns([]);
    setSelectedRun(null);
    setSkillsDialogOpen(false);
    setMcpDialog(null);
    setSubagentDialog(null);
    setLoadError(false);
    setError('');
    const agentRequest = api.agent(agentId, controller.signal);
    Promise.all([
      agentRequest,
      api.runs(agentId, controller.signal),
      api.runtimes(controller.signal),
      api.skills(controller.signal),
      api.users(controller.signal),
      agentRequest.then((loadedAgent) => loadedAgent.can_manage
        ? api.agentModelConnectionOptions(agentId, controller.signal)
        : { items: [], system_default_model_connection_id: null })
    ]).then(([loadedAgent, loadedRuns, loadedRuntimes, loadedSkills, loadedUsers, loadedModelOptions]) => {
      if (controller.signal.aborted || !mounted.current || generation !== loadGeneration.current) return;
      applyLoadedAgent(loadedAgent);
      setRuns(loadedRuns);
      setSelectedRun(loadedRuns[0] ?? null);
      setRuntimes(loadedRuntimes);
      setSkills(loadedSkills);
      setUsers(loadedUsers);
      setModelOptions(loadedModelOptions);
    }).catch(() => {
      if (controller.signal.aborted || !mounted.current || generation !== loadGeneration.current) return;
      setLoadError(true);
    });
    return () => controller.abort();
  }, [agentId, applyLoadedAgent, loadRetry]);

  const loadedAgentId = agent?.id ?? null;

  useEffect(() => {
    const blocker = () => {
      if (configPendingRef.current) return false;
      return !dirty || window.confirm(t('unsavedAgentConfirm'));
    };
    setNavigationBlocker(blocker);
    const beforeUnload = (event: BeforeUnloadEvent) => {
      if (!dirty && !configPendingRef.current) return;
      event.preventDefault();
      event.returnValue = '';
    };
    window.addEventListener('beforeunload', beforeUnload);
    return () => {
      setNavigationBlocker(null);
      window.removeEventListener('beforeunload', beforeUnload);
    };
  }, [dirty, setNavigationBlocker, t]);

  useEffect(() => {
    if (!loadedAgentId) return;
    let active = true;
    const refresh = async () => {
      if (refreshController.current || configPendingRef.current) return;
      const controller = new AbortController();
      refreshController.current = controller;
      const generation = runGeneration.current;
      const runWasPending = runPendingRef.current;
      try {
        const latest = await api.runs(agentId, controller.signal);
        if (!active || controller.signal.aborted || runWasPending || generation !== runGeneration.current) return;
        setRuns(latest);
        setSelectedRun((current) => latest.find((run) => run.id === current?.id) ?? latest[0] ?? null);
      } catch {
        // Background refresh keeps the last usable history.
      } finally {
        if (refreshController.current === controller) refreshController.current = null;
      }
    };
    const launchRefresh = () => {
      const pending = refresh();
      refreshPromise.current = pending;
      void pending.finally(() => {
        if (refreshPromise.current === pending) refreshPromise.current = null;
      });
    };
    const timer = window.setInterval(launchRefresh, 2000);
    return () => {
      active = false;
      refreshController.current?.abort();
      refreshController.current = null;
      runGeneration.current += 1;
      window.clearInterval(timer);
    };
  }, [agentId, loadedAgentId]);

  useEffect(() => () => {
    mounted.current = false;
    loadController.current?.abort();
    mutationController.current?.abort();
    runController.current?.abort();
    refreshController.current?.abort();
  }, []);

  function beginConfigMutation() {
    if (configPendingRef.current) return null;
    configPendingRef.current = true;
    setConfigPending(true);
    setError('');
    mutationController.current?.abort();
    const controller = new AbortController();
    mutationController.current = controller;
    return { controller, generation: ++mutationGeneration.current };
  }

  function finishConfigMutation(controller: AbortController, generation: number) {
    if (!mounted.current || controller.signal.aborted || generation !== mutationGeneration.current) return;
    configPendingRef.current = false;
    setConfigPending(false);
  }

  async function saveAgentTab(event: FormEvent, tab: 'instructions' | 'models' | 'skills' | 'access') {
    event.preventDefault();
    if (!agent || !agent.can_manage) return;
    const operation = beginConfigMutation();
    if (!operation) return;
    try {
      let next: Agent = { ...agent };
      if (tab === 'instructions') next = { ...next, ...instructionDraft };
      if (tab === 'models') next = {
        ...next,
        default_model_connection_id: modelDraft.defaultModelConnectionId,
        reasoning_effort: modelDraft.reasoningEffort,
        codex_subagents: modelDraft.codexSubagents
      };
      if (tab === 'skills') next = { ...next, managed_skill_ids: managedSkillDraft };
      if (tab === 'access') next = {
        ...next,
        visibility: accessDraft.visibility,
        public_to: accessDraft.visibility === 'public_to' ? accessDraft.publicTo : [],
        runtime_id: accessDraft.runtimeId
      };
      const updated = await api.updateAgent(agentId, next, operation.controller.signal);
      if (operation.controller.signal.aborted || !mounted.current || operation.generation !== mutationGeneration.current) return;
      setAgent(updated);
      if (tab === 'instructions') {
        const saved = { name: updated.name, instructions: updated.instructions };
        setInstructionDraft(saved);
        setInstructionBase(saved);
      }
      if (tab === 'skills') {
        setManagedSkillDraft(updated.managed_skill_ids);
        setManagedSkillBase(updated.managed_skill_ids);
        setSkillsDialogOpen(false);
      }
      if (tab === 'models') {
        const saved = agentModelDraft(updated);
        setModelDraft(saved);
        setModelBase(saved);
        setSubagentDialog(null);
      }
      if (tab === 'access') {
        const saved = { visibility: updated.visibility, publicTo: updated.public_to, runtimeId: updated.runtime_id };
        setAccessDraft(saved);
        setAccessBase(saved);
      }
    } catch (caught) {
      if (!operation.controller.signal.aborted && mounted.current) setError(caught instanceof ApiError && caught.code === 'mcp_redacted_secret_missing' ? t('mcpRedactedSecretMissing') : t('genericError'));
    } finally {
      finishConfigMutation(operation.controller, operation.generation);
    }
  }

  function openSubagentDialog(index: number | null) {
    if (!agent?.can_manage) return;
    const definition = index === null ? undefined : modelDraft.codexSubagents[index];
    if (index !== null && !definition) return;
    setSubagentDialog({ index, draft: editableSubagent(definition), error: '' });
  }

  function commitSubagent(index: number | null, definition: CodexSubagentDefinition) {
    setModelDraft((current) => ({
      ...current,
      codexSubagents: index === null
        ? [...current.codexSubagents, definition]
        : current.codexSubagents.map((candidate, candidateIndex) => candidateIndex === index ? definition : candidate)
    }));
    setSubagentDialog(null);
  }

  function deleteSubagentDefinition(index: number) {
    if (!agent?.can_manage || configPendingRef.current) return;
    const definition = modelDraft.codexSubagents[index];
    if (!definition || !window.confirm(t('confirmDeleteCodexSubagent').replace('{name}', definition.name))) return;
    setModelDraft((current) => ({ ...current, codexSubagents: current.codexSubagents.filter((_, candidateIndex) => candidateIndex !== index) }));
  }

  function openSkillsDialog() {
    if (!agent?.can_manage) return;
    setManagedSkillDraft(agent.managed_skill_ids);
    setManagedSkillBase(agent.managed_skill_ids);
    setSkillsDialogOpen(true);
  }

  function closeSkillsDialog() {
    if (configPendingRef.current) return;
    setManagedSkillDraft(managedSkillBase);
    setSkillsDialogOpen(false);
  }

  function openMcpDialog(index: number | null) {
    if (!agent?.can_manage) return;
    const entries = normalizeMcpEntries(agent.mcp_allowlist);
    const entry = index === null ? undefined : entries[index];
    if (index !== null && !entry) return;
    const base = mcpDraftFor(entry);
    setMcpDialog({ index, draft: base, base, error: '' });
  }

  function closeMcpDialog() {
    if (!configPendingRef.current) setMcpDialog(null);
  }

  function updateMcpDraft(update: (draft: McpEntryDraft) => McpEntryDraft) {
    setMcpDialog((current) => current ? { ...current, draft: update(current.draft), error: '' } : null);
  }

  function validateMcpDraft(dialog: McpDialogState, entries: McpEntry[]) {
    const name = dialog.draft.name.trim();
    if (!name) return 'invalid';
    if (entries.some((entry, index) => index !== dialog.index && entry.name.trim() === name)) return 'duplicate';
    const keys = dialog.draft.secrets.map(({ key }) => key.trim());
    if (keys.some((key) => !key) || new Set(keys).size !== keys.length || dialog.draft.secrets.some(({ value }) => !value)) return 'invalid';
    const original = dialog.index === null ? null : entries[dialog.index];
    const hasUnpreservablePlaceholder = dialog.draft.secrets.some(({ key, value }) => value === REDACTED_MCP_SECRET
      && (original?.name !== name || original.secrets?.[key.trim()] !== REDACTED_MCP_SECRET));
    return hasUnpreservablePlaceholder ? 'redacted' : null;
  }

  async function saveMcpEntry(event: FormEvent) {
    event.preventDefault();
    if (!agent?.can_manage || !mcpDialog) return;
    const entries = normalizeMcpEntries(agent.mcp_allowlist);
    const validation = validateMcpDraft(mcpDialog, entries);
    if (validation) {
      setMcpDialog((current) => current ? {
        ...current,
        error: validation === 'redacted'
          ? t('mcpRedactedSecretMissing')
          : validation === 'duplicate' ? t('mcpNameDuplicate') : t('mcpEntryInvalid')
      } : null);
      return;
    }
    const nextEntry = mcpEntryFor(mcpDialog.draft);
    const nextEntries = mcpDialog.index === null
      ? [...entries, nextEntry]
      : entries.map((entry, index) => index === mcpDialog.index ? nextEntry : entry);
    const operation = beginConfigMutation();
    if (!operation) return;
    try {
      const updated = await api.updateAgent(agentId, { ...agent, mcp_allowlist: nextEntries }, operation.controller.signal);
      if (operation.controller.signal.aborted || !mounted.current || operation.generation !== mutationGeneration.current) return;
      setAgent(updated);
      setMcpDialog(null);
    } catch (caught) {
      if (!operation.controller.signal.aborted && mounted.current) {
        const message = caught instanceof ApiError && caught.code === 'mcp_redacted_secret_missing'
          ? t('mcpRedactedSecretMissing')
          : t('genericError');
        setMcpDialog((current) => current ? { ...current, error: message } : null);
      }
    } finally {
      finishConfigMutation(operation.controller, operation.generation);
    }
  }

  async function deleteMcpEntry(index: number) {
    if (!agent?.can_manage || configPendingRef.current) return;
    const entries = normalizeMcpEntries(agent.mcp_allowlist);
    const entry = entries[index];
    if (!entry || !window.confirm(t('confirmDeleteMcpEntry').replace('{name}', entry.name))) return;
    const operation = beginConfigMutation();
    if (!operation) return;
    try {
      const updated = await api.updateAgent(agentId, {
        ...agent,
        mcp_allowlist: entries.filter((_, entryIndex) => entryIndex !== index)
      }, operation.controller.signal);
      if (operation.controller.signal.aborted || !mounted.current || operation.generation !== mutationGeneration.current) return;
      setAgent(updated);
    } catch {
      if (!operation.controller.signal.aborted && mounted.current) setError(t('genericError'));
    } finally {
      finishConfigMutation(operation.controller, operation.generation);
    }
  }

  async function deleteAgent() {
    if (!agent?.can_administer || configPendingRef.current) return;
    if (!window.confirm(t('confirmDeleteAgent').replace('{name}', agent.name))) return;
    const operation = beginConfigMutation();
    if (!operation) return;
    runGeneration.current += 1;
    const pendingRefresh = refreshPromise.current;
    refreshController.current?.abort();
    refreshController.current = null;
    if (pendingRefresh) await pendingRefresh;
    try {
      await api.deleteAgent(agentId, operation.controller.signal);
      if (operation.controller.signal.aborted || !mounted.current || operation.generation !== mutationGeneration.current) return;
      setNavigationBlocker(null);
      navigate('/agents', true);
    } catch {
      if (!operation.controller.signal.aborted && mounted.current) setError(t('genericError'));
      finishConfigMutation(operation.controller, operation.generation);
    }
  }

  async function startRun(event: FormEvent) {
    event.preventDefault();
    if (!agent?.can_invoke || runPendingRef.current) return;
    runPendingRef.current = true;
    setRunPending(true);
    setError('');
    runController.current?.abort();
    const controller = new AbortController();
    runController.current = controller;
    const generation = ++runGeneration.current;
    refreshController.current?.abort();
    refreshController.current = null;
    try {
      const run = await api.createRun(
        agentId,
        message,
        continueThread ? selectedRun?.hub_session_id ?? null : null,
        continueThread ? selectedRun?.id ?? null : null,
        controller.signal
      );
      if (controller.signal.aborted || !mounted.current || generation !== runGeneration.current) return;
      setRuns((current) => [run, ...current.filter((item) => item.id !== run.id)]);
      setSelectedRun(run);
    } catch {
      if (!controller.signal.aborted && mounted.current) setError(t('genericError'));
    } finally {
      if (mounted.current && generation === runGeneration.current) {
        runPendingRef.current = false;
        setRunPending(false);
      }
    }
  }

  if (!agent) return <section className="agents-state">{loadError ? <div className="stack" role="alert"><span>{t('agentLoadFailed')}</span><button type="button" className="secondary" onClick={() => setLoadRetry((value) => value + 1)}>{t('retry')}</button></div> : t('loading')}</section>;

  const runtime = agent.runtime_id ? runtimes.find((item) => item.id === agent.runtime_id) ?? null : null;
  const availability = availabilityFor(agent, new Map(runtimes.map((item) => [item.id, item])));
  const managedSkills = agent.managed_skill_ids.flatMap((skillId) => {
    const skill = skills.find((candidate) => candidate.id === skillId);
    return skill ? [skill] : [];
  });
  const mcpEntries = normalizeMcpEntries(agent.mcp_allowlist);
  const canSetPublic = currentUser.role === 'admin' || currentUser.role === 'super_admin';
  const selectedRunForAgent = selectedRun?.agent_id === agentId ? selectedRun : null;

  return <><div className="agent-detail-page">
    <header className="agent-detail-header"><div><Bot size={18} /><h1>{agent.name}</h1></div>{agent.can_administer && <button type="button" className="secondary danger" disabled={configPending} onClick={deleteAgent}><Trash2 size={16} /> {t('deleteAgent')}</button>}</header>
    <div className="agent-detail-layout">
      <aside className="agent-inspector" role="complementary" aria-label={t('agentInspector')}>
        <section className="agent-identity"><Bot size={28} /><div><strong>{agent.name}</strong><span>{agent.instructions}</span></div></section>
        <section><h2>{t('agentInspectorRuntime')}</h2><dl><div><dt>{t('agentAvailability')}</dt><dd>{availabilityLabel(availability, t)}</dd></div><div><dt>{t('runtime')}</dt><dd>{runtime?.hostname ?? availabilityLabel(availability, t)}</dd></div></dl></section>
        <section><h2>{t('agentInspectorModel')}</h2><dl><div><dt>{t('defaultModelConnection')}</dt><dd>{modelName(agent.default_model_connection_id, modelOptions.items, t('modelNotConfigured'))}</dd></div><div><dt>{t('reasoningEffort')}</dt><dd>{reasoningEffortLabel(agent.reasoning_effort, t)}</dd></div><div><dt>{t('codexSubagents')}</dt><dd>{agent.codex_subagents.length}</dd></div></dl></section>
        <section><h2>{t('agentInspectorAccess')}</h2><dl><div><dt>{t('visibility')}</dt><dd>{visibilityLabel(agent.visibility, t)}</dd></div>{agent.visibility === 'public_to' && <div><dt>{t('agentPublicTo')}</dt><dd>{agent.public_to.length}</dd></div>}</dl></section>
        <section><h2>{t('managedSkills')}</h2><div className="agent-skill-chips">{managedSkills.map((skill) => <span key={skill.id}>{skill.name}</span>)}{managedSkills.length === 0 && <span>{t('none')}</span>}</div></section>
        <section><h2>{t('details')}</h2><dl><div><dt>{t('created')}</dt><dd>{new Date(agent.created_at).toLocaleString(locale)}</dd></div><div><dt>{t('updated')}</dt><dd>{new Date(agent.updated_at).toLocaleString(locale)}</dd></div></dl></section>
      </aside>
      <section className="agent-overview">
        <div className="agent-tabs" role="tablist" aria-label={t('agentDetailSections')}>{detailTabs.map((tab) => <button key={tab.id} id={`agent-tab-${tab.id}`} type="button" role="tab" aria-selected={activeTab === tab.id} aria-controls={`agent-panel-${tab.id}`} disabled={configPending && activeTab !== tab.id} onClick={() => setActiveTab(tab.id)}>{t(tab.key)}</button>)}</div>
        <div className="agent-panels">
          <section id="agent-panel-activity" role="tabpanel" aria-labelledby="agent-tab-activity" aria-label={t('tabActivity')} hidden={activeTab !== 'activity'}>
            {agent.can_invoke && <form className="stack agent-run-composer" onSubmit={startRun}><label>{t('message')}<textarea value={message} onChange={(event) => setMessage(event.target.value)} /></label><label className="check-row"><input type="checkbox" checked={continueThread} disabled={!selectedRunForAgent || runPending} onChange={(event) => setContinueThread(event.target.checked)} /> {t('continueThread')}</label><button className="primary" disabled={runPending}><Send size={16} /> {runPending ? t('startingRun') : t('startRun')}</button></form>}
            <div className="agent-activity-grid"><section><h2>{t('runHistory')}</h2><div className="list run-list">{runs.map((run) => <button className={`list-row ${selectedRunForAgent?.id === run.id ? 'selected' : ''}`} data-run-id={run.id} key={run.id} onClick={() => setSelectedRun(run)}><strong>{runStatusLabel(run.status, t)}</strong><span>{runSourceLabel(run.source, t)} · {run.initial_message}</span></button>)}{runs.length === 0 && <div className="compact-empty">{t('noRuns')}</div>}</div></section><section className="agent-console">{selectedRunForAgent ? <RunConsole run={selectedRunForAgent} /> : <div className="compact-empty">{t('noRuns')}</div>}</section></div>
          </section>
          <section id="agent-panel-instructions" role="tabpanel" aria-labelledby="agent-tab-instructions" aria-label={t('tabInstructions')} hidden={activeTab !== 'instructions'}>{agent.can_manage ? <form className="stack" onSubmit={(event) => saveAgentTab(event, 'instructions')}><label>{t('name')}<input disabled={configPending} value={instructionDraft.name} onChange={(event) => setInstructionDraft((current) => ({ ...current, name: event.target.value }))} /></label><MarkdownEditor label={t('instructions')} disabled={configPending} value={instructionDraft.instructions} onChange={(instructions) => setInstructionDraft((current) => ({ ...current, instructions }))} /><button className="primary" disabled={configPending || !instructionDirty}><Save size={16} /> {configPending ? t('saving') : t('saveAgent')}</button></form> : <div className="agent-readonly"><h2>{agent.name}</h2><p>{agent.instructions}</p></div>}</section>
          <section id="agent-panel-models" role="tabpanel" aria-labelledby="agent-tab-models" aria-label={t('tabModels')} hidden={activeTab !== 'models'}>
            {agent.can_manage ? <form className="stack" onSubmit={(event) => saveAgentTab(event, 'models')}>
              <div className="agent-model-fields"><label>{t('defaultModelConnection')}<select disabled={configPending} value={modelDraft.defaultModelConnectionId ?? ''} onChange={(event) => setModelDraft((current) => ({ ...current, defaultModelConnectionId: event.target.value || null }))}><option value="">{t('modelNotConfigured')}</option>{modelOptions.items.map((option) => <option key={option.id} value={option.id} disabled={option.status === 'disabled' && option.id !== modelDraft.defaultModelConnectionId}>{modelOptionLabel(option, t)}</option>)}</select></label><label>{t('reasoningEffort')}<select disabled={configPending} value={modelDraft.reasoningEffort} onChange={(event) => setModelDraft((current) => ({ ...current, reasoningEffort: event.target.value as ReasoningEffort }))}>{reasoningEfforts.map((effort) => <option key={effort} value={effort}>{reasoningEffortLabel(effort, t)}</option>)}</select></label></div>
              <section className="agent-subagent-section"><div className="agent-subagent-heading"><span className="field-label">{t('codexSubagents')}</span><button type="button" className="secondary" disabled={configPending || modelDraft.codexSubagents.length >= 32} onClick={() => openSubagentDialog(null)}><Plus size={16} /> {t('addCodexSubagent')}</button></div><SubagentTable definitions={modelDraft.codexSubagents} modelOptions={modelOptions.items} canManage disabled={configPending} onEdit={openSubagentDialog} onDelete={deleteSubagentDefinition} /></section>
              <button className="primary" disabled={configPending || !modelDirty}><Save size={16} /> {configPending ? t('saving') : t('saveAgent')}</button>
            </form> : <div className="stack agent-readonly"><dl className="agent-model-summary"><div><dt>{t('defaultModelConnection')}</dt><dd>{modelName(agent.default_model_connection_id, modelOptions.items, t('modelNotConfigured'))}</dd></div><div><dt>{t('reasoningEffort')}</dt><dd>{reasoningEffortLabel(agent.reasoning_effort, t)}</dd></div></dl><SubagentTable definitions={agent.codex_subagents} modelOptions={modelOptions.items} canManage={false} disabled onEdit={() => undefined} onDelete={() => undefined} /></div>}
          </section>
          <section id="agent-panel-skills" role="tabpanel" aria-labelledby="agent-tab-skills" aria-label={t('tabSkills')} hidden={activeTab !== 'skills'}>
            {agent.can_manage && <div className="button-row agent-panel-actions"><button type="button" className="secondary" disabled={configPending} onClick={openSkillsDialog}><Pencil size={16} /> {t('editManagedSkills')}</button></div>}
            <div className="agent-skill-chips">{managedSkills.map((skill) => <span key={skill.id}>{skill.name}</span>)}{managedSkills.length === 0 && <span>{t('none')}</span>}</div>
          </section>
          <section id="agent-panel-mcp" role="tabpanel" aria-labelledby="agent-tab-mcp" aria-label={t('tabMcp')} hidden={activeTab !== 'mcp'}>
            {agent.can_manage && <div className="button-row agent-panel-actions"><button type="button" className="secondary" disabled={configPending} onClick={() => openMcpDialog(null)}><Plus size={16} /> {t('addMcpEntry')}</button></div>}
            <div className="agents-table-wrap agent-mcp-table-wrap">
              <table className="agents-table agent-mcp-table" aria-label={t('mcpAllowlist')}>
                <thead><tr><th>{t('name')}</th><th>{t('mcpCommand')}</th><th>{t('mcpArgs')}</th><th>{t('mcpSecrets')}</th>{agent.can_manage && <th className="agent-mcp-actions-column">{t('mcpActions')}</th>}</tr></thead>
                <tbody>{mcpEntries.map((entry, index) => <tr key={entry.name}>
                  <td><strong>{entry.name}</strong></td>
                  <td><code>{entry.command ?? '-'}</code></td>
                  <td><code>{entry.args?.join(' ') || '-'}</code></td>
                  <td><div className="agent-mcp-secret-list">{Object.keys(entry.secrets ?? {}).map((key) => <code key={key}>{key}={REDACTED_MCP_SECRET}</code>)}{Object.keys(entry.secrets ?? {}).length === 0 && '-'}</div></td>
                  {agent.can_manage && <td className="agent-mcp-actions-column"><div className="button-row agent-mcp-actions"><button type="button" className="icon-button" disabled={configPending} aria-label={`${t('editMcpEntry')}: ${entry.name}`} title={`${t('editMcpEntry')}: ${entry.name}`} onClick={() => openMcpDialog(index)}><Pencil size={16} /></button><button type="button" className="icon-button" disabled={configPending} aria-label={`${t('delete')} ${entry.name}`} title={`${t('delete')} ${entry.name}`} onClick={() => deleteMcpEntry(index)}><Trash2 size={16} /></button></div></td>}
                </tr>)}{mcpEntries.length === 0 && <tr><td colSpan={agent.can_manage ? 5 : 4}><div className="compact-empty">{t('noMcpEntries')}</div></td></tr>}</tbody>
              </table>
            </div>
          </section>
          <section id="agent-panel-access" role="tabpanel" aria-labelledby="agent-tab-access" aria-label={t('tabAccess')} hidden={activeTab !== 'access'}>{agent.can_manage ? <form className="stack" onSubmit={(event) => saveAgentTab(event, 'access')}><label>{t('visibility')}<select disabled={configPending} value={accessDraft.visibility} onChange={(event) => setAccessDraft((current) => ({ ...current, visibility: event.target.value, publicTo: event.target.value === 'public_to' ? current.publicTo : [] }))}><option value="private">{t('private')}</option><option value="public_to">{t('specificUsers')}</option>{(canSetPublic || accessDraft.visibility === 'public') && <option value="public">{t('public')}</option>}</select></label>{accessDraft.visibility === 'public_to' && <fieldset className="agent-user-picker" disabled={configPending}><legend>{t('agentPublicTo')}</legend>{users.filter((user) => user.id !== agent.owner_id).map((user) => <label className="check-row" key={user.id}><input type="checkbox" checked={accessDraft.publicTo.includes(user.id)} onChange={(event) => setAccessDraft((current) => ({ ...current, publicTo: event.target.checked ? [...current.publicTo, user.id] : current.publicTo.filter((id) => id !== user.id) }))} /> {user.display_name} ({user.email ?? user.username})</label>)}</fieldset>}<label>{t('runtime')}<select disabled={configPending} value={accessDraft.runtimeId ?? ''} onChange={(event) => setAccessDraft((current) => ({ ...current, runtimeId: event.target.value || null }))}><option value="">{t('automatic')}</option>{runtimes.map((item) => <option key={item.id} value={item.id}>{item.hostname} · {runtimeStatusLabel(item.status, t)}</option>)}</select></label><button className="primary" disabled={configPending || !accessDirty}><Save size={16} /> {configPending ? t('saving') : t('saveAgent')}</button></form> : <div className="agent-readonly">{visibilityLabel(agent.visibility, t)}</div>}</section>
        </div>
        {error && <div className="error agent-detail-error" role="alert">{error}</div>}
      </section>
    </div>
  </div>
  {skillsDialogOpen && <FormDialog
    title={t('editManagedSkills')}
    busy={configPending}
    onClose={closeSkillsDialog}
    className="agent-skills-dialog"
    initialFocusRef={firstSkillRef}
    footer={<><button type="button" className="secondary" disabled={configPending} onClick={closeSkillsDialog}>{t('cancel')}</button><button type="submit" form="agent-skills-form" className="primary" disabled={configPending || !skillsDirty}>{configPending ? t('saving') : t('saveChanges')}</button></>}
  >
    <form id="agent-skills-form" className="stack" onSubmit={(event) => saveAgentTab(event, 'skills')}>
      <fieldset className="agent-user-picker" disabled={configPending}><legend>{t('managedSkills')}</legend>{skills.map((skill, index) => <label className="check-row" key={skill.id}><input ref={index === 0 ? firstSkillRef : undefined} type="checkbox" checked={managedSkillDraft.includes(skill.id)} onChange={(event) => setManagedSkillDraft((current) => event.target.checked ? [...current, skill.id] : current.filter((id) => id !== skill.id))} /><span>{skill.name}{skill.description && <small className="muted">{skill.description}</small>}</span></label>)}{skills.length === 0 && <span>{t('noSkills')}</span>}</fieldset>
    </form>
  </FormDialog>}
  {mcpDialog && <FormDialog
    title={mcpDialog.index === null ? t('addMcpEntry') : `${t('editMcpEntry')}: ${mcpDialog.draft.name}`}
    busy={configPending}
    onClose={closeMcpDialog}
    className="agent-mcp-dialog"
    initialFocusRef={mcpNameRef}
    footer={<><button type="button" className="secondary" disabled={configPending} onClick={closeMcpDialog}>{t('cancel')}</button><button type="submit" form="agent-mcp-form" className="primary" disabled={configPending}>{configPending ? t('saving') : t('saveChanges')}</button></>}
  >
    <form id="agent-mcp-form" className="stack" onSubmit={saveMcpEntry}>
      <label>{t('name')}<input ref={mcpNameRef} required disabled={configPending} value={mcpDialog.draft.name} onChange={(event) => updateMcpDraft((draft) => ({ ...draft, name: event.target.value }))} /></label>
      <label>{t('mcpCommand')}<input disabled={configPending} value={mcpDialog.draft.command} onChange={(event) => updateMcpDraft((draft) => ({ ...draft, command: event.target.value }))} /></label>
      <label>{t('mcpArgs')}<textarea disabled={configPending} value={mcpDialog.draft.args} onChange={(event) => updateMcpDraft((draft) => ({ ...draft, args: event.target.value }))} /></label>
      <fieldset className="agent-user-picker agent-mcp-secrets" disabled={configPending}><legend>{t('mcpSecrets')}</legend>
        {mcpDialog.draft.secrets.map((secret, index) => <div className="agent-mcp-secret-row" key={index}>
          <label><span className="sr-only">{t('mcpSecretName').replace('{index}', String(index + 1))}</span><input required aria-label={t('mcpSecretName').replace('{index}', String(index + 1))} value={secret.key} onChange={(event) => updateMcpDraft((draft) => ({ ...draft, secrets: draft.secrets.map((item, itemIndex) => itemIndex === index ? { ...item, key: event.target.value } : item) }))} /></label>
          <label><span className="sr-only">{t('mcpSecretValue').replace('{index}', String(index + 1))}</span><input required type="password" autoComplete="new-password" aria-label={t('mcpSecretValue').replace('{index}', String(index + 1))} value={secret.value} onChange={(event) => updateMcpDraft((draft) => ({ ...draft, secrets: draft.secrets.map((item, itemIndex) => itemIndex === index ? { ...item, value: event.target.value } : item) }))} /></label>
          <button type="button" className="icon-button" aria-label={`${t('delete')} ${secret.key || index + 1}`} title={`${t('delete')} ${secret.key || index + 1}`} onClick={() => updateMcpDraft((draft) => ({ ...draft, secrets: draft.secrets.filter((_, itemIndex) => itemIndex !== index) }))}><Trash2 size={16} /></button>
        </div>)}
        <button type="button" className="secondary" onClick={() => updateMcpDraft((draft) => ({ ...draft, secrets: [...draft.secrets, { key: '', value: '' }] }))}><Plus size={16} /> {t('addMcpSecret')}</button>
      </fieldset>
      {mcpDialog.error && <div className="error" role="alert">{mcpDialog.error}</div>}
    </form>
  </FormDialog>}
  {subagentDialog && <SubagentDialog dialog={subagentDialog} definitions={modelDraft.codexSubagents} modelOptions={modelOptions.items} formId="agent-subagent-form" busy={configPending} onChange={setSubagentDialog} onCommit={commitSubagent} onClose={() => setSubagentDialog(null)} />}
  </>;
}
