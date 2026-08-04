import { ArrowDown, ArrowUp, Bot, Pencil, Plus, Save, Search, Trash2 } from 'lucide-react';
import { ComponentType, FormEvent, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  Agent,
  api,
  ApiError,
  type AgentSecretDeclaration,
  type AgentModelSettings,
  type AgentModelSettingsOverride,
  type SubagentDefinition,
  type ModelConnectionOption,
  type ModelConnectionOptions,
  type ModelReasoningSummary,
  type ModelReasoningSummarySupport,
  type ModelRequestSettings,
  type ModelSelection,
  type ModelUpstreamProtocol,
  type ModelVerbosity,
  type ReasoningEffort,
  Run,
  Runtime,
  Skill,
  User
} from './api/client';
import { FormDialog } from './components/form-dialog';
import { MarkdownEditor } from './components/markdown-editor';
import { builtInTools, normalizeToolAllowlist, ToolAllowlistPicker } from './components/tool-allowlist';
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

const reasoningSummaries: ModelReasoningSummary[] = ['default', 'auto', 'concise', 'detailed', 'none'];
const verbosities: ModelVerbosity[] = ['default', 'low', 'medium', 'high'];
const summarySupports: ModelReasoningSummarySupport[] = ['auto', 'supported', 'unsupported'];

const automaticAgentModelSettings: AgentModelSettings = {
  reasoning_effort: 'default',
  reasoning_summary: 'default',
  verbosity: 'default',
  context_window_tokens: null,
  auto_compact_token_limit: null,
  reasoning_summary_support: 'auto',
  service_tier: null,
  provider_request_timeout_ms: null,
  stream_max_retries: null,
  stream_idle_timeout_ms: null,
  request_settings: { protocol: 'openai_responses' }
};

function requestSettingsFor(apiType: ModelUpstreamProtocol): ModelRequestSettings {
  if (apiType === 'openai_chat_completions') {
    return { protocol: apiType, temperature: null, top_p: null, max_completion_tokens: null };
  }
  if (apiType === 'anthropic_messages') {
    return { protocol: apiType, temperature: null, top_p: null, max_tokens: null };
  }
  return { protocol: apiType };
}

function selectedOption(selection: ModelSelection | null, options: ModelConnectionOption[]) {
  if (!selection) return undefined;
  return options.find((option) => option.connection_id === selection.connection_id && option.model_id === selection.model_id);
}

function selectionValue(selection: ModelSelection | null) {
  return selection ? `${selection.connection_id}\n${selection.model_id}` : '';
}

function selectionFromValue(value: string, options: ModelConnectionOption[]) {
  const option = options.find((candidate) => `${candidate.connection_id}\n${candidate.model_id}` === value);
  return option ? { connection_id: option.connection_id, model_id: option.model_id } : null;
}

function settingsForSelection(settings: AgentModelSettings, selection: ModelSelection | null, options: ModelConnectionOption[]) {
  const apiType = selectedOption(selection, options)?.api_type ?? 'openai_responses';
  return settings.request_settings.protocol === apiType
    ? settings
    : { ...settings, request_settings: requestSettingsFor(apiType) };
}

function optionalNumber(value: string) {
  return value.trim() ? Number(value) : null;
}

const secretVariableNamePattern = /^[A-Z_][A-Z0-9_]*$/;

function emptySecretDeclaration(): AgentSecretDeclaration {
  return { name: '', kind: 'value', description: '' };
}

function validateSecretDeclarations(declarations: AgentSecretDeclaration[]): 'secretNameInvalid' | 'secretVariableNameDuplicate' | null {
  const names = new Set<string>();
  for (const declaration of declarations) {
    const name = declaration.name.trim();
    if (!secretVariableNamePattern.test(name) || name.length > 128) return 'secretNameInvalid';
    if (names.has(name)) return 'secretVariableNameDuplicate';
    names.add(name);
  }
  return null;
}

function sameSecretDeclarations(left: AgentSecretDeclaration[], right: AgentSecretDeclaration[]) {
  return JSON.stringify(left) === JSON.stringify(right);
}

function SecretDeclarationsEditor({ declarations, disabled, onChange }: { declarations: AgentSecretDeclaration[]; disabled: boolean; onChange: (declarations: AgentSecretDeclaration[]) => void }) {
  const { t } = useI18n();
  function update(index: number, patch: Partial<AgentSecretDeclaration>) {
    onChange(declarations.map((declaration, candidateIndex) => candidateIndex === index ? { ...declaration, ...patch } : declaration));
  }
  return <section className="secret-variable-editor">
    <div className="agent-subagent-heading"><span className="field-label">{t('secretVariables')}</span><button type="button" className="secondary" disabled={disabled || declarations.length >= 128} onClick={() => onChange([...declarations, emptySecretDeclaration()])}><Plus size={16} /> {t('addSecretVariable')}</button></div>
    {declarations.length === 0 ? <div className="compact-empty">{t('noSecretVariables')}</div> : <div className="secret-variable-list">
      {declarations.map((declaration, index) => <div className="secret-variable-row" key={`${declaration.name}-${index}`}>
        <label>{t('secretName')}<input maxLength={128} disabled={disabled} value={declaration.name} onChange={(event) => update(index, { name: event.target.value })} /></label>
        <label>{t('secretKind')}<select disabled={disabled} value={declaration.kind} onChange={(event) => update(index, { kind: event.target.value === 'file' ? 'file' : 'value' })}><option value="value">{t('secretKindValue')}</option><option value="file">{t('secretKindFile')}</option></select></label>
        <label>{t('secretDescription')}<input maxLength={512} disabled={disabled} value={declaration.description} onChange={(event) => update(index, { description: event.target.value })} /></label>
        <button type="button" className="icon-button" disabled={disabled} aria-label={`${t('removeSecretVariable')}: ${declaration.name || index + 1}`} title={t('removeSecretVariable')} onClick={() => onChange(declarations.filter((_, candidateIndex) => candidateIndex !== index))}><Trash2 size={16} /></button>
      </div>)}
    </div>}
  </section>;
}

function modelOptionLabel(option: ModelConnectionOption, t: ReturnType<typeof useI18n>['t']) {
  const scope = option.scope === 'global' ? t('modelScopeGlobal') : t('modelScopePersonal');
  const status = option.status === 'disabled' ? ` · ${t('disabled')}` : '';
  return `${option.connection_name} · ${option.model_id} · ${scope}${status}`;
}

function modelName(
  selection: ModelSelection | null,
  options: ModelConnectionOption[],
  fallback: string
) {
  if (!selection) return fallback;
  const option = selectedOption(selection, options);
  return option ? `${option.connection_name} · ${option.model_id}` : `${selection.connection_id} · ${selection.model_id}`;
}

function settingValue(value: unknown, t: ReturnType<typeof useI18n>['t']) {
  if (value === null || value === undefined) return t('modelParameterAutomatic');
  if (typeof value === 'object') return JSON.stringify(value);
  return String(value);
}

function AgentModelSettingsFields({
  settings,
  apiType,
  disabled,
  onChange
}: {
  settings: AgentModelSettings;
  apiType: ModelUpstreamProtocol;
  disabled: boolean;
  onChange: (settings: AgentModelSettings) => void;
}) {
  const { t } = useI18n();
  const requestSettings = settings.request_settings.protocol === apiType
    ? settings.request_settings
    : requestSettingsFor(apiType);
  const update = <K extends keyof AgentModelSettings>(key: K, value: AgentModelSettings[K]) => onChange({ ...settings, [key]: value });
  const updateRequest = (value: ModelRequestSettings) => onChange({ ...settings, request_settings: value });
  const source = (value: unknown) => <small className="agent-setting-effective">{t('effectiveValue')}: {settingValue(value, t)} · {value === null || value === 'default' || value === 'auto' ? t('sourceAutomatic') : t('sourceAgent')}</small>;

  return <div className="agent-model-settings">
    <fieldset><legend>{t('modelParameterGenerationGroup')}</legend><div className="agent-model-setting-grid">
      <label>{t('reasoningEffort')}<select disabled={disabled} value={settings.reasoning_effort} onChange={(event) => update('reasoning_effort', event.target.value as ReasoningEffort)}>{reasoningEfforts.map((value) => <option key={value} value={value}>{value}</option>)}</select>{source(settings.reasoning_effort)}</label>
      <label>{t('reasoningSummary')}<select disabled={disabled} value={settings.reasoning_summary} onChange={(event) => update('reasoning_summary', event.target.value as ModelReasoningSummary)}>{reasoningSummaries.map((value) => <option key={value} value={value}>{value}</option>)}</select>{source(settings.reasoning_summary)}</label>
      <label>{t('verbosity')}<select disabled={disabled} value={settings.verbosity} onChange={(event) => update('verbosity', event.target.value as ModelVerbosity)}>{verbosities.map((value) => <option key={value} value={value}>{value}</option>)}</select>{source(settings.verbosity)}</label>
      <label>{t('reasoningSummarySupport')}<select disabled={disabled} value={settings.reasoning_summary_support} onChange={(event) => update('reasoning_summary_support', event.target.value as ModelReasoningSummarySupport)}>{summarySupports.map((value) => <option key={value} value={value}>{value}</option>)}</select>{source(settings.reasoning_summary_support)}</label>
      <label>{t('serviceTier')}<input disabled={disabled} maxLength={64} value={settings.service_tier ?? ''} placeholder={t('modelParameterAutomatic')} onChange={(event) => update('service_tier', event.target.value || null)} />{source(settings.service_tier)}</label>
    </div></fieldset>
    <fieldset><legend>{t('modelParameterContextGroup')}</legend><div className="agent-model-setting-grid">
      <label>{t('contextWindowTokens')}<input disabled={disabled} type="number" min={1} step={1} value={settings.context_window_tokens ?? ''} placeholder={t('modelParameterAutomatic')} onChange={(event) => update('context_window_tokens', optionalNumber(event.target.value))} />{source(settings.context_window_tokens)}</label>
      <label>{t('autoCompactTokenLimit')}<input disabled={disabled} type="number" min={1} max={settings.context_window_tokens ?? undefined} step={1} value={settings.auto_compact_token_limit ?? ''} placeholder={t('modelParameterAutomatic')} onChange={(event) => update('auto_compact_token_limit', optionalNumber(event.target.value))} />{source(settings.auto_compact_token_limit)}</label>
    </div></fieldset>
    <fieldset><legend>{t('modelParameterReliabilityGroup')}</legend><div className="agent-model-setting-grid">
      <label>{t('providerRequestTimeoutMs')}<input disabled={disabled} type="number" min={1} step={1} value={settings.provider_request_timeout_ms ?? ''} placeholder={t('modelParameterAutomatic')} onChange={(event) => update('provider_request_timeout_ms', optionalNumber(event.target.value))} />{source(settings.provider_request_timeout_ms)}</label>
      <label>{t('streamMaxRetries')}<input disabled={disabled} type="number" min={0} max={100} step={1} value={settings.stream_max_retries ?? ''} placeholder={t('modelParameterAutomatic')} onChange={(event) => update('stream_max_retries', optionalNumber(event.target.value))} />{source(settings.stream_max_retries)}</label>
      <label>{t('streamIdleTimeoutMs')}<input disabled={disabled} type="number" min={1} step={1} value={settings.stream_idle_timeout_ms ?? ''} placeholder={t('modelParameterAutomatic')} onChange={(event) => update('stream_idle_timeout_ms', optionalNumber(event.target.value))} />{source(settings.stream_idle_timeout_ms)}</label>
    </div></fieldset>
    <fieldset><legend>{t('modelRequestParametersGroup')} · <code>{apiType}</code></legend>
      {apiType === 'openai_responses' ? <p className="agent-setting-note">{t('responsesRequestSettingsAutomatic')}</p> : <div className="agent-model-setting-grid">
        <label><code>temperature</code><input aria-label="temperature" disabled={disabled} type="number" min={0} max={apiType === 'anthropic_messages' ? 1 : 2} step="any" value={'temperature' in requestSettings ? requestSettings.temperature ?? '' : ''} onChange={(event) => {
          if (!('temperature' in requestSettings)) return;
          updateRequest({ ...requestSettings, temperature: optionalNumber(event.target.value), ...(apiType === 'anthropic_messages' && event.target.value ? { top_p: null } : {}) });
        }} />{source('temperature' in requestSettings ? requestSettings.temperature : null)}</label>
        <label><code>top_p</code><input aria-label="top_p" disabled={disabled} type="number" min={0} max={1} step="any" value={'top_p' in requestSettings ? requestSettings.top_p ?? '' : ''} onChange={(event) => {
          if (!('top_p' in requestSettings)) return;
          updateRequest({ ...requestSettings, top_p: optionalNumber(event.target.value), ...(apiType === 'anthropic_messages' && event.target.value ? { temperature: null } : {}) });
        }} />{source('top_p' in requestSettings ? requestSettings.top_p : null)}</label>
        {apiType === 'openai_chat_completions' && requestSettings.protocol === apiType && <label><code>max_completion_tokens</code><input aria-label="max_completion_tokens" disabled={disabled} type="number" min={1} step={1} value={requestSettings.max_completion_tokens ?? ''} onChange={(event) => updateRequest({ ...requestSettings, max_completion_tokens: optionalNumber(event.target.value) })} />{source(requestSettings.max_completion_tokens)}</label>}
        {apiType === 'anthropic_messages' && requestSettings.protocol === apiType && <label><code>max_tokens</code><input aria-label="max_tokens" disabled={disabled} type="number" min={1} step={1} value={requestSettings.max_tokens ?? ''} onChange={(event) => updateRequest({ ...requestSettings, max_tokens: optionalNumber(event.target.value) })} />{source(requestSettings.max_tokens)}</label>}
      </div>}
    </fieldset>
  </div>;
}

type OverrideKey = Exclude<keyof AgentModelSettingsOverride, 'request_settings'>;

function overrideMode(overrides: AgentModelSettingsOverride, key: keyof AgentModelSettingsOverride) {
  return Object.hasOwn(overrides, key) ? overrides[key] === null ? 'automatic' : 'override' : 'inherit';
}

function changeOverrideMode(
  overrides: AgentModelSettingsOverride,
  key: keyof AgentModelSettingsOverride,
  mode: string,
  fallback: unknown
) {
  if (mode === 'inherit') {
    const next = { ...overrides };
    delete next[key];
    return next;
  }
  return { ...overrides, [key]: mode === 'automatic' ? null : fallback };
}

function effectiveOverrideValue(
  overrides: AgentModelSettingsOverride,
  parent: AgentModelSettings,
  key: OverrideKey
) {
  const mode = overrideMode(overrides, key);
  if (mode === 'inherit') return { value: parent[key], source: 'sourceAgent' as const };
  if (mode === 'automatic') return { value: automaticAgentModelSettings[key], source: 'sourceAutomatic' as const };
  return { value: overrides[key], source: 'sourceSubagent' as const };
}

function SubagentModelSettingsFields({
  overrides,
  parent,
  apiType,
  protocolChanged,
  disabled,
  onChange
}: {
  overrides: AgentModelSettingsOverride;
  parent: AgentModelSettings;
  apiType: ModelUpstreamProtocol;
  protocolChanged: boolean;
  disabled: boolean;
  onChange: (overrides: AgentModelSettingsOverride) => void;
}) {
  const { t } = useI18n();
  const setValue = (key: OverrideKey, value: unknown) => onChange({ ...overrides, [key]: value });
  const source = (key: OverrideKey) => {
    const effective = effectiveOverrideValue(overrides, parent, key);
    return <small className="agent-setting-effective">{t('effectiveValue')}: {settingValue(effective.value, t)} · {t(effective.source)}</small>;
  };
  const modeSelect = (key: OverrideKey, label: string, fallback: unknown) => <select className="agent-override-mode" aria-label={`${label} ${t('overrideMode')}`} disabled={disabled} value={overrideMode(overrides, key)} onChange={(event) => onChange(changeOverrideMode(overrides, key, event.target.value, fallback))}><option value="inherit">{t('inheritAgentSetting')}</option><option value="automatic">{t('modelParameterAutomatic')}</option><option value="override">{t('overrideValue')}</option></select>;
  const enumField = <T extends string>(key: OverrideKey, label: string, values: T[], fallback: T) => <label>{label}<span className="agent-override-controls">{modeSelect(key, label, fallback)}{overrideMode(overrides, key) === 'override' && <select disabled={disabled} value={String(overrides[key] ?? fallback)} onChange={(event) => setValue(key, event.target.value)}>{values.map((value) => <option key={value} value={value}>{value}</option>)}</select>}</span>{source(key)}</label>;
  const numberField = (key: OverrideKey, label: string, min: number, max?: number) => <label>{label}<span className="agent-override-controls">{modeSelect(key, label, min)}{overrideMode(overrides, key) === 'override' && <input disabled={disabled} type="number" min={min} max={max} step={1} value={String(overrides[key] ?? min)} onChange={(event) => setValue(key, Number(event.target.value))} />}</span>{source(key)}</label>;
  const requestMode = overrideMode(overrides, 'request_settings');
  const parentRequest = parent.request_settings.protocol === apiType ? parent.request_settings : requestSettingsFor(apiType);
  const effectiveRequest = requestMode === 'override' && overrides.request_settings
    ? overrides.request_settings
    : requestMode === 'inherit' && !protocolChanged ? parentRequest : requestSettingsFor(apiType);
  const requestSource = requestMode === 'override' ? t('sourceSubagent') : requestMode === 'inherit' && !protocolChanged ? t('sourceAgent') : t('sourceAutomatic');

  return <div className="agent-model-settings agent-subagent-settings">
    <fieldset><legend>{t('modelParameterGenerationGroup')}</legend><div className="agent-model-setting-grid">
      {enumField('reasoning_effort', t('reasoningEffort'), reasoningEfforts, parent.reasoning_effort)}
      {enumField('reasoning_summary', t('reasoningSummary'), reasoningSummaries, parent.reasoning_summary)}
      {enumField('verbosity', t('verbosity'), verbosities, parent.verbosity)}
      {enumField('reasoning_summary_support', t('reasoningSummarySupport'), summarySupports, parent.reasoning_summary_support)}
      <label>{t('serviceTier')}<span className="agent-override-controls">{modeSelect('service_tier', t('serviceTier'), parent.service_tier ?? 'default')}{overrideMode(overrides, 'service_tier') === 'override' && <input disabled={disabled} maxLength={64} value={String(overrides.service_tier ?? '')} onChange={(event) => setValue('service_tier', event.target.value)} />}</span>{source('service_tier')}</label>
    </div></fieldset>
    <fieldset><legend>{t('modelParameterContextGroup')}</legend><div className="agent-model-setting-grid">
      {numberField('context_window_tokens', t('contextWindowTokens'), 1)}
      {numberField('auto_compact_token_limit', t('autoCompactTokenLimit'), 1)}
    </div></fieldset>
    <fieldset><legend>{t('modelParameterReliabilityGroup')}</legend><div className="agent-model-setting-grid">
      {numberField('provider_request_timeout_ms', t('providerRequestTimeoutMs'), 1)}
      {numberField('stream_max_retries', t('streamMaxRetries'), 0, 100)}
      {numberField('stream_idle_timeout_ms', t('streamIdleTimeoutMs'), 1)}
    </div></fieldset>
    <fieldset><legend>{t('modelRequestParametersGroup')} · <code>{apiType}</code></legend>
      <label>{t('overrideMode')}<select disabled={disabled} value={requestMode} onChange={(event) => onChange(changeOverrideMode(overrides, 'request_settings', event.target.value, requestSettingsFor(apiType)))}><option value="inherit">{t('inheritAgentSetting')}</option><option value="automatic">{t('modelParameterAutomatic')}</option><option value="override">{t('overrideValue')}</option></select><small className="agent-setting-effective">{t('effectiveValue')}: {settingValue(effectiveRequest, t)} · {requestSource}</small></label>
      {requestMode === 'override' && (apiType === 'openai_responses' ? <p className="agent-setting-note">{t('responsesRequestSettingsAutomatic')}</p> : <div className="agent-model-setting-grid">
        <label><code>temperature</code><input aria-label="temperature" disabled={disabled} type="number" min={0} max={apiType === 'anthropic_messages' ? 1 : 2} step="any" value={'temperature' in effectiveRequest ? effectiveRequest.temperature ?? '' : ''} onChange={(event) => {
          if (!('temperature' in effectiveRequest)) return;
          onChange({ ...overrides, request_settings: { ...effectiveRequest, temperature: optionalNumber(event.target.value), ...(apiType === 'anthropic_messages' && event.target.value ? { top_p: null } : {}) } });
        }} /></label>
        <label><code>top_p</code><input aria-label="top_p" disabled={disabled} type="number" min={0} max={1} step="any" value={'top_p' in effectiveRequest ? effectiveRequest.top_p ?? '' : ''} onChange={(event) => {
          if (!('top_p' in effectiveRequest)) return;
          onChange({ ...overrides, request_settings: { ...effectiveRequest, top_p: optionalNumber(event.target.value), ...(apiType === 'anthropic_messages' && event.target.value ? { temperature: null } : {}) } });
        }} /></label>
        {apiType === 'openai_chat_completions' && effectiveRequest.protocol === apiType && <label><code>max_completion_tokens</code><input aria-label="max_completion_tokens" disabled={disabled} type="number" min={1} step={1} value={effectiveRequest.max_completion_tokens ?? ''} onChange={(event) => onChange({ ...overrides, request_settings: { ...effectiveRequest, max_completion_tokens: optionalNumber(event.target.value) } })} /></label>}
        {apiType === 'anthropic_messages' && effectiveRequest.protocol === apiType && <label><code>max_tokens</code><input aria-label="max_tokens" disabled={disabled} type="number" min={1} step={1} value={effectiveRequest.max_tokens ?? ''} onChange={(event) => onChange({ ...overrides, request_settings: { ...effectiveRequest, max_tokens: optionalNumber(event.target.value) } })} /></label>}
      </div>)}
    </fieldset>
  </div>;
}

type SubagentDialogState = {
  index: number | null;
  draft: SubagentDefinition;
  error: string;
};

function emptySubagent(): SubagentDefinition {
  return {
    name: '',
    description: '',
    developer_instructions: '',
    model_selection: null,
    model_settings_override: {}
  };
}

function editableSubagent(definition?: SubagentDefinition): SubagentDefinition {
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
  definitions: SubagentDefinition[];
  modelOptions: ModelConnectionOption[];
  canManage: boolean;
  disabled: boolean;
  onEdit: (index: number) => void;
  onDelete: (index: number) => void;
}) {
  const { t } = useI18n();
  return <div className="agents-table-wrap agent-subagent-table-wrap">
    <table className="agents-table agent-subagent-table" aria-label={t('subagents')}>
      <thead><tr><th>{t('subagentName')}</th><th>{t('subagentDescription')}</th><th>{t('subagentModelOverride')}</th><th>{t('subagentReasoningOverride')}</th><th>{t('status')}</th>{canManage && <th className="agent-subagent-actions-column">{t('actions')}</th>}</tr></thead>
      <tbody>{definitions.map((definition, index) => <tr key={`${definition.name}-${index}`}>
        <td><strong>{definition.name}</strong></td>
        <td>{definition.description}</td>
        <td>{modelName(definition.model_selection, modelOptions, t('inheritAgentModel'))}</td>
        <td>{Object.keys(definition.model_settings_override).length ? t('subagentOverrideCount').replace('{count}', String(Object.keys(definition.model_settings_override).length)) : t('inheritAllAgentSettings')}</td>
        <td>{definition.enabled === false ? <span title={definition.disabled_reason ?? undefined}>{t('disabled')}</span> : t('enabled')}</td>
        {canManage && <td className="agent-subagent-actions-column"><div className="button-row agent-subagent-actions"><button type="button" className="icon-button" disabled={disabled} aria-label={`${t('editSubagent')}: ${definition.name}`} title={`${t('editSubagent')}: ${definition.name}`} onClick={() => onEdit(index)}><Pencil size={16} /></button><button type="button" className="icon-button" disabled={disabled} aria-label={`${t('delete')} ${definition.name}`} title={`${t('delete')} ${definition.name}`} onClick={() => onDelete(index)}><Trash2 size={16} /></button></div></td>}
      </tr>)}{definitions.length === 0 && <tr><td colSpan={canManage ? 6 : 5}><div className="compact-empty">{t('noSubagents')}</div></td></tr>}</tbody>
    </table>
  </div>;
}

function SubagentDialog({
  dialog,
  definitions,
  modelOptions,
  parentSelection,
  parentSettings,
  formId,
  busy,
  onChange,
  onCommit,
  onClose
}: {
  dialog: SubagentDialogState;
  definitions: SubagentDefinition[];
  modelOptions: ModelConnectionOption[];
  parentSelection: ModelSelection | null;
  parentSettings: AgentModelSettings;
  formId: string;
  busy: boolean;
  onChange: (dialog: SubagentDialogState) => void;
  onCommit: (index: number | null, definition: SubagentDefinition) => void;
  onClose: () => void;
}) {
  const { t } = useI18n();
  const nameRef = useRef<HTMLInputElement>(null);
  const effectiveSelection = dialog.draft.model_selection ?? parentSelection;
  const parentApiType = selectedOption(parentSelection, modelOptions)?.api_type ?? 'openai_responses';
  const effectiveApiType = selectedOption(effectiveSelection, modelOptions)?.api_type ?? parentApiType;
  const protocolChanged = Boolean(dialog.draft.model_selection && effectiveApiType !== parentApiType);

  function update(update: Partial<SubagentDefinition>) {
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
    title={dialog.index === null ? t('addSubagent') : `${t('editSubagent')}: ${dialog.draft.name}`}
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
      <label>{t('subagentModelOverride')}<select disabled={busy} value={selectionValue(dialog.draft.model_selection)} onChange={(event) => update({ model_selection: selectionFromValue(event.target.value, modelOptions) })}><option value="">{t('inheritAgentModel')}</option>{modelOptions.map((option) => {
        const value = selectionValue({ connection_id: option.connection_id, model_id: option.model_id });
        return <option key={value} value={value} disabled={option.status === 'disabled' && value !== selectionValue(dialog.draft.model_selection)}>{modelOptionLabel(option, t)}</option>;
      })}</select><small className="agent-setting-effective">{t('effectiveValue')}: {modelName(effectiveSelection, modelOptions, t('modelNotConfigured'))} · {dialog.draft.model_selection ? t('sourceSubagent') : t('sourceAgent')}</small></label>
      <SubagentModelSettingsFields overrides={dialog.draft.model_settings_override} parent={parentSettings} apiType={effectiveApiType} protocolChanged={protocolChanged} disabled={busy} onChange={(modelSettingsOverride) => update({ model_settings_override: modelSettingsOverride })} />
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
  const [modelOptions, setModelOptions] = useState<ModelConnectionOptions>({ items: [], system_default: null });
  const [modelsLoading, setModelsLoading] = useState(true);
  const [modelsError, setModelsError] = useState(false);
  const [modelSelection, setModelSelection] = useState<ModelSelection | null>(null);
  const [modelSettings, setModelSettings] = useState<AgentModelSettings>(automaticAgentModelSettings);
  const [subagents, setSubagents] = useState<SubagentDefinition[]>([]);
  const [toolAllowlist, setToolAllowlist] = useState<string[]>([...builtInTools]);
  const [secretDeclarations, setSecretDeclarations] = useState<AgentSecretDeclaration[]>([]);
  const [subagentDialog, setSubagentDialog] = useState<SubagentDialogState | null>(null);
  const [pending, setPending] = useState(false);
  const [error, setError] = useState(false);
  const [secretError, setSecretError] = useState('');
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
      setModelSelection(loaded.system_default);
      setModelSettings((current) => settingsForSelection(current, loaded.system_default, loaded.items));
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
    const declarationError = validateSecretDeclarations(secretDeclarations);
    if (declarationError) {
      setSecretError(t(declarationError));
      return;
    }
    setSecretError('');
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
        model_selection: modelSelection,
        model_settings: modelSettings,
        subagents: subagents,
        tool_allowlist: toolAllowlist,
        secret_declarations: secretDeclarations
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
    const definition = index === null ? undefined : subagents[index];
    if (index !== null && !definition) return;
    setSubagentDialog({ index, draft: editableSubagent(definition), error: '' });
  }

  function commitSubagent(index: number | null, definition: SubagentDefinition) {
    setSubagents((current) => index === null
      ? [...current, definition]
      : current.map((candidate, candidateIndex) => candidateIndex === index ? definition : candidate));
    setSubagentDialog(null);
  }

  function deleteSubagent(index: number) {
    const definition = subagents[index];
    if (!definition || !window.confirm(t('confirmDeleteSubagent').replace('{name}', definition.name))) return;
    setSubagents((current) => current.filter((_, candidateIndex) => candidateIndex !== index));
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
            : <><label>{t('agentModelSelection')}<select value={selectionValue(modelSelection)} onChange={(event) => {
              const selection = selectionFromValue(event.target.value, modelOptions.items);
              setModelSelection(selection);
              setModelSettings((current) => settingsForSelection(current, selection, modelOptions.items));
            }}><option value="">{t('modelNotConfigured')}</option>{modelOptions.items.map((option) => {
              const value = selectionValue({ connection_id: option.connection_id, model_id: option.model_id });
              return <option key={value} value={value} disabled={option.status === 'disabled' && value !== selectionValue(modelSelection)}>{modelOptionLabel(option, t)}</option>;
            })}</select></label><AgentModelSettingsFields settings={modelSettings} apiType={selectedOption(modelSelection, modelOptions.items)?.api_type ?? 'openai_responses'} disabled={pending} onChange={setModelSettings} /></>}
        <SecretDeclarationsEditor declarations={secretDeclarations} disabled={pending} onChange={setSecretDeclarations} />
        {secretError && <div className="error" role="alert">{secretError}</div>}
        <section className="agent-subagent-section">
          <div className="agent-subagent-heading"><span className="field-label">{t('subagents')}</span><button type="button" className="secondary" disabled={pending || modelsLoading || modelsError || subagents.length >= 32} onClick={() => openSubagent(null)}><Plus size={16} /> {t('addSubagent')}</button></div>
          <SubagentTable definitions={subagents} modelOptions={modelOptions.items} canManage disabled={pending} onEdit={openSubagent} onDelete={deleteSubagent} />
        </section>
        <ToolAllowlistPicker value={toolAllowlist} onChange={setToolAllowlist} disabled={pending} legend={t('toolAllowlist')} />
        <label>{t('visibility')}<select value={visibility} onChange={(event) => { setVisibility(event.target.value); if (event.target.value !== 'public_to') setPublicTo([]); }}><option value="private">{t('private')}</option><option value="public_to">{t('specificUsers')}</option>{canCreatePublic && <option value="public">{t('public')}</option>}</select></label>
        {visibility === 'public_to' && <fieldset className="agent-user-picker" disabled={pending || usersLoading}><legend>{t('agentPublicTo')}</legend>
          {usersLoading ? <span>{t('loadingUsers')}</span> : usersError ? <div role="alert"><span>{t('usersLoadFailed')}</span><button type="button" className="text-button" onClick={loadUsers}>{t('retry')}</button></div> : users.map((user) => <label className="check-row" key={user.id}><input type="checkbox" checked={publicTo.includes(user.id)} onChange={(event) => setPublicTo((current) => event.target.checked ? [...current, user.id] : current.filter((id) => id !== user.id))} /> {user.display_name} ({user.email})</label>)}</fieldset>}
        {error && <div className="error" role="alert">{t('agentCreateFailed')}</div>}
      </form>
  </FormDialog>
  {subagentDialog && <SubagentDialog dialog={subagentDialog} definitions={subagents} modelOptions={modelOptions.items} parentSelection={modelSelection} parentSettings={modelSettings} formId="create-agent-subagent-form" busy={pending} onChange={setSubagentDialog} onCommit={commitSubagent} onClose={() => setSubagentDialog(null)} />}
  </>;
}

type DetailTab = 'activity' | 'instructions' | 'models' | 'secrets' | 'skills' | 'mcp' | 'access';
type NavigationBlockerSetter = (blocker: (() => boolean) | null) => void;
type RunConsoleComponent = ComponentType<{ run: Run }>;

const detailTabs: Array<{ id: DetailTab; key: 'tabActivity' | 'tabInstructions' | 'tabModels' | 'tabSecrets' | 'tabSkills' | 'tabMcp' | 'tabAccess' }> = [
  { id: 'activity', key: 'tabActivity' },
  { id: 'instructions', key: 'tabInstructions' },
  { id: 'models', key: 'tabModels' },
  { id: 'secrets', key: 'tabSecrets' },
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
  modelSelection: ModelSelection | null;
  modelSettings: AgentModelSettings;
  subagents: SubagentDefinition[];
};

function agentModelDraft(agent: Agent): AgentModelDraft {
  return {
    modelSelection: agent.model_selection,
    modelSettings: agent.model_settings,
    subagents: agent.subagents
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
  const [modelOptions, setModelOptions] = useState<ModelConnectionOptions>({ items: [], system_default: null });
  const [selectedRun, setSelectedRun] = useState<Run | null>(null);
  const [activeTab, setActiveTab] = useState<DetailTab>('activity');
  const [loadError, setLoadError] = useState(false);
  const [loadRetry, setLoadRetry] = useState(0);
  const [error, setError] = useState('');
  const [configPending, setConfigPending] = useState(false);
  const [skillsDialogOpen, setSkillsDialogOpen] = useState(false);
  const [mcpDialog, setMcpDialog] = useState<McpDialogState | null>(null);
  const [subagentDialog, setSubagentDialog] = useState<SubagentDialogState | null>(null);

  const [instructionDraft, setInstructionDraft] = useState({ name: '', instructions: '' });
  const [instructionBase, setInstructionBase] = useState({ name: '', instructions: '' });
  const [managedSkillDraft, setManagedSkillDraft] = useState<string[]>([]);
  const [managedSkillBase, setManagedSkillBase] = useState<string[]>([]);
  const [modelDraft, setModelDraft] = useState<AgentModelDraft>({ modelSelection: null, modelSettings: automaticAgentModelSettings, subagents: [] });
  const [modelBase, setModelBase] = useState<AgentModelDraft>({ modelSelection: null, modelSettings: automaticAgentModelSettings, subagents: [] });
  const [secretDraft, setSecretDraft] = useState<AgentSecretDeclaration[]>([]);
  const [secretBase, setSecretBase] = useState<AgentSecretDeclaration[]>([]);
  const [accessDraft, setAccessDraft] = useState({ visibility: 'private', publicTo: [] as string[], runtimeId: null as string | null, toolAllowlist: [...builtInTools] as string[] });
  const [accessBase, setAccessBase] = useState({ visibility: 'private', publicTo: [] as string[], runtimeId: null as string | null, toolAllowlist: [...builtInTools] as string[] });

  const mounted = useRef(true);
  const loadGeneration = useRef(0);
  const mutationGeneration = useRef(0);
  const loadController = useRef<AbortController | null>(null);
  const mutationController = useRef<AbortController | null>(null);
  const refreshController = useRef<AbortController | null>(null);
  const refreshPromise = useRef<Promise<void> | null>(null);
  const configPendingRef = useRef(false);
  const firstSkillRef = useRef<HTMLInputElement>(null);
  const mcpNameRef = useRef<HTMLInputElement>(null);

  const instructionDirty = instructionDraft.name !== instructionBase.name || instructionDraft.instructions !== instructionBase.instructions;
  const skillsDirty = skillsDialogOpen && !sameIds(managedSkillDraft, managedSkillBase);
  const mcpDirty = Boolean(mcpDialog && !sameMcpDraft(mcpDialog.draft, mcpDialog.base));
  const modelDirty = !sameAgentModelDraft(modelDraft, modelBase);
  const secretDirty = !sameSecretDeclarations(secretDraft, secretBase);
  const subagentDialogDirty = Boolean(subagentDialog && !sameAgentModelDraft(
    { ...modelDraft, subagents: [subagentDialog.draft] },
    { ...modelDraft, subagents: [editableSubagent(subagentDialog.index === null ? undefined : modelDraft.subagents[subagentDialog.index])] }
  ));
  const accessDirty = accessDraft.visibility !== accessBase.visibility
    || !sameIds(accessDraft.publicTo, accessBase.publicTo)
    || accessDraft.runtimeId !== accessBase.runtimeId
    || !sameIds(accessDraft.toolAllowlist, accessBase.toolAllowlist);
  const dirty = instructionDirty || skillsDirty || mcpDirty || modelDirty || secretDirty || subagentDialogDirty || accessDirty;

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
    setSecretDraft(loaded.secret_declarations);
    setSecretBase(loaded.secret_declarations);
    const access = {
      visibility: loaded.visibility,
      publicTo: loaded.public_to,
      runtimeId: loaded.runtime_id,
      toolAllowlist: normalizeToolAllowlist(loaded.tool_allowlist)
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
        : { items: [], system_default: null })
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
      try {
        const latest = await api.runs(agentId, controller.signal);
        if (!active || controller.signal.aborted) return;
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
      window.clearInterval(timer);
    };
  }, [agentId, loadedAgentId]);

  useEffect(() => () => {
    mounted.current = false;
    loadController.current?.abort();
    mutationController.current?.abort();
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

  async function saveAgentTab(event: FormEvent, tab: 'instructions' | 'models' | 'secrets' | 'skills' | 'access') {
    event.preventDefault();
    if (!agent || !agent.can_manage) return;
    if (tab === 'secrets') {
      const declarationError = validateSecretDeclarations(secretDraft);
      if (declarationError) {
        setError(t(declarationError));
        return;
      }
    }
    const operation = beginConfigMutation();
    if (!operation) return;
    try {
      let next: Agent = { ...agent };
      if (tab === 'instructions') next = { ...next, ...instructionDraft };
      if (tab === 'models') next = {
        ...next,
        model_selection: modelDraft.modelSelection,
        model_settings: modelDraft.modelSettings,
        subagents: modelDraft.subagents
      };
      if (tab === 'secrets') next = { ...next, secret_declarations: secretDraft };
      if (tab === 'skills') next = { ...next, managed_skill_ids: managedSkillDraft };
      if (tab === 'access') next = {
        ...next,
        visibility: accessDraft.visibility,
        public_to: accessDraft.visibility === 'public_to' ? accessDraft.publicTo : [],
        runtime_id: accessDraft.runtimeId,
        tool_allowlist: accessDraft.toolAllowlist
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
      if (tab === 'secrets') {
        setSecretDraft(updated.secret_declarations);
        setSecretBase(updated.secret_declarations);
      }
      if (tab === 'access') {
        const saved = { visibility: updated.visibility, publicTo: updated.public_to, runtimeId: updated.runtime_id, toolAllowlist: normalizeToolAllowlist(updated.tool_allowlist) };
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
    const definition = index === null ? undefined : modelDraft.subagents[index];
    if (index !== null && !definition) return;
    setSubagentDialog({ index, draft: editableSubagent(definition), error: '' });
  }

  function commitSubagent(index: number | null, definition: SubagentDefinition) {
    setModelDraft((current) => ({
      ...current,
      subagents: index === null
        ? [...current.subagents, definition]
        : current.subagents.map((candidate, candidateIndex) => candidateIndex === index ? definition : candidate)
    }));
    setSubagentDialog(null);
  }

  function deleteSubagentDefinition(index: number) {
    if (!agent?.can_manage || configPendingRef.current) return;
    const definition = modelDraft.subagents[index];
    if (!definition || !window.confirm(t('confirmDeleteSubagent').replace('{name}', definition.name))) return;
    setModelDraft((current) => ({ ...current, subagents: current.subagents.filter((_, candidateIndex) => candidateIndex !== index) }));
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
        <section><h2>{t('agentInspectorModel')}</h2><dl><div><dt>{t('agentModelSelection')}</dt><dd>{modelName(agent.model_selection, modelOptions.items, t('modelNotConfigured'))}</dd></div><div><dt>{t('reasoningEffort')}</dt><dd>{agent.model_settings.reasoning_effort}</dd></div><div><dt>{t('subagents')}</dt><dd>{agent.subagents.length}</dd></div></dl></section>
        <section><h2>{t('agentInspectorAccess')}</h2><dl><div><dt>{t('visibility')}</dt><dd>{visibilityLabel(agent.visibility, t)}</dd></div>{agent.visibility === 'public_to' && <div><dt>{t('agentPublicTo')}</dt><dd>{agent.public_to.length}</dd></div>}</dl></section>
        <section><h2>{t('managedSkills')}</h2><div className="agent-skill-chips">{managedSkills.map((skill) => <span key={skill.id}>{skill.name}</span>)}{managedSkills.length === 0 && <span>{t('none')}</span>}</div></section>
        <section><h2>{t('details')}</h2><dl><div><dt>{t('created')}</dt><dd>{new Date(agent.created_at).toLocaleString(locale)}</dd></div><div><dt>{t('updated')}</dt><dd>{new Date(agent.updated_at).toLocaleString(locale)}</dd></div></dl></section>
      </aside>
      <section className="agent-overview">
        <div className="agent-tabs" role="tablist" aria-label={t('agentDetailSections')}>{detailTabs.map((tab) => <button key={tab.id} id={`agent-tab-${tab.id}`} type="button" role="tab" aria-selected={activeTab === tab.id} aria-controls={`agent-panel-${tab.id}`} disabled={configPending && activeTab !== tab.id} onClick={() => setActiveTab(tab.id)}>{t(tab.key)}</button>)}</div>
        <div className="agent-panels">
          <section id="agent-panel-activity" role="tabpanel" aria-labelledby="agent-tab-activity" aria-label={t('tabActivity')} hidden={activeTab !== 'activity'}>
            <div className="agent-activity-grid"><section><h2>{t('runHistory')}</h2><div className="list run-list">{runs.map((run) => <button className={`list-row ${selectedRunForAgent?.id === run.id ? 'selected' : ''}`} data-run-id={run.id} key={run.id} onClick={() => setSelectedRun(run)}><strong>{runStatusLabel(run.status, t)}</strong><span>{runSourceLabel(run.source, t)} · {run.initial_message}</span></button>)}{runs.length === 0 && <div className="compact-empty">{t('noRuns')}</div>}</div></section><section className="agent-console">{selectedRunForAgent ? <RunConsole run={selectedRunForAgent} /> : <div className="compact-empty">{t('noRuns')}</div>}</section></div>
          </section>
          <section id="agent-panel-instructions" role="tabpanel" aria-labelledby="agent-tab-instructions" aria-label={t('tabInstructions')} hidden={activeTab !== 'instructions'}>{agent.can_manage ? <form className="stack" onSubmit={(event) => saveAgentTab(event, 'instructions')}><label>{t('name')}<input disabled={configPending} value={instructionDraft.name} onChange={(event) => setInstructionDraft((current) => ({ ...current, name: event.target.value }))} /></label><MarkdownEditor label={t('instructions')} disabled={configPending} value={instructionDraft.instructions} onChange={(instructions) => setInstructionDraft((current) => ({ ...current, instructions }))} /><button className="primary" disabled={configPending || !instructionDirty}><Save size={16} /> {configPending ? t('saving') : t('saveAgent')}</button></form> : <div className="agent-readonly"><h2>{agent.name}</h2><p>{agent.instructions}</p></div>}</section>
          <section id="agent-panel-models" role="tabpanel" aria-labelledby="agent-tab-models" aria-label={t('tabModels')} hidden={activeTab !== 'models'}>
            {agent.can_manage ? <form className="stack" onSubmit={(event) => saveAgentTab(event, 'models')}>
              <label>{t('agentModelSelection')}<select disabled={configPending} value={selectionValue(modelDraft.modelSelection)} onChange={(event) => setModelDraft((current) => {
                const modelSelection = selectionFromValue(event.target.value, modelOptions.items);
                return { ...current, modelSelection, modelSettings: settingsForSelection(current.modelSettings, modelSelection, modelOptions.items) };
              })}><option value="">{t('modelNotConfigured')}</option>{modelOptions.items.map((option) => {
                const value = selectionValue({ connection_id: option.connection_id, model_id: option.model_id });
                return <option key={value} value={value} disabled={option.status === 'disabled' && value !== selectionValue(modelDraft.modelSelection)}>{modelOptionLabel(option, t)}</option>;
              })}</select></label>
              <AgentModelSettingsFields settings={modelDraft.modelSettings} apiType={selectedOption(modelDraft.modelSelection, modelOptions.items)?.api_type ?? 'openai_responses'} disabled={configPending} onChange={(modelSettings) => setModelDraft((current) => ({ ...current, modelSettings }))} />
              <section className="agent-subagent-section"><div className="agent-subagent-heading"><span className="field-label">{t('subagents')}</span><button type="button" className="secondary" disabled={configPending || modelDraft.subagents.length >= 32} onClick={() => openSubagentDialog(null)}><Plus size={16} /> {t('addSubagent')}</button></div><SubagentTable definitions={modelDraft.subagents} modelOptions={modelOptions.items} canManage disabled={configPending} onEdit={openSubagentDialog} onDelete={deleteSubagentDefinition} /></section>
              <button className="primary" disabled={configPending || !modelDirty}><Save size={16} /> {configPending ? t('saving') : t('saveAgent')}</button>
            </form> : <div className="stack agent-readonly"><dl className="agent-model-summary"><div><dt>{t('agentModelSelection')}</dt><dd>{modelName(agent.model_selection, modelOptions.items, t('modelNotConfigured'))}</dd></div><div><dt>{t('reasoningEffort')}</dt><dd>{agent.model_settings.reasoning_effort}</dd></div></dl><SubagentTable definitions={agent.subagents} modelOptions={modelOptions.items} canManage={false} disabled onEdit={() => undefined} onDelete={() => undefined} /></div>}
          </section>
          <section id="agent-panel-secrets" role="tabpanel" aria-labelledby="agent-tab-secrets" aria-label={t('tabSecrets')} hidden={activeTab !== 'secrets'}>
            {agent.can_manage ? <form className="stack" onSubmit={(event) => saveAgentTab(event, 'secrets')}>
              <SecretDeclarationsEditor declarations={secretDraft} disabled={configPending} onChange={setSecretDraft} />
              <button className="primary" disabled={configPending || !secretDirty}><Save size={16} /> {configPending ? t('saving') : t('saveAgent')}</button>
            </form> : <SecretDeclarationsEditor declarations={agent.secret_declarations} disabled onChange={() => undefined} />}
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
          <section id="agent-panel-access" role="tabpanel" aria-labelledby="agent-tab-access" aria-label={t('tabAccess')} hidden={activeTab !== 'access'}>{agent.can_manage ? <form className="stack" onSubmit={(event) => saveAgentTab(event, 'access')}><label>{t('visibility')}<select disabled={configPending} value={accessDraft.visibility} onChange={(event) => setAccessDraft((current) => ({ ...current, visibility: event.target.value, publicTo: event.target.value === 'public_to' ? current.publicTo : [] }))}><option value="private">{t('private')}</option><option value="public_to">{t('specificUsers')}</option>{(canSetPublic || accessDraft.visibility === 'public') && <option value="public">{t('public')}</option>}</select></label>{accessDraft.visibility === 'public_to' && <fieldset className="agent-user-picker" disabled={configPending}><legend>{t('agentPublicTo')}</legend>{users.filter((user) => user.id !== agent.owner_id).map((user) => <label className="check-row" key={user.id}><input type="checkbox" checked={accessDraft.publicTo.includes(user.id)} onChange={(event) => setAccessDraft((current) => ({ ...current, publicTo: event.target.checked ? [...current.publicTo, user.id] : current.publicTo.filter((id) => id !== user.id) }))} /> {user.display_name} ({user.email})</label>)}</fieldset>}<label>{t('runtime')}<select disabled={configPending} value={accessDraft.runtimeId ?? ''} onChange={(event) => setAccessDraft((current) => ({ ...current, runtimeId: event.target.value || null }))}><option value="">{t('automatic')}</option>{runtimes.map((item) => <option key={item.id} value={item.id}>{item.hostname} · {runtimeStatusLabel(item.status, t)}</option>)}</select></label><ToolAllowlistPicker value={accessDraft.toolAllowlist} onChange={(toolAllowlist) => setAccessDraft((current) => ({ ...current, toolAllowlist }))} disabled={configPending} legend={t('toolAllowlist')} /><button className="primary" disabled={configPending || !accessDirty}><Save size={16} /> {configPending ? t('saving') : t('saveAgent')}</button></form> : <div className="agent-readonly">{visibilityLabel(agent.visibility, t)}</div>}</section>
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
  {subagentDialog && <SubagentDialog dialog={subagentDialog} definitions={modelDraft.subagents} modelOptions={modelOptions.items} parentSelection={modelDraft.modelSelection} parentSettings={modelDraft.modelSettings} formId="agent-subagent-form" busy={configPending} onChange={setSubagentDialog} onCommit={commitSubagent} onClose={() => setSubagentDialog(null)} />}
  </>;
}
