import { Check, Copy, ExternalLink, Link2, Pencil, Plus, RefreshCw, RotateCw, Trash2 } from 'lucide-react';
import { FormEvent, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  Agent,
  api,
  CreateIntegrationAppRequest,
  IntegrationApp,
  IntegrationAppOptions,
  IntegrationAppSecretResponse,
  UpdateIntegrationAppRequest,
  User
} from './api/client';
import { FormDialog } from './components/form-dialog';
import { normalizeToolAllowlist, publicWidgetTools, ToolAllowlistPicker } from './components/tool-allowlist';
import { useI18n } from './i18n';
import type { TranslationKey } from './i18n';

type AppDialog =
  | { kind: 'create' }
  | { kind: 'edit'; app: IntegrationApp }
  | { kind: 'rotate'; app: IntegrationApp }
  | { kind: 'widgets'; app: IntegrationApp }
  | { kind: 'secret'; result: IntegrationAppSecretResponse }
  | null;

function replaceName(template: string, name: string) {
  return template.replace('{name}', name);
}

function appAgentNames(app: IntegrationApp, agents: Agent[]) {
  const byId = new Map(agents.map((agent) => [agent.id, agent.name]));
  return app.agent_ids.map((id) => byId.get(id) ?? id);
}

function IntegrationAppForm({
  app,
  options,
  agents,
  canConfigureAnonymous,
  onClose,
  onSubmit
}: {
  app?: IntegrationApp;
  options: IntegrationAppOptions;
  agents: Agent[];
  canConfigureAnonymous: boolean;
  onClose: () => void;
  onSubmit: (request: CreateIntegrationAppRequest | UpdateIntegrationAppRequest) => Promise<void>;
}) {
  const { t } = useI18n();
  const isEditing = Boolean(app);
  const nameRef = useRef<HTMLInputElement>(null);
  const submittingRef = useRef(false);
  const mountedRef = useRef(true);
  const initialPlatformId = app?.external_platform_id ?? options.external_platforms[0]?.id ?? '';
  const initialChannelId = app?.authentication_channel_id
    ?? options.authentication_channels.find((channel) => channel.platform_id === initialPlatformId)?.id
    ?? '';
  const [name, setName] = useState(app?.name ?? '');
  const [platformId, setPlatformId] = useState(initialPlatformId);
  const [channelId, setChannelId] = useState(initialChannelId);
  const [redirectUris, setRedirectUris] = useState(app?.redirect_uris.length ? [...app.redirect_uris] : ['']);
  const [agentIds, setAgentIds] = useState<string[]>(app?.agent_ids ?? []);
  const [widgetHistoryEnabled, setWidgetHistoryEnabled] = useState(app?.widget_history_enabled ?? false);
  const [loginRequired, setLoginRequired] = useState(app?.login_required ?? true);
  const [allowedOrigins, setAllowedOrigins] = useState(app?.allowed_origins?.length ? [...app.allowed_origins] : ['']);
  const [restrictTools, setRestrictTools] = useState(app?.tool_allowlist !== null && app?.tool_allowlist !== undefined);
  const [toolAllowlist, setToolAllowlist] = useState(() => normalizeToolAllowlist(app?.tool_allowlist));
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<TranslationKey | null>(null);
  const availableAgents = useMemo(() => agents.filter((agent) => agent.can_invoke), [agents]);
  const channels = useMemo(
    () => options.authentication_channels.filter((channel) => channel.platform_id === platformId),
    [options.authentication_channels, platformId]
  );
  const platform = options.external_platforms.find((option) => option.id === app?.external_platform_id);
  const channel = options.authentication_channels.find((option) => option.id === app?.authentication_channel_id);

  useEffect(() => () => { mountedRef.current = false; }, []);
  useEffect(() => {
    if (isEditing || channels.some((option) => option.id === channelId)) return;
    setChannelId(channels[0]?.id ?? '');
  }, [channelId, channels, isEditing]);

  function updateRedirectUri(index: number, value: string) {
    setRedirectUris((current) => current.map((uri, currentIndex) => currentIndex === index ? value : uri));
  }

  function removeRedirectUri(index: number) {
    setRedirectUris((current) => current.filter((_, currentIndex) => currentIndex !== index));
  }

  function toggleAgent(agentId: string, selected: boolean) {
    if (!loginRequired) {
      setAgentIds(selected ? [agentId] : []);
      return;
    }
    setAgentIds((current) => selected
      ? [...new Set([...current, agentId])]
      : current.filter((id) => id !== agentId));
  }

  function updateAllowedOrigin(index: number, value: string) {
    setAllowedOrigins((current) => current.map((origin, originIndex) => originIndex === index ? value : origin));
  }

  function isExactOrigin(value: string) {
    try {
      const url = new URL(value);
      return (url.protocol === 'https:' || url.protocol === 'http:') && url.origin === value;
    } catch {
      return false;
    }
  }

  async function submit(event: FormEvent) {
    event.preventDefault();
    if (submittingRef.current) return;
    submittingRef.current = true;
    setPending(true);
    setError(null);
    const normalizedOrigins = allowedOrigins.map((origin) => origin.trim()).filter(Boolean);
    if (!loginRequired && (agentIds.length !== 1 || normalizedOrigins.length === 0 || normalizedOrigins.some((origin) => !isExactOrigin(origin)))) {
      setError(agentIds.length !== 1 ? 'anonymousWidgetAgentRequired' : 'allowedOriginsInvalid');
      submittingRef.current = false;
      setPending(false);
      return;
    }
    const common = {
      name: name.trim(),
      redirect_uris: redirectUris.map((uri) => uri.trim()).filter(Boolean),
      agent_ids: agentIds,
      widget_history_enabled: loginRequired && widgetHistoryEnabled,
      login_required: loginRequired,
      allowed_origins: loginRequired ? [] : normalizedOrigins,
      tool_allowlist: restrictTools ? toolAllowlist : null
    };
    try {
      await onSubmit(isEditing ? common : {
        ...common,
        external_platform_id: platformId,
        authentication_channel_id: channelId
      });
    } catch {
      if (mountedRef.current) setError('integrationAppSaveFailed');
    } finally {
      submittingRef.current = false;
      if (mountedRef.current) setPending(false);
    }
  }

  const title = isEditing ? t('editIntegrationApp') : t('createIntegrationApp');
  const formId = isEditing ? 'edit-integration-app-form' : 'create-integration-app-form';
  return (
    <FormDialog
      title={title}
      eyebrow={t('integrationApps')}
      onClose={onClose}
      busy={pending}
      initialFocusRef={nameRef}
      className="integration-app-dialog"
      footer={<><button className="secondary" type="button" disabled={pending} onClick={onClose}>{t('cancel')}</button><button className="primary" form={formId} type="submit" disabled={pending}>{pending ? t('saving') : isEditing ? t('saveChanges') : t('createIntegrationApp')}</button></>}
    >
      <form id={formId} className="stack" onSubmit={submit}>
        <label>{t('name')}<input ref={nameRef} required value={name} onChange={(event) => setName(event.target.value)} /></label>
        {isEditing ? <dl className="integration-origin-details">
          <div><dt>{t('externalPlatform')}</dt><dd>{platform?.name ?? app?.external_platform_id}</dd></div>
          <div><dt>{t('authenticationChannel')}</dt><dd>{channel?.name ?? app?.authentication_channel_id}</dd></div>
        </dl> : <div className="integration-origin-fields">
          <label>{t('externalPlatform')}<select required value={platformId} onChange={(event) => setPlatformId(event.target.value)}>{options.external_platforms.map((option) => <option key={option.id} value={option.id}>{option.name}</option>)}</select></label>
          <label>{t('authenticationChannel')}<select required value={channelId} onChange={(event) => setChannelId(event.target.value)}>{channels.map((option) => <option key={option.id} value={option.id}>{option.name}</option>)}</select></label>
        </div>}
        <fieldset className="integration-fieldset">
          <legend>{t('redirectUris')}</legend>
          <div className="integration-uri-list">
            {redirectUris.map((uri, index) => <div className="integration-uri-row" key={index}>
              <label>{t('redirectUriLabel').replace('{index}', String(index + 1))}<input type="url" required value={uri} onChange={(event) => updateRedirectUri(index, event.target.value)} /></label>
              <button className="icon-button" type="button" disabled={redirectUris.length === 1} aria-label={t('removeRedirectUri').replace('{index}', String(index + 1))} title={t('removeRedirectUri').replace('{index}', String(index + 1))} onClick={() => removeRedirectUri(index)}><Trash2 size={17} /></button>
            </div>)}
          </div>
          <button className="secondary integration-add-uri" type="button" onClick={() => setRedirectUris((current) => [...current, ''])}><Plus size={16} /> {t('addRedirectUri')}</button>
        </fieldset>
        {canConfigureAnonymous && <label className="check-row"><input type="checkbox" checked={!loginRequired} onChange={(event) => {
          const anonymous = event.target.checked;
          setLoginRequired(!anonymous);
          if (anonymous) {
            setAgentIds((current) => current.slice(0, 1));
            setWidgetHistoryEnabled(false);
            const narrowedTools = toolAllowlist.filter((tool) => publicWidgetTools.includes(tool as typeof publicWidgetTools[number]));
            setToolAllowlist(narrowedTools);
            if (narrowedTools.length === 0) setRestrictTools(false);
          }
        }} /><span>{t('anonymousPublicWidget')}</span></label>}
        <fieldset className="integration-fieldset integration-agent-selector">
          <legend>{t('delegatedAgents')}</legend>
          {availableAgents.length === 0 && <span>{t('noInvocableAgents')}</span>}
          {availableAgents.map((agent) => <label className="check-row" key={agent.id}><input type={loginRequired ? 'checkbox' : 'radio'} name={loginRequired ? undefined : 'anonymous-widget-agent'} aria-label={replaceName(t('delegateAgent'), agent.name)} checked={agentIds.includes(agent.id)} onChange={(event) => toggleAgent(agent.id, event.target.checked)} /><span>{agent.name}</span></label>)}
        </fieldset>
        {!loginRequired && <>
          <fieldset className="integration-fieldset">
            <legend>{t('allowedOrigins')}</legend>
            <div className="integration-uri-list">
              {allowedOrigins.map((origin, index) => <div className="integration-uri-row" key={index}>
                <label>{t('allowedOriginLabel').replace('{index}', String(index + 1))}<input aria-label={t('allowedOriginLabel').replace('{index}', String(index + 1))} value={origin} placeholder="https://example.com" onChange={(event) => updateAllowedOrigin(index, event.target.value)} /></label>
                <button className="icon-button" type="button" disabled={allowedOrigins.length === 1} aria-label={t('removeAllowedOrigin').replace('{index}', String(index + 1))} title={t('removeAllowedOrigin').replace('{index}', String(index + 1))} onClick={() => setAllowedOrigins((current) => current.filter((_, originIndex) => originIndex !== index))}><Trash2 size={17} /></button>
              </div>)}
            </div>
            <button className="secondary integration-add-uri" type="button" onClick={() => setAllowedOrigins((current) => [...current, ''])}><Plus size={16} /> {t('addAllowedOrigin')}</button>
          </fieldset>
        </>}
        <label className="check-row"><input type="checkbox" checked={restrictTools} onChange={(event) => setRestrictTools(event.target.checked)} /><span>{t('restrictAppTools')}</span></label>
        {restrictTools && <ToolAllowlistPicker value={toolAllowlist} onChange={setToolAllowlist} disabled={pending} legend={t('toolAllowlist')} tools={loginRequired ? undefined : publicWidgetTools} />}
        {loginRequired && <label className="check-row"><input type="checkbox" checked={widgetHistoryEnabled} onChange={(event) => setWidgetHistoryEnabled(event.target.checked)} /><span>{t('widgetHistoryEnabled')}</span></label>}
        {error && <div className="error" role="alert">{t(error)}</div>}
      </form>
    </FormDialog>
  );
}

function SecretDialog({ result, onClose }: { result: IntegrationAppSecretResponse; onClose: () => void }) {
  const { t } = useI18n();
  const [copied, setCopied] = useState<'clientId' | 'secret' | null>(null);

  async function copy(value: string, field: 'clientId' | 'secret') {
    try {
      await navigator.clipboard.writeText(value);
      setCopied(field);
    } catch {
      setCopied(null);
    }
  }

  return <FormDialog title={t('integrationAppSecret')} eyebrow={result.integration_app.name} onClose={onClose} className="integration-secret-dialog" footer={<button className="primary" type="button" onClick={onClose}>{t('close')}</button>}>
    <p className="integration-secret-warning">{t('oneTimeClientSecret')}</p>
    <dl className="integration-credential-list">
      <div><dt>{t('clientId')}</dt><dd><code>{result.integration_app.client_id}</code><button className="icon-button" type="button" aria-label={t('copyClientId')} title={t('copyClientId')} onClick={() => copy(result.integration_app.client_id, 'clientId')}>{copied === 'clientId' ? <Check size={17} /> : <Copy size={17} />}</button></dd></div>
      <div><dt>{t('clientSecret')}</dt><dd><code>{result.client_secret}</code><button className="icon-button" type="button" aria-label={t('copyClientSecret')} title={t('copyClientSecret')} onClick={() => copy(result.client_secret, 'secret')}>{copied === 'secret' ? <Check size={17} /> : <Copy size={17} />}</button></dd></div>
    </dl>
  </FormDialog>;
}

function RotateSecretDialog({ app, onClose, onRotated }: { app: IntegrationApp; onClose: () => void; onRotated: (result: IntegrationAppSecretResponse) => void }) {
  const { t } = useI18n();
  const pendingRef = useRef(false);
  const mountedRef = useRef(true);
  const [pending, setPending] = useState(false);
  const [error, setError] = useState(false);
  useEffect(() => () => { mountedRef.current = false; }, []);

  async function rotate() {
    if (pendingRef.current) return;
    pendingRef.current = true;
    setPending(true);
    setError(false);
    try {
      onRotated(await api.rotateIntegrationAppSecret(app.id));
    } catch {
      if (mountedRef.current) setError(true);
    } finally {
      pendingRef.current = false;
      if (mountedRef.current) setPending(false);
    }
  }

  return <FormDialog title={t('rotateClientSecret')} eyebrow={app.name} onClose={onClose} busy={pending} footer={<><button className="secondary" type="button" disabled={pending} onClick={onClose}>{t('cancel')}</button><button className="primary" type="button" disabled={pending} onClick={rotate}><RotateCw size={16} /> {pending ? t('saving') : t('rotateSecret')}</button></>}>
    <p className="integration-rotate-warning">{t('rotateSecretWarning')}</p>
    {error && <div className="error" role="alert">{t('integrationSecretRotateFailed')}</div>}
  </FormDialog>;
}

function WidgetLinksDialog({ app, agents, onClose }: { app: IntegrationApp; agents: Agent[]; onClose: () => void }) {
  const { t } = useI18n();
  const [links, setLinks] = useState<Record<string, string>>({});
  const [pendingId, setPendingId] = useState<string | null>(null);
  const [errorId, setErrorId] = useState<string | null>(null);
  const [copiedId, setCopiedId] = useState<string | null>(null);
  const mountedRef = useRef(true);
  const agentById = useMemo(() => new Map(agents.map((agent) => [agent.id, agent])), [agents]);
  const publicLink = `${window.location.origin}/widget?app=${encodeURIComponent(app.client_id)}`;
  const loginRequired = app.login_required !== false;
  useEffect(() => () => { mountedRef.current = false; }, []);

  async function generate(agentId: string) {
    if (pendingId) return;
    setPendingId(agentId);
    setErrorId(null);
    try {
      const response = await api.createIntegrationAppWidgetSession(app.id, agentId);
      if (!mountedRef.current) return;
      setLinks((current) => ({ ...current, [agentId]: `${window.location.origin}/widget#token=${encodeURIComponent(response.token)}` }));
    } catch {
      if (mountedRef.current) setErrorId(agentId);
    } finally {
      if (mountedRef.current) setPendingId(null);
    }
  }

  async function copyLink(agentId: string) {
    try {
      await navigator.clipboard.writeText(agentId === 'public' ? publicLink : links[agentId]);
      setCopiedId(agentId);
    } catch {
      setCopiedId(null);
    }
  }

  return <FormDialog title={t('widgetLinks')} eyebrow={app.name} onClose={onClose} className="integration-widgets-dialog" footer={<button className="primary" type="button" onClick={onClose}>{t('close')}</button>}>
    {!loginRequired && <div className="integration-widget-list"><div className="integration-widget-row"><strong>{t('publicWidgetLink')}</strong><div className="integration-widget-link"><code>{publicLink}</code><button className="icon-button" type="button" aria-label={t('copyPublicWidgetLink')} title={t('copyPublicWidgetLink')} onClick={() => copyLink('public')}>{copiedId === 'public' ? <Check size={17} /> : <Copy size={17} />}</button><a className="icon-button" aria-label={t('openPublicWidget')} title={t('openPublicWidget')} href={publicLink} target="_blank" rel="noreferrer"><ExternalLink size={17} /></a></div></div></div>}
    {loginRequired && <>
    {app.agent_ids.length === 0 && <div className="state-panel">{t('noDelegatedAgents')}</div>}
    <div className="integration-widget-list">
      {app.agent_ids.map((agentId) => {
        const name = agentById.get(agentId)?.name ?? agentId;
        const link = links[agentId];
        return <div className="integration-widget-row" key={agentId}>
          <strong>{name}</strong>
          {!link && <button className="secondary" type="button" disabled={Boolean(pendingId)} aria-label={replaceName(t('generateWidgetLink'), name)} onClick={() => generate(agentId)}><Link2 size={16} /> {pendingId === agentId ? t('saving') : t('generateLink')}</button>}
          {link && <div className="integration-widget-link"><code>{link}</code><button className="icon-button" type="button" aria-label={replaceName(t('copyWidgetLink'), name)} title={replaceName(t('copyWidgetLink'), name)} onClick={() => copyLink(agentId)}>{copiedId === agentId ? <Check size={17} /> : <Copy size={17} />}</button><a className="icon-button" aria-label={replaceName(t('openWidget'), name)} title={replaceName(t('openWidget'), name)} href={link} target="_blank" rel="noreferrer"><ExternalLink size={17} /></a></div>}
          {errorId === agentId && <div className="error" role="alert">{t('widgetLinkFailed')}</div>}
        </div>;
      })}
    </div>
    </>}
  </FormDialog>;
}

export function IntegrationAppsPage({ currentUser }: { currentUser: User }) {
  const { locale, t } = useI18n();
  const mountedRef = useRef(true);
  const loadGenerationRef = useRef(0);
  const createButtonRef = useRef<HTMLButtonElement>(null);
  const [apps, setApps] = useState<IntegrationApp[]>([]);
  const [options, setOptions] = useState<IntegrationAppOptions>({ external_platforms: [], authentication_channels: [] });
  const [agents, setAgents] = useState<Agent[]>([]);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState(false);
  const [dialog, setDialog] = useState<AppDialog>(null);

  const load = useCallback(() => {
    const generation = ++loadGenerationRef.current;
    const controller = new AbortController();
    setLoading(true);
    setLoadError(false);
    Promise.all([
      api.integrationApps(controller.signal),
      api.integrationAppOptions(controller.signal),
      api.agents(controller.signal)
    ]).then(([loadedApps, loadedOptions, loadedAgents]) => {
      if (!mountedRef.current || generation !== loadGenerationRef.current) return;
      setApps(loadedApps);
      setOptions(loadedOptions);
      setAgents(loadedAgents);
    }).catch((error) => {
      if (mountedRef.current && generation === loadGenerationRef.current && error?.name !== 'AbortError') setLoadError(true);
    }).finally(() => {
      if (mountedRef.current && generation === loadGenerationRef.current) setLoading(false);
    });
    return () => controller.abort();
  }, []);

  useEffect(() => {
    mountedRef.current = true;
    const cancel = load();
    return () => {
      mountedRef.current = false;
      loadGenerationRef.current += 1;
      cancel();
    };
  }, [load]);

  const platformById = useMemo(() => new Map(options.external_platforms.map((platform) => [platform.id, platform])), [options.external_platforms]);
  const channelById = useMemo(() => new Map(options.authentication_channels.map((channel) => [channel.id, channel])), [options.authentication_channels]);

  function closeDialog() {
    setDialog(null);
  }

  async function createApp(request: CreateIntegrationAppRequest | UpdateIntegrationAppRequest) {
    const result = await api.createIntegrationApp(request as CreateIntegrationAppRequest);
    if (!mountedRef.current) return;
    setApps((current) => [...current, result.integration_app]);
    setDialog({ kind: 'secret', result });
  }

  async function updateApp(app: IntegrationApp, request: CreateIntegrationAppRequest | UpdateIntegrationAppRequest) {
    const updated = await api.updateIntegrationApp(app.id, request as UpdateIntegrationAppRequest);
    if (!mountedRef.current) return;
    setApps((current) => current.map((item) => item.id === updated.id ? updated : item));
    setDialog(null);
  }

  return <div className="workspace-page integrations-page">
    <header className="page-header">
      <div><h1>{t('integrationApps')}</h1><p>{t('integrationAppsSubtitle')}</p></div>
      <button ref={createButtonRef} className="primary" type="button" disabled={loading || loadError || options.external_platforms.length === 0} onClick={() => setDialog({ kind: 'create' })}><Plus size={16} /> {t('createIntegrationApp')}</button>
    </header>
    {loading && <div className="panel state-panel">{t('loading')}</div>}
    {!loading && loadError && <div className="panel state-panel" role="alert"><p>{t('integrationAppsLoadFailed')}</p><button className="secondary" type="button" onClick={load}>{t('retry')}</button></div>}
    {!loading && !loadError && apps.length === 0 && <div className="panel state-panel"><p>{t('noIntegrationApps')}</p><button className="primary" type="button" disabled={options.external_platforms.length === 0} onClick={() => setDialog({ kind: 'create' })}><Plus size={16} /> {t('createIntegrationApp')}</button></div>}
    {!loading && !loadError && apps.length > 0 && <div className="integration-table-wrap">
      <table className="integration-table" aria-label={t('integrationAppList')}>
        <thead><tr><th>{t('name')}</th><th>{t('clientId')}</th><th>{t('platformChannel')}</th><th>{t('delegatedAgents')}</th><th>{t('updated')}</th><th>{t('integrationAppActions')}</th></tr></thead>
        <tbody>{apps.map((app) => {
          const names = appAgentNames(app, agents);
          const editLabel = replaceName(t('editIntegrationAppAria'), app.name);
          const rotateLabel = replaceName(t('rotateIntegrationAppAria'), app.name);
          const widgetLabel = replaceName(t('widgetLinksAria'), app.name);
          return <tr key={app.id}>
            <td><strong>{app.name}</strong></td>
            <td><code>{app.client_id}</code></td>
            <td><span className="integration-origin-cell"><strong>{platformById.get(app.external_platform_id)?.name ?? app.external_platform_id}</strong><small>{channelById.get(app.authentication_channel_id)?.name ?? app.authentication_channel_id}</small></span></td>
            <td>{names.length === 0 ? t('none') : names.length === 1 ? names[0] : t('agentCount').replace('{count}', String(names.length))}</td>
            <td><time dateTime={app.updated_at}>{new Date(app.updated_at).toLocaleString(locale)}</time></td>
            <td><div className="integration-table-actions"><button className="icon-button" type="button" aria-label={editLabel} title={editLabel} onClick={() => setDialog({ kind: 'edit', app })}><Pencil size={17} /></button><button className="icon-button" type="button" aria-label={rotateLabel} title={rotateLabel} onClick={() => setDialog({ kind: 'rotate', app })}><RefreshCw size={17} /></button><button className="icon-button" type="button" aria-label={widgetLabel} title={widgetLabel} onClick={() => setDialog({ kind: 'widgets', app })}><Link2 size={17} /></button></div></td>
          </tr>;
        })}</tbody>
      </table>
    </div>}
    {dialog?.kind === 'create' && <IntegrationAppForm options={options} agents={agents} canConfigureAnonymous={currentUser.role === 'admin' || currentUser.role === 'super_admin'} onClose={closeDialog} onSubmit={createApp} />}
    {dialog?.kind === 'edit' && <IntegrationAppForm app={dialog.app} options={options} agents={agents} canConfigureAnonymous={currentUser.role === 'admin' || currentUser.role === 'super_admin'} onClose={closeDialog} onSubmit={(request) => updateApp(dialog.app, request)} />}
    {dialog?.kind === 'rotate' && <RotateSecretDialog app={dialog.app} onClose={closeDialog} onRotated={(result) => {
      setApps((current) => current.map((app) => app.id === result.integration_app.id ? result.integration_app : app));
      setDialog({ kind: 'secret', result });
    }} />}
    {dialog?.kind === 'widgets' && <WidgetLinksDialog app={dialog.app} agents={agents} onClose={closeDialog} />}
    {dialog?.kind === 'secret' && <SecretDialog result={dialog.result} onClose={closeDialog} />}
  </div>;
}
