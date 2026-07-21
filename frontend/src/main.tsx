import { Activity, BookOpen, Bot, BrainCircuit, Check, CirclePause, Clock, Copy, ExternalLink, KeyRound, Languages, LogOut, Monitor, Plug, Plus, RotateCcw, Save, Search, Send, Settings, ShieldAlert, Sparkles, Trash2, Workflow, X } from 'lucide-react';
import React, { FormEvent, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { Agent, api, ApiError, ApiKey, ApiKeyValidity, Automation, HubSession, Run, RunEvent, Runtime, RuntimeEnrollmentToken, User, WidgetAgent } from './api/client';
import { I18nProvider, useI18n } from './i18n';
import type { TranslationKey } from './i18n';
import { FormDialog } from './components/form-dialog';
import { MarkdownEditor } from './components/markdown-editor';
import { SkillDetailPage, SkillsPage } from './skills';
import { SessionsPage } from './sessions';
import { AdministrationPage } from './administration';
import { AgentPage as AgentDetailPage, AgentsPage } from './agents';
import { IntegrationAppsPage } from './integrations';
import { AutomationsPage as AutomationsWorkspacePage } from './automations';
import { RuntimesPage as RuntimesWorkspacePage } from './runtimes';
import { ModelsPage } from './models';
import { clearConversationDrafts } from './session-drafts';
import './styles.css';

type Route = { name: 'login' } | { name: 'agents' } | { name: 'agent'; agentId: string } | { name: 'sessions' } | { name: 'integrations' } | { name: 'skills' } | { name: 'skill'; skillId: string } | { name: 'models' } | { name: 'apiKeys' } | { name: 'docs' } | { name: 'automations' } | { name: 'runtimes' } | { name: 'administration' } | { name: 'widget'; token?: string } | { name: 'notFound' };

function parseRoute(): Route {
  const path = window.location.pathname;
  if (path.startsWith('/widget')) {
    const token = new URLSearchParams(window.location.hash.slice(1)).get('token') ?? undefined;
    if (token) window.history.replaceState(null, '', '/widget');
    return { name: 'widget', token };
  }
  if (path.startsWith('/agents/')) {
    return { name: 'agent', agentId: path.split('/')[2] };
  }
  if (path === '/agents' || path === '/agents/') return { name: 'agents' };
  if (path === '/skills' || path === '/skills/') return { name: 'skills' };
  const skillMatch = path.match(/^\/skills\/([^/]+)\/?$/);
  if (skillMatch) return { name: 'skill', skillId: decodeURIComponent(skillMatch[1]) };
  if (path.startsWith('/skills/')) return { name: 'notFound' };
  if (path.startsWith('/api-keys')) return { name: 'apiKeys' };
  if (path === '/models' || path === '/models/') return { name: 'models' };
  if (path.startsWith('/models/')) return { name: 'notFound' };
  if (path.startsWith('/sessions')) return { name: 'sessions' };
  if (path === '/integrations' || path === '/integrations/') return { name: 'integrations' };
  if (path.startsWith('/integrations/')) return { name: 'notFound' };
  if (path.startsWith('/docs')) return { name: 'docs' };
  if (path.startsWith('/automations')) return { name: 'automations' };
  if (path.startsWith('/runtimes')) return { name: 'runtimes' };
  if (path.startsWith('/administration')) return { name: 'administration' };
  if (path.startsWith('/login')) return { name: 'login' };
  return { name: 'sessions' };
}

const historyIndexKey = '__agentHubHistoryIndex';
let historyIndex = Number.isInteger(window.history.state?.[historyIndexKey]) ? window.history.state[historyIndexKey] : 0;
window.history.replaceState({ ...(window.history.state ?? {}), [historyIndexKey]: historyIndex }, '', window.location.href);
let navigationBlocker: (() => boolean) | null = null;
let restoringHistory = false;

function setNavigationBlocker(blocker: (() => boolean) | null) { navigationBlocker = blocker; }

function canNavigate(force = false) {
  return force || !navigationBlocker || navigationBlocker();
}

function navigate(path: string, force = false) {
  if (!canNavigate(force)) return;
  historyIndex += 1;
  window.history.pushState({ [historyIndexKey]: historyIndex }, '', path);
  window.dispatchEvent(new PopStateEvent('popstate', { state: window.history.state }));
}

function App() {
  const { t } = useI18n();
  const [route, setRoute] = useState<Route>(parseRoute());
  const [user, setUser] = useState<User | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const onRoute = (event: PopStateEvent) => {
      const nextIndex = Number.isInteger(event.state?.[historyIndexKey]) ? event.state[historyIndexKey] : historyIndex;
      if (restoringHistory) { restoringHistory = false; historyIndex = nextIndex; setRoute(parseRoute()); return; }
      if (nextIndex !== historyIndex && navigationBlocker && !navigationBlocker()) {
        restoringHistory = true;
        window.history.go(historyIndex - nextIndex);
        return;
      }
      historyIndex = nextIndex;
      setRoute(parseRoute());
    };
    window.addEventListener('popstate', onRoute);
    return () => window.removeEventListener('popstate', onRoute);
  }, []);

  useEffect(() => {
    if (route.name === 'widget') {
      setLoading(false);
      return;
    }
    api.me().then(setUser).catch(() => setUser(null)).finally(() => setLoading(false));
  }, [route.name]);

  if (route.name === 'widget') return <WidgetApp token={route.token} />;
  if (loading) return <Shell user={user}><div className="panel">{t('loading')}</div></Shell>;
  if (!user || route.name === 'login') return <LoginPage onLogin={setUser} />;

  return (
    <Shell user={user}>
      {route.name === 'agents' && <AgentsPage currentUser={user} navigate={navigate} />}
      {route.name === 'sessions' && <SessionsPage currentUserId={user.id} />}
      {route.name === 'integrations' && <IntegrationAppsPage />}
      {/* agentId 变化时重建详情页，避免旧 Agent 的表单、运行列表和 controls 在新路由加载期间残留。 */}
      {route.name === 'agent' && <AgentDetailPage key={route.agentId} agentId={route.agentId} currentUser={user} navigate={navigate} setNavigationBlocker={setNavigationBlocker} RunConsole={RunConsole} />}
      {route.name === 'skills' && <SkillsPage navigate={navigate} />}
      {route.name === 'skill' && <SkillDetailPage key={route.skillId} skillId={route.skillId} navigate={navigate} setNavigationBlocker={setNavigationBlocker} />}
      {route.name === 'apiKeys' && <ApiKeysPage />}
      {route.name === 'models' && <ModelsPage currentUser={user} />}
      {route.name === 'docs' && <ApiDocsPage />}
      {route.name === 'automations' && <AutomationsWorkspacePage RunConsole={RunConsole} />}
      {route.name === 'runtimes' && <RuntimesWorkspacePage user={user} />}
      {route.name === 'administration' && (user.role === 'admin' || user.role === 'super_admin' ? <AdministrationPage currentUser={user} /> : <div className="panel state-panel"><h1>{t('pageNotFound')}</h1></div>)}
      {route.name === 'notFound' && <div className="panel state-panel"><h1>{t('pageNotFound')}</h1></div>}
    </Shell>
  );
}

function Shell({ user, children }: { user: User | null; children: React.ReactNode }) {
  const { language, setLanguage, t } = useI18n();
  const currentRoute = parseRoute().name;
  const current = (name: Route['name']) => currentRoute === name || (name === 'skills' && currentRoute === 'skill') ? 'page' as const : undefined;
  async function logout() {
    if (!canNavigate()) return;
    if (user) clearConversationDrafts(user.id);
    await api.logout();
    setNavigationBlocker(null);
    navigate('/login', true);
  }
  return (
    <div className="app-shell">
      <aside className="sidebar" aria-label={t('primaryNavigation')}>
        <div className="brand"><Workflow size={22} /> Agent Hub</div>
        <nav className="nav-groups" aria-label={t('primaryNavigation')}>
          <div className="nav-group"><span>{t('workspace')}</span>
            <button className="nav-button" aria-current={current('sessions')} onClick={() => navigate('/sessions')}><Activity size={18} /> {t('sessions')}</button>
            <button className="nav-button" aria-current={current('agents') ?? current('agent')} onClick={() => navigate('/agents')}><Bot size={18} /> {t('agents')}</button>
            <button className="nav-button" aria-current={current('integrations')} onClick={() => navigate('/integrations')}><Plug size={18} /> {t('integrationApps')}</button>
            <button className="nav-button" aria-current={current('automations')} onClick={() => navigate('/automations')}><Clock size={18} /> {t('automations')}</button>
          </div>
          <div className="nav-group"><span>{t('resources')}</span>
            <button className="nav-button" aria-current={current('skills')} onClick={() => navigate('/skills')}><Sparkles size={18} /> {t('skills')}</button>
            <button className="nav-button" aria-current={current('models')} onClick={() => navigate('/models')}><BrainCircuit size={18} /> {t('models')}</button>
            <button className="nav-button" aria-current={current('apiKeys')} onClick={() => navigate('/api-keys')}><KeyRound size={18} /> {t('apiKeys')}</button>
          </div>
          <div className="nav-group"><span>{t('system')}</span>
            <button className="nav-button" aria-current={current('runtimes')} onClick={() => navigate('/runtimes')}><Monitor size={18} /> {t('runtimes')}</button>
            {(user?.role === 'admin' || user?.role === 'super_admin') && <button className="nav-button" aria-current={current('administration')} onClick={() => navigate('/administration')}><Settings size={18} /> {t('administration')}</button>}
            <button className="nav-button" aria-current={current('docs')} onClick={() => navigate('/docs')}><BookOpen size={18} /> {t('apiDocs')}</button>
          </div>
        </nav>
        <div className="sidebar-footer">
          <label className="language-control"><Languages size={16} /><span className="sr-only">{t('language')}</span><select aria-label={t('language')} value={language} onChange={(event) => setLanguage(event.target.value as 'en' | 'zh-CN')}><option value="en">{t('english')}</option><option value="zh-CN">{t('chinese')}</option></select></label>
          <div className="account-row">{user && <span>{user.email ?? user.username}</span>}{user && <button className="icon-button" title={t('logout')} aria-label={t('logout')} onClick={logout}><LogOut size={17} /></button>}</div>
        </div>
      </aside>
      <main className="main">{children}</main>
    </div>
  );
}

function LoginPage({ onLogin }: { onLogin: (user: User) => void }) {
  const { language, setLanguage, t } = useI18n();
  const [email, setEmail] = useState('admin@example.com');
  const [password, setPassword] = useState('admin123');
  const [oidcMockEnabled, setOidcMockEnabled] = useState(false);
  const [error, setError] = useState<TranslationKey | null>(null);

  useEffect(() => {
    api.authProviders().then((providers) => setOidcMockEnabled(providers.oidc_mock)).catch(() => setOidcMockEnabled(false));
  }, []);

  async function submit(event: FormEvent) {
    event.preventDefault();
    setError(null);
    try {
      const response = await api.login(email, password);
      onLogin(response.user);
      navigate('/sessions');
    } catch (err) {
      setError('loginFailed');
    }
  }

  return (
    <div className="login-screen">
      <form className="login-panel" onSubmit={submit}>
        <div className="login-heading"><h1>Agent Hub</h1><select aria-label={t('language')} value={language} onChange={(event) => setLanguage(event.target.value as 'en' | 'zh-CN')}><option value="en">English</option><option value="zh-CN">简体中文</option></select></div>
        <label>{t('email')}<input aria-label={t('email')} value={email} onChange={(event) => setEmail(event.target.value)} /></label>
        <label>{t('password')}<input aria-label={t('password')} type="password" value={password} onChange={(event) => setPassword(event.target.value)} /></label>
        {error && <div className="error">{t(error)}</div>}
        <button className="primary" type="submit">{t('signIn')}</button>
        {oidcMockEnabled && <button className="secondary" type="button" onClick={() => { const params = new URLSearchParams({ email, sub: `mock:${email}` }); window.location.href = `/api/auth/oidc/mock/start?${params}`; }}>{t('signInOidc')}</button>}
      </form>
    </div>
  );
}

function localizedStatus(status: string, t: ReturnType<typeof useI18n>['t']) {
  const keys = {
    pending: 'statusPending', running: 'statusRunning', completed: 'statusCompleted', failed: 'statusFailed',
    cancelled: 'statusCancelled', waiting_tool: 'statusWaitingTool', online: 'statusOnline', offline: 'statusOffline'
  } as const;
  return status in keys ? t(keys[status as keyof typeof keys]) : status;
}

type ApiKeyValidityChoice = '30' | '90' | '180' | '365' | 'date' | 'never';

function isoDateAfter(date: Date, days: number) {
  const next = new Date(date);
  next.setUTCDate(next.getUTCDate() + days);
  return next.toISOString().slice(0, 10);
}

function apiKeyValidity(choice: ApiKeyValidityChoice, customDate: string): ApiKeyValidity {
  if (choice === 'never') return { kind: 'never' };
  if (choice === 'date') return { kind: 'date', expires_at: new Date(`${customDate}T23:59:59.999Z`).toISOString() };
  return { kind: 'days', days: Number(choice) };
}

function ApiKeysPage() {
  const { locale, t } = useI18n();
  const pageSize = 20;
  const [apiKeys, setApiKeys] = useState<ApiKey[]>([]);
  const [page, setPage] = useState(1);
  const [total, setTotal] = useState(0);
  const [name, setName] = useState(() => t('defaultApiKeyName'));
  const [validityChoice, setValidityChoice] = useState<ApiKeyValidityChoice>('90');
  const [customExpiration, setCustomExpiration] = useState(() => isoDateAfter(new Date(), 90));
  const [createOpen, setCreateOpen] = useState(false);
  const [secret, setSecret] = useState<{ token: string; label: string } | null>(null);
  const [renewingKey, setRenewingKey] = useState<ApiKey | null>(null);
  const [renewalDate, setRenewalDate] = useState('');
  const [renewPermanently, setRenewPermanently] = useState(false);
  const [notice, setNotice] = useState('');
  const [copied, setCopied] = useState(false);
  const [mutating, setMutating] = useState(false);
  const [loadingPage, setLoadingPage] = useState<number | null>(1);
  const [error, setError] = useState('');
  const [loadError, setLoadError] = useState(false);
  const mutationActive = useRef(false);
  const mutationGeneration = useRef(0);
  const loadGeneration = useRef(0);
  const loadController = useRef<AbortController | null>(null);
  const activeLoadPage = useRef<number | null>(null);
  const mounted = useRef(true);
  const createButtonRef = useRef<HTMLButtonElement>(null);
  const createDialogRef = useRef<HTMLElement>(null);
  const nameInputRef = useRef<HTMLInputElement>(null);
  const secretCloseRef = useRef<HTMLButtonElement>(null);
  const secretOpenerRef = useRef<HTMLElement | null>(null);
  const restoreSecretFocus = useRef(false);

  const load = useCallback(async (requestedPage: number, { force = false }: { force?: boolean } = {}) => {
    if (!force && activeLoadPage.current === requestedPage) return;
    loadController.current?.abort();
    const controller = new AbortController();
    const generation = ++loadGeneration.current;
    loadController.current = controller;
    activeLoadPage.current = requestedPage;
    if (mounted.current) setLoadingPage(requestedPage);

    const fetchPage = async (targetPage: number): Promise<void> => {
      const response = await api.apiKeys(targetPage, pageSize, controller.signal);
      if (!mounted.current || generation !== loadGeneration.current) return;
      const validPage = Math.max(1, Math.ceil(response.total / pageSize));
      if (response.items.length === 0 && response.page > validPage) {
        await fetchPage(validPage);
        return;
      }
      setApiKeys(response.items);
      setTotal(response.total);
      setPage(response.page);
      setLoadError(false);
    };

    try {
      await fetchPage(requestedPage);
    } catch (err) {
      if (controller.signal.aborted) return;
      if (mounted.current && generation === loadGeneration.current) setLoadError(true);
    } finally {
      if (mounted.current && generation === loadGeneration.current) {
        activeLoadPage.current = null;
        loadController.current = null;
        setLoadingPage(null);
      }
    }
  }, []);
  useEffect(() => { load(1); }, [load]);
  useEffect(() => () => {
    mounted.current = false;
    mutationGeneration.current += 1;
    loadGeneration.current += 1;
    loadController.current?.abort();
  }, []);
  useEffect(() => {
    if (!createOpen && !secret && !renewingKey) return;
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key !== 'Escape') return;
      if (secret) closeSecret();
      else if (renewingKey && !mutating) setRenewingKey(null);
      else if (!mutating) closeCreate();
    };
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [createOpen, mutating, renewingKey, secret]);
  useEffect(() => {
    if (createOpen) nameInputRef.current?.focus();
    else if (secret) secretCloseRef.current?.focus();
  }, [createOpen, secret]);
  useEffect(() => {
    if (createOpen && mutating) createDialogRef.current?.focus();
  }, [createOpen, mutating]);
  useEffect(() => {
    if (!mutating && restoreSecretFocus.current) focusSecretOpener();
  }, [mutating]);

  function beginMutation() {
    if (mutationActive.current) return null;
    mutationActive.current = true;
    const generation = ++mutationGeneration.current;
    setMutating(true);
    setError('');
    return generation;
  }

  function finishMutation(generation: number) {
    if (generation !== mutationGeneration.current) return;
    mutationActive.current = false;
    setMutating(false);
  }

  async function create(event: FormEvent) {
    event.preventDefault();
    const generation = beginMutation();
    if (generation === null) return;
    secretOpenerRef.current = createButtonRef.current;
    try {
      const response = await api.createApiKey(name, apiKeyValidity(validityChoice, customExpiration));
      if (generation !== mutationGeneration.current) return;
      setCreateOpen(false);
      setSecret({ token: response.token, label: response.api_key.name });
      await load(1, { force: true });
    } catch (err) {
      if (generation === mutationGeneration.current) setError(t('genericError'));
    } finally {
      finishMutation(generation);
    }
  }

  async function renew(event: FormEvent) {
    event.preventDefault();
    if (!renewingKey) return;
    const generation = beginMutation();
    if (generation === null) return;
    try {
      await api.renewApiKey(renewingKey.id, renewPermanently
        ? { kind: 'never' }
        : { kind: 'date', expires_at: new Date(`${renewalDate}T23:59:59.999Z`).toISOString() });
      if (generation !== mutationGeneration.current) return;
      setRenewingKey(null);
      setNotice(t('apiKeyRenewed'));
      await load(page, { force: true });
    } catch (err) {
      if (generation === mutationGeneration.current) setError(t('genericError'));
    } finally {
      finishMutation(generation);
    }
  }

  async function remove(apiKey: ApiKey) {
    if (mutationActive.current) return;
    if (!window.confirm(t('confirmDeleteKey').replace('{name}', apiKey.name))) return;
    const generation = beginMutation();
    if (generation === null) return;
    try {
      await api.deleteApiKey(apiKey.id);
      if (generation !== mutationGeneration.current) return;
      await load(page, { force: true });
    } catch (err) {
      if (generation === mutationGeneration.current) {
        setError(t('genericError'));
        await load(page, { force: true });
      }
    } finally {
      finishMutation(generation);
    }
  }

  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  const loading = loadingPage !== null;
  const pageLabel = t('pageOf').replace('{page}', String(page)).replace('{total}', String(totalPages));
  const tomorrow = isoDateAfter(new Date(), 1);
  const renewalMinimum = renewingKey?.expires_at
    ? isoDateAfter(new Date(Math.max(Date.now(), new Date(renewingKey.expires_at).getTime())), 1)
    : tomorrow;
  function beginRenew(apiKey: ApiKey) {
    if (!apiKey.expires_at) return;
    const currentExpiration = new Date(apiKey.expires_at);
    setRenewalDate(isoDateAfter(currentExpiration > new Date() ? currentExpiration : new Date(), 365));
    setRenewPermanently(false);
    setNotice('');
    setError('');
    setRenewingKey(apiKey);
  }
  function closeCreate() {
    if (mutating) return;
    setCreateOpen(false);
    window.setTimeout(() => createButtonRef.current?.focus());
  }
  function focusSecretOpener() {
    const opener = secretOpenerRef.current;
    if (!opener?.isConnected || opener.matches(':disabled')) {
      restoreSecretFocus.current = true;
      return;
    }
    restoreSecretFocus.current = false;
    opener.focus();
    secretOpenerRef.current = null;
  }
  function closeSecret() {
    setSecret(null);
    setCopied(false);
    restoreSecretFocus.current = true;
    window.setTimeout(focusSecretOpener);
  }
  async function copySecret() {
    if (!secret) return;
    await navigator.clipboard.writeText(secret.token);
    setCopied(true);
  }
  const trapModalFocus = (event: React.KeyboardEvent<HTMLElement>) => {
    if (event.key !== 'Tab') return;
    const focusable = Array.from(event.currentTarget.querySelectorAll<HTMLElement>('button:not([disabled]), input:not([disabled])'));
    const first = focusable[0];
    const last = focusable.at(-1);
    if (!first || !last) {
      event.preventDefault();
      event.currentTarget.focus();
      return;
    }
    if (document.activeElement === event.currentTarget) {
      event.preventDefault();
      (event.shiftKey ? last : first).focus();
      return;
    }
    if (event.shiftKey && document.activeElement === first) {
      event.preventDefault();
      last.focus();
    } else if (!event.shiftKey && document.activeElement === last) {
      event.preventDefault();
      first.focus();
    }
  };

  return (
    <div className="api-keys-page">
      <header className="page-header">
        <h1>{t('apiKeys')}</h1>
        <button ref={createButtonRef} className="primary" type="button" disabled={mutating} onClick={() => { setError(''); setCreateOpen(true); }}><Plus size={16} /> {t('createApiKeyAction')}</button>
      </header>
      <section className="api-key-list" aria-label={t('apiKeys')}>
        {error && <div className="error api-key-error" role="alert">{error}</div>}
        {notice && <div className="success" role="status">{notice}</div>}
        {loadError && <div className="error docs-error" role="alert"><span>{t('apiKeysLoadError')}</span><button className="secondary" type="button" disabled={loading} onClick={() => load(page)}>{t('retry')}</button></div>}
        <div className="api-key-list-header" aria-hidden="true"><span>{t('name')}</span><span>{t('apiKeyPrefix')}</span><span>{t('apiKeyExpiration')}</span><span>{t('apiKeyUsage')}</span><span>{t('apiKeyActions')}</span></div>
        {loading && apiKeys.length === 0 && <div className="api-key-state">{t('loadingApiKeys')}</div>}
        {!loading && !loadError && apiKeys.length === 0 && <div className="api-key-state">{t('noApiKeys')}</div>}
        {apiKeys.length > 0 && (
          <div className="api-key-rows">
          {apiKeys.map((apiKey) => (
            <div className="api-key-row" key={apiKey.id}>
              <strong className="api-key-name">{apiKey.name}</strong>
              <code>{apiKey.prefix}...</code>
              <span>{apiKey.expires_at ? new Date(apiKey.expires_at).toLocaleString(locale) : t('neverExpires')}</span>
              <span>{apiKey.last_used_at ? `${t('lastUsed')} ${new Date(apiKey.last_used_at).toLocaleString(locale)}` : t('neverUsed')}</span>
              <div className="button-row">
                {apiKey.expires_at && <button className="secondary" disabled={mutating} onClick={() => beginRenew(apiKey)}>{t('renew')}</button>}
                <button className="icon-button danger" aria-label={t('delete')} title={t('delete')} disabled={mutating} onClick={() => remove(apiKey)}><Trash2 size={16} /></button>
              </div>
            </div>
          ))}
          </div>
        )}
        <nav className="pagination" aria-label={pageLabel}>
          <button className="secondary" type="button" disabled={mutating || page <= 1 || loadingPage === page - 1} onClick={() => load(page - 1)}>{t('previous')}</button>
          <span>{pageLabel}</span>
          <button className="secondary" type="button" disabled={mutating || page >= totalPages || loadingPage === page + 1} onClick={() => load(page + 1)}>{t('next')}</button>
        </nav>
      </section>
      {createOpen && <FormDialog title={t('createApiKeyAction')} onClose={closeCreate} busy={mutating} initialFocusRef={nameInputRef} footer={<><button className="secondary" type="button" disabled={mutating} onClick={closeCreate}>{t('cancel')}</button><button className="primary" form="create-api-key-form" disabled={mutating || !name.trim() || (validityChoice === 'date' && !customExpiration)}><Plus size={16} /> {t('createKey')}</button></>}>
        <form id="create-api-key-form" className="stack" onSubmit={create}>
          <label>{t('name')}<input ref={nameInputRef} disabled={mutating} value={name} onChange={(event) => setName(event.target.value)} /></label>
          <label>{t('apiKeyValidity')}<select value={validityChoice} disabled={mutating} onChange={(event) => setValidityChoice(event.target.value as ApiKeyValidityChoice)}><option value="30">{t('validFor30Days')}</option><option value="90">{t('validFor90Days')}</option><option value="180">{t('validFor180Days')}</option><option value="365">{t('validFor365Days')}</option><option value="date">{t('customExpiration')}</option><option value="never">{t('neverExpires')}</option></select></label>
          {validityChoice === 'date' && <label>{t('apiKeyExpiration')}<input type="date" min={tomorrow} required value={customExpiration} onChange={(event) => setCustomExpiration(event.target.value)} /></label>}
          {error && <div className="error" role="alert">{error}</div>}
        </form>
      </FormDialog>}
      {renewingKey && <FormDialog title={t('renewApiKey')} eyebrow={renewingKey.name} onClose={() => setRenewingKey(null)} busy={mutating} footer={<><button className="secondary" type="button" disabled={mutating} onClick={() => setRenewingKey(null)}>{t('cancel')}</button><button className="primary" form="renew-api-key-form" disabled={mutating || (!renewPermanently && !renewalDate)}>{t('renew')}</button></>}>
        <form id="renew-api-key-form" className="stack" onSubmit={renew}>
          <label>{t('apiKeyExpiration')}<input type="date" min={renewalMinimum} disabled={renewPermanently || mutating} required={!renewPermanently} value={renewalDate} onChange={(event) => setRenewalDate(event.target.value)} /></label>
          <label className="check-row"><input type="checkbox" checked={renewPermanently} onChange={(event) => setRenewPermanently(event.target.checked)} /> {t('makePermanent')}</label>
          <p className="muted">{t('renewKeepsToken')}</p>
          {error && <div className="error" role="alert">{error}</div>}
        </form>
      </FormDialog>}
      {secret && <FormDialog title={t('oneTimeApiKey')} onClose={closeSecret} initialFocusRef={secretCloseRef} footer={<button ref={secretCloseRef} className="primary" type="button" onClick={closeSecret}>{t('close')}</button>}>
        <div className="secret-result"><strong>{secret.label}</strong><span>{t('shownOnce')}</span><div className="secret-token-line"><code className="secret-token">{secret.token}</code><button className="icon-button secret-copy" type="button" aria-label={t('copyApiKey')} title={t('copyApiKey')} onClick={copySecret}>{copied ? <Check size={17} /> : <Copy size={17} />}</button></div></div>
      </FormDialog>}
    </div>
  );
}

type OpenApiDocument = {
  info: { title: string; version: string; description?: string };
  paths: Record<string, Record<string, { summary?: string }>>;
};

function ApiDocsPage() {
  const { t } = useI18n();
  const [document, setDocument] = useState<OpenApiDocument | null>(null);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const [loadGeneration, setLoadGeneration] = useState(0);

  useEffect(() => {
    const controller = new AbortController();
    let active = true;
    setLoading(true);
    setError('');
    fetch('/openapi.json', { signal: controller.signal })
      .then((response) => {
        if (!response.ok) throw new Error('Failed to load OpenAPI document');
        if (!response.headers.get('content-type')?.includes('application/json')) {
          throw new Error('OpenAPI response is not JSON');
        }
        return response.json();
      })
      .then((nextDocument) => {
        if (!active) return;
        if (!nextDocument?.paths || !nextDocument?.info) throw new Error('OpenAPI document is incomplete');
        setDocument(nextDocument);
      })
      .catch((err) => {
        if (!active || (err instanceof DOMException && err.name === 'AbortError')) return;
        setDocument(null);
        setError(t('apiDocsLoadError'));
      })
      .finally(() => {
        if (active) setLoading(false);
      });
    return () => {
      active = false;
      controller.abort();
    };
  }, [loadGeneration, t]);

  const endpoints = document
    ? Object.entries(document.paths).flatMap(([path, operations]) =>
        Object.entries(operations).map(([method, operation]) => ({ path, method, summary: operation.summary ?? '' })))
    : [];

  return (
    <div className="docs-page">
      <header className="docs-header">
        <div>
          <div className="section-title"><BookOpen size={20} /> {t('apiReference')}</div>
          <h1>{document?.info.title ?? 'Agent Hub API'}</h1>
          <p>{document?.info.description ?? 'Loading the machine-readable API description...'}</p>
        </div>
        <a className="secondary link-button" href="/openapi.json" target="_blank" rel="noreferrer">{t('openApiJson')} <ExternalLink size={15} /></a>
      </header>
      <section className="docs-section">
        <h2>{t('authentication')}</h2><p>{t('authHelp')}</p>
        <code className="auth-example">Authorization: Bearer ahk_your_api_key</code>
      </section>
      <section className="docs-section" aria-busy={loading}>
        <h2>{t('endpoints')}</h2>
        {loading && <div className="empty compact-empty loading-status" role="status" aria-live="polite"><span>{t('loadingApiDocs')}</span><button className="secondary" type="button" onClick={() => setLoadGeneration((value) => value + 1)}>{t('retry')}</button></div>}
        {error && <div className="error docs-error" role="alert"><span>{error}</span><button className="secondary" onClick={() => setLoadGeneration((value) => value + 1)}>{t('retry')}</button></div>}
        <div className="endpoint-list">
          {endpoints.map((endpoint) => (
            <div className="endpoint-row" key={`${endpoint.method}-${endpoint.path}`}>
              <span className={`method method-${endpoint.method}`}>{endpoint.method.toUpperCase()}</span>
              <code>{endpoint.path}</code>
              <span>{endpoint.summary}</span>
            </div>
          ))}
        </div>
      </section>
    </div>
  );
}
const TERMINAL_RUN_STATUSES = new Set(['completed', 'failed', 'cancelled', 'waiting_tool']);

function statusFromRunEvents(runStatus: string, events: RunEvent[]) {
  if (TERMINAL_RUN_STATUSES.has(runStatus)) return runStatus;
  const statuses = events.flatMap((event) => {
    if (event.event_type !== 'status') return [];
    const status = event.content ?? (typeof event.payload.status === 'string' ? event.payload.status : null);
    return status ? [{ seq: event.seq, status }] : [];
  }).sort((left, right) => left.seq - right.seq);
  const latestTerminal = [...statuses].reverse().find((event) => TERMINAL_RUN_STATUSES.has(event.status));
  return latestTerminal?.status ?? statuses.at(-1)?.status ?? runStatus;
}

function RunConsole({ run }: { run: Run }) {
  const { t } = useI18n();
  const [eventState, setEventState] = useState<{ runId: string; events: RunEvent[] }>({ runId: run.id, events: [] });
  const [eventError, setEventError] = useState(false);
  const [eventsLoading, setEventsLoading] = useState(true);
  const [eventRetry, setEventRetry] = useState(0);
  const events = eventState.runId === run.id ? eventState.events : [];
  const status = statusFromRunEvents(run.status, events);

  useEffect(() => {
    let active = true;
    const controller = new AbortController();
    setEventState({ runId: run.id, events: [] });
    setEventError(false);
    setEventsLoading(true);
    api.runEvents(run.id, controller.signal).then((loaded) => {
      if (!active) return;
      setEventState((current) => {
        const liveEvents = current.runId === run.id ? current.events : [];
        const merged = new Map(loaded.map((event) => [event.seq, event]));
        for (const event of liveEvents) merged.set(event.seq, event);
        return {
          runId: run.id,
          events: [...merged.values()].sort((left, right) => left.seq - right.seq)
        };
      });
      setEventsLoading(false);
    }).catch(() => {
      if (!active || controller.signal.aborted) return;
      setEventError(true);
      setEventsLoading(false);
    });

    const source = new EventSource(`/api/runs/${run.id}/events/stream`, { withCredentials: true });
    source.addEventListener('run_event', (event) => {
      let parsed: RunEvent;
      try {
        parsed = JSON.parse((event as MessageEvent).data) as RunEvent;
      } catch {
        return;
      }
      if (!active || parsed.run_id !== run.id) return;
      setEventState((current) => {
        const currentEvents = current.runId === run.id ? current.events : [];
        return currentEvents.some((item) => item.seq === parsed.seq)
          ? current
          : { runId: run.id, events: [...currentEvents, parsed].sort((left, right) => left.seq - right.seq) };
      });
    });
    return () => {
      active = false;
      controller.abort();
      source.close();
    };
  }, [eventRetry, run.id]);

  // 最终消息是持久化的规范结果；到达后不再重复展示此前的流式片段。
  const assistantComplete = events.some((event) => event.event_type === 'message' && event.role === 'assistant');
  const visibleEvents = assistantComplete ? events.filter((event) => event.event_type !== 'message_delta') : events;

  return (
    <div role="region" aria-label={t('runEvents')}>
      <div className="console-header">
        <div className="section-title"><Activity size={18} /> {t('runConsole')}</div>
        <span className={`status ${status}`}>{localizedStatus(status, t)}</span>
      </div>
      <div className="console">
        {eventsLoading && <div className="console-state">{t('loadingRunEvents')}</div>}
        {eventError && <div className="console-state error">{t('runEventsLoadFailed')} <button type="button" className="text-button console-retry" onClick={() => setEventRetry((value) => value + 1)}>{t('retry')}</button></div>}
        {!eventsLoading && !eventError && visibleEvents.length === 0 && <div className="console-state">{t('noRunEvents')}</div>}
        {visibleEvents.map((event) => (
          <div className="event-row" key={event.seq}>
            <span className="event-meta">#{event.seq} {event.event_type}{event.role ? ` · ${event.role}` : ''}</span>
            {event.content && <p>{event.content}</p>}
            {(() => {
              const failure = typeof event.payload.error === 'string'
                ? event.payload.error
                : typeof event.payload.reason === 'string' ? event.payload.reason : null;
              return failure && failure !== event.content ? <p className="event-error">{failure}</p> : null;
            })()}
          </div>
        ))}
      </div>
    </div>
  );
}

function RuntimesPage({ user }: { user: User }) {
  const { locale, t } = useI18n();
  const [runtimes, setRuntimes] = useState<Runtime[]>([]);
  const [agents, setAgents] = useState<Agent[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [search, setSearch] = useState('');
  const [filter, setFilter] = useState<'all' | 'online' | 'issues'>('all');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);
  const [retryGeneration, setRetryGeneration] = useState(0);
  const [enrollments, setEnrollments] = useState<RuntimeEnrollmentToken[]>([]);
  const [createdEnrollment, setCreatedEnrollment] = useState<{ enrollment: RuntimeEnrollmentToken; token: string } | null>(null);
  const [enrollmentOpen, setEnrollmentOpen] = useState(false);
  const [adminBusy, setAdminBusy] = useState(false);
  const [adminError, setAdminError] = useState(false);
  const [adminNotice, setAdminNotice] = useState<TranslationKey | null>(null);
  const [affectedSessions, setAffectedSessions] = useState<HubSession[]>([]);
  const [forceDeleteResult, setForceDeleteResult] = useState<{ recoverable: string[]; failed: string[] } | null>(null);
  const isSuperAdmin = user.role === 'super_admin';

  useEffect(() => {
    let active = true;
    let timer: number | undefined;
    const poll = async () => {
      try {
        const [runtimeResult, agentResult] = await Promise.allSettled([api.runtimes(), api.agents()]);
        if (!active) return;
        if (runtimeResult.status === 'rejected' || agentResult.status === 'rejected') {
          setError(true);
          return;
        }
        const runtimeResponse = runtimeResult.value;
        const agentResponse = agentResult.value;
        const sorted = [...runtimeResponse].sort((left, right) =>
          left.hostname.localeCompare(right.hostname) || left.id.localeCompare(right.id));
        setRuntimes(sorted);
        setAgents(agentResponse);
        setSelectedId((current) => current && sorted.some((runtime) => runtime.id === current)
          ? current
          : sorted[0]?.id ?? null);
        setError(false);
      } finally {
        if (active) {
          setLoading(false);
          timer = window.setTimeout(poll, 2000);
        }
      }
    };
    poll();
    return () => {
      active = false;
      if (timer !== undefined) window.clearTimeout(timer);
    };
  }, [retryGeneration]);

  useEffect(() => {
    if (!isSuperAdmin) return;
    const controller = new AbortController();
    api.runtimeEnrollmentTokens(controller.signal)
      .then(setEnrollments)
      .catch(() => { if (!controller.signal.aborted) setAdminError(true); });
    return () => controller.abort();
  }, [isSuperAdmin]);

  const counts = useMemo(() => ({
    all: runtimes.length,
    online: runtimes.filter((runtime) => runtime.status === 'online').length,
    issues: runtimes.filter((runtime) => runtime.status !== 'online').length
  }), [runtimes]);
  const filteredRuntimes = useMemo(() => {
    const query = search.trim().toLocaleLowerCase(locale);
    return runtimes.filter((runtime) => {
      const matchesFilter = filter === 'all'
        || (filter === 'online' ? runtime.status === 'online' : runtime.status !== 'online');
      const haystack = [runtime.hostname, runtime.status, runtime.codex_version, ...runtime.labels].join(' ').toLocaleLowerCase(locale);
      return matchesFilter && (!query || haystack.includes(query));
    });
  }, [filter, locale, runtimes, search]);
  const selectedRuntime = runtimes.find((runtime) => runtime.id === selectedId) ?? null;
  const boundAgents = agents.filter((agent) => agent.runtime_id === selectedRuntime?.id);

  const visibleCapabilities = ['driver', 'codex_source', 'model_proxy', 'mcp_allowlist', 'thread_resume', 'local_skills']
    .flatMap((key) => selectedRuntime && key in selectedRuntime.capabilities ? [[key, selectedRuntime.capabilities[key]] as const] : []);

  function capabilityValue(value: unknown) {
    if (value === true) return t('enabled');
    if (value === false) return t('disabled');
    return typeof value === 'string' || typeof value === 'number' ? String(value) : t('unavailable');
  }

  function replaceRuntime(updated: Runtime) {
    setRuntimes((current) => current.map((runtime) => runtime.id === updated.id ? updated : runtime));
  }

  async function createEnrollment() {
    if (adminBusy) return;
    setAdminBusy(true);
    setAdminError(false);
    setAdminNotice(null);
    try {
      const response = await api.createRuntimeEnrollmentToken();
      setCreatedEnrollment(response);
      setEnrollments((current) => [response.enrollment, ...current.filter((item) => item.id !== response.enrollment.id)]);
      setEnrollmentOpen(true);
    } catch {
      setAdminError(true);
    } finally {
      setAdminBusy(false);
    }
  }

  async function revokeEnrollment(enrollmentId: string) {
    if (adminBusy) return;
    setAdminBusy(true);
    setAdminError(false);
    try {
      const updated = await api.revokeRuntimeEnrollmentToken(enrollmentId);
      setEnrollments((current) => current.map((item) => item.id === updated.id ? updated : item));
      setCreatedEnrollment((current) => current?.enrollment.id === updated.id ? { ...current, enrollment: updated } : current);
    } catch {
      setAdminError(true);
    } finally {
      setAdminBusy(false);
    }
  }

  async function rotateCredential(runtime: Runtime) {
    if (adminBusy) return;
    setAdminBusy(true);
    setAdminError(false);
    setAdminNotice(null);
    try {
      replaceRuntime(await api.requestRuntimeCredentialRotation(runtime.id));
      setAdminNotice('runtimeRotationRequested');
    } catch {
      setAdminError(true);
    } finally {
      setAdminBusy(false);
    }
  }

  async function drain(runtime: Runtime) {
    if (!window.confirm(t('confirmDrainRuntime').replace('{hostname}', runtime.hostname))) return;
    setAdminBusy(true);
    setAdminError(false);
    setAdminNotice(null);
    setForceDeleteResult(null);
    try {
      const response = await api.drainRuntime(runtime.id, runtime.hostname);
      replaceRuntime(response.runtime);
      setAffectedSessions(response.owned_sessions);
      setAdminNotice('runtimeDrainStarted');
    } catch {
      setAdminError(true);
    } finally {
      setAdminBusy(false);
    }
  }

  async function cancelDrain(runtime: Runtime) {
    setAdminBusy(true);
    setAdminError(false);
    setAdminNotice(null);
    try {
      const response = await api.cancelRuntimeDrain(runtime.id);
      replaceRuntime(response.runtime);
      setAffectedSessions(response.owned_sessions);
      setAdminNotice('runtimeDrainCancelled');
    } catch {
      setAdminError(true);
    } finally {
      setAdminBusy(false);
    }
  }

  async function deleteRuntime(runtime: Runtime) {
    if (!window.confirm(t('confirmDeleteRuntime').replace('{hostname}', runtime.hostname))) return;
    setAdminBusy(true);
    setAdminError(false);
    setAdminNotice(null);
    try {
      await api.deleteRuntime(runtime.id, runtime.hostname);
      setRuntimes((current) => current.filter((item) => item.id !== runtime.id));
      setSelectedId((current) => current === runtime.id ? null : current);
      setAffectedSessions([]);
      setAdminNotice('runtimeDeleted');
    } catch {
      setAdminError(true);
    } finally {
      setAdminBusy(false);
    }
  }

  async function forceDelete(runtime: Runtime) {
    if (!window.confirm(t('confirmForceDeleteRuntime').replace('{hostname}', runtime.hostname))) return;
    setAdminBusy(true);
    setAdminError(false);
    setAdminNotice(null);
    try {
      const response = await api.forceDeleteRuntime(runtime.id, runtime.hostname);
      setRuntimes((current) => current.filter((item) => item.id !== runtime.id));
      setSelectedId((current) => current === runtime.id ? null : current);
      setAffectedSessions([]);
      setForceDeleteResult({ recoverable: response.recoverable_session_ids, failed: response.recovery_failed_session_ids });
      setAdminNotice('runtimeForceDeleted');
    } catch {
      setAdminError(true);
    } finally {
      setAdminBusy(false);
    }
  }

  function enrollmentStatus(enrollment: RuntimeEnrollmentToken) {
    if (enrollment.revoked_at) return t('enrollmentRevoked');
    if (enrollment.consumed_at) return t('enrollmentConsumed');
    if (new Date(enrollment.expires_at).getTime() <= Date.now()) return t('enrollmentExpired');
    return t('enrollmentUnused');
  }

  return (
    <section className="runtime-workspace" aria-labelledby="runtime-page-title">
      <header className="runtime-page-header">
        <div>
          <h1 id="runtime-page-title"><Monitor size={19} /> {t('runtimeNodes')}</h1>
          <p>{t('runtimeSubtitle')}</p>
        </div>
        <div className="runtime-header-actions">
          {!loading && <span className="runtime-count">{counts.all}</span>}
          {isSuperAdmin && <button type="button" className="secondary compact-action" disabled={adminBusy} onClick={createEnrollment}><Plus size={15} /> {t('createEnrollmentToken')}</button>}
        </div>
      </header>

      {error && <div className="runtime-alert" role="alert"><span>{t('runtimeLoadFailed')}</span><button type="button" onClick={() => { setLoading(true); setRetryGeneration((current) => current + 1); }}>{t('retry')}</button></div>}
      {adminError && <div className="runtime-alert" role="alert"><span>{t('runtimeActionFailed')}</span><button type="button" onClick={() => setAdminError(false)}>{t('close')}</button></div>}
      {adminNotice && <div className="runtime-notice" role="status">{t(adminNotice)}</div>}
      {forceDeleteResult && <div className="runtime-notice force-result"><span>{t('recoverableSessions')}: {forceDeleteResult.recoverable.join(', ') || t('none')}</span><span>{t('recoveryFailedSessions')}: {forceDeleteResult.failed.join(', ') || t('none')}</span></div>}

      <div className="runtime-layout">
        <aside className="runtime-master" aria-label={t('runtimeList')}>
          <div className="runtime-tools">
            <label className="runtime-search">
              <span className="sr-only">{t('searchRuntimes')}</span>
              <Search size={16} aria-hidden="true" />
              <input value={search} onChange={(event) => setSearch(event.target.value)} placeholder={t('searchRuntimes')} />
            </label>
            <fieldset className="runtime-filters">
              <legend className="sr-only">{t('runtimeFilter')}</legend>
              {(['all', 'online', 'issues'] as const).map((value) => (
                <label key={value}>
                  <input type="radio" name="runtime-filter" value={value} checked={filter === value} onChange={() => setFilter(value)} />
                  <span>{t(value === 'all' ? 'filterAll' : value === 'online' ? 'filterOnline' : 'filterIssues')} <small>{counts[value]}</small></span>
                </label>
              ))}
            </fieldset>
          </div>

          <div className="runtime-list" aria-live="polite" aria-busy={loading}>
            {loading && <div className="runtime-state" role="status">{t('loadingRuntimes')}</div>}
            {!loading && runtimes.length === 0 && !error && <div className="runtime-state">{t('noRuntimes')}</div>}
            {!loading && runtimes.length > 0 && filteredRuntimes.length === 0 && <div className="runtime-state">{t('noRuntimeMatches')}</div>}
            {filteredRuntimes.map((runtime) => (
              <button
                className={`runtime-row${runtime.id === selectedId ? ' selected' : ''}`}
                key={runtime.id}
                type="button"
                aria-pressed={runtime.id === selectedId}
                onClick={() => setSelectedId(runtime.id)}
              >
                <span className="runtime-row-heading"><strong>{runtime.hostname}</strong><span className={`status ${runtime.status}`}>{localizedStatus(runtime.status, t)}</span></span>
                <span className="runtime-row-meta"><span>{t('lastHeartbeat')}: {new Date(runtime.last_heartbeat_at).toLocaleString(locale)}</span><span>{runtime.codex_version}</span></span>
              </button>
            ))}
          </div>
        </aside>

        <section className="runtime-detail" role="region" aria-label={t('runtimeDetails')}>
          {!selectedRuntime ? (
            <div className="runtime-state runtime-detail-state">{loading ? t('loadingRuntimes') : t('selectRuntime')}</div>
          ) : (
            <>
              <header className="runtime-detail-header">
                <div><span className="eyebrow">{t('runtimeIdentity')}</span><h2>{selectedRuntime.hostname}</h2><code>{selectedRuntime.id}</code></div>
                <span className={`status ${selectedRuntime.status}`}>{localizedStatus(selectedRuntime.status, t)}</span>
              </header>

              <div className="runtime-detail-grid">
                <section className="runtime-detail-section">
                  <h3>{t('runtimeStatus')}</h3>
                  <dl className="runtime-properties">
                    <div><dt>{t('hostname')}</dt><dd>{selectedRuntime.hostname}</dd></div>
                    <div><dt>{t('codexVersion')}</dt><dd>{selectedRuntime.codex_version}</dd></div>
                    <div><dt>{t('lastHeartbeat')}</dt><dd>{new Date(selectedRuntime.last_heartbeat_at).toLocaleString(locale)}</dd></div>
                  </dl>
                </section>
                <section className="runtime-detail-section">
                  <h3>{t('runtimeExecution')}</h3>
                  <dl className="runtime-properties">
                    <div><dt>{t('sandbox')}</dt><dd>{selectedRuntime.sandbox_mode}</dd></div>
                    <div><dt>{t('modelProxy')}</dt><dd>{selectedRuntime.capabilities.model_proxy === true ? t('enabled') : t('unavailable')}</dd></div>
                  </dl>
                  {selectedRuntime.capabilities.sandbox_downgraded === true && <p className="runtime-warning">{t('sandboxDowngraded')}: {String(selectedRuntime.capabilities.sandbox_downgrade_reason ?? t('runtimeLimitation'))}</p>}
                </section>
                <section className="runtime-detail-section">
                  <h3>{t('capabilities')}</h3>
                  <dl className="runtime-properties runtime-capabilities">
                    {visibleCapabilities.map(([key, value]) => <div key={key}><dt>{key}</dt><dd>{capabilityValue(value)}</dd></div>)}
                  </dl>
                  {visibleCapabilities.length === 0 && <p className="runtime-muted">{t('noCapabilities')}</p>}
                </section>
                <section className="runtime-detail-section">
                  <h3>{t('labels')}</h3>
                  {selectedRuntime.labels.length > 0 ? <div className="runtime-labels">{selectedRuntime.labels.map((label) => <span key={label}>{label}</span>)}</div> : <p className="runtime-muted">{t('noLabels')}</p>}
                </section>
                <section className="runtime-detail-section runtime-agents">
                  <h3>{t('boundAgents')}</h3>
                  {boundAgents.length > 0 ? <div className="runtime-agent-list">{boundAgents.map((agent) => <a key={agent.id} href={`/agents/${agent.id}`} onClick={(event) => { event.preventDefault(); navigate(`/agents/${agent.id}`); }}>{agent.name}</a>)}</div> : <p className="runtime-muted">{t('noBoundAgents')}</p>}
                </section>
                {isSuperAdmin && <section className="runtime-detail-section runtime-administration">
                  <h3>{t('runtimeAdministration')}</h3>
                  <div className="runtime-admin-actions">
                    <button type="button" className="secondary" disabled={adminBusy || Boolean(selectedRuntime.credential_rotation_requested_at)} onClick={() => rotateCredential(selectedRuntime)}><RotateCcw size={15} /> {t('rotateCredential')}</button>
                    {selectedRuntime.status === 'draining'
                      ? <button type="button" className="secondary" disabled={adminBusy} onClick={() => cancelDrain(selectedRuntime)}><RotateCcw size={15} /> {t('cancelDrain')}</button>
                      : <button type="button" className="secondary" disabled={adminBusy} onClick={() => drain(selectedRuntime)}><CirclePause size={15} /> {t('drainRuntime')}</button>}
                    {selectedRuntime.status === 'draining' && <button type="button" className="secondary danger" disabled={adminBusy} onClick={() => deleteRuntime(selectedRuntime)}><Trash2 size={15} /> {t('deleteRuntime')}</button>}
                    <button type="button" className="secondary danger" disabled={adminBusy} onClick={() => forceDelete(selectedRuntime)}><ShieldAlert size={15} /> {t('forceDeleteRuntime')}</button>
                  </div>
                  {selectedRuntime.credential_rotation_requested_at && <p className="runtime-muted">{t('runtimeRotationPending')}</p>}
                  {affectedSessions.length > 0 && <div className="affected-sessions"><strong>{t('affectedSessions')}</strong>{affectedSessions.map((session) => <a key={session.id} href={`/sessions`} onClick={(event) => { event.preventDefault(); navigate('/sessions'); }}><span>{session.agent_name}</span><span className={`status ${session.lifecycle_status}`}>{localizedStatus(session.lifecycle_status, t)}</span></a>)}</div>}
                  {selectedRuntime.status === 'draining' && affectedSessions.length === 0 && <p className="runtime-muted">{t('noAffectedSessions')}</p>}
                </section>}
              </div>
            </>
          )}
        </section>
      </div>
      {enrollmentOpen && <div className="modal-backdrop" role="presentation">
        <section className="modal runtime-enrollment-modal" role="dialog" aria-modal="true" aria-labelledby="runtime-enrollment-title">
          <header className="modal-header"><h2 id="runtime-enrollment-title">{t('runtimeEnrollment')}</h2><button type="button" className="icon-button" aria-label={t('close')} onClick={() => { setEnrollmentOpen(false); setCreatedEnrollment(null); }}><X size={17} /></button></header>
          {createdEnrollment && <div className="secret-result"><strong>{t('oneTimeEnrollmentToken')}</strong><span>{t('shownOnce')}</span><code className="secret-token">{createdEnrollment.token}</code></div>}
          <div className="enrollment-list">
            <h3>{t('enrollmentHistory')}</h3>
            {enrollments.length === 0 && <p className="runtime-muted">{t('noEnrollmentTokens')}</p>}
            {enrollments.map((enrollment) => <div className="enrollment-row" key={enrollment.id}><span><code>{enrollment.id}</code><small>{t('enrollmentExpires')}: {new Date(enrollment.expires_at).toLocaleString(locale)}</small></span><span className="status">{enrollmentStatus(enrollment)}</span>{!enrollment.consumed_at && !enrollment.revoked_at && new Date(enrollment.expires_at).getTime() > Date.now() && <button type="button" className="secondary compact-action" disabled={adminBusy} onClick={() => revokeEnrollment(enrollment.id)}>{t('revokeToken')}</button>}</div>)}
          </div>
        </section>
      </div>}
    </section>
  );
}

function AutomationsPage() {
  const { locale, t } = useI18n();
  const historyPageSize = 20;
  const [agents, setAgents] = useState<Agent[]>([]);
  const [automations, setAutomations] = useState<Automation[]>([]);
  const [user, setUser] = useState<User | null>(null);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [filter, setFilter] = useState('');
  const [selectedAgentId, setSelectedAgentId] = useState('');
  const [name, setName] = useState(() => t('defaultAutomationName'));
  const [triggerType, setTriggerType] = useState('manual');
  const [prompt, setPrompt] = useState(() => t('defaultAutomationPrompt'));
  const [schedule, setSchedule] = useState('');
  const [enabled, setEnabled] = useState(true);
  const [selectedRun, setSelectedRun] = useState<Run | null>(null);
  const [historyRuns, setHistoryRuns] = useState<Run[]>([]);
  const [historyPage, setHistoryPage] = useState(1);
  const [historyTotal, setHistoryTotal] = useState(0);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [historyError, setHistoryError] = useState(false);
  const [historyRefresh, setHistoryRefresh] = useState(0);
  const [createdWebhook, setCreatedWebhook] = useState<{ id: string; token: string } | null>(null);
  const [error, setError] = useState<TranslationKey | null>(null);
  const [notice, setNotice] = useState<TranslationKey | null>(null);
  const [saving, setSaving] = useState(false);
  const loadGeneration = useRef(0);
  const selectionGeneration = useRef(0);
  const triggerLabel = (triggerType: string) => {
    if (triggerType === 'manual' || triggerType === 'webhook' || triggerType === 'interval' || triggerType === 'cron') return t(triggerType);
    return t('unknownTrigger');
  };

  const load = useCallback(async () => {
    const generation = ++loadGeneration.current;
    try {
      const [userResponse, agentResponse, automationResponse] = await Promise.all([api.me(), api.agents(), api.automations()]);
      if (generation !== loadGeneration.current) return;
      const ownedAgents = agentResponse.filter((agent) => agent.owner_id === userResponse.id);
      setUser(userResponse);
      setAgents(agentResponse);
      setAutomations(automationResponse);
      setSelectedAgentId((current) => current || ownedAgents[0]?.id || '');
      setError(null);
    } catch {
      if (generation === loadGeneration.current) setError('automationLoadFailed');
    }
  }, []);

  useEffect(() => { load(); return () => { loadGeneration.current += 1; }; }, [load]);

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
        const response = await api.automationRuns(selectedId, historyPage, historyPageSize, controller.signal);
        if (!active) return;
        setHistoryRuns(response.items);
        setHistoryTotal(response.total);
        setHistoryError(false);
        setHistoryLoading(false);
        if (response.items.some((run) => ['pending', 'running', 'waiting_tool'].includes(run.status))) {
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

  function restoreAutomationForm(automation: Automation) {
    setSelectedAgentId(automation.agent_id);
    setName(automation.name);
    setTriggerType(automation.trigger_type);
    setPrompt(automation.prompt);
    setSchedule(automation.schedule ?? '');
    setEnabled(automation.enabled);
    setError(null);
    setNotice(null);
  }

  function resetAutomationSelection(automation: Automation) {
    setSelectedId(automation.id);
    restoreAutomationForm(automation);
    setHistoryPage(1);
    setHistoryRuns([]);
    setHistoryTotal(0);
    setSelectedRun(null);
    setCreatedWebhook(null);
  }

  function beginNew() {
    selectionGeneration.current += 1;
    setSelectedId(null);
    setName(t('defaultAutomationName'));
    setTriggerType('manual');
    setPrompt(t('defaultAutomationPrompt'));
    setSchedule('');
    setEnabled(true);
    setHistoryPage(1);
    setSelectedRun(null);
    setCreatedWebhook(null);
    setError(null);
    setNotice(null);
  }

  function selectAutomation(automation: Automation) {
    if (selectedId === automation.id) return;
    selectionGeneration.current += 1;
    resetAutomationSelection(automation);
  }

  function applyTriggeredSelection(automation: Automation, triggerGeneration: number) {
    if (selectionGeneration.current !== triggerGeneration) return;
    selectionGeneration.current += 1;
    resetAutomationSelection(automation);
    setHistoryRefresh((value) => value + 1);
  }

  async function save(event: FormEvent) {
    event.preventDefault();
    if (saving) return;
    setError(null);
    setNotice(null);
    setSaving(true);
    try {
      const editingId = selectedId;
      const automation = editingId
        ? await api.updateAutomation(selectedId, name, triggerType, prompt, schedule, enabled)
        : await api.createAutomation(selectedAgentId, name, triggerType, prompt, schedule, enabled);
      setCreatedWebhook(automation.webhook_token ? { id: automation.id, token: automation.webhook_token } : null);
      setAutomations((current) => {
        const safe = { ...automation, webhook_token: null };
        return current.some((item) => item.id === automation.id)
          ? current.map((item) => item.id === automation.id ? safe : item)
          : [safe, ...current];
      });
      if (editingId) {
        setSelectedId(automation.id);
      } else {
        setSelectedId(null);
      }
      setNotice('changesSaved');
    } catch {
      setError('automationSaveFailed');
    } finally {
      setSaving(false);
    }
  }

  async function trigger(automation: Automation) {
    const triggerGeneration = selectionGeneration.current;
    setError(null);
    try {
      await api.triggerAutomation(automation.id);
    } catch {
      setError('automationRunFailed');
      return;
    }
    applyTriggeredSelection(automation, triggerGeneration);
    try {
      setAutomations(await api.automations());
    } catch {
      setError('automationLoadFailed');
    }
  }

  async function triggerWebhook(automation: Automation) {
    if (!automation.webhook_token) return;
    const triggerGeneration = selectionGeneration.current;
    setError(null);
    try {
      await api.triggerAutomationWebhook(automation.webhook_token);
    } catch {
      setError('automationRunFailed');
      return;
    }
    applyTriggeredSelection(automation, triggerGeneration);
    try {
      setAutomations(await api.automations());
    } catch {
      setError('automationLoadFailed');
    }
  }

  const selected = automations.find((automation) => automation.id === selectedId) ?? null;
  const filtered = automations.filter((automation) => automation.name.toLocaleLowerCase().includes(filter.toLocaleLowerCase()));
  const webhookUrl = `${window.location.origin}/api/automations/webhook`;
  const historyTotalPages = Math.max(1, Math.ceil(historyTotal / historyPageSize));
  const historyPageLabel = t('pageOf').replace('{page}', String(historyPage)).replace('{total}', String(historyTotalPages));

  return (
    <div className="workspace-page">
      <header className="page-header"><div><h1>{t('automations')}</h1><p>{t('automationSubtitle')}</p></div><button className="primary" disabled={saving} onClick={beginNew}><Plus size={16} /> {t('newAutomation')}</button></header>
      <div className="master-detail">
        <section className="master-pane" aria-label={t('list')}>
          <label className="search-field"><span className="sr-only">{t('filterAutomations')}</span><Search size={16} /><input aria-label={t('filterAutomations')} placeholder={t('searchAutomations')} value={filter} onChange={(event) => setFilter(event.target.value)} /></label>
          <div className="compact-list">
            {filtered.map((automation) => {
              const agent = agents.find((item) => item.id === automation.agent_id);
              return <div className={`list-row master-row action-row ${selectedId === automation.id ? 'selected' : ''}`} key={automation.id}>
                <button className="master-select" disabled={saving} onClick={() => selectAutomation(automation)}>
                  <span className="master-row-title"><strong>{automation.name}</strong><span className={`status-dot ${automation.enabled ? 'on' : ''}`} /></span>
                  <span>{agent?.name ?? t('agent')} · {triggerLabel(automation.trigger_type)} · {automation.enabled ? t('enabledStatus') : t('disabledStatus')}</span>
                  <small>{automation.last_triggered_at ? `${t('lastRun')} ${new Date(automation.last_triggered_at).toLocaleString(locale)}` : t('neverTriggered')}</small>
                </button>
                {automation.enabled && automation.trigger_type === 'manual' && <button className="secondary compact-action" disabled={saving} onClick={() => trigger(automation)}>{t('runNow')}</button>}
                {automation.trigger_type === 'webhook' && <code>{webhookUrl}</code>}
                {createdWebhook?.id === automation.id && <><code className="secret-token">{createdWebhook.token}</code>{automation.enabled && <button className="secondary compact-action" disabled={saving} onClick={() => triggerWebhook({ ...automation, webhook_token: createdWebhook.token })}>{t('triggerWebhook')}</button>}</>}
              </div>;
            })}
            {!filtered.length && <div className="compact-empty-state">{t('noAutomations')}</div>}
          </div>
        </section>
        <section className="detail-pane" aria-label={t('details')}>
          <div className="detail-header"><div><span className="eyebrow">{selected ? t('editAutomation') : t('createAutomation')}</span><h2>{selected ? selected.name : t('newAutomation')}</h2></div>{selected && <span className={`status ${selected.enabled ? 'online' : ''}`}>{selected.enabled ? t('enabled') : t('disabled')}</span>}</div>
          <form className="detail-form" onSubmit={save}>
          <label>{t('agent')}<select disabled={Boolean(selectedId) || saving} value={selectedAgentId} onChange={(event) => setSelectedAgentId(event.target.value)}>
            {agents.filter((agent) => !user || agent.owner_id === user.id).map((agent) => <option key={agent.id} value={agent.id}>{agent.name}</option>)}
          </select></label>
          <label>{t('name')}<input disabled={saving} value={name} onChange={(event) => setName(event.target.value)} /></label>
          <label>{t('trigger')}<select disabled={saving} value={triggerType} onChange={(event) => { const next = event.target.value; setTriggerType(next); setSchedule(''); setCreatedWebhook(null); }}>
            <option value="manual">{t('manual')}</option><option value="webhook">{t('webhook')}</option><option value="interval">{t('interval')}</option><option value="cron">{t('cron')}</option>
          </select></label>
          <MarkdownEditor className="form-wide" label={t('prompt')} disabled={saving} value={prompt} onChange={setPrompt} />
          {(triggerType === 'interval' || triggerType === 'cron') && <label className="form-wide">{t('schedule')}<input disabled={saving} value={schedule} onChange={(event) => setSchedule(event.target.value)} placeholder={t('scheduleHint')} /></label>}
          <label className="check-row form-wide"><input aria-label={t('enabled')} disabled={saving} type="checkbox" checked={enabled} onChange={(event) => setEnabled(event.target.checked)} /> {t('enabled')}</label>
          {triggerType === 'webhook' && <div className="webhook-block form-wide"><span>{t('webhookEndpoint')}</span><code>{webhookUrl}</code>{createdWebhook?.id === selectedId && <><span>{t('webhookToken')}</span><code data-testid="webhook-token" className="secret-token">{createdWebhook.token}</code></>}</div>}
          {error && <div className="error form-wide" role="alert">{t(error)} <button type="button" className="text-button" onClick={load}>{t('retry')}</button></div>}
          {notice && <div className="success form-wide" role="status">{t(notice)}</div>}
          <div className="detail-actions form-wide"><button className="primary" disabled={saving || !selectedAgentId}><Save size={16} /> {saving ? t('saving') : selectedId ? t('saveChanges') : t('createAutomationAction')}</button><button className="secondary" type="button" disabled={saving} onClick={() => selected ? restoreAutomationForm(selected) : beginNew()}><RotateCcw size={16} /> {t('discard')}</button>{selected?.enabled && selected.trigger_type === 'manual' && <button className="secondary" type="button" onClick={() => trigger(selected)}><Send size={16} /> {t('runNow')}</button>}{selected?.enabled && selected.trigger_type === 'webhook' && createdWebhook?.id === selected.id && <button className="secondary" type="button" onClick={() => triggerWebhook({ ...selected, webhook_token: createdWebhook.token })}>{t('triggerWebhook')}</button>}</div>
          </form>
        </section>
      </div>
      {selected && <section className="automation-history" role="region" aria-label={t('runHistory')}>
        <div className="automation-history-header"><div className="section-title"><Clock size={18} /> {t('runHistory')}</div></div>
        {historyLoading && historyRuns.length === 0 && <div className="automation-history-state">{t('loadingRunHistory')}</div>}
        {historyError && <div className="automation-history-state error">{t('automationHistoryLoadFailed')} <button type="button" className="text-button" onClick={() => setHistoryRefresh((value) => value + 1)}>{t('retry')}</button></div>}
        {!historyLoading && !historyError && historyRuns.length === 0 && <div className="automation-history-state">{t('noAutomationRuns')}</div>}
        {historyRuns.length > 0 && <div className="automation-history-list">{historyRuns.map((run) => <button type="button" className={`automation-history-row ${selectedRun?.id === run.id ? 'selected' : ''}`} data-run-id={run.id} key={run.id} onClick={() => setSelectedRun(run)}>
          <span><strong>{localizedStatus(run.status, t)}</strong><small>{t('status')}</small></span>
          <span><strong>{run.source === 'integration:tool_result' ? t('runSourceIntegrationToolResult') : run.source}</strong><small>{t('source')}</small></span>
          <span className="automation-history-message"><strong>{run.initial_message}</strong><small>{t('initialMessage')}</small></span>
          <span><strong>{new Date(run.created_at).toLocaleString(locale)}</strong><small>{t('created')}</small></span>
          <span><strong>{new Date(run.updated_at).toLocaleString(locale)}</strong><small>{t('updated')}</small></span>
        </button>)}</div>}
        {historyTotal > historyPageSize && <div className="pagination"><button type="button" className="secondary" disabled={historyPage <= 1 || historyLoading} onClick={() => { setSelectedRun(null); setHistoryPage((value) => Math.max(1, value - 1)); }}>{t('previous')}</button><span>{historyPageLabel}</span><button type="button" className="secondary" disabled={historyPage >= historyTotalPages || historyLoading} onClick={() => { setSelectedRun(null); setHistoryPage((value) => value + 1); }}>{t('next')}</button></div>}
      </section>}
      <section className="console-band">{selectedRun ? <RunConsole run={selectedRun} /> : <div className="empty compact-empty">{t('noAutomationRun')}</div>}</section>
    </div>
  );
}

function WidgetApp({ token }: { token?: string }) {
  const { language, setLanguage, t } = useI18n();
  const [sessionToken, setSessionToken] = useState(token ?? '');
  const [agent, setAgent] = useState<WidgetAgent | null>(null);
  const [message, setMessage] = useState(() => t('defaultWidgetMessage'));
  const [run, setRun] = useState<Run | null>(null);
  const [error, setError] = useState('');
  const [hostOrigin, setHostOrigin] = useState<string | null>(null);
  const [channelId] = useState(() => crypto.randomUUID());
  const widgetRef = useRef<HTMLDivElement>(null);
  const sessionTokenRef = useRef(token ?? '');
  const sessionGeneration = useRef(0);
  const runGeneration = useRef(0);
  const runPendingRef = useRef(false);
  const hostOriginRef = useRef<string | null>(null);
  const [runPending, setRunPending] = useState(false);

  const postWidgetMessage = useCallback((type: string, payload: Record<string, unknown> = {}) => {
    const origin = hostOriginRef.current;
    if (!origin || window.parent === window) return;
    window.parent.postMessage({ type, channelId, ...payload }, origin);
  }, [channelId]);

  const selectSession = useCallback((nextToken: string) => {
    if (nextToken === sessionTokenRef.current) return;
    sessionGeneration.current += 1;
    runGeneration.current += 1;
    runPendingRef.current = false;
    setRunPending(false);
    sessionTokenRef.current = nextToken;
    setSessionToken(nextToken);
    setAgent(null);
    setRun(null);
    setError('');
  }, []);

  const exchangeEmbedJwt = useCallback(async (jwt: string) => {
    // 先使已有 session 和 in-flight run 失效，较晚返回的 exchange 不能覆盖新选择。
    selectSession('');
    const generation = sessionGeneration.current;
    try {
      const response = await api.exchangeEmbedJwt(jwt);
      if (generation !== sessionGeneration.current) return false;
      selectSession(response.token);
      return true;
    } catch (err) {
      if (generation === sessionGeneration.current) {
        setError(t('genericError'));
      }
      return false;
    }
  }, [selectSession]);

  const startWidgetRun = useCallback(async (content: string) => {
    const token = sessionTokenRef.current;
    if (!token || runPendingRef.current) return;
    runPendingRef.current = true;
    setRunPending(true);
    const generation = sessionGeneration.current;
    const requestGeneration = ++runGeneration.current;
    setError('');
    try {
      const createdRun = await api.createWidgetRun(token, content);
      if (generation !== sessionGeneration.current || requestGeneration !== runGeneration.current) return;
      setRun(createdRun);
      postWidgetMessage('agent-hub:run-started', { runId: createdRun.id });
    } catch (err) {
      if (generation !== sessionGeneration.current || requestGeneration !== runGeneration.current) return;
      setError(t('genericError'));
    } finally {
      if (generation === sessionGeneration.current && requestGeneration === runGeneration.current) {
        runPendingRef.current = false;
        setRunPending(false);
      }
    }
  }, [postWidgetMessage]);

  const reportWidgetRunEvent = useCallback((event: RunEvent) => {
    postWidgetMessage('agent-hub:run-event', { runId: event.run_id, event });
  }, [postWidgetMessage]);

  useEffect(() => {
    if (!hostOrigin && window.parent !== window) {
      window.parent.postMessage({ type: 'agent-hub:ready', channelId, protocolVersion: 1 }, '*');
    }
    const onMessage = async (event: MessageEvent) => {
      if (event.source !== window.parent || typeof event.data !== 'object' || event.data === null) return;
      if (event.data.channelId !== channelId || typeof event.data.type !== 'string') return;
      const boundOrigin = hostOriginRef.current;
      if (!boundOrigin) {
        if (event.data.type !== 'agent-hub:init') return;
        hostOriginRef.current = event.origin;
        setHostOrigin(event.origin);
        let sessionReady = false;
        if (typeof event.data.token === 'string') {
          selectSession(event.data.token);
          sessionReady = true;
        }
        if (typeof event.data.jwt === 'string') {
          sessionReady = await exchangeEmbedJwt(event.data.jwt);
        }
        window.parent.postMessage(
          { type: 'agent-hub:ready', channelId, protocolVersion: 1, bound: true, sessionReady },
          event.origin,
        );
        return;
      }
      if (event.origin !== boundOrigin) return;
      if (event.data.type === 'agent-hub:embed-jwt' && typeof event.data.jwt === 'string') {
        await exchangeEmbedJwt(event.data.jwt);
      }
      if (event.data.type === 'agent-hub:session-select' && typeof event.data.token === 'string') {
        selectSession(event.data.token);
      }
      if (event.data.type === 'agent-hub:message-submit') {
        const content = typeof event.data.message === 'string' ? event.data.message : message;
        setMessage(content);
        await startWidgetRun(content);
      }
    };
    window.addEventListener('message', onMessage);
    return () => window.removeEventListener('message', onMessage);
  }, [channelId, exchangeEmbedJwt, message, selectSession, startWidgetRun]);

  useEffect(() => {
    if (!sessionToken) return;
    let cancelled = false;
    api.widgetAgent(sessionToken)
      .then((loaded) => { if (!cancelled) setAgent(loaded); })
      .catch(() => { if (!cancelled) setError(t('genericError')); });
    return () => { cancelled = true; };
  }, [sessionToken, t]);

  useEffect(() => {
    if (!hostOrigin || !widgetRef.current) return;
    const reportSize = () => {
      const width = Math.ceil(widgetRef.current?.getBoundingClientRect().width ?? 0);
      const height = Math.ceil(document.documentElement.scrollHeight);
      postWidgetMessage('agent-hub:resize', { width, height });
    };
    const observer = new ResizeObserver(reportSize);
    observer.observe(widgetRef.current);
    window.addEventListener('resize', reportSize);
    reportSize();
    return () => {
      observer.disconnect();
      window.removeEventListener('resize', reportSize);
    };
  }, [hostOrigin, postWidgetMessage]);

  async function submit(event: FormEvent) {
    event.preventDefault();
    await startWidgetRun(message);
  }

  return (
    <div className="widget" ref={widgetRef}>
      <header><span className="widget-title"><Bot size={18} /> <strong>{agent?.name ?? t('agentWidget')}</strong></span><select className="widget-language" aria-label={t('language')} value={language} onChange={(event) => setLanguage(event.target.value as 'en' | 'zh-CN')}><option value="en">English</option><option value="zh-CN">简体中文</option></select></header>
      <form className="widget-form" onSubmit={submit}>
        <textarea disabled={runPending} value={message} onChange={(event) => setMessage(event.target.value)} />
        <button className="primary" disabled={!sessionToken || runPending}><Send size={16} /> {runPending ? t('sending') : t('send')}</button>
      </form>
      {error && <div className="error">{error}</div>}
      {run && <WidgetRunConsole key={`${run.id}:${sessionToken}`} run={run} token={sessionToken} onEvent={reportWidgetRunEvent} />}
    </div>
  );
}

function WidgetRunConsole({ run, token, onEvent }: { run: Run; token: string; onEvent: (event: RunEvent) => void }) {
  const [events, setEvents] = useState<RunEvent[]>([]);

  useEffect(() => {
    setEvents([]);
    const controller = new AbortController();
    api.streamWidgetRunEvents(run.id, token, controller.signal, (parsed) => {
      setEvents((current) => current.some((item) => item.seq === parsed.seq) ? current : [...current, parsed]);
      onEvent(parsed);
    }).catch((err) => {
      if (!controller.signal.aborted) console.error(err);
    });
    return () => controller.abort();
  }, [onEvent, run.id, token]);

  const assistantComplete = events.some((event) => event.event_type === 'message' && event.role === 'assistant');
  const visibleEvents = assistantComplete ? events.filter((event) => event.event_type !== 'message_delta') : events;
  return <div className="console compact">{visibleEvents.map((event) => event.content && <p key={event.seq}>{event.role ? `${event.role}: ` : ''}{event.content}</p>)}</div>;
}

createRoot(document.getElementById('root')!).render(<I18nProvider><App /></I18nProvider>);
