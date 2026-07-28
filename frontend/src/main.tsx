import { Activity, ArrowUp, BookOpen, Bot, BrainCircuit, Check, CirclePause, Clock, Copy, ExternalLink, History, KeyRound, Languages, LogOut, Menu, Monitor, Plug, Plus, RotateCcw, Save, Search, Send, Settings, ShieldAlert, Sparkles, Trash2, Workflow, X } from 'lucide-react';
import React, { FormEvent, type TouchEvent, type WheelEvent, useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { Agent, api, ApiError, ApiKey, ApiKeyValidity, Automation, HubSession, HubSessionMessage, Run, RunEvent, Runtime, RuntimeEnrollmentToken, User, WidgetAgent, WidgetHistorySession } from './api/client';
import { I18nProvider, useI18n } from './i18n';
import type { TranslationKey } from './i18n';
import { FormDialog } from './components/form-dialog';
import { MarkdownEditor } from './components/markdown-editor';
import { SkillDetailPage, SkillsPage } from './skills';
import { ChatActivityGroup, ChatMessageBubble, ChatThinkingBubble, mergeRunEvents, projectActivities, resizeComposer, selectSessionMessagePage, sessionMessageRequestLimit, SessionsPage, type ActivityEntry } from './sessions';
import { AdministrationPage } from './administration';
import { AgentPage as AgentDetailPage, AgentsPage } from './agents';
import { IntegrationAppsPage } from './integrations';
import { AutomationsPage as AutomationsWorkspacePage } from './automations';
import { RuntimesPage as RuntimesWorkspacePage } from './runtimes';
import { ModelsPage } from './models';
import { clearConversationDrafts } from './session-drafts';
import './styles.css';

type Route = { name: 'login' } | { name: 'agents' } | { name: 'agent'; agentId: string } | { name: 'sessions' } | { name: 'integrations' } | { name: 'skills' } | { name: 'skill'; skillId: string } | { name: 'models' } | { name: 'apiKeys' } | { name: 'docs' } | { name: 'automations' } | { name: 'runtimes' } | { name: 'administration' } | { name: 'widget'; token?: string; appClientId?: string } | { name: 'notFound' };

function parseRoute(): Route {
  const path = window.location.pathname;
  if (path.startsWith('/widget')) {
    const token = new URLSearchParams(window.location.hash.slice(1)).get('token') ?? undefined;
    if (token) window.history.replaceState(null, '', '/widget');
    const appClientId = token ? undefined : new URLSearchParams(window.location.search).get('app') || undefined;
    return { name: 'widget', token, appClientId };
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

  if (route.name === 'widget') return <WidgetApp token={route.token} appClientId={route.appClientId} />;
  if (loading) return <Shell user={user}><div className="panel">{t('loading')}</div></Shell>;
  if (!user || route.name === 'login') return <LoginPage onLogin={setUser} />;

  return (
    <Shell user={user}>
      {route.name === 'agents' && <AgentsPage currentUser={user} navigate={navigate} />}
      {route.name === 'sessions' && <SessionsPage currentUserId={user.id} />}
      {route.name === 'integrations' && <IntegrationAppsPage currentUser={user} />}
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
  const [mobileNavigationOpen, setMobileNavigationOpen] = useState(false);
  const current = (name: Route['name']) => currentRoute === name || (name === 'skills' && currentRoute === 'skill') ? 'page' as const : undefined;

  useEffect(() => setMobileNavigationOpen(false), [currentRoute]);

  useEffect(() => {
    if (!mobileNavigationOpen) return;
    const closeOnEscape = (event: KeyboardEvent) => {
      if (event.key === 'Escape') setMobileNavigationOpen(false);
    };
    window.addEventListener('keydown', closeOnEscape);
    return () => window.removeEventListener('keydown', closeOnEscape);
  }, [mobileNavigationOpen]);

  function goTo(path: string) {
    setMobileNavigationOpen(false);
    navigate(path);
  }

  async function logout() {
    if (!canNavigate()) return;
    if (user) clearConversationDrafts(user.id);
    await api.logout();
    setNavigationBlocker(null);
    navigate('/login', true);
  }
  return (
    <div className="app-shell">
      <aside className={`sidebar${mobileNavigationOpen ? ' navigation-open' : ''}`} aria-label={t('primaryNavigation')}>
        <div className="sidebar-top">
          <div className="brand"><span className="brand-mark"><Workflow size={18} /></span><span>Agent Hub</span></div>
          <button className="icon-button mobile-nav-toggle" type="button" aria-controls="primary-navigation" aria-expanded={mobileNavigationOpen} aria-label={mobileNavigationOpen ? t('close') : t('primaryNavigation')} onClick={() => setMobileNavigationOpen((open) => !open)}>
            {mobileNavigationOpen ? <X size={18} /> : <Menu size={18} />}
          </button>
        </div>
        <nav id="primary-navigation" className="nav-groups" aria-label={t('primaryNavigation')}>
          <div className="nav-group"><span>{t('workspace')}</span>
            <button className="nav-button" aria-current={current('sessions')} onClick={() => goTo('/sessions')}><Activity size={18} /> {t('sessions')}</button>
            <button className="nav-button" aria-current={current('agents') ?? current('agent')} onClick={() => goTo('/agents')}><Bot size={18} /> {t('agents')}</button>
            <button className="nav-button" aria-current={current('integrations')} onClick={() => goTo('/integrations')}><Plug size={18} /> {t('integrationApps')}</button>
            <button className="nav-button" aria-current={current('automations')} onClick={() => goTo('/automations')}><Clock size={18} /> {t('automations')}</button>
          </div>
          <div className="nav-group"><span>{t('resources')}</span>
            <button className="nav-button" aria-current={current('skills')} onClick={() => goTo('/skills')}><Sparkles size={18} /> {t('skills')}</button>
            <button className="nav-button" aria-current={current('models')} onClick={() => goTo('/models')}><BrainCircuit size={18} /> {t('models')}</button>
            <button className="nav-button" aria-current={current('apiKeys')} onClick={() => goTo('/api-keys')}><KeyRound size={18} /> {t('apiKeys')}</button>
          </div>
          <div className="nav-group"><span>{t('system')}</span>
            <button className="nav-button" aria-current={current('runtimes')} onClick={() => goTo('/runtimes')}><Monitor size={18} /> {t('runtimes')}</button>
            {(user?.role === 'admin' || user?.role === 'super_admin') && <button className="nav-button" aria-current={current('administration')} onClick={() => goTo('/administration')}><Settings size={18} /> {t('administration')}</button>}
            <button className="nav-button" aria-current={current('docs')} onClick={() => goTo('/docs')}><BookOpen size={18} /> {t('apiDocs')}</button>
          </div>
        </nav>
        {mobileNavigationOpen && <button className="navigation-backdrop" type="button" aria-label={t('close')} onClick={() => setMobileNavigationOpen(false)} />}
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
        <div className="login-heading"><div className="login-brand"><span className="brand-mark"><Workflow size={19} /></span><h1>Agent Hub</h1></div><select aria-label={t('language')} value={language} onChange={(event) => setLanguage(event.target.value as 'en' | 'zh-CN')}><option value="en">English</option><option value="zh-CN">简体中文</option></select></div>
        <label>{t('email')}<input aria-label={t('email')} value={email} onChange={(event) => setEmail(event.target.value)} /></label>
        <label>{t('password')}<input aria-label={t('password')} type="password" value={password} onChange={(event) => setPassword(event.target.value)} /></label>
        {error && <div className="error">{t(error)}</div>}
        <div className="login-actions">
          <button className="primary" type="submit">{t('signIn')}</button>
          {oidcMockEnabled && <button className="secondary" type="button" onClick={() => { const params = new URLSearchParams({ email, sub: `mock:${email}` }); window.location.href = `/api/auth/oidc/mock/start?${params}`; }}>{t('signInOidc')}</button>}
        </div>
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
      const haystack = [runtime.hostname, runtime.status, runtime.engine_version, ...runtime.labels].join(' ').toLocaleLowerCase(locale);
      return matchesFilter && (!query || haystack.includes(query));
    });
  }, [filter, locale, runtimes, search]);
  const selectedRuntime = runtimes.find((runtime) => runtime.id === selectedId) ?? null;
  const boundAgents = agents.filter((agent) => agent.runtime_id === selectedRuntime?.id);

  const visibleCapabilities = ['driver', 'engine_source', 'model_proxy', 'mcp_allowlist', 'native_session_resume', 'local_skills']
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
                <span className="runtime-row-meta"><span>{t('lastHeartbeat')}: {new Date(runtime.last_heartbeat_at).toLocaleString(locale)}</span><span>{runtime.engine_version}</span></span>
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
                    <div><dt>{t('engineVersion')}</dt><dd>{selectedRuntime.engine_version}</dd></div>
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

function widgetChannelId() {
  if (typeof crypto.randomUUID === 'function') return crypto.randomUUID();
  // randomUUID is secure-context-only, while development Widgets may use LAN HTTP.
  const bytes = crypto.getRandomValues(new Uint8Array(16));
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = Array.from(bytes, (value) => value.toString(16).padStart(2, '0')).join('');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

type WidgetExchange = {
  id: string;
  message: string;
  runId: string | null;
  streamSessionId: string | null;
  initialEvents: RunEvent[];
  displayRun: boolean;
};

type WidgetSessionTarget = {
  integrationSessionId: string | null;
  hubSessionId: string;
};

function widgetExchangesFromHistory(
  messages: HubSessionMessage[],
  events: RunEvent[],
  accessToken: string,
  target: WidgetSessionTarget,
  laterRunIds = new Set<string>()
) {
  const eventsByRun = new Map<string, RunEvent[]>();
  for (const event of events) {
    const current = eventsByRun.get(event.run_id) ?? [];
    current.push(event);
    eventsByRun.set(event.run_id, current);
  }
  const userMessages = messages.filter((item) => item.role === 'user' && item.content !== null);
  const finalMessageIndexForRun = new Map<string, number>();
  userMessages.forEach((item, index) => {
    if (item.run_id) finalMessageIndexForRun.set(item.run_id, index);
  });
  return userMessages.map((item, index): WidgetExchange => ({
    id: item.id,
    message: item.content ?? '',
    runId: item.run_id,
    streamSessionId: accessToken.startsWith('ahw_')
      ? target.integrationSessionId ?? target.hubSessionId
      : null,
    initialEvents: item.run_id ? mergeRunEvents([], eventsByRun.get(item.run_id) ?? []) : [],
    displayRun: item.run_id !== null
      && finalMessageIndexForRun.get(item.run_id) === index
      && !laterRunIds.has(item.run_id)
  }));
}

type StoredWidgetState = {
  token: string;
  expiresAt?: string;
  historyEnabled: boolean;
  target: WidgetSessionTarget | null;
  draft: string;
  draftClientMessageKey: string | null;
  visitorKey?: string;
};

const widgetStateStorageKey = 'agent-hub-widget-state-v1';

function publicWidgetStateStorageKey(clientId: string) {
  return `agent-hub-public-widget-state-v1:${clientId}`;
}

function sameWidgetSessionTarget(left: WidgetSessionTarget | null, right: WidgetSessionTarget | null) {
  return left === right || (left !== null && right !== null
    && left.integrationSessionId === right.integrationSessionId
    && left.hubSessionId === right.hubSessionId);
}

function loadStoredWidgetState(storageKey: string): StoredWidgetState | null {
  try {
    const parsed = JSON.parse(sessionStorage.getItem(storageKey) ?? 'null') as Record<string, unknown> | null;
    if (!parsed || typeof parsed.token !== 'string' || !parsed.token) return null;
    const candidate = parsed.target;
    const target = candidate && typeof candidate === 'object'
      && (typeof (candidate as Record<string, unknown>).integrationSessionId === 'string' || (candidate as Record<string, unknown>).integrationSessionId === null)
      && typeof (candidate as Record<string, unknown>).hubSessionId === 'string'
      ? {
          integrationSessionId: (candidate as { integrationSessionId: string | null }).integrationSessionId,
          hubSessionId: (candidate as { hubSessionId: string }).hubSessionId
        }
      : null;
    return {
      token: parsed.token,
      expiresAt: typeof parsed.expiresAt === 'string' ? parsed.expiresAt : undefined,
      historyEnabled: parsed.historyEnabled === true,
      target,
      draft: typeof parsed.draft === 'string' ? parsed.draft : '',
      draftClientMessageKey: typeof parsed.draftClientMessageKey === 'string' ? parsed.draftClientMessageKey : null,
      visitorKey: typeof parsed.visitorKey === 'string' && parsed.visitorKey ? parsed.visitorKey : undefined
    };
  } catch {
    return null;
  }
}

function storeWidgetState(storageKey: string, state: StoredWidgetState) {
  try {
    sessionStorage.setItem(storageKey, JSON.stringify(state));
  } catch {
    // The Widget remains usable when storage is unavailable or quota-limited.
  }
}

type WidgetTimelineMessage = { kind: 'message'; id: string; content: string; occurredAt: number; sequence: number };
type WidgetTimelineEntry = WidgetTimelineMessage
  | { kind: 'activity'; activity: ActivityEntry; occurredAt: number; sequence: number };
type WidgetTimelineItem = WidgetTimelineMessage
  | { kind: 'activity-group'; id: string; activities: ActivityEntry[] };

const widgetTerminalStatuses = new Set(['completed', 'failed', 'cancelled', 'interrupted']);

function WidgetApp({ token, appClientId }: { token?: string; appClientId?: string }) {
  const { language, setLanguage, t } = useI18n();
  const publicWidget = Boolean(appClientId && !token);
  const storageKey = publicWidget && appClientId ? publicWidgetStateStorageKey(appClientId) : widgetStateStorageKey;
  const [bootstrap] = useState(() => {
    const stored = loadStoredWidgetState(storageKey);
    const restoresStoredState = publicWidget || !token || stored?.token === token;
    return {
      sessionToken: publicWidget ? stored?.token ?? '' : token ?? stored?.token ?? '',
      expiresAt: restoresStoredState ? stored?.expiresAt : undefined,
      historyEnabled: publicWidget ? false : restoresStoredState ? stored?.historyEnabled ?? false : false,
      target: restoresStoredState ? stored?.target ?? null : null,
      draft: restoresStoredState ? stored?.draft ?? '' : '',
      draftClientMessageKey: restoresStoredState ? stored?.draftClientMessageKey ?? null : null,
      visitorKey: publicWidget ? stored?.visitorKey ?? widgetChannelId() : undefined
    };
  });
  const [sessionToken, setSessionToken] = useState(bootstrap.sessionToken);
  const [credentialExpiresAt, setCredentialExpiresAt] = useState<string | undefined>(bootstrap.expiresAt);
  const [historyEnabled, setHistoryEnabled] = useState(bootstrap.historyEnabled);
  const [selectedSession, setSelectedSession] = useState<WidgetSessionTarget | null>(bootstrap.target);
  const [agent, setAgent] = useState<WidgetAgent | null>(null);
  const [message, setMessage] = useState(bootstrap.draft);
  const [draftClientMessageKey, setDraftClientMessageKey] = useState<string | null>(bootstrap.draftClientMessageKey);
  const [exchanges, setExchanges] = useState<WidgetExchange[]>([]);
  const [pendingMessage, setPendingMessage] = useState<string | null>(null);
  const [error, setError] = useState('');
  const [hostOrigin, setHostOrigin] = useState<string | null>(null);
  const [historyOpen, setHistoryOpen] = useState(false);
  const [historySessions, setHistorySessions] = useState<WidgetHistorySession[]>([]);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [historyError, setHistoryError] = useState('');
  const [transcriptLoading, setTranscriptLoading] = useState(false);
  const [hasOlderWidgetMessages, setHasOlderWidgetMessages] = useState(false);
  const [olderWidgetMessagesLoading, setOlderWidgetMessagesLoading] = useState(false);
  const [credentialEpoch, setCredentialEpoch] = useState(0);
  const [publicAccessReady, setPublicAccessReady] = useState(!publicWidget);
  const [channelId] = useState(widgetChannelId);
  const widgetRef = useRef<HTMLDivElement>(null);
  const chatScrollRef = useRef<HTMLDivElement>(null);
  const widgetFollowBottomRef = useRef(true);
  const widgetHistoryPagingReadyRef = useRef(false);
  const widgetLastScrollTopRef = useRef(0);
  const widgetOlderMessagesLoadingRef = useRef(false);
  const widgetOldestSequenceRef = useRef<number | null>(null);
  const widgetHistoryEventsRef = useRef<RunEvent[]>([]);
  const widgetHistoryAnchorRef = useRef<{ scrollHeight: number; scrollTop: number } | null>(null);
  const widgetTouchStartYRef = useRef<number | null>(null);
  const composerRef = useRef<HTMLTextAreaElement>(null);
  const sessionTokenRef = useRef(bootstrap.sessionToken);
  const selectedSessionRef = useRef<WidgetSessionTarget | null>(bootstrap.target);
  const messageRef = useRef(bootstrap.draft);
  const draftClientMessageKeyRef = useRef<string | null>(bootstrap.draftClientMessageKey);
  const credentialSelectionGeneration = useRef(0);
  const logicalSessionGeneration = useRef(0);
  const exchangeRequestGeneration = useRef(0);
  const equivalentTokensRef = useRef(new Set(bootstrap.sessionToken ? [bootstrap.sessionToken] : []));
  const renewalInFlightRef = useRef(false);
  const renewalPromiseRef = useRef<Promise<unknown> | null>(null);
  const publicAccessInFlightRef = useRef<Promise<unknown> | null>(null);
  const pendingSubmissionRef = useRef<string | null>(null);
  const historyEnabledRef = useRef(bootstrap.historyEnabled);
  const visitorKeyRef = useRef(bootstrap.visitorKey);
  const publicAccessReadyRef = useRef(!publicWidget);
  const runPendingRef = useRef(false);
  const hostOriginRef = useRef<string | null>(null);
  const [runPending, setRunPending] = useState(false);

  const postWidgetMessage = useCallback((type: string, payload: Record<string, unknown> = {}) => {
    const origin = hostOriginRef.current;
    if (!origin || window.parent === window) return;
    window.parent.postMessage({ type, channelId, ...payload }, origin);
  }, [channelId]);

  const scrollChatToBottom = useCallback((force = false) => {
    if (force) widgetFollowBottomRef.current = true;
    requestAnimationFrame(() => {
      const scroll = chatScrollRef.current;
      if (scroll && widgetFollowBottomRef.current) scroll.scrollTop = scroll.scrollHeight;
    });
  }, []);

  const resetWidgetTranscriptPagination = useCallback(() => {
    widgetFollowBottomRef.current = true;
    widgetHistoryPagingReadyRef.current = false;
    widgetLastScrollTopRef.current = 0;
    widgetOlderMessagesLoadingRef.current = false;
    widgetOldestSequenceRef.current = null;
    widgetHistoryEventsRef.current = [];
    widgetHistoryAnchorRef.current = null;
    setHasOlderWidgetMessages(false);
    setOlderWidgetMessagesLoading(false);
  }, []);

  const updateDraft = useCallback((nextMessage: string) => {
    if (nextMessage !== messageRef.current) {
      draftClientMessageKeyRef.current = null;
      setDraftClientMessageKey(null);
    }
    messageRef.current = nextMessage;
    setMessage(nextMessage);
  }, []);

  const clearDraft = useCallback(() => {
    messageRef.current = '';
    draftClientMessageKeyRef.current = null;
    setMessage('');
    setDraftClientMessageKey(null);
  }, []);

  const updateSelectedSession = useCallback((nextTarget: WidgetSessionTarget | null) => {
    selectedSessionRef.current = nextTarget;
    setSelectedSession(nextTarget);
  }, []);

  const rotateCredential = useCallback((expectedToken: string, nextToken: string, expiresAt?: string) => {
    if (!nextToken || sessionTokenRef.current !== expectedToken) return;
    const tokens = new Set(equivalentTokensRef.current);
    tokens.add(expectedToken);
    tokens.add(nextToken);
    equivalentTokensRef.current = tokens;
    sessionTokenRef.current = nextToken;
    setSessionToken(nextToken);
    if (expiresAt) setCredentialExpiresAt(expiresAt);
    setError('');
  }, []);

  const requestPublicWidgetAccess = useCallback(async () => {
    if (!publicWidget || !appClientId || !visitorKeyRef.current) return;
    if (publicAccessInFlightRef.current) return publicAccessInFlightRef.current;
    const expectedToken = sessionTokenRef.current;
    const operation = api.publicWidgetAccess(appClientId, visitorKeyRef.current)
      .then((access) => {
        if (sessionTokenRef.current !== expectedToken) return;
        const expiresAt = Number.isFinite(access.expires_in) && access.expires_in > 0
          ? new Date(Date.now() + access.expires_in * 1_000).toISOString()
          : undefined;
        rotateCredential(expectedToken, access.access_token, expiresAt);
        publicAccessReadyRef.current = true;
        setPublicAccessReady(true);
        setAgent(access.agent);
        historyEnabledRef.current = false;
        setHistoryEnabled(false);
        setHistoryOpen(false);
        setHistorySessions([]);
        if (!selectedSessionRef.current && access.hub_session_id) {
          updateSelectedSession({ integrationSessionId: null, hubSessionId: access.hub_session_id });
        }
      })
      .catch(() => { if (sessionTokenRef.current === expectedToken) setError(t('genericError')); })
      .finally(() => {
        if (publicAccessInFlightRef.current === operation) publicAccessInFlightRef.current = null;
      });
    publicAccessInFlightRef.current = operation;
    return operation;
  }, [appClientId, publicWidget, rotateCredential, t, updateSelectedSession]);

  const selectCredential = useCallback((nextToken: string) => {
    if (!nextToken || equivalentTokensRef.current.has(nextToken)) return false;
    credentialSelectionGeneration.current += 1;
    logicalSessionGeneration.current += 1;
    equivalentTokensRef.current = new Set([nextToken]);
    sessionTokenRef.current = nextToken;
    historyEnabledRef.current = false;
    setSessionToken(nextToken);
    setCredentialExpiresAt(undefined);
    setHistoryEnabled(false);
    setAgent(null);
    setHistoryOpen(false);
    setHistorySessions([]);
    setHistoryError('');
    setTranscriptLoading(false);
    resetWidgetTranscriptPagination();
    updateSelectedSession(null);
    setExchanges([]);
    setPendingMessage(null);
    clearDraft();
    setError('');
    setCredentialEpoch((current) => current + 1);
    return true;
  }, [clearDraft, resetWidgetTranscriptPagination, updateSelectedSession]);

  const exchangeEmbedJwt = useCallback(async (jwt: string) => {
    const requestGeneration = ++exchangeRequestGeneration.current;
    const selectionGeneration = credentialSelectionGeneration.current;
    try {
      const response = await api.exchangeEmbedJwt(jwt);
      if (requestGeneration !== exchangeRequestGeneration.current
        || selectionGeneration !== credentialSelectionGeneration.current) return false;
      selectCredential(response.token);
      return true;
    } catch {
      if (requestGeneration === exchangeRequestGeneration.current
        && selectionGeneration === credentialSelectionGeneration.current) {
        setError(t('genericError'));
      }
      return false;
    }
  }, [selectCredential, t]);

  const loadWidgetTranscript = useCallback(async (
    accessToken: string,
    target: WidgetSessionTarget,
    expectedLogicalGeneration: number
  ) => {
    widgetHistoryPagingReadyRef.current = false;
    setTranscriptLoading(true);
    try {
      const [messageResponse, events] = await Promise.all([
        api.widgetSessionMessagePage(
          accessToken,
          target.integrationSessionId ?? target.hubSessionId,
          { limit: sessionMessageRequestLimit }
        ),
        api.widgetSessionEvents(accessToken, target.integrationSessionId ?? target.hubSessionId)
      ]);
      if (expectedLogicalGeneration !== logicalSessionGeneration.current
        || !sameWidgetSessionTarget(target, selectedSessionRef.current)) return;
      const page = selectSessionMessagePage(messageResponse);
      widgetHistoryEventsRef.current = events;
      widgetOldestSequenceRef.current = page.items[0]?.sequence ?? null;
      setHasOlderWidgetMessages(page.hasMore);
      setExchanges(widgetExchangesFromHistory(page.items, events, accessToken, target));
      scrollChatToBottom(true);
    } catch {
      if (expectedLogicalGeneration === logicalSessionGeneration.current
        && sameWidgetSessionTarget(target, selectedSessionRef.current)) {
        setError(t('genericError'));
      }
    } finally {
      if (expectedLogicalGeneration === logicalSessionGeneration.current
        && sameWidgetSessionTarget(target, selectedSessionRef.current)) {
        setTranscriptLoading(false);
      }
    }
  }, [scrollChatToBottom, t]);

  const loadOlderWidgetMessages = useCallback(async () => {
    const target = selectedSessionRef.current;
    const beforeSequence = widgetOldestSequenceRef.current;
    if (!target || beforeSequence === null || !hasOlderWidgetMessages || widgetOlderMessagesLoadingRef.current) return;
    const logicalGeneration = logicalSessionGeneration.current;
    const accessToken = sessionTokenRef.current;
    widgetOlderMessagesLoadingRef.current = true;
    setOlderWidgetMessagesLoading(true);
    try {
      const response = await api.widgetSessionMessagePage(
        accessToken,
        target.integrationSessionId ?? target.hubSessionId,
        { beforeSequence, limit: sessionMessageRequestLimit }
      );
      if (logicalGeneration !== logicalSessionGeneration.current
        || !sameWidgetSessionTarget(target, selectedSessionRef.current)) return;
      const page = selectSessionMessagePage(response);
      const scroll = chatScrollRef.current;
      if (scroll) {
        widgetHistoryAnchorRef.current = {
          scrollHeight: scroll.scrollHeight,
          scrollTop: scroll.scrollTop
        };
      }
      widgetOldestSequenceRef.current = page.items[0]?.sequence ?? null;
      setHasOlderWidgetMessages(page.hasMore);
      setExchanges((current) => {
        const existingIds = new Set(current.map((exchange) => exchange.id));
        const laterRunIds = new Set(current.flatMap((exchange) => exchange.runId ? [exchange.runId] : []));
        const older = widgetExchangesFromHistory(
          page.items,
          widgetHistoryEventsRef.current,
          accessToken,
          target,
          laterRunIds
        ).filter((exchange) => !existingIds.has(exchange.id));
        return [...older, ...current];
      });
    } catch {
      if (logicalGeneration === logicalSessionGeneration.current) setError(t('genericError'));
    } finally {
      widgetOlderMessagesLoadingRef.current = false;
      if (logicalGeneration === logicalSessionGeneration.current) setOlderWidgetMessagesLoading(false);
    }
  }, [hasOlderWidgetMessages, t]);

  const requestOlderWidgetMessages = useCallback(() => {
    if (!widgetHistoryPagingReadyRef.current || !hasOlderWidgetMessages || transcriptLoading) return;
    widgetFollowBottomRef.current = false;
    void loadOlderWidgetMessages();
  }, [hasOlderWidgetMessages, loadOlderWidgetMessages, transcriptLoading]);

  const handleWidgetChatScroll = useCallback(() => {
    const scroll = chatScrollRef.current;
    if (!scroll) return;
    const scrollingUp = scroll.scrollTop < widgetLastScrollTopRef.current - 1;
    widgetLastScrollTopRef.current = scroll.scrollTop;
    widgetFollowBottomRef.current = scroll.scrollHeight - scroll.clientHeight - scroll.scrollTop <= 24;
    if (widgetHistoryPagingReadyRef.current
      && scrollingUp
      && scroll.scrollTop <= 64) {
      requestOlderWidgetMessages();
    }
  }, [requestOlderWidgetMessages]);

  const handleWidgetChatWheel = useCallback((event: WheelEvent<HTMLDivElement>) => {
    const scroll = chatScrollRef.current;
    if (event.deltaY < 0 && scroll && scroll.scrollTop <= 64) requestOlderWidgetMessages();
  }, [requestOlderWidgetMessages]);

  const handleWidgetChatTouchStart = useCallback((event: TouchEvent<HTMLDivElement>) => {
    widgetTouchStartYRef.current = event.touches[0]?.clientY ?? null;
  }, []);

  const handleWidgetChatTouchEnd = useCallback((event: TouchEvent<HTMLDivElement>) => {
    const startY = widgetTouchStartYRef.current;
    widgetTouchStartYRef.current = null;
    const endY = event.changedTouches[0]?.clientY;
    const scroll = chatScrollRef.current;
    if (startY !== null && endY !== undefined && endY > startY + 12 && scroll && scroll.scrollTop <= 64) {
      requestOlderWidgetMessages();
    }
  }, [requestOlderWidgetMessages]);

  const refreshWidgetHistory = useCallback(async (accessToken: string, expectedCredentialGeneration: number) => {
    setHistoryLoading(true);
    setHistoryError('');
    try {
      const sessions = await api.widgetSessions(accessToken);
      if (expectedCredentialGeneration === credentialSelectionGeneration.current) {
        setHistorySessions(sessions);
      }
    } catch {
      if (expectedCredentialGeneration === credentialSelectionGeneration.current) {
        setHistoryError(t('widgetHistoryLoadFailed'));
      }
    } finally {
      if (expectedCredentialGeneration === credentialSelectionGeneration.current) {
        setHistoryLoading(false);
      }
    }
  }, [t]);

  const selectLogicalSession = useCallback((nextTarget: WidgetSessionTarget | null) => {
    if (sameWidgetSessionTarget(nextTarget, selectedSessionRef.current)) {
      setHistoryOpen(false);
      return;
    }
    logicalSessionGeneration.current += 1;
    const generation = logicalSessionGeneration.current;
    resetWidgetTranscriptPagination();
    updateSelectedSession(nextTarget);
    setExchanges([]);
    setPendingMessage(null);
    setTranscriptLoading(nextTarget !== null);
    clearDraft();
    setError('');
    setHistoryOpen(false);
    if (nextTarget && sessionTokenRef.current.startsWith('ahw_')) {
      void loadWidgetTranscript(sessionTokenRef.current, nextTarget, generation);
    }
  }, [clearDraft, loadWidgetTranscript, resetWidgetTranscriptPagination, updateSelectedSession]);

  const startWidgetRun = useCallback(async (content: string) => {
    if (!sessionTokenRef.current || !content.trim() || runPendingRef.current || (publicWidget && !publicAccessReadyRef.current)) return;
    runPendingRef.current = true;
    setRunPending(true);
    setPendingMessage(content);
    const logicalGeneration = logicalSessionGeneration.current;
    const requestTarget = selectedSessionRef.current;
    const submissionId = widgetChannelId();
    pendingSubmissionRef.current = submissionId;
    let clientMessageKey = widgetChannelId();
    if (messageRef.current === content) {
      clientMessageKey = draftClientMessageKeyRef.current ?? clientMessageKey;
      draftClientMessageKeyRef.current = clientMessageKey;
      setDraftClientMessageKey(clientMessageKey);
    }
    setError('');
    try {
      const pendingRenewal = renewalPromiseRef.current;
      if (pendingRenewal) await pendingRenewal.catch(() => undefined);
      if (logicalGeneration !== logicalSessionGeneration.current) return;
      const accessToken = sessionTokenRef.current;
      const createdRun = await api.createWidgetRun(accessToken, content, {
        ...(requestTarget ? {
          integration_session_id: requestTarget.integrationSessionId,
          hub_session_id: requestTarget.hubSessionId
        } : {}),
        client_message_key: clientMessageKey
      });
      postWidgetMessage('agent-hub:run-started', { runId: createdRun.id });
      if (logicalGeneration !== logicalSessionGeneration.current) return;
      if (createdRun.hub_session_id) {
        updateSelectedSession({
          integrationSessionId: createdRun.integration_session_id,
          hubSessionId: createdRun.hub_session_id
        });
      }
      setExchanges((current) => current.some((exchange) => exchange.runId === createdRun.id)
        ? current
        : [...current, {
            id: `widget-run-${createdRun.id}`,
            message: content,
            runId: createdRun.id,
            streamSessionId: null,
            initialEvents: [],
            displayRun: true
          }]);
      if (messageRef.current === content) clearDraft();
      if (historyEnabledRef.current) {
        void refreshWidgetHistory(sessionTokenRef.current, credentialSelectionGeneration.current);
      }
      scrollChatToBottom();
    } catch {
      if (logicalGeneration === logicalSessionGeneration.current) setError(t('genericError'));
    } finally {
      if (pendingSubmissionRef.current === submissionId) {
        pendingSubmissionRef.current = null;
        runPendingRef.current = false;
        setRunPending(false);
        if (logicalGeneration === logicalSessionGeneration.current) setPendingMessage(null);
      }
    }
  }, [clearDraft, postWidgetMessage, publicWidget, refreshWidgetHistory, scrollChatToBottom, t, updateSelectedSession]);

  const reportWidgetRunEvent = useCallback((event: RunEvent) => {
    widgetHistoryEventsRef.current = mergeRunEvents(widgetHistoryEventsRef.current, [event]);
    postWidgetMessage('agent-hub:run-event', { runId: event.run_id, event });
    scrollChatToBottom();
  }, [postWidgetMessage, scrollChatToBottom]);

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
          selectCredential(event.data.token);
          sessionReady = true;
        }
        if (typeof event.data.jwt === 'string') {
          sessionReady = (await exchangeEmbedJwt(event.data.jwt)) || sessionReady;
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
        selectCredential(event.data.token);
      }
      if (event.data.type === 'agent-hub:message-submit') {
        const content = typeof event.data.message === 'string' ? event.data.message : messageRef.current;
        await startWidgetRun(content);
      }
    };
    window.addEventListener('message', onMessage);
    return () => window.removeEventListener('message', onMessage);
  }, [channelId, exchangeEmbedJwt, selectCredential, startWidgetRun]);

  useEffect(() => {
    if (!publicWidget) return;
    const restoredTarget = selectedSessionRef.current;
    void requestPublicWidgetAccess()?.then(() => {
      if (restoredTarget && sameWidgetSessionTarget(restoredTarget, selectedSessionRef.current)) {
        void loadWidgetTranscript(sessionTokenRef.current, restoredTarget, logicalSessionGeneration.current);
      }
    });
  }, [loadWidgetTranscript, publicWidget, requestPublicWidgetAccess]);

  useEffect(() => {
    if (publicWidget) return;
    const accessToken = sessionTokenRef.current;
    if (!accessToken) return;
    let cancelled = false;
    const expectedCredentialGeneration = credentialSelectionGeneration.current;
    api.widgetAgent(accessToken)
      .then((loaded) => {
        if (cancelled || expectedCredentialGeneration !== credentialSelectionGeneration.current) return;
        setAgent(loaded);
        const externalCredential = accessToken.startsWith('ahw_');
        const nextHistoryEnabled = loaded.history_enabled === true;
        historyEnabledRef.current = nextHistoryEnabled;
        setCredentialExpiresAt(loaded.expires_at);
        setHistoryEnabled(nextHistoryEnabled);
        if (nextHistoryEnabled) {
          void refreshWidgetHistory(sessionTokenRef.current, expectedCredentialGeneration);
        } else {
          setHistorySessions([]);
          setHistoryOpen(false);
        }
        const restoredTarget = selectedSessionRef.current;
        if (externalCredential && restoredTarget) {
          void loadWidgetTranscript(sessionTokenRef.current, restoredTarget, logicalSessionGeneration.current);
        }
      })
      .catch(() => { if (!cancelled && expectedCredentialGeneration === credentialSelectionGeneration.current) setError(t('genericError')); });
    return () => { cancelled = true; };
  }, [credentialEpoch, loadWidgetTranscript, publicWidget, refreshWidgetHistory, t]);

  useEffect(() => {
    if (!sessionToken) return;
    storeWidgetState(storageKey, {
      token: sessionToken,
      expiresAt: credentialExpiresAt,
      historyEnabled,
      target: selectedSession,
      draft: message,
      draftClientMessageKey,
      visitorKey: bootstrap.visitorKey
    });
  }, [bootstrap.visitorKey, credentialExpiresAt, draftClientMessageKey, historyEnabled, message, selectedSession, sessionToken, storageKey]);

  useEffect(() => {
    if (!publicWidget || !credentialExpiresAt) return;
    const expiresAt = Date.parse(credentialExpiresAt);
    if (!Number.isFinite(expiresAt)) return;
    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | null = null;
    const schedule = (delay: number) => { timer = setTimeout(() => { void refresh(); }, delay); };
    const refresh = async () => {
      if (cancelled) return;
      if (runPendingRef.current) {
        schedule(1_000);
        return;
      }
      await requestPublicWidgetAccess();
    };
    schedule(Math.max(0, expiresAt - Date.now() - 60_000));
    return () => { cancelled = true; if (timer) clearTimeout(timer); };
  }, [credentialExpiresAt, publicWidget, requestPublicWidgetAccess]);

  useEffect(() => {
    if (publicWidget || !sessionToken.startsWith('ahw_') || !credentialExpiresAt) return;
    const expiresAt = Date.parse(credentialExpiresAt);
    if (!Number.isFinite(expiresAt)) return;
    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | null = null;
    const schedule = (delay: number) => {
      timer = setTimeout(() => { void renew(); }, delay);
    };
    const renew = async () => {
      if (cancelled || sessionTokenRef.current !== sessionToken) return;
      if (runPendingRef.current) {
        schedule(1_000);
        return;
      }
      if (renewalInFlightRef.current) {
        schedule(1_000);
        return;
      }
      renewalInFlightRef.current = true;
      const operation = api.renewWidgetSession(sessionToken);
      renewalPromiseRef.current = operation;
      try {
        const renewed = await operation;
        if (!cancelled) rotateCredential(sessionToken, renewed.token, renewed.expires_at);
      } catch {
        if (!cancelled && sessionTokenRef.current === sessionToken) {
          setError(t('genericError'));
          schedule(10_000);
        }
      } finally {
        if (renewalPromiseRef.current === operation) renewalPromiseRef.current = null;
        renewalInFlightRef.current = false;
      }
    };
    schedule(Math.max(0, expiresAt - Date.now() - 60_000));
    return () => { cancelled = true; if (timer) clearTimeout(timer); };
  }, [credentialExpiresAt, publicWidget, rotateCredential, sessionToken, t]);

  useEffect(() => {
    resizeComposer(composerRef.current);
  }, [message]);

  useLayoutEffect(() => {
    const scroll = chatScrollRef.current;
    if (!scroll) return;
    const anchor = widgetHistoryAnchorRef.current;
    if (anchor) {
      scroll.scrollTop = anchor.scrollTop + scroll.scrollHeight - anchor.scrollHeight;
      widgetLastScrollTopRef.current = scroll.scrollTop;
      widgetHistoryAnchorRef.current = null;
      widgetHistoryPagingReadyRef.current = true;
      return;
    }
    if (widgetFollowBottomRef.current) scroll.scrollTop = scroll.scrollHeight;
    widgetLastScrollTopRef.current = scroll.scrollTop;
    if (!transcriptLoading) widgetHistoryPagingReadyRef.current = true;
  }, [exchanges, pendingMessage, transcriptLoading]);

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
    <div className="widget session-chat" ref={widgetRef}>
      <header className="session-detail-header session-chat-header widget-header">
        <div className="session-chat-title"><span className="widget-agent-icon" aria-hidden="true"><Bot size={17} /></span><div><h2>{agent?.name ?? t('agentWidget')}</h2><span>{t('hubNative')}</span></div></div>
        <div className="widget-header-actions">
          {historyEnabled && <button type="button" className="icon-button widget-history-toggle" aria-label={t('widgetHistory')} title={t('widgetHistory')} onClick={() => setHistoryOpen((open) => !open)}><History size={17} /></button>}
          <label className="widget-language-control"><Languages size={15} aria-hidden="true" /><span className="sr-only">{t('language')}</span><select className="widget-language" aria-label={t('language')} value={language} onChange={(event) => setLanguage(event.target.value as 'en' | 'zh-CN')}><option value="en">English</option><option value="zh-CN">简体中文</option></select></label>
        </div>
      </header>
      {historyOpen && <aside className="widget-history" role="dialog" aria-label={t('widgetHistory')}>
        <header><strong>{t('widgetHistory')}</strong><button type="button" className="icon-button" aria-label={t('widgetCloseHistory')} title={t('widgetCloseHistory')} onClick={() => setHistoryOpen(false)}><X size={16} /></button></header>
        <button type="button" className="secondary widget-history-new" onClick={() => selectLogicalSession(null)}><Plus size={15} /> {t('newConversation')}</button>
        {historyLoading && <div className="widget-history-state">{t('loadingMessages')}</div>}
        {historyError && <div className="widget-history-state error">{historyError} <button type="button" className="text-button" onClick={() => void refreshWidgetHistory(sessionTokenRef.current, credentialSelectionGeneration.current)}>{t('retry')}</button></div>}
        {!historyLoading && !historyError && historySessions.length === 0 && <div className="widget-history-state">{t('widgetNoHistory')}</div>}
        {!historyLoading && !historyError && historySessions.length > 0 && <div className="widget-history-list">{historySessions.map((item) => {
          const itemTarget = { integrationSessionId: item.id, hubSessionId: item.hub_session_id };
          return <button type="button" key={item.id} className={`widget-history-item ${sameWidgetSessionTarget(itemTarget, selectedSession) ? 'selected' : ''}`} onClick={() => selectLogicalSession(itemTarget)}>
            <span>{item.preview || t('newConversation')}</span><time>{new Date(item.updated_at).toLocaleString(language)}</time>
          </button>;
        })}</div>}
      </aside>}
      <div className="session-chat-scroll" ref={chatScrollRef} onScroll={handleWidgetChatScroll} onWheel={handleWidgetChatWheel} onTouchStart={handleWidgetChatTouchStart} onTouchEnd={handleWidgetChatTouchEnd}>
        {error && <div className="session-banner error" role="alert">{error}</div>}
        <div className="session-transcript widget-transcript" aria-live="polite" aria-busy={transcriptLoading || olderWidgetMessagesLoading}>
          {transcriptLoading && exchanges.length === 0 && <div className="widget-transcript-state">{t('loadingMessages')}</div>}
          {exchanges.map((exchange) => <React.Fragment key={exchange.id}>
            <ChatMessageBubble agentName={agent?.name ?? null} content={exchange.message} role="user" />
            {exchange.displayRun && exchange.runId && <WidgetRunConsole runId={exchange.runId} streamSessionId={exchange.streamSessionId} token={sessionToken} initialEvents={exchange.initialEvents} agentName={agent?.name ?? null} onEvent={reportWidgetRunEvent} />}
          </React.Fragment>)}
          {pendingMessage && <>
            <ChatMessageBubble agentName={agent?.name ?? null} content={pendingMessage} role="user" />
            <ChatThinkingBubble />
          </>}
        </div>
      </div>
      <form className="session-composer session-chat-composer widget-composer" onSubmit={submit}>
        <label><span className="sr-only">{t('message')}</span><textarea ref={composerRef} rows={2} aria-label={t('message')} value={message} onChange={(event) => updateDraft(event.target.value)} onInput={(event) => resizeComposer(event.currentTarget)} onKeyDown={(event) => {
          if (event.key !== 'Enter' || event.shiftKey || event.nativeEvent.isComposing) return;
          event.preventDefault();
          event.currentTarget.form?.requestSubmit();
        }} placeholder={t('messagePlaceholder')} /></label>
        <div><span className="session-composer-actions"><button type="submit" className="icon-button session-send-button" aria-label={runPending ? t('sending') : t('send')} title={t('send')} disabled={!sessionToken || runPending || !message.trim() || !publicAccessReady}><ArrowUp size={18} /></button></span></div>
      </form>
    </div>
  );
}

function WidgetRunConsole({ runId, streamSessionId, token, initialEvents, agentName, onEvent }: { runId: string; streamSessionId: string | null; token: string; initialEvents: RunEvent[]; agentName: string | null; onEvent: (event: RunEvent) => void }) {
  const [events, setEvents] = useState<RunEvent[]>(() => mergeRunEvents([], initialEvents));

  useEffect(() => {
    setEvents(mergeRunEvents([], initialEvents));
  }, [runId]);

  useEffect(() => {
    const controller = new AbortController();
    const abortForPageExit = () => controller.abort();
    window.addEventListener('pagehide', abortForPageExit, { once: true });
    const receiveEvent = (parsed: RunEvent) => {
      if (parsed.run_id !== runId) return;
      setEvents((current) => mergeRunEvents(current, [parsed]));
      onEvent(parsed);
    };
    const stream = streamSessionId
      ? api.streamWidgetSessionEvents(streamSessionId, token, controller.signal, receiveEvent)
      : api.streamWidgetRunEvents(runId, token, controller.signal, receiveEvent);
    stream.catch((err) => {
      if (!controller.signal.aborted) console.error(err);
    });
    return () => {
      window.removeEventListener('pagehide', abortForPageExit);
      controller.abort();
    };
  }, [onEvent, runId, streamSessionId, token]);

  const timeline = useMemo<WidgetTimelineItem[]>(() => {
    const entries: WidgetTimelineEntry[] = projectActivities(events).map((activity) => ({
      kind: 'activity',
      activity,
      occurredAt: activity.occurredAt,
      sequence: activity.sequence
    }));
    const completeMessages = events.filter((event) => event.event_type === 'message' && event.role === 'assistant' && event.content);
    if (completeMessages.length > 0) {
      for (const event of completeMessages) entries.push({
        kind: 'message',
        id: `widget-message-${event.seq}`,
        content: event.content!,
        occurredAt: Date.parse(event.created_at) || 0,
        sequence: event.seq
      });
    } else {
      const deltas = events.filter((event) => event.event_type === 'message_delta' && event.role === 'assistant' && event.content);
      if (deltas.length > 0) entries.push({
        kind: 'message',
        id: 'widget-message-live',
        content: deltas.map((event) => event.content).join(''),
        occurredAt: Date.parse(deltas[0].created_at) || 0,
        sequence: deltas[0].seq
      });
    }
    entries.sort((left, right) => left.occurredAt - right.occurredAt || left.sequence - right.sequence);
    return entries.reduce<WidgetTimelineItem[]>((items, entry) => {
      if (entry.kind === 'message') {
        items.push(entry);
        return items;
      }
      const previous = items.at(-1);
      if (previous?.kind === 'activity-group') previous.activities.push(entry.activity);
      else items.push({ kind: 'activity-group', id: `widget-activity-${entry.activity.id}`, activities: [entry.activity] });
      return items;
    }, []);
  }, [events]);
  const terminal = events.some((event) => {
    if (event.event_type !== 'status') return false;
    const status = event.content ?? (typeof event.payload.status === 'string' ? event.payload.status : null);
    return status !== null && widgetTerminalStatuses.has(status);
  });
  const hasAssistantMessage = timeline.some((entry) => entry.kind === 'message');

  return <>{timeline.map((entry) => entry.kind === 'activity-group'
    ? <ChatActivityGroup activities={entry.activities} key={entry.id} />
    : <ChatMessageBubble agentName={agentName} content={entry.content} key={entry.id} role="assistant" />)}
    {!terminal && !hasAssistantMessage && <ChatThinkingBubble />}
  </>;
}

createRoot(document.getElementById('root')!).render(<I18nProvider><App /></I18nProvider>);
