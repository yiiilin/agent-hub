import { CirclePause, Monitor, Plus, RotateCcw, Search, ShieldAlert, Trash2 } from 'lucide-react';
import { useEffect, useMemo, useState } from 'react';
import type { Agent, HubSession, Runtime, RuntimeEnrollmentToken, User } from './api/client';
import { api } from './api/client';
import { FormDialog } from './components/form-dialog';
import { useI18n } from './i18n';
import type { TranslationKey } from './i18n';
import './runtimes.css';

function localizedStatus(status: string, t: ReturnType<typeof useI18n>['t']) {
  const keys = {
    pending: 'statusPending',
    running: 'statusRunning',
    completed: 'statusCompleted',
    failed: 'statusFailed',
    cancelled: 'statusCancelled',
    waiting_tool: 'statusWaitingTool',
    online: 'statusOnline',
    offline: 'statusOffline'
  } as const;
  return status in keys ? t(keys[status as keyof typeof keys]) : status;
}

function isAvailableEnrollment(enrollment: RuntimeEnrollmentToken, now: number) {
  return !enrollment.consumed_at
    && !enrollment.revoked_at
    && new Date(enrollment.expires_at).getTime() > now;
}

export function RuntimesPage({ user }: { user: User }) {
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
  const [enrollmentClock, setEnrollmentClock] = useState(() => Date.now());
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
        const sorted = [...runtimeResult.value].sort((left, right) =>
          left.hostname.localeCompare(right.hostname) || left.id.localeCompare(right.id));
        setRuntimes(sorted);
        setAgents(agentResult.value);
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
      .then((response) => {
        setEnrollments(response);
        setEnrollmentClock(Date.now());
      })
      .catch(() => { if (!controller.signal.aborted) setAdminError(true); });
    return () => controller.abort();
  }, [isSuperAdmin]);

  useEffect(() => {
    const nextExpiry = enrollments
      .filter((enrollment) => !enrollment.consumed_at && !enrollment.revoked_at)
      .map((enrollment) => new Date(enrollment.expires_at).getTime())
      .filter((expiresAt) => expiresAt > enrollmentClock)
      .sort((left, right) => left - right)[0];
    if (nextExpiry === undefined) return;
    const delay = Math.min(nextExpiry - enrollmentClock + 1, 2_147_483_647);
    const timer = window.setTimeout(() => setEnrollmentClock(Date.now()), delay);
    return () => window.clearTimeout(timer);
  }, [enrollmentClock, enrollments]);

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
  const availableEnrollments = useMemo(() => enrollments
    .filter((enrollment) => isAvailableEnrollment(enrollment, enrollmentClock))
    .sort((left, right) => new Date(right.created_at).getTime() - new Date(left.created_at).getTime()), [enrollmentClock, enrollments]);
  const selectedRuntime = runtimes.find((runtime) => runtime.id === selectedId) ?? null;
  const boundAgents = agents.filter((agent) => agent.runtime_id === selectedRuntime?.id);
  const visibleCapabilities = ['driver', 'codex_source', 'model_proxy', 'mcp_allowlist', 'thread_resume', 'local_skills']
    .flatMap((key) => selectedRuntime && key in selectedRuntime.capabilities
      ? [[key, selectedRuntime.capabilities[key]] as const]
      : []);

  function capabilityValue(value: unknown) {
    if (value === true) return t('enabled');
    if (value === false) return t('disabled');
    return typeof value === 'string' || typeof value === 'number' ? String(value) : t('unavailable');
  }

  function replaceRuntime(updated: Runtime) {
    setRuntimes((current) => current.map((runtime) => runtime.id === updated.id ? updated : runtime));
  }

  function closeEnrollment() {
    setEnrollmentOpen(false);
    setCreatedEnrollment(null);
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
      setEnrollmentClock(Date.now());
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
      setEnrollmentClock(Date.now());
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
    if (boundAgents.length === 0 || !window.confirm(t('confirmDrainRuntime').replace('{hostname}', runtime.hostname))) return;
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

  return (
    <section className={`runtime-workspace${isSuperAdmin ? ' runtime-admin-workspace' : ''}`} aria-labelledby="runtime-page-title">
      <header className="runtime-page-header">
        <div>
          <h1 id="runtime-page-title"><Monitor size={19} /> {t('runtimeNodes')}</h1>
          <p>{t('runtimeSubtitle')}</p>
        </div>
        <div className="runtime-header-actions">
          {!loading && <span className="runtime-count">{counts.all}</span>}
          {isSuperAdmin && <button type="button" className="secondary compact-action" disabled={adminBusy} onClick={() => { setCreatedEnrollment(null); setEnrollmentOpen(true); }}><Plus size={15} /> {t('addRuntimeNode')}</button>}
        </div>
      </header>

      <div className="runtime-feedback">
        {error && <div className="runtime-alert" role="alert"><span>{t('runtimeLoadFailed')}</span><button type="button" onClick={() => { setLoading(true); setRetryGeneration((current) => current + 1); }}>{t('retry')}</button></div>}
        {adminError && <div className="runtime-alert" role="alert"><span>{t('runtimeActionFailed')}</span><button type="button" onClick={() => setAdminError(false)}>{t('close')}</button></div>}
        {adminNotice && <div className="runtime-notice" role="status">{t(adminNotice)}</div>}
        {forceDeleteResult && <div className="runtime-notice force-result"><span>{t('recoverableSessions')}: {forceDeleteResult.recoverable.join(', ') || t('none')}</span><span>{t('recoveryFailedSessions')}: {forceDeleteResult.failed.join(', ') || t('none')}</span></div>}
      </div>

      {isSuperAdmin && <section className="runtime-enrollment-panel" aria-label={t('enrollmentHistory')}>
        <div className="runtime-enrollment-heading">
          <h2>{t('enrollmentHistory')}</h2>
        </div>
        {availableEnrollments.length === 0
          ? <p className="runtime-muted">{t('noEnrollmentTokens')}</p>
          : <div className="runtime-enrollment-list" role="list">
            {availableEnrollments.map((enrollment) => <div className="runtime-enrollment-row" role="listitem" key={enrollment.id}>
              <span className="runtime-enrollment-meta">
                <time dateTime={enrollment.created_at}>{t('created')}: {new Date(enrollment.created_at).toLocaleString(locale)}</time>
                <small>{t('enrollmentExpires')}: {new Date(enrollment.expires_at).toLocaleString(locale)}</small>
              </span>
              <span className="status">{t('enrollmentUnused')}</span>
              <button type="button" className="secondary compact-action" disabled={adminBusy} onClick={() => revokeEnrollment(enrollment.id)}>{t('revokeToken')}</button>
            </div>)}
          </div>}
      </section>}

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
                  {boundAgents.length > 0 ? <div className="runtime-agent-list">{boundAgents.map((agent) => <a key={agent.id} href={`/agents/${agent.id}`}>{agent.name}</a>)}</div> : <p className="runtime-muted">{t('noBoundAgents')}</p>}
                </section>
                {isSuperAdmin && <section className="runtime-detail-section runtime-administration">
                  <h3>{t('runtimeAdministration')}</h3>
                  <div className="runtime-admin-actions">
                    <button type="button" className="secondary" disabled={adminBusy || Boolean(selectedRuntime.credential_rotation_requested_at)} onClick={() => rotateCredential(selectedRuntime)}><RotateCcw size={15} /> {t('rotateCredential')}</button>
                    {selectedRuntime.status === 'draining'
                      ? <button type="button" className="secondary" disabled={adminBusy} onClick={() => cancelDrain(selectedRuntime)}><RotateCcw size={15} /> {t('cancelDrain')}</button>
                      : <button type="button" className="secondary" disabled={adminBusy || boundAgents.length === 0} onClick={() => drain(selectedRuntime)}><CirclePause size={15} /> {t('drainRuntime')}</button>}
                    {selectedRuntime.status === 'draining' && <button type="button" className="secondary danger" disabled={adminBusy} onClick={() => deleteRuntime(selectedRuntime)}><Trash2 size={15} /> {t('deleteRuntime')}</button>}
                    <button type="button" className="secondary danger" disabled={adminBusy} onClick={() => forceDelete(selectedRuntime)}><ShieldAlert size={15} /> {t('forceDeleteRuntime')}</button>
                  </div>
                  {selectedRuntime.credential_rotation_requested_at && <p className="runtime-muted">{t('runtimeRotationPending')}</p>}
                  {affectedSessions.length > 0 && <div className="affected-sessions"><strong>{t('affectedSessions')}</strong>{affectedSessions.map((session) => <a key={session.id} href="/sessions"><span>{session.agent_name}</span><span className={`status ${session.lifecycle_status}`}>{localizedStatus(session.lifecycle_status, t)}</span></a>)}</div>}
                  {selectedRuntime.status === 'draining' && affectedSessions.length === 0 && <p className="runtime-muted">{t('noAffectedSessions')}</p>}
                </section>}
              </div>
            </>
          )}
        </section>
      </div>

      {enrollmentOpen && <FormDialog
        title={t('addRuntimeNode')}
        eyebrow={t('runtimeNodes')}
        onClose={closeEnrollment}
        busy={adminBusy}
        className="runtime-enrollment-dialog"
        footer={createdEnrollment
          ? <button className="primary" type="button" onClick={closeEnrollment}>{t('close')}</button>
          : <><button className="secondary" type="button" disabled={adminBusy} onClick={closeEnrollment}>{t('cancel')}</button><button className="primary" type="button" disabled={adminBusy} onClick={createEnrollment}><Plus size={16} /> {t('createEnrollmentToken')}</button></>}
      >
        <ol className="runtime-enrollment-steps">
          <li><strong>{t('runtimeDeploymentStep')}</strong><code>docker build -f deploy/runtime.Dockerfile -t agent-hub-runtime .</code></li>
          <li><strong>{t('runtimeEnvironmentStep')}</strong><span className="runtime-environment"><code>HUB_URL=https://hub.example.com</code><code>RUNTIME_ENROLLMENT_TOKEN=&lt;token&gt;</code><code>RUNTIME_CREDENTIAL_FILE=/var/lib/agent-hub-runtime/runtime-credential.json</code><code>RUNTIME_WORK_ROOT=/var/lib/agent-hub-runtime</code></span></li>
          <li><strong>{t('runtimeStartStep')}</strong><code>docker run --env-file runtime.env --volume runtime-data:/var/lib/agent-hub-runtime agent-hub-runtime</code></li>
        </ol>
        {createdEnrollment && <div className="secret-result runtime-enrollment-secret"><strong>{t('oneTimeEnrollmentToken')}</strong><span>{t('shownOnce')}</span><code className="secret-token" data-testid="runtime-enrollment-token">{createdEnrollment.token}</code></div>}
        {adminError && <div className="error" role="alert">{t('runtimeActionFailed')}</div>}
      </FormDialog>}
    </section>
  );
}
