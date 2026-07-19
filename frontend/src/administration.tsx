import {
  Eye,
  KeyRound,
  PackageCheck,
  Pencil,
  Plus,
  Save,
  Settings,
  ShieldCheck,
  UserX,
  Users
} from 'lucide-react';
import { FormEvent, KeyboardEvent, useEffect, useRef, useState } from 'react';
import {
  api,
  type AdminUserDetail,
  type AuthenticationChannel,
  type AuthPolicy,
  type CodexVersionRollout,
  type ExternalPlatform,
  type User,
  type UserErasure
} from './api/client';
import { FormDialog } from './components/form-dialog';
import { useI18n } from './i18n';
import './administration.css';

const emptyPolicy: AuthPolicy = {
  password_registration_enabled: false,
  password_login_enabled: false,
  email_verification_required: false
};

type AdministrationTab = 'authentication' | 'platforms' | 'users' | 'codex';
type PlatformDialogState = { mode: 'create' } | { mode: 'edit'; platform: ExternalPlatform };
type UserDialogState = { kind: 'details' | 'password' | 'erase'; detail: AdminUserDetail };

const CODEX_ROLLOUT_POLL_INTERVAL_MS = 2_000;

function isCodexRolloutPending(status: string) {
  return status === 'downloading' || status === 'distributing';
}

function Feedback({ error, notice }: { error: boolean; notice: string }) {
  const { t } = useI18n();
  return <>
    {error && <div className="admin-alert error" role="alert">{t('administrationActionFailed')}</div>}
    {notice && <div className="admin-alert success" role="status">{notice}</div>}
  </>;
}

function AuthenticationTab() {
  const { t } = useI18n();
  const [policy, setPolicy] = useState<AuthPolicy>(emptyPolicy);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState(false);
  const [saving, setSaving] = useState(false);
  const [actionError, setActionError] = useState(false);
  const [notice, setNotice] = useState('');

  useEffect(() => {
    const controller = new AbortController();
    api.authPolicy(controller.signal)
      .then((response) => {
        setPolicy(response);
        setLoadError(false);
      })
      .catch(() => { if (!controller.signal.aborted) setLoadError(true); })
      .finally(() => { if (!controller.signal.aborted) setLoading(false); });
    return () => controller.abort();
  }, []);

  async function savePolicy() {
    if (saving) return;
    setSaving(true);
    setActionError(false);
    setNotice('');
    try {
      setPolicy(await api.updateAuthPolicy(policy));
      setNotice(t('authenticationPolicySaved'));
    } catch {
      setActionError(true);
    } finally {
      setSaving(false);
    }
  }

  if (loading) return <div className="panel state-panel" role="status">{t('loadingAdministration')}</div>;
  if (loadError) return <div className="panel state-panel" role="alert"><p>{t('administrationLoadFailed')}</p></div>;

  return <div className="administration-tab-content">
    <Feedback error={actionError} notice={notice} />
    <section className="admin-section administration-policy" aria-labelledby="auth-policy-title">
      <header><ShieldCheck size={18} /><div><h2 id="auth-policy-title">{t('authenticationPolicy')}</h2><p>{t('authenticationPolicyHelp')}</p></div></header>
      <div className="admin-toggle-list">
        <label><input type="checkbox" checked={policy.password_registration_enabled} onChange={(event) => setPolicy((current) => ({ ...current, password_registration_enabled: event.target.checked }))} /> {t('passwordRegistration')}</label>
        <label><input type="checkbox" checked={policy.password_login_enabled} onChange={(event) => setPolicy((current) => ({ ...current, password_login_enabled: event.target.checked }))} /> {t('passwordLogin')}</label>
        <label><input type="checkbox" checked={policy.email_verification_required} onChange={(event) => setPolicy((current) => ({ ...current, email_verification_required: event.target.checked }))} /> {t('emailVerification')}</label>
      </div>
      <button type="button" className="primary admin-save" disabled={saving} onClick={savePolicy}><Save size={15} /> {saving ? t('saving') : t('saveAuthenticationPolicy')}</button>
    </section>
  </div>;
}

function PlatformFormDialog({
  state,
  onClose,
  onSaved
}: {
  state: PlatformDialogState;
  onClose: () => void;
  onSaved: (platform: ExternalPlatform, created: boolean) => void;
}) {
  const { t } = useI18n();
  const platform = state.mode === 'edit' ? state.platform : null;
  const [platformKey, setPlatformKey] = useState(platform?.key ?? '');
  const [platformName, setPlatformName] = useState(platform?.name ?? '');
  const [channels, setChannels] = useState<AuthenticationChannel[]>([]);
  const [selectedChannelId, setSelectedChannelId] = useState<string | null>(null);
  const [channelKey, setChannelKey] = useState('');
  const [channelName, setChannelName] = useState('');
  const [newChannelEnabled, setNewChannelEnabled] = useState(true);
  const [newChannelTrustedEmail, setNewChannelTrustedEmail] = useState(true);
  const [channelsLoading, setChannelsLoading] = useState(Boolean(platform));
  const [platformBusy, setPlatformBusy] = useState(false);
  const [channelBusy, setChannelBusy] = useState(false);
  const [error, setError] = useState(false);

  useEffect(() => {
    if (!platform) return;
    const controller = new AbortController();
    api.authenticationChannels(platform.id, controller.signal)
      .then((response) => {
        setChannels(response);
        setSelectedChannelId(response[0]?.id ?? null);
      })
      .catch(() => { if (!controller.signal.aborted) setError(true); })
      .finally(() => { if (!controller.signal.aborted) setChannelsLoading(false); });
    return () => controller.abort();
  }, [platform]);

  const selectedChannel = channels.find((channel) => channel.id === selectedChannelId) ?? null;
  const busy = platformBusy || channelBusy;

  async function savePlatform() {
    const key = platformKey.trim();
    const name = platformName.trim();
    if (platformBusy || !name || (!platform && !key)) return;
    setPlatformBusy(true);
    setError(false);
    try {
      const saved = platform
        ? await api.updateExternalPlatform(platform.id, name)
        : await api.createExternalPlatform(key, name);
      onSaved(saved, !platform);
    } catch {
      setError(true);
    } finally {
      setPlatformBusy(false);
    }
  }

  async function updateChannel(event: FormEvent) {
    event.preventDefault();
    if (!selectedChannel || channelBusy || !selectedChannel.name.trim()) return;
    setChannelBusy(true);
    setError(false);
    try {
      const updated = await api.updateAuthenticationChannel(selectedChannel.id, {
        name: selectedChannel.name.trim(),
        enabled: selectedChannel.enabled,
        trusted_email: selectedChannel.trusted_email
      });
      setChannels((current) => current.map((channel) => channel.id === updated.id ? updated : channel));
    } catch {
      setError(true);
    } finally {
      setChannelBusy(false);
    }
  }

  async function addChannel(event: FormEvent) {
    event.preventDefault();
    if (!platform || channelBusy || !channelKey.trim() || !channelName.trim()) return;
    setChannelBusy(true);
    setError(false);
    try {
      const created = await api.createAuthenticationChannel(platform.id, {
        key: channelKey.trim(),
        name: channelName.trim(),
        enabled: newChannelEnabled,
        trusted_email: newChannelTrustedEmail
      });
      setChannels((current) => [...current, created]);
      setSelectedChannelId(created.id);
      setChannelKey('');
      setChannelName('');
      setNewChannelEnabled(true);
      setNewChannelTrustedEmail(true);
    } catch {
      setError(true);
    } finally {
      setChannelBusy(false);
    }
  }

  return <FormDialog
    title={platform ? t('editExternalPlatform') : t('addPlatform')}
    eyebrow={platform?.name}
    onClose={onClose}
    busy={busy}
    className="administration-platform-dialog"
    footer={<>
      <button className="secondary" type="button" disabled={busy} onClick={onClose}>{t('cancel')}</button>
      <button className="primary" type="button" disabled={busy || !platformName.trim() || (!platform && !platformKey.trim())} onClick={savePlatform}><Save size={16} /> {platformBusy ? t('saving') : platform ? t('saveChanges') : t('addPlatform')}</button>
    </>}
  >
    <div className="administration-platform-fields">
      <label>{t('platformKey')}<input value={platformKey} disabled={Boolean(platform)} onChange={(event) => setPlatformKey(event.target.value)} /></label>
      <label>{t('platformName')}<input value={platformName} onChange={(event) => setPlatformName(event.target.value)} /></label>
    </div>
    {platform && <section className="administration-channel-section" aria-labelledby="authentication-channels-title">
      <h3 id="authentication-channels-title">{t('authenticationChannels')}</h3>
      {channelsLoading && <p className="runtime-muted" role="status">{t('loading')}</p>}
      {!channelsLoading && channels.length === 0 && <p className="runtime-muted">{t('noAuthenticationChannels')}</p>}
      {!channelsLoading && channels.length > 0 && <div className="administration-channel-picker">
        {channels.map((channel) => <button type="button" key={channel.id} aria-pressed={channel.id === selectedChannelId} onClick={() => setSelectedChannelId(channel.id)}><span>{channel.name}</span><code>{channel.key}</code></button>)}
      </div>}
      {selectedChannel && <form className="administration-channel-form" onSubmit={updateChannel}>
        <label>{t('channelKey')}<input value={selectedChannel.key} disabled /></label>
        <label>{t('channelName')}<input value={selectedChannel.name} onChange={(event) => setChannels((current) => current.map((channel) => channel.id === selectedChannel.id ? { ...channel, name: event.target.value } : channel))} /></label>
        <div className="administration-channel-flags">
          <label className="admin-checkbox"><input type="checkbox" checked={selectedChannel.enabled} onChange={(event) => setChannels((current) => current.map((channel) => channel.id === selectedChannel.id ? { ...channel, enabled: event.target.checked } : channel))} /> {t('channelEnabled')}</label>
          <label className="admin-checkbox"><input type="checkbox" checked={selectedChannel.trusted_email} onChange={(event) => setChannels((current) => current.map((channel) => channel.id === selectedChannel.id ? { ...channel, trusted_email: event.target.checked } : channel))} /> {t('trustedEmail')}</label>
        </div>
        <button className="secondary" disabled={busy || !selectedChannel.name.trim()}><Save size={15} /> {t('saveChannel')}</button>
      </form>}
      <form className="administration-new-channel-form" onSubmit={addChannel}>
        <label>{t('channelKey')}<input value={channelKey} onChange={(event) => setChannelKey(event.target.value)} /></label>
        <label>{t('newChannelName')}<input value={channelName} onChange={(event) => setChannelName(event.target.value)} /></label>
        <div className="administration-channel-flags">
          <label className="admin-checkbox"><input type="checkbox" checked={newChannelEnabled} onChange={(event) => setNewChannelEnabled(event.target.checked)} /> {t('enableNewChannel')}</label>
          <label className="admin-checkbox"><input type="checkbox" checked={newChannelTrustedEmail} onChange={(event) => setNewChannelTrustedEmail(event.target.checked)} /> {t('trustEmailForNewChannel')}</label>
        </div>
        <button className="secondary" disabled={busy || !channelKey.trim() || !channelName.trim()}><Plus size={15} /> {t('addChannel')}</button>
      </form>
    </section>}
    {error && <div className="admin-alert error" role="alert">{t('administrationActionFailed')}</div>}
  </FormDialog>;
}

function ExternalPlatformsTab() {
  const { t } = useI18n();
  const [platforms, setPlatforms] = useState<ExternalPlatform[]>([]);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState(false);
  const [dialog, setDialog] = useState<PlatformDialogState | null>(null);
  const [notice, setNotice] = useState('');

  useEffect(() => {
    const controller = new AbortController();
    api.externalPlatforms(controller.signal)
      .then((response) => {
        setPlatforms(response);
        setLoadError(false);
      })
      .catch(() => { if (!controller.signal.aborted) setLoadError(true); })
      .finally(() => { if (!controller.signal.aborted) setLoading(false); });
    return () => controller.abort();
  }, []);

  function saved(platform: ExternalPlatform, created: boolean) {
    setPlatforms((current) => created
      ? [...current, platform]
      : current.map((item) => item.id === platform.id ? platform : item));
    setNotice(created ? t('externalPlatformAdded') : t('changesSaved'));
    setDialog(null);
  }

  if (loading) return <div className="panel state-panel" role="status">{t('loadingAdministration')}</div>;
  if (loadError) return <div className="panel state-panel" role="alert"><p>{t('administrationLoadFailed')}</p></div>;

  return <div className="administration-tab-content">
    <Feedback error={false} notice={notice} />
    <section className="admin-section administration-list-section" aria-labelledby="external-platforms-title">
      <header>
        <ShieldCheck size={18} />
        <div><h2 id="external-platforms-title">{t('externalPlatforms')}</h2><p>{t('externalPlatformsHelp')}</p></div>
        <button className="primary administration-header-action" type="button" onClick={() => setDialog({ mode: 'create' })}><Plus size={16} /> {t('addPlatform')}</button>
      </header>
      <div className="administration-table-wrap">
        <table className="administration-table administration-platform-table" aria-label={t('externalPlatforms')}>
          <thead><tr><th>{t('platformName')}</th><th>{t('platformKey')}</th><th>{t('actions')}</th></tr></thead>
          <tbody>{platforms.length === 0 ? <tr><td colSpan={3}>{t('noExternalPlatforms')}</td></tr> : platforms.map((platform) => <tr key={platform.id}>
            <td><strong>{platform.name}</strong></td>
            <td><code>{platform.key}</code></td>
            <td><button className="icon-button administration-table-action" type="button" aria-label={`${t('editExternalPlatform')}: ${platform.name}`} title={t('editExternalPlatform')} onClick={() => setDialog({ mode: 'edit', platform })}><Pencil size={16} /></button></td>
          </tr>)}</tbody>
        </table>
      </div>
    </section>
    {dialog && <PlatformFormDialog state={dialog} onClose={() => setDialog(null)} onSaved={saved} />}
  </div>;
}

function UserDetailsDialog({ detail, onClose }: { detail: AdminUserDetail; onClose: () => void }) {
  const { locale, t } = useI18n();
  const [loaded, setLoaded] = useState<AdminUserDetail | null>(null);
  const [error, setError] = useState(false);

  useEffect(() => {
    const controller = new AbortController();
    api.adminUser(detail.user.id, controller.signal)
      .then(setLoaded)
      .catch(() => { if (!controller.signal.aborted) setError(true); });
    return () => controller.abort();
  }, [detail.user.id]);

  const current = loaded ?? detail;
  return <FormDialog title={t('userInformation')} eyebrow={detail.user.username} onClose={onClose} footer={<button className="primary" type="button" onClick={onClose}>{t('close')}</button>}>
    {!loaded && !error && <p className="runtime-muted" role="status">{t('loading')}</p>}
    {error && <div className="admin-alert error" role="alert">{t('administrationLoadFailed')}</div>}
    {!error && <dl className="administration-user-details">
      <div><dt>{t('name')}</dt><dd>{current.user.display_name}</dd></div>
      <div><dt>{t('email')}</dt><dd>{current.user.email ?? t('unavailable')}</dd></div>
      <div><dt>{t('userRole')}</dt><dd>{current.user.role}</dd></div>
      <div><dt>{t('emailVerification')}</dt><dd>{current.email_verified ? t('enabled') : t('disabled')}</dd></div>
      <div><dt>{t('password')}</dt><dd>{current.has_password ? t('enabled') : t('disabled')}</dd></div>
      <div><dt>{t('created')}</dt><dd>{new Date(current.created_at).toLocaleString(locale)}</dd></div>
    </dl>}
  </FormDialog>;
}

function UserPasswordDialog({
  detail,
  onClose,
  onSaved
}: {
  detail: AdminUserDetail;
  onClose: () => void;
  onSaved: (detail: AdminUserDetail) => void;
}) {
  const { t } = useI18n();
  const [password, setPassword] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState(false);

  async function submit(event: FormEvent) {
    event.preventDefault();
    if (busy || password.length < 8) return;
    setBusy(true);
    setError(false);
    try {
      onSaved(await api.setAdminUserPassword(detail.user.id, password));
    } catch {
      setError(true);
    } finally {
      setBusy(false);
    }
  }

  return <FormDialog
    title={t('setUserPassword')}
    eyebrow={detail.user.username}
    onClose={onClose}
    busy={busy}
    footer={<>
      <button className="secondary" type="button" disabled={busy} onClick={onClose}>{t('cancel')}</button>
      <button className="primary" type="submit" form="administration-password-form" disabled={busy || password.length < 8}><KeyRound size={16} /> {busy ? t('saving') : t('saveChanges')}</button>
    </>}
  >
    <form id="administration-password-form" className="administration-dialog-form" onSubmit={submit}>
      <label>{t('password')}<input type="password" minLength={8} maxLength={1024} value={password} onChange={(event) => setPassword(event.target.value)} /></label>
      {error && <div className="admin-alert error" role="alert">{t('administrationActionFailed')}</div>}
    </form>
  </FormDialog>;
}

function UserEraseDialog({
  detail,
  onClose,
  onErased
}: {
  detail: AdminUserDetail;
  onClose: () => void;
  onErased: (erasure: UserErasure) => void;
}) {
  const { t } = useI18n();
  const [confirmation, setConfirmation] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState(false);
  const confirmed = confirmation === detail.user.username;

  async function submit(event: FormEvent) {
    event.preventDefault();
    if (busy || !confirmed) return;
    setBusy(true);
    setError(false);
    try {
      onErased(await api.eraseUser(detail.user.id, detail.user.username));
    } catch {
      setError(true);
    } finally {
      setBusy(false);
    }
  }

  return <FormDialog
    title={t('eraseUser')}
    eyebrow={detail.user.username}
    onClose={onClose}
    busy={busy}
    footer={<>
      <button className="secondary" type="button" disabled={busy} onClick={onClose}>{t('cancel')}</button>
      <button className="primary administration-danger-action" type="submit" form="administration-erasure-form" disabled={busy || !confirmed}><UserX size={16} /> {busy ? t('deleting') : t('eraseUser')}</button>
    </>}
  >
    <form id="administration-erasure-form" className="administration-dialog-form" onSubmit={submit}>
      <p className="administration-erasure-warning">{t('userErasureHelp')}</p>
      <label>{t('confirmUsername')}<input value={confirmation} placeholder={detail.user.username} onChange={(event) => setConfirmation(event.target.value)} /></label>
      {error && <div className="admin-alert error" role="alert">{t('administrationActionFailed')}</div>}
    </form>
  </FormDialog>;
}

function UsersTab({ currentUser }: { currentUser: User }) {
  const { locale, t } = useI18n();
  const [users, setUsers] = useState<AdminUserDetail[]>([]);
  const [erasures, setErasures] = useState<UserErasure[]>([]);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState(false);
  const [dialog, setDialog] = useState<UserDialogState | null>(null);
  const [notice, setNotice] = useState('');

  useEffect(() => {
    const controller = new AbortController();
    Promise.all([api.adminUsers(controller.signal), api.userErasures(controller.signal)])
      .then(([loadedUsers, loadedErasures]) => {
        setUsers(loadedUsers);
        setErasures(loadedErasures);
        setLoadError(false);
      })
      .catch(() => { if (!controller.signal.aborted) setLoadError(true); })
      .finally(() => { if (!controller.signal.aborted) setLoading(false); });
    return () => controller.abort();
  }, []);

  function passwordSaved(updated: AdminUserDetail) {
    setUsers((current) => current.map((item) => item.user.id === updated.user.id ? updated : item));
    setNotice(t('changesSaved'));
    setDialog(null);
  }

  function erased(erasure: UserErasure) {
    setUsers((current) => current.filter((item) => item.user.id !== erasure.user_id));
    setErasures((current) => [erasure, ...current.filter((item) => item.user_id !== erasure.user_id)]);
    setNotice(t('userErasureStarted'));
    setDialog(null);
  }

  if (loading) return <div className="panel state-panel" role="status">{t('loadingUsers')}</div>;
  if (loadError) return <div className="panel state-panel" role="alert"><p>{t('usersLoadFailed')}</p></div>;

  return <div className="administration-tab-content">
    <Feedback error={false} notice={notice} />
    <section className="admin-section administration-list-section" aria-labelledby="user-management-title">
      <header><Users size={18} /><div><h2 id="user-management-title">{t('userManagement')}</h2><p>{t('userManagementHelp')}</p></div></header>
      <div className="administration-table-wrap">
        <table className="administration-table administration-users-table" aria-label={t('userManagement')}>
          <thead><tr><th>{t('name')}</th><th>{t('email')}</th><th>{t('userRole')}</th><th>{t('actions')}</th></tr></thead>
          <tbody>{users.length === 0 ? <tr><td colSpan={4}>{t('noUsers')}</td></tr> : users.map((detail) => {
            const user = detail.user;
            const isCurrent = user.id === currentUser.id;
            return <tr key={user.id}>
              <td><span className="administration-user-identity"><strong>{user.username}</strong><small>{user.display_name}</small></span></td>
              <td>{user.email ?? t('unavailable')}</td>
              <td><span className="status">{user.role}</span></td>
              <td><div className="administration-table-actions">
                <button className="icon-button administration-table-action" type="button" aria-label={`${t('userInformation')}: ${user.username}`} title={t('userInformation')} onClick={() => setDialog({ kind: 'details', detail })}><Eye size={16} /></button>
                <button className="icon-button administration-table-action" type="button" aria-label={`${t('setUserPassword')}: ${user.username}`} title={t('setUserPassword')} onClick={() => setDialog({ kind: 'password', detail })}><KeyRound size={16} /></button>
                <button className="icon-button administration-table-action danger" type="button" disabled={isCurrent} aria-label={`${t('eraseUser')}: ${user.username}`} title={isCurrent ? t('cannotDeleteCurrentUser') : t('eraseUser')} onClick={() => { if (!isCurrent) setDialog({ kind: 'erase', detail }); }}><UserX size={16} /></button>
              </div></td>
            </tr>;
          })}</tbody>
        </table>
      </div>
      {erasures.length > 0 && <div className="erasure-history"><h3>{t('erasureHistory')}</h3>{erasures.map((erasure) => <div key={erasure.user_id}><code>{erasure.username ?? erasure.user_id}</code><span className={`status ${erasure.status}`}>{erasure.status}</span><time>{new Date(erasure.completed_at ?? erasure.requested_at).toLocaleString(locale)}</time></div>)}</div>}
    </section>
    {dialog?.kind === 'details' && <UserDetailsDialog detail={dialog.detail} onClose={() => setDialog(null)} />}
    {dialog?.kind === 'password' && <UserPasswordDialog detail={dialog.detail} onClose={() => setDialog(null)} onSaved={passwordSaved} />}
    {dialog?.kind === 'erase' && <UserEraseDialog detail={dialog.detail} onClose={() => setDialog(null)} onErased={erased} />}
  </div>;
}

function CodexVersionTab() {
  const { t } = useI18n();
  const [rollout, setRollout] = useState<CodexVersionRollout | null>(null);
  const [targetVersion, setTargetVersion] = useState('');
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState(false);
  const [busy, setBusy] = useState(false);
  const [actionError, setActionError] = useState(false);
  const [notice, setNotice] = useState('');
  const [pollStartGeneration, setPollStartGeneration] = useState(0);
  const rolloutGeneration = useRef(0);
  const pollController = useRef<AbortController | null>(null);

  useEffect(() => {
    const controller = new AbortController();
    api.codexVersionRollout(controller.signal)
      .then((response) => {
        setRollout(response);
        setLoadError(false);
      })
      .catch(() => { if (!controller.signal.aborted) setLoadError(true); })
      .finally(() => { if (!controller.signal.aborted) setLoading(false); });
    return () => {
      rolloutGeneration.current += 1;
      pollController.current?.abort();
      controller.abort();
    };
  }, []);

  useEffect(() => {
    if (!rollout || !isCodexRolloutPending(rollout.status)) return;
    const generation = rolloutGeneration.current;
    let active = true;
    let timer: number | undefined;

    const poll = async () => {
      if (!active || generation !== rolloutGeneration.current) return;
      const controller = new AbortController();
      pollController.current = controller;
      try {
        const response = await api.codexVersionRollout(controller.signal);
        if (!active || controller.signal.aborted || generation !== rolloutGeneration.current) return;
        setActionError(false);
        setRollout(response);
        if (isCodexRolloutPending(response.status)) {
          timer = window.setTimeout(poll, CODEX_ROLLOUT_POLL_INTERVAL_MS);
        }
      } catch {
        if (active && !controller.signal.aborted && generation === rolloutGeneration.current) {
          setActionError(true);
          timer = window.setTimeout(poll, CODEX_ROLLOUT_POLL_INTERVAL_MS);
        }
      } finally {
        if (pollController.current === controller) pollController.current = null;
      }
    };

    timer = window.setTimeout(poll, CODEX_ROLLOUT_POLL_INTERVAL_MS);
    return () => {
      active = false;
      if (timer !== undefined) window.clearTimeout(timer);
      pollController.current?.abort();
    };
  }, [pollStartGeneration, rollout?.status, rollout?.target_version]);

  async function prepareVersion(event: FormEvent) {
    event.preventDefault();
    const version = targetVersion.trim();
    if (busy || !version) return;
    setBusy(true);
    setActionError(false);
    setNotice('');
    const generation = rolloutGeneration.current + 1;
    rolloutGeneration.current = generation;
    pollController.current?.abort();
    try {
      const response = await api.setCodexTargetVersion(version);
      if (generation !== rolloutGeneration.current) return;
      setRollout(response);
      setTargetVersion('');
      setNotice(t('codexVersionPrepared'));
    } catch {
      if (generation === rolloutGeneration.current) setActionError(true);
    } finally {
      if (generation === rolloutGeneration.current) {
        setPollStartGeneration(generation);
        setBusy(false);
      }
    }
  }

  async function promoteVersion() {
    if (busy) return;
    setBusy(true);
    setActionError(false);
    setNotice('');
    const generation = rolloutGeneration.current + 1;
    rolloutGeneration.current = generation;
    pollController.current?.abort();
    try {
      const response = await api.promoteCodexTargetVersion();
      if (generation !== rolloutGeneration.current) return;
      setRollout(response);
      setNotice(t('codexVersionPromoted'));
    } catch {
      if (generation === rolloutGeneration.current) setActionError(true);
    } finally {
      if (generation === rolloutGeneration.current) {
        setPollStartGeneration(generation);
        setBusy(false);
      }
    }
  }

  if (loading) return <div className="panel state-panel" role="status">{t('loadingAdministration')}</div>;
  if (loadError) return <div className="panel state-panel" role="alert"><p>{t('administrationLoadFailed')}</p></div>;

  return <div className="administration-tab-content">
    <Feedback error={actionError} notice={notice} />
    <section className="admin-section administration-codex" aria-labelledby="codex-rollout-title">
      <header><PackageCheck size={18} /><div><h2 id="codex-rollout-title">{t('codexRollout')}</h2><p>{t('codexRolloutHelp')}</p></div></header>
      {rollout && <dl className="admin-summary">
        <div><dt>{t('activeVersion')}</dt><dd>{rollout.active_version ?? t('none')}</dd></div>
        <div><dt>{t('targetVersion')}</dt><dd>{rollout.target_version ?? t('none')}</dd></div>
        <div><dt>{t('status')}</dt><dd><span className={`status ${rollout.status}`}>{rollout.status}</span></dd></div>
      </dl>}
      {rollout?.error && <p className="error">{rollout.error}</p>}
      <form className="admin-inline-form" onSubmit={prepareVersion}>
        <label>{t('targetCodexVersion')}<input value={targetVersion} onChange={(event) => setTargetVersion(event.target.value)} /></label>
        <button className="secondary" disabled={busy || !targetVersion.trim()}><PackageCheck size={15} /> {busy ? t('saving') : t('prepareVersion')}</button>
      </form>
      {rollout?.target_version && rollout.status === 'ready' && <button type="button" className="primary admin-save" disabled={busy} onClick={promoteVersion}>{t('promoteReadyVersion')}</button>}
      {rollout && rollout.runtimes.length > 0 && <div className="rollout-runtime-list"><strong>{t('runtimeReadiness')}</strong>{rollout.runtimes.map((runtime) => <div key={runtime.runtime_id}><span><b>{runtime.hostname}</b><small>{runtime.os}/{runtime.architecture}</small></span><code>{runtime.current_version}</code><span className={`status ${runtime.status}`}>{runtime.status}</span></div>)}</div>}
    </section>
  </div>;
}

export function AdministrationPage({ currentUser }: { currentUser: User }) {
  const { t } = useI18n();
  const [activeTab, setActiveTab] = useState<AdministrationTab>('authentication');
  const tabs = [
    { id: 'authentication' as const, label: t('authentication'), icon: ShieldCheck },
    { id: 'platforms' as const, label: t('externalPlatforms'), icon: Settings },
    { id: 'users' as const, label: t('userManagement'), icon: Users },
    { id: 'codex' as const, label: t('codexVersion'), icon: PackageCheck }
  ];

  function handleTabKeyDown(event: KeyboardEvent<HTMLButtonElement>, index: number) {
    if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) return;
    event.preventDefault();
    const nextIndex = event.key === 'Home'
      ? 0
      : event.key === 'End'
        ? tabs.length - 1
        : (index + (event.key === 'ArrowRight' ? 1 : -1) + tabs.length) % tabs.length;
    setActiveTab(tabs[nextIndex].id);
    document.getElementById(`administration-tab-${tabs[nextIndex].id}`)?.focus();
  }

  return <div className="administration-page" aria-labelledby="administration-title">
    <header className="page-header administration-header">
      <div><h1 id="administration-title"><Settings size={21} /> {t('administration')}</h1><p>{t('administrationSubtitle')}</p></div>
    </header>
    <div className="administration-tabs" role="tablist" aria-label={t('administration')}>
      {tabs.map((tab, index) => {
        const Icon = tab.icon;
        const selected = tab.id === activeTab;
        return <button
          id={`administration-tab-${tab.id}`}
          key={tab.id}
          type="button"
          role="tab"
          aria-selected={selected}
          aria-controls={`administration-panel-${tab.id}`}
          tabIndex={selected ? 0 : -1}
          onClick={() => setActiveTab(tab.id)}
          onKeyDown={(event) => handleTabKeyDown(event, index)}
        ><Icon size={16} /> <span>{tab.label}</span></button>;
      })}
    </div>
    <div id={`administration-panel-${activeTab}`} className="administration-tab-panel" role="tabpanel" aria-labelledby={`administration-tab-${activeTab}`}>
      {activeTab === 'authentication' && <AuthenticationTab />}
      {activeTab === 'platforms' && <ExternalPlatformsTab />}
      {activeTab === 'users' && <UsersTab currentUser={currentUser} />}
      {activeTab === 'codex' && <CodexVersionTab />}
    </div>
  </div>;
}
