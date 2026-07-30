import {
  AlertTriangle,
  BadgePlus,
  Building2,
  Eye,
  FlaskConical,
  KeyRound,
  Pencil,
  Plus,
  Save,
  Settings,
  Shield,
  ShieldCheck,
  UserX,
  Users,
  X
} from 'lucide-react';
import { FormEvent, KeyboardEvent, useEffect, useState } from 'react';
import {
  ApiError,
  api,
  type AdminUserDetail,
  type AuthenticationChannel,
  type AuthPolicy,
  type ExternalPlatform,
  type LdapConfiguration,
  type LdapTestResult,
  type User,
  type UserErasure,
  type UserRole
} from './api/client';
import { FormDialog } from './components/form-dialog';
import { useI18n } from './i18n';
import './administration.css';

const emptyPolicy: AuthPolicy = {
  password_registration_enabled: false,
  password_login_enabled: false,
  ldap_login_enabled: false
};

const emptyLdapConfiguration: LdapConfiguration = {
  url: '',
  security: 'starttls',
  base_dn: '',
  bind_identity_template: '{email}',
  user_filter: '(userPrincipalName={email})',
  email_attribute: 'mail',
  display_name_attribute: 'displayName',
  allow_insecure: false,
  skip_tls_verify: false
};

type AdministrationTab = 'authentication' | 'platforms' | 'users';
type PlatformDialogState = { mode: 'create' } | { mode: 'edit'; platform: ExternalPlatform };
type UserDialogState =
  | { kind: 'create' }
  | { kind: 'details' | 'edit' | 'password' | 'role' | 'erase'; detail: AdminUserDetail };

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
  const [ldapConfiguration, setLdapConfiguration] = useState<LdapConfiguration>(emptyLdapConfiguration);
  const [ldapConfigured, setLdapConfigured] = useState(false);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState(false);
  const [policySaving, setPolicySaving] = useState(false);
  const [configurationSaving, setConfigurationSaving] = useState(false);
  const [testEmail, setTestEmail] = useState('');
  const [testPassword, setTestPassword] = useState('');
  const [testBusy, setTestBusy] = useState(false);
  const [testError, setTestError] = useState<string | null>(null);
  const [testResult, setTestResult] = useState<LdapTestResult | null>(null);
  const [actionError, setActionError] = useState(false);
  const [notice, setNotice] = useState('');

  useEffect(() => {
    const controller = new AbortController();
    Promise.all([api.authPolicy(controller.signal), api.ldapConfiguration(controller.signal)])
      .then(([loadedPolicy, loadedConfiguration]) => {
        setPolicy(loadedPolicy);
        if (loadedConfiguration) {
          setLdapConfiguration(loadedConfiguration);
          setLdapConfigured(true);
        }
        setLoadError(false);
      })
      .catch(() => { if (!controller.signal.aborted) setLoadError(true); })
      .finally(() => { if (!controller.signal.aborted) setLoading(false); });
    return () => controller.abort();
  }, []);

  async function savePolicy() {
    if (policySaving) return;
    setPolicySaving(true);
    setActionError(false);
    setNotice('');
    try {
      setPolicy(await api.updateAuthPolicy(policy));
      setNotice(t('authenticationPolicySaved'));
    } catch {
      setActionError(true);
    } finally {
      setPolicySaving(false);
    }
  }

  function updateLdap<K extends keyof LdapConfiguration>(key: K, value: LdapConfiguration[K]) {
    setLdapConfiguration((current) => ({ ...current, [key]: value }));
    setTestResult(null);
    setTestError(null);
  }

  function updateSecurity(security: LdapConfiguration['security']) {
    setLdapConfiguration((current) => ({
      ...current,
      security,
      allow_insecure: security === 'plain' ? current.allow_insecure : false,
      skip_tls_verify: security === 'plain' ? false : current.skip_tls_verify
    }));
    setTestResult(null);
    setTestError(null);
  }

  const configurationComplete = Boolean(
    ldapConfiguration.url.trim()
    && ldapConfiguration.base_dn.trim()
    && ldapConfiguration.bind_identity_template.trim()
    && ldapConfiguration.user_filter.trim()
    && ldapConfiguration.email_attribute.trim()
    && ldapConfiguration.display_name_attribute.trim()
    && (ldapConfiguration.security !== 'plain' || ldapConfiguration.allow_insecure)
  );

  async function saveConfiguration(event: FormEvent) {
    event.preventDefault();
    if (configurationSaving || !configurationComplete) return;
    setConfigurationSaving(true);
    setActionError(false);
    setNotice('');
    try {
      setLdapConfiguration(await api.updateLdapConfiguration(ldapConfiguration));
      setLdapConfigured(true);
      setNotice(t('ldapConfigurationSaved'));
    } catch {
      setActionError(true);
    } finally {
      setConfigurationSaving(false);
    }
  }

  async function testConfiguration(event: FormEvent) {
    event.preventDefault();
    if (testBusy || !configurationComplete || !testEmail.trim() || !testPassword) return;
    const credentialEmail = testEmail;
    const credentialPassword = testPassword;
    setTestBusy(true);
    setTestError(null);
    setTestResult(null);
    try {
      setTestResult(await api.testLdapConfiguration(ldapConfiguration, credentialEmail, credentialPassword));
    } catch (caught) {
      setTestError(caught instanceof ApiError ? (caught.detail ?? '') : '');
    } finally {
      setTestEmail('');
      setTestPassword('');
      setTestBusy(false);
    }
  }

  if (loading) return <div className="panel state-panel" role="status">{t('loadingAdministration')}</div>;
  if (loadError) return <div className="panel state-panel" role="alert"><p>{t('administrationLoadFailed')}</p></div>;

  return <div className="administration-tab-content">
    <Feedback error={actionError} notice={notice} />
    <section className="admin-section administration-policy" aria-labelledby="auth-policy-title">
      <header><ShieldCheck size={18} /><div><h2 id="auth-policy-title">{t('authenticationPolicy')}</h2><p>{t('authenticationPolicyHelp')}</p></div></header>
      <div className="admin-toggle-list">
        <label><input type="checkbox" checked={policy.password_registration_enabled} disabled={!policy.password_login_enabled} onChange={(event) => setPolicy((current) => ({ ...current, password_registration_enabled: event.target.checked }))} /> {t('passwordRegistration')}</label>
        <label><input type="checkbox" checked={policy.password_login_enabled} disabled={!policy.ldap_login_enabled} onChange={(event) => setPolicy((current) => ({ ...current, password_login_enabled: event.target.checked, password_registration_enabled: event.target.checked ? current.password_registration_enabled : false }))} /> {t('passwordLogin')}</label>
        <label><input type="checkbox" checked={policy.ldap_login_enabled} disabled={!ldapConfigured || !policy.password_login_enabled} onChange={(event) => setPolicy((current) => ({ ...current, ldap_login_enabled: event.target.checked }))} /> {t('ldapLogin')}</label>
      </div>
      {policy.password_registration_enabled && <div className="administration-persistent-warning" role="note"><AlertTriangle size={17} /><span>{t('passwordRegistrationRisk')}</span></div>}
      {!ldapConfigured && <p className="administration-field-help">{t('configureLdapBeforeEnabling')}</p>}
      <button type="button" className="primary admin-save" disabled={policySaving} onClick={savePolicy}><Save size={15} /> {policySaving ? t('saving') : t('saveAuthenticationPolicy')}</button>
    </section>
    <section className="admin-section administration-ldap" aria-labelledby="ldap-configuration-title">
      <header><Building2 size={18} /><div><h2 id="ldap-configuration-title">{t('ldapConfiguration')}</h2><p>{t('ldapConfigurationHelp')}</p></div></header>
      <form className="administration-ldap-form" onSubmit={saveConfiguration}>
        <label className="administration-form-wide">{t('ldapUrl')}<input type="url" required value={ldapConfiguration.url} placeholder={t(ldapConfiguration.security === 'ldaps' ? 'ldapSecureUrlPlaceholder' : 'ldapUrlPlaceholder')} onChange={(event) => updateLdap('url', event.target.value)} /></label>
        <label>{t('ldapSecurity')}<select value={ldapConfiguration.security} onChange={(event) => updateSecurity(event.target.value as LdapConfiguration['security'])}><option value="ldaps">{t('ldapSecurityLdaps')}</option><option value="starttls">{t('ldapSecurityStarttls')}</option><option value="plain">{t('ldapSecurityPlain')}</option></select></label>
        <label>{t('ldapBaseDn')}<input required value={ldapConfiguration.base_dn} onChange={(event) => updateLdap('base_dn', event.target.value)} /></label>
        <label className="administration-form-wide">{t('ldapBindIdentityTemplate')}<input required value={ldapConfiguration.bind_identity_template} onChange={(event) => updateLdap('bind_identity_template', event.target.value)} /><span className="administration-field-help">{t('ldapBindIdentityTemplateHelp')}</span></label>
        <label className="administration-form-wide">{t('ldapUserFilter')}<input required value={ldapConfiguration.user_filter} onChange={(event) => updateLdap('user_filter', event.target.value)} /></label>
        <label>{t('ldapEmailAttribute')}<input required value={ldapConfiguration.email_attribute} onChange={(event) => updateLdap('email_attribute', event.target.value)} /></label>
        <label>{t('ldapDisplayNameAttribute')}<input required value={ldapConfiguration.display_name_attribute} onChange={(event) => updateLdap('display_name_attribute', event.target.value)} /></label>
        <div className="administration-ldap-flags administration-form-wide">
          <label className="admin-checkbox"><input type="checkbox" checked={ldapConfiguration.allow_insecure} disabled={ldapConfiguration.security !== 'plain'} onChange={(event) => updateLdap('allow_insecure', event.target.checked)} /> {t('ldapAllowInsecure')}</label>
          <label className="admin-checkbox"><input type="checkbox" checked={ldapConfiguration.skip_tls_verify} disabled={ldapConfiguration.security === 'plain'} onChange={(event) => updateLdap('skip_tls_verify', event.target.checked)} /> {t('ldapSkipTlsVerify')}</label>
        </div>
        {ldapConfiguration.security === 'plain' && <div className="administration-persistent-warning administration-form-wide" role="alert"><AlertTriangle size={17} /><span>{t('ldapPlainWarning')}</span></div>}
        {ldapConfiguration.skip_tls_verify && <div className="administration-persistent-warning administration-form-wide" role="alert"><AlertTriangle size={17} /><span>{t('ldapSkipTlsWarning')}</span></div>}
        <button className="primary admin-save administration-form-wide" disabled={configurationSaving || !configurationComplete}><Save size={15} /> {configurationSaving ? t('saving') : t('saveLdapConfiguration')}</button>
      </form>
      <div className="administration-ldap-test">
        <div className="administration-subsection-heading"><FlaskConical size={17} /><div><h3>{t('testLdapConfiguration')}</h3><p>{t('testLdapConfigurationHelp')}</p></div></div>
        <form className="administration-ldap-test-form" onSubmit={testConfiguration}>
          <label>{t('testEmail')}<input type="email" autoComplete="off" required disabled={testBusy} value={testEmail} onChange={(event) => setTestEmail(event.target.value)} /></label>
          <label>{t('testPassword')}<input type="password" autoComplete="new-password" required disabled={testBusy} value={testPassword} onChange={(event) => setTestPassword(event.target.value)} /></label>
          <button className="secondary" disabled={testBusy || !configurationComplete || !testEmail.trim() || !testPassword}><FlaskConical size={15} /> {testBusy ? t('testing') : t('runLdapTest')}</button>
        </form>
        {testError !== null && <div className="admin-alert error" role="alert"><strong>{t('ldapTestFailed')}</strong>{testError && <div>{testError}</div>}</div>}
        {testResult && <div className="administration-ldap-result" role="status"><div><strong>{t('ldapTestSucceeded')}</strong><span>{testResult.email}</span><span>{testResult.display_name}</span><span>{testResult.duration_ms} {t('milliseconds')}</span></div><button className="icon-button" type="button" aria-label={t('clearTestResult')} title={t('clearTestResult')} onClick={() => setTestResult(null)}><X size={16} /></button></div>}
      </div>
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
  return <FormDialog title={t('userInformation')} eyebrow={detail.user.email} onClose={onClose} footer={<button className="primary" type="button" onClick={onClose}>{t('close')}</button>}>
    {!loaded && !error && <p className="runtime-muted" role="status">{t('loading')}</p>}
    {error && <div className="admin-alert error" role="alert">{t('administrationLoadFailed')}</div>}
    {!error && <dl className="administration-user-details">
      <div><dt>{t('displayName')}</dt><dd>{current.user.display_name}</dd></div>
      <div><dt>{t('email')}</dt><dd>{current.user.email}</dd></div>
      <div><dt>{t('userRole')}</dt><dd>{userRoleLabel(current.user.role, t)}</dd></div>
      <div><dt>{t('password')}</dt><dd>{current.has_password ? t('enabled') : t('disabled')}</dd></div>
      <div><dt>{t('created')}</dt><dd>{new Date(current.created_at).toLocaleString(locale)}</dd></div>
    </dl>}
  </FormDialog>;
}

function userRoleLabel(role: UserRole, t: ReturnType<typeof useI18n>['t']) {
  if (role === 'super_admin') return t('roleSuperAdministrator');
  if (role === 'admin') return t('roleAdministrator');
  return t('roleMember');
}

function UserCreateDialog({
  currentUser,
  onClose,
  onSaved
}: {
  currentUser: User;
  onClose: () => void;
  onSaved: (detail: AdminUserDetail) => void;
}) {
  const { t } = useI18n();
  const [email, setEmail] = useState('');
  const [displayName, setDisplayName] = useState('');
  const [password, setPassword] = useState('');
  const [role, setRole] = useState<UserRole>('member');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState(false);
  const isSuperAdministrator = currentUser.role === 'super_admin';

  async function submit(event: FormEvent) {
    event.preventDefault();
    if (busy || !email.trim() || (password && password.length < 8)) return;
    setBusy(true);
    setError(false);
    try {
      onSaved(await api.createAdminUser({
        email,
        role: isSuperAdministrator ? role : 'member',
        ...(displayName.trim() ? { display_name: displayName } : {}),
        ...(password ? { password } : {})
      }));
    } catch {
      setError(true);
    } finally {
      setBusy(false);
    }
  }

  return <FormDialog
    title={t('createUser')}
    onClose={onClose}
    busy={busy}
    footer={<>
      <button className="secondary" type="button" disabled={busy} onClick={onClose}>{t('cancel')}</button>
      <button className="primary" type="submit" form="administration-create-user-form" disabled={busy || !email.trim() || Boolean(password && password.length < 8)}><BadgePlus size={16} /> {busy ? t('creating') : t('createUser')}</button>
    </>}
  >
    <form id="administration-create-user-form" className="administration-dialog-form" onSubmit={submit}>
      <label>{t('email')}<input type="email" autoFocus required value={email} onChange={(event) => setEmail(event.target.value)} /></label>
      <label>{t('displayNameOptional')}<input maxLength={128} value={displayName} onChange={(event) => setDisplayName(event.target.value)} /></label>
      <label>{t('passwordOptional')}<input type="password" minLength={8} maxLength={1024} value={password} onChange={(event) => setPassword(event.target.value)} /></label>
      <label>{t('userRole')}<select value={isSuperAdministrator ? role : 'member'} disabled={!isSuperAdministrator} onChange={(event) => setRole(event.target.value as UserRole)}><option value="member">{t('roleMember')}</option>{isSuperAdministrator && <><option value="admin">{t('roleAdministrator')}</option><option value="super_admin">{t('roleSuperAdministrator')}</option></>}</select></label>
      {!password && <p className="administration-field-help">{t('passwordlessUserHelp')}</p>}
      {error && <div className="admin-alert error" role="alert">{t('administrationActionFailed')}</div>}
    </form>
  </FormDialog>;
}

function UserEditDialog({
  detail,
  onClose,
  onSaved
}: {
  detail: AdminUserDetail;
  onClose: () => void;
  onSaved: (detail: AdminUserDetail) => void;
}) {
  const { t } = useI18n();
  const [email, setEmail] = useState(detail.user.email);
  const [displayName, setDisplayName] = useState(detail.user.display_name);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState(false);

  async function submit(event: FormEvent) {
    event.preventDefault();
    if (busy || !email.trim() || !displayName.trim()) return;
    setBusy(true);
    setError(false);
    try {
      onSaved(await api.updateAdminUser(detail.user.id, { email, display_name: displayName }));
    } catch {
      setError(true);
    } finally {
      setBusy(false);
    }
  }

  return <FormDialog
    title={t('editUser')}
    eyebrow={detail.user.email}
    onClose={onClose}
    busy={busy}
    footer={<>
      <button className="secondary" type="button" disabled={busy} onClick={onClose}>{t('cancel')}</button>
      <button className="primary" type="submit" form="administration-edit-user-form" disabled={busy || !email.trim() || !displayName.trim()}><Save size={16} /> {busy ? t('saving') : t('saveChanges')}</button>
    </>}
  >
    <form id="administration-edit-user-form" className="administration-dialog-form" onSubmit={submit}>
      <label>{t('email')}<input type="email" required value={email} onChange={(event) => setEmail(event.target.value)} /></label>
      <label>{t('displayName')}<input required maxLength={128} value={displayName} onChange={(event) => setDisplayName(event.target.value)} /></label>
      {email.trim() !== detail.user.email && <p className="administration-field-help">{t('emailChangeSessionWarning')}</p>}
      {error && <div className="admin-alert error" role="alert">{t('administrationActionFailed')}</div>}
    </form>
  </FormDialog>;
}

function UserRoleDialog({
  detail,
  onClose,
  onSaved
}: {
  detail: AdminUserDetail;
  onClose: () => void;
  onSaved: (detail: AdminUserDetail) => void;
}) {
  const { t } = useI18n();
  const [role, setRole] = useState<UserRole>(detail.user.role);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState(false);

  async function submit(event: FormEvent) {
    event.preventDefault();
    if (busy || role === detail.user.role) return;
    setBusy(true);
    setError(false);
    try {
      onSaved(await api.setAdminUserRole(detail.user.id, role));
    } catch {
      setError(true);
    } finally {
      setBusy(false);
    }
  }

  return <FormDialog
    title={t('changeUserRole')}
    eyebrow={detail.user.email}
    onClose={onClose}
    busy={busy}
    footer={<>
      <button className="secondary" type="button" disabled={busy} onClick={onClose}>{t('cancel')}</button>
      <button className="primary" type="submit" form="administration-role-form" disabled={busy || role === detail.user.role}><Shield size={16} /> {busy ? t('saving') : t('saveChanges')}</button>
    </>}
  >
    <form id="administration-role-form" className="administration-dialog-form" onSubmit={submit}>
      <label>{t('userRole')}<select value={role} onChange={(event) => setRole(event.target.value as UserRole)}><option value="member">{t('roleMember')}</option><option value="admin">{t('roleAdministrator')}</option><option value="super_admin">{t('roleSuperAdministrator')}</option></select></label>
      <p className="administration-field-help">{t('roleChangeHelp')}</p>
      {error && <div className="admin-alert error" role="alert">{t('administrationActionFailed')}</div>}
    </form>
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
    eyebrow={detail.user.email}
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
  const confirmed = confirmation === detail.user.email;

  async function submit(event: FormEvent) {
    event.preventDefault();
    if (busy || !confirmed) return;
    setBusy(true);
    setError(false);
    try {
      onErased(await api.eraseUser(detail.user.id, detail.user.email));
    } catch {
      setError(true);
    } finally {
      setBusy(false);
    }
  }

  return <FormDialog
    title={t('eraseUser')}
    eyebrow={detail.user.email}
    onClose={onClose}
    busy={busy}
    footer={<>
      <button className="secondary" type="button" disabled={busy} onClick={onClose}>{t('cancel')}</button>
      <button className="primary administration-danger-action" type="submit" form="administration-erasure-form" disabled={busy || !confirmed}><UserX size={16} /> {busy ? t('deleting') : t('eraseUser')}</button>
    </>}
  >
    <form id="administration-erasure-form" className="administration-dialog-form" onSubmit={submit}>
      <p className="administration-erasure-warning">{t('userErasureHelp')}</p>
      <label>{t('confirmEmail')}<input type="email" value={confirmation} placeholder={detail.user.email} onChange={(event) => setConfirmation(event.target.value)} /></label>
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

  function userSaved(updated: AdminUserDetail) {
    setUsers((current) => current.map((item) => item.user.id === updated.user.id ? updated : item));
    setNotice(t('changesSaved'));
    setDialog(null);
  }

  function userCreated(created: AdminUserDetail) {
    setUsers((current) => [...current, created]);
    setNotice(t('userCreated'));
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
      <header><Users size={18} /><div><h2 id="user-management-title">{t('userManagement')}</h2><p>{t('userManagementHelp')}</p></div><button className="primary administration-header-action" type="button" onClick={() => setDialog({ kind: 'create' })}><Plus size={16} /> {t('createUser')}</button></header>
      <div className="administration-table-wrap">
        <table className="administration-table administration-users-table" aria-label={t('userManagement')}>
          <thead><tr><th>{t('displayName')}</th><th>{t('email')}</th><th>{t('userRole')}</th><th>{t('actions')}</th></tr></thead>
          <tbody>{users.length === 0 ? <tr><td colSpan={4}>{t('noUsers')}</td></tr> : users.map((detail) => {
            const user = detail.user;
            const isCurrent = user.id === currentUser.id;
            return <tr key={user.id}>
              <td><span className="administration-user-identity"><strong>{user.display_name}</strong>{isCurrent && <small>{t('currentUser')}</small>}</span></td>
              <td>{user.email}</td>
              <td><span className="status">{userRoleLabel(user.role, t)}</span></td>
              <td><div className="administration-table-actions">
                <button className="icon-button administration-table-action" type="button" aria-label={`${t('userInformation')}: ${user.email}`} title={t('userInformation')} onClick={() => setDialog({ kind: 'details', detail })}><Eye size={16} /></button>
                <button className="icon-button administration-table-action" type="button" aria-label={`${t('editUser')}: ${user.email}`} title={t('editUser')} onClick={() => setDialog({ kind: 'edit', detail })}><Pencil size={16} /></button>
                <button className="icon-button administration-table-action" type="button" aria-label={`${t('setUserPassword')}: ${user.email}`} title={t('setUserPassword')} onClick={() => setDialog({ kind: 'password', detail })}><KeyRound size={16} /></button>
                {currentUser.role === 'super_admin' && <button className="icon-button administration-table-action" type="button" aria-label={`${t('changeUserRole')}: ${user.email}`} title={t('changeUserRole')} onClick={() => setDialog({ kind: 'role', detail })}><Shield size={16} /></button>}
                <button className="icon-button administration-table-action danger" type="button" disabled={isCurrent} aria-label={`${t('eraseUser')}: ${user.email}`} title={isCurrent ? t('cannotDeleteCurrentUser') : t('eraseUser')} onClick={() => { if (!isCurrent) setDialog({ kind: 'erase', detail }); }}><UserX size={16} /></button>
              </div></td>
            </tr>;
          })}</tbody>
        </table>
      </div>
      {erasures.length > 0 && <div className="erasure-history"><h3>{t('erasureHistory')}</h3>{erasures.map((erasure) => <div key={erasure.user_id}><code>{erasure.email ?? erasure.user_id}</code><span className={`status ${erasure.status}`}>{erasure.status === 'pending' ? t('statusPending') : erasure.status === 'completed' ? t('statusCompleted') : erasure.status}</span><time>{new Date(erasure.completed_at ?? erasure.requested_at).toLocaleString(locale)}</time></div>)}</div>}
    </section>
    {dialog?.kind === 'create' && <UserCreateDialog currentUser={currentUser} onClose={() => setDialog(null)} onSaved={userCreated} />}
    {dialog?.kind === 'details' && <UserDetailsDialog detail={dialog.detail} onClose={() => setDialog(null)} />}
    {dialog?.kind === 'edit' && <UserEditDialog detail={dialog.detail} onClose={() => setDialog(null)} onSaved={userSaved} />}
    {dialog?.kind === 'password' && <UserPasswordDialog detail={dialog.detail} onClose={() => setDialog(null)} onSaved={userSaved} />}
    {dialog?.kind === 'role' && <UserRoleDialog detail={dialog.detail} onClose={() => setDialog(null)} onSaved={userSaved} />}
    {dialog?.kind === 'erase' && <UserEraseDialog detail={dialog.detail} onClose={() => setDialog(null)} onErased={erased} />}
  </div>;
}

export function AdministrationPage({ currentUser }: { currentUser: User }) {
  const { t } = useI18n();
  const [activeTab, setActiveTab] = useState<AdministrationTab>('authentication');
  const tabs = [
    { id: 'authentication' as const, label: t('authentication'), icon: ShieldCheck },
    { id: 'platforms' as const, label: t('externalPlatforms'), icon: Settings },
    { id: 'users' as const, label: t('userManagement'), icon: Users }
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
    </div>
  </div>;
}
