import { KeyRound, Pencil, Plus, Trash2, X } from 'lucide-react';
import { FormEvent, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { api, type Agent, type CreateUserSecretRequest, type SecretGrant, type UpdateUserSecretRequest, type UserSecret } from './api/client';
import { FormDialog } from './components/form-dialog';
import { useI18n } from './i18n';

const secretNamePattern = /^[A-Z_][A-Z0-9_]*$/;
const maxSecretFileBytes = 1024 * 1024;

function readFileAsBase64(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => {
      const result = typeof reader.result === 'string' ? reader.result : '';
      const separator = result.indexOf(',');
      resolve(separator >= 0 ? result.slice(separator + 1) : result);
    };
    reader.onerror = () => reject(reader.error ?? new Error('File read failed'));
    reader.readAsDataURL(file);
  });
}

function formatFileSize(bytes: number | null | undefined, locale: string) {
  if (bytes === null || bytes === undefined || !Number.isFinite(bytes)) return '-';
  return new Intl.NumberFormat(locale, {
    style: 'unit',
    unit: 'byte',
    unitDisplay: 'short',
    maximumFractionDigits: 1
  }).format(bytes);
}

type SecretDraft = {
  name: string;
  kind: 'value' | 'file';
  value: string;
  file: File | null;
};

function emptyDraft(): SecretDraft {
  return { name: '', kind: 'value', value: '', file: null };
}

export function SecretsPage() {
  const { locale, t } = useI18n();
  const [secrets, setSecrets] = useState<UserSecret[]>([]);
  const [grants, setGrants] = useState<SecretGrant[]>([]);
  const [agents, setAgents] = useState<Agent[]>([]);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState(false);
  const [retry, setRetry] = useState(0);
  const [createOpen, setCreateOpen] = useState(false);
  const [editing, setEditing] = useState<UserSecret | null>(null);
  const [draft, setDraft] = useState<SecretDraft>(emptyDraft);
  const [busy, setBusy] = useState(false);
  const [formError, setFormError] = useState('');
  const [actionError, setActionError] = useState(false);
  const loadGeneration = useRef(0);
  const createButtonRef = useRef<HTMLButtonElement>(null);
  const nameInputRef = useRef<HTMLInputElement>(null);
  const busyRef = useRef(false);

  useEffect(() => { busyRef.current = busy; }, [busy]);

  const load = useCallback(() => {
    const generation = ++loadGeneration.current;
    const controller = new AbortController();
    setLoading(true);
    setLoadError(false);
    Promise.all([
      api.secrets(controller.signal),
      api.secretGrants(undefined, controller.signal),
      api.agents(controller.signal)
    ]).then(([loadedSecrets, loadedGrants, loadedAgents]) => {
      if (generation !== loadGeneration.current || controller.signal.aborted) return;
      setSecrets(loadedSecrets);
      setGrants(loadedGrants);
      setAgents(loadedAgents);
      setLoading(false);
    }).catch(() => {
      if (generation !== loadGeneration.current || controller.signal.aborted) return;
      setLoadError(true);
      setLoading(false);
    });
    return controller;
  }, []);

  useEffect(() => {
    const controller = load();
    return () => {
      loadGeneration.current += 1;
      controller.abort();
    };
  }, [load, retry]);

  const grantsBySecret = useMemo(() => {
    const map = new Map<string, SecretGrant[]>();
    for (const grant of grants) {
      const list = map.get(grant.secret_name) ?? [];
      list.push(grant);
      map.set(grant.secret_name, list);
    }
    return map;
  }, [grants]);

  const agentNames = useMemo(() => new Map(agents.map((agent) => [agent.id, agent.name])), [agents]);

  function openCreate() {
    setEditing(null);
    setDraft(emptyDraft());
    setFormError('');
    setCreateOpen(true);
  }

  function openEdit(secret: UserSecret) {
    setEditing(secret);
    setDraft({ name: secret.name, kind: secret.kind, value: '', file: null });
    setFormError('');
    setCreateOpen(true);
  }

  function closeDialog() {
    if (busyRef.current) return;
    setCreateOpen(false);
    setEditing(null);
    setFormError('');
    window.requestAnimationFrame(() => createButtonRef.current?.focus());
  }

  async function saveSecret(event: FormEvent) {
    event.preventDefault();
    if (busyRef.current) return;
    const name = draft.name.trim();
    if (!secretNamePattern.test(name) || name.length > 128) {
      setFormError(t('secretNameInvalid'));
      return;
    }
    if (draft.kind === 'value' && !draft.value) {
      setFormError(t('secretValueRequired'));
      return;
    }
    if (draft.kind === 'file') {
      if (!draft.file) {
        setFormError(t('secretFileRequired'));
        return;
      }
      if (draft.file.size > maxSecretFileBytes) {
        setFormError(t('secretFileTooLarge'));
        return;
      }
    }
    busyRef.current = true;
    setBusy(true);
    setFormError('');
    try {
      if (editing) {
        const body: UpdateUserSecretRequest = draft.kind === 'file'
          ? { file_name: draft.file!.name, file_base64: await readFileAsBase64(draft.file!) }
          : { value: draft.value };
        await api.updateSecret(editing.id, body);
      } else {
        const body: CreateUserSecretRequest = draft.kind === 'file'
          ? { name, kind: 'file', file_name: draft.file!.name, file_base64: await readFileAsBase64(draft.file!) }
          : { name, kind: 'value', value: draft.value };
        await api.createSecret(body);
      }
      setCreateOpen(false);
      setEditing(null);
      load();
    } catch {
      setFormError(t('secretSaveFailed'));
    } finally {
      busyRef.current = false;
      setBusy(false);
    }
  }

  async function deleteSecret(secret: UserSecret) {
    if (busyRef.current) return;
    if (!window.confirm(t('confirmDeleteSecret').replace('{name}', secret.name))) return;
    busyRef.current = true;
    setBusy(true);
    setActionError(false);
    try {
      await api.deleteSecret(secret.id);
      load();
    } catch {
      setActionError(true);
    } finally {
      busyRef.current = false;
      setBusy(false);
    }
  }

  async function revokeGrant(secret: UserSecret, grant: SecretGrant) {
    if (busyRef.current) return;
    busyRef.current = true;
    setBusy(true);
    setActionError(false);
    try {
      await api.deleteSecretGrant(grant.agent_id, grant.secret_name);
      load();
    } catch {
      setActionError(true);
    } finally {
      busyRef.current = false;
      setBusy(false);
    }
  }

  return <div className="secrets-page">
    <header className="secrets-header">
      <div><h1><KeyRound size={19} /> {t('personalSecrets')}</h1><p>{t('personalSecretsSubtitle')}</p></div>
      <button ref={createButtonRef} type="button" className="primary" onClick={openCreate}><Plus size={16} /> {t('createSecret')}</button>
    </header>
    {actionError && <div className="operation-alert" role="alert"><span>{t('secretGrantRevokeFailed')}</span></div>}
    {loading ? <section className="agents-state" aria-live="polite">{t('loadingSecrets')}</section>
      : loadError ? <section className="agents-state" role="alert"><p>{t('secretsLoadFailed')}</p><button type="button" className="secondary" onClick={() => setRetry((value) => value + 1)}>{t('retry')}</button></section>
        : secrets.length === 0 ? <section className="agents-state"><h2>{t('noSecrets')}</h2><button type="button" className="primary" onClick={openCreate}><Plus size={16} /> {t('createSecret')}</button></section>
          : <div className="agents-table-wrap"><table className="agents-table secrets-table" aria-label={t('personalSecrets')}>
            <thead><tr><th>{t('secretName')}</th><th>{t('secretKind')}</th><th>{t('secretFileName')}</th><th>{t('secretFileSize')}</th><th>{t('secretFileSha256')}</th><th>{t('secretGrantedAgents')}</th><th>{t('actions')}</th></tr></thead>
            <tbody>{secrets.map((secret) => {
              const secretGrants = grantsBySecret.get(secret.name) ?? [];
              return <tr key={secret.id}>
                <td><strong>{secret.name}</strong></td>
                <td>{secret.kind === 'file' ? t('secretKindFile') : t('secretKindValue')}</td>
                <td>{secret.file_name ?? '-'}</td>
                <td>{formatFileSize(secret.file_size_bytes, locale)}</td>
                <td>{secret.file_sha256 ? <code className="secret-sha">{secret.file_sha256}</code> : '-'}</td>
                <td><div className="secret-grant-list">{secretGrants.length === 0 ? <span className="muted">{t('noSecretGrants')}</span> : secretGrants.map((grant) => {
                  const agentName = agentNames.get(grant.agent_id) ?? grant.agent_id;
                  return <span className="secret-grant-chip" key={grant.agent_id}><span>{agentName}</span><button type="button" className="icon-button" disabled={busy} aria-label={t('revokeSecretGrantAria').replace('{agent}', agentName).replace('{name}', secret.name)} title={t('revokeSecretGrant')} onClick={() => void revokeGrant(secret, grant)}><X size={12} /></button></span>;
                })}</div></td>
                <td><div className="button-row agent-mcp-actions"><button type="button" className="icon-button" disabled={busy} aria-label={`${t('editSecret')}: ${secret.name}`} title={t('editSecret')} onClick={() => openEdit(secret)}><Pencil size={16} /></button><button type="button" className="icon-button" disabled={busy} aria-label={`${t('deleteSecret')}: ${secret.name}`} title={t('deleteSecret')} onClick={() => void deleteSecret(secret)}><Trash2 size={16} /></button></div></td>
              </tr>;
            })}</tbody>
          </table></div>}
    {createOpen && <FormDialog title={editing ? `${t('editSecret')}: ${editing.name}` : t('createSecret')} busy={busy} onClose={closeDialog} initialFocusRef={nameInputRef} className="secret-dialog" footer={<>
      <button type="button" className="secondary" disabled={busy} onClick={closeDialog}>{t('cancel')}</button>
      <button type="submit" form="secret-form" className="primary" disabled={busy}>{busy ? t('saving') : t('saveSecret')}</button>
    </>}>
      <form id="secret-form" className="stack" onSubmit={saveSecret}>
        <label>{t('secretName')}<input ref={nameInputRef} required maxLength={128} disabled={busy || Boolean(editing)} value={draft.name} onChange={(event) => setDraft((current) => ({ ...current, name: event.target.value }))} /></label>
        <label>{t('secretKind')}<select disabled={busy || Boolean(editing)} value={draft.kind} onChange={(event) => setDraft((current) => ({ ...current, kind: event.target.value === 'file' ? 'file' : 'value', value: '', file: null }))}><option value="value">{t('secretKindValue')}</option><option value="file">{t('secretKindFile')}</option></select></label>
        {draft.kind === 'value'
          ? <label>{t('secretValue')}<input type="password" autoComplete="new-password" disabled={busy} value={draft.value} onChange={(event) => setDraft((current) => ({ ...current, value: event.target.value }))} /></label>
          : <label>{t('secretFile')}<input type="file" disabled={busy} onChange={(event) => setDraft((current) => ({ ...current, file: event.target.files?.[0] ?? null }))} /></label>}
        {formError && <div className="error" role="alert">{formError}</div>}
      </form>
    </FormDialog>}
  </div>;
}
