import { ArrowLeft, Plus, Save, Search, Sparkles, Trash2 } from 'lucide-react';
import { FormEvent, MouseEvent, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Agent, api, ApiError, Skill } from './api/client';
import { FormDialog } from './components/form-dialog';
import { MarkdownEditor } from './components/markdown-editor';
import { useI18n } from './i18n';
import type { TranslationKey } from './i18n';

type Navigate = (path: string, force?: boolean) => void;
type SetNavigationBlocker = (blocker: (() => boolean) | null) => void;

function appLink(event: MouseEvent<HTMLAnchorElement>, path: string, navigate: Navigate) {
  if (event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
  event.preventDefault();
  navigate(path);
}

function skillUsage(skillId: string, agents: Agent[]) {
  return agents.filter((agent) => agent.managed_skill_ids.includes(skillId));
}

function CreateSkillModal({ onClose, onCreated }: { onClose: () => void; onCreated: (skill: Skill) => void }) {
  const { t } = useI18n();
  const nameRef = useRef<HTMLInputElement>(null);
  const submittingRef = useRef(false);
  const mountedRef = useRef(true);
  const [name, setName] = useState(() => t('defaultSkillName'));
  const [description, setDescription] = useState(() => t('defaultSkillDescription'));
  const [content, setContent] = useState(() => t('defaultSkillContent'));
  const [pending, setPending] = useState(false);
  const [error, setError] = useState(false);

  useEffect(() => {
    return () => { mountedRef.current = false; };
  }, []);

  async function submit(event: FormEvent) {
    event.preventDefault();
    if (submittingRef.current) return;
    submittingRef.current = true;
    setPending(true);
    setError(false);
    try {
      const created = await api.createSkill(name, description, content);
      if (mountedRef.current) onCreated(created);
    } catch {
      if (mountedRef.current) setError(true);
    } finally {
      submittingRef.current = false;
      if (mountedRef.current) setPending(false);
    }
  }

  return (
    <FormDialog
      title={t('createSkillAction')}
      eyebrow={t('skills')}
      busy={pending}
      onClose={onClose}
      initialFocusRef={nameRef}
      className="skill-create-modal"
      footer={<><button className="secondary" type="button" onClick={onClose} disabled={pending}>{t('cancel')}</button><button className="primary" form="create-skill-form" type="submit" disabled={pending}>{pending ? t('creating') : t('createSkillAction')}</button></>}
    >
        <form id="create-skill-form" className="stack" onSubmit={submit}>
          <label>{t('name')}<input ref={nameRef} required value={name} onChange={(event) => setName(event.target.value)} /></label>
          <label>{t('description')}<input value={description} onChange={(event) => setDescription(event.target.value)} /></label>
          <MarkdownEditor label={t('content')} required value={content} onChange={setContent} />
          {error && <div className="error" role="alert">{t('skillSaveFailed')}</div>}
        </form>
    </FormDialog>
  );
}

export function SkillsPage({ navigate }: { navigate: Navigate }) {
  const { locale, t } = useI18n();
  const mountedRef = useRef(true);
  const loadGeneration = useRef(0);
  const createButtonRef = useRef<HTMLButtonElement>(null);
  const [skills, setSkills] = useState<Skill[]>([]);
  const [agents, setAgents] = useState<Agent[]>([]);
  const [loadingSkills, setLoadingSkills] = useState(true);
  const [loadingAgents, setLoadingAgents] = useState(true);
  const [skillsError, setSkillsError] = useState(false);
  const [agentsError, setAgentsError] = useState(false);
  const [search, setSearch] = useState('');
  const [filter, setFilter] = useState<'all' | 'used' | 'unused'>('all');
  const [sort, setSort] = useState('updated-desc');
  const [createOpen, setCreateOpen] = useState(false);
  const [selectedIds, setSelectedIds] = useState<string[]>([]);
  const [deleting, setDeleting] = useState(false);
  const [deleteError, setDeleteError] = useState(false);
  const selectAllRef = useRef<HTMLInputElement>(null);

  const load = useCallback(() => {
    const generation = ++loadGeneration.current;
    const skillsController = new AbortController();
    const agentsController = new AbortController();
    setLoadingSkills(true);
    setLoadingAgents(true);
    setSkillsError(false);
    setAgentsError(false);
    api.skills(skillsController.signal).then((response) => {
      if (mountedRef.current && generation === loadGeneration.current) setSkills(response);
    }).catch((error) => {
      if (mountedRef.current && generation === loadGeneration.current && error?.name !== 'AbortError') setSkillsError(true);
    }).finally(() => {
      if (mountedRef.current && generation === loadGeneration.current) setLoadingSkills(false);
    });
    api.agents(agentsController.signal).then((response) => {
      if (mountedRef.current && generation === loadGeneration.current) setAgents(response);
    }).catch((error) => {
      if (mountedRef.current && generation === loadGeneration.current && error?.name !== 'AbortError') setAgentsError(true);
    }).finally(() => {
      if (mountedRef.current && generation === loadGeneration.current) setLoadingAgents(false);
    });
    return () => { skillsController.abort(); agentsController.abort(); };
  }, []);

  useEffect(() => {
    const cancel = load();
    return () => { mountedRef.current = false; loadGeneration.current += 1; cancel(); };
  }, [load]);

  const visibleSkills = useMemo(() => {
    const query = search.trim().toLocaleLowerCase(locale);
    return skills.filter((skill) => {
      const used = skillUsage(skill.id, agents).length > 0;
      return (!query || `${skill.name} ${skill.description}`.toLocaleLowerCase(locale).includes(query))
        && (filter === 'all' || (filter === 'used' ? used : !used));
    }).sort((left, right) => {
      if (sort === 'name-asc') return left.name.localeCompare(right.name, locale);
      if (sort === 'name-desc') return right.name.localeCompare(left.name, locale);
      const delta = Date.parse(left.updated_at) - Date.parse(right.updated_at);
      return sort === 'updated-asc' ? delta : -delta;
    });
  }, [agents, filter, locale, search, skills, sort]);

  const visibleIds = useMemo(() => visibleSkills.map((skill) => skill.id), [visibleSkills]);
  const selectedVisibleCount = visibleIds.filter((id) => selectedIds.includes(id)).length;
  const allVisibleSelected = visibleIds.length > 0 && selectedVisibleCount === visibleIds.length;

  useEffect(() => {
    if (selectAllRef.current) {
      selectAllRef.current.indeterminate = selectedVisibleCount > 0 && !allVisibleSelected;
    }
  }, [allVisibleSelected, selectedVisibleCount]);

  useEffect(() => {
    const available = new Set(skills.map((skill) => skill.id));
    setSelectedIds((current) => current.filter((id) => available.has(id)));
  }, [skills]);

  function closeCreate() {
    setCreateOpen(false);
    requestAnimationFrame(() => createButtonRef.current?.focus());
  }

  async function deleteSelected() {
    if (deleting || selectedIds.length === 0) return;
    if (!window.confirm(t('confirmBulkDeleteSkills').replace('{count}', String(selectedIds.length)))) return;
    setDeleting(true);
    setDeleteError(false);
    const deletingIds = [...selectedIds];
    try {
      await api.bulkDeleteSkills(deletingIds);
      if (!mountedRef.current) return;
      const deleted = new Set(deletingIds);
      setSkills((current) => current.filter((skill) => !deleted.has(skill.id)));
      setSelectedIds([]);
    } catch {
      if (mountedRef.current) setDeleteError(true);
    } finally {
      if (mountedRef.current) setDeleting(false);
    }
  }

  return (
    <div className="workspace-page skills-page">
      <header className="page-header">
        <div><h1>{t('skills')}</h1><p>{t('skillsSubtitle')}</p></div>
        <button ref={createButtonRef} className="primary" type="button" onClick={() => setCreateOpen(true)}><Plus size={16} /> {t('createSkillAction')}</button>
      </header>
      <section className="skills-toolbar" aria-label={t('skillListControls')}>
        <label className="search-field skills-search"><Search size={16} /><span className="sr-only">{t('searchSkills')}</span><input type="search" role="searchbox" aria-label={t('searchSkills')} value={search} onChange={(event) => setSearch(event.target.value)} placeholder={t('searchSkills')} /></label>
        <div className="segmented-control" aria-label={t('filterSkills')}>
          {(['all', 'used', 'unused'] as const).map((value) => <button key={value} type="button" disabled={value !== 'all' && (loadingAgents || agentsError)} aria-pressed={filter === value} onClick={() => setFilter(value)}>{t(value === 'all' ? 'filterAll' : value === 'used' ? 'filterUsed' : 'filterUnused')}</button>)}
        </div>
        <label className="sort-control"><span>{t('sortBy')}</span><select aria-label={t('sortSkills')} value={sort} onChange={(event) => setSort(event.target.value)}><option value="updated-desc">{t('sortUpdatedDesc')}</option><option value="updated-asc">{t('sortUpdatedAsc')}</option><option value="name-asc">{t('sortNameAsc')}</option><option value="name-desc">{t('sortNameDesc')}</option></select></label>
        <button className="secondary danger" type="button" disabled={deleting || selectedIds.length === 0} onClick={deleteSelected}><Trash2 size={16} /> {deleting ? t('deleting') : t('deleteSelected')}</button>
      </section>
      {deleteError && <div className="error" role="alert">{t('skillDeleteFailed')}</div>}
      {agentsError && !loadingSkills && !skillsError && <div className="warning" role="alert">{t('skillUsageUnavailable')}</div>}
      {loadingSkills && <div className="panel state-panel">{t('loadingSkills')}</div>}
      {!loadingSkills && skillsError && <div className="panel state-panel" role="alert"><p>{t('skillsLoadFailed')}</p><button className="secondary" type="button" onClick={load}>{t('retry')}</button></div>}
      {!loadingSkills && !skillsError && skills.length === 0 && <div className="panel state-panel"><p>{t('noSkills')}</p><button className="primary" type="button" onClick={() => setCreateOpen(true)}><Plus size={16} /> {t('createSkillAction')}</button></div>}
      {!loadingSkills && !skillsError && skills.length > 0 && visibleSkills.length === 0 && <div className="panel state-panel">{t('noSkillMatches')}</div>}
      {!loadingSkills && !skillsError && visibleSkills.length > 0 && <div className="skills-list" aria-label={t('skills')}>
        <div className="skill-list-header"><label className="check-row"><input ref={selectAllRef} type="checkbox" aria-label={t('selectVisibleSkills')} checked={allVisibleSelected} onChange={(event) => {
          setSelectedIds((current) => event.target.checked
            ? [...new Set([...current, ...visibleIds])]
            : current.filter((id) => !visibleIds.includes(id)));
        }} /> {t('selectVisibleSkills')}</label></div>
        {visibleSkills.map((skill) => {
          const count = skillUsage(skill.id, agents).length;
          return <div className="skill-list-row" key={skill.id}>
            <input type="checkbox" aria-label={t('selectSkill').replace('{name}', skill.name)} checked={selectedIds.includes(skill.id)} onChange={(event) => setSelectedIds((current) => event.target.checked ? [...current, skill.id] : current.filter((id) => id !== skill.id))} />
            <a className="skill-list-main" href={`/skills/${skill.id}`} onClick={(event) => appLink(event, `/skills/${skill.id}`, navigate)}><strong>{skill.name}</strong><span>{skill.description || t('noDescription')}</span></a>
            <span className="skill-list-usage">{loadingAgents ? t('loadingUsage') : agentsError ? t('skillUsageUnavailable') : count === 1 ? t('oneAgent') : t('agentCount').replace('{count}', String(count))}</span>
            <time dateTime={skill.updated_at}>{new Date(skill.updated_at).toLocaleString(locale)}</time>
          </div>;
        })}
      </div>}
      {createOpen && <CreateSkillModal onClose={closeCreate} onCreated={(skill) => navigate(`/skills/${skill.id}`, true)} />}
    </div>
  );
}

export function SkillDetailPage({ skillId, navigate, setNavigationBlocker }: { skillId: string; navigate: Navigate; setNavigationBlocker: SetNavigationBlocker }) {
  const { locale, t } = useI18n();
  const mountedRef = useRef(true);
  const generationRef = useRef(0);
  const mutationRef = useRef(false);
  const [skill, setSkill] = useState<Skill | null>(null);
  const [agents, setAgents] = useState<Agent[]>([]);
  const [name, setName] = useState('');
  const [description, setDescription] = useState('');
  const [content, setContent] = useState('');
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState(false);
  const [notFound, setNotFound] = useState(false);
  const [agentsError, setAgentsError] = useState(false);
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<TranslationKey | null>(null);
  const [saved, setSaved] = useState(false);

  const load = useCallback(() => {
    const generation = ++generationRef.current;
    const detailController = new AbortController();
    const agentsController = new AbortController();
    setLoading(true); setLoadError(false); setNotFound(false); setAgentsError(false);
    api.skill(skillId, detailController.signal).then((response) => {
      if (!mountedRef.current || generation !== generationRef.current) return;
      setSkill(response); setName(response.name); setDescription(response.description); setContent(response.content);
    }).catch((loadFailure) => {
      if (!mountedRef.current || generation !== generationRef.current || loadFailure?.name === 'AbortError') return;
      if (loadFailure instanceof ApiError && loadFailure.status === 404) setNotFound(true);
      else setLoadError(true);
    }).finally(() => {
      if (mountedRef.current && generation === generationRef.current) setLoading(false);
    });
    api.agents(agentsController.signal).then((response) => {
      if (mountedRef.current && generation === generationRef.current) setAgents(response);
    }).catch((loadFailure) => {
      if (mountedRef.current && generation === generationRef.current && loadFailure?.name !== 'AbortError') setAgentsError(true);
    });
    return () => { detailController.abort(); agentsController.abort(); };
  }, [skillId]);

  useEffect(() => {
    mountedRef.current = true;
    const cancel = load();
    return () => { mountedRef.current = false; generationRef.current += 1; cancel(); };
  }, [load]);

  const dirty = Boolean(skill) && (name !== skill?.name || description !== skill?.description || content !== skill?.content);
  useEffect(() => {
    if (!dirty && !pending) { setNavigationBlocker(null); return; }
    const blocker = () => pending ? false : window.confirm(t('unsavedSkillConfirm'));
    setNavigationBlocker(blocker);
    const beforeUnload = (event: BeforeUnloadEvent) => { event.preventDefault(); event.returnValue = ''; };
    window.addEventListener('beforeunload', beforeUnload);
    return () => { setNavigationBlocker(null); window.removeEventListener('beforeunload', beforeUnload); };
  }, [dirty, pending, setNavigationBlocker, t]);

  async function save(event: FormEvent) {
    event.preventDefault();
    if (mutationRef.current || !skill) return;
    mutationRef.current = true; setPending(true); setError(null); setSaved(false);
    try {
      const response = await api.updateSkill(skill.id, name, description, content);
      if (mountedRef.current) { setSkill(response); setName(response.name); setDescription(response.description); setContent(response.content); setSaved(true); }
    } catch { if (mountedRef.current) setError('skillSaveFailed'); }
    finally { mutationRef.current = false; if (mountedRef.current) setPending(false); }
  }

  async function remove() {
    if (mutationRef.current || !skill) return;
    if (!window.confirm(t('confirmDeleteSkill').replace('{name}', skill.name))) return;
    mutationRef.current = true; setPending(true); setError(null);
    try {
      await api.deleteSkill(skill.id);
      if (mountedRef.current) { setNavigationBlocker(null); navigate('/skills', true); }
    } catch { if (mountedRef.current) setError('skillDeleteFailed'); }
    finally { mutationRef.current = false; if (mountedRef.current) setPending(false); }
  }

  if (loading) return <div className="workspace-page"><div className="panel state-panel">{t('loadingSkill')}</div></div>;
  if (notFound) return <div className="workspace-page"><div className="panel state-panel"><h1>{t('skillNotFound')}</h1><button className="secondary" onClick={() => navigate('/skills', true)}>{t('backToSkills')}</button></div></div>;
  if (loadError || !skill) return <div className="workspace-page"><div className="panel state-panel" role="alert"><p>{t('skillLoadFailed')}</p><button className="secondary" onClick={load}>{t('retry')}</button></div></div>;
  const attachedAgents = skillUsage(skill.id, agents);
  return (
    <div className="workspace-page skill-detail-page">
      <header className="page-header skill-detail-header">
        <div><button className="text-button back-button" type="button" disabled={pending} onClick={() => navigate('/skills')}><ArrowLeft size={16} /> {t('backToSkills')}</button><h1>{skill.name}</h1><p>{skill.description || t('noDescription')}</p></div>
      </header>
      <div className="skill-detail-layout">
        <section className="panel skill-editor">
          <div className="section-title"><Sparkles size={18} /> {t('editSkill')}</div>
          <form className="stack" onSubmit={save}>
            <label>{t('name')}<input required disabled={pending} value={name} onChange={(event) => { setName(event.target.value); setSaved(false); }} /></label>
            <label>{t('description')}<input disabled={pending} value={description} onChange={(event) => { setDescription(event.target.value); setSaved(false); }} /></label>
            <MarkdownEditor className="skill-content" label={t('content')} required disabled={pending} value={content} onChange={(markdown) => { setContent(markdown); setSaved(false); }} />
            {error && <div className="error" role="alert">{t(error)}</div>}
            {saved && <div className="success" role="status">{t('changesSaved')}</div>}
            <div className="button-row"><button className="primary" disabled={pending || !dirty}><Save size={16} /> {pending ? t('saving') : t('saveSkill')}</button><button className="secondary danger" type="button" disabled={pending} onClick={remove}><Trash2 size={16} /> {t('deleteSkill')}</button></div>
          </form>
        </section>
        <aside className="skill-sidebar">
          <section className="panel"><h2>{t('details')}</h2><dl className="metadata-list"><div><dt>{t('created')}</dt><dd>{new Date(skill.created_at).toLocaleString(locale)}</dd></div><div><dt>{t('updated')}</dt><dd>{new Date(skill.updated_at).toLocaleString(locale)}</dd></div></dl></section>
          <section className="panel"><h2>{t('attachedAgents')}</h2>{agentsError ? <div className="warning" role="alert">{t('attachedAgentsUnavailable')}</div> : attachedAgents.length === 0 ? <p className="muted">{t('noAttachedAgents')}</p> : <ul className="attached-agent-list">{attachedAgents.map((agent) => <li key={agent.id}><a href={`/agents/${agent.id}`} aria-disabled={pending || undefined} tabIndex={pending ? -1 : undefined} onClick={(event) => pending ? event.preventDefault() : appLink(event, `/agents/${agent.id}`, navigate)}>{agent.name}</a></li>)}</ul>}</section>
        </aside>
      </div>
    </div>
  );
}
