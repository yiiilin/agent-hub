import { Download, FileText, Loader2, X } from 'lucide-react';
import { type ChangeEvent, type DragEvent, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { HubSessionAttachment } from '../api/client';
import { useI18n } from '../i18n';

export type PendingAttachment = {
  id: string;
  file: File;
  status: 'pending' | 'uploading' | 'ready' | 'error';
  progress: number;
  error: string | null;
  attachment: HubSessionAttachment | null;
};

export type AttachmentUploader = (
  sessionId: string,
  file: File,
  onProgress: ((loaded: number, total: number) => void) | undefined,
  signal?: AbortSignal
) => Promise<HubSessionAttachment>;

let pendingAttachmentSequence = 0;

function pendingAttachmentId() {
  pendingAttachmentSequence += 1;
  return `pending-attachment-${pendingAttachmentSequence}`;
}

export function formatAttachmentSize(bytes: number, locale: string) {
  if (bytes < 1024) return `${bytes} B`;
  const kilobytes = bytes / 1024;
  if (kilobytes < 1024) {
    return `${new Intl.NumberFormat(locale, { maximumFractionDigits: kilobytes < 10 ? 1 : 0 }).format(kilobytes)} KB`;
  }
  return `${new Intl.NumberFormat(locale, { maximumFractionDigits: 1 }).format(kilobytes / 1024)} MB`;
}

export function useChatAttachments(sessionId: string | null, upload: AttachmentUploader) {
  const [items, setItems] = useState<PendingAttachment[]>([]);
  const [dragging, setDragging] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);
  const controllersRef = useRef(new Map<string, AbortController>());
  const sessionIdRef = useRef(sessionId);
  sessionIdRef.current = sessionId;
  const uploadRef = useRef(upload);
  uploadRef.current = upload;

  const markSendingForUpload = useCallback(() => {
    // 发送消息时随 multipart 一起提交的 pending 附件：标记为上传中，
    // 返回 multipart 上传进度回调（整体进度映射到各附件条目）。
    setItems((current) => current.map((item) => item.status === 'pending'
      ? { ...item, status: 'uploading' as const, progress: 0 }
      : item));
    return (loaded: number, total: number) => {
      const percent = total > 0 ? Math.round((loaded / total) * 100) : 0;
      setItems((current) => current.map((item) => item.status === 'uploading'
        ? { ...item, progress: percent }
        : item));
    };
  }, []);

  const revertSendingUpload = useCallback(() => {
    // 发送失败时把随消息上传的附件条目从 uploading 恢复为 pending，
    // 保留文件与顺序，允许用户重试；已单独上传完成的 ready 条目不受影响。
    setItems((current) => current.map((item) => item.status === 'uploading'
      ? { ...item, status: 'pending' as const, progress: 0 }
      : item));
  }, []);

  const clear = useCallback(() => {
    for (const controller of controllersRef.current.values()) controller.abort();
    controllersRef.current.clear();
    setItems([]);
  }, []);

  useEffect(() => {
    clear();
  }, [sessionId, clear]);

  const remove = useCallback((id: string) => {
    controllersRef.current.get(id)?.abort();
    controllersRef.current.delete(id);
    setItems((current) => current.filter((item) => item.id !== id));
  }, []);

  const uploadOne = useCallback((entry: PendingAttachment) => {
    const targetSessionId = sessionIdRef.current;
    if (!targetSessionId) return;
    const controller = new AbortController();
    controllersRef.current.set(entry.id, controller);
    void uploadRef.current(targetSessionId, entry.file, (loaded, total) => {
      const percent = total > 0 ? Math.round((loaded / total) * 100) : 0;
      setItems((current) => current.map((item) => item.id === entry.id
        ? { ...item, progress: percent }
        : item));
    }, controller.signal)
      .then((attachment) => {
        controllersRef.current.delete(entry.id);
        setItems((current) => current.map((item) => item.id === entry.id
          ? { ...item, status: 'ready', progress: 100, error: null, attachment }
          : item));
      })
      .catch((error) => {
        controllersRef.current.delete(entry.id);
        if ((error as Error)?.name === 'AbortError') return;
        setItems((current) => current.map((item) => item.id === entry.id
          ? { ...item, status: 'error', error: error instanceof Error ? error.message : null }
          : item));
      });
  }, []);

  const addFiles = useCallback((files: readonly File[]) => {
    if (files.length === 0) return;
    const targetSessionId = sessionIdRef.current;
    const entries = [...files].map((file) => ({
      id: pendingAttachmentId(),
      file,
      status: targetSessionId ? ('uploading' as const) : ('pending' as const),
      progress: 0,
      error: null,
      attachment: null
    }));
    setItems((current) => [...current, ...entries]);
    if (targetSessionId) {
      for (const entry of entries) uploadOne(entry);
    }
  }, [uploadOne]);

  const openPicker = useCallback(() => inputRef.current?.click(), []);

  const handleInputChange = useCallback((event: ChangeEvent<HTMLInputElement>) => {
    addFiles(event.target.files ? [...event.target.files] : []);
    event.target.value = '';
  }, [addFiles]);

  const handleDragOver = useCallback((event: DragEvent) => {
    event.preventDefault();
    setDragging(true);
  }, []);

  const handleDragLeave = useCallback((event: DragEvent) => {
    event.preventDefault();
    setDragging(false);
  }, []);

  const handleDrop = useCallback((event: DragEvent) => {
    event.preventDefault();
    setDragging(false);
    addFiles(event.dataTransfer.files ? [...event.dataTransfer.files] : []);
  }, [addFiles]);

  const readyIds = useMemo(
    () => items.flatMap((item) => item.status === 'ready' && item.attachment ? [item.attachment.id] : []),
    [items]
  );
  const uploading = items.some((item) => item.status === 'uploading');

  return {
    items,
    readyIds,
    uploading,
    pendingFiles: items.flatMap((item) => item.status === 'pending' ? [item.file] : []),
    dragging,
    inputRef,
    addFiles,
    openPicker,
    handleInputChange,
    markSendingForUpload,
    revertSendingUpload,
    remove,
    clear,
    handleDragOver,
    handleDragLeave,
    handleDrop
  };
}

function PendingAttachmentThumb({ file }: { file: File }) {
  const [url, setUrl] = useState<string | null>(null);
  useEffect(() => {
    const objectUrl = URL.createObjectURL(file);
    setUrl(objectUrl);
    return () => URL.revokeObjectURL(objectUrl);
  }, [file]);
  if (!url) return <FileText size={14} aria-hidden="true" />;
  return <img className="session-composer-attachment-thumb" src={url} alt={file.name} />;
}

export function ComposerAttachmentPreview({ items, onRemove }: { items: PendingAttachment[]; onRemove: (id: string) => void }) {
  const { locale, t } = useI18n();
  if (items.length === 0) return null;
  return <div className="session-composer-attachments" aria-label={t('attachment')}>
    {items.map((item) => (
      <div key={item.id} className={`session-composer-attachment status-${item.status}`}>
        {item.file.type.startsWith('image/')
          ? <PendingAttachmentThumb file={item.file} />
          : item.status === 'uploading'
            ? <Loader2 className="session-composer-attachment-spinner" size={14} aria-hidden="true" />
            : <FileText size={14} aria-hidden="true" />}
        {item.status === 'pending' && <span className="session-composer-attachment-status">{t('attachmentPending')}</span>}
        <span className="session-composer-attachment-name">{item.file.name}</span>
        <small>{formatAttachmentSize(item.file.size, locale)}</small>
        {item.status === 'uploading' && <span className="session-composer-attachment-progress" role="progressbar" aria-valuemin={0} aria-valuemax={100} aria-valuenow={item.progress}><span className="session-composer-attachment-progress-track"><span className="session-composer-attachment-progress-fill" style={{ width: `${item.progress}%` }} /></span><span className="session-composer-attachment-progress-text">{item.progress}%</span></span>}
        {item.status === 'error' && <span className="session-composer-attachment-status error">{t('attachmentUploadFailed')}</span>}
        <button type="button" className="icon-button session-composer-attachment-remove" aria-label={t('attachmentRemove')} title={t('attachmentRemove')} onClick={() => onRemove(item.id)}><X size={13} /></button>
      </div>
    ))}
  </div>;
}

export function MessageAttachments({ attachments, urlFor }: { attachments: HubSessionAttachment[]; urlFor: (id: string) => string }) {
  const { locale, t } = useI18n();
  if (attachments.length === 0) return null;
  return <div className="session-attachment-list">
    {attachments.map((attachment) => (
      attachment.content_type.startsWith('image/')
        ? <MessageImageAttachment key={attachment.id} attachment={attachment} urlFor={urlFor} />
        : <a key={attachment.id} className="session-attachment-file" href={urlFor(attachment.id)} download={attachment.name}>
            <FileText size={15} aria-hidden="true" />
            <span><strong>{attachment.name}</strong><small>{formatAttachmentSize(attachment.size_bytes, locale)}</small><small>{t('attachmentDownload')}</small></span>
            <Download size={14} aria-hidden="true" />
          </a>
    ))}
  </div>;
}

function MessageImageAttachment({ attachment, urlFor }: { attachment: HubSessionAttachment; urlFor: (id: string) => string }) {
  const { t } = useI18n();
  const [open, setOpen] = useState(false);
  const url = urlFor(attachment.id);
  return <>
    <button type="button" className="session-attachment-image-button" aria-label={t('attachmentImage')} title={t('attachmentImage')} onClick={() => setOpen(true)}>
      <img className="session-attachment-image" src={url} alt={attachment.name} loading="lazy" />
    </button>
    {open && <AttachmentLightbox attachment={attachment} url={url} onClose={() => setOpen(false)} />}
  </>;
}

function AttachmentLightbox({ attachment, url, onClose }: { attachment: HubSessionAttachment; url: string; onClose: () => void }) {
  const { t } = useI18n();
  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [onClose]);
  return <div className="session-attachment-lightbox" role="dialog" aria-modal="true" aria-label={attachment.name} onClick={onClose}>
    <img src={url} alt={attachment.name} />
    <button type="button" className="icon-button session-attachment-lightbox-close" aria-label={t('attachmentLightboxClose')} title={t('attachmentLightboxClose')} onClick={onClose}><X size={20} /></button>
  </div>;
}
