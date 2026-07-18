import { X } from 'lucide-react';
import { KeyboardEvent, ReactNode, RefObject, useEffect, useId, useRef } from 'react';
import { useI18n } from '../i18n';

const focusableSelector = [
  'button:not(:disabled)',
  'input:not(:disabled)',
  'textarea:not(:disabled)',
  'select:not(:disabled)',
  'a[href]',
  '[contenteditable="true"]',
  '[tabindex]:not([tabindex="-1"])'
].join(',');

type FormDialogProps = {
  title: string;
  eyebrow?: string;
  children: ReactNode;
  footer?: ReactNode;
  onClose: () => void;
  busy?: boolean;
  className?: string;
  initialFocusRef?: RefObject<HTMLElement | null>;
};

export function FormDialog({
  title,
  eyebrow,
  children,
  footer,
  onClose,
  busy = false,
  className = '',
  initialFocusRef
}: FormDialogProps) {
  const { t } = useI18n();
  const titleId = useId();
  const dialogRef = useRef<HTMLElement>(null);
  const openerRef = useRef<HTMLElement | null>(null);
  const busyRef = useRef(busy);
  busyRef.current = busy;

  useEffect(() => {
    openerRef.current = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = 'hidden';
    const frame = window.requestAnimationFrame(() => {
      const target = initialFocusRef?.current
        ?? dialogRef.current?.querySelector<HTMLElement>(focusableSelector)
        ?? dialogRef.current;
      target?.focus();
    });
    return () => {
      window.cancelAnimationFrame(frame);
      document.body.style.overflow = previousOverflow;
      if (openerRef.current?.isConnected) openerRef.current.focus();
    };
  }, [initialFocusRef]);

  function requestClose() {
    if (!busyRef.current) onClose();
  }

  function handleKeyDown(event: KeyboardEvent<HTMLElement>) {
    if (event.key === 'Escape') {
      event.preventDefault();
      requestClose();
      return;
    }
    if (event.key !== 'Tab') return;
    const focusable = Array.from(dialogRef.current?.querySelectorAll<HTMLElement>(focusableSelector) ?? [])
      .filter((element) => element.getClientRects().length > 0);
    if (focusable.length === 0) {
      event.preventDefault();
      dialogRef.current?.focus();
      return;
    }
    const first = focusable[0];
    const last = focusable.at(-1)!;
    const active = document.activeElement;
    const activeIndex = focusable.indexOf(active as HTMLElement);
    if (event.shiftKey && (active === dialogRef.current || activeIndex <= 0)) {
      event.preventDefault();
      last.focus();
    } else if (!event.shiftKey && (active === dialogRef.current || activeIndex === focusable.length - 1)) {
      event.preventDefault();
      first.focus();
    }
  }

  return (
    <div className="modal-backdrop form-dialog-backdrop" onMouseDown={(event) => {
      if (event.target === event.currentTarget) requestClose();
    }}>
      <section
        ref={dialogRef}
        className={`modal form-dialog ${className}`.trim()}
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        aria-busy={busy || undefined}
        tabIndex={-1}
        onKeyDown={handleKeyDown}
      >
        <header className="modal-header">
          <div>{eyebrow && <span className="eyebrow">{eyebrow}</span>}<h2 id={titleId}>{title}</h2></div>
          <button className="icon-button" type="button" aria-label={t('close')} title={t('close')} disabled={busy} onClick={requestClose}><X size={18} /></button>
        </header>
        <div className="form-dialog-body">{children}</div>
        {footer && <footer className="modal-actions">{footer}</footer>}
      </section>
    </div>
  );
}
