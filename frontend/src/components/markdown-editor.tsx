import {
  BlockTypeSelect,
  BoldItalicUnderlineToggles,
  CodeMirrorEditor,
  CreateLink,
  DiffSourceToggleWrapper,
  InsertThematicBreak,
  ListsToggle,
  MDXEditor,
  type MDXEditorMethods,
  UndoRedo,
  codeBlockPlugin,
  codeMirrorPlugin,
  diffSourcePlugin,
  headingsPlugin,
  linkPlugin,
  listsPlugin,
  markdownShortcutPlugin,
  quotePlugin,
  thematicBreakPlugin,
  toolbarPlugin
} from '@mdxeditor/editor';
import '@mdxeditor/editor/style.css';
import { useEffect, useId, useMemo, useRef, useState } from 'react';
import { editorTranslation, useI18n } from '../i18n';

function MarkdownToolbar() {
  return (
    <DiffSourceToggleWrapper options={['rich-text', 'source']}>
      <UndoRedo />
      <BlockTypeSelect />
      <BoldItalicUnderlineToggles />
      <CreateLink />
      <ListsToggle />
      <InsertThematicBreak />
    </DiffSourceToggleWrapper>
  );
}

type MarkdownEditorProps = {
  label: string;
  value: string;
  onChange: (markdown: string) => void;
  disabled?: boolean;
  required?: boolean;
  className?: string;
};

export function MarkdownEditor({
  label,
  value,
  onChange,
  disabled = false,
  required = false,
  className = ''
}: MarkdownEditorProps) {
  const { language, t } = useI18n();
  const labelId = useId();
  const editorRef = useRef<MDXEditorMethods>(null);
  const rootRef = useRef<HTMLDivElement>(null);
  const [parseError, setParseError] = useState(false);
  const plugins = useMemo(() => [
    headingsPlugin(),
    listsPlugin(),
    quotePlugin(),
    thematicBreakPlugin(),
    linkPlugin(),
    markdownShortcutPlugin(),
    codeMirrorPlugin(),
    codeBlockPlugin({
      codeBlockEditorDescriptors: [
        {
          priority: 0,
          match: () => true,
          Editor: CodeMirrorEditor
        }
      ]
    }),
    diffSourcePlugin({ viewMode: 'rich-text' }),
    toolbarPlugin({ toolbarContents: MarkdownToolbar })
  ], []);

  useEffect(() => {
    if (editorRef.current?.getMarkdown() !== value) editorRef.current?.setMarkdown(value);
  }, [value]);

  useEffect(() => {
    const root = rootRef.current;
    if (!root) return;
    const labelEditors = () => {
      root.querySelectorAll<HTMLElement>('.markdown-editor-content, .cm-content').forEach((element) => {
        element.setAttribute('aria-labelledby', labelId);
        if (required) element.setAttribute('aria-required', 'true');
        else element.removeAttribute('aria-required');
        if (disabled) element.setAttribute('aria-disabled', 'true');
        else element.removeAttribute('aria-disabled');
      });
    };
    labelEditors();
    const observer = new MutationObserver(labelEditors);
    observer.observe(root, { childList: true, subtree: true });
    return () => observer.disconnect();
  }, [disabled, labelId, required]);

  return (
    <div ref={rootRef} className={`markdown-field ${className}`.trim()}>
      <span className="field-label" id={labelId}>{label}</span>
      <div className={`markdown-editor${disabled ? ' disabled' : ''}`}>
        <MDXEditor
          key={language}
          ref={editorRef}
          className="markdown-editor-root"
          contentEditableClassName="markdown-editor-content"
          markdown={value}
          readOnly={disabled}
          translation={(key, defaultValue, interpolations) => editorTranslation(language, key, defaultValue, interpolations)}
          plugins={plugins}
          onChange={(markdown, initialMarkdownNormalize) => {
            setParseError(false);
            if (!initialMarkdownNormalize) onChange(markdown);
          }}
          onError={() => setParseError(true)}
        />
      </div>
      {parseError && <span className="error markdown-editor-error" role="alert">{t('markdownParseError')}</span>}
    </div>
  );
}
