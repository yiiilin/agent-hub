export const builtInTools = ['read', 'grep', 'find', 'ls', 'edit', 'write', 'bash', 'skill_exec', 'integration'] as const;
export const publicWidgetTools = ['read', 'grep', 'find', 'ls', 'integration'] as const;

export type BuiltInTool = (typeof builtInTools)[number];

export function normalizeToolAllowlist(value: readonly string[] | null | undefined) {
  const selected = new Set(value ?? []);
  return builtInTools.filter((tool) => selected.has(tool));
}

export function ToolAllowlistPicker({
  value,
  onChange,
  disabled = false,
  legend,
  tools = builtInTools
}: {
  value: readonly string[];
  onChange: (next: BuiltInTool[]) => void;
  disabled?: boolean;
  legend: string;
  tools?: readonly BuiltInTool[];
}) {
  const selected = new Set(value);
  return <fieldset className="agent-user-picker" disabled={disabled}>
    <legend>{legend}</legend>
    {tools.map((tool) => <label className="check-row" key={tool}>
      <input
        type="checkbox"
        checked={selected.has(tool)}
        onChange={(event) => onChange(event.target.checked
          ? tools.filter((candidate) => candidate === tool || selected.has(candidate))
          : tools.filter((candidate) => candidate !== tool && selected.has(candidate)))}
      />
      <code>{tool}</code>
    </label>)}
  </fieldset>;
}
