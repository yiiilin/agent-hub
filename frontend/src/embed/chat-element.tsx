import { createRoot, type Root } from 'react-dom/client';
import { I18nProvider } from '../i18n';
import { configureApiBase } from '../api/client';
import { WidgetApp } from '../widget';
import styles from '../styles.css?inline';

// `<agent-hub-chat>` 可嵌入 Web Component。
// 属性：
//   client-id  匿名 Integration App 的 client id（匿名模式）
//   api-base   Hub 基地址（跨域嵌入时必须；默认同源）
//   mode       bubble（默认，浮动气泡）| fullscreen（占满容器，iframe Widget 用）
//   lang       en | zh-CN（默认跟随浏览器）
// iframe Widget 场景（/widget?token=..&app=..）：fullscreen 模式从 URL 读取 token/app。

type Mode = 'bubble' | 'fullscreen';

type EmbedConfig = {
  clientId: string | null;
  apiBase: string | null;
  mode: Mode;
  lang: string | null;
};

function attributeValue(element: HTMLElement, name: string): string | null {
  const value = element.getAttribute(name);
  return value && value.trim() ? value.trim() : null;
}

function parseConfig(element: HTMLElement): EmbedConfig {
  const mode = attributeValue(element, 'mode');
  return {
    clientId: attributeValue(element, 'client-id'),
    apiBase: attributeValue(element, 'api-base'),
    mode: mode === 'fullscreen' ? 'fullscreen' : 'bubble',
    lang: attributeValue(element, 'lang')
  };
}

function widgetTokenFromUrl(): { token: string | null; appClientId: string | null } {
  const params = new URLSearchParams(window.location.search);
  return { token: params.get('token'), appClientId: params.get('app') };
}

export class AgentHubChatElement extends HTMLElement {
  private root: Root | null = null;
  private panel: HTMLElement | null = null;
  private open = false;

  connectedCallback() {
    const shadow = this.attachShadow({ mode: 'open' });
    const style = document.createElement('style');
    style.textContent = styles;
    shadow.appendChild(style);

    const config = parseConfig(this);
    if (config.apiBase) configureApiBase(config.apiBase);

    // 气泡外壳样式（Shadow DOM 内）。
    const shellStyle = document.createElement('style');
    shellStyle.textContent = `
      .agent-hub-chat-shell { position: relative; width: 100%; height: 100%; font-family: ui-sans-serif, system-ui, sans-serif; }
      .agent-hub-chat-launcher {
        position: fixed; right: 24px; bottom: 24px; z-index: 2147483000;
        width: 56px; height: 56px; border-radius: 50%; border: 0; cursor: pointer;
        display: grid; place-items: center;
        background: #24706d; color: #fff; box-shadow: 0 6px 20px rgb(0 0 0 / 25%);
      }
      .agent-hub-chat-launcher:hover { background: #1e5c59; }
      .agent-hub-chat-launcher svg { width: 26px; height: 26px; }
      .agent-hub-chat-panel {
        position: fixed; right: 24px; bottom: 92px; z-index: 2147483000;
        width: min(400px, calc(100vw - 48px)); height: min(620px, calc(100vh - 116px));
        border-radius: 16px; overflow: hidden;
        background: #fff; box-shadow: 0 12px 48px rgb(0 0 0 / 22%);
        display: flex; flex-direction: column;
        border: 1px solid rgb(0 0 0 / 8%);
      }
      .agent-hub-chat-panel.fullscreen {
        position: static; width: 100%; height: 100%; border-radius: 0;
        border: 0; box-shadow: none;
      }
    `;
    shadow.appendChild(shellStyle);

    const shell = document.createElement('div');
    shell.className = 'agent-hub-chat-shell';
    shadow.appendChild(shell);

    if (config.mode === 'fullscreen') {
      const panel = document.createElement('div');
      panel.className = 'agent-hub-chat-panel fullscreen';
      shell.appendChild(panel);
      this.panel = panel;
      this.open = true;
    } else {
      const launcher = document.createElement('button');
      launcher.type = 'button';
      launcher.className = 'agent-hub-chat-launcher';
      launcher.setAttribute('aria-label', 'AI assistant');
      launcher.innerHTML = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg>';
      const panel = document.createElement('div');
      panel.className = 'agent-hub-chat-panel';
      panel.hidden = true;
      shell.append(launcher, panel);
      launcher.addEventListener('click', () => {
        this.open = !this.open;
        panel.hidden = !this.open;
        launcher.setAttribute('aria-expanded', String(this.open));
      });
      this.panel = panel;
      this.open = false;
    }

    // iframe Widget 场景：从 URL 读取 token/app（fullscreen）。
    const { token, appClientId } = config.mode === 'fullscreen' ? widgetTokenFromUrl() : { token: null, appClientId: null };
    const resolvedClientId = config.clientId ?? (config.mode === 'fullscreen' ? appClientId : null);

    this.root = createRoot(this.panel);
    this.root.render(
      <I18nProvider>
        <WidgetApp
          token={token ?? undefined}
          appClientId={resolvedClientId ?? undefined}
          apiBase={config.apiBase ?? undefined}
          embeddedMode={config.mode}
        />
      </I18nProvider>
    );
  }

  disconnectedCallback() {
    this.root?.unmount();
    this.root = null;
  }
}
