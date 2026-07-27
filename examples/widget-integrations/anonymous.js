const response = await fetch('/api/public/config', { headers: { accept: 'application/json' } });
if (!response.ok) throw new Error(`Unable to load public Widget configuration (${response.status})`);
const configuration = await response.json();

document.querySelector('#app-name').textContent = configuration.app_name;
document.querySelector('#agent-name').textContent = configuration.agent_name;
const toolList = document.querySelector('#tool-list');
for (const tool of configuration.tools) {
  const item = document.createElement('code');
  item.textContent = tool;
  toolList.appendChild(item);
}

const iframe = document.querySelector('#widget');
const status = document.querySelector('#connection-status');
const statusContainer = status.parentElement;
iframe.src = configuration.widget_url;

window.addEventListener('message', (event) => {
  if (event.origin !== configuration.hub_origin || event.source !== iframe.contentWindow) return;
  if (event.data?.type === 'agent-hub:ready' && event.data.bound !== true) {
    iframe.contentWindow.postMessage({
      type: 'agent-hub:init',
      channelId: event.data.channelId
    }, configuration.hub_origin);
  }
  if (event.data?.type === 'agent-hub:ready' && event.data.bound === true) {
    status.textContent = '匿名会话已连接';
    statusContainer.classList.add('connected');
  }
  if (event.data?.type === 'agent-hub:resize' && Number.isFinite(event.data.height)) {
    iframe.style.minHeight = `${Math.max(620, Math.min(900, event.data.height))}px`;
  }
});
