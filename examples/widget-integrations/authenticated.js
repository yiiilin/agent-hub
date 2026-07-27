const [configurationResponse, accessResponse] = await Promise.all([
  fetch('/api/authenticated/config', { headers: { accept: 'application/json' } }),
  fetch('/api/authenticated/widget-access', {
    method: 'POST',
    headers: { accept: 'application/json', 'x-requested-with': 'widget-example' }
  })
]);
if (!configurationResponse.ok) {
  throw new Error(`Unable to load authenticated Widget configuration (${configurationResponse.status})`);
}
if (!accessResponse.ok) {
  const status = document.querySelector('#connection-status');
  status.textContent = '身份凭证签发失败';
  status.parentElement.classList.add('error');
  throw new Error(`Unable to issue Widget access (${accessResponse.status})`);
}

const configuration = await configurationResponse.json();
const access = await accessResponse.json();
document.querySelector('#app-name').textContent = configuration.app_name;
document.querySelector('#agent-name').textContent = configuration.agent_name;
document.querySelector('#display-name').textContent = configuration.user.display_name;
document.querySelector('#username').textContent = `@${configuration.user.username}`;
document.querySelector('#tenant-id').textContent = configuration.user.tenant_id;
document.querySelector('#email').textContent = configuration.user.email;
document.querySelector('#user-initials').textContent = configuration.user.display_name
  .split(/\s+/)
  .map((part) => part[0])
  .join('')
  .slice(0, 2)
  .toUpperCase();
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
      channelId: event.data.channelId,
      token: access.token
    }, configuration.hub_origin);
  }
  if (event.data?.type === 'agent-hub:ready' && event.data.bound === true) {
    status.textContent = '用户会话已连接';
    statusContainer.classList.add('connected');
  }
  if (event.data?.type === 'agent-hub:resize' && Number.isFinite(event.data.height)) {
    iframe.style.minHeight = `${Math.max(620, Math.min(900, event.data.height))}px`;
  }
});
