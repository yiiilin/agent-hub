import { AgentHubChatElement } from './chat-element';

if (!customElements.get('agent-hub-chat')) {
  customElements.define('agent-hub-chat', AgentHubChatElement);
}
