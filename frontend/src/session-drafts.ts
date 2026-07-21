type StoredConversationDraft = {
  content: string;
};

type StoredConversationDrafts = Record<string, StoredConversationDraft>;

function draftStorageKey(userId: string) {
  return `agent-hub:conversation-drafts:${userId}`;
}

function selectedAgentStorageKey(userId: string) {
  return `agent-hub:selected-session-agent:${userId}`;
}

function readDrafts(userId: string): StoredConversationDrafts {
  try {
    const value = JSON.parse(localStorage.getItem(draftStorageKey(userId)) ?? '{}');
    if (!value || typeof value !== 'object' || Array.isArray(value)) return {};
    return Object.fromEntries(Object.entries(value).flatMap(([agentId, draft]) => {
      if (!draft || typeof draft !== 'object' || Array.isArray(draft)) return [];
      const content = (draft as Record<string, unknown>).content;
      return typeof content === 'string' ? [[agentId, { content }]] : [];
    }));
  } catch {
    return {};
  }
}

function writeDrafts(userId: string, drafts: StoredConversationDrafts) {
  try {
    if (Object.keys(drafts).length === 0) {
      localStorage.removeItem(draftStorageKey(userId));
    } else {
      localStorage.setItem(draftStorageKey(userId), JSON.stringify(drafts));
    }
  } catch {
    // A storage failure must not prevent the user from continuing in memory.
  }
}

export function loadConversationDraft(userId: string, agentId: string) {
  return readDrafts(userId)[agentId] ?? null;
}

export function saveConversationDraft(userId: string, agentId: string, content: string) {
  writeDrafts(userId, {
    ...readDrafts(userId),
    [agentId]: { content }
  });
}

export function discardConversationDraft(userId: string, agentId: string) {
  const drafts = readDrafts(userId);
  delete drafts[agentId];
  writeDrafts(userId, drafts);
}

export function clearConversationDrafts(userId: string) {
  try {
    localStorage.removeItem(draftStorageKey(userId));
  } catch {
    // Logout still proceeds when browser storage is unavailable.
  }
}

export function loadSelectedSessionAgent(userId: string) {
  try {
    return localStorage.getItem(selectedAgentStorageKey(userId));
  } catch {
    return null;
  }
}

export function saveSelectedSessionAgent(userId: string, agentId: string) {
  try {
    localStorage.setItem(selectedAgentStorageKey(userId), agentId);
  } catch {
    // The current in-memory selection remains usable.
  }
}
