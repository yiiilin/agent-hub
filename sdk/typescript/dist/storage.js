const DATABASE_NAME = "agent-hub-client";
const DATABASE_VERSION = 1;
const STORE_NAME = "tool-journal";
function entryKey(clientInstanceId, toolCallId) {
    return `${clientInstanceId}:${toolCallId}`;
}
function withoutKey(entry) {
    const { key: _key, ...journalEntry } = entry;
    return journalEntry;
}
function requestResult(request) {
    return new Promise((resolve, reject) => {
        request.onsuccess = () => resolve(request.result);
        request.onerror = () => reject(request.error ?? new Error("IndexedDB request failed"));
    });
}
function transactionDone(transaction) {
    return new Promise((resolve, reject) => {
        transaction.oncomplete = () => resolve();
        transaction.onerror = () => reject(transaction.error ?? new Error("IndexedDB transaction failed"));
        transaction.onabort = () => reject(transaction.error ?? new Error("IndexedDB transaction aborted"));
    });
}
export class IndexedDbToolJournalStorage {
    #factory;
    #databasePromise;
    constructor(factory = globalThis.indexedDB) {
        if (!factory) {
            throw new Error("IndexedDB is unavailable; provide a ToolJournalStorage adapter");
        }
        this.#factory = factory;
    }
    async get(clientInstanceId, toolCallId) {
        const database = await this.#database();
        const transaction = database.transaction(STORE_NAME, "readonly");
        const done = transactionDone(transaction);
        const stored = await requestResult(transaction.objectStore(STORE_NAME).get(entryKey(clientInstanceId, toolCallId)));
        await done;
        return stored ? withoutKey(stored) : undefined;
    }
    async put(entry) {
        const database = await this.#database();
        const transaction = database.transaction(STORE_NAME, "readwrite");
        transaction.objectStore(STORE_NAME).put({ ...entry, key: entryKey(entry.clientInstanceId, entry.toolCallId) });
        await transactionDone(transaction);
    }
    async delete(clientInstanceId, toolCallId) {
        const database = await this.#database();
        const transaction = database.transaction(STORE_NAME, "readwrite");
        transaction.objectStore(STORE_NAME).delete(entryKey(clientInstanceId, toolCallId));
        await transactionDone(transaction);
    }
    async list(clientInstanceId) {
        const database = await this.#database();
        const transaction = database.transaction(STORE_NAME, "readonly");
        const done = transactionDone(transaction);
        const stored = await requestResult(transaction.objectStore(STORE_NAME).getAll());
        await done;
        return stored
            .filter((entry) => entry.clientInstanceId === clientInstanceId)
            .map(withoutKey);
    }
    #database() {
        this.#databasePromise ??= new Promise((resolve, reject) => {
            const request = this.#factory.open(DATABASE_NAME, DATABASE_VERSION);
            request.onupgradeneeded = () => {
                if (!request.result.objectStoreNames.contains(STORE_NAME)) {
                    request.result.createObjectStore(STORE_NAME, { keyPath: "key" });
                }
            };
            request.onsuccess = () => resolve(request.result);
            request.onerror = () => reject(request.error ?? new Error("Unable to open the Agent Hub journal"));
        });
        return this.#databasePromise;
    }
}
export class MemoryToolJournalStorage {
    #entries = new Map();
    async get(clientInstanceId, toolCallId) {
        const entry = this.#entries.get(entryKey(clientInstanceId, toolCallId));
        return entry ? structuredClone(entry) : undefined;
    }
    async put(entry) {
        this.#entries.set(entryKey(entry.clientInstanceId, entry.toolCallId), structuredClone(entry));
    }
    async delete(clientInstanceId, toolCallId) {
        this.#entries.delete(entryKey(clientInstanceId, toolCallId));
    }
    async list(clientInstanceId) {
        return [...this.#entries.values()]
            .filter((entry) => entry.clientInstanceId === clientInstanceId)
            .map((entry) => structuredClone(entry));
    }
}
//# sourceMappingURL=storage.js.map