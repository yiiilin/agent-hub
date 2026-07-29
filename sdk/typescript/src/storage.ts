import type { ToolJournalEntry, ToolJournalStorage } from "./types.js";

const DATABASE_NAME = "agent-hub-client";
const DATABASE_VERSION = 1;
const STORE_NAME = "tool-journal";

type StoredEntry = ToolJournalEntry & { key: string };

function entryKey(clientInstanceId: string, toolCallId: string): string {
  return `${clientInstanceId}:${toolCallId}`;
}

function withoutKey(entry: StoredEntry): ToolJournalEntry {
  const { key: _key, ...journalEntry } = entry;
  return journalEntry;
}

function requestResult<T>(request: IDBRequest<T>): Promise<T> {
  return new Promise((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error ?? new Error("IndexedDB request failed"));
  });
}

function transactionDone(transaction: IDBTransaction): Promise<void> {
  return new Promise((resolve, reject) => {
    transaction.oncomplete = () => resolve();
    transaction.onerror = () => reject(transaction.error ?? new Error("IndexedDB transaction failed"));
    transaction.onabort = () => reject(transaction.error ?? new Error("IndexedDB transaction aborted"));
  });
}

export class IndexedDbToolJournalStorage implements ToolJournalStorage {
  readonly #factory: IDBFactory;
  #databasePromise?: Promise<IDBDatabase>;

  constructor(factory: IDBFactory = globalThis.indexedDB) {
    if (!factory) {
      throw new Error("IndexedDB is unavailable; provide a ToolJournalStorage adapter");
    }
    this.#factory = factory;
  }

  async get(clientInstanceId: string, toolCallId: string): Promise<ToolJournalEntry | undefined> {
    const database = await this.#database();
    const transaction = database.transaction(STORE_NAME, "readonly");
    const done = transactionDone(transaction);
    const stored = await requestResult(
      transaction.objectStore(STORE_NAME).get(entryKey(clientInstanceId, toolCallId)) as IDBRequest<StoredEntry | undefined>,
    );
    await done;
    return stored ? withoutKey(stored) : undefined;
  }

  async put(entry: ToolJournalEntry): Promise<void> {
    const database = await this.#database();
    const transaction = database.transaction(STORE_NAME, "readwrite");
    transaction.objectStore(STORE_NAME).put({ ...entry, key: entryKey(entry.clientInstanceId, entry.toolCallId) });
    await transactionDone(transaction);
  }

  async delete(clientInstanceId: string, toolCallId: string): Promise<void> {
    const database = await this.#database();
    const transaction = database.transaction(STORE_NAME, "readwrite");
    transaction.objectStore(STORE_NAME).delete(entryKey(clientInstanceId, toolCallId));
    await transactionDone(transaction);
  }

  async list(clientInstanceId: string): Promise<ToolJournalEntry[]> {
    const database = await this.#database();
    const transaction = database.transaction(STORE_NAME, "readonly");
    const done = transactionDone(transaction);
    const stored = await requestResult(
      transaction.objectStore(STORE_NAME).getAll() as IDBRequest<StoredEntry[]>,
    );
    await done;
    return stored
      .filter((entry) => entry.clientInstanceId === clientInstanceId)
      .map(withoutKey);
  }

  #database(): Promise<IDBDatabase> {
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

export class MemoryToolJournalStorage implements ToolJournalStorage {
  readonly #entries = new Map<string, ToolJournalEntry>();

  async get(clientInstanceId: string, toolCallId: string): Promise<ToolJournalEntry | undefined> {
    const entry = this.#entries.get(entryKey(clientInstanceId, toolCallId));
    return entry ? structuredClone(entry) : undefined;
  }

  async put(entry: ToolJournalEntry): Promise<void> {
    this.#entries.set(entryKey(entry.clientInstanceId, entry.toolCallId), structuredClone(entry));
  }

  async delete(clientInstanceId: string, toolCallId: string): Promise<void> {
    this.#entries.delete(entryKey(clientInstanceId, toolCallId));
  }

  async list(clientInstanceId: string): Promise<ToolJournalEntry[]> {
    return [...this.#entries.values()]
      .filter((entry) => entry.clientInstanceId === clientInstanceId)
      .map((entry) => structuredClone(entry));
  }
}
