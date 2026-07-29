import type { ToolJournalEntry, ToolJournalStorage } from "./types.js";
export declare class IndexedDbToolJournalStorage implements ToolJournalStorage {
    #private;
    constructor(factory?: IDBFactory);
    get(clientInstanceId: string, toolCallId: string): Promise<ToolJournalEntry | undefined>;
    put(entry: ToolJournalEntry): Promise<void>;
    delete(clientInstanceId: string, toolCallId: string): Promise<void>;
    list(clientInstanceId: string): Promise<ToolJournalEntry[]>;
}
export declare class MemoryToolJournalStorage implements ToolJournalStorage {
    #private;
    get(clientInstanceId: string, toolCallId: string): Promise<ToolJournalEntry | undefined>;
    put(entry: ToolJournalEntry): Promise<void>;
    delete(clientInstanceId: string, toolCallId: string): Promise<void>;
    list(clientInstanceId: string): Promise<ToolJournalEntry[]>;
}
//# sourceMappingURL=storage.d.ts.map