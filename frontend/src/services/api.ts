const API_BASE = import.meta.env.VITE_API_URL ?? "";

export interface QueryResponse {
  question: string;
  sql: string;
  rows: Record<string, unknown>[];
  row_count: number;
  summary: string;
  chart: {
    type: string;
    x: string;
    y: string;
    title: string;
  } | null;
  execution_time_ms: number;
  trace: {
    tables_selected: string[];
    relationships_used: string[];
    schema_issues: string[];
    attempts: number;
    correction_history: { attempt: number; sql: string; error: string }[];
  };
  confidence: number;
}

export async function runQuery(
  question: string,
  provider: "openai" | "anthropic" = "openai",
): Promise<QueryResponse> {
  const res = await fetch(`${API_BASE}/ask`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ question, provider }),
  });

  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(String(err.detail ?? "Query failed"));
  }

  return res.json();
}

export async function fetchSchema(): Promise<Record<string, unknown>> {
  const res = await fetch(`${API_BASE}/schema`);
  if (!res.ok) throw new Error("Could not load schema");
  return res.json();
}

export interface SchemaGraphData {
  nodes: {
    id: string;
    columns: string[];
    primary_keys: string[];
    row_count: number;
  }[];
  edges: {
    from_table: string;
    from_col: string;
    to_table: string;
    to_col: string;
    source: "explicit_fk" | "heuristic";
  }[];
}

export async function fetchSchemaGraph(): Promise<SchemaGraphData> {
  const res = await fetch(`${API_BASE}/schema/graph`);
  if (!res.ok) throw new Error("Could not load schema graph");
  return res.json();
}

// ---------------------------------------------------------------------------
// Streaming types
// ---------------------------------------------------------------------------

export type StreamEvent =
  | { type: "status";     data: string }
  | { type: "generating"; data: null }
  | { type: "token";      data: string }
  | { type: "correction"; data: { attempt: number; error: string } }
  | { type: "result";     data: QueryResponse }
  | { type: "error";      data: string };

export type StreamCallbacks = {
  onStatus:     (msg: string) => void;
  onGenerating: () => void;
  onToken:      (token: string) => void;
  onCorrection: (info: { attempt: number; error: string }) => void;
  onResult:     (result: QueryResponse) => void;
  onError:      (msg: string) => void;
};

// ---------------------------------------------------------------------------
// Session management
// ---------------------------------------------------------------------------

export interface SessionInfo {
  session_id: string;
  turn_count: number;
}

export async function createSession(): Promise<SessionInfo> {
  const res = await fetch(`${API_BASE}/sessions`, { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}" });
  if (!res.ok) throw new Error("Could not create session");
  return res.json();
}

export async function deleteSession(sessionId: string): Promise<void> {
  await fetch(`${API_BASE}/sessions/${sessionId}`, { method: "DELETE" });
}

// ---------------------------------------------------------------------------
// Streaming (with optional session_id for multi-turn)
// ---------------------------------------------------------------------------

/**
 * Stream a query via SSE. Calls the appropriate callback for each event type.
 * Returns a cleanup function that aborts the fetch.
 */
export function streamQuery(
  question: string,
  callbacks: StreamCallbacks,
  provider: "openai" | "anthropic" = "openai",
  sessionId?: string,
): () => void {
  const controller = new AbortController();

  (async () => {
    let res: Response;
    try {
      res = await fetch(`${API_BASE}/ask/stream`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ question, provider, ...(sessionId ? { session_id: sessionId } : {}) }),
        signal: controller.signal,
      });
    } catch (err) {
      if ((err as Error).name !== "AbortError") {
        callbacks.onError(err instanceof Error ? err.message : "Network error");
      }
      return;
    }

    if (!res.ok) {
      const errData = await res.json().catch(() => ({ detail: res.statusText }));
      callbacks.onError(String(errData.detail ?? "Query failed"));
      return;
    }

    const reader = res.body!.getReader();
    const decoder = new TextDecoder();
    let buffer = "";

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });

      // SSE blocks are separated by double newlines
      const blocks = buffer.split("\n\n");
      buffer = blocks.pop() ?? "";

      for (const block of blocks) {
        const dataLine = block.split("\n").find((l) => l.startsWith("data: "));
        if (!dataLine) continue;
        let event: StreamEvent;
        try {
          event = JSON.parse(dataLine.slice(6)) as StreamEvent;
        } catch {
          continue;
        }
        switch (event.type) {
          case "status":     callbacks.onStatus(event.data);     break;
          case "generating": callbacks.onGenerating();            break;
          case "token":      callbacks.onToken(event.data);      break;
          case "correction": callbacks.onCorrection(event.data); break;
          case "result":     callbacks.onResult(event.data);     break;
          case "error":      callbacks.onError(event.data);      break;
        }
      }
    }
  })();

  return () => controller.abort();
}
