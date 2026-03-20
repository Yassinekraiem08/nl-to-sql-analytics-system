import { useState, useRef, useEffect, KeyboardEvent } from "react";
import { motion, AnimatePresence } from "framer-motion";
import {
  Database, BarChart3, MessageSquare, Table, FileText,
  Send, Loader2, AlertCircle, ChevronDown, Zap, RotateCcw,
  MessageCircle, PlusCircle, Clock, Download, Lightbulb,
} from "lucide-react";
import {
  BarChart, Bar, LineChart, Line, ScatterChart, Scatter,
  XAxis, YAxis, Tooltip, ResponsiveContainer, Cell,
} from "recharts";
import { toast } from "sonner";
import { streamQuery, QueryResponse, createSession, deleteSession, exportData } from "@/services/api";
import { ConfidenceBadge } from "@/components/SchemaGraph";

const ease: [number, number, number, number] = [0.16, 1, 0.3, 1];

const SAMPLES = [
  "Top 10 customers by total spend",
  "Monthly revenue for 2023",
  "Revenue by product category",
  "Best-selling products by units sold",
  "Orders placed by users from New York",
  "Average order value for completed orders",
];

const TEAL = "hsl(174 72% 52%)";
const TOOLTIP_STYLE = {
  background: "hsl(220 18% 7%)",
  border: "1px solid hsl(220 14% 14%)",
  borderRadius: 8,
  fontSize: 12,
  color: "hsl(210 20% 92%)",
};

// ── Pipeline phases ───────────────────────────────────────────────────────────

type Phase = "idle" | "analyzing" | "generating" | "executing" | "formatting" | "done" | "error";

const PHASE_LABEL: Record<Phase, string> = {
  idle:       "",
  analyzing:  "Analyzing schema…",
  generating: "Generating SQL…",
  executing:  "Running query…",
  formatting: "Generating narrative…",
  done:       "",
  error:      "",
};

// ── Chart renderer ────────────────────────────────────────────────────────────

function QueryChart({ chart, rows }: {
  chart: NonNullable<QueryResponse["chart"]>;
  rows: Record<string, unknown>[];
}) {
  const data = rows.slice(0, 50);
  if (chart.type === "line") {
    return (
      <ResponsiveContainer width="100%" height={200}>
        <LineChart data={data}>
          <XAxis dataKey={chart.x} tick={{ fontSize: 10, fill: "#64748b" }} tickLine={false} axisLine={false} />
          <YAxis tick={{ fontSize: 10, fill: "#64748b" }} tickLine={false} axisLine={false} width={48} />
          <Tooltip contentStyle={TOOLTIP_STYLE} />
          <Line type="monotone" dataKey={chart.y} stroke={TEAL} strokeWidth={2} dot={false} />
        </LineChart>
      </ResponsiveContainer>
    );
  }
  if (chart.type === "scatter") {
    return (
      <ResponsiveContainer width="100%" height={200}>
        <ScatterChart>
          <XAxis dataKey={chart.x} name={chart.x} tick={{ fontSize: 10, fill: "#64748b" }} tickLine={false} axisLine={false} />
          <YAxis dataKey={chart.y} name={chart.y} tick={{ fontSize: 10, fill: "#64748b" }} tickLine={false} axisLine={false} width={48} />
          <Tooltip contentStyle={TOOLTIP_STYLE} />
          <Scatter data={data} fill={TEAL} />
        </ScatterChart>
      </ResponsiveContainer>
    );
  }
  return (
    <ResponsiveContainer width="100%" height={200}>
      <BarChart data={data} barCategoryGap="30%">
        <XAxis dataKey={chart.x} tick={{ fontSize: 10, fill: "#64748b" }} tickLine={false} axisLine={false} />
        <YAxis tick={{ fontSize: 10, fill: "#64748b" }} tickLine={false} axisLine={false} width={48} />
        <Tooltip contentStyle={TOOLTIP_STYLE} cursor={{ fill: "rgba(255,255,255,0.03)" }} />
        <Bar dataKey={chart.y} radius={[3, 3, 0, 0]}>
          {data.map((_, i) => (
            <Cell key={i} fill={`hsl(174 ${72 - i * 3}% ${52 - i * 1}%)`} />
          ))}
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  );
}

// ── Blinking cursor ───────────────────────────────────────────────────────────

function Cursor() {
  return (
    <motion.span
      className="inline-block w-[2px] h-[1em] bg-primary align-middle ml-0.5"
      animate={{ opacity: [1, 0] }}
      transition={{ duration: 0.6, repeat: Infinity, repeatType: "reverse" }}
    />
  );
}

// ── Main component ────────────────────────────────────────────────────────────

type HistoryEntry = { question: string; sql: string; summary: string };

export default function QueryTerminal() {
  const [question, setQuestion] = useState("");
  const [phase, setPhase] = useState<Phase>("idle");
  const [statusMsg, setStatusMsg] = useState("");
  const [streamedSql, setStreamedSql] = useState("");
  const [corrections, setCorrections] = useState<{ attempt: number; error: string }[]>([]);
  const [result, setResult] = useState<QueryResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [traceOpen, setTraceOpen] = useState(false);
  const [hintsOpen, setHintsOpen] = useState(false);
  const [sessionId, setSessionId] = useState<string | null>(null);
  const [history, setHistory] = useState<HistoryEntry[]>([]);
  const cancelRef = useRef<(() => void) | null>(null);
  const sqlEndRef = useRef<HTMLDivElement>(null);

  // Create session on mount
  useEffect(() => {
    createSession().then((s) => setSessionId(s.session_id)).catch(() => {});
    return () => { cancelRef.current?.(); };
  }, []);

  const startNewConversation = async () => {
    cancelRef.current?.();
    if (sessionId) await deleteSession(sessionId).catch(() => {});
    const s = await createSession().catch(() => null);
    if (s) setSessionId(s.session_id);
    setHistory([]);
    setResult(null);
    setError(null);
    setPhase("idle");
    setQuestion("");
  };

  // Auto-scroll the SQL pre block as tokens arrive
  useEffect(() => {
    if (phase === "generating") {
      sqlEndRef.current?.scrollIntoView({ behavior: "smooth", block: "nearest" });
    }
  }, [streamedSql, phase]);

  const isRunning = phase !== "idle" && phase !== "done" && phase !== "error";

  const submit = (q?: string) => {
    const query = (q ?? question).trim();
    if (!query || isRunning) return;
    if (q) setQuestion(q);

    // Reset state
    setPhase("analyzing");
    setStatusMsg("Analyzing schema…");
    setStreamedSql("");
    setCorrections([]);
    setResult(null);
    setError(null);
    setTraceOpen(false);

    cancelRef.current = streamQuery(query, {
      onStatus: (msg) => {
        setStatusMsg(msg);
        if (msg.includes("Running")) setPhase("executing");
        else if (msg.includes("narrative")) setPhase("formatting");
        else setPhase("analyzing");
      },
      onGenerating: () => {
        setPhase("generating");
        setStatusMsg("");
        setStreamedSql("");  // clear for new attempt
      },
      onToken: (token) => {
        setStreamedSql((prev) => prev + token);
      },
      onCorrection: (info) => {
        setCorrections((prev) => [...prev, info]);
      },
      onResult: (data) => {
        setResult(data);
        setPhase("done");
        setHintsOpen(false);
        setHistory((prev) => [...prev, {
          question: query,
          sql: data.sql,
          summary: data.summary,
        }]);
      },
      onError: (msg) => {
        setError(msg);
        setPhase("error");
        toast.error(msg);
      },
    }, "openai", sessionId ?? undefined);
  };

  const cancel = () => {
    cancelRef.current?.();
    setPhase("idle");
  };

  const onKeyDown = (e: KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) {
      e.preventDefault();
      submit();
    }
  };

  const columns = result?.rows.length ? Object.keys(result.rows[0]) : [];

  return (
    <div className="rounded-xl border border-border bg-card shadow-layered overflow-hidden">

      {/* Terminal chrome */}
      <div className="flex items-center gap-2 px-4 py-3 border-b border-border bg-muted/20">
        <div className="h-2.5 w-2.5 rounded-full bg-destructive/50" />
        <div className="h-2.5 w-2.5 rounded-full bg-yellow-500/50" />
        <div className="h-2.5 w-2.5 rounded-full bg-green-500/50" />
        <span className="ml-2 text-xs text-muted-foreground font-mono">querymind.sql</span>

        {/* Turn counter */}
        {history.length > 0 && (
          <span className="flex items-center gap-1 text-[10px] font-mono text-muted-foreground/60 ml-2">
            <MessageCircle className="h-3 w-3" />
            {history.length} turn{history.length !== 1 ? "s" : ""}
          </span>
        )}

        {/* Cache hit badge */}
        {result?.cache_hit && (
          <span className="flex items-center gap-1 text-[10px] font-mono px-1.5 py-0.5 rounded bg-green-500/10 text-green-400 border border-green-500/20 ml-2">
            <Zap className="h-2.5 w-2.5" />
            cached
          </span>
        )}

        <div className="ml-auto flex items-center gap-2">
          {history.length > 0 && (
            <button
              onClick={startNewConversation}
              className="flex items-center gap-1 text-[10px] font-mono text-muted-foreground/60 hover:text-primary transition-colors"
              title="Start a new conversation"
            >
              <PlusCircle className="h-3 w-3" />
              new
            </button>
          )}
          <div className="flex items-center gap-1 text-[10px] font-mono text-muted-foreground/60">
            <span className="w-1.5 h-1.5 rounded-full bg-primary animate-pulse-glow" />
            live
          </div>
        </div>
      </div>

      {/* Conversation history (collapsed pill list) */}
      <AnimatePresence>
        {history.length > 0 && (
          <motion.div
            initial={{ opacity: 0, height: 0 }}
            animate={{ opacity: 1, height: "auto" }}
            className="px-5 py-3 border-b border-border bg-muted/10 space-y-1.5"
          >
            <p className="text-[9px] font-semibold uppercase tracking-widest text-muted-foreground/40 mb-2 flex items-center gap-1">
              <Clock className="h-2.5 w-2.5" /> Conversation history
            </p>
            {history.map((h, i) => (
              <button
                key={i}
                onClick={() => { setQuestion(h.question); }}
                className="w-full text-left flex items-start gap-2 text-xs group"
              >
                <span className="font-mono text-[9px] text-muted-foreground/40 mt-0.5 shrink-0">
                  {String(i + 1).padStart(2, "0")}
                </span>
                <span className="text-muted-foreground/70 group-hover:text-foreground truncate transition-colors">
                  {h.question}
                </span>
              </button>
            ))}
          </motion.div>
        )}
      </AnimatePresence>

      {/* Input */}
      <div className="p-5 border-b border-border">
        <div className="flex items-start gap-3">
          <MessageSquare className="h-4 w-4 text-primary mt-2 shrink-0" />
          <textarea
            value={question}
            onChange={(e) => setQuestion(e.target.value)}
            onKeyDown={onKeyDown}
            placeholder="Ask a question about your data…"
            rows={2}
            disabled={isRunning}
            className="flex-1 bg-transparent text-sm text-foreground placeholder:text-muted-foreground/50 resize-none outline-none leading-relaxed font-sans disabled:opacity-60"
          />
          {isRunning ? (
            <button
              onClick={cancel}
              className="shrink-0 mt-0.5 flex items-center gap-1.5 bg-muted text-muted-foreground text-xs font-semibold px-3 py-1.5 rounded-md hover:opacity-90 transition-opacity"
            >
              <RotateCcw className="h-3.5 w-3.5" />
              Cancel
            </button>
          ) : (
            <button
              onClick={() => submit()}
              disabled={!question.trim()}
              className="shrink-0 mt-0.5 flex items-center gap-1.5 bg-primary text-primary-foreground text-xs font-semibold px-3 py-1.5 rounded-md hover:opacity-90 transition-opacity disabled:opacity-30 disabled:cursor-not-allowed"
            >
              <Send className="h-3.5 w-3.5" />
              Run
            </button>
          )}
        </div>
        <p className="mt-2 ml-7 text-[11px] text-muted-foreground/40 font-mono">⌘ Enter to run</p>
      </div>

      {/* Sample chips — always visible when not running */}
      {!isRunning && (
        <div className="px-5 py-4 border-b border-border">
          <p className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground/50 mb-3">
            {result ? "Try another" : "Try a sample query"}
          </p>
          <div className="flex flex-wrap gap-2">
            {SAMPLES.map((s) => (
              <button
                key={s}
                onClick={() => submit(s)}
                className="text-xs text-muted-foreground border border-border rounded-full px-3 py-1.5 hover:border-primary/60 hover:text-primary hover:bg-primary/5 transition-all duration-200"
              >
                {s}
              </button>
            ))}
          </div>
        </div>
      )}

      {/* ── Pipeline status bar ─────────────────────────────────────────── */}
      <AnimatePresence>
        {isRunning && (
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            className="px-5 py-3 border-b border-border bg-muted/10 flex items-center gap-2"
          >
            <Loader2 className="h-3.5 w-3.5 text-primary animate-spin shrink-0" />
            <span className="text-xs font-mono text-muted-foreground">
              {statusMsg || PHASE_LABEL[phase]}
            </span>
            {corrections.length > 0 && (
              <span className="ml-auto text-[10px] font-mono px-1.5 py-0.5 rounded bg-yellow-500/10 text-yellow-400 border border-yellow-500/20">
                correcting…
              </span>
            )}
          </motion.div>
        )}
      </AnimatePresence>

      {/* ── Live SQL stream ─────────────────────────────────────────────── */}
      <AnimatePresence>
        {(phase === "generating" || phase === "executing" || phase === "formatting") && streamedSql && (
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            className="p-5 border-b border-border bg-muted/20"
          >
            <div className="flex items-center gap-2 mb-3">
              <Database className="h-3.5 w-3.5 text-primary" />
              <span className="text-[10px] font-bold uppercase tracking-widest text-muted-foreground">
                Generated SQL
              </span>
              {corrections.length > 0 && (
                <span className="ml-1 text-[10px] font-mono text-yellow-400/70">
                  attempt {corrections.length + 1}
                </span>
              )}
            </div>
            <pre className="font-mono text-xs leading-relaxed overflow-x-auto text-foreground/80 whitespace-pre-wrap">
              {streamedSql}
              {phase === "generating" && <Cursor />}
            </pre>
            <div ref={sqlEndRef} />
          </motion.div>
        )}
      </AnimatePresence>

      {/* ── Error ──────────────────────────────────────────────────────── */}
      <AnimatePresence>
        {error && (
          <motion.div
            initial={{ opacity: 0, height: 0 }}
            animate={{ opacity: 1, height: "auto" }}
            exit={{ opacity: 0, height: 0 }}
            className="px-5 py-4 border-b border-border bg-destructive/5"
          >
            <div className="flex items-center gap-2 text-destructive text-sm">
              <AlertCircle className="h-4 w-4 shrink-0" />
              <span className="font-mono text-xs">{error}</span>
            </div>
          </motion.div>
        )}
      </AnimatePresence>

      {/* ── Final results ───────────────────────────────────────────────── */}
      <AnimatePresence>
        {result && phase === "done" && (
          <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} transition={{ duration: 0.3 }}>

            {/* SQL (final, with line-by-line reveal) */}
            <div className="p-5 border-b border-border bg-muted/20">
              <div className="flex items-center gap-2 mb-3">
                <Database className="h-3.5 w-3.5 text-primary" />
                <span className="text-[10px] font-bold uppercase tracking-widest text-muted-foreground">
                  Generated SQL
                </span>
              </div>
              <pre className="font-mono text-xs leading-relaxed overflow-x-auto">
                {result.sql.split("\n").map((line, i) => (
                  <motion.div
                    key={i}
                    initial={{ opacity: 0, x: -6 }}
                    animate={{ opacity: 1, x: 0 }}
                    transition={{ delay: i * 0.03, duration: 0.25, ease }}
                    className="text-foreground/80"
                  >
                    {line || "\u00A0"}
                  </motion.div>
                ))}
              </pre>
            </div>

            {/* Ambiguity warning */}
            {result.ambiguity_warning && (
              <div className="px-5 py-3 border-b border-border bg-yellow-500/5">
                <div className="flex items-start gap-2 text-yellow-400/80 text-xs">
                  <AlertCircle className="h-3.5 w-3.5 shrink-0 mt-0.5" />
                  <span className="font-mono leading-relaxed">{result.ambiguity_warning}</span>
                </div>
              </div>
            )}

            {/* Results table */}
            {result.rows.length > 0 && (
              <div className="p-5 border-b border-border">
                <div className="flex items-center gap-2 mb-3">
                  <Table className="h-3.5 w-3.5 text-primary" />
                  <span className="text-[10px] font-bold uppercase tracking-widest text-muted-foreground">Results</span>
                  <span className="ml-auto flex items-center gap-2 text-[10px] font-mono text-muted-foreground/60">
                    <span className="flex items-center gap-1">
                      <Zap className="h-3 w-3" />
                      {result.row_count.toLocaleString()} rows · {result.execution_time_ms.toFixed(0)}ms
                    </span>
                    <ConfidenceBadge score={result.confidence} />
                    {/* Export buttons */}
                    <button
                      onClick={() => exportData(result.sql, "csv", result.question.slice(0, 40)).catch((e) => toast.error(e.message))}
                      className="flex items-center gap-1 px-2 py-0.5 rounded border border-border hover:border-primary/50 hover:text-primary transition-all"
                      title="Export as CSV"
                    >
                      <Download className="h-2.5 w-2.5" />
                      CSV
                    </button>
                    <button
                      onClick={() => exportData(result.sql, "excel", result.question.slice(0, 40)).catch((e) => toast.error(e.message))}
                      className="flex items-center gap-1 px-2 py-0.5 rounded border border-border hover:border-primary/50 hover:text-primary transition-all"
                      title="Export as Excel"
                    >
                      <Download className="h-2.5 w-2.5" />
                      Excel
                    </button>
                  </span>
                </div>
                <div className="overflow-auto max-h-52 rounded-lg border border-border">
                  <table className="w-full text-xs">
                    <thead className="sticky top-0 bg-card">
                      <tr>
                        {columns.map((col) => (
                          <th key={col} className="text-left px-3 py-2 text-muted-foreground font-semibold whitespace-nowrap border-b border-border">
                            {col}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {result.rows.slice(0, 25).map((row, i) => (
                        <tr key={i} className="border-b border-border/40 hover:bg-muted/20 transition-colors">
                          {columns.map((col) => (
                            <td key={col} className="px-3 py-2 font-mono text-foreground/80 whitespace-nowrap">
                              {String(row[col] ?? "")}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}

            {/* Chart */}
            {result.chart && result.rows.length > 0 && (
              <div className="p-5 border-b border-border">
                <div className="flex items-center gap-2 mb-3">
                  <BarChart3 className="h-3.5 w-3.5 text-primary" />
                  <span className="text-[10px] font-bold uppercase tracking-widest text-muted-foreground">
                    {result.chart.title}
                  </span>
                </div>
                <QueryChart chart={result.chart} rows={result.rows} />
              </div>
            )}

            {/* Narrative */}
            <div className="p-5 border-b border-border">
              <div className="flex items-center gap-2 mb-3">
                <FileText className="h-3.5 w-3.5 text-primary" />
                <span className="text-[10px] font-bold uppercase tracking-widest text-muted-foreground">Narrative</span>
              </div>
              <p className="text-sm text-muted-foreground leading-relaxed">{result.summary}</p>
            </div>

            {/* Pipeline trace */}
            <div className="border-t border-border">
              <button
                onClick={() => setTraceOpen((o) => !o)}
                className="w-full flex items-center gap-2 px-5 py-3 text-xs text-muted-foreground hover:text-foreground transition-colors text-left"
              >
                <ChevronDown className={`h-3.5 w-3.5 transition-transform duration-200 ${traceOpen ? "rotate-180" : ""}`} />
                <span className="font-mono">Pipeline trace</span>
                <span className="ml-2 text-primary font-mono">
                  {result.trace.tables_selected.join(" · ")}
                </span>
                {result.trace.attempts > 1 && (
                  <span className="ml-auto text-[10px] font-mono px-1.5 py-0.5 rounded bg-yellow-500/10 text-yellow-400 border border-yellow-500/20">
                    {result.trace.attempts} attempts
                  </span>
                )}
              </button>
              <AnimatePresence>
                {traceOpen && (
                  <motion.div
                    initial={{ height: 0, opacity: 0 }}
                    animate={{ height: "auto", opacity: 1 }}
                    exit={{ height: 0, opacity: 0 }}
                    transition={{ duration: 0.2 }}
                    className="overflow-hidden"
                  >
                    <div className="px-5 pb-4 space-y-2 bg-muted/10">
                      {result.trace.relationships_used.length > 0 ? (
                        result.trace.relationships_used.map((r, i) => (
                          <div key={i} className="flex items-center gap-2 text-xs font-mono text-muted-foreground">
                            <span className="text-primary shrink-0">→</span>
                            <span>{r}</span>
                          </div>
                        ))
                      ) : (
                        <p className="text-xs font-mono text-muted-foreground/50">No joins needed</p>
                      )}
                      {result.trace.correction_history.length > 0 && (
                        <div className="mt-2 pt-2 border-t border-border space-y-1.5">
                          <p className="text-[10px] font-semibold uppercase tracking-widest text-yellow-400/70">
                            Self-corrections
                          </p>
                          {result.trace.correction_history.map((c, i) => (
                            <div key={i} className="flex gap-2 text-xs font-mono">
                              <span className="text-yellow-400 shrink-0">↺ attempt {c.attempt}</span>
                              <span className="text-destructive/70 truncate">{c.error}</span>
                            </div>
                          ))}
                        </div>
                      )}
                      {result.trace.schema_issues.length > 0 && (
                        <div className="mt-2 pt-2 border-t border-border">
                          {result.trace.schema_issues.map((issue, i) => (
                            <div key={i} className="flex items-center gap-2 text-xs text-yellow-500/80 font-mono">
                              <span>⚠</span><span>{issue}</span>
                            </div>
                          ))}
                        </div>
                      )}
                    </div>
                  </motion.div>
                )}
              </AnimatePresence>
            </div>

            {/* Performance hints */}
            {result.performance_hints.length > 0 && (
              <div className="border-t border-border">
                <button
                  onClick={() => setHintsOpen((o) => !o)}
                  className="w-full flex items-center gap-2 px-5 py-3 text-xs text-muted-foreground hover:text-foreground transition-colors text-left"
                >
                  <ChevronDown className={`h-3.5 w-3.5 transition-transform duration-200 ${hintsOpen ? "rotate-180" : ""}`} />
                  <Lightbulb className="h-3.5 w-3.5 text-yellow-400/70" />
                  <span className="font-mono">Performance hints</span>
                  <span className="ml-2 text-[10px] font-mono px-1.5 py-0.5 rounded bg-yellow-500/10 text-yellow-400 border border-yellow-500/20">
                    {result.performance_hints.length}
                  </span>
                </button>
                <AnimatePresence>
                  {hintsOpen && (
                    <motion.div
                      initial={{ height: 0, opacity: 0 }}
                      animate={{ height: "auto", opacity: 1 }}
                      exit={{ height: 0, opacity: 0 }}
                      transition={{ duration: 0.2 }}
                      className="overflow-hidden"
                    >
                      <div className="px-5 pb-4 space-y-1.5 bg-muted/10">
                        {result.performance_hints.map((hint, i) => (
                          <div key={i} className="flex items-start gap-2 text-xs font-mono text-yellow-400/70">
                            <span className="shrink-0 mt-0.5">→</span>
                            <span>{hint}</span>
                          </div>
                        ))}
                      </div>
                    </motion.div>
                  )}
                </AnimatePresence>
              </div>
            )}

          </motion.div>
        )}
      </AnimatePresence>

    </div>
  );
}
