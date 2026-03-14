import { useEffect, useState, useCallback } from "react";
import { motion } from "framer-motion";
import { Loader2, Database } from "lucide-react";
import { fetchSchemaGraph, SchemaGraphData } from "@/services/api";

// ── Constants ────────────────────────────────────────────────────────────────

const W = 560;
const H = 360;
const NODE_W = 130;
const NODE_H = 52;
const RADIUS = 130;
const CX = W / 2;
const CY = H / 2;

// Dark-theme colors (matching CSS variables)
const C = {
  bg:       "hsl(220 18% 7%)",
  border:   "hsl(220 14% 14%)",
  primary:  "hsl(174 72% 52%)",
  muted:    "hsl(215 12% 48%)",
  fg:       "hsl(210 20% 92%)",
  fgDim:    "hsl(210 20% 72%)",
  hover:    "hsl(220 16% 12%)",
  edgeHard: "hsl(174 72% 52% / 0.5)",
  edgeSoft: "hsl(220 14% 22%)",
};

// ── Layout helpers ────────────────────────────────────────────────────────────

function circleLayout(n: number): { x: number; y: number }[] {
  if (n === 1) return [{ x: CX, y: CY }];
  return Array.from({ length: n }, (_, i) => {
    const angle = (i * 2 * Math.PI) / n - Math.PI / 2;
    return {
      x: CX + RADIUS * Math.cos(angle),
      y: CY + RADIUS * Math.sin(angle),
    };
  });
}

function edgePath(
  x1: number, y1: number,
  x2: number, y2: number,
): string {
  const mx = (x1 + x2) / 2;
  const my = (y1 + y2) / 2;
  const dx = x2 - x1;
  const dy = y2 - y1;
  // Perpendicular offset for a subtle curve
  const off = Math.sqrt(dx * dx + dy * dy) * 0.15;
  const cpx = my - (-dy / Math.sqrt(dx * dx + dy * dy + 0.0001)) * off;
  const cpy = mx + (dx / Math.sqrt(dx * dx + dy * dy + 0.0001)) * off;
  return `M ${x1} ${y1} Q ${cpy} ${cpx} ${x2} ${y2}`;
}

// ── Confidence badge helper ────────────────────────────────────────────────────

export function ConfidenceBadge({ score }: { score: number }) {
  const pct = Math.round(score * 100);
  let color: string;
  if (score >= 0.9)       color = "text-emerald-400 bg-emerald-500/10 border-emerald-500/20";
  else if (score >= 0.75) color = "text-primary bg-primary/10 border-primary/20";
  else if (score >= 0.6)  color = "text-yellow-400 bg-yellow-500/10 border-yellow-500/20";
  else                    color = "text-destructive bg-destructive/10 border-destructive/20";

  return (
    <span className={`text-[10px] font-mono px-1.5 py-0.5 rounded border ${color}`}>
      confidence {pct}%
    </span>
  );
}

// ── Main component ────────────────────────────────────────────────────────────

export default function SchemaGraph() {
  const [data, setData] = useState<SchemaGraphData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [hovered, setHovered] = useState<string | null>(null);
  const [selected, setSelected] = useState<string | null>(null);

  useEffect(() => {
    fetchSchemaGraph()
      .then(setData)
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  const positions = data ? circleLayout(data.nodes.length) : [];
  const posMap = data
    ? Object.fromEntries(data.nodes.map((n, i) => [n.id, positions[i]]))
    : {};

  const isConnected = useCallback(
    (tableId: string) => {
      if (!hovered || !data) return true;
      if (tableId === hovered) return true;
      return data.edges.some(
        (e) =>
          (e.from_table === hovered && e.to_table === tableId) ||
          (e.to_table === hovered && e.from_table === tableId),
      );
    },
    [hovered, data],
  );

  const selectedNode = data?.nodes.find((n) => n.id === selected);

  if (loading) {
    return (
      <div className="flex items-center gap-2 h-24 text-muted-foreground text-sm">
        <Loader2 className="h-4 w-4 animate-spin" />
        Loading schema…
      </div>
    );
  }

  if (error || !data) {
    return (
      <p className="text-destructive text-sm font-mono">
        {error ?? "Could not load schema"}
      </p>
    );
  }

  return (
    <div className="flex flex-col lg:flex-row gap-6 items-start">

      {/* SVG graph */}
      <div className="rounded-xl border border-border bg-card overflow-hidden shrink-0">
        <svg
          width={W}
          height={H}
          viewBox={`0 0 ${W} ${H}`}
          className="block"
          style={{ maxWidth: "100%" }}
        >
          {/* Edges */}
          {data.edges.map((e, i) => {
            const from = posMap[e.from_table];
            const to   = posMap[e.to_table];
            if (!from || !to) return null;
            const active =
              !hovered ||
              e.from_table === hovered ||
              e.to_table === hovered;
            const isFK = e.source === "explicit_fk";
            return (
              <g key={i}>
                <path
                  d={edgePath(from.x, from.y, to.x, to.y)}
                  fill="none"
                  stroke={isFK ? C.edgeHard : C.edgeSoft}
                  strokeWidth={isFK ? 1.5 : 1}
                  strokeDasharray={isFK ? "none" : "4 3"}
                  opacity={active ? 1 : 0.2}
                  style={{ transition: "opacity 0.2s" }}
                />
                {/* FK label at midpoint */}
                {isFK && (
                  <text
                    x={(from.x + to.x) / 2}
                    y={(from.y + to.y) / 2 - 4}
                    textAnchor="middle"
                    fontSize={8}
                    fill={C.primary}
                    opacity={active ? 0.8 : 0.15}
                    style={{ transition: "opacity 0.2s" }}
                  >
                    FK
                  </text>
                )}
              </g>
            );
          })}

          {/* Nodes */}
          {data.nodes.map((node) => {
            const pos = posMap[node.id];
            if (!pos) return null;
            const x = pos.x - NODE_W / 2;
            const y = pos.y - NODE_H / 2;
            const isHov = hovered === node.id;
            const isSel = selected === node.id;
            const dimmed = hovered ? !isConnected(node.id) : false;

            return (
              <g
                key={node.id}
                style={{ cursor: "pointer" }}
                onMouseEnter={() => setHovered(node.id)}
                onMouseLeave={() => setHovered(null)}
                onClick={() => setSelected(selected === node.id ? null : node.id)}
              >
                {/* Glow ring when selected */}
                {isSel && (
                  <rect
                    x={x - 2} y={y - 2}
                    width={NODE_W + 4} height={NODE_H + 4}
                    rx={10}
                    fill="none"
                    stroke={C.primary}
                    strokeWidth={1}
                    opacity={0.4}
                  />
                )}
                {/* Node body */}
                <rect
                  x={x} y={y}
                  width={NODE_W} height={NODE_H}
                  rx={8}
                  fill={isHov ? C.hover : C.bg}
                  stroke={isHov || isSel ? C.primary : C.border}
                  strokeWidth={isHov || isSel ? 1.5 : 1}
                  opacity={dimmed ? 0.3 : 1}
                  style={{ transition: "all 0.15s" }}
                />
                {/* Table name */}
                <text
                  x={pos.x} y={y + 20}
                  textAnchor="middle"
                  fontSize={11}
                  fontWeight="600"
                  fill={isHov || isSel ? C.primary : C.fg}
                  opacity={dimmed ? 0.3 : 1}
                  style={{ transition: "all 0.15s", fontFamily: "monospace" }}
                >
                  {node.id}
                </text>
                {/* Row count + col count */}
                <text
                  x={pos.x} y={y + 36}
                  textAnchor="middle"
                  fontSize={9}
                  fill={C.muted}
                  opacity={dimmed ? 0.2 : 0.8}
                  style={{ transition: "opacity 0.15s" }}
                >
                  {node.columns.length} cols
                  {node.row_count >= 0 ? ` · ${node.row_count.toLocaleString()} rows` : ""}
                </text>
              </g>
            );
          })}
        </svg>
      </div>

      {/* Detail panel */}
      <div className="flex-1 min-w-0">
        {selectedNode ? (
          <motion.div
            key={selectedNode.id}
            initial={{ opacity: 0, x: 8 }}
            animate={{ opacity: 1, x: 0 }}
            transition={{ duration: 0.2 }}
            className="rounded-xl border border-border bg-card p-5"
          >
            <div className="flex items-center gap-2 mb-4">
              <Database className="h-4 w-4 text-primary" />
              <span className="font-mono font-semibold text-sm text-foreground">
                {selectedNode.id}
              </span>
              {selectedNode.row_count >= 0 && (
                <span className="ml-auto text-[10px] font-mono text-muted-foreground">
                  {selectedNode.row_count.toLocaleString()} rows
                </span>
              )}
            </div>
            <div className="space-y-1">
              {selectedNode.columns.map((col) => (
                <div key={col} className="flex items-center gap-2 text-xs font-mono">
                  <span className={selectedNode.primary_keys.includes(col)
                    ? "text-primary" : "text-muted-foreground"}>
                    {selectedNode.primary_keys.includes(col) ? "▸" : " "}
                  </span>
                  <span className={selectedNode.primary_keys.includes(col)
                    ? "text-foreground font-medium" : "text-muted-foreground"}>
                    {col}
                  </span>
                  {selectedNode.primary_keys.includes(col) && (
                    <span className="text-[9px] text-primary/60 font-mono">PK</span>
                  )}
                </div>
              ))}
            </div>
            <p className="mt-4 text-[10px] text-muted-foreground/50">
              Click another table or click again to deselect
            </p>
          </motion.div>
        ) : (
          <div className="rounded-xl border border-dashed border-border p-5 text-center">
            <p className="text-xs text-muted-foreground/50">
              Click a table node to inspect its columns
            </p>
            <p className="text-[10px] text-muted-foreground/30 mt-1">
              Solid edges = declared FK · Dashed = inferred
            </p>
          </div>
        )}
      </div>

    </div>
  );
}
