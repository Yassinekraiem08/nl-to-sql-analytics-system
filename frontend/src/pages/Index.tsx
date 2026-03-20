import { motion } from "framer-motion";
import {
  Database, BarChart3, MessageSquare, Zap,
  ArrowRight, Terminal, Table, FileText,
} from "lucide-react";
import QueryTerminal from "@/components/QueryTerminal";
import SchemaGraph from "@/components/SchemaGraph";

const ease: [number, number, number, number] = [0.16, 1, 0.3, 1];

const fadeUp = {
  hidden: { opacity: 0, y: 24 },
  visible: (i: number) => ({
    opacity: 1, y: 0,
    transition: { delay: i * 0.1, duration: 0.7, ease },
  }),
};

const features = [
  { icon: MessageSquare, title: "Natural Language Input",     desc: "Ask questions in plain English. No SQL knowledge required." },
  { icon: Database,      title: "Intelligent SQL Generation", desc: "Schema-aware query builder that resolves joins and aliases automatically." },
  { icon: Table,         title: "Structured Tables",          desc: "Results rendered as sortable, filterable data tables." },
  { icon: BarChart3,     title: "Auto-Generated Charts",      desc: "Visualizations chosen based on data shape and query intent." },
  { icon: FileText,      title: "Narrative Explanations",     desc: "Plain-English summaries that explain what the data means." },
  { icon: Zap,           title: "Real-Time Pipeline",         desc: "Question → SQL → Execute → Visualize in under 2 seconds." },
];

const steps = [
  { num: "01", label: "Ask",     detail: "Type any question about your data in plain English." },
  { num: "02", label: "Parse",   detail: "Intent mapped to schema, joins resolved, validated SQL generated." },
  { num: "03", label: "Execute", detail: "Query runs against your database with sandboxed read-only access." },
  { num: "04", label: "Respond", detail: "Table + chart + narrative returned in a single unified response." },
];

export default function LandingPage() {
  return (
    <div className="min-h-screen bg-background overflow-hidden">

      {/* Nav */}
      <nav className="fixed top-0 inset-x-0 z-50 surface-glass">
        <div className="container flex items-center justify-between h-14">
          <div className="flex items-center gap-2">
            <Terminal className="h-5 w-5 text-primary" />
            <span className="font-semibold tracking-tight text-foreground">QueryMind</span>
          </div>
          <div className="hidden sm:flex items-center gap-6 text-sm text-muted-foreground">
            <a href="#features" className="hover:text-foreground transition-colors">Features</a>
            <a href="#how"      className="hover:text-foreground transition-colors">How it works</a>
            <a href="#demo"     className="hover:text-foreground transition-colors">Demo</a>
          </div>
          <a
            href="#demo"
            className="flex items-center gap-1.5 text-sm font-medium bg-primary text-primary-foreground px-3 py-1.5 rounded-md hover:opacity-90 transition-opacity"
          >
            Try demo <ArrowRight className="h-3.5 w-3.5" />
          </a>
        </div>
      </nav>

      {/* Hero */}
      <section className="container pt-32 pb-20">
        <motion.div initial="hidden" animate="visible" className="max-w-3xl">
          <motion.div
            variants={fadeUp} custom={0}
            className="inline-flex items-center gap-2 text-xs font-mono text-primary border border-primary/30 bg-primary/5 px-3 py-1 rounded-full mb-6"
          >
            <span className="w-1.5 h-1.5 rounded-full bg-primary animate-pulse-glow" />
            NL-to-SQL Analytics Engine
          </motion.div>

          <motion.h1
            variants={fadeUp} custom={1}
            className="text-5xl font-bold leading-tight tracking-tight text-foreground mb-4"
          >
            Ask questions in{" "}
            <span className="text-gradient">plain English.</span>
            <br />
            Get SQL-powered insights.
          </motion.h1>

          <motion.p
            variants={fadeUp} custom={2}
            className="text-lg text-muted-foreground leading-relaxed mb-6 max-w-lg"
          >
            Type a question. Get SQL, a results table, a chart, and a plain-English summary — live.
          </motion.p>

          <motion.div variants={fadeUp} custom={3} className="flex items-center gap-3 mb-8 text-xs font-mono text-muted-foreground/60">
            <span>212 tests</span>
            <span className="text-border">·</span>
            <span>SQLite + PostgreSQL</span>
            <span className="text-border">·</span>
            <span>Self-correcting pipeline</span>
            <span className="text-border">·</span>
            <span>&lt;2s queries</span>
          </motion.div>

          <motion.div variants={fadeUp} custom={4} className="flex items-center gap-4">
            <a
              href="#demo"
              className="flex items-center gap-2 bg-primary text-primary-foreground px-5 py-2.5 rounded-lg font-medium hover:opacity-90 transition-opacity"
            >
              Try it live <ArrowRight className="h-4 w-4" />
            </a>
            <a href="#how" className="text-sm text-muted-foreground hover:text-foreground transition-colors">
              How it works →
            </a>
          </motion.div>
        </motion.div>
      </section>

      {/* Live demo */}
      <section id="demo" className="container pb-24">
        <motion.div
          initial="hidden"
          whileInView="visible"
          viewport={{ once: true }}
          className="max-w-2xl"
        >
          <motion.div variants={fadeUp} custom={0} className="mb-6">
            <h2 className="text-2xl font-bold text-foreground mb-1">Live demo</h2>
            <p className="text-sm text-muted-foreground">
              Connected to a real backend. Every response is generated live.
            </p>
          </motion.div>

          <motion.div variants={fadeUp} custom={1}>
            <QueryTerminal />
          </motion.div>
        </motion.div>
      </section>

      {/* Schema graph */}
      <section id="schema" className="container max-w-4xl pb-24">
        <motion.div
          initial="hidden" whileInView="visible" viewport={{ once: true }}
          className="mb-8"
        >
          <motion.div variants={fadeUp} custom={0} className="mb-1 flex items-center gap-3">
            <h2 className="text-2xl font-bold text-foreground">Schema intelligence</h2>
            <span className="flex items-center gap-1.5 text-[10px] font-mono px-2 py-0.5 rounded-full border border-primary/30 bg-primary/5 text-primary">
              <span className="w-1.5 h-1.5 rounded-full bg-primary animate-pulse" />
              live from backend
            </span>
          </motion.div>
          <motion.p variants={fadeUp} custom={1} className="text-sm text-muted-foreground">
            Introspected at runtime — tables, foreign keys, and row counts pulled directly from the database.
            Click a node to inspect columns.
          </motion.p>
        </motion.div>
        <motion.div
          initial="hidden" whileInView="visible" viewport={{ once: true }}
          variants={fadeUp} custom={2}
        >
          <SchemaGraph />
        </motion.div>
      </section>

      {/* Features */}
      <section id="features" className="container max-w-5xl pb-24">
        <motion.div
          initial="hidden" whileInView="visible" viewport={{ once: true }}
          className="text-center mb-14"
        >
          <motion.h2 variants={fadeUp} custom={0} className="text-3xl font-bold text-foreground mb-2">
            Built for speed and clarity
          </motion.h2>
          <motion.p variants={fadeUp} custom={1} className="text-muted-foreground">
            Every component designed to minimize time from question to insight.
          </motion.p>
        </motion.div>

        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
          {features.map((f, i) => (
            <motion.div
              key={f.title}
              initial="hidden" whileInView="visible" viewport={{ once: true }}
              variants={fadeUp} custom={i}
              className="group p-5 rounded-xl border border-border bg-card hover:border-primary/40 hover:glow-sm transition-all duration-300"
            >
              <f.icon className="h-5 w-5 text-primary mb-3" />
              <h3 className="font-semibold text-sm mb-1.5 text-foreground">{f.title}</h3>
              <p className="text-xs text-muted-foreground leading-relaxed">{f.desc}</p>
            </motion.div>
          ))}
        </div>
      </section>

      {/* How it works */}
      <section id="how" className="container max-w-3xl pb-24">
        <motion.div
          initial="hidden" whileInView="visible" viewport={{ once: true }}
          className="text-center mb-14"
        >
          <motion.h2 variants={fadeUp} custom={0} className="text-3xl font-bold text-foreground mb-2">
            How it works
          </motion.h2>
          <motion.p variants={fadeUp} custom={1} className="text-muted-foreground">
            Four steps. One seamless pipeline.
          </motion.p>
        </motion.div>

        <div className="space-y-4">
          {steps.map((s, i) => (
            <motion.div
              key={s.num}
              initial="hidden" whileInView="visible" viewport={{ once: true }}
              variants={fadeUp} custom={i}
              className="flex items-start gap-5 p-5 rounded-xl border border-border bg-card"
            >
              <span className="font-mono text-xs text-primary font-bold mt-0.5 shrink-0">{s.num}</span>
              <div>
                <h3 className="font-semibold text-sm mb-1 text-foreground">{s.label}</h3>
                <p className="text-xs text-muted-foreground leading-relaxed">{s.detail}</p>
              </div>
            </motion.div>
          ))}
        </div>
      </section>

      {/* Footer */}
      <footer className="border-t border-border">
        <div className="container flex items-center justify-between py-6 text-xs text-muted-foreground">
          <div className="flex items-center gap-2">
            <Terminal className="h-4 w-4 text-primary" />
            <span className="font-medium text-foreground">QueryMind</span>
          </div>
          <span>Built with Python, SQL, and good taste. · <a href="https://github.com/Yassinekraiem08/nl-to-sql-analytics-system" target="_blank" rel="noopener noreferrer" className="hover:text-foreground transition-colors underline underline-offset-2">GitHub</a></span>
        </div>
      </footer>

    </div>
  );
}
