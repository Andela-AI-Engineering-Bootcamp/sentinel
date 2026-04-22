import { useMemo } from "react";

import { getLogStatsFromJob } from "../lib/logStatsClient";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Legend,
  Line,
  LineChart,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

const LEVEL_COLORS = {
  error: "#f43f5e",
  warn: "#f59e0b",
  info: "#38bdf8",
  debug: "#a78bfa",
  other: "#64748b",
};

const HTTP_CLASS_COLORS = {
  "2xx": "#22c55e",
  "3xx": "#38bdf8",
  "4xx": "#f59e0b",
  "5xx": "#ef4444",
  other: "#94a3b8",
};

function levelsPieData(levels) {
  if (!levels) return [];
  return Object.entries(levels)
    .filter(([, v]) => v > 0)
    .map(([name, value]) => ({ name, value }));
}

function httpClassBarData(httpClass) {
  if (!httpClass) return [];
  return Object.entries(httpClass)
    .filter(([, v]) => v > 0)
    .map(([name, value]) => ({ name, value }));
}

function signalBarData(signals) {
  if (!signals || typeof signals !== "object") return [];
  return Object.entries(signals).map(([name, value]) => ({ name, value: Number(value) ? 1 : 0 }));
}

export default function LogDataCharts({ result }) {
  const stats = useMemo(() => getLogStatsFromJob(result), [result]);
  const pieData = useMemo(() => levelsPieData(stats.levels), [stats]);
  const httpBar = useMemo(() => httpClassBarData(stats.http_class), [stats]);
  const signalBar = useMemo(() => signalBarData(stats.signal_keywords), [stats]);
  const lineData = useMemo(() => {
    if (!stats.buckets?.length) return [];
    return stats.buckets.map((b) => ({
      name: b.label,
      error: b.error,
      warn: b.warn,
    }));
  }, [stats]);

  if (!result || (!result.normalized_text && !result.log_stats)) {
    return null;
  }

  if (!stats.line_count) {
    return (
      <section className="log-charts card-elevated" aria-label="Log data visualization">
        <h2 className="run-title">Log data overview</h2>
        <p className="muted small">No log lines to chart yet. Run an analysis to see line-level patterns.</p>
      </section>
    );
  }

  return (
    <section className="log-charts card-elevated" aria-label="Log data visualization" id="log-data-charts">
      <div className="log-charts-head">
        <h2 className="run-title">Log data overview</h2>
        <p className="muted small">
          Heuristic parse of your input: line-level severities, HTTP status codes, and how errors and warnings are distributed
          across the file. Timestamps: <strong className="mono">{stats.timestamped_points}</strong> lines with time-like tokens.
        </p>
        <p className="muted small log-charts-meta">
          {stats.line_count} lines · {stats.char_count.toLocaleString()} characters
        </p>
      </div>

      <div className="log-charts-grid">
        <div className="log-charts-card">
          <h3 className="log-charts-h">Severity mix (per line)</h3>
          <p className="muted small log-charts-desc">Count of lines tagged ERROR / WARN / INFO / DEBUG / other.</p>
          {pieData.length > 0 ? (
            <div className="log-charts-viz" style={{ minHeight: 280 }}>
              <ResponsiveContainer width="100%" height={280}>
                <PieChart>
                  <Pie
                    data={pieData}
                    dataKey="value"
                    nameKey="name"
                    cx="50%"
                    cy="50%"
                    innerRadius={48}
                    outerRadius={100}
                    paddingAngle={2}
                    label={({ name, value }) => `${name}: ${value}`}
                  >
                    {pieData.map((entry) => (
                      <Cell key={entry.name} fill={LEVEL_COLORS[entry.name] || "#94a3b8"} />
                    ))}
                  </Pie>
                  <Tooltip />
                  <Legend />
                </PieChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <p className="muted small">No level tags detected.</p>
          )}
        </div>

        <div className="log-charts-card log-charts-card-wide">
          <h3 className="log-charts-h">Errors &amp; warnings across the log</h3>
          <p className="muted small log-charts-desc">Each point is a slice of line numbers; height shows error/warn density.</p>
          {lineData.length > 0 ? (
            <div className="log-charts-viz" style={{ minHeight: 280 }}>
              <ResponsiveContainer width="100%" height={280}>
                <LineChart data={lineData} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
                  <CartesianGrid stroke="rgba(148,163,184,0.25)" strokeDasharray="3 3" />
                  <XAxis dataKey="name" tick={{ fontSize: 10 }} interval={0} angle={-25} textAnchor="end" height={70} />
                  <YAxis allowDecimals={false} />
                  <Tooltip />
                  <Legend />
                  <Line type="monotone" dataKey="error" name="errors" stroke="#f43f5e" dot={false} strokeWidth={2} />
                  <Line type="monotone" dataKey="warn" name="warnings" stroke="#f59e0b" dot={false} strokeWidth={2} />
                </LineChart>
              </ResponsiveContainer>
            </div>
          ) : null}
        </div>

        <div className="log-charts-card">
          <h3 className="log-charts-h">HTTP status classes</h3>
          <p className="muted small log-charts-desc">Counts of 3-digit codes in the text (2xx–5xx).</p>
          {httpBar.length > 0 ? (
            <div className="log-charts-viz" style={{ minHeight: 240 }}>
              <ResponsiveContainer width="100%" height={240}>
                <BarChart data={httpBar} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
                  <CartesianGrid stroke="rgba(148,163,184,0.2)" vertical={false} />
                  <XAxis dataKey="name" tick={{ fontSize: 12 }} />
                  <YAxis allowDecimals={false} />
                  <Tooltip />
                  <Bar dataKey="value" name="Count" radius={[4, 4, 0, 0]}>
                    {httpBar.map((entry) => (
                      <Cell key={entry.name} fill={HTTP_CLASS_COLORS[entry.name] || "#64748b"} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <p className="muted small">No HTTP status codes found in the text.</p>
          )}
        </div>

        <div className="log-charts-card">
          <h3 className="log-charts-h">Reliability signals</h3>
          <p className="muted small log-charts-desc">Binary flags: timeout, connection, auth, throttle, database patterns.</p>
          {signalBar.length > 0 ? (
            <div className="log-charts-viz" style={{ minHeight: 220 }}>
              <ResponsiveContainer width="100%" height={220}>
                <BarChart
                  data={signalBar}
                  layout="vertical"
                  margin={{ top: 8, right: 16, left: 8, bottom: 0 }}
                >
                  <CartesianGrid stroke="rgba(148,163,184,0.2)" horizontal={false} />
                  <XAxis type="number" domain={[0, 1]} allowDecimals={false} />
                  <YAxis type="category" dataKey="name" width={90} tick={{ fontSize: 11 }} />
                  <Tooltip formatter={(v) => (v ? "matched" : "—")} />
                  <Bar dataKey="value" name="Signal" fill="#7c3aed" radius={[0, 4, 4, 0]}>
                    {signalBar.map((e) => (
                      <Cell key={e.name} fill={e.value ? "#7c3aed" : "rgba(148,163,184,0.25)"} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <p className="muted small">No signal keywords available.</p>
          )}
        </div>
      </div>
    </section>
  );
}
