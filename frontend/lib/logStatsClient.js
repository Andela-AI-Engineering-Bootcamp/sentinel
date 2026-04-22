/**
 * Client-side log stats (fallback if server did not return log_stats, e.g. old session).
 * Kept in sync with backend common/log_stats.py heuristics.
 */
export function computeLogStatsFromText(text) {
  if (!text || !String(text).trim()) {
    return {
      line_count: 0,
      char_count: 0,
      levels: { error: 0, warn: 0, info: 0, debug: 0, other: 0 },
      http_status: {},
      http_class: { "2xx": 0, "3xx": 0, "4xx": 0, "5xx": 0, other: 0 },
      signal_keywords: {},
      buckets: [],
      timestamped_points: 0,
    };
  }
  const body = String(text);
  const lines = body.split(/\r?\n/);
  const n = lines.length;
  const levelRules = [
    [/\b(?:FATAL|CRITICAL|CRIT)\b|^\s*\[?CRITICAL\]?[:,\s]/i, "error"],
    [/\bERROR\b|^\s*\[?ERROR\]?[:,\s]|\[E\]\s|level[=:]error/i, "error"],
    [/exception|traceback|stack\s*trace|oomkilled|out\s*of\s*memory|segfault|panic/i, "error"],
    [/\bWARN(?:ING)?\b|^\s*\[?WARN\]?[:,\s]|\[W\]\s|level[=:]warn/i, "warn"],
    [/\bINFO\b|^\s*\[?INFO\]?[:,\s]|\[I\]\s|level[=:]info/i, "info"],
    [/\bDEBUG\b|^\s*\[?DEBUG\]?[:,\s]|\[D\]\s|level[=:]debug/i, "debug"],
  ];
  const lineLevel = (line) => {
    for (const [re, name] of levelRules) {
      if (re.test(line)) return name;
    }
    return "other";
  };
  const levels = { error: 0, warn: 0, info: 0, debug: 0, other: 0 };
  for (const line of lines) {
    const lv = lineLevel(line);
    levels[lv] += 1;
  }
  const httpRe = /(?<![0-9])([1-5][0-9]{2})(?![0-9])/g;
  const httpClass = { "2xx": 0, "3xx": 0, "4xx": 0, "5xx": 0, other: 0 };
  const httpStatus = {};
  let m;
  while ((m = httpRe.exec(body)) !== null) {
    const code = parseInt(m[1], 10);
    if (code < 100 || code > 599) continue;
    const s = String(code);
    httpStatus[s] = (httpStatus[s] || 0) + 1;
    if (code >= 200 && code < 300) httpClass["2xx"] += 1;
    else if (code < 400) httpClass["3xx"] += 1;
    else if (code < 500) httpClass["4xx"] += 1;
    else if (code < 600) httpClass["5xx"] += 1;
    else httpClass.other += 1;
  }
  const timeHint = /(?:\d{4}-\d{2}-\d{2}T|\d{4}-\d{2}-\d{2} |\d{2}\/[A-Za-z]{3}\/\d{4})/;
  const timestampedPoints = lines.filter((l) => timeHint.test(l)).length;
  const maxBuckets = 12;
  const nb = Math.min(maxBuckets, Math.max(1, n));
  const bucketSize = Math.max(1, Math.ceil(n / nb));
  const buckets = [];
  let idx = 0;
  let bnum = 0;
  while (idx < n && bnum < nb) {
    const end = Math.min(n, idx + bucketSize);
    const chunk = lines.slice(idx, end);
    let err = 0;
    let wrn = 0;
    for (const line of chunk) {
      const lv = lineLevel(line);
      if (lv === "error") err += 1;
      if (lv === "warn") wrn += 1;
    }
    const startLine = idx + 1;
    const endLine = end;
    buckets.push({
      label: `L${startLine}–L${endLine}`,
      line_start: startLine,
      line_end: endLine,
      error: err,
      warn: wrn,
    });
    bnum += 1;
    idx = end;
  }
  const signalRules = [
    ["timeout", /\btimeout|timed?\s*out|deadline\s*exceed/i],
    ["connection", /\brefused|econnrefused|connection\s*reset|broken\s*pipe/i],
    ["auth", /\b401\b|\b403\b|unauthor|forbidden|denied|invalid\s*token|jwt/i],
    ["throttle", /throttl|rate[\s-]?limit|429|too many requests|quota/i],
    ["database", /\bpostgres|\bmysql|sql\s*error|deadlock|constraint/i],
  ];
  const sk = {};
  for (const [name, re] of signalRules) {
    sk[name] = re.test(body) ? 1 : 0;
  }
  return {
    line_count: n,
    char_count: body.length,
    levels,
    http_status: httpStatus,
    http_class: httpClass,
    signal_keywords: sk,
    buckets,
    timestamped_points: timestampedPoints,
  };
}

export function getLogStatsFromJob(job) {
  if (job?.log_stats && typeof job.log_stats === "object") {
    return job.log_stats;
  }
  return computeLogStatsFromText(job?.normalized_text || "");
}
