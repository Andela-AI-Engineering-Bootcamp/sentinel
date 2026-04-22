import { createContext, useCallback, useContext, useEffect, useMemo, useState } from "react";

const STORAGE_KEY = "sentinel-analyze-session";

const defaultDraft = {
  title: "Production incident",
  source: "manual",
  text: "",
};

function readStored() {
  if (typeof window === "undefined") return null;
  try {
    const raw = sessionStorage.getItem(STORAGE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    return {
      result: parsed.result ?? null,
      pipelineEvents: Array.isArray(parsed.pipelineEvents) ? parsed.pipelineEvents : [],
      error: typeof parsed.error === "string" ? parsed.error : "",
      draft: { ...defaultDraft, ...(parsed.draft && typeof parsed.draft === "object" ? parsed.draft : {}) },
    };
  } catch {
    return null;
  }
}

const AnalyzeSessionContext = createContext(null);

export function AnalyzeSessionProvider({ children }) {
  const [result, setResult] = useState(null);
  const [pipelineEvents, setPipelineEvents] = useState([]);
  const [error, setError] = useState("");
  const [draft, setDraftState] = useState(defaultDraft);
  const [hydrated, setHydrated] = useState(false);

  useEffect(() => {
    const s = readStored();
    if (s) {
      setResult(s.result);
      setPipelineEvents(s.pipelineEvents);
      setError(s.error);
      setDraftState(s.draft);
    }
    setHydrated(true);
  }, []);

  useEffect(() => {
    if (!hydrated || typeof window === "undefined") return;
    try {
      sessionStorage.setItem(
        STORAGE_KEY,
        JSON.stringify({ result, pipelineEvents, error, draft }),
      );
    } catch {
      /* ignore quota or JSON errors */
    }
  }, [hydrated, result, pipelineEvents, error, draft]);

  const updateDraft = useCallback((partial) => {
    setDraftState((d) => ({ ...d, ...partial }));
  }, []);

  const clearAnalysis = useCallback(() => {
    setResult(null);
    setPipelineEvents([]);
    setError("");
    setDraftState({ ...defaultDraft });
    if (typeof window !== "undefined") {
      try {
        sessionStorage.removeItem(STORAGE_KEY);
      } catch {
        /* noop */
      }
    }
  }, []);

  const value = useMemo(
    () => ({
      result,
      setResult,
      pipelineEvents,
      setPipelineEvents,
      error,
      setError,
      draft,
      updateDraft,
      clearAnalysis,
      hydrated,
    }),
    [result, pipelineEvents, error, draft, hydrated, updateDraft, clearAnalysis],
  );

  return <AnalyzeSessionContext.Provider value={value}>{children}</AnalyzeSessionContext.Provider>;
}

export function useAnalyzeSession() {
  const ctx = useContext(AnalyzeSessionContext);
  if (!ctx) {
    throw new Error("useAnalyzeSession must be used within AnalyzeSessionProvider");
  }
  return ctx;
}
