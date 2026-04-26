import { createContext, useCallback, useContext, useEffect, useMemo, useState } from "react";

/** Legacy key — removed on load so full reload always starts a clean Analyze form. */
const STORAGE_KEY = "sentinel-analyze-session";

const defaultDraft = {
  title: "Production incident",
  source: "manual",
  text: "",
};

const AnalyzeSessionContext = createContext(null);

export function AnalyzeSessionProvider({ children }) {
  const [result, setResult] = useState(null);
  const [pipelineEvents, setPipelineEvents] = useState([]);
  const [error, setError] = useState("");
  const [draft, setDraftState] = useState(defaultDraft);
  const [hydrated, setHydrated] = useState(false);

  useEffect(() => {
    if (typeof window !== "undefined") {
      try {
        sessionStorage.removeItem(STORAGE_KEY);
      } catch {
        /* noop */
      }
    }
    setHydrated(true);
  }, []);

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
