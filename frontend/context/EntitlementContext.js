import { createContext, useContext, useEffect, useMemo, useState } from "react";

import { fetchCurrentUser } from "../lib/api";

const DEFAULT_FEATURES = Object.freeze({
  live_incident_board: false,
});

const EntitlementContext = createContext({
  currentUser: null,
  subscriptionTier: "free",
  features: DEFAULT_FEATURES,
  loading: true,
  hasFeature: () => false,
  refresh: async () => null,
});

export function EntitlementProvider({ children, tokenProvider = null }) {
  const [currentUser, setCurrentUser] = useState(null);
  const [loading, setLoading] = useState(true);

  async function loadCurrentUser() {
    setLoading(true);
    try {
      const token = tokenProvider ? await tokenProvider() : null;
      const user = await fetchCurrentUser(token);
      setCurrentUser(user);
      return user;
    } catch {
      setCurrentUser(null);
      return null;
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    let cancelled = false;

    async function run() {
      setLoading(true);
      try {
        const token = tokenProvider ? await tokenProvider() : null;
        const user = await fetchCurrentUser(token);
        if (!cancelled) {
          setCurrentUser(user);
        }
      } catch {
        if (!cancelled) {
          setCurrentUser(null);
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }

    run();
    return () => {
      cancelled = true;
    };
  }, [tokenProvider]);

  const value = useMemo(() => {
    const features = currentUser?.features || DEFAULT_FEATURES;
    return {
      currentUser,
      subscriptionTier: currentUser?.subscription_tier || "free",
      features,
      loading,
      hasFeature: (name) => Boolean(features?.[name]),
      refresh: loadCurrentUser,
    };
  }, [currentUser, loading]);

  return <EntitlementContext.Provider value={value}>{children}</EntitlementContext.Provider>;
}

export function useEntitlements() {
  return useContext(EntitlementContext);
}
