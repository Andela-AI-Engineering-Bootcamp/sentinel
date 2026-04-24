import { ClerkProvider, useAuth } from "@clerk/nextjs";
import { AnalyzeSessionProvider } from "../context/AnalyzeSessionContext";
import { EntitlementProvider } from "../context/EntitlementContext";
import { isClerkEnabled } from "../lib/clerk";
import "../styles/globals.css";

const clerkEnabled = isClerkEnabled();

function AppProviders({ children, tokenProvider = null }) {
  return (
    <EntitlementProvider tokenProvider={tokenProvider}>
      <AnalyzeSessionProvider>{children}</AnalyzeSessionProvider>
    </EntitlementProvider>
  );
}

function ClerkAwareProviders({ children }) {
  const { getToken } = useAuth();
  return <AppProviders tokenProvider={getToken}>{children}</AppProviders>;
}

export default function App({ Component, pageProps }) {
  if (!clerkEnabled) {
    return (
      <AppProviders>
        <Component {...pageProps} />
      </AppProviders>
    );
  }

  return (
    <ClerkProvider {...pageProps}>
      <ClerkAwareProviders>
        <Component {...pageProps} />
      </ClerkAwareProviders>
    </ClerkProvider>
  );
}
