import { ClerkProvider } from "@clerk/nextjs";
import { useAuth } from "@clerk/nextjs";
import { AnalyzeSessionProvider } from "../context/AnalyzeSessionContext";
import { EntitlementProvider } from "../context/EntitlementContext";
import { isClerkEnabled } from "../lib/clerk";
import "../styles/globals.css";

const clerkEnabled = isClerkEnabled();

function AppProviders({
  children,
  tokenProvider = null,
  authLoaded = true,
  signedIn = true,
  authUserId = "local",
}) {
  return (
    <EntitlementProvider
      tokenProvider={tokenProvider}
      authLoaded={authLoaded}
      signedIn={signedIn}
      authUserId={authUserId}
    >
      <AnalyzeSessionProvider>{children}</AnalyzeSessionProvider>
    </EntitlementProvider>
  );
}

function AuthenticatedProviders({ children }) {
  const { getToken, isLoaded, isSignedIn, userId } = useAuth();
  return (
    <AppProviders
      tokenProvider={getToken}
      authLoaded={isLoaded}
      signedIn={Boolean(isSignedIn)}
      authUserId={userId || ""}
    >
      {children}
    </AppProviders>
  );
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
      <AuthenticatedProviders>
        <Component {...pageProps} />
      </AuthenticatedProviders>
    </ClerkProvider>
  );
}
