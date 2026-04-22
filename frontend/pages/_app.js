import { ClerkProvider } from "@clerk/nextjs";
import { AnalyzeSessionProvider } from "../context/AnalyzeSessionContext";
import { isClerkEnabled } from "../lib/clerk";
import "../styles/globals.css";

const clerkEnabled = isClerkEnabled();

export default function App({ Component, pageProps }) {
  if (!clerkEnabled) {
    return (
      <AnalyzeSessionProvider>
        <Component {...pageProps} />
      </AnalyzeSessionProvider>
    );
  }

  return (
    <ClerkProvider {...pageProps}>
      <AnalyzeSessionProvider>
        <Component {...pageProps} />
      </AnalyzeSessionProvider>
    </ClerkProvider>
  );
}
