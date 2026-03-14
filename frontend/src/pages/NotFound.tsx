import { Terminal } from "lucide-react";

export default function NotFound() {
  return (
    <div className="min-h-screen bg-background flex items-center justify-center">
      <div className="text-center space-y-4">
        <Terminal className="h-10 w-10 text-primary mx-auto" />
        <h1 className="text-4xl font-bold text-foreground font-mono">404</h1>
        <p className="text-muted-foreground text-sm">Page not found.</p>
        <a href="/" className="inline-block text-sm text-primary hover:opacity-80 transition-opacity">
          ← Back home
        </a>
      </div>
    </div>
  );
}
