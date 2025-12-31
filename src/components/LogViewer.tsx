import { useEffect, useRef } from "react";
import { useLogs, LogLevel } from "@/contexts/LogContext";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Button } from "@/components/ui/button";
import { Trash2, Terminal } from "lucide-react";

const getLogColor = (level: LogLevel): string => {
  switch (level) {
    case "success":
      return "text-emerald-400";
    case "error":
      return "text-red-400";
    case "warning":
      return "text-amber-400";
    case "info":
    default:
      return "text-zinc-400";
  }
};

const formatTimestamp = (date: Date): string => {
  return date.toLocaleTimeString("en-US", {
    hour12: false,
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
};

export const LogViewer = () => {
  const { logs, clearLogs } = useLogs();
  const scrollRef = useRef<HTMLDivElement>(null);

  // Auto-scroll to bottom when new logs are added
  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [logs]);

  return (
    <div className="rounded-lg border border-border bg-zinc-950 overflow-hidden">
      <div className="flex items-center justify-between px-4 py-2 border-b border-border bg-zinc-900">
        <div className="flex items-center gap-2 text-zinc-300">
          <Terminal className="h-4 w-4" />
          <span className="font-mono text-sm font-medium">Execution Log</span>
          <span className="text-xs text-zinc-500">({logs.length} entries)</span>
        </div>
        <Button
          variant="ghost"
          size="sm"
          onClick={clearLogs}
          className="h-7 text-zinc-400 hover:text-zinc-200 hover:bg-zinc-800"
        >
          <Trash2 className="h-3.5 w-3.5 mr-1" />
          Clear
        </Button>
      </div>
      <ScrollArea className="h-[300px]" ref={scrollRef}>
        <div className="p-4 font-mono text-sm space-y-1">
          {logs.length === 0 ? (
            <p className="text-zinc-600 italic">No logs yet. Trigger a parsing action to see live updates.</p>
          ) : (
            logs.map((log) => (
              <div key={log.id} className="flex gap-3">
                <span className="text-zinc-600 shrink-0">
                  [{formatTimestamp(log.timestamp)}]
                </span>
                <span className={getLogColor(log.level)}>{log.message}</span>
              </div>
            ))
          )}
        </div>
      </ScrollArea>
    </div>
  );
};
