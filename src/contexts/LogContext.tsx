import React, { createContext, useContext, useState, useCallback, ReactNode } from "react";

export type LogLevel = "info" | "success" | "error" | "warning";

export interface LogEntry {
  id: string;
  timestamp: Date;
  message: string;
  level: LogLevel;
}

interface LogContextType {
  logs: LogEntry[];
  addLog: (message: string, level?: LogLevel) => void;
  clearLogs: () => void;
}

const LogContext = createContext<LogContextType | undefined>(undefined);

export const LogProvider = ({ children }: { children: ReactNode }) => {
  const [logs, setLogs] = useState<LogEntry[]>([]);

  const addLog = useCallback((message: string, level: LogLevel = "info") => {
    const entry: LogEntry = {
      id: crypto.randomUUID(),
      timestamp: new Date(),
      message,
      level,
    };
    setLogs((prev) => [...prev, entry]);
  }, []);

  const clearLogs = useCallback(() => {
    setLogs([]);
  }, []);

  return (
    <LogContext.Provider value={{ logs, addLog, clearLogs }}>
      {children}
    </LogContext.Provider>
  );
};

export const useLogs = () => {
  const context = useContext(LogContext);
  if (!context) {
    throw new Error("useLogs must be used within a LogProvider");
  }
  return context;
};
