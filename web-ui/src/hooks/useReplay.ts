// React hooks for replay API calls

import { useState, useEffect, useCallback, useRef } from 'react';
import {
  getReplaySession,
  listReplaySessions,
  startReplay,
  pauseReplay,
  resumeReplay,
  stopReplay,
  deleteReplaySession,
} from '../lib/api';
import type {
  GetReplaySessionResponse,
  ListReplaySessionsResponse,
  ReplayControlResponse,
  APIError,
} from '../types/api';
import { useWebSocket, type WSMessage } from './useWebSocket';

// Replay sessions list hook
export interface UseReplaySessionsResult {
  sessions: ListReplaySessionsResponse | null;
  loading: boolean;
  error: APIError | null;
  refetch: () => Promise<void>;
}

export function useReplaySessions(stream?: string, intervalMs: number = 5000): UseReplaySessionsResult {
  const [sessions, setSessions] = useState<ListReplaySessionsResponse | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<APIError | null>(null);
  const intervalRef = useRef<number | null>(null);

  const fetchSessions = useCallback(async () => {
    try {
      const result = await listReplaySessions(stream);
      setSessions(result);
      setError(null);
    } catch (err: unknown) {
      const apiError: APIError = {
        status: (err as any).response?.status || 500,
        statusText: (err as any).response?.statusText || 'Unknown Error',
        message: (err as any).response?.data?.message || (err as Error).message,
        details: (err as any).response?.data?.details || '',
      };
      setError(apiError);
      setSessions(null);
    } finally {
      setLoading(false);
    }
  }, [stream]);

  useEffect(() => {
    // Initial fetch
    fetchSessions();

    // Set up polling interval
    if (intervalMs > 0) {
      intervalRef.current = window.setInterval(() => {
        fetchSessions();
      }, intervalMs);
    }

    return () => {
      if (intervalRef.current !== null) {
        clearInterval(intervalRef.current);
      }
    };
  }, [fetchSessions, intervalMs]);

  return {
    sessions,
    loading,
    error,
    refetch: fetchSessions,
  };
}

// Single replay session hook with progress polling
export interface UseReplaySessionResult {
  session: GetReplaySessionResponse | null;
  loading: boolean;
  error: APIError | null;
  refetch: () => Promise<void>;
}

export function useReplaySession(
  sessionId: string | null,
  intervalMs: number = 2000
): UseReplaySessionResult {
  const [session, setSession] = useState<GetReplaySessionResponse | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<APIError | null>(null);
  const intervalRef = useRef<number | null>(null);

  const topic = sessionId ? `replay.session.${sessionId}` : null;

  // WebSocket for real-time updates
  const { connected: wsConnected } = useWebSocket({
    topics: topic ? [topic] : [],
    onMessage: useCallback((message: WSMessage) => {
      if (message.type === 'replay.session' && topic && message.topic === topic) {
        try {
          const sessionData = message.payload as GetReplaySessionResponse;
          setSession(sessionData);
          setError(null);
        } catch (err) {
          console.error('Failed to parse WebSocket replay session:', err);
        }
      }
    }, [topic]),
  });

  const fetchSession = useCallback(async () => {
    if (!sessionId) {
      setLoading(false);
      return;
    }

    try {
      const result = await getReplaySession(sessionId);
      setSession(result);
      setError(null);
    } catch (err: unknown) {
      const apiError: APIError = {
        status: (err as any).response?.status || 500,
        statusText: (err as any).response?.statusText || 'Unknown Error',
        message: (err as any).response?.data?.message || (err as Error).message,
        details: (err as any).response?.data?.details || '',
      };
      setError(apiError);
      setSession(null);
    } finally {
      setLoading(false);
    }
  }, [sessionId]);

  useEffect(() => {
    if (!sessionId) {
      setLoading(false);
      return;
    }

    // Initial fetch
    fetchSession();

    // Poll more frequently when session is active or paused (as fallback if WebSocket not connected)
    const shouldPoll = session?.session?.status === 'active' || session?.session?.status === 'paused';
    const pollInterval = shouldPoll && !wsConnected ? intervalMs : 0;

    if (pollInterval > 0) {
      intervalRef.current = window.setInterval(() => {
        fetchSession();
      }, pollInterval);
    } else {
      if (intervalRef.current !== null) {
        clearInterval(intervalRef.current);
        intervalRef.current = null;
      }
    }

    return () => {
      if (intervalRef.current !== null) {
        clearInterval(intervalRef.current);
      }
    };
  }, [fetchSession, intervalMs, sessionId, session?.session?.status, wsConnected]);

  return {
    session,
    loading,
    error,
    refetch: fetchSession,
  };
}

// Replay controls hook
export interface UseReplayControlsResult {
  start: (sessionId: string) => Promise<boolean>;
  pause: (sessionId: string) => Promise<boolean>;
  resume: (sessionId: string) => Promise<boolean>;
  stop: (sessionId: string) => Promise<boolean>;
  deleteSession: (sessionId: string) => Promise<boolean>;
  loading: {
    start: boolean;
    pause: boolean;
    resume: boolean;
    stop: boolean;
    delete: boolean;
  };
  error: APIError | null;
}

export function useReplayControls(onSuccess?: () => void, onError?: (error: APIError) => void): UseReplayControlsResult {
  const [loading, setLoading] = useState({
    start: false,
    pause: false,
    resume: false,
    stop: false,
    delete: false,
  });
  const [error, setError] = useState<APIError | null>(null);

  const executeAction = useCallback(
    async (
      action: 'start' | 'pause' | 'resume' | 'stop' | 'delete',
      sessionId: string,
      apiCall: (sessionId: string) => Promise<ReplayControlResponse>
    ): Promise<boolean> => {
      setLoading((prev) => ({ ...prev, [action]: true }));
      setError(null);

      try {
        await apiCall(sessionId);
        setLoading((prev) => ({ ...prev, [action]: false }));
        onSuccess?.();
        return true;
      } catch (err: unknown) {
        const apiError: APIError = {
          status: (err as any).response?.status || 500,
          statusText: (err as any).response?.statusText || 'Unknown Error',
          message: (err as any).response?.data?.message || (err as Error).message,
          details: (err as any).response?.data?.details || '',
        };
        setError(apiError);
        setLoading((prev) => ({ ...prev, [action]: false }));
        onError?.(apiError);
        return false;
      }
    },
    [onSuccess, onError]
  );

  const start = useCallback(
    (sessionId: string) => executeAction('start', sessionId, startReplay),
    [executeAction]
  );

  const pause = useCallback(
    (sessionId: string) => executeAction('pause', sessionId, pauseReplay),
    [executeAction]
  );

  const resume = useCallback(
    (sessionId: string) => executeAction('resume', sessionId, resumeReplay),
    [executeAction]
  );

  const stop = useCallback(
    (sessionId: string) => executeAction('stop', sessionId, stopReplay),
    [executeAction]
  );

  const deleteSession = useCallback(
    (sessionId: string) => executeAction('delete', sessionId, deleteReplaySession),
    [executeAction]
  );

  return {
    start,
    pause,
    resume,
    stop,
    deleteSession,
    loading,
    error,
  };
}
