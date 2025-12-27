// React hooks for stream API calls

import { useState, useEffect, useCallback, useRef } from 'react';
import { listStreams, getStreamStats, listConsumerGroups } from '../lib/api';
import type {
  StreamListItem,
  StreamStatsResponse,
  ListConsumerGroupsResponse,
  APIError,
} from '../types/api';
import { useWebSocket, type WSMessage } from './useWebSocket';

// Stream list hook
export interface UseStreamListResult {
  streams: StreamListItem[];
  loading: boolean;
  error: APIError | null;
  refetch: () => Promise<void>;
}

export function useStreamList(tenant?: string, namespace?: string): UseStreamListResult {
  const [streams, setStreams] = useState<StreamListItem[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<APIError | null>(null);

  const fetchStreams = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const result = await listStreams(tenant, namespace);
      setStreams(result.streams || []);
    } catch (err: unknown) {
      const apiError: APIError = {
        status: (err as any).response?.status || 500,
        statusText: (err as any).response?.statusText || 'Unknown Error',
        message: (err as any).response?.data?.message || (err as Error).message,
        details: (err as any).response?.data?.details || '',
      };
      setError(apiError);
      setStreams([]);
    } finally {
      setLoading(false);
    }
  }, [tenant, namespace]);

  useEffect(() => {
    fetchStreams();
  }, [fetchStreams]);

  return {
    streams,
    loading,
    error,
    refetch: fetchStreams,
  };
}

// Stream stats hook with polling
export interface UseStreamStatsResult {
  stats: StreamStatsResponse | null;
  loading: boolean;
  error: APIError | null;
  lastChecked: Date | null;
}

export function useStreamStats(
  tenant: string,
  namespace: string,
  name: string,
  intervalMs: number = 5000
): UseStreamStatsResult {
  const [stats, setStats] = useState<StreamStatsResponse | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<APIError | null>(null);
  const [lastChecked, setLastChecked] = useState<Date | null>(null);
  const intervalRef = useRef<number | null>(null);

  const topic = `stream.stats.${tenant}/${namespace}/${name}`;

  // WebSocket for real-time updates
  const { connected: wsConnected } = useWebSocket({
    topics: [topic],
    onMessage: useCallback((message: WSMessage) => {
      if (message.type === 'stream.stats' && message.topic === topic) {
        try {
          const statsData = message.payload as StreamStatsResponse['stats'];
          setStats({
            status: 'success',
            message: 'stream statistics retrieved successfully',
            stats: statsData,
          });
          setLastChecked(new Date());
          setError(null);
        } catch (err) {
          console.error('Failed to parse WebSocket stream stats:', err);
        }
      }
    }, [topic]),
  });

  const fetchStats = useCallback(async () => {
    try {
      const result = await getStreamStats(tenant, namespace, name);
      setStats(result);
      setError(null);
      setLastChecked(new Date());
    } catch (err: unknown) {
      const apiError: APIError = {
        status: (err as any).response?.status || 500,
        statusText: (err as any).response?.statusText || 'Unknown Error',
        message: (err as any).response?.data?.message || (err as Error).message || 'An error occurred',
        details: (err as any).response?.data?.details || '',
      };
      setError(apiError);
      setStats(null);
      setLastChecked(new Date());
    } finally {
      setLoading(false);
    }
  }, [tenant, namespace, name]);

  useEffect(() => {
    if (!tenant || !namespace || !name) {
      setLoading(false);
      return;
    }

    // Initial fetch
    fetchStats();

    // Set up polling interval as fallback (only if WebSocket is not connected)
    if (intervalMs > 0 && !wsConnected) {
      intervalRef.current = window.setInterval(() => {
        fetchStats();
      }, intervalMs);
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
  }, [fetchStats, intervalMs, tenant, namespace, name, wsConnected]);

  return {
    stats,
    loading,
    error,
    lastChecked,
  };
}

// Consumer groups hook with polling
export interface UseConsumerGroupsResult {
  consumerGroups: ListConsumerGroupsResponse | null;
  loading: boolean;
  error: APIError | null;
  lastChecked: Date | null;
}

export function useConsumerGroups(
  tenant: string,
  namespace: string,
  name: string,
  intervalMs: number = 5000
): UseConsumerGroupsResult {
  const [consumerGroups, setConsumerGroups] = useState<ListConsumerGroupsResponse | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<APIError | null>(null);
  const [lastChecked, setLastChecked] = useState<Date | null>(null);
  const intervalRef = useRef<number | null>(null);

  const fetchConsumerGroups = useCallback(async () => {
    try {
      const result = await listConsumerGroups(tenant, namespace, name);
      setConsumerGroups(result);
      setError(null);
      setLastChecked(new Date());
    } catch (err: unknown) {
      const apiError: APIError = {
        status: (err as any).response?.status || 500,
        statusText: (err as any).response?.statusText || 'Unknown Error',
        message: (err as any).response?.data?.message || (err as Error).message || 'An error occurred',
        details: (err as any).response?.data?.details || '',
      };
      setError(apiError);
      setConsumerGroups(null);
      setLastChecked(new Date());
    } finally {
      setLoading(false);
    }
  }, [tenant, namespace, name]);

  useEffect(() => {
    if (!tenant || !namespace || !name) {
      setLoading(false);
      return;
    }

    // Initial fetch
    fetchConsumerGroups();

    // Set up polling interval
    if (intervalMs > 0) {
      intervalRef.current = window.setInterval(() => {
        fetchConsumerGroups();
      }, intervalMs);
    }

    return () => {
      if (intervalRef.current !== null) {
        clearInterval(intervalRef.current);
      }
    };
  }, [fetchConsumerGroups, intervalMs, tenant, namespace, name]);

  return {
    consumerGroups,
    loading,
    error,
    lastChecked,
  };
}

