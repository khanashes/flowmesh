// React hooks for queue API calls

import { useState, useEffect, useCallback, useRef } from 'react';
import { listQueues, getQueueStats } from '../lib/api';
import type { QueueListItem, QueueStatsResponse, APIError } from '../types/api';

// Queue list hook
export interface UseQueueListResult {
  queues: QueueListItem[];
  loading: boolean;
  error: APIError | null;
  refetch: () => Promise<void>;
}

export function useQueueList(tenant?: string, namespace?: string): UseQueueListResult {
  const [queues, setQueues] = useState<QueueListItem[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<APIError | null>(null);

  const fetchQueues = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const result = await listQueues(tenant, namespace);
      setQueues(result.queues || []);
    } catch (err: unknown) {
      const error = err as { response?: { status?: number; statusText?: string; data?: { message?: string; details?: string } }; message?: string };
      const apiError: APIError = {
        status: error.response?.status || 500,
        statusText: error.response?.statusText || 'Unknown Error',
        message: error.response?.data?.message || error.message || 'An error occurred',
        details: error.response?.data?.details || '',
      };
      setError(apiError);
      setQueues([]);
    } finally {
      setLoading(false);
    }
  }, [tenant, namespace]);

  useEffect(() => {
    fetchQueues();
  }, [fetchQueues]);

  return {
    queues,
    loading,
    error,
    refetch: fetchQueues,
  };
}

// Queue stats hook with polling
export interface UseQueueStatsResult {
  stats: QueueStatsResponse | null;
  loading: boolean;
  error: APIError | null;
  lastChecked: Date | null;
}

export function useQueueStats(
  tenant: string,
  namespace: string,
  name: string,
  intervalMs: number = 5000
): UseQueueStatsResult {
  const [stats, setStats] = useState<QueueStatsResponse | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<APIError | null>(null);
  const [lastChecked, setLastChecked] = useState<Date | null>(null);
  const intervalRef = useRef<number | null>(null);

  const fetchStats = useCallback(async () => {
    try {
      const result = await getQueueStats(tenant, namespace, name);
      setStats(result);
      setError(null);
      setLastChecked(new Date());
    } catch (err: unknown) {
      const error = err as { response?: { status?: number; statusText?: string; data?: { message?: string; details?: string } }; message?: string };
      const apiError: APIError = {
        status: error.response?.status || 500,
        statusText: error.response?.statusText || 'Unknown Error',
        message: error.response?.data?.message || error.message || 'An error occurred',
        details: error.response?.data?.details || '',
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

    // Set up polling interval
    if (intervalMs > 0) {
      intervalRef.current = window.setInterval(() => {
        fetchStats();
      }, intervalMs);
    }

    return () => {
      if (intervalRef.current !== null) {
        clearInterval(intervalRef.current);
      }
    };
  }, [fetchStats, intervalMs, tenant, namespace, name]);

  return {
    stats,
    loading,
    error,
    lastChecked,
  };
}

