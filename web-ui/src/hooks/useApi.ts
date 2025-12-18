// React hooks for API calls

import { useState, useEffect, useCallback, useRef } from 'react';
import { healthCheck, readinessCheck } from '../lib/api';
import type { HealthCheckResponse, ReadinessCheckResponse, APIError } from '../types/api';

// Generic API call hook
export interface UseApiCallResult<T> {
  data: T | null;
  loading: boolean;
  error: APIError | null;
  refetch: () => Promise<void>;
}

export function useApiCall<T>(
  apiCall: () => Promise<T>,
  immediate: boolean = true
): UseApiCallResult<T> {
  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState<boolean>(immediate);
  const [error, setError] = useState<APIError | null>(null);

  const execute = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const result = await apiCall();
      setData(result);
    } catch (err) {
      setError(err as APIError);
    } finally {
      setLoading(false);
    }
  }, [apiCall]);

  useEffect(() => {
    if (immediate) {
      execute();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [immediate]);

  return {
    data,
    loading,
    error,
    refetch: execute,
  };
}

// Health check hook with polling
export interface UseHealthCheckResult {
  health: HealthCheckResponse | null;
  readiness: ReadinessCheckResponse | null;
  loading: boolean;
  error: APIError | null;
  isHealthy: boolean;
  isReady: boolean;
  lastChecked: Date | null;
}

export function useHealthCheck(intervalMs: number = 5000): UseHealthCheckResult {
  const [health, setHealth] = useState<HealthCheckResponse | null>(null);
  const [readiness, setReadiness] = useState<ReadinessCheckResponse | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<APIError | null>(null);
  const [lastChecked, setLastChecked] = useState<Date | null>(null);
  const intervalRef = useRef<number | null>(null);

  const checkHealth = useCallback(async () => {
    try {
      const [healthResult, readinessResult] = await Promise.all([
        healthCheck(),
        readinessCheck(),
      ]);
      setHealth(healthResult);
      setReadiness(readinessResult);
      setError(null);
      setLastChecked(new Date());
    } catch (err) {
      setError(err as APIError);
      setHealth(null);
      setReadiness(null);
      setLastChecked(new Date());
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    // Initial check
    checkHealth();

    // Set up polling interval
    if (intervalMs > 0) {
      intervalRef.current = window.setInterval(() => {
        checkHealth();
      }, intervalMs);
    }

    return () => {
      if (intervalRef.current !== null) {
        clearInterval(intervalRef.current);
      }
    };
  }, [checkHealth, intervalMs]);

  const isHealthy = health?.status.code === 200 || health?.status.code === 0;
  const isReady = readiness?.ready === true && readiness?.status.code === 200;

  return {
    health,
    readiness,
    loading,
    error,
    isHealthy,
    isReady,
    lastChecked,
  };
}
