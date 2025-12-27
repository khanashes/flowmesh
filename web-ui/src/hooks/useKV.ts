// Hook for KV store operations

import { useState, useEffect, useCallback } from 'react';
import { listKVStores, listKeys, getKV } from '../lib/api';
import type { KVStoreListItem, ListKeysResponse, GetResponse, APIError } from '../types/api';

export interface UseKVStoreListResult {
  stores: KVStoreListItem[];
  loading: boolean;
  error: APIError | null;
  refetch: () => Promise<void>;
}

export function useKVStoreList(tenant?: string, namespace?: string): UseKVStoreListResult {
  const [stores, setStores] = useState<KVStoreListItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<APIError | null>(null);

  const fetchStores = useCallback(async () => {
    try {
      setLoading(true);
      setError(null);
      const response = await listKVStores(tenant, namespace);
      setStores(response.stores || []);
    } catch (err) {
      setError(err as APIError);
      setStores([]);
    } finally {
      setLoading(false);
    }
  }, [tenant, namespace]);

  useEffect(() => {
    fetchStores();
  }, [fetchStores]);

  return { stores, loading, error, refetch: fetchStores };
}

export interface UseKVKeysResult {
  keys: string[];
  loading: boolean;
  error: APIError | null;
  refetch: () => Promise<void>;
}

export function useKVKeys(
  tenant: string,
  namespace: string,
  name: string,
  prefix?: string,
  intervalMs: number = 0
): UseKVKeysResult {
  const [keys, setKeys] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<APIError | null>(null);

  const fetchKeys = useCallback(async () => {
    try {
      setLoading(true);
      setError(null);
      const response: ListKeysResponse = await listKeys(tenant, namespace, name, prefix);
      setKeys(response.keys || []);
    } catch (err) {
      setError(err as APIError);
      setKeys([]);
    } finally {
      setLoading(false);
    }
  }, [tenant, namespace, name, prefix]);

  useEffect(() => {
    fetchKeys();
    if (intervalMs > 0) {
      const interval = setInterval(fetchKeys, intervalMs);
      return () => clearInterval(interval);
    }
  }, [fetchKeys, intervalMs]);

  return { keys, loading, error, refetch: fetchKeys };
}

export interface UseKVValueResult {
  value: Uint8Array | null;
  loading: boolean;
  error: APIError | null;
  refetch: () => Promise<void>;
}

export function useKVValue(tenant: string, namespace: string, name: string, key: string): UseKVValueResult {
  const [value, setValue] = useState<Uint8Array | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<APIError | null>(null);

  const fetchValue = useCallback(async () => {
    if (!key) {
      setValue(null);
      setLoading(false);
      return;
    }

    try {
      setLoading(true);
      setError(null);
      const response: GetResponse = await getKV(tenant, namespace, name, key);
      // Convert base64 string to Uint8Array if needed
      if (typeof response.value === 'string') {
        const binaryString = atob(response.value);
        const bytes = new Uint8Array(binaryString.length);
        for (let i = 0; i < binaryString.length; i++) {
          bytes[i] = binaryString.charCodeAt(i);
        }
        setValue(bytes);
      } else {
        setValue(response.value);
      }
    } catch (err) {
      setError(err as APIError);
      setValue(null);
    } finally {
      setLoading(false);
    }
  }, [tenant, namespace, name, key]);

  useEffect(() => {
    fetchValue();
  }, [fetchValue]);

  return { value, loading, error, refetch: fetchValue };
}

