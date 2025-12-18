// Stream selector component

import { useState, useEffect } from 'react';
import { listStreams } from '../lib/api';
import { LoadingSpinner } from './LoadingSpinner';
import { ErrorAlert } from './ErrorAlert';
import type { StreamListItem, APIError } from '../types/api';

interface StreamSelectorProps {
  value?: string;
  onChange: (stream: string) => void;
  required?: boolean;
  disabled?: boolean;
}

export function StreamSelector({ value, onChange, required = false, disabled = false }: StreamSelectorProps) {
  const [streams, setStreams] = useState<StreamListItem[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<APIError | null>(null);

  useEffect(() => {
    const fetchStreams = async () => {
      setLoading(true);
      setError(null);
      try {
        const result = await listStreams();
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
    };

    fetchStreams();
  }, []);

  const formatStreamPath = (stream: StreamListItem): string => {
    return `${stream.tenant}/${stream.namespace}/${stream.name}`;
  };

  if (loading) {
    return <LoadingSpinner />;
  }

  if (error) {
    return <ErrorAlert error={error} />;
  }

  return (
    <div>
      <select
        value={value || ''}
        onChange={(e) => onChange(e.target.value)}
        required={required}
        disabled={disabled}
        className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500 dark:bg-gray-700 dark:border-gray-600 dark:text-white"
      >
        <option value="">Select a stream...</option>
        {streams.map((stream) => {
          const path = formatStreamPath(stream);
          return (
            <option key={path} value={path}>
              {path}
            </option>
          );
        })}
      </select>
    </div>
  );
}

