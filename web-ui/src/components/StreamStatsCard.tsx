// Stream statistics card component

import { LoadingSpinner } from './LoadingSpinner';
import { ErrorAlert } from './ErrorAlert';
import type { APIError, StreamStatsResponse } from '../types/api';

interface StreamStatsCardProps {
  stats: StreamStatsResponse | null;
  loading: boolean;
  error: APIError | null;
}

export function StreamStatsCard({ stats, loading, error }: StreamStatsCardProps) {
  if (loading) {
    return <LoadingSpinner />;
  }

  if (error) {
    return <ErrorAlert error={error} />;
  }

  if (!stats || !stats.stats) {
    return (
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6 text-gray-600 dark:text-gray-400">
        <p>No statistics available for this stream.</p>
      </div>
    );
  }

  const formatNumber = (num: number) => {
    if (num >= 1000000) return (num / 1000000).toFixed(1) + 'M';
    if (num >= 1000) return (num / 1000).toFixed(1) + 'K';
    return num.toLocaleString();
  };

  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
      <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">Stream Statistics</h3>
      <div className="grid grid-cols-2 gap-4">
        <div>
          <p className="text-sm text-gray-500 dark:text-gray-400">Latest Offset</p>
          <p className="text-xl font-bold text-gray-900 dark:text-white">
            {formatNumber(stats.stats.latest_offset)}
          </p>
        </div>
        <div>
          <p className="text-sm text-gray-500 dark:text-gray-400">Partition</p>
          <p className="text-xl font-bold text-gray-900 dark:text-white">0</p>
        </div>
      </div>
    </div>
  );
}

