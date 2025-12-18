// Queue details view component

import { useQueueStats } from '../hooks/useQueues';
import { QueueStatsCard } from './QueueStatsCard';
import { QueueDepthChart } from './QueueDepthChart';
import { JobStatusBreakdown } from './JobStatusBreakdown';
import { LoadingSpinner } from './LoadingSpinner';
import { ErrorAlert } from './ErrorAlert';
import type { QueueListItem } from '../types/api';

interface QueueDetailsViewProps {
  queue: QueueListItem;
}

export function QueueDetailsView({ queue }: QueueDetailsViewProps) {
  const { stats, loading, error, lastChecked } = useQueueStats(
    queue.tenant,
    queue.namespace,
    queue.name,
    5000 // Poll every 5 seconds
  );

  const formatTimestamp = (date: Date | null) => {
    if (!date) return 'Never';
    return date.toLocaleTimeString();
  };

  if (loading && !stats) {
    return (
      <div className="flex items-center justify-center py-12">
        <LoadingSpinner size="lg" />
        <span className="ml-3 text-gray-600 dark:text-gray-400">Loading queue statistics...</span>
      </div>
    );
  }

  if (error) {
    return (
      <div>
        <ErrorAlert error={error} />
        <p className="mt-4 text-sm text-gray-500 dark:text-gray-400">
          Queue: {queue.tenant}/{queue.namespace}/{queue.name}
        </p>
      </div>
    );
  }

  if (!stats?.stats) {
    return (
      <div className="text-center py-12 text-gray-500 dark:text-gray-400">
        <p>No statistics available</p>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Queue Overview */}
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
        <h2 className="text-xl font-semibold text-gray-900 dark:text-white mb-2">
          {queue.name}
        </h2>
        <p className="text-sm text-gray-500 dark:text-gray-400">
          {queue.tenant}/{queue.namespace}
        </p>
        <div className="mt-4 flex flex-wrap gap-2">
          <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300">
            {queue.partitions} {queue.partitions === 1 ? 'partition' : 'partitions'}
          </span>
          {queue.dlq?.enabled && (
            <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-orange-100 text-orange-800 dark:bg-orange-900/30 dark:text-orange-400">
              DLQ Enabled
            </span>
          )}
        </div>
        {lastChecked && (
          <p className="mt-4 text-xs text-gray-500 dark:text-gray-400">
            Last updated: {formatTimestamp(lastChecked)} (Auto-refreshing every 5 seconds)
          </p>
        )}
      </div>

      {/* Statistics Card */}
      <QueueStatsCard stats={stats.stats} />

      {/* Queue Depth Chart */}
      <QueueDepthChart stats={stats.stats} />

      {/* Job Status Breakdown */}
      <JobStatusBreakdown stats={stats.stats} />
    </div>
  );
}

