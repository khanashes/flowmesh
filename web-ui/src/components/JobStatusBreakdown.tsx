// Job status breakdown component

import { StatusBadge } from './StatusBadge';
import type { QueueStats } from '../types/api';

interface JobStatusBreakdownProps {
  stats: QueueStats;
}

function formatNumber(num: number): string {
  if (num >= 1000000) {
    return (num / 1000000).toFixed(1) + 'M';
  }
  if (num >= 1000) {
    return (num / 1000).toFixed(1) + 'K';
  }
  return num.toString();
}

export function JobStatusBreakdown({ stats }: JobStatusBreakdownProps) {
  const total = stats.pending_jobs + stats.in_flight_jobs + stats.failed_jobs;

  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
      <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">Job Status</h3>

      <div className="space-y-4">
        {/* Pending Jobs */}
        <div className="flex items-center justify-between p-4 bg-yellow-50 dark:bg-yellow-900/20 rounded-lg">
          <div className="flex items-center">
            <StatusBadge status="pending" label="Pending" />
            <span className="ml-3 text-gray-700 dark:text-gray-300">
              Jobs waiting to be processed
            </span>
          </div>
          <span className="text-2xl font-bold text-yellow-600 dark:text-yellow-400">
            {formatNumber(stats.pending_jobs)}
          </span>
        </div>

        {/* In-Flight Jobs */}
        <div className="flex items-center justify-between p-4 bg-blue-50 dark:bg-blue-900/20 rounded-lg">
          <div className="flex items-center">
            <StatusBadge status="active" label="In-Flight" />
            <span className="ml-3 text-gray-700 dark:text-gray-300">
              Jobs currently being processed
            </span>
          </div>
          <span className="text-2xl font-bold text-blue-600 dark:text-blue-400">
            {formatNumber(stats.in_flight_jobs)}
          </span>
        </div>

        {/* Failed Jobs */}
        {stats.failed_jobs > 0 && (
          <div className="flex items-center justify-between p-4 bg-red-50 dark:bg-red-900/20 rounded-lg">
            <div className="flex items-center">
              <StatusBadge status="error" label="Failed" />
              <span className="ml-3 text-gray-700 dark:text-gray-300">
                Jobs that have failed processing
              </span>
            </div>
            <span className="text-2xl font-bold text-red-600 dark:text-red-400">
              {formatNumber(stats.failed_jobs)}
            </span>
          </div>
        )}

        {/* Completed Jobs */}
        {stats.completed_jobs > 0 && (
          <div className="flex items-center justify-between p-4 bg-green-50 dark:bg-green-900/20 rounded-lg">
            <div className="flex items-center">
              <StatusBadge status="completed" label="Completed" />
              <span className="ml-3 text-gray-700 dark:text-gray-300">
                Successfully processed jobs
              </span>
            </div>
            <span className="text-2xl font-bold text-green-600 dark:text-green-400">
              {formatNumber(stats.completed_jobs)}
            </span>
          </div>
        )}

        {/* Total Active */}
        {total > 0 && (
          <div className="pt-4 border-t border-gray-200 dark:border-gray-700">
            <div className="flex items-center justify-between">
              <span className="text-sm font-medium text-gray-500 dark:text-gray-400">
                Total Active
              </span>
              <span className="text-lg font-semibold text-gray-900 dark:text-white">
                {formatNumber(total)}
              </span>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

