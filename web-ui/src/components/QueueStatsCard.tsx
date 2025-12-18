// Queue statistics card component

import type { QueueStats } from '../types/api';

interface QueueStatsCardProps {
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

function formatAge(seconds: number): string {
  if (seconds < 60) {
    return `${Math.floor(seconds)}s`;
  }
  if (seconds < 3600) {
    return `${Math.floor(seconds / 60)}m`;
  }
  if (seconds < 86400) {
    return `${Math.floor(seconds / 3600)}h`;
  }
  return `${Math.floor(seconds / 86400)}d`;
}

export function QueueStatsCard({ stats }: QueueStatsCardProps) {
  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
      <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">Statistics</h3>
      <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
        {/* Total Jobs */}
        <div className="bg-gray-50 dark:bg-gray-700/50 rounded-lg p-4">
          <p className="text-sm text-gray-500 dark:text-gray-400">Total Jobs</p>
          <p className="text-2xl font-bold text-gray-900 dark:text-white mt-1">
            {formatNumber(stats.total_jobs)}
          </p>
        </div>

        {/* Pending Jobs */}
        <div className="bg-yellow-50 dark:bg-yellow-900/20 rounded-lg p-4">
          <p className="text-sm text-yellow-700 dark:text-yellow-400">Pending</p>
          <p className="text-2xl font-bold text-yellow-900 dark:text-yellow-300 mt-1">
            {formatNumber(stats.pending_jobs)}
          </p>
        </div>

        {/* In-Flight Jobs */}
        <div className="bg-blue-50 dark:bg-blue-900/20 rounded-lg p-4">
          <p className="text-sm text-blue-700 dark:text-blue-400">In-Flight</p>
          <p className="text-2xl font-bold text-blue-900 dark:text-blue-300 mt-1">
            {formatNumber(stats.in_flight_jobs)}
          </p>
        </div>

        {/* Completed Jobs */}
        <div className="bg-green-50 dark:bg-green-900/20 rounded-lg p-4">
          <p className="text-sm text-green-700 dark:text-green-400">Completed</p>
          <p className="text-2xl font-bold text-green-900 dark:text-green-300 mt-1">
            {formatNumber(stats.completed_jobs)}
          </p>
        </div>

        {/* Failed Jobs */}
        <div className="bg-red-50 dark:bg-red-900/20 rounded-lg p-4">
          <p className="text-sm text-red-700 dark:text-red-400">Failed</p>
          <p className="text-2xl font-bold text-red-900 dark:text-red-300 mt-1">
            {formatNumber(stats.failed_jobs)}
          </p>
        </div>

        {/* Oldest Job Age */}
        <div className="bg-purple-50 dark:bg-purple-900/20 rounded-lg p-4">
          <p className="text-sm text-purple-700 dark:text-purple-400">Oldest Job</p>
          <p className="text-2xl font-bold text-purple-900 dark:text-purple-300 mt-1">
            {formatAge(stats.oldest_job_age_seconds)}
          </p>
        </div>
      </div>
    </div>
  );
}

