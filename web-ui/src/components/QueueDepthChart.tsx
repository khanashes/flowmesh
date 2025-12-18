// Queue depth visualization component

import type { QueueStats } from '../types/api';

interface QueueDepthChartProps {
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

export function QueueDepthChart({ stats }: QueueDepthChartProps) {
  const total = stats.total_jobs;
  const pending = stats.pending_jobs;
  const inFlight = stats.in_flight_jobs;
  const completed = stats.completed_jobs;
  const failed = stats.failed_jobs;

  // Calculate percentages for visualization
  const maxValue = Math.max(total, pending + inFlight, 1);
  const pendingPercent = total > 0 ? (pending / maxValue) * 100 : 0;
  const inFlightPercent = total > 0 ? (inFlight / maxValue) * 100 : 0;

  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
      <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">Queue Depth</h3>

      {/* Summary Numbers */}
      <div className="grid grid-cols-3 gap-4 mb-6">
        <div className="text-center">
          <p className="text-sm text-gray-500 dark:text-gray-400">Total</p>
          <p className="text-2xl font-bold text-gray-900 dark:text-white">{formatNumber(total)}</p>
        </div>
        <div className="text-center">
          <p className="text-sm text-gray-500 dark:text-gray-400">Pending</p>
          <p className="text-2xl font-bold text-yellow-600 dark:text-yellow-400">
            {formatNumber(pending)}
          </p>
        </div>
        <div className="text-center">
          <p className="text-sm text-gray-500 dark:text-gray-400">In-Flight</p>
          <p className="text-2xl font-bold text-blue-600 dark:text-blue-400">
            {formatNumber(inFlight)}
          </p>
        </div>
      </div>

      {/* Visual Bar Chart */}
      <div className="space-y-4">
        {/* Pending Jobs Bar */}
        <div>
          <div className="flex justify-between items-center mb-1">
            <span className="text-sm font-medium text-gray-700 dark:text-gray-300">Pending</span>
            <span className="text-sm text-gray-500 dark:text-gray-400">
              {formatNumber(pending)} ({total > 0 ? ((pending / total) * 100).toFixed(1) : 0}%)
            </span>
          </div>
          <div className="w-full bg-gray-200 dark:bg-gray-700 rounded-full h-6">
            <div
              className="bg-yellow-500 h-6 rounded-full flex items-center justify-center text-white text-xs font-medium"
              style={{ width: `${Math.min(pendingPercent, 100)}%` }}
            >
              {pending > 0 && pendingPercent > 10 ? formatNumber(pending) : ''}
            </div>
          </div>
        </div>

        {/* In-Flight Jobs Bar */}
        <div>
          <div className="flex justify-between items-center mb-1">
            <span className="text-sm font-medium text-gray-700 dark:text-gray-300">In-Flight</span>
            <span className="text-sm text-gray-500 dark:text-gray-400">
              {formatNumber(inFlight)} ({total > 0 ? ((inFlight / total) * 100).toFixed(1) : 0}%)
            </span>
          </div>
          <div className="w-full bg-gray-200 dark:bg-gray-700 rounded-full h-6">
            <div
              className="bg-blue-500 h-6 rounded-full flex items-center justify-center text-white text-xs font-medium"
              style={{ width: `${Math.min(inFlightPercent, 100)}%` }}
            >
              {inFlight > 0 && inFlightPercent > 10 ? formatNumber(inFlight) : ''}
            </div>
          </div>
        </div>

        {/* Completed/Failed Summary */}
        {(completed > 0 || failed > 0) && (
          <div className="pt-4 border-t border-gray-200 dark:border-gray-700">
            <div className="grid grid-cols-2 gap-4">
              <div>
                <p className="text-sm text-gray-500 dark:text-gray-400">Completed</p>
                <p className="text-lg font-semibold text-green-600 dark:text-green-400">
                  {formatNumber(completed)}
                </p>
              </div>
              <div>
                <p className="text-sm text-gray-500 dark:text-gray-400">Failed</p>
                <p className="text-lg font-semibold text-red-600 dark:text-red-400">
                  {formatNumber(failed)}
                </p>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

