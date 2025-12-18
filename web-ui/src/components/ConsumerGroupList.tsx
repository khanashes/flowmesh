// Consumer group list component

import { LoadingSpinner } from './LoadingSpinner';
import { ErrorAlert } from './ErrorAlert';
import { StatusBadge } from './StatusBadge';
import type { APIError, ListConsumerGroupsResponse } from '../types/api';

interface ConsumerGroupListProps {
  consumerGroups: ListConsumerGroupsResponse | null;
  loading: boolean;
  error: APIError | null;
}

function getLagStatus(lag: number): string {
  if (lag === 0) return 'completed';
  if (lag < 100) return 'active';
  if (lag < 1000) return 'pending';
  return 'error';
}

function formatNumber(num: number): string {
  if (num >= 1000000) return (num / 1000000).toFixed(1) + 'M';
  if (num >= 1000) return (num / 1000).toFixed(1) + 'K';
  return num.toLocaleString();
}

export function ConsumerGroupList({ consumerGroups, loading, error }: ConsumerGroupListProps) {
  if (loading) {
    return <LoadingSpinner />;
  }

  if (error) {
    return <ErrorAlert error={error} />;
  }

  if (!consumerGroups || !consumerGroups.consumer_groups || consumerGroups.consumer_groups.length === 0) {
    return (
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6 text-gray-600 dark:text-gray-400">
        <p>No consumer groups found for this stream.</p>
      </div>
    );
  }

  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
      <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">Consumer Groups</h3>
      <div className="space-y-4">
        {consumerGroups.consumer_groups.map((cg, index) => (
          <div
            key={`${cg.group}-${cg.partition}-${index}`}
            className="border border-gray-200 dark:border-gray-700 rounded-lg p-4"
          >
            <div className="flex items-center justify-between mb-3">
              <div>
                <h4 className="text-sm font-medium text-gray-900 dark:text-white">{cg.group}</h4>
                <p className="text-xs text-gray-500 dark:text-gray-400">Partition: {cg.partition}</p>
              </div>
              <StatusBadge status={getLagStatus(cg.lag)} />
            </div>
            <div className="grid grid-cols-3 gap-4 text-sm">
              <div>
                <p className="text-gray-500 dark:text-gray-400">Committed Offset</p>
                <p className="font-semibold text-gray-900 dark:text-white">
                  {formatNumber(cg.committed_offset)}
                </p>
              </div>
              <div>
                <p className="text-gray-500 dark:text-gray-400">Latest Offset</p>
                <p className="font-semibold text-gray-900 dark:text-white">
                  {formatNumber(cg.latest_offset)}
                </p>
              </div>
              <div>
                <p className="text-gray-500 dark:text-gray-400">Lag</p>
                <p
                  className={`font-semibold ${
                    cg.lag === 0
                      ? 'text-green-600 dark:text-green-400'
                      : cg.lag < 100
                      ? 'text-blue-600 dark:text-blue-400'
                      : cg.lag < 1000
                      ? 'text-yellow-600 dark:text-yellow-400'
                      : 'text-red-600 dark:text-red-400'
                  }`}
                >
                  {formatNumber(cg.lag)}
                </p>
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

