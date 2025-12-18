// Queue list component

import { useQueueList } from '../hooks/useQueues';
import { LoadingSpinner } from './LoadingSpinner';
import { ErrorAlert } from './ErrorAlert';
import type { QueueListItem } from '../types/api';

interface QueueListProps {
  tenant?: string;
  namespace?: string;
  onSelectQueue?: (queue: QueueListItem) => void;
  selectedQueue?: QueueListItem | null;
}

export function QueueList({ tenant, namespace, onSelectQueue, selectedQueue }: QueueListProps) {
  const { queues, loading, error, refetch } = useQueueList(tenant, namespace);

  const isSelected = (queue: QueueListItem) => {
    if (!selectedQueue) return false;
    return (
      selectedQueue.tenant === queue.tenant &&
      selectedQueue.namespace === queue.namespace &&
      selectedQueue.name === queue.name
    );
  };

  if (loading && queues.length === 0) {
    return (
      <div className="flex items-center justify-center py-12">
        <LoadingSpinner size="lg" />
        <span className="ml-3 text-gray-600 dark:text-gray-400">Loading queues...</span>
      </div>
    );
  }

  if (error) {
    return (
      <div>
        <ErrorAlert error={error} />
        <button
          onClick={refetch}
          className="mt-4 px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600"
        >
          Retry
        </button>
      </div>
    );
  }

  if (queues.length === 0) {
    return (
      <div className="text-center py-12 text-gray-500 dark:text-gray-400">
        <p>No queues found</p>
        {tenant || namespace ? (
          <p className="text-sm mt-2">
            {tenant && namespace
              ? `No queues in ${tenant}/${namespace}`
              : tenant
              ? `No queues for tenant ${tenant}`
              : `No queues in namespace ${namespace}`}
          </p>
        ) : null}
      </div>
    );
  }

  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg shadow">
      <div className="px-4 py-3 border-b border-gray-200 dark:border-gray-700 flex items-center justify-between">
        <h2 className="text-lg font-semibold text-gray-900 dark:text-white">Queues</h2>
        <button
          onClick={refetch}
          className="text-sm text-blue-600 hover:text-blue-800 dark:text-blue-400 dark:hover:text-blue-300"
        >
          Refresh
        </button>
      </div>
      <div className="divide-y divide-gray-200 dark:divide-gray-700">
        {queues.map((queue) => (
          <div
            key={`${queue.tenant}/${queue.namespace}/${queue.name}`}
            onClick={() => onSelectQueue?.(queue)}
            className={`px-4 py-3 cursor-pointer hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors ${
              isSelected(queue)
                ? 'bg-blue-50 dark:bg-blue-900/20 border-l-4 border-blue-500'
                : ''
            }`}
          >
            <div className="flex items-center justify-between">
              <div className="flex-1 min-w-0">
                <p className="text-sm font-medium text-gray-900 dark:text-white truncate">
                  {queue.name}
                </p>
                <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">
                  {queue.tenant}/{queue.namespace}
                </p>
              </div>
              <div className="ml-4 flex-shrink-0">
                <span className="inline-flex items-center px-2 py-1 rounded text-xs font-medium bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300">
                  {queue.partitions} {queue.partitions === 1 ? 'partition' : 'partitions'}
                </span>
              </div>
            </div>
            {queue.dlq?.enabled && (
              <div className="mt-2">
                <span className="text-xs text-orange-600 dark:text-orange-400">DLQ enabled</span>
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}

