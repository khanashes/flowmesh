// Stream list component

import { useStreamList } from '../hooks/useStreams';
import { LoadingSpinner } from './LoadingSpinner';
import { ErrorAlert } from './ErrorAlert';
import type { StreamListItem } from '../types/api';

interface StreamListProps {
  tenant?: string;
  namespace?: string;
  onSelectStream?: (stream: StreamListItem) => void;
  selectedStream?: StreamListItem | null;
}

export function StreamList({ tenant, namespace, onSelectStream, selectedStream }: StreamListProps) {
  const { streams, loading, error, refetch } = useStreamList(tenant, namespace);

  const isSelected = (stream: StreamListItem) => {
    if (!selectedStream) return false;
    return (
      selectedStream.tenant === stream.tenant &&
      selectedStream.namespace === stream.namespace &&
      selectedStream.name === stream.name
    );
  };

  if (loading && streams.length === 0) {
    return (
      <div className="flex items-center justify-center py-12">
        <LoadingSpinner />
        <span className="ml-3 text-gray-600 dark:text-gray-400">Loading streams...</span>
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

  if (streams.length === 0) {
    return (
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6 text-gray-600 dark:text-gray-400">
        <p>No streams found. Create a stream using the API to see it here.</p>
      </div>
    );
  }

  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg shadow overflow-hidden">
      <ul className="divide-y divide-gray-200 dark:divide-gray-700">
        {streams.map((stream) => (
          <li
            key={`${stream.tenant}/${stream.namespace}/${stream.name}`}
            className={`p-4 cursor-pointer hover:bg-gray-50 dark:hover:bg-gray-700 ${
              isSelected(stream) ? 'bg-blue-50 dark:bg-blue-900/30' : ''
            }`}
            onClick={() => onSelectStream?.(stream)}
          >
            <div className="flex items-center justify-between">
              <div className="text-sm font-medium text-gray-900 dark:text-white">
                {stream.name}
              </div>
              <div className="text-xs text-gray-500 dark:text-gray-400">
                {stream.tenant}/{stream.namespace}
              </div>
            </div>
            <div className="text-xs text-gray-500 dark:text-gray-400 mt-1">
              Partitions: {stream.partitions} | Created: {new Date(stream.created_at).toLocaleDateString()}
            </div>
          </li>
        ))}
      </ul>
    </div>
  );
}

