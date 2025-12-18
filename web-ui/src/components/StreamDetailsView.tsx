// Stream details view component

import type { StreamListItem } from '../types/api';
import { useStreamStats, useConsumerGroups } from '../hooks/useStreams';
import { StreamStatsCard } from './StreamStatsCard';
import { ConsumerGroupList } from './ConsumerGroupList';

interface StreamDetailsViewProps {
  stream: StreamListItem;
}

export function StreamDetailsView({ stream }: StreamDetailsViewProps) {
  const { stats, loading: statsLoading, error: statsError, lastChecked: statsLastChecked } = useStreamStats(
    stream.tenant,
    stream.namespace,
    stream.name,
    5000 // Poll every 5 seconds
  );

  const {
    consumerGroups,
    loading: cgLoading,
    error: cgError,
  } = useConsumerGroups(stream.tenant, stream.namespace, stream.name, 5000);

  const formatTimestamp = (isoString: string) => {
    return new Date(isoString).toLocaleString();
  };

  return (
    <div className="space-y-6">
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
        <h2 className="text-2xl font-bold text-gray-900 dark:text-white mb-4">
          Stream: {stream.name}
        </h2>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm text-gray-700 dark:text-gray-300">
          <div>
            <p>
              <span className="font-semibold">Tenant:</span> {stream.tenant}
            </p>
            <p>
              <span className="font-semibold">Namespace:</span> {stream.namespace}
            </p>
            <p>
              <span className="font-semibold">Partitions:</span> {stream.partitions}
            </p>
          </div>
          <div>
            <p>
              <span className="font-semibold">Created At:</span> {formatTimestamp(stream.created_at)}
            </p>
            <p>
              <span className="font-semibold">Last Updated:</span> {formatTimestamp(stream.updated_at)}
            </p>
            {statsLastChecked && (
              <p>
                <span className="font-semibold">Stats Last Checked:</span>{' '}
                {statsLastChecked.toLocaleTimeString()}
              </p>
            )}
          </div>
        </div>
      </div>

      <StreamStatsCard stats={stats} loading={statsLoading} error={statsError} />

      <ConsumerGroupList consumerGroups={consumerGroups} loading={cgLoading} error={cgError} />
    </div>
  );
}

