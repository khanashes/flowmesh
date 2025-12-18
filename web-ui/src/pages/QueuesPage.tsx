// Queues page

import { useState } from 'react';
import { QueueList } from '../components/QueueList';
import { QueueDetailsView } from '../components/QueueDetailsView';
import type { QueueListItem } from '../types/api';

export function QueuesPage() {
  const [selectedQueue, setSelectedQueue] = useState<QueueListItem | null>(null);

  return (
    <div>
      <h1 className="text-3xl font-bold text-gray-900 dark:text-white mb-6">Queues</h1>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Queue List */}
        <div className="lg:col-span-1">
          <QueueList
            onSelectQueue={(queue) => setSelectedQueue(queue)}
            selectedQueue={selectedQueue}
          />
        </div>

        {/* Queue Details */}
        <div className="lg:col-span-2">
          {selectedQueue ? (
            <QueueDetailsView queue={selectedQueue} />
          ) : (
            <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-12 text-center">
              <p className="text-gray-500 dark:text-gray-400">
                Select a queue from the list to view details and statistics
              </p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
