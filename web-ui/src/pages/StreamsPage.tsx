// Streams page

import { useState } from 'react';
import { StreamList } from '../components/StreamList';
import { StreamDetailsView } from '../components/StreamDetailsView';
import type { StreamListItem } from '../types/api';

export function StreamsPage() {
  const [selectedStream, setSelectedStream] = useState<StreamListItem | null>(null);

  return (
    <div className="p-6">
      <h1 className="text-3xl font-bold text-gray-900 dark:text-white mb-6">Streams</h1>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Stream List */}
        <div className="lg:col-span-1">
          <StreamList
            onSelectStream={(stream) => setSelectedStream(stream)}
            selectedStream={selectedStream}
          />
        </div>

        {/* Stream Details */}
        <div className="lg:col-span-2">
          {selectedStream ? (
            <StreamDetailsView stream={selectedStream} />
          ) : (
            <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-12 text-center">
              <p className="text-gray-500 dark:text-gray-400">
                Select a stream from the list to view details and statistics
              </p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
