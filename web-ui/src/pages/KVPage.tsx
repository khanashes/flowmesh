// KV Store page

import { useState } from 'react';
import { KVStoreList } from '../components/KVStoreList';
import { KVStoreView } from '../components/KVStoreView';
import type { KVStoreListItem } from '../types/api';

export function KVPage() {
  const [selectedStore, setSelectedStore] = useState<KVStoreListItem | null>(null);

  return (
    <div>
      <div className="mb-6">
        <h1 className="text-3xl font-bold text-gray-900 dark:text-white mb-2">KV Store</h1>
        <p className="text-gray-600 dark:text-gray-400">
          Manage key-value stores and browse keys and values
        </p>
      </div>

      {selectedStore ? (
        <div className="space-y-4">
          <div className="flex items-center justify-between">
            <div>
              <h2 className="text-xl font-semibold text-gray-900 dark:text-white">
                {selectedStore.name}
              </h2>
              <p className="text-sm text-gray-500 dark:text-gray-400">
                {selectedStore.tenant}/{selectedStore.namespace}
              </p>
            </div>
            <button
              onClick={() => setSelectedStore(null)}
              className="px-4 py-2 bg-gray-300 dark:bg-gray-600 text-gray-700 dark:text-gray-200 rounded hover:bg-gray-400 dark:hover:bg-gray-500"
            >
              Back to List
            </button>
          </div>
          <KVStoreView store={selectedStore} />
        </div>
      ) : (
        <KVStoreList onSelectStore={setSelectedStore} selectedStore={selectedStore} />
      )}
    </div>
  );
}
