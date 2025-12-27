// KV Store list component

import { useKVStoreList } from '../hooks/useKV';
import { LoadingSpinner } from './LoadingSpinner';
import { ErrorAlert } from './ErrorAlert';
import type { KVStoreListItem } from '../types/api';

interface KVStoreListProps {
  tenant?: string;
  namespace?: string;
  onSelectStore?: (store: KVStoreListItem) => void;
  selectedStore?: KVStoreListItem | null;
}

export function KVStoreList({ tenant, namespace, onSelectStore, selectedStore }: KVStoreListProps) {
  const { stores, loading, error, refetch } = useKVStoreList(tenant, namespace);

  const isSelected = (store: KVStoreListItem) => {
    if (!selectedStore) return false;
    return (
      selectedStore.tenant === store.tenant &&
      selectedStore.namespace === store.namespace &&
      selectedStore.name === store.name
    );
  };

  if (loading && stores.length === 0) {
    return (
      <div className="flex items-center justify-center py-12">
        <LoadingSpinner size="lg" />
        <span className="ml-3 text-gray-600 dark:text-gray-400">Loading KV stores...</span>
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

  if (stores.length === 0) {
    return (
      <div className="text-center py-12 text-gray-500 dark:text-gray-400">
        <p>No KV stores found</p>
        {tenant || namespace ? (
          <p className="text-sm mt-2">
            {tenant && namespace
              ? `No KV stores in ${tenant}/${namespace}`
              : tenant
              ? `No KV stores for tenant ${tenant}`
              : `No KV stores in namespace ${namespace}`}
          </p>
        ) : null}
      </div>
    );
  }

  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg shadow">
      <div className="px-4 py-3 border-b border-gray-200 dark:border-gray-700 flex items-center justify-between">
        <h2 className="text-lg font-semibold text-gray-900 dark:text-white">KV Stores</h2>
        <button
          onClick={refetch}
          className="text-sm text-blue-600 hover:text-blue-800 dark:text-blue-400 dark:hover:text-blue-300"
        >
          Refresh
        </button>
      </div>
      <div className="divide-y divide-gray-200 dark:divide-gray-700">
        {stores.map((store) => (
          <div
            key={`${store.tenant}/${store.namespace}/${store.name}`}
            onClick={() => onSelectStore?.(store)}
            className={`px-4 py-3 cursor-pointer hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors ${
              isSelected(store) ? 'bg-blue-50 dark:bg-blue-900/20' : ''
            }`}
          >
            <div className="flex items-center justify-between">
              <div className="flex-1">
                <h3 className="text-sm font-medium text-gray-900 dark:text-white">{store.name}</h3>
                <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">
                  {store.tenant}/{store.namespace}
                </p>
              </div>
              <div className="text-xs text-gray-500 dark:text-gray-400">
                {new Date(store.created_at).toLocaleDateString()}
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

