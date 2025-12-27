// KV Store view component for viewing keys and values

import { useState } from 'react';
import { useKVKeys, useKVValue } from '../hooks/useKV';
import { deleteKV, setKV } from '../lib/api';
import { LoadingSpinner } from './LoadingSpinner';
import { ErrorAlert } from './ErrorAlert';
import type { KVStoreListItem, APIError } from '../types/api';

interface KVStoreViewProps {
  store: KVStoreListItem;
}

export function KVStoreView({ store }: KVStoreViewProps) {
  const [selectedKey, setSelectedKey] = useState<string | null>(null);
  const [prefixFilter, setPrefixFilter] = useState('');
  const [editingKey, setEditingKey] = useState('');
  const [editingValue, setEditingValue] = useState('');
  const [editingTTL, setEditingTTL] = useState('');
  const [showEditor, setShowEditor] = useState(false);

  const { keys, loading: keysLoading, error: keysError, refetch: refetchKeys } = useKVKeys(
    store.tenant,
    store.namespace,
    store.name,
    prefixFilter || undefined,
    5000 // Poll every 5 seconds
  );

  const { value, loading: valueLoading, error: valueError, refetch: refetchValue } = useKVValue(
    store.tenant,
    store.namespace,
    store.name,
    selectedKey || ''
  );

  const handleDelete = async (key: string) => {
    if (!confirm(`Delete key "${key}"?`)) return;

    try {
      await deleteKV(store.tenant, store.namespace, store.name, key);
      refetchKeys();
      if (selectedKey === key) {
        setSelectedKey(null);
      }
    } catch (error) {
      alert(`Failed to delete key: ${(error as APIError).message}`);
    }
  };

  const handleSetValue = async () => {
    if (!editingKey.trim()) {
      alert('Key cannot be empty');
      return;
    }

    try {
      const valueBytes = new TextEncoder().encode(editingValue);
      const ttlSeconds = editingTTL ? parseInt(editingTTL, 10) : undefined;

      await setKV(store.tenant, store.namespace, store.name, editingKey, {
        value: valueBytes,
        ttl_seconds: ttlSeconds,
      });

      refetchKeys();
      setSelectedKey(editingKey);
      setShowEditor(false);
      setEditingKey('');
      setEditingValue('');
      setEditingTTL('');
    } catch (error) {
      alert(`Failed to set value: ${(error as APIError).message}`);
    }
  };

  const filteredKeys = keys.filter((key) => !prefixFilter || key.startsWith(prefixFilter));

  const valueDisplay = value
    ? (() => {
        try {
          const text = new TextDecoder().decode(value);
          // Try to parse as JSON for pretty printing
          try {
            const json = JSON.parse(text);
            return JSON.stringify(json, null, 2);
          } catch {
            return text;
          }
        } catch {
          // If not valid UTF-8, show as hex
          return Array.from(value)
            .map((b) => b.toString(16).padStart(2, '0'))
            .join(' ');
        }
      })()
    : null;

  return (
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
      {/* Keys List */}
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow">
        <div className="px-4 py-3 border-b border-gray-200 dark:border-gray-700">
          <div className="flex items-center justify-between mb-3">
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white">Keys</h3>
            <button
              onClick={() => {
                setShowEditor(true);
                setEditingKey('');
                setEditingValue('');
                setEditingTTL('');
              }}
              className="px-3 py-1 text-sm bg-blue-500 text-white rounded hover:bg-blue-600"
            >
              + Add Key
            </button>
          </div>
          <input
            type="text"
            placeholder="Filter by prefix..."
            value={prefixFilter}
            onChange={(e) => setPrefixFilter(e.target.value)}
            className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-700 text-gray-900 dark:text-white"
          />
        </div>
        <div className="max-h-96 overflow-y-auto">
          {keysLoading && keys.length === 0 ? (
            <div className="flex items-center justify-center py-12">
              <LoadingSpinner />
            </div>
          ) : keysError ? (
            <div className="p-4">
              <ErrorAlert error={keysError} />
            </div>
          ) : filteredKeys.length === 0 ? (
            <div className="p-4 text-center text-gray-500 dark:text-gray-400">
              {prefixFilter ? `No keys matching prefix "${prefixFilter}"` : 'No keys found'}
            </div>
          ) : (
            <div className="divide-y divide-gray-200 dark:divide-gray-700">
              {filteredKeys.map((key) => (
                <div
                  key={key}
                  onClick={() => setSelectedKey(key)}
                  className={`px-4 py-2 cursor-pointer hover:bg-gray-50 dark:hover:bg-gray-700 flex items-center justify-between ${
                    selectedKey === key ? 'bg-blue-50 dark:bg-blue-900/20' : ''
                  }`}
                >
                  <span className="text-sm font-mono text-gray-900 dark:text-white truncate flex-1">
                    {key}
                  </span>
                  <button
                    onClick={(e) => {
                      e.stopPropagation();
                      handleDelete(key);
                    }}
                    className="ml-2 text-red-500 hover:text-red-700 text-xs"
                  >
                    Delete
                  </button>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>

      {/* Value View/Editor */}
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow">
        <div className="px-4 py-3 border-b border-gray-200 dark:border-gray-700">
          <h3 className="text-lg font-semibold text-gray-900 dark:text-white">
            {showEditor ? 'Add/Edit Key-Value' : 'Value'}
          </h3>
        </div>
        <div className="p-4">
          {showEditor ? (
            <div className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                  Key
                </label>
                <input
                  type="text"
                  value={editingKey}
                  onChange={(e) => setEditingKey(e.target.value)}
                  className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-700 text-gray-900 dark:text-white font-mono"
                  placeholder="key-name"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                  Value
                </label>
                <textarea
                  value={editingValue}
                  onChange={(e) => setEditingValue(e.target.value)}
                  rows={10}
                  className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-700 text-gray-900 dark:text-white font-mono text-sm"
                  placeholder='{"key": "value"}'
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                  TTL (seconds, optional)
                </label>
                <input
                  type="number"
                  value={editingTTL}
                  onChange={(e) => setEditingTTL(e.target.value)}
                  min="1"
                  className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-700 text-gray-900 dark:text-white"
                  placeholder="3600"
                />
              </div>
              <div className="flex gap-2">
                <button
                  onClick={handleSetValue}
                  className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600"
                >
                  Save
                </button>
                <button
                  onClick={() => {
                    setShowEditor(false);
                    setEditingKey('');
                    setEditingValue('');
                    setEditingTTL('');
                  }}
                  className="px-4 py-2 bg-gray-300 dark:bg-gray-600 text-gray-700 dark:text-gray-200 rounded hover:bg-gray-400 dark:hover:bg-gray-500"
                >
                  Cancel
                </button>
              </div>
            </div>
          ) : selectedKey ? (
            <div className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                  Key
                </label>
                <div className="px-3 py-2 bg-gray-100 dark:bg-gray-700 rounded-md font-mono text-sm text-gray-900 dark:text-white">
                  {selectedKey}
                </div>
              </div>
              {valueLoading ? (
                <div className="flex items-center justify-center py-12">
                  <LoadingSpinner />
                </div>
              ) : valueError ? (
                <ErrorAlert error={valueError} />
              ) : valueDisplay !== null ? (
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Value
                  </label>
                  <pre className="px-3 py-2 bg-gray-100 dark:bg-gray-700 rounded-md text-sm text-gray-900 dark:text-white overflow-auto max-h-96">
                    {valueDisplay}
                  </pre>
                </div>
              ) : (
                <div className="text-center text-gray-500 dark:text-gray-400 py-12">
                  No value found
                </div>
              )}
              <div className="flex gap-2">
                <button
                  onClick={() => {
                    setEditingKey(selectedKey);
                    setEditingValue(valueDisplay || '');
                    setShowEditor(true);
                  }}
                  className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600"
                >
                  Edit
                </button>
                <button
                  onClick={() => refetchValue()}
                  className="px-4 py-2 bg-gray-300 dark:bg-gray-600 text-gray-700 dark:text-gray-200 rounded hover:bg-gray-400 dark:hover:bg-gray-500"
                >
                  Refresh
                </button>
              </div>
            </div>
          ) : (
            <div className="text-center text-gray-500 dark:text-gray-400 py-12">
              Select a key to view its value
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

