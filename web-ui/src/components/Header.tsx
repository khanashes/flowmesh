// Header component

import { useWebSocket } from '../hooks/useWebSocket';

export function Header() {
  const { connected, error } = useWebSocket({
    topics: [],
    autoConnect: true,
  });

  return (
    <header className="bg-white dark:bg-gray-800 shadow">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex justify-between items-center py-4">
          <div className="flex items-center">
            <h1 className="text-2xl font-bold text-gray-900 dark:text-white">FlowMesh</h1>
            <span className="ml-3 text-sm text-gray-500 dark:text-gray-400">Unified Event Fabric</span>
          </div>
          <div className="flex items-center">
            {/* WebSocket connection status */}
            <div className="flex items-center space-x-2" title={connected ? 'WebSocket connected - Real-time updates enabled' : error ? 'WebSocket disconnected - Using polling fallback' : 'Connecting...'}>
              <div
                className={`w-2 h-2 rounded-full ${
                  connected
                    ? 'bg-green-500 animate-pulse'
                    : error
                    ? 'bg-red-500'
                    : 'bg-yellow-500 animate-pulse'
                }`}
              />
              <span className="text-xs text-gray-500 dark:text-gray-400">
                {connected ? 'Live' : error ? 'Offline' : 'Connecting...'}
              </span>
            </div>
          </div>
        </div>
      </div>
    </header>
  );
}
