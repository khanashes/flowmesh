// Health status page

import { useHealthCheck } from '../hooks/useApi';
import { LoadingSpinner } from '../components/LoadingSpinner';
import { ErrorAlert } from '../components/ErrorAlert';
import { StatusBadge } from '../components/StatusBadge';

export function HealthPage() {
  const { health, readiness, loading, error, isHealthy, isReady, lastChecked } = useHealthCheck(5000);

  const formatTimestamp = (date: Date | null) => {
    if (!date) return 'Never';
    return date.toLocaleTimeString();
  };

  return (
    <div>
      <h1 className="text-3xl font-bold text-gray-900 dark:text-white mb-6">Health Status</h1>

      {loading && !health && !readiness && (
        <div className="flex items-center justify-center py-12">
          <LoadingSpinner size="lg" />
          <span className="ml-3 text-gray-600 dark:text-gray-400">Checking health status...</span>
        </div>
      )}

      {error && <ErrorAlert error={error} />}

      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        {/* Health Check */}
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-xl font-semibold text-gray-900 dark:text-white">Health Check</h2>
            <StatusBadge status={isHealthy ? 'healthy' : 'unhealthy'} />
          </div>
          {health && (
            <div className="space-y-2">
              <div>
                <span className="text-sm text-gray-500 dark:text-gray-400">Status Code: </span>
                <span className="font-mono">{health.status.code}</span>
              </div>
              {health.status.message && (
                <div>
                  <span className="text-sm text-gray-500 dark:text-gray-400">Message: </span>
                  <span>{health.status.message}</span>
                </div>
              )}
            </div>
          )}
        </div>

        {/* Readiness Check */}
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-xl font-semibold text-gray-900 dark:text-white">Readiness Check</h2>
            <StatusBadge status={isReady ? 'ready' : 'not-ready'} />
          </div>
          {readiness && (
            <div className="space-y-2">
              <div>
                <span className="text-sm text-gray-500 dark:text-gray-400">Ready: </span>
                <span className="font-semibold">{readiness.ready ? 'Yes' : 'No'}</span>
              </div>
              <div>
                <span className="text-sm text-gray-500 dark:text-gray-400">Status Code: </span>
                <span className="font-mono">{readiness.status.code}</span>
              </div>
              {readiness.status.message && (
                <div>
                  <span className="text-sm text-gray-500 dark:text-gray-400">Message: </span>
                  <span>{readiness.status.message}</span>
                </div>
              )}
            </div>
          )}
        </div>
      </div>

      {/* Last Checked */}
      {lastChecked && (
        <div className="mt-6 text-sm text-gray-500 dark:text-gray-400">
          Last checked: {formatTimestamp(lastChecked)} (Auto-refreshing every 5 seconds)
        </div>
      )}
    </div>
  );
}
