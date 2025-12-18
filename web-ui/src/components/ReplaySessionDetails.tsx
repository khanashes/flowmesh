// Replay session details component

import { useReplaySession, useReplayControls } from '../hooks/useReplay';
import { LoadingSpinner } from './LoadingSpinner';
import { ErrorAlert } from './ErrorAlert';
import { StatusBadge } from './StatusBadge';
import { ReplayControls } from './ReplayControls';
import { formatTimestamp, formatOffset } from '../lib/utils';

interface ReplaySessionDetailsProps {
  sessionId: string;
  onDeleted?: () => void;
}

export function ReplaySessionDetails({ sessionId, onDeleted }: ReplaySessionDetailsProps) {
  const { session, loading, error, refetch } = useReplaySession(sessionId, 2000);
  const { start, pause, resume, stop, deleteSession, loading: controlsLoading } = useReplayControls(
    () => {
      refetch();
    },
    () => {
      // Error handling
    }
  );

  const handleDelete = async (sessionId: string): Promise<boolean> => {
    const success = await deleteSession(sessionId);
    if (success) {
      onDeleted?.();
    }
    return success;
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center py-12">
        <LoadingSpinner />
        <span className="ml-3 text-gray-600 dark:text-gray-400">Loading session details...</span>
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

  if (!session || !session.session) {
    return (
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6 text-gray-600 dark:text-gray-400">
        <p>Session not found.</p>
      </div>
    );
  }

  const s = session.session;
  const progress = s.progress;

  // Calculate progress percentage if end offset is set
  let progressPercentage: number | null = null;
  if (s.end_offset !== null && s.end_offset !== undefined && progress) {
    const total = s.end_offset - s.start_offset;
    const current = progress.current_offset - s.start_offset;
    if (total > 0) {
      progressPercentage = Math.min(100, Math.max(0, (current / total) * 100));
    }
  }

  return (
    <div className="space-y-6">
      {/* Session Metadata */}
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-2xl font-bold text-gray-900 dark:text-white">Replay Session Details</h2>
          <StatusBadge status={s.status || 'unknown'} />
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm">
          <div>
            <p className="text-gray-500 dark:text-gray-400">Session ID</p>
            <p className="font-mono text-gray-900 dark:text-white">{s.session_id}</p>
          </div>
          <div>
            <p className="text-gray-500 dark:text-gray-400">Stream</p>
            <p className="text-gray-900 dark:text-white">{s.stream}</p>
          </div>
          <div>
            <p className="text-gray-500 dark:text-gray-400">Partition</p>
            <p className="text-gray-900 dark:text-white">{s.partition}</p>
          </div>
          <div>
            <p className="text-gray-500 dark:text-gray-400">Sandbox Consumer Group</p>
            <p className="text-gray-900 dark:text-white">{s.sandbox_consumer_group}</p>
          </div>
          <div>
            <p className="text-gray-500 dark:text-gray-400">Created At</p>
            <p className="text-gray-900 dark:text-white">{formatTimestamp(s.created_at)}</p>
          </div>
          <div>
            <p className="text-gray-500 dark:text-gray-400">Updated At</p>
            <p className="text-gray-900 dark:text-white">{formatTimestamp(s.updated_at)}</p>
          </div>
        </div>
      </div>

      {/* Position Information */}
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
        <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">Position Information</h3>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm">
          <div>
            <p className="text-gray-500 dark:text-gray-400">Start Position</p>
            <p className="text-gray-900 dark:text-white">
              {s.start_timestamp ? formatTimestamp(s.start_timestamp) : formatOffset(s.start_offset)}
            </p>
          </div>
          <div>
            <p className="text-gray-500 dark:text-gray-400">End Position</p>
            <p className="text-gray-900 dark:text-white">
              {s.end_timestamp !== null && s.end_timestamp !== undefined
                ? formatTimestamp(s.end_timestamp)
                : s.end_offset !== null && s.end_offset !== undefined
                ? formatOffset(s.end_offset)
                : 'Not set'}
            </p>
          </div>
        </div>
      </div>

      {/* Progress Metrics */}
      {progress && (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
          <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">Progress</h3>

          {/* Progress Bar */}
          {progressPercentage !== null && (
            <div className="mb-4">
              <div className="flex justify-between text-sm text-gray-600 dark:text-gray-400 mb-1">
                <span>Progress</span>
                <span>{progressPercentage.toFixed(1)}%</span>
              </div>
              <div className="w-full bg-gray-200 rounded-full h-2.5 dark:bg-gray-700">
                <div
                  className="bg-blue-600 h-2.5 rounded-full transition-all duration-300"
                  style={{ width: `${progressPercentage}%` }}
                />
              </div>
            </div>
          )}

          <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
            <div>
              <p className="text-gray-500 dark:text-gray-400">Current Offset</p>
              <p className="text-xl font-bold text-gray-900 dark:text-white">
                {formatOffset(progress.current_offset)}
              </p>
            </div>
            <div>
              <p className="text-gray-500 dark:text-gray-400">Messages Replayed</p>
              <p className="text-xl font-bold text-gray-900 dark:text-white">
                {progress.messages_replayed.toLocaleString()}
              </p>
            </div>
            <div>
              <p className="text-gray-500 dark:text-gray-400">Errors</p>
              <p className={`text-xl font-bold ${progress.errors > 0 ? 'text-red-600 dark:text-red-400' : 'text-gray-900 dark:text-white'}`}>
                {progress.errors.toLocaleString()}
              </p>
            </div>
            <div>
              <p className="text-gray-500 dark:text-gray-400">Status</p>
              <p className="text-gray-900 dark:text-white">
                {progress.started_at ? 'Started' : progress.paused_at ? 'Paused' : progress.completed_at ? 'Completed' : 'Not started'}
              </p>
            </div>
          </div>

          {/* Timestamps */}
          <div className="mt-4 pt-4 border-t border-gray-200 dark:border-gray-700 grid grid-cols-1 md:grid-cols-3 gap-4 text-sm">
            {progress.started_at && progress.started_at > 0 && (
              <div>
                <p className="text-gray-500 dark:text-gray-400">Started At</p>
                <p className="text-gray-900 dark:text-white">{formatTimestamp(progress.started_at)}</p>
              </div>
            )}
            {progress.paused_at && progress.paused_at > 0 && (
              <div>
                <p className="text-gray-500 dark:text-gray-400">Paused At</p>
                <p className="text-gray-900 dark:text-white">{formatTimestamp(progress.paused_at)}</p>
              </div>
            )}
            {progress.completed_at && progress.completed_at > 0 && (
              <div>
                <p className="text-gray-500 dark:text-gray-400">Completed At</p>
                <p className="text-gray-900 dark:text-white">{formatTimestamp(progress.completed_at)}</p>
              </div>
            )}
          </div>
        </div>
      )}

      {/* Controls */}
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
        <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">Controls</h3>
        <ReplayControls
          session={s}
          onStart={start}
          onPause={pause}
          onResume={resume}
          onStop={stop}
          onDelete={handleDelete}
          loading={controlsLoading}
        />
      </div>
    </div>
  );
}

