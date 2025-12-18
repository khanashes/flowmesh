// Replay session list component

import { useReplaySessions, useReplayControls } from '../hooks/useReplay';
import { LoadingSpinner } from './LoadingSpinner';
import { ErrorAlert } from './ErrorAlert';
import { StatusBadge } from './StatusBadge';
import { ReplayControls } from './ReplayControls';
import { formatTimestamp, formatOffset, unixNanoToDate } from '../lib/utils';
import type { ReplaySession } from '../types/api';

interface ReplaySessionListProps {
  selectedStream?: string;
  onSelectSession?: (session: ReplaySession) => void;
  selectedSessionId?: string | null;
}

export function ReplaySessionList({
  selectedStream,
  onSelectSession,
  selectedSessionId,
}: ReplaySessionListProps) {
  const { sessions, loading, error, refetch } = useReplaySessions(selectedStream, 5000);
  const { start, pause, resume, stop, deleteSession, loading: controlsLoading } = useReplayControls(() => {
    refetch();
  });

  if (loading && (!sessions || !sessions.sessions || sessions.sessions.length === 0)) {
    return (
      <div className="flex items-center justify-center py-12">
        <LoadingSpinner />
        <span className="ml-3 text-gray-600 dark:text-gray-400">Loading replay sessions...</span>
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

  if (!sessions || !sessions.sessions || sessions.sessions.length === 0) {
    return (
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6 text-gray-600 dark:text-gray-400 text-center">
        <p>No replay sessions found.</p>
        {selectedStream && <p className="text-sm mt-2">No sessions for stream: {selectedStream}</p>}
      </div>
    );
  }

  // Sort by creation time (newest first)
  const sortedSessions = [...sessions.sessions].sort((a, b) => {
    const timeA = unixNanoToDate(a.created_at || 0).getTime();
    const timeB = unixNanoToDate(b.created_at || 0).getTime();
    return timeB - timeA;
  });

  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg shadow overflow-hidden">
      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
          <thead className="bg-gray-50 dark:bg-gray-900">
            <tr>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                Session ID
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                Stream
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                Status
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                Start Position
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                Progress
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                Created
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
                Actions
              </th>
            </tr>
          </thead>
          <tbody className="bg-white dark:bg-gray-800 divide-y divide-gray-200 dark:divide-gray-700">
            {sortedSessions.map((session) => {
              const isSelected = selectedSessionId === session.session_id;
              const progress = session.progress;
              const startPosition = session.start_timestamp
                ? formatTimestamp(session.start_timestamp)
                : formatOffset(session.start_offset);
              const progressText = progress
                ? `Offset: ${formatOffset(progress.current_offset)} | Messages: ${progress.messages_replayed}${progress.errors > 0 ? ` | Errors: ${progress.errors}` : ''}`
                : 'N/A';

              return (
                <tr
                  key={session.session_id}
                  className={`hover:bg-gray-50 dark:hover:bg-gray-700 cursor-pointer ${isSelected ? 'bg-blue-50 dark:bg-blue-900/30' : ''}`}
                  onClick={() => onSelectSession?.(session)}
                >
                  <td className="px-6 py-4 whitespace-nowrap text-sm font-mono text-gray-900 dark:text-white">
                    <span title={session.session_id}>{session.session_id.substring(0, 8)}...</span>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900 dark:text-white">
                    {session.stream}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <StatusBadge status={session.status || 'unknown'} />
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-600 dark:text-gray-400">
                    {startPosition}
                  </td>
                  <td className="px-6 py-4 text-sm text-gray-600 dark:text-gray-400">
                    {progressText}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-600 dark:text-gray-400">
                    {formatTimestamp(session.created_at)}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm" onClick={(e) => e.stopPropagation()}>
                    <ReplayControls
                      session={session}
                      onStart={start}
                      onPause={pause}
                      onResume={resume}
                      onStop={stop}
                      onDelete={deleteSession}
                      loading={controlsLoading}
                    />
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

