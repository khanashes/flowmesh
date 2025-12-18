// Replay controls component

import { LoadingSpinner } from './LoadingSpinner';
import type { ReplaySession } from '../types/api';

interface ReplayControlsProps {
  session: ReplaySession;
  onStart: (sessionId: string) => Promise<boolean>;
  onPause: (sessionId: string) => Promise<boolean>;
  onResume: (sessionId: string) => Promise<boolean>;
  onStop: (sessionId: string) => Promise<boolean>;
  onDelete: (sessionId: string) => Promise<boolean>;
  loading: {
    start: boolean;
    pause: boolean;
    resume: boolean;
    stop: boolean;
    delete: boolean;
  };
}

export function ReplayControls({
  session,
  onStart,
  onPause,
  onResume,
  onStop,
  onDelete,
  loading,
}: ReplayControlsProps) {
  const status = session.status?.toLowerCase() || '';

  const handleAction = async (action: () => Promise<boolean>) => {
    await action();
  };

  return (
    <div className="flex flex-wrap gap-2">
      {status === 'created' && (
        <button
          onClick={() => handleAction(() => onStart(session.session_id))}
          disabled={loading.start}
          className="px-4 py-2 bg-green-500 text-white rounded-md hover:bg-green-600 focus:outline-none focus:ring-2 focus:ring-green-500 focus:ring-offset-2 disabled:opacity-50 disabled:cursor-not-allowed text-sm"
        >
          {loading.start ? (
            <span className="flex items-center">
              <LoadingSpinner />
              <span className="ml-2">Starting...</span>
            </span>
          ) : (
            'Start'
          )}
        </button>
      )}

      {status === 'active' && (
        <>
          <button
            onClick={() => handleAction(() => onPause(session.session_id))}
            disabled={loading.pause}
            className="px-4 py-2 bg-yellow-500 text-white rounded-md hover:bg-yellow-600 focus:outline-none focus:ring-2 focus:ring-yellow-500 focus:ring-offset-2 disabled:opacity-50 disabled:cursor-not-allowed text-sm"
          >
            {loading.pause ? (
              <span className="flex items-center">
                <LoadingSpinner />
                <span className="ml-2">Pausing...</span>
              </span>
            ) : (
              'Pause'
            )}
          </button>
          <button
            onClick={() => handleAction(() => onStop(session.session_id))}
            disabled={loading.stop}
            className="px-4 py-2 bg-red-500 text-white rounded-md hover:bg-red-600 focus:outline-none focus:ring-2 focus:ring-red-500 focus:ring-offset-2 disabled:opacity-50 disabled:cursor-not-allowed text-sm"
          >
            {loading.stop ? (
              <span className="flex items-center">
                <LoadingSpinner />
                <span className="ml-2">Stopping...</span>
              </span>
            ) : (
              'Stop'
            )}
          </button>
        </>
      )}

      {status === 'paused' && (
        <>
          <button
            onClick={() => handleAction(() => onResume(session.session_id))}
            disabled={loading.resume}
            className="px-4 py-2 bg-blue-500 text-white rounded-md hover:bg-blue-600 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 disabled:opacity-50 disabled:cursor-not-allowed text-sm"
          >
            {loading.resume ? (
              <span className="flex items-center">
                <LoadingSpinner />
                <span className="ml-2">Resuming...</span>
              </span>
            ) : (
              'Resume'
            )}
          </button>
          <button
            onClick={() => handleAction(() => onStop(session.session_id))}
            disabled={loading.stop}
            className="px-4 py-2 bg-red-500 text-white rounded-md hover:bg-red-600 focus:outline-none focus:ring-2 focus:ring-red-500 focus:ring-offset-2 disabled:opacity-50 disabled:cursor-not-allowed text-sm"
          >
            {loading.stop ? (
              <span className="flex items-center">
                <LoadingSpinner />
                <span className="ml-2">Stopping...</span>
              </span>
            ) : (
              'Stop'
            )}
          </button>
        </>
      )}

      <button
        onClick={() => handleAction(() => onDelete(session.session_id))}
        disabled={loading.delete}
        className="px-4 py-2 bg-gray-500 text-white rounded-md hover:bg-gray-600 focus:outline-none focus:ring-2 focus:ring-gray-500 focus:ring-offset-2 disabled:opacity-50 disabled:cursor-not-allowed text-sm"
      >
        {loading.delete ? (
          <span className="flex items-center">
            <LoadingSpinner />
            <span className="ml-2">Deleting...</span>
          </span>
        ) : (
          'Delete'
        )}
      </button>
    </div>
  );
}

