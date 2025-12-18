// Replay page

import { useState } from 'react';
import { CreateReplaySessionForm } from '../components/CreateReplaySessionForm';
import { ReplaySessionList } from '../components/ReplaySessionList';
import { ReplaySessionDetails } from '../components/ReplaySessionDetails';
import { StreamSelector } from '../components/StreamSelector';
import type { ReplaySession } from '../types/api';

export function ReplayPage() {
  const [selectedStream, setSelectedStream] = useState<string>('');
  const [selectedSession, setSelectedSession] = useState<ReplaySession | null>(null);

  const handleSessionCreated = (sessionId: string) => {
    // Optionally select the newly created session
    console.log('Session created:', sessionId);
  };

  const handleSessionDeleted = () => {
    setSelectedSession(null);
  };

  return (
    <div className="p-6 space-y-6">
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-3xl font-bold text-gray-900 dark:text-white">Replay Sessions</h1>
        
        {/* Stream filter */}
        <div className="w-64">
          <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
            Filter by Stream (Optional)
          </label>
          <StreamSelector
            value={selectedStream}
            onChange={(stream) => {
              setSelectedStream(stream);
              setSelectedSession(null); // Clear selection when filter changes
            }}
          />
        </div>
      </div>

      {/* Two-column layout */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Left column: Create form and sessions list */}
        <div className="lg:col-span-1 space-y-6">
          {/* Create form */}
          <CreateReplaySessionForm
            onSuccess={handleSessionCreated}
            onError={(error) => {
              console.error('Failed to create session:', error);
            }}
          />

          {/* Sessions list */}
          <div>
            <h2 className="text-xl font-semibold text-gray-900 dark:text-white mb-4">Sessions</h2>
            <ReplaySessionList
              selectedStream={selectedStream || undefined}
              onSelectSession={setSelectedSession}
              selectedSessionId={selectedSession?.session_id}
            />
          </div>
        </div>

        {/* Right column: Session details */}
        <div className="lg:col-span-2">
          {selectedSession ? (
            <ReplaySessionDetails
              sessionId={selectedSession.session_id}
              onDeleted={handleSessionDeleted}
            />
          ) : (
            <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-12 text-center">
              <p className="text-gray-500 dark:text-gray-400">
                Select a replay session from the list to view details and manage it.
              </p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
