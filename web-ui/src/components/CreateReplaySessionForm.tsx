// Create replay session form component

import { useState, type FormEvent } from 'react';
import { createReplaySession } from '../lib/api';
import { StreamSelector } from './StreamSelector';
import { dateToUnixNano, parseDateTimeInput } from '../lib/utils';
import { LoadingSpinner } from './LoadingSpinner';
import { ErrorAlert } from './ErrorAlert';
import type { APIError } from '../types/api';

interface CreateReplaySessionFormProps {
  onSuccess?: (sessionId: string) => void;
  onError?: (error: APIError) => void;
}

export function CreateReplaySessionForm({ onSuccess, onError }: CreateReplaySessionFormProps) {
  const [stream, setStream] = useState<string>('');
  const [startType, setStartType] = useState<'offset' | 'timestamp'>('offset');
  const [startOffset, setStartOffset] = useState<string>('');
  const [startTimestamp, setStartTimestamp] = useState<string>('');
  const [hasEnd, setHasEnd] = useState<boolean>(false);
  const [endType, setEndType] = useState<'offset' | 'timestamp'>('offset');
  const [endOffset, setEndOffset] = useState<string>('');
  const [endTimestamp, setEndTimestamp] = useState<string>('');
  const [sandboxConsumerGroup, setSandboxConsumerGroup] = useState<string>('');
  
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<APIError | null>(null);
  const [success, setSuccess] = useState<boolean>(false);

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    setLoading(true);
    setError(null);
    setSuccess(false);

    // Validation
    if (!stream) {
      setError({ status: 400, statusText: 'Bad Request', message: 'Stream is required', details: '' });
      setLoading(false);
      return;
    }

    if (!sandboxConsumerGroup.trim()) {
      setError({ status: 400, statusText: 'Bad Request', message: 'Sandbox consumer group is required', details: '' });
      setLoading(false);
      return;
    }

    if (startType === 'offset' && !startOffset) {
      setError({ status: 400, statusText: 'Bad Request', message: 'Start offset is required', details: '' });
      setLoading(false);
      return;
    }

    if (startType === 'timestamp' && !startTimestamp) {
      setError({ status: 400, statusText: 'Bad Request', message: 'Start timestamp is required', details: '' });
      setLoading(false);
      return;
    }

    if (hasEnd && endType === 'offset' && !endOffset) {
      setError({ status: 400, statusText: 'Bad Request', message: 'End offset is required when end position is enabled', details: '' });
      setLoading(false);
      return;
    }

    if (hasEnd && endType === 'timestamp' && !endTimestamp) {
      setError({ status: 400, statusText: 'Bad Request', message: 'End timestamp is required when end position is enabled', details: '' });
      setLoading(false);
      return;
    }

    try {
      const request: any = {
        sandbox_consumer_group: sandboxConsumerGroup.trim(),
      };

      if (startType === 'offset') {
        request.start_offset = parseInt(startOffset, 10);
        if (isNaN(request.start_offset) || request.start_offset < 0) {
          throw new Error('Start offset must be a non-negative number');
        }
      } else {
        const date = parseDateTimeInput(startTimestamp);
        request.start_timestamp = dateToUnixNano(date);
      }

      if (hasEnd) {
        if (endType === 'offset') {
          request.end_offset = parseInt(endOffset, 10);
          if (isNaN(request.end_offset) || request.end_offset < 0) {
            throw new Error('End offset must be a non-negative number');
          }
          // Validate start < end if both are offsets
          if (startType === 'offset' && request.start_offset >= request.end_offset) {
            throw new Error('Start offset must be less than end offset');
          }
        } else {
          const date = parseDateTimeInput(endTimestamp);
          request.end_timestamp = dateToUnixNano(date);
          // Validate start < end if both are timestamps
          if (startType === 'timestamp') {
            const startDate = parseDateTimeInput(startTimestamp);
            if (date <= startDate) {
              throw new Error('End timestamp must be after start timestamp');
            }
          }
        }
      }

      const response = await createReplaySession(stream, request);
      setSuccess(true);
      setLoading(false);
      
      // Reset form
      setStream('');
      setStartOffset('');
      setStartTimestamp('');
      setEndOffset('');
      setEndTimestamp('');
      setSandboxConsumerGroup('');
      setHasEnd(false);
      
      onSuccess?.(response.session.session_id);
      
      // Clear success message after 3 seconds
      setTimeout(() => setSuccess(false), 3000);
    } catch (err: unknown) {
      const apiError: APIError = {
        status: (err as any).response?.status || 500,
        statusText: (err as any).response?.statusText || 'Unknown Error',
        message: (err as any).response?.data?.message || (err as Error).message,
        details: (err as any).response?.data?.details || '',
      };
      setError(apiError);
      setLoading(false);
      onError?.(apiError);
    }
  };

  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
      <h2 className="text-xl font-semibold text-gray-900 dark:text-white mb-4">Create Replay Session</h2>
      
      {error && <ErrorAlert error={error} />}
      
      {success && (
        <div className="mb-4 bg-green-100 border border-green-400 text-green-700 px-4 py-3 rounded relative" role="alert">
          Replay session created successfully!
        </div>
      )}

      <form onSubmit={handleSubmit} className="space-y-4">
        {/* Stream selector */}
        <div>
          <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
            Stream <span className="text-red-500">*</span>
          </label>
          <StreamSelector value={stream} onChange={setStream} required disabled={loading} />
        </div>

        {/* Start position */}
        <div>
          <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
            Start Position <span className="text-red-500">*</span>
          </label>
          <div className="flex space-x-4 mb-2">
            <label className="flex items-center">
              <input
                type="radio"
                value="offset"
                checked={startType === 'offset'}
                onChange={(e) => setStartType(e.target.value as 'offset' | 'timestamp')}
                disabled={loading}
                className="mr-2"
              />
              <span className="text-sm text-gray-700 dark:text-gray-300">Offset</span>
            </label>
            <label className="flex items-center">
              <input
                type="radio"
                value="timestamp"
                checked={startType === 'timestamp'}
                onChange={(e) => setStartType(e.target.value as 'offset' | 'timestamp')}
                disabled={loading}
                className="mr-2"
              />
              <span className="text-sm text-gray-700 dark:text-gray-300">Timestamp</span>
            </label>
          </div>
          {startType === 'offset' ? (
            <input
              type="number"
              min="0"
              value={startOffset}
              onChange={(e) => setStartOffset(e.target.value)}
              disabled={loading}
              placeholder="e.g., 0"
              className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500 dark:bg-gray-700 dark:border-gray-600 dark:text-white"
            />
          ) : (
            <input
              type="datetime-local"
              value={startTimestamp}
              onChange={(e) => setStartTimestamp(e.target.value)}
              disabled={loading}
              className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500 dark:bg-gray-700 dark:border-gray-600 dark:text-white"
            />
          )}
        </div>

        {/* End position (optional) */}
        <div>
          <label className="flex items-center mb-2">
            <input
              type="checkbox"
              checked={hasEnd}
              onChange={(e) => setHasEnd(e.target.checked)}
              disabled={loading}
              className="mr-2"
            />
            <span className="text-sm font-medium text-gray-700 dark:text-gray-300">End Position (Optional)</span>
          </label>
          {hasEnd && (
            <>
              <div className="flex space-x-4 mb-2">
                <label className="flex items-center">
                  <input
                    type="radio"
                    value="offset"
                    checked={endType === 'offset'}
                    onChange={(e) => setEndType(e.target.value as 'offset' | 'timestamp')}
                    disabled={loading}
                    className="mr-2"
                  />
                  <span className="text-sm text-gray-700 dark:text-gray-300">Offset</span>
                </label>
                <label className="flex items-center">
                  <input
                    type="radio"
                    value="timestamp"
                    checked={endType === 'timestamp'}
                    onChange={(e) => setEndType(e.target.value as 'offset' | 'timestamp')}
                    disabled={loading}
                    className="mr-2"
                  />
                  <span className="text-sm text-gray-700 dark:text-gray-300">Timestamp</span>
                </label>
              </div>
              {endType === 'offset' ? (
                <input
                  type="number"
                  min="0"
                  value={endOffset}
                  onChange={(e) => setEndOffset(e.target.value)}
                  disabled={loading}
                  placeholder="e.g., 100"
                  className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500 dark:bg-gray-700 dark:border-gray-600 dark:text-white"
                />
              ) : (
                <input
                  type="datetime-local"
                  value={endTimestamp}
                  onChange={(e) => setEndTimestamp(e.target.value)}
                  disabled={loading}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500 dark:bg-gray-700 dark:border-gray-600 dark:text-white"
                />
              )}
            </>
          )}
        </div>

        {/* Sandbox consumer group */}
        <div>
          <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
            Sandbox Consumer Group <span className="text-red-500">*</span>
          </label>
          <input
            type="text"
            value={sandboxConsumerGroup}
            onChange={(e) => setSandboxConsumerGroup(e.target.value)}
            disabled={loading}
            placeholder="e.g., debug-replay-group"
            required
            className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500 dark:bg-gray-700 dark:border-gray-600 dark:text-white"
          />
        </div>

        {/* Submit button */}
        <button
          type="submit"
          disabled={loading}
          className="w-full px-4 py-2 bg-blue-500 text-white rounded-md hover:bg-blue-600 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 disabled:opacity-50 disabled:cursor-not-allowed"
        >
          {loading ? (
            <span className="flex items-center justify-center">
              <LoadingSpinner />
              <span className="ml-2">Creating...</span>
            </span>
          ) : (
            'Create Replay Session'
          )}
        </button>
      </form>
    </div>
  );
}

