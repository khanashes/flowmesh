// WebSocket hook for real-time updates

import { useEffect, useRef, useState, useCallback } from 'react';
import { API_BASE_URL } from '../lib/config';

export interface WSMessage {
  type: string;
  topic?: string;
  payload: unknown;
}

export interface UseWebSocketOptions {
  topics?: string[];
  onMessage?: (message: WSMessage) => void;
  onError?: (error: Event) => void;
  onConnect?: () => void;
  onDisconnect?: () => void;
  autoConnect?: boolean;
  reconnect?: boolean;
  reconnectInterval?: number;
}

export interface UseWebSocketResult {
  connected: boolean;
  error: Error | null;
  send: (message: WSMessage) => void;
  subscribe: (topics: string[]) => void;
  unsubscribe: (topics: string[]) => void;
  reconnect: () => void;
  disconnect: () => void;
}

// Convert HTTP URL to WebSocket URL
function getWebSocketUrl(): string {
  const url = new URL(API_BASE_URL);
  const protocol = url.protocol === 'https:' ? 'wss:' : 'ws:';
  return `${protocol}//${url.host}/ws`;
}

export function useWebSocket(options: UseWebSocketOptions = {}): UseWebSocketResult {
  const {
    topics = [],
    onMessage,
    onError,
    onConnect,
    onDisconnect,
    autoConnect = true,
    reconnect: shouldReconnect = true,
    reconnectInterval = 5000,
  } = options;

  const [connected, setConnected] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimeoutRef = useRef<number | null>(null);
  const topicsRef = useRef<string[]>(topics);
  const shouldReconnectRef = useRef(shouldReconnect);

  // Update topics ref when topics change
  useEffect(() => {
    topicsRef.current = topics;
    if (wsRef.current?.readyState === WebSocket.OPEN && topics.length > 0) {
      // Resubscribe with new topics
      wsRef.current.send(
        JSON.stringify({
          type: 'subscribe',
          payload: { topics },
        })
      );
    }
  }, [topics]);

  // Send message
  const send = useCallback((message: WSMessage) => {
    if (wsRef.current?.readyState === WebSocket.OPEN) {
      wsRef.current.send(JSON.stringify(message));
    } else {
      console.warn('WebSocket is not connected. Message not sent:', message);
    }
  }, []);

  // Subscribe to topics
  const subscribe = useCallback((newTopics: string[]) => {
    const topicsSet = new Set([...topicsRef.current, ...newTopics]);
    topicsRef.current = Array.from(topicsSet);
    if (wsRef.current?.readyState === WebSocket.OPEN) {
      send({
        type: 'subscribe',
        payload: { topics: newTopics },
      });
    }
  }, [send]);

  // Unsubscribe from topics
  const unsubscribe = useCallback((topicsToRemove: string[]) => {
    topicsRef.current = topicsRef.current.filter((t) => !topicsToRemove.includes(t));
    if (wsRef.current?.readyState === WebSocket.OPEN) {
      send({
        type: 'unsubscribe',
        payload: { topics: topicsToRemove },
      });
    }
  }, [send]);

  // Connect - use ref to avoid circular dependency
  const connectRef = useRef<(() => void) | null>(null);
  
  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN || wsRef.current?.readyState === WebSocket.CONNECTING) {
      return;
    }

    try {
      const wsUrl = getWebSocketUrl();
      const ws = new WebSocket(wsUrl);

      ws.onopen = () => {
        setConnected(true);
        setError(null);
        shouldReconnectRef.current = shouldReconnect;

        // Subscribe to initial topics
        if (topicsRef.current.length > 0) {
          ws.send(
            JSON.stringify({
              type: 'subscribe',
              payload: { topics: topicsRef.current },
            })
          );
        }

        onConnect?.();
      };

      ws.onmessage = (event) => {
        try {
          const message: WSMessage = JSON.parse(event.data);
          onMessage?.(message);
        } catch (err) {
          console.error('Failed to parse WebSocket message:', err, event.data);
        }
      };

      ws.onerror = (event) => {
        const error = new Error('WebSocket error');
        setError(error);
        onError?.(event);
      };

      ws.onclose = () => {
        setConnected(false);
        onDisconnect?.();

        // Reconnect if enabled
        if (shouldReconnectRef.current && connectRef.current) {
          reconnectTimeoutRef.current = window.setTimeout(() => {
            connectRef.current?.();
          }, reconnectInterval);
        }
      };

      wsRef.current = ws;
    } catch (err) {
      const error = err instanceof Error ? err : new Error('Failed to create WebSocket connection');
      setError(error);
      onError?.(new Event('error'));
    }
  }, [onMessage, onError, onConnect, onDisconnect, shouldReconnect, reconnectInterval]);

  // Update ref in effect to avoid render issues
  useEffect(() => {
    connectRef.current = connect;
  }, [connect]);

  // Disconnect
  const disconnect = useCallback(() => {
    shouldReconnectRef.current = false;
    if (reconnectTimeoutRef.current !== null) {
      clearTimeout(reconnectTimeoutRef.current);
      reconnectTimeoutRef.current = null;
    }
    if (wsRef.current) {
      wsRef.current.close();
      wsRef.current = null;
    }
    setConnected(false);
  }, []);

  // Manual reconnect
  const reconnect = useCallback(() => {
    disconnect();
    setTimeout(() => {
      connect();
    }, 1000);
  }, [connect, disconnect]);

  // Auto-connect on mount
  useEffect(() => {
    if (autoConnect) {
      // Use setTimeout to avoid cascading renders warning
      const timer = setTimeout(() => {
        connect();
      }, 0);
      return () => {
        clearTimeout(timer);
        disconnect();
      };
    }

    return () => {
      disconnect();
    };
  }, [autoConnect, connect, disconnect]);

  return {
    connected,
    error,
    send,
    subscribe,
    unsubscribe,
    reconnect,
    disconnect,
  };
}

