import { useState, useEffect, useCallback, useRef } from 'react';

interface ProgressData {
  percentage: number;
  stage: string;
  status: 'pending' | 'processing' | 'completed' | 'failed';
}

interface UseJobProgressReturn {
  progress: ProgressData;
  isConnected: boolean;
  error: string | null;
  reconnect: () => void;
}

export const useJobProgress = (jobId: string): UseJobProgressReturn => {
  const [progress, setProgress] = useState<ProgressData>({
    percentage: 0,
    stage: 'Initializing...',
    status: 'pending',
  });
  const [isConnected, setIsConnected] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimeoutRef = useRef<NodeJS.Timeout>();
  const reconnectAttemptsRef = useRef(0);

  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) {
      return; // Already connected
    }

    // Close existing connection if any
    if (wsRef.current) {
      wsRef.current.close();
    }

    console.log(`[WS] Connecting to job ${jobId}...`);
    const ws = new WebSocket(`ws://localhost:8000/ws/jobs/${jobId}/progress/`);
    wsRef.current = ws;

    ws.onopen = () => {
      console.log(`[WS] Connected to job ${jobId}`);
      setIsConnected(true);
      setError(null);
      reconnectAttemptsRef.current = 0;
    };

    ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        console.log(`[WS] Message received:`, data);

        if (data.type === 'progress_update') {
          setProgress({
            percentage: data.percentage,
            stage: data.stage,
            status: data.status,
          });

          // Auto-close connection when job is done
          if (data.status === 'completed' || data.status === 'failed') {
            console.log(`[WS] Job ${data.status}, closing connection in 2s`);
            setTimeout(() => ws.close(1000, 'Job finished'), 2000);
          }
        } else if (data.type === 'error') {
          console.error(`[WS] Server error:`, data.message);
          setError(data.message);
          ws.close();
        } else if (data.type === 'pong') {
          console.log(`[WS] Pong received:`, data.timestamp);
        }
      } catch (err) {
        console.error('[WS] Failed to parse message:', err);
      }
    };

    ws.onerror = (event) => {
      console.error('[WS] WebSocket error:', event);
      setError('WebSocket connection failed');
      setIsConnected(false);
    };

    ws.onclose = (event) => {
      console.log(`[WS] Disconnected (code: ${event.code}, reason: ${event.reason})`);
      setIsConnected(false);
      wsRef.current = null;

      // Auto-reconnect on unexpected disconnect (not when job is done)
      if (event.code !== 1000 && progress.status === 'processing') {
        reconnectAttemptsRef.current++;
        
        if (reconnectAttemptsRef.current <= 5) {
          const delay = Math.min(1000 * Math.pow(2, reconnectAttemptsRef.current), 10000);
          console.log(`[WS] Reconnecting in ${delay}ms (attempt ${reconnectAttemptsRef.current})...`);
          
          reconnectTimeoutRef.current = setTimeout(() => {
            connect();
          }, delay);
        } else {
          setError('Max reconnection attempts reached');
        }
      }
    };
  }, [jobId, progress.status]);

  const reconnect = useCallback(() => {
    console.log('[WS] Manual reconnect triggered');
    reconnectAttemptsRef.current = 0;
    setError(null);
    wsRef.current?.close();
    connect();
  }, [connect]);

  // Send ping every 30 seconds to keep connection alive
  useEffect(() => {
    if (!isConnected) return;

    const pingInterval = setInterval(() => {
      if (wsRef.current?.readyState === WebSocket.OPEN) {
        console.log('[WS] Sending ping...');
        wsRef.current.send(JSON.stringify({
          command: 'ping',
          timestamp: Date.now(),
        }));
      }
    }, 30000);

    return () => clearInterval(pingInterval);
  }, [isConnected]);

  // Initial connection
  useEffect(() => {
    connect();

    // Cleanup on unmount
    return () => {
      console.log('[WS] Component unmounting, cleaning up...');
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
      }
      wsRef.current?.close(1000, 'Component unmounted');
    };
  }, [connect]);

  return { progress, isConnected, error, reconnect };
};
