import React, { useState } from 'react';
import { useJobProgress } from '../hooks/useJobProgress';
import './WebSocketTest.css';

interface Job {
  job_id: string;
  status: string;
}

export const WebSocketTestPage: React.FC = () => {
  const [jobId, setJobId] = useState<string>('bigfile');
  const [activeJobId, setActiveJobId] = useState<string | null>(null);
  const [apiResponse, setApiResponse] = useState<string>('');
  const [isStarting, setIsStarting] = useState(false);

  const { progress, isConnected, error, reconnect } = useJobProgress(
    activeJobId || 'none'
  );

  const startJob = async () => {
    if (!jobId.trim()) {
      alert('Please enter a job ID');
      return;
    }

    setIsStarting(true);
    setApiResponse('Starting job...');

    try {
      // Start the job via POST
      const response = await fetch(
        `http://localhost:8000/api/v1/jobs/${jobId}/process/`,
        {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
        }
      );

      const data = await response.json();
      setApiResponse(JSON.stringify(data, null, 2));

      if (response.ok) {
        // Start monitoring this job
        setActiveJobId(jobId);
      } else {
        alert(`Failed to start job: ${data.error || 'Unknown error'}`);
      }
    } catch (err) {
      const errorMsg = err instanceof Error ? err.message : 'Unknown error';
      setApiResponse(`Error: ${errorMsg}`);
      alert(`Failed to connect to API: ${errorMsg}`);
    } finally {
      setIsStarting(false);
    }
  };

  const stopMonitoring = () => {
    setActiveJobId(null);
    setApiResponse('');
  };

  const getStatusColor = () => {
    switch (progress.status) {
      case 'completed':
        return '#10b981'; // green
      case 'failed':
        return '#ef4444'; // red
      case 'processing':
        return '#3b82f6'; // blue
      default:
        return '#6b7280'; // gray
    }
  };

  return (
    <div className="websocket-test-page">
      <div className="container">
        <h1>🚀 Django Channels WebSocket Test</h1>
        <p className="subtitle">
          Real-time job progress monitoring via WebSockets
        </p>

        {/* Job Control Panel */}
        <div className="card">
          <h2>📋 Job Control</h2>
          
          <div className="input-group">
            <label htmlFor="jobId">Job ID:</label>
            <input
              id="jobId"
              type="text"
              value={jobId}
              onChange={(e) => setJobId(e.target.value)}
              placeholder="Enter job ID (e.g., bigfile)"
              disabled={activeJobId !== null}
            />
          </div>

          <div className="button-group">
            <button
              className="btn btn-primary"
              onClick={startJob}
              disabled={isStarting || activeJobId !== null}
            >
              {isStarting ? 'Starting...' : '▶️ Start Job & Monitor'}
            </button>

            {activeJobId && (
              <button
                className="btn btn-secondary"
                onClick={stopMonitoring}
              >
                ⏹️ Stop Monitoring
              </button>
            )}
          </div>

          {apiResponse && (
            <div className="api-response">
              <h3>API Response:</h3>
              <pre>{apiResponse}</pre>
            </div>
          )}
        </div>

        {/* WebSocket Connection Status */}
        {activeJobId && (
          <div className="card">
            <h2>🔌 WebSocket Connection</h2>
            
            <div className="connection-status">
              <div className="status-indicator">
                <div
                  className={`status-dot ${isConnected ? 'connected' : 'disconnected'}`}
                />
                <span className="status-text">
                  {isConnected ? 'Connected' : 'Disconnected'}
                </span>
              </div>

              <div className="connection-details">
                <p>
                  <strong>Job ID:</strong> {activeJobId}
                </p>
                <p>
                  <strong>WebSocket URL:</strong>{' '}
                  ws://localhost:8000/ws/jobs/{activeJobId}/progress/
                </p>
              </div>

              {!isConnected && (
                <button className="btn btn-small" onClick={reconnect}>
                  🔄 Reconnect
                </button>
              )}
            </div>

            {error && (
              <div className="error-message">
                ⚠️ {error}
              </div>
            )}
          </div>
        )}

        {/* Progress Display */}
        {activeJobId && (
          <div className="card">
            <h2>📊 Job Progress</h2>

            <div className="progress-container">
              <div className="progress-bar-wrapper">
                <div
                  className="progress-bar"
                  style={{
                    width: `${progress.percentage}%`,
                    backgroundColor: getStatusColor(),
                  }}
                >
                  <span className="progress-text">{progress.percentage}%</span>
                </div>
              </div>

              <div className="progress-info">
                <div className="info-row">
                  <span className="label">Status:</span>
                  <span
                    className="badge"
                    style={{ backgroundColor: getStatusColor() }}
                  >
                    {progress.status.toUpperCase()}
                  </span>
                </div>

                <div className="info-row">
                  <span className="label">Stage:</span>
                  <span className="value">{progress.stage}</span>
                </div>

                <div className="info-row">
                  <span className="label">Progress:</span>
                  <span className="value">
                    {progress.percentage}% complete
                  </span>
                </div>
              </div>
            </div>

            {progress.status === 'completed' && (
              <div className="success-message">
                ✅ Job completed successfully!
              </div>
            )}

            {progress.status === 'failed' && (
              <div className="error-message">
                ❌ Job failed. Check the error details above.
              </div>
            )}
          </div>
        )}

        {/* Instructions */}
        <div className="card instructions">
          <h2>📖 How to Use</h2>
          <ol>
            <li>
              Make sure Django server is running:{' '}
              <code>python manage.py runserver</code>
            </li>
            <li>Enter a job ID (e.g., "bigfile", "bigfile_1")</li>
            <li>Click "Start Job & Monitor" to begin processing</li>
            <li>Watch the real-time progress updates via WebSocket!</li>
            <li>You can open multiple browser tabs to monitor different jobs</li>
          </ol>

          <h3>🧪 Testing Tips:</h3>
          <ul>
            <li>
              <strong>Create a job first:</strong> POST to{' '}
              <code>/api/v1/jobs/</code> with input file
            </li>
            <li>
              <strong>Check existing jobs:</strong> GET{' '}
              <code>/api/v1/jobs/</code>
            </li>
            <li>
              <strong>Multiple jobs:</strong> Open this page in multiple tabs
              with different job IDs
            </li>
            <li>
              <strong>Connection issues:</strong> Check browser console (F12)
              for WebSocket logs
            </li>
          </ul>
        </div>

        {/* Debug Info */}
        <div className="card debug">
          <h2>🐛 Debug Information</h2>
          <div className="debug-grid">
            <div className="debug-item">
              <strong>Connected:</strong> {isConnected ? 'Yes' : 'No'}
            </div>
            <div className="debug-item">
              <strong>Active Job:</strong> {activeJobId || 'None'}
            </div>
            <div className="debug-item">
              <strong>Progress:</strong> {progress.percentage}%
            </div>
            <div className="debug-item">
              <strong>Status:</strong> {progress.status}
            </div>
            <div className="debug-item">
              <strong>Stage:</strong> {progress.stage}
            </div>
            <div className="debug-item">
              <strong>Error:</strong> {error || 'None'}
            </div>
          </div>
          <p className="debug-note">
            💡 Open browser DevTools (F12) → Console to see detailed WebSocket
            logs
          </p>
        </div>
      </div>
    </div>
  );
};

export default WebSocketTestPage;
