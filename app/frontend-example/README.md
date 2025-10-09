# Frontend WebSocket Test Examples

Dit zijn complete frontend voorbeelden om de Django Channels WebSocket integratie te testen.

## 📁 Bestanden

### 1. **websocket-test.html** - Standalone HTML Test Page
Een complete, standalone HTML pagina die je **direct** in de browser kan openen.

**Voordelen:**
- ✅ Geen build tools nodig
- ✅ Geen npm install
- ✅ Gewoon dubbelklik en open in browser
- ✅ Perfect voor quick testing

**Gebruik:**
```bash
# Gewoon openen in je browser
open websocket-test.html
# Of dubbelklik op het bestand
```

### 2. **React/TypeScript Components** - Voor Electron App

**Bestanden:**
- `hooks/useJobProgress.ts` - Custom React hook voor WebSocket verbinding
- `components/WebSocketTestPage.tsx` - Complete test pagina component
- `components/WebSocketTest.css` - Styling

**Gebruik in je Electron/React app:**

```typescript
// In je router
import WebSocketTestPage from './components/WebSocketTestPage';

<Route path="/websocket-test" element={<WebSocketTestPage />} />
```

---

## 🚀 Quick Start

### Optie 1: HTML Test Page (Makkelijkst!)

1. **Start Django server:**
   ```bash
   cd c:\Users\djang\Documents\carmenda\carmenda_pseudonymize_core\app
   python manage.py runserver
   ```

2. **Open test page:**
   ```bash
   # Windows
   start frontend-example\websocket-test.html
   
   # Of dubbelklik op:
   c:\Users\djang\Documents\carmenda\carmenda_pseudonymize_core\app\frontend-example\websocket-test.html
   ```

3. **Test:**
   - Enter job ID: `bigfile`
   - Click "Start Job & Monitor"
   - Watch real-time progress! 🎉

### Optie 2: React Components (Voor je Electron app)

1. **Kopieer de bestanden naar je React project:**
   ```bash
   # Kopieer naar je src folder
   cp -r hooks /path/to/your/electron/app/src/
   cp -r components /path/to/your/electron/app/src/
   ```

2. **Import in je app:**
   ```typescript
   import { WebSocketTestPage } from './components/WebSocketTestPage';
   
   function App() {
     return <WebSocketTestPage />;
   }
   ```

---

## 📊 Wat de test page doet

### Features:
1. **Job Control**
   - Start een job via POST request
   - Monitor progress via WebSocket
   - Stop monitoring

2. **WebSocket Status**
   - Connection status indicator (groen/rood)
   - Auto-reconnect bij disconnects
   - Connection details

3. **Real-time Progress**
   - Animated progress bar
   - Percentage, stage, status
   - Success/failure messages

4. **Live Logs**
   - Zie alle WebSocket events
   - Timestamps
   - JSON message payloads

---

## 🧪 Test Scenario's

### Test 1: Basis WebSocket Verbinding
```javascript
1. Open websocket-test.html
2. Enter job ID: "bigfile"
3. Click "Start Job & Monitor"
4. Verwacht: Status → Processing, Progress updates elke seconde
```

### Test 2: Meerdere Jobs Parallel
```javascript
1. Open websocket-test.html in Tab 1
2. Start job "bigfile"
3. Open websocket-test.html in Tab 2
4. Start job "bigfile_1"
5. Verwacht: Beide tabs tonen hun eigen progress
```

### Test 3: Reconnection
```javascript
1. Start een job
2. Disconnect WiFi (of stop Django server kort)
3. Reconnect WiFi (of start server opnieuw)
4. Click "Reconnect"
5. Verwacht: Verbinding herstelt, progress update herstart
```

### Test 4: Job Completion
```javascript
1. Start een kleine job (weinig data)
2. Wait for completion
3. Verwacht: Status → Completed, groen, 100%, success bericht
```

### Test 5: Job Failure
```javascript
1. Start job met niet-bestaand job ID
2. Verwacht: Error message, rood, failed status
```

---

## 🔍 Browser Console Logs

Je ziet in de browser console (F12) gedetailleerde logs:

```javascript
[WS] Connecting to ws://localhost:8000/ws/jobs/bigfile/progress/...
[WS] Connected to job bigfile
[WS] Message received: {type: "progress_update", percentage: 10, stage: "Loading data", status: "processing"}
[WS] Message received: {type: "progress_update", percentage: 20, stage: "Processing...", status: "processing"}
...
[WS] Job completed, closing connection in 2s
[WS] Disconnected (code: 1000)
```

---

## 🎨 UI Features

### Progress Bar
- **Blauw** → Processing
- **Groen** → Completed
- **Rood** → Failed
- **Grijs** → Pending

### Connection Indicator
- **Groene stip (pulsing)** → Connected
- **Rode stip** → Disconnected

### Messages
- **Groen banner** → Job completed ✅
- **Rood banner** → Job failed ❌

---

## 📝 Integratie in je Electron App

### 1. Custom Hook Gebruiken

```typescript
import { useJobProgress } from './hooks/useJobProgress';

function MyComponent() {
  const { progress, isConnected, error, reconnect } = useJobProgress('bigfile');

  return (
    <div>
      <p>Status: {isConnected ? '🟢 Connected' : '🔴 Disconnected'}</p>
      <p>Progress: {progress.percentage}%</p>
      <p>Stage: {progress.stage}</p>
      {error && <p>Error: {error}</p>}
      <button onClick={reconnect}>Reconnect</button>
    </div>
  );
}
```

### 2. Native Notifications

```typescript
// In je Electron main process
ipcMain.handle('notification', (event, { title, body }) => {
  const notification = new Notification({ title, body });
  notification.show();
});

// In je React component
useEffect(() => {
  if (progress.status === 'completed') {
    window.electron?.notification({
      title: 'Job Complete!',
      body: `Job ${jobId} finished successfully`,
    });
  }
}, [progress.status]);
```

### 3. Multiple Jobs Monitoren

```typescript
function Dashboard() {
  const [jobIds, setJobIds] = useState(['bigfile', 'bigfile_1']);

  return (
    <div>
      {jobIds.map(jobId => (
        <JobProgressCard key={jobId} jobId={jobId} />
      ))}
    </div>
  );
}

function JobProgressCard({ jobId }) {
  const { progress, isConnected } = useJobProgress(jobId);
  
  return (
    <div>
      <h3>{jobId}</h3>
      <ProgressBar percentage={progress.percentage} />
      <p>{progress.stage}</p>
    </div>
  );
}
```

---

## 🐛 Troubleshooting

### WebSocket verbindt niet

**Check:**
```bash
# 1. Is Django server running?
curl http://localhost:8000/api/v1/

# 2. Is Channels correct geïnstalleerd?
python -c "import channels; print(channels.__version__)"

# 3. Check ASGI configuratie
python manage.py check

# 4. Test Channel Layer
python manage.py shell
>>> from channels.layers import get_channel_layer
>>> cl = get_channel_layer()
>>> print(cl)
```

### Progress updates komen niet door

**Check:**
```python
# In views.py - zijn de channel_layer.group_send calls aanwezig?
async_to_sync(channel_layer.group_send)(
    f'job_progress_{job_id}',
    { ... }
)
```

### CORS errors in browser

**Check settings.py:**
```python
CORS_ALLOWED_ORIGINS = [
    'http://localhost:3000',
    'http://127.0.0.1:3000',
]
```

**Voor Electron apps:**
WebSockets hebben geen CORS, maar HTTP requests wel!

### Multiple tabs/windows update niet

Dit is normaal! Elk WebSocket is een aparte connectie. Als je wilt dat ALLE clients updates krijgen:

1. Alle clients joinen dezelfde `room_group_name`
2. Server stuurt update naar de group
3. Alle clients ontvangen de update

Dit is al geïmplementeerd! Elke client die connected naar hetzelfde job_id krijgt updates.

---

## 🎉 Je bent klaar!

Nu heb je:
- ✅ Complete HTML test page
- ✅ React/TypeScript components
- ✅ Custom WebSocket hook
- ✅ Styling
- ✅ Test scenario's
- ✅ Troubleshooting guide

**Veel plezier met real-time updates!** 🚀

---

## 📚 Extra Resources

- [Django Channels Docs](https://channels.readthedocs.io/)
- [WebSocket API](https://developer.mozilla.org/en-US/docs/Web/API/WebSocket)
- [React Hooks](https://react.dev/reference/react)
- [Electron IPC](https://www.electronjs.org/docs/latest/api/ipc-main)
