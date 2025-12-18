# FlowMesh Web UI

React-based web dashboard for FlowMesh Unified Event Fabric.

## Development

### Prerequisites

- Node.js 20.19+ or 22.12+
- npm 10+

### Setup

```bash
npm install
```

### Run Development Server

```bash
npm run dev
```

The development server will start on `http://localhost:5173`.

**Note**: In development, the Web UI runs on a separate port (5173) and connects to the FlowMesh backend API (default: `http://localhost:8080`). Make sure the FlowMesh server is running before using the Web UI.

### Build for Production

```bash
npm run build
```

The build output will be in the `dist/` directory. The built files will be automatically served by the FlowMesh Go server when you run the main application.

### Preview Production Build

```bash
npm run preview
```

This will start a local preview server to test the production build.

## Project Structure

```
web-ui/
├── src/
│   ├── components/    # React components
│   │   ├── Layout.tsx
│   │   ├── Header.tsx
│   │   ├── Sidebar.tsx
│   │   ├── LoadingSpinner.tsx
│   │   ├── ErrorAlert.tsx
│   │   └── StatusBadge.tsx
│   ├── hooks/         # React hooks
│   │   └── useApi.ts  # API call hooks (useHealthCheck, useApiCall)
│   ├── lib/           # Utilities and API client
│   │   ├── api.ts     # Main API client
│   │   ├── auth.ts    # Authentication utilities
│   │   └── config.ts  # Configuration
│   ├── pages/         # Page components
│   │   ├── Dashboard.tsx
│   │   ├── StreamsPage.tsx
│   │   ├── QueuesPage.tsx
│   │   ├── KVPage.tsx
│   │   ├── ReplayPage.tsx
│   │   └── HealthPage.tsx
│   ├── types/         # TypeScript type definitions
│   │   └── api.ts     # API request/response types
│   ├── App.tsx        # Main app component with routing
│   └── main.tsx       # Entry point
├── dist/              # Production build output (served by Go server)
└── package.json
```

## Configuration

The API base URL can be configured via environment variables:
- `VITE_API_BASE_URL` (default: `http://localhost:8080`)

Create a `.env` file in the `web-ui/` directory to customize:

```env
VITE_API_BASE_URL=http://localhost:8080
```

## API Client Usage

The API client is fully typed and provides functions for all FlowMesh operations:

```typescript
import { healthCheck, enqueue, writeEvents } from './lib/api';

// Health check
const health = await healthCheck();

// Queue operations
const result = await enqueue('tenant', 'namespace', 'queue-name', {
  payload: new TextEncoder().encode('Hello, World!'),
});

// Stream operations
const response = await writeEvents('tenant', 'namespace', 'stream-name', {
  events: [
    { payload: new TextEncoder().encode('Event data'), headers: {} }
  ]
});
```

## React Hooks

Use the provided hooks for API calls with loading/error states:

```typescript
import { useHealthCheck } from './hooks/useApi';

function MyComponent() {
  const { isHealthy, isReady, loading, error } = useHealthCheck(5000); // Poll every 5s
  
  if (loading) return <LoadingSpinner />;
  if (error) return <ErrorAlert error={error} />;
  
  return <div>Health: {isHealthy ? 'OK' : 'DOWN'}</div>;
}
```

## Authentication

Currently, the Web UI supports token-based authentication. Tokens are stored in `localStorage` under the key `flowmesh_api_token`.

For development/testing, the FlowMesh server creates a default token. In production, users will need to provide their API token.

## Development Workflow

1. Start the FlowMesh backend server:
   ```bash
   cd ../engine
   ./bin/flowmesh
   ```

2. In a separate terminal, start the Web UI dev server:
   ```bash
   cd web-ui
   npm run dev
   ```

3. Open `http://localhost:5173` in your browser

4. The Web UI will automatically connect to the backend API

## Production Deployment

In production, the Web UI is built and served by the FlowMesh Go server:

1. Build the Web UI:
   ```bash
   cd web-ui
   npm run build
   ```

2. The Go server will automatically detect and serve files from `web-ui/dist/` when available

3. Access the Web UI at the same URL as the API server (default: `http://localhost:8080`)

## Documentation

For more information, see the main [README.md](../README.md).