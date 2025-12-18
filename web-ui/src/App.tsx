import { BrowserRouter, Routes, Route } from 'react-router-dom';
import { Layout } from './components/Layout';
import { Dashboard } from './pages/Dashboard';
import { StreamsPage } from './pages/StreamsPage';
import { QueuesPage } from './pages/QueuesPage';
import { KVPage } from './pages/KVPage';
import { ReplayPage } from './pages/ReplayPage';
import { HealthPage } from './pages/HealthPage';

function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Layout />}>
          <Route index element={<Dashboard />} />
          <Route path="streams" element={<StreamsPage />} />
          <Route path="queues" element={<QueuesPage />} />
          <Route path="kv" element={<KVPage />} />
          <Route path="replay" element={<ReplayPage />} />
          <Route path="health" element={<HealthPage />} />
        </Route>
      </Routes>
    </BrowserRouter>
  );
}

export default App