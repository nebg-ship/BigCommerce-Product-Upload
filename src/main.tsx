import {StrictMode} from 'react';
import {createRoot} from 'react-dom/client';
import { ConvexProvider, ConvexReactClient } from "convex/react";
import App from './App.tsx';
import './index.css';

// Initialize Convex Client. Fallback to a dummy URL to prevent crashes if env var is missing during setup.
const convexUrl = import.meta.env.VITE_CONVEX_URL || "https://happy-animal-123.convex.cloud";
const convex = new ConvexReactClient(convexUrl);

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <ConvexProvider client={convex}>
      <App />
    </ConvexProvider>
  </StrictMode>,
);
