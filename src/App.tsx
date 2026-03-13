/**
 * @license
 * SPDX-License-Identifier: Apache-2.0
 */

import React, { useState } from 'react';
import { LayoutDashboard, Package, UploadCloud, RefreshCw, Settings } from 'lucide-react';
import Overview from './components/Overview';
import Catalog from './components/Catalog';
import Imports from './components/Imports';
import SyncQueue from './components/SyncQueue';

export default function App() {
  const [activeTab, setActiveTab] = useState('overview');

  const renderContent = () => {
    switch (activeTab) {
      case 'overview': return <Overview onNavigate={setActiveTab} />;
      case 'catalog': return <Catalog />;
      case 'imports': return <Imports />;
      case 'queue': return <SyncQueue />;
      default: return <Overview onNavigate={setActiveTab} />;
    }
  };

  return (
    <div className="flex h-screen bg-zinc-50 font-sans text-zinc-900">
      {/* Sidebar */}
      <div className="w-64 bg-zinc-900 text-zinc-300 flex flex-col">
        <div className="p-6 flex items-center gap-3 text-white">
          <RefreshCw className="w-6 h-6 text-emerald-400" />
          <span className="font-semibold text-lg tracking-tight">Catalog Sync</span>
        </div>
        
        <nav className="flex-1 px-4 space-y-1 mt-4">
          <NavItem 
            icon={<LayoutDashboard className="w-5 h-5" />} 
            label="Overview" 
            active={activeTab === 'overview'} 
            onClick={() => setActiveTab('overview')} 
          />
          <NavItem 
            icon={<Package className="w-5 h-5" />} 
            label="Canonical Catalog" 
            active={activeTab === 'catalog'} 
            onClick={() => setActiveTab('catalog')} 
          />
          <NavItem 
            icon={<UploadCloud className="w-5 h-5" />} 
            label="CSV Imports" 
            active={activeTab === 'imports'} 
            onClick={() => setActiveTab('imports')} 
          />
          <NavItem 
            icon={<RefreshCw className="w-5 h-5" />} 
            label="Sync Queue" 
            active={activeTab === 'queue'} 
            onClick={() => setActiveTab('queue')} 
          />
        </nav>

        <div className="p-4 border-t border-zinc-800">
          <div className="flex items-center gap-3 px-2 py-2 text-sm text-zinc-400 hover:text-white cursor-pointer transition-colors">
            <Settings className="w-5 h-5" />
            <span>Settings</span>
          </div>
        </div>
      </div>

      {/* Main Content */}
      <div className="flex-1 flex flex-col overflow-hidden">
        <header className="h-16 bg-white border-b border-zinc-200 flex items-center px-8 shadow-sm z-10">
          <h1 className="text-xl font-medium text-zinc-800 capitalize">
            {activeTab === 'queue' ? 'Sync Queue' : activeTab.replace('-', ' ')}
          </h1>
        </header>
        <main className="flex-1 overflow-auto p-8">
          {renderContent()}
        </main>
      </div>
    </div>
  );
}

function NavItem({ icon, label, active, onClick }: { icon: React.ReactNode, label: string, active: boolean, onClick: () => void }) {
  return (
    <button
      onClick={onClick}
      className={`w-full flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium transition-colors ${
        active 
          ? 'bg-emerald-500/10 text-emerald-400' 
          : 'text-zinc-400 hover:bg-zinc-800 hover:text-zinc-200'
      }`}
    >
      {icon}
      {label}
    </button>
  );
}
