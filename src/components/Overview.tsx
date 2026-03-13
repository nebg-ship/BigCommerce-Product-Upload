import React from 'react';
import { Package, AlertCircle, RefreshCw } from 'lucide-react';
import { motion } from 'motion/react';
import { useQuery } from "convex/react";
import { api } from "../../convex/_generated/api";

export default function Overview({ onNavigate }: { onNavigate: (tab: string) => void }) {
  const stats = useQuery(api.products.getDashboardStats);

  if (!stats) return <div className="animate-pulse flex space-x-4"><div className="flex-1 space-y-4 py-1"><div className="h-4 bg-zinc-200 rounded w-3/4"></div><div className="space-y-2"><div className="h-4 bg-zinc-200 rounded"></div><div className="h-4 bg-zinc-200 rounded w-5/6"></div></div></div></div>;

  return (
    <motion.div 
      initial={{ opacity: 0, y: 10 }}
      animate={{ opacity: 1, y: 0 }}
      className="max-w-6xl mx-auto space-y-8"
    >
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        <StatCard 
          title="Total Products" 
          value={stats.totalProducts} 
          icon={<Package className="w-6 h-6 text-blue-500" />} 
          onClick={() => onNavigate('catalog')}
        />
        <StatCard 
          title="Total Variants" 
          value={stats.totalVariants} 
          icon={<Package className="w-6 h-6 text-indigo-500" />} 
          onClick={() => onNavigate('catalog')}
        />
        <StatCard 
          title="Pending Syncs" 
          value={stats.pendingSyncs} 
          icon={<RefreshCw className="w-6 h-6 text-amber-500" />} 
          onClick={() => onNavigate('queue')}
        />
        <StatCard 
          title="Failed / Dead Jobs" 
          value={stats.failedSyncs} 
          icon={<AlertCircle className="w-6 h-6 text-red-500" />} 
          onClick={() => onNavigate('queue')}
          alert={stats.failedSyncs > 0}
        />
      </div>

      <div className="bg-white rounded-2xl shadow-sm border border-zinc-200 p-6">
        <h2 className="text-lg font-medium text-zinc-900 mb-4">System Status</h2>
        <div className="flex items-center gap-4 p-4 bg-zinc-50 text-zinc-800 rounded-xl border border-zinc-200">
          <RefreshCw className="w-6 h-6 text-emerald-500" />
          <div>
            <p className="font-medium">BigCommerce queue processing is enabled</p>
            <p className="text-sm text-zinc-600 mt-1">
              Convex runs the queue processor every 60 seconds. Pending jobs can update existing
              records, create new products and variants, and create delete redirects once the
              BigCommerce credentials are configured on the Convex deployment.
            </p>
          </div>
        </div>
      </div>
    </motion.div>
  );
}

function StatCard({ title, value, icon, onClick, alert }: { title: string, value: number, icon: React.ReactNode, onClick: () => void, alert?: boolean }) {
  return (
    <div 
      onClick={onClick}
      className={`bg-white p-6 rounded-2xl shadow-sm border cursor-pointer transition-all hover:shadow-md ${alert ? 'border-red-200 bg-red-50/30' : 'border-zinc-200 hover:border-zinc-300'}`}
    >
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-sm font-medium text-zinc-500">{title}</h3>
        <div className="p-2 bg-zinc-50 rounded-lg">{icon}</div>
      </div>
      <div className="text-3xl font-semibold text-zinc-900">{value.toLocaleString()}</div>
    </div>
  );
}
