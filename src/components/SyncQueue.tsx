import React, { useState } from 'react';
import { motion } from 'motion/react';
import { RefreshCw, AlertTriangle, XCircle, CheckCircle2, Clock, ChevronDown, ChevronUp } from 'lucide-react';
import { useQuery, useMutation } from "convex/react";
import { api } from "../../convex/_generated/api";

export default function SyncQueue() {
  const jobs = useQuery(api.sync.getSyncJobs);
  const retryJob = useMutation(api.sync.retrySyncJob);
  const [filter, setFilter] = useState<string>('all');
  const [expandedJob, setExpandedJob] = useState<string | null>(null);

  const handleRetry = async (id: any) => {
    await retryJob({ id });
  };

  const filteredJobs = filter === 'all' ? (jobs || []) : (jobs || []).filter(j => j.status === filter);

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'success': return <CheckCircle2 className="w-4 h-4 text-emerald-500" />;
      case 'failed': return <AlertTriangle className="w-4 h-4 text-amber-500" />;
      case 'dead': return <XCircle className="w-4 h-4 text-red-500" />;
      case 'processing': return <RefreshCw className="w-4 h-4 text-blue-500 animate-spin" />;
      default: return <Clock className="w-4 h-4 text-zinc-400" />;
    }
  };

  const renderChanges = (payload: string | null | undefined) => {
    if (!payload) return <span className="text-zinc-400 italic">No changes recorded</span>;
    try {
      const changes = JSON.parse(payload);
      const keys = Object.keys(changes);
      if (keys.length === 0) return <span className="text-zinc-400 italic">No changes</span>;
      
      return (
        <div className="space-y-1">
          {keys.map(key => {
            const change = changes[key];
            return (
              <div key={key} className="text-xs">
                <span className="font-semibold text-zinc-700">{key}: </span>
                {change.old !== undefined && (
                  <span className="text-red-500 line-through mr-1">{String(change.old)}</span>
                )}
                <span className="text-emerald-600">{String(change.new)}</span>
              </div>
            );
          })}
        </div>
      );
    } catch (e) {
      return <span className="text-zinc-400 italic">Invalid payload</span>;
    }
  };

  return (
    <motion.div 
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      className="max-w-7xl mx-auto space-y-6"
    >
      <div className="flex gap-2">
        {['all', 'pending', 'processing', 'success', 'failed', 'dead'].map(f => (
          <button
            key={f}
            onClick={() => setFilter(f)}
            className={`px-4 py-2 rounded-lg text-sm font-medium capitalize transition-colors ${
              filter === f 
                ? 'bg-zinc-900 text-white' 
                : 'bg-white border border-zinc-200 text-zinc-600 hover:bg-zinc-50'
            }`}
          >
            {f}
          </button>
        ))}
      </div>

      <div className="bg-white rounded-xl shadow-sm border border-zinc-200 overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full text-left text-sm">
            <thead className="bg-zinc-50 border-b border-zinc-200 text-zinc-500">
              <tr>
                <th className="px-6 py-3 font-medium">Job ID</th>
                <th className="px-6 py-3 font-medium">Entity</th>
                <th className="px-6 py-3 font-medium">Action</th>
                <th className="px-6 py-3 font-medium">Status</th>
                <th className="px-6 py-3 font-medium">Attempts</th>
                <th className="px-6 py-3 font-medium">Error Message</th>
                <th className="px-6 py-3 font-medium text-right">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-zinc-200">
              {jobs === undefined ? (
                <tr><td colSpan={7} className="px-6 py-8 text-center text-zinc-500">Loading queue...</td></tr>
              ) : filteredJobs.length === 0 ? (
                <tr><td colSpan={7} className="px-6 py-8 text-center text-zinc-500">No jobs found.</td></tr>
              ) : (
                filteredJobs.map(job => (
                  <React.Fragment key={job._id}>
                    <tr className="hover:bg-zinc-50/50 transition-colors">
                      <td className="px-6 py-4 font-mono text-xs text-zinc-500">
                        <div className="flex items-center gap-2">
                          <button 
                            onClick={() => setExpandedJob(expandedJob === job._id ? null : job._id)}
                            className="p-1 hover:bg-zinc-200 rounded text-zinc-400 hover:text-zinc-600"
                          >
                            {expandedJob === job._id ? <ChevronUp className="w-4 h-4" /> : <ChevronDown className="w-4 h-4" />}
                          </button>
                          {job._id.substring(0, 8)}...
                        </div>
                      </td>
                      <td className="px-6 py-4">
                        <div className="font-medium text-zinc-900 capitalize">{job.entity_type}</div>
                        <div className="font-mono text-xs text-zinc-500 mt-0.5">{job.internal_id}</div>
                      </td>
                      <td className="px-6 py-4">
                        <span className="px-2 py-1 bg-zinc-100 text-zinc-700 rounded text-xs font-medium uppercase tracking-wider">
                          {job.action}
                        </span>
                      </td>
                      <td className="px-6 py-4">
                        <div className="flex items-center gap-2 capitalize font-medium text-zinc-700">
                          {getStatusIcon(job.status)}
                          {job.status}
                        </div>
                      </td>
                      <td className="px-6 py-4 text-zinc-600">{job.attempts}</td>
                      <td className="px-6 py-4 text-xs text-red-600 max-w-xs truncate" title={job.error_message || ''}>
                        {job.error_message || '-'}
                      </td>
                      <td className="px-6 py-4 text-right">
                        {(job.status === 'failed' || job.status === 'dead') && (
                          <button 
                            onClick={() => handleRetry(job._id)}
                            className="px-3 py-1.5 bg-white border border-zinc-300 rounded-md text-xs font-medium text-zinc-700 hover:bg-zinc-50 hover:text-zinc-900 transition-colors"
                          >
                            Retry
                          </button>
                        )}
                      </td>
                    </tr>
                    {expandedJob === job._id && (
                      <tr className="bg-zinc-50/80 border-t-0">
                        <td colSpan={7} className="px-14 py-4">
                          <div className="text-sm font-medium text-zinc-700 mb-2">Pending Changes:</div>
                          <div className="bg-white p-4 rounded-lg border border-zinc-200 shadow-sm">
                            {renderChanges(job.payload)}
                          </div>
                        </td>
                      </tr>
                    )}
                  </React.Fragment>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>
    </motion.div>
  );
}
