import React, { useRef, useState } from 'react';
import { motion } from 'motion/react';
import { UploadCloud, FileText, CheckCircle2, AlertCircle, ChevronDown, ChevronUp } from 'lucide-react';
import { useQuery, useAction } from "convex/react";
import { api } from "../../convex/_generated/api";

const NON_SELECTABLE_UPDATE_HEADERS = new Set([
  'Product ID',
  'Code',
  'Item Type',
  'Image URL',
]);

function isNonSelectableUpdateHeader(header: string) {
  return NON_SELECTABLE_UPDATE_HEADERS.has(header) ||
    /^Product Image ID - \d+$/.test(header) ||
    /^Product Image URL - \d+$/.test(header) ||
    /^Product Image File - \d+$/.test(header);
}

export default function Imports() {
  const imports = useQuery(api.imports.getImports);
  const processCsv = useAction(api.importActions.processCsvAction);

  const [uploading, setUploading] = useState(false);
  const [uploadError, setUploadError] = useState<string | null>(null);
  const [uploadResult, setUploadResult] = useState<string | null>(null);
  const [expandedImport, setExpandedImport] = useState<string | null>(null);
  const [importType, setImportType] = useState<'update' | 'delete'>('update');
  const [pendingUpload, setPendingUpload] = useState<{
    content: string;
    filename: string;
    selectableHeaders: string[];
  } | null>(null);
  const [selectedFields, setSelectedFields] = useState<string[]>([]);
  const fileInputRef = useRef<HTMLInputElement>(null);

  const resetPendingUpload = () => {
    setPendingUpload(null);
    setSelectedFields([]);
  };

  const handleImportTypeChange = (nextImportType: 'update' | 'delete') => {
    setImportType(nextImportType);
    setUploadError(null);
    setUploadResult(null);
    resetPendingUpload();
    if (fileInputRef.current) {
      fileInputRef.current.value = '';
    }
  };

  const processUpload = async (filename: string, content: string, fields?: string[]) => {
    setUploading(true);
    setUploadError(null);
    setUploadResult(null);

    try {
      const res = await processCsv({
        filename,
        content,
        importType,
        selectedFields: importType === 'update' ? fields : undefined,
      });
      if (!res.success) {
        setUploadError('Upload failed');
        return;
      }

      setUploadResult(
        `Processed ${res.validCount} valid row${res.validCount === 1 ? '' : 's'} and ${res.invalidCount} invalid. ${res.changedRowCount} row${res.changedRowCount === 1 ? '' : 's'} changed, ${res.unchangedRowCount} unchanged, ${res.syncJobsCreatedCount} sync job${res.syncJobsCreatedCount === 1 ? '' : 's'} created.`,
      );
      resetPendingUpload();
    } catch (err: any) {
      console.error(err);
      setUploadError(err.message || 'Upload error occurred while sending data.');
    } finally {
      setUploading(false);
      if (fileInputRef.current) fileInputRef.current.value = '';
    }
  };

  const handleFileUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;

    setUploading(true);
    setUploadError(null);
    resetPendingUpload();
    const reader = new FileReader();
    reader.onload = async (event) => {
      const content = event.target?.result as string;

      const firstLine = content.split(/\r?\n/)[0];
      const headers = firstLine.split(',').map((h) => h.trim().replace(/^"|"$/g, ''));
      const requiredHeaders = importType === 'update'
        ? ['Product ID']
        : ['Product ID'];
      const missingHeaders = requiredHeaders.filter((h) => !headers.includes(h));

      if (missingHeaders.length > 0) {
        setUploadError(`Missing required headers: ${missingHeaders.join(', ')}`);
        setUploading(false);
        if (fileInputRef.current) fileInputRef.current.value = '';
        return;
      }

      if (importType === 'update') {
        const selectableHeaders = headers.filter((header) => !isNonSelectableUpdateHeader(header));
        if (selectableHeaders.length === 0) {
          setUploadError('No updateable fields were found in this CSV.');
          setUploading(false);
          if (fileInputRef.current) fileInputRef.current.value = '';
          return;
        }

        setPendingUpload({
          content,
          filename: file.name,
          selectableHeaders,
        });
        setSelectedFields(selectableHeaders);
        setUploading(false);
        if (fileInputRef.current) fileInputRef.current.value = '';
        return;
      }

      await processUpload(file.name, content);
    };
    reader.readAsText(file);
  };

  const toggleSelectedField = (field: string) => {
    setSelectedFields((currentFields) => (
      currentFields.includes(field)
        ? currentFields.filter((currentField) => currentField !== field)
        : [...currentFields, field]
    ));
  };

  const renderErrors = (errorsStr: string | null | undefined) => {
    if (!errorsStr) return null;
    try {
      const errors = JSON.parse(errorsStr);
      if (!Array.isArray(errors) || errors.length === 0) return null;

      return (
        <div className="mt-4 bg-red-50 border border-red-100 rounded-lg p-4">
          <h5 className="text-sm font-medium text-red-800 mb-3 flex items-center gap-2">
            <AlertCircle className="w-4 h-4" />
            Row Errors ({errors.length})
          </h5>
          <div className="space-y-3 max-h-60 overflow-y-auto pr-2">
            {errors.map((err, idx) => (
              <div key={idx} className="bg-white p-3 rounded border border-red-100 shadow-sm text-sm">
                <div className="font-medium text-red-700 mb-1">Row {err.row}: {err.error}</div>
                <div className="text-xs text-zinc-500 font-mono bg-zinc-50 p-2 rounded overflow-x-auto">
                  {JSON.stringify(err.data)}
                </div>
              </div>
            ))}
          </div>
        </div>
      );
    } catch {
      return <div className="text-sm text-red-500 mt-2">Could not parse error details.</div>;
    }
  };

  return (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      className="max-w-5xl mx-auto space-y-8"
    >
      {uploadResult && (
        <div className="bg-emerald-50 border border-emerald-200 text-emerald-700 px-4 py-3 rounded-lg">
          <p className="text-sm font-medium">{uploadResult}</p>
        </div>
      )}

      {uploadError && (
        <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded-lg flex items-center gap-2">
          <AlertCircle className="w-5 h-5 shrink-0" />
          <p className="text-sm font-medium">{uploadError}</p>
        </div>
      )}

      <div className="bg-white p-8 rounded-2xl shadow-sm border border-zinc-200">
        <div className="mb-6 flex items-center justify-center gap-4">
          <label className="flex items-center gap-2 cursor-pointer">
            <input
              type="radio"
              name="importType"
              value="update"
              checked={importType === 'update'}
              onChange={() => handleImportTypeChange('update')}
              className="w-4 h-4 text-emerald-600 focus:ring-emerald-500"
            />
            <span className="text-sm font-medium text-zinc-900">Update Products</span>
          </label>
          <label className="flex items-center gap-2 cursor-pointer">
            <input
              type="radio"
              name="importType"
              value="delete"
              checked={importType === 'delete'}
              onChange={() => handleImportTypeChange('delete')}
              className="w-4 h-4 text-red-600 focus:ring-red-500"
            />
            <span className="text-sm font-medium text-zinc-900">Delete Products</span>
          </label>
        </div>

        <div
          className={`border-2 border-dashed rounded-2xl p-12 text-center transition-colors cursor-pointer ${
            importType === 'delete'
              ? 'border-red-200 bg-red-50/30 hover:bg-red-50'
              : 'border-zinc-300 bg-zinc-50/50 hover:bg-zinc-50'
          }`}
          onClick={() => fileInputRef.current?.click()}
        >
          <input
            type="file"
            accept=".csv"
            className="hidden"
            ref={fileInputRef}
            onChange={handleFileUpload}
          />
          <div className={`w-16 h-16 rounded-full flex items-center justify-center mx-auto mb-4 ${
            importType === 'delete' ? 'bg-red-100 text-red-600' : 'bg-emerald-100 text-emerald-600'
          }`}>
            <UploadCloud className="w-8 h-8" />
          </div>
          <h3 className="text-lg font-medium text-zinc-900 mb-2">
            {uploading ? 'Processing CSV...' : `Upload CSV to ${importType === 'delete' ? 'Delete' : 'Update'} Products`}
          </h3>
          <p className="text-zinc-500 text-sm max-w-md mx-auto">
            {importType === 'delete'
              ? 'Drag and drop your CSV here. The file must contain at least "Product ID". Products found in the CSV will be deleted.'
              : 'Drag and drop your catalog CSV here. After upload, pick the fields you want to update. "Product ID" or "Code" is used to match existing products. "Item Type" is optional if present. Image imports support numbered columns such as "Product Image ID - 1", "- 2", "- 3", and matching description/sort/thumbnail fields.'}
          </p>
        </div>

        {pendingUpload && importType === 'update' && (
          <div className="mt-6 rounded-2xl border border-zinc-200 bg-zinc-50 p-6 space-y-4">
            <div className="flex items-start justify-between gap-4">
              <div>
                <h4 className="text-base font-medium text-zinc-900">Choose Fields To Update</h4>
                <p className="text-sm text-zinc-500 mt-1">
                  {pendingUpload.filename} loaded. Product matching still uses `Product ID` or `Code` automatically.
                </p>
              </div>
              <div className="text-sm text-zinc-500">
                {selectedFields.length} of {pendingUpload.selectableHeaders.length} selected
              </div>
            </div>

            <div className="flex flex-wrap gap-2">
              <button
                type="button"
                onClick={() => setSelectedFields(pendingUpload.selectableHeaders)}
                className="px-3 py-1.5 rounded-lg border border-zinc-300 bg-white text-sm font-medium text-zinc-700 hover:bg-zinc-100"
              >
                Select All
              </button>
              <button
                type="button"
                onClick={() => setSelectedFields([])}
                className="px-3 py-1.5 rounded-lg border border-zinc-300 bg-white text-sm font-medium text-zinc-700 hover:bg-zinc-100"
              >
                Clear All
              </button>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
              {pendingUpload.selectableHeaders.map((header) => (
                <label
                  key={header}
                  className="flex items-center gap-3 rounded-xl border border-zinc-200 bg-white px-4 py-3 cursor-pointer hover:border-zinc-300"
                >
                  <input
                    type="checkbox"
                    checked={selectedFields.includes(header)}
                    onChange={() => toggleSelectedField(header)}
                    className="w-4 h-4 text-emerald-600 rounded border-zinc-300 focus:ring-emerald-500"
                  />
                  <span className="text-sm font-medium text-zinc-800">{header}</span>
                </label>
              ))}
            </div>

            <div className="flex items-center justify-between gap-4">
              <p className="text-xs text-zinc-500">
                Helper columns such as `Product ID`, `Code`, `Product Image ID - N`, and image URL/file columns are still used automatically when needed.
              </p>
              <button
                type="button"
                onClick={() => processUpload(pendingUpload.filename, pendingUpload.content, selectedFields)}
                disabled={uploading || selectedFields.length === 0}
                className="px-4 py-2 rounded-lg bg-zinc-900 text-white text-sm font-medium hover:bg-zinc-800 disabled:opacity-50"
              >
                {uploading ? 'Processing CSV...' : 'Process Selected Fields'}
              </button>
            </div>
          </div>
        )}
      </div>

      <div className="bg-white rounded-xl shadow-sm border border-zinc-200 overflow-hidden">
        <div className="px-6 py-4 border-b border-zinc-200 bg-zinc-50">
          <h3 className="font-medium text-zinc-900">Recent Import Runs</h3>
        </div>
        <div className="divide-y divide-zinc-200">
          {imports === undefined ? (
            <div className="p-8 text-center text-zinc-500">Loading imports...</div>
          ) : imports.length === 0 ? (
            <div className="p-8 text-center text-zinc-500">No imports found.</div>
          ) : (
            imports.map((run) => (
              <div key={run._id} className="flex flex-col hover:bg-zinc-50/50 transition-colors">
                <div className="p-6 flex items-center justify-between">
                  <div className="flex items-center gap-4">
                    <div className="p-3 bg-zinc-100 rounded-lg">
                      <FileText className="w-6 h-6 text-zinc-500" />
                    </div>
                    <div>
                      <h4 className="font-medium text-zinc-900 flex items-center gap-2">
                        {run.file_name}
                      </h4>
                      <p className="text-sm text-zinc-500 mt-1">
                        {new Date(run.created_at).toLocaleString()} - {run.row_count} rows total
                      </p>
                    </div>
                  </div>
                  <div className="flex items-center gap-6">
                    <div className="text-right">
                      <div className="flex items-center gap-2 text-sm text-emerald-600 font-medium">
                        <CheckCircle2 className="w-4 h-4" />
                        {run.valid_row_count} Valid
                      </div>
                      {(run.changed_row_count !== undefined || run.sync_jobs_created_count !== undefined) && (
                        <div className="text-xs text-zinc-500 mt-1">
                          {run.changed_row_count ?? 0} changed, {run.unchanged_row_count ?? 0} unchanged, {run.sync_jobs_created_count ?? 0} sync jobs
                        </div>
                      )}
                      {run.invalid_row_count > 0 && (
                        <div className="flex items-center gap-2 text-sm text-red-600 font-medium mt-1">
                          <AlertCircle className="w-4 h-4" />
                          {run.invalid_row_count} Invalid
                          <button
                            onClick={() => setExpandedImport(expandedImport === run._id ? null : run._id)}
                            className="ml-2 text-xs bg-red-100 hover:bg-red-200 text-red-700 px-2 py-0.5 rounded transition-colors flex items-center gap-1"
                          >
                            {expandedImport === run._id ? 'Hide Errors' : 'View Errors'}
                            {expandedImport === run._id ? <ChevronUp className="w-3 h-3" /> : <ChevronDown className="w-3 h-3" />}
                          </button>
                        </div>
                      )}
                    </div>
                    <span className={`px-3 py-1 rounded-full text-xs font-medium ${
                      run.status === 'processed' ? 'bg-emerald-100 text-emerald-700' : 'bg-amber-100 text-amber-700'
                    }`}>
                      {run.status.toUpperCase()}
                    </span>
                  </div>
                </div>
                {expandedImport === run._id && run.invalid_row_count > 0 && (
                  <div className="px-6 pb-6 pt-0">
                    {renderErrors(run.errors)}
                  </div>
                )}
              </div>
            ))
          )}
        </div>
      </div>
    </motion.div>
  );
}
