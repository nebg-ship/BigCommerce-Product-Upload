import React, { useState } from 'react';
import { Product } from '../types';
import { motion } from 'motion/react';
import { Search, Filter, DownloadCloud, Loader2, Edit2, X, Trash2, AlertTriangle } from 'lucide-react';
import { useQuery, useMutation, useAction, usePaginatedQuery } from "convex/react";
import { api } from "../../convex/_generated/api";

const CONVEX_CONNECTION_LOST_MESSAGE = 'Connection lost while action was in flight';
const MAX_PULL_RETRIES = 3;
const RETRY_DELAY_MS = 1500;

function getErrorMessage(error: unknown) {
  return error instanceof Error ? error.message : String(error ?? 'Unknown error');
}

function isRetriablePullError(error: unknown) {
  const message = getErrorMessage(error).toLowerCase();
  return (
    message.includes(CONVEX_CONNECTION_LOST_MESSAGE.toLowerCase()) ||
    message.includes('networkerror') ||
    message.includes('failed to fetch')
  );
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export default function Catalog() {
  const stats = useQuery(api.products.getDashboardStats);
  const {
    results: products,
    status: productsStatus,
    loadMore,
  } = usePaginatedQuery(api.products.getProducts, {}, { initialNumItems: 50 });
  const updateProduct = useMutation(api.products.updateProduct);
  const deleteProduct = useMutation(api.products.deleteProduct);
  const getCategoryUrl = useAction((api.bigcommerce as any).getCategoryUrl);
  const pullFromBigCommerce = useAction(api.bigcommerce.pullFromBigCommerce);

  const [pulling, setPulling] = useState(false);
  const [pullError, setPullError] = useState<string | null>(null);
  const [pullProgress, setPullProgress] = useState<string | null>(null);
  const [searchQuery, setSearchQuery] = useState('');
  
  const [editingProduct, setEditingProduct] = useState<Product | null>(null);
  const [editForm, setEditForm] = useState<Partial<Product>>({});
  const [saving, setSaving] = useState(false);
  
  const [deletingProduct, setDeletingProduct] = useState<Product | null>(null);
  const [isDeleting, setIsDeleting] = useState(false);
  const [deleteRedirectUrl, setDeleteRedirectUrl] = useState('');
  const [isFetchingCategoryUrl, setIsFetchingCategoryUrl] = useState(false);

  const handlePullFromBigCommerce = async () => {
    setPulling(true);
    setPullError(null);
    setPullProgress('Starting BigCommerce pull...');
    try {
      let nextPage: number | null = 1;
      let totalProcessed = 0;
      let totalInserted = 0;
      let totalUpdated = 0;
      let totalUnchanged = 0;
      let totalPages: number | null = null;
      let channelName = 'Bonsai Outlet';

      while (nextPage) {
        const pageToPull = nextPage;
        let res;

        for (let attempt = 1; attempt <= MAX_PULL_RETRIES; attempt += 1) {
          try {
            res = await pullFromBigCommerce({ page: pageToPull });
            break;
          } catch (err) {
            if (!isRetriablePullError(err) || attempt === MAX_PULL_RETRIES) {
              throw err;
            }

            setPullProgress(
              `Connection dropped while pulling ${channelName}. Retrying page ${pageToPull} (${attempt + 1}/${MAX_PULL_RETRIES})...`,
            );
            await sleep(RETRY_DELAY_MS * attempt);
          }
        }

        if (!res) {
          throw new Error(`Failed to pull page ${pageToPull}.`);
        }

        totalProcessed += res.count;
        totalInserted += res.insertedProducts;
        totalUpdated += res.updatedProducts;
        totalUnchanged += res.unchangedProducts;
        totalPages = res.totalPages;
        channelName = res.channelName;
        const pageRange = res.lastProcessedPage > res.currentPage
          ? `pages ${res.currentPage}-${res.lastProcessedPage}`
          : `page ${res.currentPage}`;
        const remoteTotal = typeof res.totalCount === 'number' ? res.totalCount.toLocaleString() : null;
        setPullProgress(
          `Pulling ${channelName}: ${pageRange}${res.totalPages ? ` of ${res.totalPages}` : ''}. Processed ${totalProcessed.toLocaleString()}${remoteTotal ? ` of ${remoteTotal}` : ''} remote products; ${totalInserted.toLocaleString()} new, ${totalUpdated.toLocaleString()} updated, ${totalUnchanged.toLocaleString()} unchanged.`,
        );
        nextPage = res.nextPage;
      }

      alert(
        `Finished pulling "${channelName}". Processed ${totalProcessed.toLocaleString()} remote product${totalProcessed === 1 ? '' : 's'}: ${totalInserted.toLocaleString()} new, ${totalUpdated.toLocaleString()} updated, ${totalUnchanged.toLocaleString()} unchanged.`,
      );
    } catch (err: any) {
      setPullError(getErrorMessage(err));
    } finally {
      setPullProgress(null);
      setPulling(false);
    }
  };

  const handleEditClick = (product: Product) => {
    setEditingProduct(product);
    setEditForm({
      name: product.name,
      description: product.description,
      brand: product.brand,
      status: product.status,
      is_visible: product.is_visible,
      default_price: product.default_price
    });
  };

  const handleSaveEdit = async () => {
    if (!editingProduct) return;
    setSaving(true);
    try {
      await updateProduct({
        id: editingProduct.id,
        name: editForm.name || '',
        description: editForm.description,
        brand: editForm.brand,
        status: editForm.status || 'inactive',
        is_visible: editForm.is_visible || 0,
        default_price: editForm.default_price || 0
      });
      setEditingProduct(null);
    } catch (err: any) {
      alert(err.message);
    } finally {
      setSaving(false);
    }
  };

  const handleDeleteClick = async (product: Product) => {
    setDeletingProduct(product);
    setDeleteRedirectUrl('');
    setIsFetchingCategoryUrl(true);
    
    try {
      const res = await getCategoryUrl({ id: product.id });
      if (res && res.url) {
        setDeleteRedirectUrl(res.url);
      }
    } catch (err) {
      console.error('Failed to fetch category URL:', err);
    } finally {
      setIsFetchingCategoryUrl(false);
    }
  };

  const handleDeleteProduct = async () => {
    if (!deletingProduct) return;
    setIsDeleting(true);
    try {
      await deleteProduct({
        id: deletingProduct.id,
        redirect_url: deleteRedirectUrl || undefined
      });
      setDeletingProduct(null);
      setDeleteRedirectUrl('');
    } catch (err: any) {
      alert(err.message);
    } finally {
      setIsDeleting(false);
    }
  };

  const filteredProducts = products.filter(product => {
    const query = searchQuery.toLowerCase();
    const matchName = product.name.toLowerCase().includes(query);
    const matchId = product.id.toLowerCase().includes(query);
    const matchSku = product.variants?.some(v => v.sku.toLowerCase().includes(query));
    return matchName || matchId || matchSku;
  });

  return (
    <motion.div 
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      className="max-w-7xl mx-auto space-y-6"
    >
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div className="bg-white p-5 rounded-xl shadow-sm border border-zinc-200 flex flex-col">
          <span className="text-sm font-medium text-zinc-500 mb-1">Total Products</span>
          <span className="text-3xl font-semibold text-zinc-900">{stats === undefined ? '-' : stats.totalProducts.toLocaleString()}</span>
        </div>
        <div className="bg-white p-5 rounded-xl shadow-sm border border-zinc-200 flex flex-col">
          <span className="text-sm font-medium text-zinc-500 mb-1">Active Products</span>
          <span className="text-3xl font-semibold text-zinc-900">{stats === undefined ? '-' : stats.activeProducts.toLocaleString()}</span>
        </div>
        <div className="bg-white p-5 rounded-xl shadow-sm border border-zinc-200 flex flex-col">
          <span className="text-sm font-medium text-zinc-500 mb-1">Visible on Storefront</span>
          <span className="text-3xl font-semibold text-zinc-900">{stats === undefined ? '-' : stats.visibleProducts.toLocaleString()}</span>
        </div>
      </div>

      <div className="flex items-center justify-between">
        <div className="relative w-96">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-zinc-400" />
          <input 
            type="text" 
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            placeholder="Search loaded products by name, ID, or SKU..." 
            className="w-full pl-10 pr-4 py-2 bg-white border border-zinc-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-emerald-500/20 focus:border-emerald-500"
          />
        </div>
        <div className="flex items-center gap-3">
          {pullError && <span className="text-xs text-red-500 font-medium">{pullError}</span>}
          {pullProgress && !pullError && <span className="text-xs text-zinc-500 font-medium">{pullProgress}</span>}
          <button 
            onClick={handlePullFromBigCommerce}
            disabled={pulling}
            className="flex items-center gap-2 px-4 py-2 bg-zinc-900 text-white rounded-lg text-sm font-medium hover:bg-zinc-800 disabled:opacity-50 transition-colors"
          >
            {pulling ? <Loader2 className="w-4 h-4 animate-spin" /> : <DownloadCloud className="w-4 h-4" />}
            {pulling ? 'Pulling Bonsai Outlet...' : 'Pull Bonsai Outlet from BigCommerce'}
          </button>
          <button className="flex items-center gap-2 px-4 py-2 bg-white border border-zinc-300 rounded-lg text-sm font-medium text-zinc-700 hover:bg-zinc-50">
            <Filter className="w-4 h-4" />
            Filters
          </button>
        </div>
      </div>

      <div className="bg-white rounded-xl shadow-sm border border-zinc-200 overflow-hidden">
        <div className="px-6 py-4 border-b border-zinc-200 bg-zinc-50 flex justify-between items-center">
          <h3 className="font-medium text-zinc-900">Products</h3>
          <span className="text-sm text-zinc-500 font-medium">
            {productsStatus === 'LoadingFirstPage' ? 'Loading catalog...' : (
              <>
                {filteredProducts.length.toLocaleString()} {filteredProducts.length === 1 ? 'loaded match' : 'loaded matches'}
                {searchQuery && ` (filtered from ${products.length.toLocaleString()} loaded)`}
                {!searchQuery && stats && ` of ${stats.totalProducts.toLocaleString()} total`}
              </>
            )}
          </span>
        </div>
        {searchQuery && productsStatus !== 'Exhausted' && (
          <div className="px-6 py-3 border-b border-zinc-200 bg-amber-50 text-amber-800 text-xs font-medium">
            Search currently filters the loaded rows only. Load more products to widen the results.
          </div>
        )}
        <div className="overflow-x-auto">
          <table className="w-full text-left text-sm">
            <thead className="bg-zinc-50 border-b border-zinc-200 text-zinc-500">
              <tr>
                <th className="px-6 py-3 font-medium">Internal ID</th>
                <th className="px-6 py-3 font-medium">Product Name</th>
                <th className="px-6 py-3 font-medium">Status</th>
                <th className="px-6 py-3 font-medium">Variants</th>
                <th className="px-6 py-3 font-medium">Sync Status</th>
                <th className="px-6 py-3 font-medium">Last Updated</th>
                <th className="px-6 py-3 font-medium text-right">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-zinc-200">
              {productsStatus === 'LoadingFirstPage' ? (
                <tr><td colSpan={7} className="px-6 py-8 text-center text-zinc-500">Loading catalog...</td></tr>
              ) : filteredProducts.length === 0 ? (
                <tr><td colSpan={7} className="px-6 py-8 text-center text-zinc-500">No products found.</td></tr>
              ) : (
                filteredProducts.map(product => (
                  <tr key={product.id} className="hover:bg-zinc-50/50 transition-colors">
                    <td className="px-6 py-4 font-mono text-xs text-zinc-500">{product.id.split('-')[0]}...</td>
                    <td className="px-6 py-4 font-medium text-zinc-900">{product.name}</td>
                    <td className="px-6 py-4">
                      <span className={`inline-flex items-center px-2 py-1 rounded-md text-xs font-medium ${
                        product.status === 'active' ? 'bg-emerald-100 text-emerald-700' : 'bg-zinc-100 text-zinc-700'
                      }`}>
                        {product.status}
                      </span>
                    </td>
                    <td className="px-6 py-4 text-zinc-600">{product.variants?.length || 0} SKUs</td>
                    <td className="px-6 py-4">
                      {product.sync_needed ? (
                        <span className="inline-flex items-center gap-1.5 text-amber-600 text-xs font-medium">
                          <span className="w-1.5 h-1.5 rounded-full bg-amber-500"></span>
                          Pending Sync
                        </span>
                      ) : (
                        <span className="inline-flex items-center gap-1.5 text-emerald-600 text-xs font-medium">
                          <span className="w-1.5 h-1.5 rounded-full bg-emerald-500"></span>
                          Synced
                        </span>
                      )}
                    </td>
                    <td className="px-6 py-4 text-zinc-500">{new Date(product.updated_at).toLocaleString()}</td>
                    <td className="px-6 py-4 text-right">
                      <div className="flex items-center justify-end gap-1">
                        <button 
                          onClick={() => handleEditClick(product)}
                          className="p-2 text-zinc-400 hover:text-emerald-600 hover:bg-emerald-50 rounded-lg transition-colors"
                          title="Edit Product"
                        >
                          <Edit2 className="w-4 h-4" />
                        </button>
                        <button 
                          onClick={() => handleDeleteClick(product)}
                          className="p-2 text-zinc-400 hover:text-red-600 hover:bg-red-50 rounded-lg transition-colors"
                          title="Delete Product"
                        >
                          <Trash2 className="w-4 h-4" />
                        </button>
                      </div>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
        <div className="px-6 py-4 border-t border-zinc-200 bg-zinc-50 flex items-center justify-between gap-4">
          <span className="text-sm text-zinc-500">
            Loaded {products.length.toLocaleString()}
            {stats && ` of ${stats.totalProducts.toLocaleString()} products`}
          </span>
          <button
            onClick={() => loadMore(50)}
            disabled={productsStatus !== 'CanLoadMore'}
            className="flex items-center gap-2 px-4 py-2 bg-white border border-zinc-300 rounded-lg text-sm font-medium text-zinc-700 hover:bg-zinc-100 disabled:opacity-50 disabled:hover:bg-white"
          >
            {productsStatus === 'LoadingMore' && <Loader2 className="w-4 h-4 animate-spin" />}
            {productsStatus === 'Exhausted' ? 'All Products Loaded' : productsStatus === 'LoadingMore' ? 'Loading More...' : 'Load More'}
          </button>
        </div>
      </div>

      {/* Edit Modal */}
      {editingProduct && (
        <div className="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4">
          <motion.div 
            initial={{ opacity: 0, scale: 0.95 }}
            animate={{ opacity: 1, scale: 1 }}
            className="bg-white rounded-xl shadow-xl max-w-lg w-full overflow-hidden"
          >
            <div className="px-6 py-4 border-b border-zinc-200 flex justify-between items-center bg-zinc-50">
              <h3 className="font-medium text-zinc-900">Edit Product</h3>
              <button 
                onClick={() => setEditingProduct(null)}
                className="text-zinc-400 hover:text-zinc-600"
              >
                <X className="w-5 h-5" />
              </button>
            </div>
            <div className="p-6 space-y-4">
              <div>
                <label className="block text-sm font-medium text-zinc-700 mb-1">Name</label>
                <input 
                  type="text" 
                  value={editForm.name || ''}
                  onChange={e => setEditForm({...editForm, name: e.target.value})}
                  className="w-full px-3 py-2 border border-zinc-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-emerald-500/20 focus:border-emerald-500"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-zinc-700 mb-1">Description</label>
                <textarea 
                  value={editForm.description || ''}
                  onChange={e => setEditForm({...editForm, description: e.target.value})}
                  rows={3}
                  className="w-full px-3 py-2 border border-zinc-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-emerald-500/20 focus:border-emerald-500"
                />
              </div>
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-zinc-700 mb-1">Brand</label>
                  <input 
                    type="text" 
                    value={editForm.brand || ''}
                    onChange={e => setEditForm({...editForm, brand: e.target.value})}
                    className="w-full px-3 py-2 border border-zinc-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-emerald-500/20 focus:border-emerald-500"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-zinc-700 mb-1">Default Price</label>
                  <input 
                    type="number" 
                    step="0.01"
                    value={editForm.default_price || ''}
                    onChange={e => setEditForm({...editForm, default_price: parseFloat(e.target.value)})}
                    className="w-full px-3 py-2 border border-zinc-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-emerald-500/20 focus:border-emerald-500"
                  />
                </div>
              </div>
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-zinc-700 mb-1">Status</label>
                  <select 
                    value={editForm.status || 'active'}
                    onChange={e => setEditForm({...editForm, status: e.target.value})}
                    className="w-full px-3 py-2 border border-zinc-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-emerald-500/20 focus:border-emerald-500"
                  >
                    <option value="active">Active</option>
                    <option value="inactive">Inactive</option>
                  </select>
                </div>
                <div className="flex items-center pt-6">
                  <label className="flex items-center gap-2 cursor-pointer">
                    <input 
                      type="checkbox" 
                      checked={editForm.is_visible === 1}
                      onChange={e => setEditForm({...editForm, is_visible: e.target.checked ? 1 : 0})}
                      className="w-4 h-4 text-emerald-600 rounded border-zinc-300 focus:ring-emerald-500"
                    />
                    <span className="text-sm font-medium text-zinc-700">Visible on Storefront</span>
                  </label>
                </div>
              </div>
            </div>
            <div className="px-6 py-4 border-t border-zinc-200 bg-zinc-50 flex justify-end gap-3">
              <button 
                onClick={() => setEditingProduct(null)}
                className="px-4 py-2 text-sm font-medium text-zinc-700 hover:bg-zinc-200 rounded-lg transition-colors"
              >
                Cancel
              </button>
              <button 
                onClick={handleSaveEdit}
                disabled={saving}
                className="flex items-center gap-2 px-4 py-2 bg-emerald-600 text-white text-sm font-medium rounded-lg hover:bg-emerald-700 disabled:opacity-50 transition-colors"
              >
                {saving && <Loader2 className="w-4 h-4 animate-spin" />}
                Save Changes
              </button>
            </div>
          </motion.div>
        </div>
      )}

      {/* Delete Confirmation Modal */}
      {deletingProduct && (
        <div className="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4">
          <motion.div 
            initial={{ opacity: 0, scale: 0.95 }}
            animate={{ opacity: 1, scale: 1 }}
            className="bg-white rounded-xl shadow-xl max-w-md w-full overflow-hidden"
          >
            <div className="p-6 text-center">
              <div className="w-12 h-12 rounded-full bg-red-100 text-red-600 flex items-center justify-center mx-auto mb-4">
                <AlertTriangle className="w-6 h-6" />
              </div>
              <h3 className="text-lg font-medium text-zinc-900 mb-2">Delete Product</h3>
              <p className="text-sm text-zinc-500 mb-4">
                Are you sure you want to delete <strong>{deletingProduct.name}</strong>? This will queue a deletion sync to BigCommerce and cannot be undone.
              </p>
              <div className="text-left">
                <label className="block text-sm font-medium text-zinc-700 mb-1 flex items-center justify-between">
                  <span>Suggested Redirect URL (Optional)</span>
                  {isFetchingCategoryUrl && <Loader2 className="w-3 h-3 animate-spin text-zinc-400" />}
                </label>
                <input 
                  type="text" 
                  placeholder="/new-category/or-product"
                  value={deleteRedirectUrl}
                  onChange={e => setDeleteRedirectUrl(e.target.value)}
                  disabled={isFetchingCategoryUrl}
                  className="w-full px-3 py-2 border border-zinc-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-red-500/20 focus:border-red-500 text-sm disabled:bg-zinc-50 disabled:text-zinc-500"
                />
                <p className="text-xs text-zinc-500 mt-1">
                  If provided, the sync processor will create a BigCommerce storefront redirect after the product is deleted.
                </p>
              </div>
            </div>
            <div className="px-6 py-4 border-t border-zinc-200 bg-zinc-50 flex justify-end gap-3">
              <button 
                onClick={() => {
                  setDeletingProduct(null);
                  setDeleteRedirectUrl('');
                }}
                disabled={isDeleting}
                className="px-4 py-2 text-sm font-medium text-zinc-700 hover:bg-zinc-200 rounded-lg transition-colors"
              >
                Cancel
              </button>
              <button 
                onClick={handleDeleteProduct}
                disabled={isDeleting}
                className="flex items-center gap-2 px-4 py-2 bg-red-600 text-white text-sm font-medium rounded-lg hover:bg-red-700 disabled:opacity-50 transition-colors"
              >
                {isDeleting && <Loader2 className="w-4 h-4 animate-spin" />}
                Delete Product
              </button>
            </div>
          </motion.div>
        </div>
      )}
    </motion.div>
  );
}
