export interface Product {
  id: string;
  external_product_id?: string | null;
  name: string;
  description?: string | null;
  brand?: string | null;
  status: string;
  is_visible: number;
  default_price: number | null;
  version?: number;
  sync_needed: number;
  updated_at: string;
  created_at: string;
  variants?: Variant[];
}

export interface Variant {
  id?: string;
  _id?: string;
  product_id: string;
  sku: string;
  price: number;
  inventory_level: number;
  sync_needed: number;
  updated_at: string;
  created_at: string;
}

export interface SyncJob {
  id: string;
  entity_type: string;
  internal_id: string;
  action: string;
  status: 'pending' | 'processing' | 'success' | 'failed' | 'dead';
  attempts: number;
  error_message: string | null;
  payload: string | null;
  created_at: string;
  updated_at: string;
}

export interface ImportRun {
  id: string;
  file_name: string;
  status: string;
  row_count: number;
  valid_row_count: number;
  invalid_row_count: number;
  errors?: string | null;
  created_at: string;
}

export interface DashboardStats {
  totalProducts: number;
  totalVariants: number;
  pendingSyncs: number;
  failedSyncs: number;
}
