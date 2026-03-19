export interface Product {
  id: string;
  external_product_id?: string | null;
  name: string;
  description?: string | null;
  brand?: string | null;
  status: string;
  is_visible: number;
  default_price: number | null;
  cost_price?: number | null;
  retail_price?: number | null;
  sale_price?: number | null;
  weight?: number | null;
  width?: number | null;
  height?: number | null;
  depth?: number | null;
  page_title?: string | null;
  meta_keywords?: string | null;
  meta_description?: string | null;
  sort_order?: number | null;
  upc?: string | null;
  mpn?: string | null;
  search_keywords?: string | null;
  condition?: string | null;
  is_condition_shown?: number | null;
  allow_purchases?: number | null;
  availability?: string | null;
  availability_description?: string | null;
  inventory_warning_level?: number | null;
  category_string?: string | null;
  category_ids?: number[] | null;
  warranty?: string | null;
  is_free_shipping?: number | null;
  fixed_cost_shipping_price?: number | null;
  order_quantity_minimum?: number | null;
  order_quantity_maximum?: number | null;
  custom_fields?: Array<{ name: string; value: string }> | null;
  images?: Array<{
    image_id?: number | null;
    image_url: string;
    description?: string | null;
    is_thumbnail?: boolean | null;
    sort_order?: number | null;
  }> | null;
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
  changed_row_count?: number | null;
  unchanged_row_count?: number | null;
  sync_jobs_created_count?: number | null;
  product_jobs_created_count?: number | null;
  variant_jobs_created_count?: number | null;
  errors?: string | null;
  created_at: string;
}

export interface DashboardStats {
  totalProducts: number;
  totalVariants: number;
  activeProducts: number | null;
  visibleProducts: number | null;
  pendingSyncs: number | null;
  failedSyncs: number;
}
