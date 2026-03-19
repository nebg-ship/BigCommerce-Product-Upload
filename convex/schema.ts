import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";

export default defineSchema({
  products: defineTable({
    external_product_id: v.optional(v.string()),
    name: v.string(),
    description: v.optional(v.string()),
    brand: v.optional(v.string()),
    status: v.string(),
    is_visible: v.number(),
    default_price: v.number(),
    cost_price: v.optional(v.number()),
    retail_price: v.optional(v.number()),
    sale_price: v.optional(v.number()),
    weight: v.optional(v.number()),
    width: v.optional(v.number()),
    height: v.optional(v.number()),
    depth: v.optional(v.number()),
    page_title: v.optional(v.string()),
    meta_keywords: v.optional(v.string()),
    meta_description: v.optional(v.string()),
    sort_order: v.optional(v.number()),
    upc: v.optional(v.string()),
    mpn: v.optional(v.string()),
    search_keywords: v.optional(v.string()),
    condition: v.optional(v.string()),
    is_condition_shown: v.optional(v.number()),
    allow_purchases: v.optional(v.number()),
    availability: v.optional(v.string()),
    availability_description: v.optional(v.string()),
    inventory_warning_level: v.optional(v.number()),
    category_string: v.optional(v.string()),
    category_ids: v.optional(v.array(v.number())),
    warranty: v.optional(v.string()),
    is_free_shipping: v.optional(v.number()),
    fixed_cost_shipping_price: v.optional(v.number()),
    order_quantity_minimum: v.optional(v.number()),
    order_quantity_maximum: v.optional(v.number()),
    custom_fields: v.optional(v.array(v.object({
      name: v.string(),
      value: v.string(),
    }))),
    images: v.optional(v.array(v.object({
      image_url: v.string(),
      description: v.optional(v.string()),
      is_thumbnail: v.optional(v.boolean()),
      sort_order: v.optional(v.number()),
    }))),
    version: v.optional(v.number()),
    sync_needed: v.number(),
    updated_at: v.string(),
    created_at: v.string(),
  })
    .index("by_external_id", ["external_product_id"])
    .index("by_status", ["status"])
    .index("by_visibility", ["is_visible"])
    .index("by_updated_at", ["updated_at"]),

  variants: defineTable({
    product_id: v.string(), // Reference to products table
    sku: v.string(),
    price: v.number(),
    inventory_level: v.number(),
    sync_needed: v.number(),
    updated_at: v.string(),
    created_at: v.string(),
  }).index("by_product", ["product_id"]).index("by_sku", ["sku"]),

  csv_import_runs: defineTable({
    file_name: v.string(),
    status: v.string(),
    row_count: v.number(),
    valid_row_count: v.number(),
    invalid_row_count: v.number(),
    errors: v.optional(v.string()),
    created_at: v.string(),
  }),

  sync_queue: defineTable({
    entity_type: v.string(),
    internal_id: v.string(),
    action: v.string(),
    status: v.string(),
    attempts: v.number(),
    error_message: v.optional(v.string()),
    payload: v.optional(v.string()),
    created_at: v.string(),
    updated_at: v.string(),
  }).index("by_status", ["status"]),
});
