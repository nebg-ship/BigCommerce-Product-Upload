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
    version: v.optional(v.number()),
    sync_needed: v.number(),
    updated_at: v.string(),
    created_at: v.string(),
  }).index("by_external_id", ["external_product_id"]),

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
