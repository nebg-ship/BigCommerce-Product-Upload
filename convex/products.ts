import { query, mutation } from "./_generated/server";
import type { Id } from "./_generated/dataModel";
import { v } from "convex/values";

export const getDashboardStats = query({
  args: {},
  handler: async (ctx) => {
    const products = await ctx.db.query("products").collect();
    const variants = await ctx.db.query("variants").collect();
    const pendingSyncs = await ctx.db.query("sync_queue").withIndex("by_status", q => q.eq("status", "pending")).collect();
    const failedSyncs = await ctx.db.query("sync_queue").withIndex("by_status", q => q.eq("status", "failed")).collect();
    const deadSyncs = await ctx.db.query("sync_queue").withIndex("by_status", q => q.eq("status", "dead")).collect();
    
    return {
      totalProducts: products.length,
      totalVariants: variants.length,
      pendingSyncs: pendingSyncs.length,
      failedSyncs: failedSyncs.length + deadSyncs.length
    };
  }
});

export const getProducts = query({
  args: {},
  handler: async (ctx) => {
    const products = await ctx.db.query("products").order("desc").collect();
    const variants = await ctx.db.query("variants").collect();
    
    return products.map(p => ({
      ...p,
      id: p.external_product_id || p._id, // Map for frontend compatibility
      variants: variants.filter(v => v.product_id === p.external_product_id || v.product_id === p._id)
    }));
  }
});

export const updateProduct = mutation({
  args: {
    id: v.string(), // This is the external_product_id or _id
    name: v.string(),
    description: v.optional(v.string()),
    brand: v.optional(v.string()),
    status: v.string(),
    is_visible: v.number(),
    default_price: v.number(),
  },
  handler: async (ctx, args) => {
    const { id, name, description, brand, status, is_visible, default_price } = args;
    
    // Find product by external_id or _id
    let product = await ctx.db.query("products").withIndex("by_external_id", q => q.eq("external_product_id", id)).first();
    if (!product) {
      // Try by _id if it's a valid Convex ID
      try {
        product = await ctx.db.get(id as Id<"products">);
      } catch (e) {
        // Ignore invalid ID error
      }
    }
    
    if (!product) {
      throw new Error("Product not found");
    }

    const productChanges: Record<string, any> = {};
    if (product.name !== name) productChanges.name = { old: product.name, new: name };
    if (product.description !== description) productChanges.description = { old: product.description, new: description };
    if (product.brand !== brand) productChanges.brand = { old: product.brand, new: brand };
    if (product.status !== status) productChanges.status = { old: product.status, new: status };
    if (product.is_visible !== is_visible) productChanges.is_visible = { old: product.is_visible, new: is_visible };
    if (product.default_price !== default_price) productChanges.default_price = { old: product.default_price, new: default_price };

    await ctx.db.patch(product._id, {
      name, description, brand, status, is_visible, default_price,
      sync_needed: 1,
      updated_at: new Date().toISOString()
    });

    if (Object.keys(productChanges).length > 0) {
      await ctx.db.insert("sync_queue", {
        entity_type: "product",
        internal_id: id,
        action: "update",
        status: "pending",
        attempts: 0,
        payload: JSON.stringify(productChanges),
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString()
      });
    }

    return { success: true };
  }
});

export const deleteProduct = mutation({
  args: {
    id: v.string(),
    redirect_url: v.optional(v.string())
  },
  handler: async (ctx, args) => {
    const { id, redirect_url } = args;
    
    let product = await ctx.db.query("products").withIndex("by_external_id", q => q.eq("external_product_id", id)).first();
    if (!product) {
      try {
        product = await ctx.db.get(id as Id<"products">);
      } catch (e) {}
    }
    
    if (!product) {
      throw new Error("Product not found");
    }

    // Delete variants
    const variants = await ctx.db.query("variants").withIndex("by_product", q => q.eq("product_id", id)).collect();
    for (const v of variants) {
      await ctx.db.delete(v._id);
    }

    // Delete product
    await ctx.db.delete(product._id);

    // Queue sync
    await ctx.db.insert("sync_queue", {
      entity_type: "product",
      internal_id: id,
      action: "delete",
      status: "pending",
      attempts: 0,
      payload: JSON.stringify({
        external_product_id: product.external_product_id,
        redirect_url: redirect_url || null
      }),
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString()
    });

    return { success: true };
  }
});
