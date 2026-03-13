import { query, mutation } from "./_generated/server";
import type { Doc, Id } from "./_generated/dataModel";
import { paginationOptsValidator } from "convex/server";
import { v } from "convex/values";

type ProductRecord = Doc<"products">;
type VariantRecord = Doc<"variants">;

async function countQuery(query: { collect: () => Promise<unknown[]> }): Promise<number> {
  const queryWithCount = query as { count?: () => Promise<number> };
  if (typeof queryWithCount.count === "function") {
    return await queryWithCount.count();
  }

  return (await query.collect()).length;
}

function toFrontendProduct(product: ProductRecord, variants: VariantRecord[]) {
  return {
    ...product,
    id: product.external_product_id || product._id,
    variants,
  };
}

async function getVariantsForProduct(ctx: any, product: ProductRecord): Promise<VariantRecord[]> {
  const identifiers = [...new Set([product._id, product.external_product_id].filter((value): value is string => !!value))];
  const groups = await Promise.all(
    identifiers.map((identifier) =>
      ctx.db.query("variants").withIndex("by_product", (q: any) => q.eq("product_id", identifier)).collect(),
    ),
  );

  return [...new Map(groups.flat().map((variant) => [variant._id, variant])).values()];
}

export const getDashboardStats = query({
  args: {},
  handler: async (ctx) => {
    const [
      totalProducts,
      totalVariants,
      activeProducts,
      visibleProducts,
      pendingSyncs,
      failedSyncs,
      deadSyncs,
    ] = await Promise.all([
      countQuery(ctx.db.query("products")),
      countQuery(ctx.db.query("variants")),
      countQuery(ctx.db.query("products").withIndex("by_status", (q) => q.eq("status", "active"))),
      countQuery(ctx.db.query("products").withIndex("by_visibility", (q) => q.eq("is_visible", 1))),
      countQuery(ctx.db.query("sync_queue").withIndex("by_status", (q) => q.eq("status", "pending"))),
      countQuery(ctx.db.query("sync_queue").withIndex("by_status", (q) => q.eq("status", "failed"))),
      countQuery(ctx.db.query("sync_queue").withIndex("by_status", (q) => q.eq("status", "dead"))),
    ]);

    return {
      totalProducts,
      totalVariants,
      activeProducts,
      visibleProducts,
      pendingSyncs,
      failedSyncs: failedSyncs + deadSyncs,
    };
  },
});

export const getProducts = query({
  args: {
    paginationOpts: paginationOptsValidator,
  },
  handler: async (ctx, args) => {
    const result = await ctx.db
      .query("products")
      .withIndex("by_updated_at")
      .order("desc")
      .paginate(args.paginationOpts);

    const page = await Promise.all(
      result.page.map(async (product) => toFrontendProduct(product, await getVariantsForProduct(ctx, product))),
    );

    return {
      ...result,
      page,
    };
  },
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

    let product = await ctx.db.query("products").withIndex("by_external_id", (q) => q.eq("external_product_id", id)).first();
    if (!product) {
      try {
        product = await ctx.db.get(id as Id<"products">);
      } catch {
        // Ignore invalid ID error.
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
      name,
      description,
      brand,
      status,
      is_visible,
      default_price,
      sync_needed: 1,
      updated_at: new Date().toISOString(),
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
        updated_at: new Date().toISOString(),
      });
    }

    return { success: true };
  },
});

export const deleteProduct = mutation({
  args: {
    id: v.string(),
    redirect_url: v.optional(v.string()),
  },
  handler: async (ctx, args) => {
    const { id, redirect_url } = args;

    let product = await ctx.db.query("products").withIndex("by_external_id", (q) => q.eq("external_product_id", id)).first();
    if (!product) {
      try {
        product = await ctx.db.get(id as Id<"products">);
      } catch {
        // Ignore invalid ID error.
      }
    }

    if (!product) {
      throw new Error("Product not found");
    }

    const productIdentifiers = [...new Set([id, product._id, product.external_product_id].filter((value): value is string => !!value))];
    const variantGroups = await Promise.all(
      productIdentifiers.map((identifier) =>
        ctx.db.query("variants").withIndex("by_product", (q) => q.eq("product_id", identifier)).collect(),
      ),
    );

    for (const variant of [...new Map(variantGroups.flat().map((entry) => [entry._id, entry])).values()]) {
      await ctx.db.delete(variant._id);
    }

    await ctx.db.delete(product._id);

    await ctx.db.insert("sync_queue", {
      entity_type: "product",
      internal_id: id,
      action: "delete",
      status: "pending",
      attempts: 0,
      payload: JSON.stringify({
        external_product_id: product.external_product_id,
        redirect_url: redirect_url || null,
      }),
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    });

    return { success: true };
  },
});
