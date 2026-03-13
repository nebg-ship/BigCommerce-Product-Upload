import { action, mutation, internalQuery, internalMutation } from "./_generated/server";
import { v } from "convex/values";
import { internal } from "./_generated/api";

export const getCategoryUrl = action({
  args: { id: v.string() },
  handler: async (ctx, args) => {
    const storeHash = process.env.BIGCOMMERCE_STORE_HASH;
    const accessToken = process.env.BIGCOMMERCE_ACCESS_TOKEN;

    if (!storeHash || !accessToken) {
      return { url: '' };
    }

    try {
      // We need to fetch the product from the DB to get the external_product_id
      const product = await ctx.runQuery(internal.bigcommerce.getProductExternalId, { id: args.id });
      if (!product || !product.external_product_id) {
        return { url: '' };
      }

      const prodRes = await fetch(`https://api.bigcommerce.com/stores/${storeHash}/v3/catalog/products/${product.external_product_id}?include_fields=categories`, {
        headers: {
          'X-Auth-Token': accessToken,
          'Accept': 'application/json'
        }
      });
      
      if (!prodRes.ok) return { url: '' };
      const prodData = await prodRes.json();
      const categories = prodData.data?.categories;

      if (categories && categories.length > 0) {
        const catRes = await fetch(`https://api.bigcommerce.com/stores/${storeHash}/v3/catalog/categories/${categories[0]}`, {
          headers: {
            'X-Auth-Token': accessToken,
            'Accept': 'application/json'
          }
        });
        
        if (catRes.ok) {
          const catData = await catRes.json();
          if (catData.data?.custom_url?.url) {
            return { url: catData.data.custom_url.url };
          }
        }
      }
      return { url: '' };
    } catch (err) {
      console.error('Fetch Category URL Error:', err);
      return { url: '' };
    }
  }
});

export const pullFromBigCommerce = action({
  args: {},
  handler: async (ctx) => {
    const storeHash = process.env.BIGCOMMERCE_STORE_HASH;
    const accessToken = process.env.BIGCOMMERCE_ACCESS_TOKEN;

    if (!storeHash || !accessToken) {
      throw new Error('BigCommerce credentials not configured in environment variables.');
    }

    try {
      const response = await fetch(`https://api.bigcommerce.com/stores/${storeHash}/v3/catalog/products?include=variants`, {
        headers: {
          'X-Auth-Token': accessToken,
          'Accept': 'application/json'
        }
      });

      if (!response.ok) {
        throw new Error(`BigCommerce API error: ${response.statusText}`);
      }

      const data = await response.json();
      const products = data.data;

      await ctx.runMutation(internal.bigcommerce.savePulledProducts, {
        products: JSON.stringify(products)
      });

      return { success: true, count: products.length };
    } catch (err: any) {
      console.error('BigCommerce Pull Error:', err);
      throw new Error(err.message);
    }
  }
});

export const getProductExternalId = internalQuery({
  args: { id: v.string() },
  handler: async (ctx, args) => {
    let product = await ctx.db.query("products").withIndex("by_external_id", q => q.eq("external_product_id", args.id)).first();
    if (!product) {
      try {
        product = await ctx.db.get(args.id as any);
      } catch (e) {}
    }
    return product;
  }
});

export const savePulledProducts = internalMutation({
  args: { products: v.string() },
  handler: async (ctx, args) => {
    const products = JSON.parse(args.products);

    for (const p of products) {
      const externalId = p.id.toString();
      const name = p.name;
      const description = p.description;
      const brand = p.brand_id ? p.brand_id.toString() : undefined;
      const isVisible = p.is_visible ? 1 : 0;
      const status = p.is_visible ? 'active' : 'inactive';
      const defaultPrice = p.price;

      let product = await ctx.db.query("products").withIndex("by_external_id", q => q.eq("external_product_id", externalId)).first();
      if (product) {
        await ctx.db.patch(product._id, {
          name, description, brand, status, is_visible: isVisible, default_price: defaultPrice,
          sync_needed: 0, updated_at: new Date().toISOString()
        });
      } else {
        await ctx.db.insert("products", {
          external_product_id: externalId,
          name, description, brand, status, is_visible: isVisible, default_price: defaultPrice,
          sync_needed: 0, created_at: new Date().toISOString(), updated_at: new Date().toISOString()
        });
      }

      if (p.variants) {
        for (const v of p.variants) {
          const sku = v.sku;
          const price = v.price || p.price;
          const inventoryLevel = v.inventory_level || 0;

          let variant = await ctx.db.query("variants").withIndex("by_sku", q => q.eq("sku", sku)).first();
          if (variant) {
            await ctx.db.patch(variant._id, {
              price, inventory_level: inventoryLevel, sync_needed: 0, updated_at: new Date().toISOString()
            });
          } else {
            await ctx.db.insert("variants", {
              product_id: externalId,
              sku, price, inventory_level: inventoryLevel, sync_needed: 0,
              created_at: new Date().toISOString(), updated_at: new Date().toISOString()
            });
          }
        }
      }
    }
  }
});
