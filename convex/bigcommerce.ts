import { action, mutation, internalQuery, internalMutation } from "./_generated/server";
import type { Id } from "./_generated/dataModel";
import { v } from "convex/values";
import { internal } from "./_generated/api";

const DEFAULT_PULL_CHANNEL_NAME = "Bonsai Outlet";
const BIGCOMMERCE_PAGE_SIZE = 50;
const PAGES_PER_PULL_CALL = 5;

type BigCommerceCredentials = {
  accessToken: string;
  storeHash: string;
};

type BigCommerceChannel = {
  id: number;
  name: string;
};

type BigCommerceChannelsResponse = {
  data?: BigCommerceChannel[];
};

type BigCommerceProduct = {
  id: number;
  name: string;
  description?: string;
  brand_id?: number | null;
  is_visible?: boolean;
  price?: number;
  variants?: Array<{
    inventory_level?: number | null;
    price?: number | null;
    sku: string;
  }>;
};

type BigCommerceProductsResponse = {
  data?: BigCommerceProduct[];
  meta?: {
    pagination?: {
      current_page?: number;
      total?: number;
      total_pages?: number;
    };
  };
};

function getBigCommerceCredentials(): BigCommerceCredentials {
  const storeHash = process.env.BIGCOMMERCE_STORE_HASH;
  const accessToken = process.env.BIGCOMMERCE_ACCESS_TOKEN;

  if (!storeHash || !accessToken) {
    throw new Error('BigCommerce credentials not configured in environment variables.');
  }

  return { storeHash, accessToken };
}

async function readBigCommerceError(response: Response): Promise<string> {
  const text = await response.text();
  if (!text) {
    return response.statusText;
  }

  try {
    const parsed = JSON.parse(text);
    if (typeof parsed?.title === "string") {
      return parsed.title;
    }
    if (typeof parsed?.message === "string") {
      return parsed.message;
    }
  } catch {
    // Fall back to the raw response body.
  }

  return text;
}

async function bigCommerceGet<T>(credentials: BigCommerceCredentials, path: string, params?: Record<string, string>): Promise<T> {
  const searchParams = new URLSearchParams(params);
  const query = searchParams.toString();
  const url = `https://api.bigcommerce.com/stores/${credentials.storeHash}${path}${query ? `?${query}` : ''}`;
  const response = await fetch(url, {
    headers: {
      'X-Auth-Token': credentials.accessToken,
      'Accept': 'application/json'
    }
  });

  if (!response.ok) {
    const message = await readBigCommerceError(response);
    throw new Error(`BigCommerce API error (${response.status}): ${message}`);
  }

  return await response.json() as T;
}

async function getChannelByName(credentials: BigCommerceCredentials, channelName: string): Promise<BigCommerceChannel> {
  const response = await bigCommerceGet<BigCommerceChannelsResponse>(credentials, "/v3/channels", {
    limit: String(BIGCOMMERCE_PAGE_SIZE),
  });
  const channels = response.data || [];
  const matchedChannel = channels.find((channel) => channel.name.trim().toLowerCase() === channelName.trim().toLowerCase());

  if (!matchedChannel) {
    const availableChannels = channels.map((channel) => channel.name).sort();
    const availableLabel = availableChannels.length > 0
      ? ` Available channels: ${availableChannels.join(", ")}.`
      : "";
    throw new Error(`BigCommerce channel "${channelName}" was not found.${availableLabel}`);
  }

  return matchedChannel;
}

function getConfiguredPullChannelId(): string | null {
  const configuredId = process.env.BIGCOMMERCE_PULL_CHANNEL_ID?.trim();
  if (!configuredId) {
    return null;
  }

  if (!/^\d+$/.test(configuredId)) {
    throw new Error(`BIGCOMMERCE_PULL_CHANNEL_ID must be a numeric channel ID. Received "${configuredId}".`);
  }

  return configuredId;
}

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
  args: {
    page: v.optional(v.number()),
  },
  handler: async (ctx, args) => {
    const credentials = getBigCommerceCredentials();
    const channelName = process.env.BIGCOMMERCE_PULL_CHANNEL_NAME?.trim() || DEFAULT_PULL_CHANNEL_NAME;
    const configuredChannelId = getConfiguredPullChannelId();

    try {
      let channelId = configuredChannelId;
      let channelLabel = configuredChannelId ? `${channelName} (#${configuredChannelId})` : channelName;

      if (!channelId) {
        try {
          const channel = await getChannelByName(credentials, channelName);
          channelId = String(channel.id);
          channelLabel = channel.name;
        } catch (err: any) {
          if (String(err?.message || "").includes("403")) {
            throw new Error(
              `The current BigCommerce token cannot read channels. Set BIGCOMMERCE_PULL_CHANNEL_ID for "${channelName}" or grant channel read scope.`,
            );
          }
          throw err;
        }
      }

      let currentPage = Math.max(args.page ?? 1, 1);
      let totalPages: number | null = null;
      let totalCount = 0;
      let processedPages = 0;
      let pulledCount = 0;

      while (processedPages < PAGES_PER_PULL_CALL) {
        const response = await bigCommerceGet<BigCommerceProductsResponse>(credentials, "/v3/catalog/products", {
          "channel_id:in": channelId,
          include: "variants",
          limit: String(BIGCOMMERCE_PAGE_SIZE),
          page: String(currentPage),
        });

        const products = response.data || [];
        totalPages = response.meta?.pagination?.total_pages || currentPage;
        totalCount = response.meta?.pagination?.total || 0;

        await ctx.runMutation(internal.bigcommerce.savePulledProducts, {
          products: JSON.stringify(products)
        });

        pulledCount += products.length;
        processedPages += 1;
        if (currentPage >= totalPages) {
          currentPage += 1;
          break;
        }
        currentPage += 1;
      }

      return {
        success: true,
        channelName: channelLabel,
        count: pulledCount,
        currentPage: Math.max(args.page ?? 1, 1),
        lastProcessedPage: currentPage - 1,
        nextPage: totalPages !== null && currentPage <= totalPages ? currentPage : null,
        processedPages,
        totalCount,
        totalPages: totalPages ?? 0,
      };
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
        product = await ctx.db.get(args.id as Id<"products">);
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
