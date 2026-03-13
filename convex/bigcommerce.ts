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

type BigCommerceCustomField = {
  name?: string | null;
  value?: string | null;
};

type BigCommerceProduct = {
  availability?: string | null;
  availability_description?: string | null;
  categories?: number[] | null;
  condition?: string | null;
  cost_price?: number | null;
  custom_fields?: BigCommerceCustomField[] | null;
  depth?: number | null;
  fixed_cost_shipping_price?: number | null;
  id: number;
  height?: number | null;
  inventory_warning_level?: number | null;
  is_condition_shown?: boolean | null;
  is_free_shipping?: boolean | null;
  meta_description?: string | null;
  meta_keywords?: string[] | null;
  mpn?: string | null;
  name: string;
  order_quantity_maximum?: number | null;
  order_quantity_minimum?: number | null;
  description?: string;
  brand_id?: number | null;
  is_visible?: boolean;
  page_title?: string | null;
  price?: number;
  retail_price?: number | null;
  sale_price?: number | null;
  search_keywords?: string | null;
  sort_order?: number | null;
  upc?: string | null;
  variants?: Array<{
    inventory_level?: number | null;
    price?: number | null;
    sku: string;
  }>;
  warranty?: string | null;
  weight?: number | null;
  width?: number | null;
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

function toOptionalNumber(value: unknown): number | undefined {
  if (value === null || value === undefined || value === "") {
    return undefined;
  }

  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function toOptionalString(value: unknown): string | undefined {
  if (value === null || value === undefined) {
    return undefined;
  }

  const trimmed = String(value).trim();
  return trimmed ? trimmed : undefined;
}

function toOptionalInteger(value: unknown): number | undefined {
  const parsed = toOptionalNumber(value);
  return parsed === undefined ? undefined : Math.trunc(parsed);
}

function toFlagNumber(value: unknown): number | undefined {
  if (value === null || value === undefined) {
    return undefined;
  }

  return value ? 1 : 0;
}

function toMetaKeywordsString(value: string[] | null | undefined): string | undefined {
  if (!Array.isArray(value) || value.length === 0) {
    return undefined;
  }

  const keywords = value.map((keyword) => keyword.trim()).filter(Boolean);
  return keywords.length > 0 ? keywords.join(", ") : undefined;
}

function normalizeProductCondition(value: unknown): string | undefined {
  const normalized = toOptionalString(value)?.toLowerCase();
  if (!normalized) {
    return undefined;
  }

  switch (normalized) {
    case "new":
      return "New";
    case "used":
      return "Used";
    case "refurbished":
      return "Refurbished";
    default:
      return toOptionalString(value);
  }
}

function toCategoryIds(value: number[] | null | undefined): number[] | undefined {
  if (!Array.isArray(value)) {
    return undefined;
  }

  return value
    .map((entry) => Number(entry))
    .filter((entry) => Number.isInteger(entry) && entry > 0);
}

function toCustomFields(value: BigCommerceCustomField[] | null | undefined) {
  if (!Array.isArray(value)) {
    return undefined;
  }

  const entries = new Map<string, string>();
  for (const field of value) {
    const name = toOptionalString(field?.name);
    if (!name) {
      continue;
    }

    entries.set(name, String(field?.value ?? "").trim());
  }

  return [...entries.entries()]
    .sort(([leftName], [rightName]) => leftName.localeCompare(rightName))
    .map(([name, fieldValue]) => ({ name, value: fieldValue }));
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
          include: "variants,custom_fields",
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
      const availability = toOptionalString(p.availability);
      const availabilityDescription = toOptionalString(p.availability_description);
      const allowPurchases = availability === "disabled" ? 0 : 1;
      const condition = normalizeProductCondition(p.condition);
      const isConditionShown = toFlagNumber(p.is_condition_shown);
      const defaultPrice = toOptionalNumber(p.price) ?? 0;
      const costPrice = toOptionalNumber(p.cost_price);
      const retailPrice = toOptionalNumber(p.retail_price);
      const salePrice = toOptionalNumber(p.sale_price);
      const weight = toOptionalNumber(p.weight);
      const width = toOptionalNumber(p.width);
      const height = toOptionalNumber(p.height);
      const depth = toOptionalNumber(p.depth);
      const inventoryWarningLevel = toOptionalInteger(p.inventory_warning_level);
      const isFreeShipping = toFlagNumber(p.is_free_shipping);
      const fixedCostShippingPrice = toOptionalNumber(p.fixed_cost_shipping_price);
      const orderQuantityMinimum = toOptionalInteger(p.order_quantity_minimum);
      const orderQuantityMaximum = toOptionalInteger(p.order_quantity_maximum);
      const pageTitle = toOptionalString(p.page_title);
      const metaKeywords = toMetaKeywordsString(p.meta_keywords);
      const metaDescription = toOptionalString(p.meta_description);
      const sortOrder = toOptionalNumber(p.sort_order);
      const searchKeywords = toOptionalString(p.search_keywords);
      const warranty = toOptionalString(p.warranty);
      const upc = toOptionalString(p.upc);
      const mpn = toOptionalString(p.mpn);
      const categoryIds = toCategoryIds(p.categories);
      const customFields = toCustomFields(p.custom_fields);

      let product = await ctx.db.query("products").withIndex("by_external_id", q => q.eq("external_product_id", externalId)).first();
      if (product) {
        await ctx.db.patch(product._id, {
          name,
          description,
          brand,
          status,
          is_visible: isVisible,
          availability,
          allow_purchases: allowPurchases,
          availability_description: availabilityDescription,
          condition,
          is_condition_shown: isConditionShown,
          default_price: defaultPrice,
          cost_price: costPrice,
          retail_price: retailPrice,
          sale_price: salePrice,
          weight,
          width,
          height,
          depth,
          inventory_warning_level: inventoryWarningLevel,
          is_free_shipping: isFreeShipping,
          fixed_cost_shipping_price: fixedCostShippingPrice,
          order_quantity_minimum: orderQuantityMinimum,
          order_quantity_maximum: orderQuantityMaximum,
          page_title: pageTitle,
          meta_keywords: metaKeywords,
          meta_description: metaDescription,
          sort_order: sortOrder,
          search_keywords: searchKeywords,
          warranty,
          custom_fields: customFields,
          upc,
          mpn,
          category_ids: categoryIds,
          sync_needed: 0, updated_at: new Date().toISOString()
        });
      } else {
        await ctx.db.insert("products", {
          external_product_id: externalId,
          name,
          description,
          brand,
          status,
          is_visible: isVisible,
          availability,
          allow_purchases: allowPurchases,
          availability_description: availabilityDescription,
          condition,
          is_condition_shown: isConditionShown,
          default_price: defaultPrice,
          cost_price: costPrice,
          retail_price: retailPrice,
          sale_price: salePrice,
          weight,
          width,
          height,
          depth,
          inventory_warning_level: inventoryWarningLevel,
          is_free_shipping: isFreeShipping,
          fixed_cost_shipping_price: fixedCostShippingPrice,
          order_quantity_minimum: orderQuantityMinimum,
          order_quantity_maximum: orderQuantityMaximum,
          page_title: pageTitle,
          meta_keywords: metaKeywords,
          meta_description: metaDescription,
          sort_order: sortOrder,
          search_keywords: searchKeywords,
          warranty,
          custom_fields: customFields,
          upc,
          mpn,
          category_ids: categoryIds,
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
