import { action, internalMutation, internalQuery } from "./_generated/server";
import { internal } from "./_generated/api";
import type { Doc, Id } from "./_generated/dataModel";
import { v } from "convex/values";

const DEFAULT_SYNC_BATCH_SIZE = 10;
const MAX_SYNC_BATCH_SIZE = 25;
const MAX_SYNC_ATTEMPTS = 5;
const ACTIONABLE_SYNC_STATUSES = ["pending", "processing", "failed", "dead"] as const;
const DEFAULT_VARIANT_OPTION_NAME = "SKU";

type SyncJob = Doc<"sync_queue">;
type ProductRecord = Doc<"products">;
type VariantRecord = Doc<"variants">;

type ProductSnapshot = {
  product: ProductRecord;
  variants: VariantRecord[];
};

type BigCommerceProduct = {
  base_variant_id?: number | null;
  custom_url?: {
    url?: string;
  } | null;
  id: number;
};

type BigCommerceProductResponse = {
  data?: BigCommerceProduct | null;
};

type BigCommerceVariant = {
  id: number;
  option_values?: Array<{
    id?: number;
    label?: string | null;
    option_display_name?: string | null;
    option_id?: number;
  }> | null;
  sku?: string | null;
};

type BigCommerceVariantListResponse = {
  data?: BigCommerceVariant[] | null;
};

type BigCommerceVariantResponse = {
  data?: BigCommerceVariant | null;
};

type BigCommerceOptionValue = {
  id: number;
  label: string;
  sort_order?: number | null;
};

type BigCommerceOption = {
  display_name: string;
  id: number;
  option_values?: BigCommerceOptionValue[] | null;
};

type BigCommerceOptionResponse = {
  data?: BigCommerceOption | null;
};

type BigCommerceOptionsResponse = {
  data?: BigCommerceOption[] | null;
};

type BigCommerceOptionValueResponse = {
  data?: BigCommerceOptionValue | null;
};

type BigCommerceSitesResponse = {
  data?: Array<{
    id: number;
  }> | null;
};

type ProcessJobOutcome = {
  productId?: Id<"products">;
  variantId?: Id<"variants">;
};

type BigCommerceClient = {
  accessToken: string;
  storeHash: string;
};

function getBigCommerceClient(): BigCommerceClient | null {
  const storeHash = process.env.BIGCOMMERCE_STORE_HASH;
  const accessToken = process.env.BIGCOMMERCE_ACCESS_TOKEN;
  if (!storeHash || !accessToken) {
    return null;
  }

  return { storeHash, accessToken };
}

function getBigCommerceUrl(client: BigCommerceClient, path: string): string {
  return `https://api.bigcommerce.com/stores/${client.storeHash}${path}`;
}

function parseIntegerList(value: string | undefined): number[] {
  if (!value) {
    return [];
  }

  return value
    .split(",")
    .map((entry) => Number(entry.trim()))
    .filter((entry) => Number.isInteger(entry) && entry > 0);
}

function getDefaultCategoryIds(): number[] {
  return parseIntegerList(process.env.BIGCOMMERCE_DEFAULT_CATEGORY_IDS);
}

function getConfiguredSiteIds(): number[] {
  return parseIntegerList(process.env.BIGCOMMERCE_SITE_IDS);
}

function getVariantOptionName(): string {
  return process.env.BIGCOMMERCE_VARIANT_OPTION_NAME?.trim() || DEFAULT_VARIANT_OPTION_NAME;
}

function normalizePath(path: string | null | undefined): string | null {
  if (!path) {
    return null;
  }

  const trimmed = path.trim();
  if (!trimmed) {
    return null;
  }

  if (/^https?:\/\//i.test(trimmed)) {
    try {
      const parsed = new URL(trimmed);
      const normalized = `${parsed.pathname || "/"}${parsed.search}${parsed.hash}`;
      return normalized.startsWith("/") ? normalized : `/${normalized}`;
    } catch {
      return null;
    }
  }

  return trimmed.startsWith("/") ? trimmed : `/${trimmed}`;
}

function isBigCommerceId(value: string | null | undefined): value is string {
  return !!value && /^\d+$/.test(value);
}

function selectPrimaryVariant(variants: VariantRecord[]): VariantRecord | null {
  if (variants.length === 0) {
    return null;
  }

  return [...variants].sort((left, right) => {
    const created = left.created_at.localeCompare(right.created_at);
    return created !== 0 ? created : left.sku.localeCompare(right.sku);
  })[0];
}

async function readErrorMessage(response: Response): Promise<string> {
  const text = await response.text();
  if (!text) {
    return response.statusText || `HTTP ${response.status}`;
  }

  try {
    const parsed = JSON.parse(text);
    if (parsed?.title && parsed?.type) {
      return `${parsed.title}: ${parsed.type}`;
    }
    if (parsed?.title) {
      return parsed.title;
    }
    if (parsed?.message) {
      return parsed.message;
    }
    if (Array.isArray(parsed?.errors) && parsed.errors.length > 0) {
      return parsed.errors.join(", ");
    }
  } catch {
    // Fall through to the raw response body.
  }

  return text;
}

async function bigCommerceRequest<T>(
  client: BigCommerceClient,
  path: string,
  init: RequestInit = {},
): Promise<T | null> {
  const response = await fetch(getBigCommerceUrl(client, path), {
    ...init,
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
      "X-Auth-Token": client.accessToken,
      ...(init.headers ?? {}),
    },
  });

  if (response.status === 404) {
    return null;
  }

  if (!response.ok) {
    const message = await readErrorMessage(response);
    throw new Error(`BigCommerce ${init.method ?? "GET"} ${path} failed (${response.status}): ${message}`);
  }

  if (response.status === 204) {
    return null;
  }

  return await response.json() as T;
}

function safeParsePayload(payload: string | undefined): Record<string, unknown> {
  if (!payload) {
    return {};
  }

  try {
    const parsed = JSON.parse(payload);
    return parsed && typeof parsed === "object" ? parsed as Record<string, unknown> : {};
  } catch {
    return {};
  }
}

function buildProductUpdatePayload(product: ProductRecord) {
  const payload: Record<string, unknown> = {
    availability: product.status === "active" ? "available" : "disabled",
    description: product.description ?? "",
    is_visible: product.is_visible === 1,
    name: product.name,
    price: product.default_price,
  };

  if (product.brand) {
    const brandId = Number(product.brand);
    if (Number.isInteger(brandId) && brandId > 0) {
      payload.brand_id = brandId;
    } else {
      payload.brand_name = product.brand;
    }
  }

  return payload;
}

function buildProductCreatePayload(snapshot: ProductSnapshot) {
  const primaryVariant = selectPrimaryVariant(snapshot.variants);
  const payload = buildProductUpdatePayload(snapshot.product);
  payload.type = "physical";
  payload.weight = 0;

  const defaultCategoryIds = getDefaultCategoryIds();
  if (defaultCategoryIds.length > 0) {
    payload.categories = defaultCategoryIds;
  }

  if (primaryVariant?.sku) {
    payload.sku = primaryVariant.sku;
  }

  if (snapshot.variants.length <= 1 && primaryVariant) {
    payload.inventory_level = primaryVariant.inventory_level;
    payload.inventory_tracking = "product";
  } else if (snapshot.variants.length > 1) {
    payload.inventory_tracking = "variant";
  }

  return payload;
}

async function listJobsForStatuses(ctx: any, statuses: readonly string[]) {
  const groups = await Promise.all(
    statuses.map((status) =>
      ctx.db.query("sync_queue").withIndex("by_status", (q: any) => q.eq("status", status)).collect(),
    ),
  );

  return groups.flat() as SyncJob[];
}

async function hasOutstandingJobs(
  ctx: any,
  entityType: "product" | "variant",
  identifiers: string[],
  excludeId: Id<"sync_queue">,
) {
  const jobs = await listJobsForStatuses(ctx, ACTIONABLE_SYNC_STATUSES);
  return jobs.some(
    (job) =>
      job.entity_type === entityType &&
      job._id !== excludeId &&
      identifiers.includes(job.internal_id),
  );
}

async function fetchRemoteProduct(client: BigCommerceClient, productId: string) {
  const response = await bigCommerceRequest<BigCommerceProductResponse>(
    client,
    `/v3/catalog/products/${productId}?include_fields=id,custom_url,base_variant_id`,
  );

  return response?.data ?? null;
}

async function fetchRemoteVariants(client: BigCommerceClient, productId: string) {
  const response = await bigCommerceRequest<BigCommerceVariantListResponse>(
    client,
    `/v3/catalog/products/${productId}/variants`,
  );

  return response?.data ?? [];
}

async function fetchRemoteOptions(client: BigCommerceClient, productId: string) {
  const response = await bigCommerceRequest<BigCommerceOptionsResponse>(
    client,
    `/v3/catalog/products/${productId}/options?limit=250`,
  );

  return response?.data ?? [];
}

async function resolveSiteIds(client: BigCommerceClient) {
  const configuredSiteIds = getConfiguredSiteIds();
  if (configuredSiteIds.length > 0) {
    return configuredSiteIds;
  }

  const response = await bigCommerceRequest<BigCommerceSitesResponse>(
    client,
    `/v3/sites?limit=250`,
  );
  const siteIds = response?.data?.map((site) => site.id).filter((id) => Number.isInteger(id) && id > 0) ?? [];
  if (siteIds.length === 0) {
    throw new Error(
      "Could not resolve BigCommerce site IDs. Set BIGCOMMERCE_SITE_IDS or grant Sites & Routes read access.",
    );
  }

  return siteIds;
}

async function ensureVariantOptionValue(
  client: BigCommerceClient,
  productId: string,
  option: BigCommerceOption,
  sku: string,
) {
  const existingValue = option.option_values?.find((value) => value.label === sku);
  if (existingValue) {
    return existingValue;
  }

  const createdValue = await bigCommerceRequest<BigCommerceOptionValueResponse>(
    client,
    `/v3/catalog/products/${productId}/options/${option.id}/values`,
    {
      body: JSON.stringify({
        label: sku,
        sort_order: option.option_values?.length ?? 0,
      }),
      method: "POST",
    },
  );

  if (!createdValue?.data) {
    throw new Error(`BigCommerce did not return the created option value for SKU ${sku}.`);
  }

  return createdValue.data;
}

async function ensureVariantOption(
  client: BigCommerceClient,
  productId: string,
  sku: string,
) {
  const optionName = getVariantOptionName();
  const remoteOptions = await fetchRemoteOptions(client, productId);
  const unsupportedOptions = remoteOptions.some((option) => option.display_name !== optionName);
  if (unsupportedOptions) {
    throw new Error(
      "Cannot auto-create a variant on a product that already has its own variant options. The local catalog does not store option values.",
    );
  }

  const existingOption = remoteOptions.find((option) => option.display_name === optionName);
  if (existingOption) {
    const optionValue = await ensureVariantOptionValue(client, productId, existingOption, sku);
    return {
      option: existingOption,
      optionValue,
    };
  }

  const createdOption = await bigCommerceRequest<BigCommerceOptionResponse>(
    client,
    `/v3/catalog/products/${productId}/options`,
    {
      body: JSON.stringify({
        display_name: optionName,
        option_values: [
          {
            label: sku,
            sort_order: 0,
          },
        ],
        product_id: Number(productId),
        type: "dropdown",
      }),
      method: "POST",
    },
  );

  if (!createdOption?.data) {
    throw new Error(`BigCommerce did not return the created variant option for SKU ${sku}.`);
  }

  const optionValue = createdOption.data.option_values?.find((value) => value.label === sku);
  if (!optionValue) {
    throw new Error(`BigCommerce did not return the created option value for SKU ${sku}.`);
  }

  return {
    option: createdOption.data,
    optionValue,
  };
}

async function createRedirectsForDeletedProduct(
  client: BigCommerceClient,
  fromPath: string,
  redirectTarget: string,
) {
  const normalizedFromPath = normalizePath(fromPath);
  const normalizedRedirectTarget = normalizePath(redirectTarget);
  if (!normalizedFromPath || !normalizedRedirectTarget) {
    throw new Error("Redirect creation requires both a valid source path and a valid destination path.");
  }

  const siteIds = await resolveSiteIds(client);
  await bigCommerceRequest(
    client,
    `/v3/storefront/redirects`,
    {
      body: JSON.stringify(
        siteIds.map((siteId) => ({
          from_path: normalizedFromPath,
          site_id: siteId,
          to: {
            type: "url",
            url: normalizedRedirectTarget,
          },
        })),
      ),
      method: "PUT",
    },
  );
}

async function createRemoteProduct(client: BigCommerceClient, snapshot: ProductSnapshot) {
  const created = await bigCommerceRequest<BigCommerceProductResponse>(
    client,
    `/v3/catalog/products`,
    {
      body: JSON.stringify(buildProductCreatePayload(snapshot)),
      method: "POST",
    },
  );

  if (!created?.data) {
    throw new Error(`BigCommerce did not return the created product for ${snapshot.product.name}.`);
  }

  return created.data;
}

async function createRemoteVariant(
  client: BigCommerceClient,
  productId: string,
  variant: VariantRecord,
) {
  const { option, optionValue } = await ensureVariantOption(client, productId, variant.sku);
  const createdVariant = await bigCommerceRequest<BigCommerceVariantResponse>(
    client,
    `/v3/catalog/products/${productId}/variants`,
    {
      body: JSON.stringify({
        inventory_level: variant.inventory_level,
        option_values: [
          {
            id: optionValue.id,
            label: optionValue.label,
            option_display_name: option.display_name,
            option_id: option.id,
          },
        ],
        price: variant.price,
        product_id: Number(productId),
        sku: variant.sku,
      }),
      method: "POST",
    },
  );

  return createdVariant?.data ?? null;
}

async function processProductJob(ctx: any, client: BigCommerceClient, job: SyncJob): Promise<ProcessJobOutcome> {
  const payload = safeParsePayload(job.payload);

  if (job.action === "delete") {
    const payloadExternalProductId =
      typeof payload.external_product_id === "string" && payload.external_product_id
        ? payload.external_product_id
        : null;
    const externalProductId = payloadExternalProductId ?? (isBigCommerceId(job.internal_id) ? job.internal_id : null);
    const redirectTarget = typeof payload.redirect_url === "string" ? payload.redirect_url : null;
    let sourcePath = typeof payload.from_path === "string" ? normalizePath(payload.from_path) : null;

    const remoteProduct = externalProductId ? await fetchRemoteProduct(client, externalProductId) : null;
    if (remoteProduct?.custom_url?.url) {
      sourcePath = normalizePath(remoteProduct.custom_url.url) ?? sourcePath;
      await ctx.runMutation(internal.syncProcessor.mergeJobPayload, {
        jobId: job._id,
        patch: JSON.stringify({
          external_product_id: externalProductId,
          from_path: sourcePath,
        }),
      });
    }

    if (externalProductId && remoteProduct) {
      await bigCommerceRequest(
        client,
        `/v3/catalog/products/${externalProductId}`,
        { method: "DELETE" },
      );
    }

    if (redirectTarget) {
      if (!sourcePath) {
        if (!externalProductId) {
          throw new Error("Cannot create a redirect for a product that has not been synced to BigCommerce yet.");
        }
        throw new Error("Could not determine the deleted product path required to create a redirect.");
      }

      await createRedirectsForDeletedProduct(client, sourcePath, redirectTarget);
    }

    return {};
  }

  const snapshot = await ctx.runQuery(internal.syncProcessor.getProductSnapshotForSync, { id: job.internal_id });
  if (!snapshot) {
    throw new Error(`Local product ${job.internal_id} was not found.`);
  }

  const { product } = snapshot;
  const remoteProductId = product.external_product_id || job.internal_id;
  const remoteProduct = isBigCommerceId(remoteProductId)
    ? await fetchRemoteProduct(client, remoteProductId)
    : null;

  if (!remoteProduct) {
    if (job.action !== "create") {
      throw new Error(`BigCommerce product ${remoteProductId} was not found.`);
    }

    const createdProduct = await createRemoteProduct(client, snapshot);
    const newExternalProductId = createdProduct.id.toString();
    await ctx.runMutation(internal.syncProcessor.rebindCreatedProduct, {
      oldIdentifier: job.internal_id,
      productId: product._id,
      publicProductId: newExternalProductId,
    });

    return { productId: product._id };
  }

  await bigCommerceRequest(
    client,
    `/v3/catalog/products/${remoteProduct.id}`,
    {
      body: JSON.stringify(buildProductUpdatePayload(product)),
      method: "PUT",
    },
  );

  return { productId: product._id };
}

async function processVariantJob(ctx: any, client: BigCommerceClient, job: SyncJob): Promise<ProcessJobOutcome> {
  const snapshot = await ctx.runQuery(internal.syncProcessor.getVariantForSync, { sku: job.internal_id });
  if (!snapshot) {
    throw new Error(`Local variant ${job.internal_id} was not found.`);
  }

  const { product, variant } = snapshot;
  if (!product?.external_product_id) {
    throw new Error(`Variant ${variant.sku} is missing a mapped BigCommerce product ID.`);
  }

  const remoteVariants = await fetchRemoteVariants(client, product.external_product_id);
  const remoteVariant = remoteVariants.find((entry) => entry.sku === variant.sku);
  if (!remoteVariant) {
    if (job.action !== "create") {
      throw new Error(`BigCommerce variant ${variant.sku} was not found.`);
    }

    const createdVariant = await createRemoteVariant(client, product.external_product_id, variant);
    if (!createdVariant) {
      throw new Error(`BigCommerce did not return the created variant for SKU ${variant.sku}.`);
    }

    return {
      productId: product._id,
      variantId: variant._id,
    };
  }

  await bigCommerceRequest(
    client,
    `/v3/catalog/products/${product.external_product_id}/variants/${remoteVariant.id}`,
    {
      body: JSON.stringify({
        inventory_level: variant.inventory_level,
        price: variant.price,
      }),
      method: "PUT",
    },
  );

  return {
    productId: product._id,
    variantId: variant._id,
  };
}

async function processSyncJob(ctx: any, client: BigCommerceClient, job: SyncJob) {
  switch (job.entity_type) {
    case "product":
      return await processProductJob(ctx, client, job);
    case "variant":
      return await processVariantJob(ctx, client, job);
    default:
      throw new Error(`Unsupported sync entity type: ${job.entity_type}`);
  }
}

export const getProductForSync = internalQuery({
  args: { id: v.string() },
  handler: async (ctx, args) => {
    let product = await ctx.db.query("products").withIndex("by_external_id", (q) => q.eq("external_product_id", args.id)).first();
    if (!product) {
      try {
        product = await ctx.db.get(args.id as Id<"products">);
      } catch {
        // Ignore invalid Convex IDs and fall through.
      }
    }

    return product;
  },
});

export const getProductSnapshotForSync = internalQuery({
  args: { id: v.string() },
  handler: async (ctx, args) => {
    let product = await ctx.db.query("products").withIndex("by_external_id", (q) => q.eq("external_product_id", args.id)).first();
    if (!product) {
      try {
        product = await ctx.db.get(args.id as Id<"products">);
      } catch {
        product = null;
      }
    }

    if (!product) {
      return null;
    }

    const identifiers = new Set<string>([args.id, product._id]);
    if (product.external_product_id) {
      identifiers.add(product.external_product_id);
    }

    const groups = await Promise.all(
      [...identifiers].map((identifier) =>
        ctx.db.query("variants").withIndex("by_product", (q) => q.eq("product_id", identifier)).collect(),
      ),
    );

    const variants = [...new Map(groups.flat().map((variant) => [variant._id, variant])).values()].sort((left, right) => {
      const created = left.created_at.localeCompare(right.created_at);
      return created !== 0 ? created : left.sku.localeCompare(right.sku);
    });

    return { product, variants };
  },
});

export const getVariantForSync = internalQuery({
  args: { sku: v.string() },
  handler: async (ctx, args) => {
    const variant = await ctx.db.query("variants").withIndex("by_sku", (q) => q.eq("sku", args.sku)).first();
    if (!variant) {
      return null;
    }

    let product = await ctx.db.query("products").withIndex("by_external_id", (q) => q.eq("external_product_id", variant.product_id)).first();
    if (!product) {
      try {
        product = await ctx.db.get(variant.product_id as Id<"products">);
      } catch {
        product = null;
      }
    }

    return { product, variant };
  },
});

export const claimPendingJobs = internalMutation({
  args: {
    limit: v.number(),
  },
  handler: async (ctx, args) => {
    const batchSize = Math.min(Math.max(args.limit, 1), MAX_SYNC_BATCH_SIZE);
    const now = new Date().toISOString();
    const pendingJobs = await ctx.db.query("sync_queue").withIndex("by_status", (q) => q.eq("status", "pending")).collect();
    const jobsToClaim = pendingJobs
      .sort((a, b) => a.created_at.localeCompare(b.created_at))
      .slice(0, batchSize);

    for (const job of jobsToClaim) {
      await ctx.db.patch(job._id, {
        attempts: job.attempts + 1,
        error_message: undefined,
        status: "processing",
        updated_at: now,
      });
    }

    return jobsToClaim.map((job) => ({
      ...job,
      attempts: job.attempts + 1,
      error_message: undefined,
      status: "processing" as const,
      updated_at: now,
    }));
  },
});

export const mergeJobPayload = internalMutation({
  args: {
    jobId: v.id("sync_queue"),
    patch: v.string(),
  },
  handler: async (ctx, args) => {
    const job = await ctx.db.get(args.jobId);
    if (!job) {
      return { success: false };
    }

    const patch = safeParsePayload(args.patch);
    const payload = {
      ...safeParsePayload(job.payload),
      ...patch,
    };

    await ctx.db.patch(args.jobId, {
      payload: JSON.stringify(payload),
      updated_at: new Date().toISOString(),
    });

    return { success: true };
  },
});

export const rebindCreatedProduct = internalMutation({
  args: {
    oldIdentifier: v.string(),
    productId: v.id("products"),
    publicProductId: v.string(),
  },
  handler: async (ctx, args) => {
    const product = await ctx.db.get(args.productId);
    if (!product) {
      return { success: false };
    }

    const now = new Date().toISOString();
    await ctx.db.patch(args.productId, {
      external_product_id: args.publicProductId,
      updated_at: now,
    });

    const variantGroups = await Promise.all([
      ctx.db.query("variants").withIndex("by_product", (q) => q.eq("product_id", args.oldIdentifier)).collect(),
      ctx.db.query("variants").withIndex("by_product", (q) => q.eq("product_id", product._id)).collect(),
    ]);

    for (const variant of new Map(variantGroups.flat().map((entry) => [entry._id, entry])).values()) {
      await ctx.db.patch(variant._id, {
        product_id: args.publicProductId,
        updated_at: now,
      });
    }

    const jobs = await listJobsForStatuses(ctx, ACTIONABLE_SYNC_STATUSES);
    for (const job of jobs) {
      if (job.entity_type !== "product" || job.internal_id !== args.oldIdentifier) {
        continue;
      }

      const payload = safeParsePayload(job.payload);
      if (payload.external_product_id === args.oldIdentifier) {
        payload.external_product_id = args.publicProductId;
      }

      await ctx.db.patch(job._id, {
        internal_id: args.publicProductId,
        payload: job.payload ? JSON.stringify(payload) : job.payload,
        updated_at: now,
      });
    }

    return { success: true };
  },
});

export const markJobSucceeded = internalMutation({
  args: {
    jobId: v.id("sync_queue"),
    productId: v.optional(v.id("products")),
    variantId: v.optional(v.id("variants")),
  },
  handler: async (ctx, args) => {
    const job = await ctx.db.get(args.jobId);
    if (!job) {
      return { status: "missing" as const };
    }

    const now = new Date().toISOString();
    await ctx.db.patch(args.jobId, {
      error_message: undefined,
      status: "success",
      updated_at: now,
    });

    if (args.productId) {
      const product = await ctx.db.get(args.productId);
      if (product) {
        const identifiers = [job.internal_id, product._id];
        if (product.external_product_id) {
          identifiers.push(product.external_product_id);
        }

        const hasMoreJobs = await hasOutstandingJobs(ctx, "product", identifiers, job._id);
        if (!hasMoreJobs) {
          await ctx.db.patch(product._id, {
            sync_needed: 0,
            updated_at: now,
          });
        }
      }
    }

    if (args.variantId) {
      const variant = await ctx.db.get(args.variantId);
      if (variant) {
        const hasMoreJobs = await hasOutstandingJobs(ctx, "variant", [variant.sku], job._id);
        if (!hasMoreJobs) {
          await ctx.db.patch(variant._id, {
            sync_needed: 0,
            updated_at: now,
          });
        }
      }
    }

    return { status: "success" as const };
  },
});

export const markJobFailed = internalMutation({
  args: {
    errorMessage: v.string(),
    jobId: v.id("sync_queue"),
  },
  handler: async (ctx, args) => {
    const job = await ctx.db.get(args.jobId);
    if (!job) {
      return { status: "missing" as const };
    }

    const nextStatus = job.attempts >= MAX_SYNC_ATTEMPTS ? "dead" : "failed";
    await ctx.db.patch(args.jobId, {
      error_message: args.errorMessage,
      status: nextStatus,
      updated_at: new Date().toISOString(),
    });

    return { status: nextStatus };
  },
});

export const processPendingSyncJobs = action({
  args: {
    limit: v.optional(v.number()),
  },
  handler: async (ctx, args) => {
    const client = getBigCommerceClient();
    if (!client) {
      return {
        failed: 0,
        jobs: [],
        processed: 0,
        reason: "BIGCOMMERCE_STORE_HASH and BIGCOMMERCE_ACCESS_TOKEN must be set in Convex environment variables.",
        skipped: true,
        succeeded: 0,
      };
    }

    const claimedJobs = await ctx.runMutation(internal.syncProcessor.claimPendingJobs, {
      limit: args.limit ?? DEFAULT_SYNC_BATCH_SIZE,
    });

    if (claimedJobs.length === 0) {
      return {
        failed: 0,
        jobs: [],
        processed: 0,
        reason: null,
        skipped: false,
        succeeded: 0,
      };
    }

    const results: Array<{ error?: string; jobId: string; status: "success" | "failed" | "dead" }> = [];
    let succeeded = 0;
    let failed = 0;

    for (const job of claimedJobs) {
      try {
        const outcome = await processSyncJob(ctx, client, job);
        await ctx.runMutation(internal.syncProcessor.markJobSucceeded, {
          jobId: job._id,
          productId: outcome.productId,
          variantId: outcome.variantId,
        });
        succeeded += 1;
        results.push({
          jobId: job._id,
          status: "success",
        });
      } catch (error) {
        const message = error instanceof Error ? error.message : "Unknown sync error";
        const failure = await ctx.runMutation(internal.syncProcessor.markJobFailed, {
          errorMessage: message,
          jobId: job._id,
        });
        const failureStatus = failure.status === "dead" ? "dead" : "failed";
        failed += 1;
        results.push({
          error: message,
          jobId: job._id,
          status: failureStatus,
        });
      }
    }

    return {
      failed,
      jobs: results,
      processed: claimedJobs.length,
      reason: null,
      skipped: false,
      succeeded,
    };
  },
});
