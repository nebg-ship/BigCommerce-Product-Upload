import { mutation, query, internalMutation } from "./_generated/server";
import type { Doc, Id } from "./_generated/dataModel";
import { v } from "convex/values";

type ProductRecord = Doc<"products">;
type VariantRecord = Doc<"variants">;
type ProductCustomField = {
  name: string;
  value: string;
};

function readString(value: unknown): string | undefined {
  if (value === null || value === undefined) {
    return undefined;
  }

  const trimmed = String(value).trim();
  return trimmed ? trimmed : undefined;
}

function readNumber(value: unknown): number | undefined {
  if (value === null || value === undefined || value === "") {
    return undefined;
  }

  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function readInteger(value: unknown): number | undefined {
  const parsed = readNumber(value);
  return parsed === undefined ? undefined : Math.trunc(parsed);
}

function normalizeProductCondition(value: unknown): string | undefined {
  const raw = readString(value);
  if (!raw) {
    return undefined;
  }

  switch (raw.toLowerCase()) {
    case "new":
      return "New";
    case "used":
      return "Used";
    case "refurbished":
      return "Refurbished";
    default:
      throw new Error(`Unsupported Product Condition "${raw}". Expected New, Used, or Refurbished.`);
  }
}

function parseCategoryIdsFromDetails(value: unknown): number[] | undefined {
  if (value === null || value === undefined) {
    return undefined;
  }

  const raw = String(value).trim();
  if (!raw) {
    return [];
  }

  const categoryIds = [...raw.matchAll(/Category ID:\s*(\d+)/g)]
    .map((match) => Number(match[1]))
    .filter((entry) => Number.isInteger(entry) && entry > 0);

  return [...new Set(categoryIds)];
}

function normalizeProductCustomFields(fields: ProductCustomField[]): ProductCustomField[] {
  const entries = new Map<string, string>();
  for (const field of fields) {
    const name = readString(field.name);
    if (!name) {
      continue;
    }

    entries.set(name, String(field.value ?? "").trim());
  }

  return [...entries.entries()]
    .sort(([leftName], [rightName]) => leftName.localeCompare(rightName))
    .map(([name, fieldValue]) => ({ name, value: fieldValue }));
}

function parseProductCustomFields(value: unknown): ProductCustomField[] | undefined {
  if (value === null || value === undefined) {
    return undefined;
  }

  const raw = String(value).trim();
  if (!raw) {
    return [];
  }

  if (raw.startsWith("[") || raw.startsWith("{")) {
    try {
      const parsed = JSON.parse(raw);
      if (!Array.isArray(parsed)) {
        throw new Error("Expected a JSON array.");
      }

      return normalizeProductCustomFields(
        parsed.flatMap((entry) => {
          if (!entry || typeof entry !== "object") {
            return [];
          }

          const name = readString((entry as Record<string, unknown>).name);
          const fieldValue = (entry as Record<string, unknown>).value ?? (entry as Record<string, unknown>).text;
          if (!name || fieldValue === null || fieldValue === undefined) {
            return [];
          }

          return [{ name, value: String(fieldValue).trim() }];
        }),
      );
    } catch (error: any) {
      throw new Error(`Could not parse Product Custom Fields JSON: ${error.message || "Invalid JSON."}`);
    }
  }

  const separators = raw.includes("\n")
    ? /\r?\n/
    : raw.includes("|")
      ? /\s*\|\s*/
      : raw.includes(";")
        ? /\s*;\s*/
        : null;
  const parts = separators ? raw.split(separators) : [raw];

  return normalizeProductCustomFields(
    parts
      .map((entry) => entry.trim())
      .filter(Boolean)
      .map((entry) => {
        const delimiterIndex = entry.includes("=") ? entry.indexOf("=") : entry.indexOf(":");
        if (delimiterIndex <= 0) {
          throw new Error(
            `Could not parse Product Custom Fields entry "${entry}". Expected "Name=Value", "Name: Value", or a JSON array.`,
          );
        }

        return {
          name: entry.slice(0, delimiterIndex).trim(),
          value: entry.slice(delimiterIndex + 1).trim(),
        };
      }),
  );
}

function getProductSyncIdentifier(product: ProductRecord): string {
  return product.external_product_id || product._id;
}

async function getProductByStoredIdentifier(ctx: any, identifier: string): Promise<ProductRecord | null> {
  let product = await ctx.db.query("products").withIndex("by_external_id", (q: any) => q.eq("external_product_id", identifier)).first();
  if (!product) {
    try {
      product = await ctx.db.get(identifier as Id<"products">);
    } catch {
      product = null;
    }
  }

  return product;
}

async function getProductForImport(
  ctx: any,
  externalId: string | undefined,
  sku: string | undefined,
): Promise<ProductRecord | null> {
  if (externalId) {
    const product = await ctx.db.query("products").withIndex("by_external_id", (q: any) => q.eq("external_product_id", externalId)).first();
    if (product) {
      return product;
    }
  }

  if (!sku) {
    return null;
  }

  const variant = await ctx.db.query("variants").withIndex("by_sku", (q: any) => q.eq("sku", sku)).first();
  if (!variant) {
    return null;
  }

  return await getProductByStoredIdentifier(ctx, variant.product_id);
}

async function getVariantsForProduct(ctx: any, identifiers: string[]): Promise<VariantRecord[]> {
  const groups = await Promise.all(
    identifiers.map((identifier) =>
      ctx.db.query("variants").withIndex("by_product", (q: any) => q.eq("product_id", identifier)).collect(),
    ),
  );

  return [...new Map(groups.flat().map((variant) => [variant._id, variant])).values()];
}

export const getImports = query({
  args: {},
  handler: async (ctx) => {
    return await ctx.db.query("csv_import_runs").order("desc").take(50);
  }
});

export const processRecords = internalMutation({
  args: {
    filename: v.string(),
    records: v.string(), // JSON string to avoid complex types
    importType: v.string()
  },
  handler: async (ctx, args) => {
    const records = JSON.parse(args.records);
    let validCount = 0;
    let invalidCount = 0;
    const errors: { row: number; error: string; data: any }[] = [];

    let rowIndex = 0;
    for (const record of records) {
      rowIndex++;
      const itemType = record['Item Type'];
      if (itemType !== 'Product') continue;

      const externalId = readString(record['Product ID']);
      if (args.importType === 'delete' && !externalId) {
        invalidCount++;
        errors.push({ row: rowIndex, error: 'Missing Product ID', data: record });
        continue;
      }

      if (args.importType === 'delete') {
        try {
          const product = await ctx.db.query("products").withIndex("by_external_id", q => q.eq("external_product_id", externalId!)).first();
          if (!product) {
            invalidCount++;
            errors.push({ row: rowIndex, error: 'Product not found in local database', data: record });
            continue;
          }

          const productIdentifiers = [...new Set([product._id, product.external_product_id].filter((value): value is string => !!value))];
          const variants = await getVariantsForProduct(ctx, productIdentifiers);
          for (const v of variants) await ctx.db.delete(v._id);
          
          await ctx.db.delete(product._id);

          await ctx.db.insert("sync_queue", {
            entity_type: "product",
            internal_id: externalId!,
            action: "delete",
            status: "pending",
            attempts: 0,
            payload: JSON.stringify({ external_product_id: product.external_product_id, redirect_url: null }),
            created_at: new Date().toISOString(),
            updated_at: new Date().toISOString()
          });
          validCount++;
        } catch (err: any) {
          invalidCount++;
          errors.push({ row: rowIndex, error: err.message || 'Database delete error', data: record });
        }
        continue;
      }

      const name = readString(record['Name']);
      if (!name) {
        invalidCount++;
        errors.push({ row: rowIndex, error: 'Missing Name', data: record });
        continue;
      }

      const description = readString(record['Description']);
      const brand = readString(record['Brand']);
      const isVisible = parseInt(record['Product Visible']) === 1 ? 1 : 0;
      const status = isVisible ? 'active' : 'inactive';
      const price = parseFloat(record['Price']) || 0;
      const costPrice = readNumber(record['Cost Price']);
      const retailPrice = readNumber(record['Retail Price']);
      const salePrice = readNumber(record['Sale Price']);
      const sku = readString(record['Code']);
      const inventoryLevel = parseInt(record['Stock Level']) || 0;
      const weight = readNumber(record['Weight']);
      const width = readNumber(record['Width']);
      const height = readNumber(record['Height']);
      const depth = readNumber(record['Depth']);
      const pageTitle = readString(record['Page Title']);
      const metaKeywords = readString(record['Meta Keywords']);
      const metaDescription = readString(record['Meta Description']);
      const sortOrder = readInteger(record['Sort Order']);
      const searchKeywords = readString(record['Search Keywords']);
      const condition = normalizeProductCondition(record['Product Condition']);
      const isConditionShown = readInteger(record['Show Product Condition']);
      const allowPurchases = readInteger(record['Allow Purchases']);
      const inventoryWarningLevel = readInteger(record['Low Stock Level']);
      const categoryString = readString(record['Category String']);
      const categoryIds = parseCategoryIdsFromDetails(record['Category Details']);
      const availabilityDescription = readString(record['Product Availability']);
      const warranty = readString(record['Warranty']);
      const freeShippingValue = readInteger(record['Free Shipping']);
      const isFreeShipping = freeShippingValue === undefined ? undefined : freeShippingValue === 1 ? 1 : 0;
      const fixedCostShippingPrice = readNumber(record['Fixed Shipping Price']);
      const orderQuantityMinimum = readInteger(record['Minimum Purchase Quantity']);
      const orderQuantityMaximum = readInteger(record['Maximum Purchase Quantity']);
      const customFields = parseProductCustomFields(record['Product Custom Fields']);
      const upc = readString(record['Product UPC/EAN']);
      const mpn = readString(record['Manufacturer Part Number']);

      try {
        let product = await getProductForImport(ctx, externalId, sku);
        if (categoryString && categoryIds === undefined) {
          throw new Error('Category updates require the Category Details column with category IDs.');
        }

        const availability = allowPurchases === undefined
          ? product?.availability
          : allowPurchases === 0
            ? 'disabled'
            : product?.availability === 'preorder'
              ? 'preorder'
              : 'available';
        const productChanges: Record<string, any> = {};
        let productAction = 'create';
        let productIdentifier: string;

        if (product) {
          productAction = 'update';
          if (product.name !== name) productChanges.name = { old: product.name, new: name };
          if (product.description !== description) productChanges.description = { old: product.description, new: description };
          if (product.brand !== brand) productChanges.brand = { old: product.brand, new: brand };
          if (product.status !== status) productChanges.status = { old: product.status, new: status };
          if (product.is_visible !== isVisible) productChanges.is_visible = { old: product.is_visible, new: isVisible };
          if (product.default_price !== price) productChanges.default_price = { old: product.default_price, new: price };
          if (product.cost_price !== costPrice) productChanges.cost_price = { old: product.cost_price, new: costPrice };
          if (product.retail_price !== retailPrice) productChanges.retail_price = { old: product.retail_price, new: retailPrice };
          if (product.sale_price !== salePrice) productChanges.sale_price = { old: product.sale_price, new: salePrice };
          if (product.weight !== weight) productChanges.weight = { old: product.weight, new: weight };
          if (product.width !== width) productChanges.width = { old: product.width, new: width };
          if (product.height !== height) productChanges.height = { old: product.height, new: height };
          if (product.depth !== depth) productChanges.depth = { old: product.depth, new: depth };
          if (product.page_title !== pageTitle) productChanges.page_title = { old: product.page_title, new: pageTitle };
          if (product.meta_keywords !== metaKeywords) productChanges.meta_keywords = { old: product.meta_keywords, new: metaKeywords };
          if (product.meta_description !== metaDescription) productChanges.meta_description = { old: product.meta_description, new: metaDescription };
          if (product.sort_order !== sortOrder) productChanges.sort_order = { old: product.sort_order, new: sortOrder };
          if (product.search_keywords !== searchKeywords) productChanges.search_keywords = { old: product.search_keywords, new: searchKeywords };
          if (product.condition !== condition) productChanges.condition = { old: product.condition, new: condition };
          if (product.is_condition_shown !== isConditionShown) productChanges.is_condition_shown = { old: product.is_condition_shown, new: isConditionShown };
          if (product.allow_purchases !== allowPurchases) productChanges.allow_purchases = { old: product.allow_purchases, new: allowPurchases };
          if (product.availability !== availability) productChanges.availability = { old: product.availability, new: availability };
          if (product.availability_description !== availabilityDescription) productChanges.availability_description = { old: product.availability_description, new: availabilityDescription };
          if (product.inventory_warning_level !== inventoryWarningLevel) productChanges.inventory_warning_level = { old: product.inventory_warning_level, new: inventoryWarningLevel };
          if (product.category_string !== categoryString) productChanges.category_string = { old: product.category_string, new: categoryString };
          if (JSON.stringify(product.category_ids ?? []) !== JSON.stringify(categoryIds ?? [])) productChanges.category_ids = { old: product.category_ids, new: categoryIds };
          if (product.warranty !== warranty) productChanges.warranty = { old: product.warranty, new: warranty };
          if (product.is_free_shipping !== isFreeShipping) productChanges.is_free_shipping = { old: product.is_free_shipping, new: isFreeShipping };
          if (product.fixed_cost_shipping_price !== fixedCostShippingPrice) productChanges.fixed_cost_shipping_price = { old: product.fixed_cost_shipping_price, new: fixedCostShippingPrice };
          if (product.order_quantity_minimum !== orderQuantityMinimum) productChanges.order_quantity_minimum = { old: product.order_quantity_minimum, new: orderQuantityMinimum };
          if (product.order_quantity_maximum !== orderQuantityMaximum) productChanges.order_quantity_maximum = { old: product.order_quantity_maximum, new: orderQuantityMaximum };
          if (JSON.stringify(product.custom_fields ?? []) !== JSON.stringify(customFields ?? [])) productChanges.custom_fields = { old: product.custom_fields, new: customFields };
          if (product.upc !== upc) productChanges.upc = { old: product.upc, new: upc };
          if (product.mpn !== mpn) productChanges.mpn = { old: product.mpn, new: mpn };
          
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
            default_price: price,
            cost_price: costPrice,
            retail_price: retailPrice,
            sale_price: salePrice,
            weight,
            width,
            height,
            depth,
            inventory_warning_level: inventoryWarningLevel,
            page_title: pageTitle,
            meta_keywords: metaKeywords,
            meta_description: metaDescription,
            sort_order: sortOrder,
            search_keywords: searchKeywords,
            category_string: categoryString,
            category_ids: categoryIds,
            warranty,
            is_free_shipping: isFreeShipping,
            fixed_cost_shipping_price: fixedCostShippingPrice,
            order_quantity_minimum: orderQuantityMinimum,
            order_quantity_maximum: orderQuantityMaximum,
            custom_fields: customFields,
            upc,
            mpn,
            sync_needed: 1, updated_at: new Date().toISOString()
          });
          productIdentifier = getProductSyncIdentifier(product);
        } else {
          productChanges.name = { new: name };
          productChanges.description = { new: description };
          productChanges.brand = { new: brand };
          productChanges.status = { new: status };
          productChanges.is_visible = { new: isVisible };
          productChanges.default_price = { new: price };
          productChanges.cost_price = { new: costPrice };
          productChanges.retail_price = { new: retailPrice };
          productChanges.sale_price = { new: salePrice };
          productChanges.weight = { new: weight };
          productChanges.width = { new: width };
          productChanges.height = { new: height };
          productChanges.depth = { new: depth };
          productChanges.page_title = { new: pageTitle };
          productChanges.meta_keywords = { new: metaKeywords };
          productChanges.meta_description = { new: metaDescription };
          productChanges.sort_order = { new: sortOrder };
          productChanges.search_keywords = { new: searchKeywords };
          productChanges.condition = { new: condition };
          productChanges.is_condition_shown = { new: isConditionShown };
          productChanges.allow_purchases = { new: allowPurchases };
          productChanges.availability = { new: availability };
          productChanges.availability_description = { new: availabilityDescription };
          productChanges.inventory_warning_level = { new: inventoryWarningLevel };
          productChanges.category_string = { new: categoryString };
          productChanges.category_ids = { new: categoryIds };
          productChanges.warranty = { new: warranty };
          productChanges.is_free_shipping = { new: isFreeShipping };
          productChanges.fixed_cost_shipping_price = { new: fixedCostShippingPrice };
          productChanges.order_quantity_minimum = { new: orderQuantityMinimum };
          productChanges.order_quantity_maximum = { new: orderQuantityMaximum };
          productChanges.custom_fields = { new: customFields };
          productChanges.upc = { new: upc };
          productChanges.mpn = { new: mpn };

          const productId = await ctx.db.insert("products", {
            ...(externalId ? { external_product_id: externalId } : {}),
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
            default_price: price,
            cost_price: costPrice,
            retail_price: retailPrice,
            sale_price: salePrice,
            weight,
            width,
            height,
            depth,
            inventory_warning_level: inventoryWarningLevel,
            page_title: pageTitle,
            meta_keywords: metaKeywords,
            meta_description: metaDescription,
            sort_order: sortOrder,
            search_keywords: searchKeywords,
            category_string: categoryString,
            category_ids: categoryIds,
            warranty,
            is_free_shipping: isFreeShipping,
            fixed_cost_shipping_price: fixedCostShippingPrice,
            order_quantity_minimum: orderQuantityMinimum,
            order_quantity_maximum: orderQuantityMaximum,
            custom_fields: customFields,
            upc,
            mpn,
            sync_needed: 1, created_at: new Date().toISOString(), updated_at: new Date().toISOString()
          });
          product = await ctx.db.get(productId);
          if (!product) {
            throw new Error('Created product could not be reloaded');
          }
          productIdentifier = externalId || productId;
        }

        if (Object.keys(productChanges).length > 0) {
          await ctx.db.insert("sync_queue", {
            entity_type: "product",
            internal_id: productIdentifier,
            action: productAction,
            status: "pending",
            attempts: 0,
            payload: JSON.stringify(productChanges),
            created_at: new Date().toISOString(),
            updated_at: new Date().toISOString()
          });
        }

        if (sku) {
          let variant = await ctx.db.query("variants").withIndex("by_sku", q => q.eq("sku", sku)).first();
          const variantChanges: Record<string, any> = {};
          let variantAction = 'create';

          if (variant) {
            variantAction = 'update';
            if (variant.product_id !== productIdentifier) {
              throw new Error(`SKU ${sku} is already linked to another product.`);
            }
            if (variant.price !== price) variantChanges.price = { old: variant.price, new: price };
            if (variant.inventory_level !== inventoryLevel) variantChanges.inventory_level = { old: variant.inventory_level, new: inventoryLevel };
            
            await ctx.db.patch(variant._id, {
              product_id: productIdentifier, price, inventory_level: inventoryLevel, sync_needed: 1, updated_at: new Date().toISOString()
            });
          } else {
            variantChanges.price = { new: price };
            variantChanges.inventory_level = { new: inventoryLevel };

            await ctx.db.insert("variants", {
              product_id: productIdentifier,
              sku, price, inventory_level: inventoryLevel, sync_needed: 1,
              created_at: new Date().toISOString(), updated_at: new Date().toISOString()
            });
          }

          if (Object.keys(variantChanges).length > 0) {
            await ctx.db.insert("sync_queue", {
              entity_type: "variant",
              internal_id: sku,
              action: variantAction,
              status: "pending",
              attempts: 0,
              payload: JSON.stringify(variantChanges),
              created_at: new Date().toISOString(),
              updated_at: new Date().toISOString()
            });
          }
        }
        
        validCount++;
      } catch (err: any) {
        invalidCount++;
        errors.push({ row: rowIndex, error: err.message || 'Database insert error', data: record });
      }
    }

    const importId = await ctx.db.insert("csv_import_runs", {
      file_name: args.filename,
      status: "processed",
      row_count: records.length,
      valid_row_count: validCount,
      invalid_row_count: invalidCount,
      errors: JSON.stringify(errors),
      created_at: new Date().toISOString()
    });

    return { success: true, importId, validCount, invalidCount, errors };
  }
});
