import { mutation, query, internalMutation } from "./_generated/server";
import type { Doc, Id } from "./_generated/dataModel";
import { v } from "convex/values";

type ProductRecord = Doc<"products">;
type VariantRecord = Doc<"variants">;
type ProductCustomField = {
  name: string;
  value: string;
};

type ProductImage = {
  image_url: string;
  description?: string;
  is_thumbnail?: boolean;
  sort_order?: number;
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

function readBooleanFlag(value: unknown): boolean | undefined {
  if (value === null || value === undefined) {
    return undefined;
  }

  if (typeof value === "boolean") {
    return value;
  }

  const normalized = String(value).trim().toLowerCase();
  if (!normalized) {
    return undefined;
  }

  if (["1", "true", "yes", "y"].includes(normalized)) {
    return true;
  }

  if (["0", "false", "no", "n"].includes(normalized)) {
    return false;
  }

  return undefined;
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

function normalizeProductImages(images: ProductImage[]): ProductImage[] {
  const entries = new Map<string, ProductImage>();
  for (const image of images) {
    const imageUrl = readString(image.image_url);
    if (!imageUrl) {
      continue;
    }

    entries.set(imageUrl, {
      image_url: imageUrl,
      description: readString(image.description),
      is_thumbnail: typeof image.is_thumbnail === "boolean" ? image.is_thumbnail : undefined,
      sort_order: readInteger(image.sort_order),
    });
  }

  return [...entries.values()].sort((left, right) => {
    const sortDifference = (left.sort_order ?? 0) - (right.sort_order ?? 0);
    return sortDifference !== 0 ? sortDifference : left.image_url.localeCompare(right.image_url);
  });
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

function parseProductImages(value: unknown): ProductImage[] | undefined {
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
      const entries = Array.isArray(parsed) ? parsed : [parsed];
      return normalizeProductImages(
        entries.flatMap((entry) => {
          if (!entry || typeof entry !== "object") {
            return [];
          }

          const record = entry as Record<string, unknown>;
          const imageUrl = readString(record.image_url ?? record.imageUrl ?? record.url);
          if (!imageUrl) {
            return [];
          }

          return [{
            image_url: imageUrl,
            description: readString(record.description ?? record.alt_text ?? record.alt),
            is_thumbnail: readBooleanFlag(record.is_thumbnail ?? record.isThumbnail),
            sort_order: readInteger(record.sort_order ?? record.sortOrder),
          }];
        }),
      );
    } catch (error: any) {
      throw new Error(`Could not parse Product Images JSON: ${error.message || "Invalid JSON."}`);
    }
  }

  return normalizeProductImages(raw
    .split(/\r?\n/)
    .map((entry) => entry.trim())
    .filter(Boolean)
    .map((entry) => {
      const [imageUrl, description, sortOrder, isThumbnail] = entry.split("|").map((part) => part.trim());
      return {
        image_url: imageUrl,
        description: readString(description),
        sort_order: readInteger(sortOrder),
        is_thumbnail: readBooleanFlag(isThumbnail),
      };
    }));
}

function arraysEqual(left: unknown, right: unknown) {
  return JSON.stringify(left ?? []) === JSON.stringify(right ?? []);
}

function hasColumn(record: Record<string, unknown>, column: string) {
  return Object.prototype.hasOwnProperty.call(record, column);
}

const ALWAYS_INCLUDED_UPDATE_COLUMNS = new Set(["Product ID", "Code", "Item Type"]);
const IMAGE_SELECTION_COLUMNS = new Set(["Image Description", "Image Sort Order", "Image Is Thumbnail"]);

function filterRecordForSelectedFields(
  record: Record<string, unknown>,
  importType: string,
  selectedFields: string[] | undefined,
) {
  if (importType !== "update" || !selectedFields) {
    return record;
  }

  const selectedFieldSet = new Set(selectedFields);
  const includeImageUrl = [...IMAGE_SELECTION_COLUMNS].some((column) => selectedFieldSet.has(column));
  const filteredRecord: Record<string, unknown> = {};

  for (const [key, value] of Object.entries(record)) {
    if (
      ALWAYS_INCLUDED_UPDATE_COLUMNS.has(key) ||
      selectedFieldSet.has(key) ||
      (key === "Image URL" && includeImageUrl)
    ) {
      filteredRecord[key] = value;
    }
  }

  return filteredRecord;
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
    importType: v.string(),
    selectedFields: v.optional(v.array(v.string())),
  },
  handler: async (ctx, args) => {
    const records = JSON.parse(args.records);
    if (args.importType === "update" && args.selectedFields && args.selectedFields.length === 0) {
      throw new Error("Select at least one field to update.");
    }

    let validCount = 0;
    let invalidCount = 0;
    const errors: { row: number; error: string; data: any }[] = [];

    let rowIndex = 0;
    for (const record of records) {
      rowIndex++;
      const itemType = readString(record['Item Type']);
      if (itemType && itemType !== 'Product') continue;

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

      try {
        const row = filterRecordForSelectedFields(
          record as Record<string, unknown>,
          args.importType,
          args.selectedFields,
        );
        const name = readString(row['Name']);
        const description = readString(row['Description']);
        const brand = readString(row['Brand']);
        const isVisible = hasColumn(row, 'Product Visible') ? (readInteger(row['Product Visible']) === 1 ? 1 : 0) : undefined;
        const status = isVisible === undefined ? undefined : isVisible ? 'active' : 'inactive';
        const price = readNumber(row['Price']);
        const costPrice = readNumber(row['Cost Price']);
        const retailPrice = readNumber(row['Retail Price']);
        const salePrice = readNumber(row['Sale Price']);
        const sku = readString(row['Code']);
        const inventoryLevel = readInteger(row['Stock Level']);
        const weight = readNumber(row['Weight']);
        const width = readNumber(row['Width']);
        const height = readNumber(row['Height']);
        const depth = readNumber(row['Depth']);
        const pageTitle = readString(row['Page Title']);
        const metaKeywords = readString(row['Meta Keywords']);
        const metaDescription = readString(row['Meta Description']);
        const sortOrder = readInteger(row['Sort Order']);
        const searchKeywords = readString(row['Search Keywords']);
        const condition = hasColumn(row, 'Product Condition') ? normalizeProductCondition(row['Product Condition']) : undefined;
        const isConditionShown = readInteger(row['Show Product Condition']);
        const allowPurchases = readInteger(row['Allow Purchases']);
        const inventoryWarningLevel = readInteger(row['Low Stock Level']);
        const categoryString = readString(row['Category String']);
        const categoryIds = parseCategoryIdsFromDetails(row['Category Details']);
        const availabilityDescription = readString(row['Product Availability']);
        const warranty = readString(row['Warranty']);
        const freeShippingValue = readInteger(row['Free Shipping']);
        const isFreeShipping = freeShippingValue === undefined ? undefined : freeShippingValue === 1 ? 1 : 0;
        const fixedCostShippingPrice = readNumber(row['Fixed Shipping Price']);
        const orderQuantityMinimum = readInteger(row['Minimum Purchase Quantity']);
        const orderQuantityMaximum = readInteger(row['Maximum Purchase Quantity']);
        const customFields = parseProductCustomFields(row['Product Custom Fields']);
        const productImages = hasColumn(row, 'Product Images')
          ? parseProductImages(row['Product Images'])
          : (
            hasColumn(row, 'Image URL') ||
            hasColumn(row, 'Image Description') ||
            hasColumn(row, 'Image Sort Order') ||
            hasColumn(row, 'Image Is Thumbnail')
          )
            ? parseProductImages(JSON.stringify([{
                image_url: row['Image URL'],
                description: row['Image Description'],
                sort_order: row['Image Sort Order'],
                is_thumbnail: row['Image Is Thumbnail'],
              }]))
            : undefined;
        const upc = readString(row['Product UPC/EAN']);
        const mpn = readString(row['Manufacturer Part Number']);

        let product = await getProductForImport(ctx, externalId, sku);
        if (product && !externalId && !sku) {
          throw new Error('Updates require either Product ID or Code.');
        }

        if (!product && !name) {
          throw new Error('Missing Name for product creation.');
        }

        if (hasColumn(row, 'Category String') && !hasColumn(row, 'Category Details') && categoryString) {
          throw new Error('Category updates require the Category Details column with category IDs.');
        }

        const availability = hasColumn(row, 'Allow Purchases')
          ? allowPurchases === 0
            ? 'disabled'
            : product?.availability === 'preorder'
              ? 'preorder'
              : 'available'
          : undefined;
        const productChanges: Record<string, any> = {};
        let productAction = 'create';
        let productIdentifier: string;
        const now = new Date().toISOString();

        if (product) {
          productAction = 'update';
          const productPatch: Record<string, any> = {
            sync_needed: 1,
            updated_at: now,
          };

          const assignScalarField = (column: string, field: keyof ProductRecord, nextValue: unknown) => {
            if (!hasColumn(row, column)) {
              return;
            }

            if ((product as any)[field] !== nextValue) {
              productChanges[field] = { old: (product as any)[field], new: nextValue };
              productPatch[field] = nextValue;
            }
          };

          assignScalarField('Name', 'name', name);
          assignScalarField('Description', 'description', description);
          assignScalarField('Brand', 'brand', brand);
          assignScalarField('Product Visible', 'is_visible', isVisible);
          assignScalarField('Product Visible', 'status', status);
          assignScalarField('Price', 'default_price', price);
          assignScalarField('Cost Price', 'cost_price', costPrice);
          assignScalarField('Retail Price', 'retail_price', retailPrice);
          assignScalarField('Sale Price', 'sale_price', salePrice);
          assignScalarField('Weight', 'weight', weight);
          assignScalarField('Width', 'width', width);
          assignScalarField('Height', 'height', height);
          assignScalarField('Depth', 'depth', depth);
          assignScalarField('Page Title', 'page_title', pageTitle);
          assignScalarField('Meta Keywords', 'meta_keywords', metaKeywords);
          assignScalarField('Meta Description', 'meta_description', metaDescription);
          assignScalarField('Sort Order', 'sort_order', sortOrder);
          assignScalarField('Search Keywords', 'search_keywords', searchKeywords);
          assignScalarField('Product Condition', 'condition', condition);
          assignScalarField('Show Product Condition', 'is_condition_shown', isConditionShown);
          assignScalarField('Allow Purchases', 'allow_purchases', allowPurchases);
          assignScalarField('Allow Purchases', 'availability', availability);
          assignScalarField('Product Availability', 'availability_description', availabilityDescription);
          assignScalarField('Low Stock Level', 'inventory_warning_level', inventoryWarningLevel);
          assignScalarField('Category String', 'category_string', categoryString);
          assignScalarField('Warranty', 'warranty', warranty);
          assignScalarField('Free Shipping', 'is_free_shipping', isFreeShipping);
          assignScalarField('Fixed Shipping Price', 'fixed_cost_shipping_price', fixedCostShippingPrice);
          assignScalarField('Minimum Purchase Quantity', 'order_quantity_minimum', orderQuantityMinimum);
          assignScalarField('Maximum Purchase Quantity', 'order_quantity_maximum', orderQuantityMaximum);
          assignScalarField('Product UPC/EAN', 'upc', upc);
          assignScalarField('Manufacturer Part Number', 'mpn', mpn);

          if (hasColumn(row, 'Category Details') && !arraysEqual(product.category_ids, categoryIds)) {
            productChanges.category_ids = { old: product.category_ids, new: categoryIds };
            productPatch.category_ids = categoryIds;
          }

          if (hasColumn(row, 'Product Custom Fields') && !arraysEqual(product.custom_fields, customFields)) {
            productChanges.custom_fields = { old: product.custom_fields, new: customFields };
            productPatch.custom_fields = customFields;
          }

          if (
            (hasColumn(row, 'Product Images') ||
              hasColumn(row, 'Image URL') ||
              hasColumn(row, 'Image Description') ||
              hasColumn(row, 'Image Sort Order') ||
              hasColumn(row, 'Image Is Thumbnail')) &&
            !arraysEqual(product.images, productImages)
          ) {
            productChanges.images = { old: product.images, new: productImages };
            productPatch.images = productImages;
          }

          if (Object.keys(productChanges).length > 0) {
            await ctx.db.patch(product._id, productPatch);
          }
          productIdentifier = getProductSyncIdentifier(product);
        } else {
          const createPayload: Record<string, any> = {
            ...(externalId ? { external_product_id: externalId } : {}),
            name,
            status: status ?? 'inactive',
            is_visible: isVisible ?? 0,
            default_price: price ?? 0,
            sync_needed: 1,
            created_at: now,
            updated_at: now,
          };

          const assignCreateField = (column: string, field: string, nextValue: unknown) => {
            if (!hasColumn(row, column)) {
              return;
            }

            createPayload[field] = nextValue;
            productChanges[field] = { new: nextValue };
          };

          productChanges.name = { new: name };
          productChanges.status = { new: createPayload.status };
          productChanges.is_visible = { new: createPayload.is_visible };
          productChanges.default_price = { new: createPayload.default_price };

          assignCreateField('Description', 'description', description);
          assignCreateField('Brand', 'brand', brand);
          assignCreateField('Cost Price', 'cost_price', costPrice);
          assignCreateField('Retail Price', 'retail_price', retailPrice);
          assignCreateField('Sale Price', 'sale_price', salePrice);
          assignCreateField('Weight', 'weight', weight);
          assignCreateField('Width', 'width', width);
          assignCreateField('Height', 'height', height);
          assignCreateField('Depth', 'depth', depth);
          assignCreateField('Page Title', 'page_title', pageTitle);
          assignCreateField('Meta Keywords', 'meta_keywords', metaKeywords);
          assignCreateField('Meta Description', 'meta_description', metaDescription);
          assignCreateField('Sort Order', 'sort_order', sortOrder);
          assignCreateField('Search Keywords', 'search_keywords', searchKeywords);
          assignCreateField('Product Condition', 'condition', condition);
          assignCreateField('Show Product Condition', 'is_condition_shown', isConditionShown);
          assignCreateField('Allow Purchases', 'allow_purchases', allowPurchases);
          assignCreateField('Allow Purchases', 'availability', availability);
          assignCreateField('Product Availability', 'availability_description', availabilityDescription);
          assignCreateField('Low Stock Level', 'inventory_warning_level', inventoryWarningLevel);
          assignCreateField('Category String', 'category_string', categoryString);
          assignCreateField('Category Details', 'category_ids', categoryIds);
          assignCreateField('Warranty', 'warranty', warranty);
          assignCreateField('Free Shipping', 'is_free_shipping', isFreeShipping);
          assignCreateField('Fixed Shipping Price', 'fixed_cost_shipping_price', fixedCostShippingPrice);
          assignCreateField('Minimum Purchase Quantity', 'order_quantity_minimum', orderQuantityMinimum);
          assignCreateField('Maximum Purchase Quantity', 'order_quantity_maximum', orderQuantityMaximum);
          assignCreateField('Product Custom Fields', 'custom_fields', customFields);
          assignCreateField('Product Images', 'images', productImages);
          if (!hasColumn(row, 'Product Images') && productImages !== undefined) {
            createPayload.images = productImages;
            productChanges.images = { new: productImages };
          }
          assignCreateField('Product UPC/EAN', 'upc', upc);
          assignCreateField('Manufacturer Part Number', 'mpn', mpn);

          const productId = await ctx.db.insert("products", createPayload as any);
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
            created_at: now,
            updated_at: now
          });
        }

        if (sku) {
          let variant = await ctx.db.query("variants").withIndex("by_sku", q => q.eq("sku", sku)).first();
          const variantChanges: Record<string, any> = {};
          let variantAction = 'create';
          const variantPrice = price ?? product?.default_price ?? 0;
          const nextInventoryLevel = inventoryLevel ?? 0;
          const variantFieldsProvided = hasColumn(row, 'Price') || hasColumn(row, 'Stock Level');

          if (variant) {
            variantAction = 'update';
            if (variant.product_id !== productIdentifier) {
              throw new Error(`SKU ${sku} is already linked to another product.`);
            }
            const variantPatch: Record<string, any> = {
              product_id: productIdentifier,
              sync_needed: 1,
              updated_at: now,
            };

            if (hasColumn(row, 'Price') && variant.price !== variantPrice) {
              variantChanges.price = { old: variant.price, new: variantPrice };
              variantPatch.price = variantPrice;
            }

            if (hasColumn(row, 'Stock Level') && variant.inventory_level !== nextInventoryLevel) {
              variantChanges.inventory_level = { old: variant.inventory_level, new: nextInventoryLevel };
              variantPatch.inventory_level = nextInventoryLevel;
            }

            if (Object.keys(variantChanges).length > 0) {
              await ctx.db.patch(variant._id, variantPatch);
            }
          } else {
            if (hasColumn(row, 'Price')) {
              variantChanges.price = { new: variantPrice };
            }
            if (hasColumn(row, 'Stock Level')) {
              variantChanges.inventory_level = { new: nextInventoryLevel };
            }

            await ctx.db.insert("variants", {
              product_id: productIdentifier,
              sku, price: variantPrice, inventory_level: nextInventoryLevel, sync_needed: 1,
              created_at: now, updated_at: now
            });
          }

          if (variantFieldsProvided && Object.keys(variantChanges).length > 0) {
            await ctx.db.insert("sync_queue", {
              entity_type: "variant",
              internal_id: sku,
              action: variantAction,
              status: "pending",
              attempts: 0,
              payload: JSON.stringify(variantChanges),
              created_at: now,
              updated_at: now
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
