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
  image_id?: number;
  image_url: string;
  description?: string;
  is_thumbnail?: boolean;
  sort_order?: number;
};

type MutableProductImage = {
  image_id?: number;
  image_url?: string;
  description?: string;
  is_thumbnail?: boolean;
  sort_order?: number;
};

const IMAGE_ID_COLUMN_PATTERN = /^Product Image ID - (\d+)$/;
const IMAGE_URL_COLUMN_PATTERNS = [
  /^Product Image URL - (\d+)$/,
  /^Product Image File - (\d+)$/,
];
const IMAGE_DESCRIPTION_COLUMN_PATTERN = /^Product Image Description - (\d+)$/;
const IMAGE_SORT_COLUMN_PATTERNS = [
  /^Product Image Sort Order - (\d+)$/,
  /^Product Image Sort - (\d+)$/,
];
const IMAGE_THUMBNAIL_COLUMN_PATTERN = /^Product Image Is Thumbnail - (\d+)$/;

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
  const normalizedImages: MutableProductImage[] = [];
  const imagesById = new Map<number, MutableProductImage>();
  const imagesByUrl = new Map<string, MutableProductImage>();

  const unlinkImage = (image: MutableProductImage) => {
    if (image.image_id !== undefined && imagesById.get(image.image_id) === image) {
      imagesById.delete(image.image_id);
    }

    if (image.image_url && imagesByUrl.get(image.image_url) === image) {
      imagesByUrl.delete(image.image_url);
    }
  };

  const linkImage = (image: MutableProductImage) => {
    if (image.image_id !== undefined) {
      imagesById.set(image.image_id, image);
    }

    if (image.image_url) {
      imagesByUrl.set(image.image_url, image);
    }
  };

  const applyImageValues = (target: MutableProductImage, source: MutableProductImage) => {
    unlinkImage(target);

    if (source.image_id !== undefined) {
      target.image_id = source.image_id;
    }

    if (source.image_url !== undefined) {
      target.image_url = source.image_url;
    }

    if (source.description !== undefined) {
      target.description = source.description;
    }

    if (source.is_thumbnail !== undefined) {
      target.is_thumbnail = source.is_thumbnail;
    }

    if (source.sort_order !== undefined) {
      target.sort_order = source.sort_order;
    }

    linkImage(target);
  };

  const mergeImages = (target: MutableProductImage, source: MutableProductImage) => {
    if (target === source) {
      return target;
    }

    applyImageValues(target, source);
    unlinkImage(source);

    const sourceIndex = normalizedImages.indexOf(source);
    if (sourceIndex >= 0) {
      normalizedImages.splice(sourceIndex, 1);
    }

    return target;
  };

  for (const image of images) {
    const imageUrl = readString(image.image_url);
    const imageId = readInteger(image.image_id);
    if (!imageUrl && imageId === undefined) {
      continue;
    }

    const nextImage: MutableProductImage = {
      ...(imageId === undefined ? {} : { image_id: imageId }),
      ...(imageUrl === undefined ? {} : { image_url: imageUrl }),
      description: readString(image.description),
      is_thumbnail: typeof image.is_thumbnail === "boolean" ? image.is_thumbnail : undefined,
      sort_order: readInteger(image.sort_order),
    };

    const matchedById = imageId === undefined ? undefined : imagesById.get(imageId);
    const matchedByUrl = imageUrl === undefined ? undefined : imagesByUrl.get(imageUrl);
    let target = matchedById ?? matchedByUrl;

    if (matchedById && matchedByUrl && matchedById !== matchedByUrl) {
      target = mergeImages(matchedById, matchedByUrl);
    }

    if (!target) {
      target = {};
      normalizedImages.push(target);
    }

    applyImageValues(target, nextImage);
  }

  return normalizedImages.map((image) => ({
    ...(image.image_id === undefined ? {} : { image_id: image.image_id }),
    image_url: image.image_url ?? `image-${image.image_id}`,
    ...(image.description === undefined ? {} : { description: image.description }),
    ...(image.is_thumbnail === undefined ? {} : { is_thumbnail: image.is_thumbnail }),
    ...(image.sort_order === undefined ? {} : { sort_order: image.sort_order }),
  })).sort((left, right) => {
    const sortDifference = (left.sort_order ?? 0) - (right.sort_order ?? 0);
    if (sortDifference !== 0) {
      return sortDifference;
    }

    if ((left.image_id ?? 0) !== (right.image_id ?? 0)) {
      return (left.image_id ?? 0) - (right.image_id ?? 0);
    }

    return left.image_url.localeCompare(right.image_url);
  });
}

function findMatchingProductImage(
  images: ProductImage[],
  imageId: number | undefined,
  imageUrl: string | undefined,
  slot: number,
) {
  const slotIndex = slot - 1;
  const slotImage = slotIndex >= 0 && slotIndex < images.length ? images[slotIndex] : undefined;

  const idIndex = imageId === undefined
    ? -1
    : images.findIndex((image) => image.image_id === imageId);
  if (idIndex >= 0) {
    const mergeIndex = slotImage && slotIndex !== idIndex && slotImage.image_id === undefined
      ? slotIndex
      : -1;

    return {
      matchIndex: idIndex,
      mergeIndex,
    };
  }

  const urlIndex = imageUrl === undefined
    ? -1
    : images.findIndex((image) => image.image_url === imageUrl);
  if (urlIndex >= 0) {
    return {
      matchIndex: urlIndex,
      mergeIndex: -1,
    };
  }

  if (imageId !== undefined && slotImage && slotImage.image_id === undefined) {
    return {
      matchIndex: slotIndex,
      mergeIndex: -1,
    };
  }

  return null;
}

function readFirstPresentValue(row: Record<string, unknown>, columns: string[]) {
  for (const column of columns) {
    if (hasColumn(row, column)) {
      return row[column];
    }
  }

  return undefined;
}

function hasAnyColumn(row: Record<string, unknown>, columns: string[]) {
  return columns.some((column) => hasColumn(row, column));
}

function getImageFieldColumns(
  slot: number,
  field: "id" | "url" | "description" | "sort" | "thumbnail",
) {
  switch (field) {
    case "id":
      return [`Product Image ID - ${slot}`];
    case "url":
      return slot === 1
        ? [`Product Image URL - 1`, `Product Image File - 1`, "Image URL"]
        : [`Product Image URL - ${slot}`, `Product Image File - ${slot}`];
    case "description":
      return slot === 1
        ? [`Product Image Description - 1`, "Image Description"]
        : [`Product Image Description - ${slot}`];
    case "sort":
      return slot === 1
        ? [`Product Image Sort Order - 1`, `Product Image Sort - 1`, "Image Sort Order"]
        : [`Product Image Sort Order - ${slot}`, `Product Image Sort - ${slot}`];
    case "thumbnail":
      return slot === 1
        ? [`Product Image Is Thumbnail - 1`, "Image Is Thumbnail"]
        : [`Product Image Is Thumbnail - ${slot}`];
  }
}

function getImageSlotFromColumn(column: string): number | null {
  if (column === "Image URL" || column === "Image Description" || column === "Image Sort Order" || column === "Image Is Thumbnail") {
    return 1;
  }

  const patterns = [
    IMAGE_ID_COLUMN_PATTERN,
    IMAGE_DESCRIPTION_COLUMN_PATTERN,
    IMAGE_THUMBNAIL_COLUMN_PATTERN,
    ...IMAGE_URL_COLUMN_PATTERNS,
    ...IMAGE_SORT_COLUMN_PATTERNS,
  ];

  for (const pattern of patterns) {
    const match = column.match(pattern);
    if (match) {
      return Number(match[1]);
    }
  }

  return null;
}

function getImageSlotsFromRow(row: Record<string, unknown>) {
  const slots = new Set<number>();
  for (const column of Object.keys(row)) {
    const slot = getImageSlotFromColumn(column);
    if (slot !== null) {
      slots.add(slot);
    }
  }

  return [...slots].sort((left, right) => left - right);
}

function hasMeaningfulValue(value: unknown) {
  if (value === null || value === undefined) {
    return false;
  }

  if (typeof value === "string") {
    return value.trim().length > 0;
  }

  return true;
}

function slotHasAnyImageData(row: Record<string, unknown>, slot: number) {
  const slotColumns = [
    ...getImageFieldColumns(slot, "id"),
    ...getImageFieldColumns(slot, "url"),
    ...getImageFieldColumns(slot, "description"),
    ...getImageFieldColumns(slot, "sort"),
    ...getImageFieldColumns(slot, "thumbnail"),
  ];

  return slotColumns.some((column) => hasColumn(row, column) && hasMeaningfulValue(row[column]));
}

function hasImageFieldForSlot(
  row: Record<string, unknown>,
  slot: number,
  field: "id" | "url" | "description" | "sort" | "thumbnail",
) {
  return hasAnyColumn(row, getImageFieldColumns(slot, field));
}

function buildImageUpdatesFromRow(
  existingImages: ProductImage[] | undefined,
  row: Record<string, unknown>,
): ProductImage[] {
  const nextImages = normalizeProductImages(existingImages ?? []);

  for (const slot of getImageSlotsFromRow(row)) {
    if (!slotHasAnyImageData(row, slot)) {
      continue;
    }

    const imageId = readInteger(readFirstPresentValue(row, getImageFieldColumns(slot, "id")));
    const imageUrl = readString(readFirstPresentValue(row, getImageFieldColumns(slot, "url")));
    if (imageId === undefined && !imageUrl) {
      throw new Error(`Product Image ID - ${slot} or image URL for slot ${slot} is required when updating image fields.`);
    }

    const match = findMatchingProductImage(nextImages, imageId, imageUrl, slot);
    const matchedImage = match ? nextImages[match.matchIndex] : undefined;
    if (!matchedImage && !imageUrl) {
      throw new Error(`Image slot ${slot} could not be matched locally. Provide the image URL for this slot.`);
    }

    const mergeImage = match && match.mergeIndex >= 0 ? nextImages[match.mergeIndex] : undefined;
    const currentImage = mergeImage ?? matchedImage ?? {
      ...(imageId === undefined ? {} : { image_id: imageId }),
      image_url: imageUrl ?? `image-${imageId}`,
    };

    const nextImage: ProductImage = {
      ...currentImage,
      ...(imageId === undefined ? {} : { image_id: imageId }),
      image_url: imageId === undefined
        ? imageUrl ?? currentImage.image_url
        : currentImage.image_url,
    };

    if (hasImageFieldForSlot(row, slot, "description")) {
      nextImage.description = readString(readFirstPresentValue(row, getImageFieldColumns(slot, "description")));
    }

    if (hasImageFieldForSlot(row, slot, "sort")) {
      nextImage.sort_order = readInteger(readFirstPresentValue(row, getImageFieldColumns(slot, "sort")));
    }

    if (hasImageFieldForSlot(row, slot, "thumbnail")) {
      nextImage.is_thumbnail = readBooleanFlag(readFirstPresentValue(row, getImageFieldColumns(slot, "thumbnail")));
    }

    if (match) {
      const targetIndex = match.mergeIndex >= 0 ? match.mergeIndex : match.matchIndex;
      nextImages[targetIndex] = nextImage;

      if (match.mergeIndex >= 0 && match.matchIndex !== targetIndex) {
        nextImages.splice(match.matchIndex, 1);
      }
    } else {
      nextImages.push(nextImage);
    }
  }

  return normalizeProductImages(nextImages);
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
            image_id: readInteger(record.image_id ?? record.imageId ?? record.id),
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
const IMAGE_SELECTION_COLUMNS = new Set([
  "Image Description",
  "Image Sort Order",
  "Image Is Thumbnail",
  "Product Image Description - 1",
  "Product Image Sort Order - 1",
  "Product Image Sort - 1",
  "Product Image Is Thumbnail - 1",
]);

function isImageHelperColumn(column: string) {
  return column === "Image URL" ||
    IMAGE_ID_COLUMN_PATTERN.test(column) ||
    IMAGE_URL_COLUMN_PATTERNS.some((pattern) => pattern.test(column));
}

function isImageSelectableColumn(column: string) {
  return IMAGE_SELECTION_COLUMNS.has(column) ||
    IMAGE_DESCRIPTION_COLUMN_PATTERN.test(column) ||
    IMAGE_SORT_COLUMN_PATTERNS.some((pattern) => pattern.test(column)) ||
    IMAGE_THUMBNAIL_COLUMN_PATTERN.test(column);
}

function filterRecordForSelectedFields(
  record: Record<string, unknown>,
  importType: string,
  selectedFields: string[] | undefined,
) {
  if (importType !== "update" || !selectedFields) {
    return record;
  }

  const selectedFieldSet = new Set(selectedFields);
  const selectedImageSlots = new Set<number>();
  for (const field of selectedFieldSet) {
    if (!isImageSelectableColumn(field)) {
      continue;
    }

    const slot = getImageSlotFromColumn(field);
    if (slot !== null) {
      selectedImageSlots.add(slot);
    }
  }

  const filteredRecord: Record<string, unknown> = {};

  for (const [key, value] of Object.entries(record)) {
    const imageSlot = getImageSlotFromColumn(key);
    if (
      ALWAYS_INCLUDED_UPDATE_COLUMNS.has(key) ||
      selectedFieldSet.has(key) ||
      (imageSlot !== null && isImageHelperColumn(key) && selectedImageSlots.has(imageSlot))
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
    let changedRowCount = 0;
    let unchangedRowCount = 0;
    let productJobsCreated = 0;
    let variantJobsCreated = 0;
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
          changedRowCount++;
          productJobsCreated++;
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
        let productJobCreated = false;
        let variantJobCreated = false;
        const hasFullImagesColumn = hasColumn(row, 'Product Images');
        const imageSlots = getImageSlotsFromRow(row);
        const hasSingleImageColumns = imageSlots.length > 0;
        const productImages = hasFullImagesColumn
          ? parseProductImages(row['Product Images'])
          : hasSingleImageColumns
            ? buildImageUpdatesFromRow(product?.images as ProductImage[] | undefined, row)
            : undefined;

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

          if ((hasFullImagesColumn || hasSingleImageColumns) && !arraysEqual(product.images, productImages)) {
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
          if (!hasFullImagesColumn && productImages !== undefined) {
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
          productJobCreated = true;
          productJobsCreated++;
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
            variantJobCreated = true;
            variantJobsCreated++;
          }
        }

        if (productJobCreated || variantJobCreated) {
          changedRowCount++;
        } else {
          unchangedRowCount++;
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
      changed_row_count: changedRowCount,
      unchanged_row_count: unchangedRowCount,
      sync_jobs_created_count: productJobsCreated + variantJobsCreated,
      product_jobs_created_count: productJobsCreated,
      variant_jobs_created_count: variantJobsCreated,
      errors: JSON.stringify(errors),
      created_at: new Date().toISOString()
    });

    return {
      success: true,
      importId,
      validCount,
      invalidCount,
      changedRowCount,
      unchangedRowCount,
      syncJobsCreatedCount: productJobsCreated + variantJobsCreated,
      productJobsCreatedCount: productJobsCreated,
      variantJobsCreatedCount: variantJobsCreated,
      errors,
    };
  }
});
