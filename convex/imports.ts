import { mutation, query, internalMutation } from "./_generated/server";
import { v } from "convex/values";

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

      const externalId = record['Product ID'];
      if (!externalId) {
        invalidCount++;
        errors.push({ row: rowIndex, error: 'Missing Product ID', data: record });
        continue;
      }

      const id = externalId;

      if (args.importType === 'delete') {
        try {
          const product = await ctx.db.query("products").withIndex("by_external_id", q => q.eq("external_product_id", id)).first();
          if (!product) {
            invalidCount++;
            errors.push({ row: rowIndex, error: 'Product not found in local database', data: record });
            continue;
          }

          // Delete variants
          const variants = await ctx.db.query("variants").withIndex("by_product", q => q.eq("product_id", id)).collect();
          for (const v of variants) await ctx.db.delete(v._id);
          
          // Delete product
          await ctx.db.delete(product._id);

          // Queue sync
          await ctx.db.insert("sync_queue", {
            entity_type: "product",
            internal_id: id,
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

      // Update logic
      const name = record['Name'];
      if (!name) {
        invalidCount++;
        errors.push({ row: rowIndex, error: 'Missing Name', data: record });
        continue;
      }

      const description = record['Description'] || null;
      const brand = record['Brand'] || null;
      const isVisible = parseInt(record['Product Visible']) === 1 ? 1 : 0;
      const status = isVisible ? 'active' : 'inactive';
      const price = parseFloat(record['Price']) || 0;
      const sku = record['Code'];
      const inventoryLevel = parseInt(record['Stock Level']) || 0;

      try {
        let product = await ctx.db.query("products").withIndex("by_external_id", q => q.eq("external_product_id", id)).first();
        const productChanges: Record<string, any> = {};
        let productAction = 'create';

        if (product) {
          productAction = 'update';
          if (product.name !== name) productChanges.name = { old: product.name, new: name };
          if (product.description !== description) productChanges.description = { old: product.description, new: description };
          if (product.brand !== brand) productChanges.brand = { old: product.brand, new: brand };
          if (product.status !== status) productChanges.status = { old: product.status, new: status };
          if (product.is_visible !== isVisible) productChanges.is_visible = { old: product.is_visible, new: isVisible };
          if (product.default_price !== price) productChanges.default_price = { old: product.default_price, new: price };
          
          await ctx.db.patch(product._id, {
            name, description, brand, status, is_visible: isVisible, default_price: price,
            sync_needed: 1, updated_at: new Date().toISOString()
          });
        } else {
          productChanges.name = { new: name };
          productChanges.description = { new: description };
          productChanges.brand = { new: brand };
          productChanges.status = { new: status };
          productChanges.is_visible = { new: isVisible };
          productChanges.default_price = { new: price };

          await ctx.db.insert("products", {
            external_product_id: id,
            name, description, brand, status, is_visible: isVisible, default_price: price,
            sync_needed: 1, created_at: new Date().toISOString(), updated_at: new Date().toISOString()
          });
        }

        if (Object.keys(productChanges).length > 0) {
          await ctx.db.insert("sync_queue", {
            entity_type: "product",
            internal_id: id,
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
            if (variant.price !== price) variantChanges.price = { old: variant.price, new: price };
            if (variant.inventory_level !== inventoryLevel) variantChanges.inventory_level = { old: variant.inventory_level, new: inventoryLevel };
            
            await ctx.db.patch(variant._id, {
              price, inventory_level: inventoryLevel, sync_needed: 1, updated_at: new Date().toISOString()
            });
          } else {
            variantChanges.price = { new: price };
            variantChanges.inventory_level = { new: inventoryLevel };

            await ctx.db.insert("variants", {
              product_id: id,
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
