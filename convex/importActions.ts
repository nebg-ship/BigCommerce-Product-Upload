"use node";
import { action } from "./_generated/server";
import { v } from "convex/values";
import { parse } from "csv-parse/sync";
import { internal } from "./_generated/api";

export const processCsvAction = action({
  args: {
    filename: v.string(),
    content: v.string(),
    importType: v.string()
  },
  handler: async (ctx, args) => {
    const { filename, content, importType } = args;
    
    let records: any[];
    try {
      records = parse(content, {
        columns: true,
        skip_empty_lines: true,
        relax_column_count: true
      });
    } catch (err) {
      console.error('CSV Parse Error:', err);
      throw new Error('Failed to parse CSV format');
    }

    // Call internal mutation to process records in batches or all at once
    // Since Convex mutations have a limit, we might need to batch, but for now we'll send it all
    // if it's not huge.
    const result = await ctx.runMutation(internal.imports.processRecords, {
      filename,
      records: JSON.stringify(records),
      importType
    });

    return result;
  }
});
