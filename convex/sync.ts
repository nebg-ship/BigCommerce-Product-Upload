import { query, mutation } from "./_generated/server";
import { v } from "convex/values";

export const getSyncJobs = query({
  args: {},
  handler: async (ctx) => {
    return await ctx.db.query("sync_queue").order("desc").take(100);
  },
});

export const retrySyncJob = mutation({
  args: { id: v.id("sync_queue") },
  handler: async (ctx, args) => {
    const job = await ctx.db.get(args.id);
    if (!job) {
      throw new Error("Job not found");
    }

    await ctx.db.patch(args.id, {
      error_message: undefined,
      status: "pending",
      updated_at: new Date().toISOString(),
    });

    return { success: true };
  },
});
