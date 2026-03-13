import { cronJobs } from "convex/server";
import { api } from "./_generated/api";

const crons = cronJobs();

crons.interval(
  "process bigcommerce sync queue",
  { minutes: 1 },
  api.syncProcessor.processPendingSyncJobs,
  { limit: 10 },
);

export default crons;
