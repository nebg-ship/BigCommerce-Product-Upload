/* eslint-disable */
/**
 * Generated `api` utility.
 *
 * THIS CODE IS AUTOMATICALLY GENERATED.
 *
 * To regenerate, run `npx convex dev`.
 * @module
 */

import type * as bigcommerce from "../bigcommerce.js";
import type * as crons from "../crons.js";
import type * as importActions from "../importActions.js";
import type * as imports from "../imports.js";
import type * as products from "../products.js";
import type * as sync from "../sync.js";
import type * as syncProcessor from "../syncProcessor.js";

import type {
  ApiFromModules,
  FilterApi,
  FunctionReference,
} from "convex/server";

declare const fullApi: ApiFromModules<{
  bigcommerce: typeof bigcommerce;
  crons: typeof crons;
  importActions: typeof importActions;
  imports: typeof imports;
  products: typeof products;
  sync: typeof sync;
  syncProcessor: typeof syncProcessor;
}>;

/**
 * A utility for referencing Convex functions in your app's public API.
 *
 * Usage:
 * ```js
 * const myFunctionReference = api.myModule.myFunction;
 * ```
 */
export declare const api: FilterApi<
  typeof fullApi,
  FunctionReference<any, "public">
>;

/**
 * A utility for referencing Convex functions in your app's internal API.
 *
 * Usage:
 * ```js
 * const myFunctionReference = internal.myModule.myFunction;
 * ```
 */
export declare const internal: FilterApi<
  typeof fullApi,
  FunctionReference<any, "internal">
>;

export declare const components: {};
