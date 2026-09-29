import { Type } from "typebox";
import { TimestampSchema } from "./common.ts";

// Read-only V1 wire metadata: retained in canonical historical hashes, never
// emitted by current reducers/adapters and never used as an execution deadline.
export const HistoricalLeaseFieldsV1 = { expiresAt: Type.Optional(Type.Union([TimestampSchema, Type.Null()])) };
export const HistoricalAuthorizationFieldsV1 = { validUntil: Type.Optional(Type.Union([TimestampSchema, Type.Null()])) };
