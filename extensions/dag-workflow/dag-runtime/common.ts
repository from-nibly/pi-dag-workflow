import { createHash } from "node:crypto";
import { Type, type Static } from "typebox";
import { Errors } from "typebox/value";

export type JsonPrimitive = null | boolean | number | string;
export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };

export const HASH_PATTERN = "^sha256:[0-9a-f]{64}$";
export const ID_PATTERN = "^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$";
export const GIT_OID_PATTERN = "^(?:[0-9a-f]{40}|[0-9a-f]{64})$";
export const ROOT_RELATIVE_PATTERN = "^(?!/)(?![A-Za-z][A-Za-z0-9+.-]*:)(?!.*\\\\)(?!.*(?:^|/)\\.\\.(?:/|$)).+$";

export const IdSchema = Type.String({ minLength: 1, maxLength: 128, pattern: ID_PATTERN });
export const HashSchema = Type.String({ pattern: HASH_PATTERN });
export const GitOidSchema = Type.String({ pattern: GIT_OID_PATTERN });
export const TimestampSchema = Type.String({ pattern: "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d{1,9})?Z$" });
export const RootRelativePathSchema = Type.String({ minLength: 1, maxLength: 1024, pattern: ROOT_RELATIVE_PATTERN });
export const NonNegativeIntegerSchema = Type.Integer({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER });
export const PositiveIntegerSchema = Type.Integer({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER });
export const BoundedTextSchema = Type.String({ maxLength: 65_536 });

export const Nullable = <const T>(schema: T) => Type.Union([schema as never, Type.Null()]);
export const StrictObject = <const P extends Record<string, any>>(properties: P) =>
  Type.Object(properties, { additionalProperties: false });
export const IdMap = <const T>(schema: T) => Type.Record(
  Type.String({ pattern: ID_PATTERN }),
  schema as never,
  { additionalProperties: false },
);
export const HashMap = <const T>(schema: T) => Type.Record(
  Type.String({ pattern: HASH_PATTERN }),
  schema as never,
  { additionalProperties: false },
);
export const StringSet = (options: Record<string, unknown> = {}) => Type.Array(IdSchema, options);
export const HashSet = (options: Record<string, unknown> = {}) => Type.Array(HashSchema, options);

export const SensitivitySchema = Type.Enum(["public", "internal", "restricted"]);
export const RetentionSchema = Type.Enum(["ephemeral", "run", "project"]);

export const ArtifactRefV1Schema = StrictObject({
  artifactId: IdSchema,
  hash: HashSchema,
  bytes: NonNegativeIntegerSchema,
  mediaType: Type.String({ minLength: 1, maxLength: 128 }),
  schemaId: Nullable(IdSchema),
  sensitivity: SensitivitySchema,
  retention: RetentionSchema,
  locator: Nullable(RootRelativePathSchema),
});
export type ArtifactRefV1 = Static<typeof ArtifactRefV1Schema>;

export const GitTreeRefV1Schema = StrictObject({
  repositoryId: IdSchema,
  commit: GitOidSchema,
  tree: GitOidSchema,
});
export type GitTreeRefV1 = Static<typeof GitTreeRefV1Schema>;

export interface ValidationIssue {
  path: string;
  message: string;
}

export interface ValidationResult<T> {
  ok: boolean;
  value?: T;
  issues: ValidationIssue[];
}

export function schemaIssues(schema: any, value: unknown): ValidationIssue[] {
  return [...Errors(schema, value)].map((error) => ({
    path: error.instancePath || "/",
    message: error.message,
  }));
}

export function canonicalizeJson(value: unknown, path = "$"): JsonValue {
  if (value === null || typeof value === "boolean" || typeof value === "string") {
    if (typeof value === "string") assertValidUnicode(value, path);
    return value;
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new Error(`${path} contains a non-finite number`);
    if (!Number.isSafeInteger(value) && Number.isInteger(value)) throw new Error(`${path} contains an unsafe integer`);
    return Object.is(value, -0) ? 0 : value;
  }
  if (Array.isArray(value)) {
    for (let index = 0; index < value.length; index += 1) {
      if (!(index in value)) throw new Error(`${path}[${index}] is a sparse array slot`);
    }
    const expectedKeys = new Set(Array.from({ length: value.length }, (_, index) => String(index)));
    for (const key of Object.keys(value)) if (!expectedKeys.has(key)) throw new Error(`${path} contains a non-JSON array property ${key}`);
    if (Object.getOwnPropertySymbols(value).length) throw new Error(`${path} contains symbol keys`);
    return value.map((item, index) => canonicalizeJson(item, `${path}[${index}]`));
  }
  if (value && typeof value === "object") {
    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) throw new Error(`${path} is not a plain JSON object`);
    if (Object.getOwnPropertySymbols(value).length) throw new Error(`${path} contains symbol keys`);
    if (Object.getOwnPropertyNames(value).length !== Object.keys(value).length) throw new Error(`${path} contains non-enumerable properties`);
    const output: Record<string, JsonValue> = Object.create(null);
    for (const key of Object.keys(value as Record<string, unknown>).sort()) {
      assertValidUnicode(key, `${path} key`);
      const descriptor = Object.getOwnPropertyDescriptor(value, key);
      if (!descriptor || !("value" in descriptor)) throw new Error(`${path}.${key} is not a JSON data property`);
      const item = descriptor.value;
      if (item === undefined) throw new Error(`${path}.${key} is undefined`);
      output[key] = canonicalizeJson(item, `${path}.${key}`);
    }
    return output;
  }
  throw new Error(`${path} is not a JSON value`);
}

export function canonicalStringify(value: unknown): string {
  return JSON.stringify(canonicalizeJson(value));
}

export function canonicalHash(value: unknown): string {
  return `sha256:${createHash("sha256").update(canonicalStringify(value)).digest("hex")}`;
}

export function hashWithoutField(value: Record<string, unknown>, field: string): string {
  const copy = { ...value };
  delete copy[field];
  return canonicalHash(copy);
}

export function contentHashMatches(value: unknown, field = "contentHash"): boolean {
  return Boolean(value && typeof value === "object" && !Array.isArray(value) &&
    typeof (value as Record<string, unknown>)[field] === "string" &&
    (value as Record<string, unknown>)[field] === hashWithoutField(value as Record<string, unknown>, field));
}

export function isSortedUnique(values: readonly string[]): boolean {
  for (let index = 1; index < values.length; index += 1) if (values[index - 1] >= values[index]) return false;
  return true;
}

export function pushIssue(issues: ValidationIssue[], path: string, condition: unknown, message: string): void {
  if (!condition) issues.push({ path, message });
}

export function validateTimestampFields(value: unknown, issues: ValidationIssue[], path = ""): void {
  if (Array.isArray(value)) {
    value.forEach((item, index) => validateTimestampFields(item, issues, `${path}/${index}`));
    return;
  }
  if (!value || typeof value !== "object") return;
  for (const [key, item] of Object.entries(value as Record<string, unknown>)) {
    const childPath = `${path}/${key}`;
    if ((key.endsWith("At") || key === "validFrom" || key === "validUntil" || key === "expiresAt") && item !== null) {
      pushIssue(issues, childPath, typeof item === "string" && isValidUtcTimestamp(item), "must be a valid UTC RFC 3339 timestamp");
    } else validateTimestampFields(item, issues, childPath);
  }
}

export function assertValid<T>(result: ValidationResult<T>, label: string): asserts result is ValidationResult<T> & { ok: true; value: T } {
  if (!result.ok) throw new Error(`${label}:\n${result.issues.map(({ path, message }) => `- ${path}: ${message}`).join("\n")}`);
}

export function parseStrictJson(text: string): JsonValue {
  return new StrictJsonParser(text).parse();
}

class StrictJsonParser {
  #index = 0;
  readonly text: string;
  constructor(text: string) { this.text = text; }

  parse(): JsonValue {
    this.#space();
    const value = this.#value("$");
    this.#space();
    if (this.#index !== this.text.length) this.#fail("unexpected trailing content");
    return value;
  }

  #value(path: string): JsonValue {
    this.#space();
    const char = this.text[this.#index];
    if (char === "{") return this.#object(path);
    if (char === "[") return this.#array(path);
    if (char === '"') return this.#string(path);
    if (char === "t" && this.text.slice(this.#index, this.#index + 4) === "true") { this.#index += 4; return true; }
    if (char === "f" && this.text.slice(this.#index, this.#index + 5) === "false") { this.#index += 5; return false; }
    if (char === "n" && this.text.slice(this.#index, this.#index + 4) === "null") { this.#index += 4; return null; }
    return this.#number(path);
  }

  #object(path: string): Record<string, JsonValue> {
    this.#index += 1;
    this.#space();
    const output: Record<string, JsonValue> = Object.create(null);
    const keys = new Set<string>();
    if (this.text[this.#index] === "}") { this.#index += 1; return output; }
    while (true) {
      if (this.text[this.#index] !== '"') this.#fail("object key must be a string");
      const key = this.#string(`${path} key`);
      if (keys.has(key)) this.#fail(`duplicate object key ${JSON.stringify(key)}`);
      keys.add(key);
      this.#space();
      if (this.text[this.#index] !== ":") this.#fail("expected ':' after object key");
      this.#index += 1;
      output[key] = this.#value(`${path}.${key}`);
      this.#space();
      const char = this.text[this.#index++];
      if (char === "}") return output;
      if (char !== ",") this.#fail("expected ',' or '}' in object");
      this.#space();
    }
  }

  #array(path: string): JsonValue[] {
    this.#index += 1;
    this.#space();
    const output: JsonValue[] = [];
    if (this.text[this.#index] === "]") { this.#index += 1; return output; }
    while (true) {
      output.push(this.#value(`${path}[${output.length}]`));
      this.#space();
      const char = this.text[this.#index++];
      if (char === "]") return output;
      if (char !== ",") this.#fail("expected ',' or ']' in array");
      this.#space();
    }
  }

  #string(path: string): string {
    const start = this.#index;
    this.#index += 1;
    while (this.#index < this.text.length) {
      const code = this.text.charCodeAt(this.#index);
      if (code === 0x22) {
        this.#index += 1;
        let value: string;
        try { value = JSON.parse(this.text.slice(start, this.#index)); }
        catch { this.#fail("invalid JSON string"); }
        assertValidUnicode(value!, path);
        return value!;
      }
      if (code < 0x20) this.#fail("unescaped control character in string");
      if (code === 0x5c) {
        this.#index += 1;
        const escape = this.text[this.#index];
        if (!'"\\/bfnrtu'.includes(escape ?? "")) this.#fail("invalid string escape");
        if (escape === "u") {
          const hex = this.text.slice(this.#index + 1, this.#index + 5);
          if (!/^[0-9a-fA-F]{4}$/.test(hex)) this.#fail("invalid unicode escape");
          this.#index += 4;
        }
      }
      this.#index += 1;
    }
    this.#fail("unterminated string");
  }

  #number(path: string): number {
    const match = /^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?/.exec(this.text.slice(this.#index));
    if (!match) this.#fail("invalid JSON value");
    this.#index += match![0].length;
    const value = Number(match![0]);
    if (!Number.isFinite(value)) throw new Error(`${path} contains a non-finite number`);
    if (Number.isInteger(value) && !Number.isSafeInteger(value)) throw new Error(`${path} contains an unsafe integer`);
    return Object.is(value, -0) ? 0 : value;
  }

  #space(): void { while (/[\u0009\u000a\u000d\u0020]/.test(this.text[this.#index] ?? "")) this.#index += 1; }
  #fail(message: string): never { throw new Error(`Invalid JSON at byte ${this.#index}: ${message}`); }
}

export function utcTimestampOrderValue(value: string): number {
  return /:60(?:\.\d{1,9})?Z$/.test(value) ? Date.parse(value.replace(":60", ":59")) + 1000 : Date.parse(value);
}

function isValidUtcTimestamp(value: string): boolean {
  const match = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?Z$/.exec(value);
  if (!match) return false;
  const [, yearText, monthText, dayText, hourText, minuteText, secondText] = match;
  const year = Number(yearText), month = Number(monthText), day = Number(dayText), hour = Number(hourText), minute = Number(minuteText), second = Number(secondText);
  if (month < 1 || month > 12 || day < 1 || hour > 23 || minute > 59 || second > 60) return false;
  const leapYear = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
  const daysInMonth = [31, leapYear ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][month - 1];
  if (day > daysInMonth) return false;
  return second < 60 || (hour === 23 && minute === 59 && ((month === 6 && day === 30) || (month === 12 && day === 31)));
}

function assertValidUnicode(value: string, path: string): void {
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(index + 1);
      if (!(next >= 0xdc00 && next <= 0xdfff)) throw new Error(`${path} contains an unpaired high surrogate`);
      index += 1;
    } else if (code >= 0xdc00 && code <= 0xdfff) throw new Error(`${path} contains an unpaired low surrogate`);
  }
}
