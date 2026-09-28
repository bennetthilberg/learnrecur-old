import { JobStageTimeoutError } from "./deadline";

export const REFILL_FAILURE_CODES = [
  "REFILL_PROVIDER_UNAVAILABLE",
  "REFILL_SOURCE_UNAVAILABLE",
  "REFILL_DATABASE_UNAVAILABLE",
  "REFILL_STAGE_TIMEOUT",
] as const;
export type RefillFailureCode = (typeof REFILL_FAILURE_CODES)[number];
const STAGES = new Set([
  "refill-load",
  "refill-reservation",
  "refill-source-evidence",
  "refill-recent-history",
  "refill-generation",
  "refill-verification",
  "refill-publication",
  "refill-event-publication",
  "material-event-publication",
  "job-visibility",
]);
const TRANSIENT_CODES = new Set([
  "ECONNREFUSED",
  "ECONNRESET",
  "ETIMEDOUT",
  "ENOTFOUND",
  "EAI_AGAIN",
  "P2024",
  "P2028",
  "P2034",
  "P1001",
  "P1002",
  "P1008",
  "40001",
  "40P01",
  "53300",
  "53400",
  "55P03",
  "57014",
  "57P01",
  "57P02",
  "57P03",
  "DB_CONNECTION_TIMEOUT",
  "EPIPE",
  "UND_ERR_CONNECT_TIMEOUT",
  "UND_ERR_HEADERS_TIMEOUT",
  "UND_ERR_BODY_TIMEOUT",
  ...REFILL_FAILURE_CODES,
]);
const HTTP_RETRIES = new Set([408, 429, 500, 502, 503, 504]);

export class RetryableRefillError extends Error {
  readonly retryable = true;
  constructor(
    readonly code: RefillFailureCode,
    readonly expectedAttempt?: number,
  ) {
    super("Exercise preparation encountered a temporary failure.");
    this.name = "RetryableRefillError";
  }
}

export function isTransientJobError(error: unknown): boolean {
  try {
    for (const value of causes(error)) {
      if (value.retryable === false) return false;
      if (value instanceof JobStageTimeoutError || value.retryable === true)
        return true;
      const code = safeCode(value);
      if (code && TRANSIENT_CODES.has(code)) return true;
      if (code?.startsWith("08")) return true;
      const sqlState = readSqlState(value);
      if (
        sqlState &&
        (TRANSIENT_CODES.has(sqlState) || sqlState.startsWith("08"))
      )
        return true;
      const status = httpStatus(value);
      if (status && HTTP_RETRIES.has(status)) return true;
      if (
        [
          "DEADLINE_EXCEEDED",
          "INTERNAL",
          "RESOURCE_EXHAUSTED",
          "UNAVAILABLE",
        ].includes(typeof value.status === "string" ? value.status : "")
      )
        return true;
    }
  } catch {
    return false;
  }
  return false;
}

/** Only fixed codes, HTTP numbers and approved stage names leave the worker. */
export type SafeJobErrorDetails = {
  errorCode?: string;
  causeCode?: string;
  sqlState?: string;
  providerStatus?: number;
  failureStage?: string;
};
export function getSafeJobErrorDetails(error: unknown): SafeJobErrorDetails {
  const result: SafeJobErrorDetails = {};
  try {
    for (const value of causes(error)) {
      const code = safeCode(value);
      if (code && !result.errorCode) result.errorCode = code;
      else if (code && code !== result.errorCode) result.causeCode ??= code;
      const status = httpStatus(value);
      if (status && !result.providerStatus) result.providerStatus = status;
      if (typeof value.stage === "string" && STAGES.has(value.stage))
        result.failureStage ??= value.stage;
      const sqlState = readSqlState(value);
      if (sqlState) result.sqlState ??= sqlState;
    }
  } catch {
    /* Diagnostics must never break delivery handling. */
  }
  return result;
}

function* causes(error: unknown): Generator<Record<string, unknown>> {
  const seen = new Set<unknown>();
  let value = error;
  for (
    let depth = 0;
    depth < 4 &&
    value !== null &&
    typeof value === "object" &&
    !seen.has(value);
    depth += 1
  ) {
    seen.add(value);
    yield value as Record<string, unknown>;
    value = "cause" in value ? value.cause : undefined;
  }
}

function safeCode(value: Record<string, unknown>): string | undefined {
  if (value instanceof JobStageTimeoutError) return "JOB_STAGE_TIMEOUT";
  if (
    value instanceof Error &&
    [
      "timeout exceeded when trying to connect",
      "Connection terminated due to connection timeout",
    ].includes(value.message)
  )
    return "DB_CONNECTION_TIMEOUT";
  const code = value.code;
  return typeof code === "string" &&
    (/^P\d{4}$/.test(code) ||
      /^(?:08|22|23|25|28|40|42|53|55|57|58|XX)[0-9A-Z]{3}$/.test(code) ||
      TRANSIENT_CODES.has(code) ||
      code === "MAINTENANCE_RECOVERY_FAILED" ||
      code === "JOB_PUBLISH_FAILED")
    ? code
    : undefined;
}

function httpStatus(value: Record<string, unknown>): number | undefined {
  const metadata = value.$metadata;
  const raw =
    typeof value.code === "number"
      ? value.code
      : typeof value.status === "number"
        ? value.status
        : metadata &&
            typeof metadata === "object" &&
            "httpStatusCode" in metadata
          ? metadata.httpStatusCode
          : undefined;
  return typeof raw === "number" &&
    Number.isInteger(raw) &&
    raw >= 100 &&
    raw <= 599
    ? raw
    : undefined;
}

function readSqlState(value: Record<string, unknown>): string | undefined {
  if (value.code !== "P2010" || !value.meta || typeof value.meta !== "object")
    return undefined;
  const meta = value.meta as Record<string, unknown>;
  const adapter = meta.driverAdapterError;
  const cause =
    adapter && typeof adapter === "object" && "cause" in adapter
      ? adapter.cause
      : undefined;
  const nested =
    cause && typeof cause === "object"
      ? "originalCode" in cause
        ? cause.originalCode
        : "code" in cause
          ? cause.code
          : undefined
      : undefined;
  return [meta.code, nested].find(
    (code): code is string =>
      typeof code === "string" &&
      /^(?:08|22|23|25|28|40|42|53|55|57|58|XX)[0-9A-Z]{3}$/.test(code),
  );
}
