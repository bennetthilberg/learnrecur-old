import { describe, expect, it } from "vitest";
import { getSafeJobErrorDetails, isTransientJobError } from "@/lib/jobs/errors";
import { JobStageTimeoutError } from "@/lib/jobs/deadline";

describe("job failure classification and redacted diagnostics", () => {
  it.each([429, 500, 502, 503, 504])("retries HTTP %i", (status) => {
    expect(isTransientJobError({ $metadata: { httpStatusCode: status } })).toBe(
      true,
    );
    expect(
      getSafeJobErrorDetails({
        code: status,
        message: "private provider body",
      }),
    ).toEqual({ providerStatus: status });
  });
  it.each([
    "ECONNRESET",
    "ETIMEDOUT",
    "EAI_AGAIN",
    "P2024",
    "P2028",
    "P2034",
    "40001",
    "40P01",
    "57014",
  ])("retries %s through a wrapped cause", (code) => {
    const error = new Error("private wrapper", {
      cause: Object.assign(new Error("private SQL"), { code }),
    });
    expect(isTransientJobError(error)).toBe(true);
    expect(getSafeJobErrorDetails(error)).toEqual({ errorCode: code });
  });
  it.each([400, 401, 403, 404, 422])("does not retry HTTP %i", (code) =>
    expect(isTransientJobError({ code })).toBe(false),
  );
  it("does not override an explicit permanent outcome", () =>
    expect(isTransientJobError({ retryable: false, code: 503 })).toBe(false));
  it("bounds cyclic cause traversal and ignores arbitrary codes and stages", () => {
    const error: { code: string; stage: string; cause?: unknown } = {
      code: "private-source-text",
      stage: "private-document",
    };
    error.cause = error;
    expect(getSafeJobErrorDetails(error)).toEqual({});
    expect(isTransientJobError(error)).toBe(false);
  });
  it("reports a stage timeout without logging its private message", () => {
    const error = new JobStageTimeoutError(
      "private SQL",
      "refill-source-evidence",
    );
    expect(isTransientJobError(error)).toBe(true);
    expect(getSafeJobErrorDetails(error)).toEqual({
      errorCode: "JOB_STAGE_TIMEOUT",
      failureStage: "refill-source-evidence",
    });
  });
  it("recognizes a canceled SQL statement behind Prisma's raw-query error", () => {
    expect(
      isTransientJobError({ code: "P2010", meta: { code: "57014" } }),
    ).toBe(true);
  });
  it.each([
    "UND_ERR_CONNECT_TIMEOUT",
    "UND_ERR_HEADERS_TIMEOUT",
    "UND_ERR_BODY_TIMEOUT",
    "EPIPE",
  ])("retries transport failure %s", (code) =>
    expect(isTransientJobError({ code })).toBe(true),
  );
  it("does not let a hostile diagnostic getter fail the delivery logger", () => {
    const error = {
      get code() {
        throw new Error("private getter");
      },
    };
    expect(getSafeJobErrorDetails(error)).toEqual({});
  });
  it("retains the approved code behind a maintenance wrapper", () => {
    const error = Object.assign(
      new Error("private maintenance detail", { cause: { code: "P2028" } }),
      { code: "MAINTENANCE_RECOVERY_FAILED" },
    );
    expect(getSafeJobErrorDetails(error)).toEqual({
      errorCode: "MAINTENANCE_RECOVERY_FAILED",
      causeCode: "P2028",
    });
  });
});
