import { describe, expect, it, vi } from "vitest";
import type { SQSRecord } from "aws-lambda";
import { buildJobEnvelope, getJobMessageGroupId } from "@/lib/jobs/contracts";
import { createJobWorker, type JobWorkerDependencies } from "@/lib/jobs/worker";
import { JobContinuationLimitError } from "@/lib/jobs/publication-context";

const queueArn = "arn:aws:sqs:us-east-1:123456789012:learnrecur-staging-jobs.fifo";
function record(overrides: Partial<SQSRecord> = {}): SQSRecord {
  const job = buildJobEnvelope("learnrecur/choice-refill.requested", {
    userId: "user-a", skillId: "skill-a", generationJobId: "job-a",
    targetReadyCount: 5, requestedAt: "2026-09-04T21:00:00.000Z",
  }, "staging");
  return {
    messageId: "message-a", receiptHandle: "receipt-a", body: JSON.stringify(job),
    attributes: { ApproximateReceiveCount: "1", SentTimestamp: "1", SenderId: "sender", ApproximateFirstReceiveTimestamp: "1", MessageGroupId: getJobMessageGroupId(job) },
    messageAttributes: {}, md5OfBody: "", eventSource: "aws:sqs", eventSourceARN: queueArn,
    awsRegion: "us-east-1", ...overrides,
  };
}

function maintenanceRecord(): SQSRecord {
  const job = buildJobEnvelope("learnrecur/agent-access.maintenance", {
    requestedAt: "2026-09-04T21:00:00.000Z",
  }, "staging");
  const base = record();
  return {
    ...base,
    body: JSON.stringify(job),
    attributes: { ...base.attributes, MessageGroupId: getJobMessageGroupId(job) },
  };
}

function setup() {
  const dependencies: JobWorkerDependencies = {
    environment: "staging", queueArn,
    claim: vi.fn().mockResolvedValue({ status: "claimed", attempt: 1, token: "lease" }),
    complete: vi.fn().mockResolvedValue(undefined),
    fail: vi.fn().mockResolvedValue(undefined),
    execute: vi.fn().mockResolvedValue(undefined),
    deadLetter: vi.fn().mockResolvedValue(undefined),
    finalize: vi.fn().mockResolvedValue(undefined),
    retry: vi.fn().mockResolvedValue(undefined),
    log: vi.fn(),
    now: () => new Date("2026-09-04T21:00:00.000Z"),
  };
  return { dependencies, run: createJobWorker(dependencies) };
}

describe("SQS worker delivery safety", () => {
  it("finalizes exhausted domain work before acknowledging a dead letter", async () => {
    const { dependencies, run } = setup();
    vi.mocked(dependencies.claim).mockResolvedValue({ status: "dead-letter", reason: "JOB_RETRIES_EXHAUSTED" });
    await run({ Records: [record()] });
    expect(dependencies.finalize).toHaveBeenCalledWith(expect.objectContaining({ name: "learnrecur/choice-refill.requested" }), "JOB_RETRIES_EXHAUSTED");
    expect(vi.mocked(dependencies.finalize).mock.invocationCallOrder[0]).toBeLessThan(vi.mocked(dependencies.deadLetter).mock.invocationCallOrder[0]);
    expect(dependencies.execute).not.toHaveBeenCalled();
  });

  it("keeps a terminal message available if domain finalization fails", async () => {
    const { dependencies, run } = setup();
    vi.mocked(dependencies.claim).mockResolvedValue({ status: "dead-letter", reason: "JOB_RETRIES_EXHAUSTED" });
    vi.mocked(dependencies.finalize).mockRejectedValue({ code: "P2028" });
    expect(await run({ Records: [record()] })).toEqual({ batchItemFailures: [{ itemIdentifier: "message-a" }] });
    expect(dependencies.deadLetter).not.toHaveBeenCalled();
  });
  it("acknowledges only after execution and durable completion", async () => {
    const { dependencies, run } = setup();
    expect(await run({ Records: [record()] })).toEqual({ batchItemFailures: [] });
    expect(dependencies.execute).toHaveBeenCalledWith(
      expect.objectContaining({ name: "learnrecur/choice-refill.requested" }),
      expect.objectContaining({ attempt: 0, maxAttempts: 3, deadlineAt: expect.any(Date) }),
    );
    expect(dependencies.complete).toHaveBeenCalledOnce();
    expect(dependencies.deadLetter).not.toHaveBeenCalled();
  });

  it("logs only approved maintenance counters with a successful maintenance delivery", async () => {
    const { dependencies, run } = setup();
    vi.mocked(dependencies.execute).mockResolvedValue({
      purgedPayloads: 2,
      purgedRateBuckets: 3,
      expiredUploadOperations: 4,
      revocationsAttempted: 5,
      revocationsFailed: 0,
      refillEventsAttempted: 6,
      refillEventsDelivered: 6,
      refillEventsFailed: 0,
      activationItemsScanned: 7,
      activationItemsRequeued: 1,
      activationItemsFinalized: 2,
      activationItemsWaiting: 3,
      activationLegacyItemsPromoted: 1,
      activationContinuations: 1,
      activationContinuationPublishFailures: 0,
      privateSourceText: "must not be logged",
    });

    expect(await run({ Records: [maintenanceRecord()] })).toEqual({ batchItemFailures: [] });
    expect(dependencies.log).toHaveBeenCalledWith(expect.objectContaining({
      outcome: "completed",
      name: "learnrecur/agent-access.maintenance",
      purgedPayloads: 2,
      purgedRateBuckets: 3,
      expiredUploadOperations: 4,
      revocationsAttempted: 5,
      revocationsFailed: 0,
      refillEventsAttempted: 6,
      refillEventsDelivered: 6,
      refillEventsFailed: 0,
      activationItemsScanned: 7,
      activationItemsRequeued: 1,
      activationItemsFinalized: 2,
      activationItemsWaiting: 3,
      activationLegacyItemsPromoted: 1,
      activationContinuations: 1,
      activationContinuationPublishFailures: 0,
    }));
    expect(JSON.stringify(vi.mocked(dependencies.log).mock.calls)).not.toContain("must not be logged");
    expect(dependencies.complete).toHaveBeenCalledOnce();
  });

  it("skips a durably completed duplicate", async () => {
    const { dependencies, run } = setup();
    vi.mocked(dependencies.claim).mockResolvedValue({ status: "complete" });
    expect(await run({ Records: [record()] })).toEqual({ batchItemFailures: [] });
    expect(dependencies.execute).not.toHaveBeenCalled();
  });

  it("retries an active lease instead of losing a duplicate after its original fails", async () => {
    const { dependencies, run } = setup();
    vi.mocked(dependencies.claim).mockResolvedValue({ status: "busy", retryAfterSeconds: 661 });
    expect(await run({ Records: [record()] })).toEqual({ batchItemFailures: [{ itemIdentifier: "message-a" }] });
    expect(dependencies.execute).not.toHaveBeenCalled();
    expect(dependencies.retry).toHaveBeenCalledWith(expect.anything(), 661);
  });

  it("retries transient errors with bounded delay and without logging private error text", async () => {
    const { dependencies, run } = setup();
    vi.mocked(dependencies.execute).mockRejectedValue(new Error("private study material and token"));
    expect(await run({ Records: [record()] })).toEqual({ batchItemFailures: [{ itemIdentifier: "message-a" }] });
    expect(dependencies.fail).toHaveBeenCalledWith(expect.anything(), "lease", "JOB_EXECUTION_FAILED", false);
    expect(dependencies.retry).toHaveBeenCalledWith(expect.anything(), 30);
    expect(JSON.stringify(vi.mocked(dependencies.log).mock.calls)).not.toMatch(/private study material|token/);
  });

  it("sends permanent failures to the DLQ before acknowledging", async () => {
    const { dependencies, run } = setup();
    vi.mocked(dependencies.execute).mockRejectedValue(Object.assign(new Error("permanent"), { retryable: false }));
    expect(await run({ Records: [record()] })).toEqual({ batchItemFailures: [] });
    expect(dependencies.deadLetter).toHaveBeenCalledWith(expect.anything(), "JOB_NON_RETRYABLE");
    expect(dependencies.fail).toHaveBeenCalledWith(expect.anything(), "lease", "JOB_NON_RETRYABLE", true);
  });
  it("shortens claimed visibility before execution while keeping it beyond the worker's hard lifetime", async () => {
    const { dependencies, run } = setup();
    await run({ Records: [record()] });
    expect(dependencies.retry).toHaveBeenCalledWith(expect.anything(), 720);
    expect(vi.mocked(dependencies.retry).mock.invocationCallOrder[0]).toBeLessThan(vi.mocked(dependencies.execute).mock.invocationCallOrder[0]);
  });
  it("does not execute if setting claimed visibility fails", async () => {
    const { dependencies, run } = setup();
    vi.mocked(dependencies.retry).mockRejectedValueOnce(new Error("SQS unavailable"));
    expect(await run({ Records: [record()] })).toEqual({ batchItemFailures: [{ itemIdentifier: "message-a" }] });
    expect(dependencies.execute).not.toHaveBeenCalled();
    expect(dependencies.fail).not.toHaveBeenCalled();
  });

  it("includes approved execution diagnostics without private exception text", async () => {
    const { dependencies, run } = setup();
    vi.mocked(dependencies.execute).mockRejectedValue(Object.assign(new Error("private SQL and token"), { code: "P2028" }));
    await run({ Records: [record()] });
    expect(dependencies.log).toHaveBeenCalledWith(expect.objectContaining({ outcome: "JOB_EXECUTION_FAILED", phase: "execute", errorCode: "P2028" }));
    expect(JSON.stringify(vi.mocked(dependencies.log).mock.calls)).not.toMatch(/private SQL|token/);
  });

  it("records a continuation safety-limit breach as a terminal, visible DLQ outcome", async () => {
    const { dependencies, run } = setup();
    vi.mocked(dependencies.execute).mockRejectedValue(new JobContinuationLimitError("depth"));
    expect(await run({ Records: [record()] })).toEqual({ batchItemFailures: [] });
    expect(dependencies.fail).toHaveBeenCalledWith(
      expect.anything(),
      "lease",
      "JOB_CONTINUATION_LIMIT_EXCEEDED",
      true,
    );
    expect(dependencies.deadLetter).toHaveBeenCalledWith(
      expect.anything(),
      "JOB_CONTINUATION_LIMIT_EXCEEDED",
    );
    expect(dependencies.log).toHaveBeenCalledWith(expect.objectContaining({
      outcome: "JOB_CONTINUATION_LIMIT_EXCEEDED",
    }));
  });

  it("quarantines a retryable failure on its last allowed execution", async () => {
    const { dependencies, run } = setup();
    vi.mocked(dependencies.claim).mockResolvedValue({ status: "claimed", attempt: 3, token: "lease" });
    vi.mocked(dependencies.execute).mockRejectedValue(new Error("outage"));
    expect(await run({ Records: [record()] })).toEqual({ batchItemFailures: [] });
    expect(dependencies.deadLetter).toHaveBeenCalledWith(expect.anything(), "JOB_RETRIES_EXHAUSTED");
  });

  it("does not acknowledge when DLQ persistence fails", async () => {
    const { dependencies, run } = setup();
    vi.mocked(dependencies.execute).mockRejectedValue(Object.assign(new Error("permanent"), { retryable: false }));
    vi.mocked(dependencies.deadLetter).mockRejectedValue(new Error("SQS unavailable"));
    expect(await run({ Records: [record()] })).toEqual({ batchItemFailures: [{ itemIdentifier: "message-a" }] });
  });

  it("does not acknowledge when the database claim fails", async () => {
    const { dependencies, run } = setup();
    vi.mocked(dependencies.claim).mockRejectedValue(Object.assign(new Error("private study material"), { code: "P1001" }));
    expect(await run({ Records: [record()] })).toEqual({ batchItemFailures: [{ itemIdentifier: "message-a" }] });
    expect(dependencies.execute).not.toHaveBeenCalled();
    expect(dependencies.retry).toHaveBeenCalledWith(expect.anything(), 30);
    expect(dependencies.log).toHaveBeenCalledWith(expect.objectContaining({
      outcome: "JOB_DELIVERY_FAILED",
      name: "learnrecur/choice-refill.requested",
      id: expect.any(String),
      phase: "claim",
      errorCode: "P1001",
    }));
    expect(JSON.stringify(vi.mocked(dependencies.log).mock.calls)).not.toContain("private study material");
  });

  it.each([
    { meta: { code: "57P01", message: "private query and password" }, sqlState: "57P01" },
    { meta: { driverAdapterError: { cause: { originalCode: "08006", message: "private source text" } } }, sqlState: "08006" },
    { meta: { driverAdapterError: { cause: { code: "53300", message: "private source text" } } }, sqlState: "53300" },
    { meta: { code: "TOKEN", driverAdapterError: { cause: { originalCode: "private" } } }, sqlState: undefined },
  ])("logs a safe PostgreSQL code from a failed Prisma claim", async ({ meta, sqlState }) => {
    const { dependencies, run } = setup();
    vi.mocked(dependencies.claim).mockRejectedValue(Object.assign(new Error("private material"), {
      code: "P2010", meta,
    }));

    expect(await run({ Records: [record()] })).toEqual({
      batchItemFailures: [{ itemIdentifier: "message-a" }],
    });
    expect(dependencies.log).toHaveBeenCalledWith(expect.objectContaining({
      outcome: "JOB_DELIVERY_FAILED", phase: "claim", errorCode: "P2010", sqlState,
    }));
    expect(JSON.stringify(vi.mocked(dependencies.log).mock.calls)).not.toMatch(/private|TOKEN/);
  });

  it("retries a pool connection timeout before the one-hour queue visibility expires", async () => {
    const { dependencies, run } = setup();
    vi.mocked(dependencies.claim).mockRejectedValue(new Error("Connection terminated due to connection timeout"));
    const failed = record({
      attributes: { ...record().attributes, ApproximateReceiveCount: "4" },
    });
    expect(await run({ Records: [failed] })).toEqual({
      batchItemFailures: [{ itemIdentifier: "message-a" }],
    });
    expect(dependencies.retry).toHaveBeenCalledWith(failed, 240);
    expect(dependencies.log).toHaveBeenCalledWith(expect.objectContaining({
      outcome: "JOB_DELIVERY_FAILED",
      phase: "claim",
      errorCode: "DB_CONNECTION_TIMEOUT",
    }));
  });

  it("keeps a failed delivery unacknowledged when rescheduling also fails", async () => {
    const { dependencies, run } = setup();
    vi.mocked(dependencies.claim).mockRejectedValue(new Error("timeout exceeded when trying to connect"));
    vi.mocked(dependencies.retry).mockRejectedValue(new Error("private SQS response"));
    expect(await run({ Records: [record()] })).toEqual({
      batchItemFailures: [{ itemIdentifier: "message-a" }],
    });
    expect(dependencies.log).toHaveBeenCalledWith(expect.objectContaining({
      outcome: "JOB_RETRY_SCHEDULE_FAILED",
      phase: "claim",
    }));
    expect(JSON.stringify(vi.mocked(dependencies.log).mock.calls)).not.toContain("private SQS response");
  });

  it.each([
    { count: "invalid", delay: 30 },
    { count: "0", delay: 30 },
    { count: "999999999999999999999", delay: 30 },
    { count: "30", delay: 900 },
  ])("bounds infrastructure retry for receive count $count", async ({ count, delay }) => {
    const { dependencies, run } = setup();
    vi.mocked(dependencies.claim).mockRejectedValue(new Error("database unavailable"));
    const failed = record({
      attributes: { ...record().attributes, ApproximateReceiveCount: count },
    });
    expect(await run({ Records: [failed] })).toEqual({
      batchItemFailures: [{ itemIdentifier: "message-a" }],
    });
    expect(dependencies.retry).toHaveBeenCalledWith(failed, delay);
  });

  it("does not log arbitrary provider error codes", async () => {
    const { dependencies, run } = setup();
    vi.mocked(dependencies.claim).mockRejectedValue(Object.assign(new Error("private"), {
      code: "TOKEN",
    }));
    expect(await run({ Records: [record()] })).toEqual({ batchItemFailures: [{ itemIdentifier: "message-a" }] });
    expect(JSON.stringify(vi.mocked(dependencies.log).mock.calls)).not.toContain("TOKEN");
  });

  it("does not rerun business logic after completion persistence fails", async () => {
    const { dependencies, run } = setup();
    vi.mocked(dependencies.complete).mockRejectedValue(new Error("database unavailable"));
    expect(await run({ Records: [record()] })).toEqual({ batchItemFailures: [{ itemIdentifier: "message-a" }] });
    // Keep the running lease; the domain's own idempotency handles eventual recovery.
    expect(dependencies.fail).not.toHaveBeenCalled();
    expect(dependencies.log).toHaveBeenCalledWith(expect.objectContaining({
      outcome: "JOB_DELIVERY_FAILED",
      phase: "complete",
    }));
  });

  it.each([
    { body: "{invalid" },
    { body: " ".repeat(65_537) },
    { attributes: { ...record().attributes, MessageGroupId: "wrong-partition" } },
  ])("quarantines malformed or wrongly partitioned messages", async (overrides) => {
    const { dependencies, run } = setup();
    expect(await run({ Records: [record(overrides)] })).toEqual({ batchItemFailures: [] });
    expect(dependencies.claim).not.toHaveBeenCalled();
    expect(dependencies.deadLetter).toHaveBeenCalledWith(expect.anything(), "JOB_INVALID_MESSAGE");
  });

  it("rejects an unexpected source queue without acting on the message", async () => {
    const { dependencies, run } = setup();
    expect(await run({ Records: [record({ eventSourceARN: queueArn.replace("staging", "production") })] }))
      .toEqual({ batchItemFailures: [{ itemIdentifier: "message-a" }] });
    expect(dependencies.claim).not.toHaveBeenCalled();
    expect(dependencies.deadLetter).not.toHaveBeenCalled();
    expect(dependencies.retry).not.toHaveBeenCalled();
  });

  it("stops after failure and returns all remaining FIFO records unprocessed", async () => {
    const { dependencies, run } = setup();
    vi.mocked(dependencies.execute).mockRejectedValue(new Error("outage"));
    expect(await run({ Records: [record(), record({ messageId: "message-b" })] }))
      .toEqual({ batchItemFailures: [{ itemIdentifier: "message-a" }, { itemIdentifier: "message-b" }] });
    expect(dependencies.execute).toHaveBeenCalledOnce();
  });
});
