import { beforeEach, describe, expect, it, vi } from "vitest";
import type { Context, SQSEvent } from "aws-lambda";

const mocks = vi.hoisted(() => ({ load: vi.fn(), run: vi.fn(), execute: vi.fn(), purge: vi.fn() }));
vi.mock("@/lib/prisma", () => ({ getPrisma: () => ({}) }));
vi.mock("@/lib/jobs/config", () => ({ getJobsConfig: () => ({ environment: "staging", region: "us-east-1", queueArn: "fixture", queueUrl: "fixture" }) }));
vi.mock("@/lib/jobs/environment", () => ({ loadWorkerEnvironment: mocks.load }));
vi.mock("@/lib/jobs/delivery-store", () => ({ createJobDeliveryStore: () => ({ purgeExpired: mocks.purge }) }));
vi.mock("@/lib/jobs/transport", () => ({ createJobTransport: () => ({}) }));
vi.mock("@/lib/jobs/worker", () => ({ createJobWorker: () => mocks.run }));
vi.mock("@/lib/jobs/dispatch", () => ({ executeJob: mocks.execute }));
vi.mock("@/lib/jobs/recovery", () => ({ finalizeFailedJob: vi.fn() }));

const event = { Records: [] } as SQSEvent;
const context = () => ({ callbackWaitsForEmptyEventLoop: true } as Context);
beforeEach(() => { vi.resetModules(); vi.clearAllMocks(); mocks.load.mockResolvedValue(undefined); mocks.run.mockResolvedValue({ batchItemFailures: [] }); });

describe("Lambda entry point", () => {
  it("loads secrets once across warm invocations and releases the event loop", async () => {
    const { handler } = await import("@/lib/jobs/lambda");
    const firstContext = context();
    await expect(handler(event, firstContext)).resolves.toEqual({ batchItemFailures: [] });
    await handler(event, context());
    expect(firstContext.callbackWaitsForEmptyEventLoop).toBe(false);
    expect(mocks.load).toHaveBeenCalledTimes(1);
    expect(mocks.run).toHaveBeenCalledTimes(2);
  });

  it("retries initialization after a transient secret-store failure without logging secret content", async () => {
    const log = vi.spyOn(console, "error").mockImplementation(() => {});
    mocks.load.mockRejectedValueOnce(new Error("sensitive database URL"));
    const { handler } = await import("@/lib/jobs/lambda");
    try {
      await expect(handler(event, context())).rejects.toThrow("JOB_WORKER_UNAVAILABLE");
      expect(mocks.run).not.toHaveBeenCalled();
      await expect(handler(event, context())).resolves.toEqual({ batchItemFailures: [] });
      expect(mocks.load).toHaveBeenCalledTimes(2);
      expect(JSON.stringify(log.mock.calls)).not.toContain("sensitive database URL");
    } finally { log.mockRestore(); }
  });

  it("preserves partial batch failures from the worker", async () => {
    mocks.run.mockResolvedValueOnce({ batchItemFailures: [{ itemIdentifier: "retry-this-delivery" }] });
    const { handler } = await import("@/lib/jobs/lambda");
    await expect(handler(event, context())).resolves.toEqual({ batchItemFailures: [{ itemIdentifier: "retry-this-delivery" }] });
  });
});
