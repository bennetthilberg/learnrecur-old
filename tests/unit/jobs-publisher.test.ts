import { beforeEach, describe, expect, it, vi } from "vitest";

const send = vi.hoisted(() => vi.fn());
vi.mock("@aws-sdk/client-sqs", () => ({
  SQSClient: class { send = send; },
  SendMessageCommand: class { constructor(public input: unknown) {} },
}));

import { getJobsConfig, getJobsEnvStatus } from "@/lib/jobs/config";
import { createJobPublisher } from "@/lib/jobs/publisher";
import { getSafeJobErrorDetails } from "@/lib/jobs/errors";
import {
  getJobMessageGroupId,
  MAX_JOB_CONTINUATION_DEPTH,
  parseJobEnvelope,
} from "@/lib/jobs/contracts";
import {
  MAX_JOB_FOLLOW_UPS_PER_DELIVERY,
  withWorkerJobPublicationContext,
} from "@/lib/jobs/publication-context";

const env = {
  JOBS_ENVIRONMENT: "staging", AWS_REGION: "us-east-1",
  JOBS_QUEUE_URL: "https://sqs.us-east-1.amazonaws.com/123456789012/learnrecur-staging-jobs.fifo",
};
const data = { userId: "user-a", operationId: "operation-a", requestedAt: "2026-09-05T01:00:00.000Z" };

describe("AWS job publishing", () => {
  beforeEach(() => {
    send.mockReset().mockResolvedValue({ MessageId: "message-a" });
  });

  it("publishes a validated envelope with stable FIFO and deduplication keys", async () => {
    await createJobPublisher(getJobsConfig(env))("learnrecur/agent-skill-operation.requested", data);
    const command = send.mock.calls[0][0].input;
    const job = parseJobEnvelope(command.MessageBody, "staging");
    expect(command).toMatchObject({ QueueUrl: env.JOBS_QUEUE_URL, MessageGroupId: getJobMessageGroupId(job), MessageDeduplicationId: job.id });
    expect(job.data).toEqual(data);
  });

  it("preserves a continuation ID across ambiguous publisher retries", async () => {
    const eventId = "agent-op-0123456789abcdef0123456789abcdef01234567";
    await withWorkerJobPublicationContext(() => createJobPublisher(getJobsConfig(env))(
      "learnrecur/agent-skill-operation.requested",
      data,
      { id: eventId },
    ));
    const command = send.mock.calls[0][0].input;
    const job = parseJobEnvelope(command.MessageBody, "staging");
    expect(job.id).toBe(eventId);
    expect(job.continuationDepth).toBe(1);
    expect(command.MessageDeduplicationId).toBe(eventId);
  });

  it("passes a stage abort signal to the SQS request", async () => {
    const signal = new AbortController().signal;
    await createJobPublisher(getJobsConfig(env))(
      "learnrecur/agent-skill-operation.requested",
      data,
      { id: "agent-op-0123456789abcdef0123456789abcdef01234567", signal },
    );
    expect(send.mock.calls[0][1]).toEqual({ abortSignal: signal });
  });

  it("stops a runaway continuation at the maximum chain depth", async () => {
    const publisher = createJobPublisher(getJobsConfig(env));
    await expect(withWorkerJobPublicationContext(
      () => publisher("learnrecur/agent-skill-operation.requested", data),
      MAX_JOB_CONTINUATION_DEPTH,
    )).rejects.toMatchObject({
      limit: "depth",
      retryable: false,
      failureCode: "JOB_CONTINUATION_LIMIT_EXCEEDED",
    });
    expect(send).not.toHaveBeenCalled();
  });

  it("limits worker fan-out per delivery and reports the permanent limit", async () => {
    const publisher = createJobPublisher(getJobsConfig(env));
    await withWorkerJobPublicationContext(async () => {
      await Promise.all(Array.from({ length: MAX_JOB_FOLLOW_UPS_PER_DELIVERY }, () =>
        publisher("learnrecur/agent-skill-operation.requested", data),
      ));
      await expect(publisher("learnrecur/agent-skill-operation.requested", data))
        .rejects.toMatchObject({ limit: "fanout", retryable: false });
    });
    expect(send).toHaveBeenCalledTimes(MAX_JOB_FOLLOW_UPS_PER_DELIVERY);
  });

  it("does not contact AWS for invalid payloads", async () => {
    await expect(createJobPublisher(getJobsConfig(env))("learnrecur/agent-skill-operation.requested", { ...data, sourceText: "private" })).rejects.toThrow();
    expect(send).not.toHaveBeenCalled();
  });

  it("surfaces failed enqueue without exposing the provider response", async () => {
    send.mockRejectedValue(new Error("sensitive provider response"));
    await expect(createJobPublisher(getJobsConfig(env))("learnrecur/agent-skill-operation.requested", data)).rejects.toThrow("JOB_PUBLISH_FAILED");
  });

  it("retains approved transport diagnostics behind the public enqueue error", async () => {
    send.mockRejectedValue(Object.assign(new Error("private response body"), { code: "ECONNRESET" }));
    try {
      await createJobPublisher(getJobsConfig(env))("learnrecur/agent-skill-operation.requested", data);
      throw new Error("expected failure");
    } catch (error) {
      expect((error as Error).message).toBe("JOB_PUBLISH_FAILED");
      expect(getSafeJobErrorDetails(error)).toEqual({ errorCode: "JOB_PUBLISH_FAILED", causeCode: "ECONNRESET" });
    }
  });

  it("rejects an enqueue response that lacks a message receipt", async () => {
    send.mockResolvedValue({});
    await expect(createJobPublisher(getJobsConfig(env))("learnrecur/agent-skill-operation.requested", data)).rejects.toThrow("JOB_PUBLISH_FAILED");
  });
});

describe("AWS environment isolation", () => {
  it("derives the exact source ARN from the queue URL", () => {
    expect(getJobsConfig(env)).toEqual({ environment: "staging", region: "us-east-1", queueUrl: env.JOBS_QUEUE_URL, queueArn: "arn:aws:sqs:us-east-1:123456789012:learnrecur-staging-jobs.fifo" });
    expect(getJobsEnvStatus(env).status).toBe("configured");
  });

  it.each([
    { JOBS_ENVIRONMENT: undefined },
    { JOBS_ENVIRONMENT: "production" },
    { AWS_REGION: "us-west-2" },
    { JOBS_QUEUE_URL: "http://localhost:9324/123456789012/learnrecur-staging-jobs.fifo" },
    { JOBS_QUEUE_URL: env.JOBS_QUEUE_URL + "?redirect=somewhere" },
    { LEARNRECUR_DEPLOYMENT_TIER: "production" },
    { VERCEL_ENV: "production" },
  ])("fails closed for invalid or mismatched environment configuration", (overrides) => {
    expect(() => getJobsConfig({ ...env, ...overrides })).toThrow();
    expect(getJobsEnvStatus({ ...env, ...overrides }).status).toBe("missing-env");
  });

  it("accepts explicitly isolated staging deployed to a Vercel production slot", () => {
    expect(getJobsEnvStatus({ ...env, LEARNRECUR_DEPLOYMENT_TIER: "staging", VERCEL_ENV: "production" }).status).toBe("configured");
  });

  it("keeps the worker publication context isolated from concurrent caller work", async () => {
    send.mockReset().mockResolvedValue({ MessageId: "message-a" });
    const publisher = createJobPublisher(getJobsConfig(env));
    await Promise.all([
      withWorkerJobPublicationContext(async () => {
        await publisher("learnrecur/agent-skill-operation.requested", data);
      }),
      publisher("learnrecur/agent-skill-operation.requested", data),
    ]);

    expect(send).toHaveBeenCalledTimes(2);
    const publishedJobs = send.mock.calls.map(([command]) =>
      parseJobEnvelope(command.input.MessageBody, "staging"),
    );
    expect(publishedJobs.map((job) => job.continuationDepth).sort()).toEqual([1, undefined]);
  });
});
