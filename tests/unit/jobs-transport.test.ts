import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { SQSRecord } from "aws-lambda";
const send = vi.hoisted(() => vi.fn());
vi.mock("@aws-sdk/client-sqs", () => ({
  SQSClient: class { send = send; },
  SendMessageCommand: class { constructor(public input: unknown) {} },
  ChangeMessageVisibilityCommand: class { constructor(public input: unknown) {} },
  GetQueueAttributesCommand: class { constructor(public input: unknown) {} },
}));
import { createJobTransport } from "@/lib/jobs/transport";
import { getJobsConfig } from "@/lib/jobs/config";

const config = getJobsConfig({ AWS_REGION: "us-east-1", JOBS_ENVIRONMENT: "staging", JOBS_QUEUE_URL: "https://sqs.us-east-1.amazonaws.com/123456789012/learnrecur-staging-jobs.fifo" });
const record = { messageId: "message-a", receiptHandle: "receipt-a", body: "invalid JSON", attributes: { MessageGroupId: "a".repeat(64) } } as SQSRecord;

describe("SQS transport", () => {
  beforeEach(() => { send.mockReset(); });
  afterEach(() => vi.useRealTimers());
  it("aborts a stuck visibility change instead of consuming the invocation", async () => {
    vi.useFakeTimers();
    send.mockImplementation(() => new Promise(() => {}));
    const work = createJobTransport(config).retry(record, 720);
    const rejected = expect(work).rejects.toMatchObject({ stage: "job-visibility", retryable: true });
    await vi.advanceTimersByTimeAsync(5_000);
    await rejected;
    expect(send.mock.calls[0][1].abortSignal.aborted).toBe(true);
  });
  it("persists the original message and safe failure category in the matching DLQ", async () => {
    send.mockResolvedValue({ MessageId: "dead-letter-a" });
    await createJobTransport(config).deadLetter(record, "JOB_INVALID_MESSAGE");
    expect(send.mock.calls[0][0].input).toMatchObject({
      QueueUrl: config.queueUrl.replace("jobs.fifo", "jobs-dlq.fifo"),
      MessageBody: record.body, MessageGroupId: record.attributes.MessageGroupId,
      MessageAttributes: { failureCode: { DataType: "String", StringValue: "JOB_INVALID_MESSAGE" } },
    });
  });
  it("cannot acknowledge a DLQ write without a receipt", async () => {
    send.mockResolvedValue({});
    await expect(createJobTransport(config).deadLetter(record, "JOB_INVALID_MESSAGE")).rejects.toThrow();
  });
  it("changes visibility on the source receipt for bounded retries", async () => {
    send.mockResolvedValue({});
    await createJobTransport(config).retry(record, 60);
    expect(send.mock.calls[0][0].input).toEqual({ QueueUrl: config.queueUrl, ReceiptHandle: record.receiptHandle, VisibilityTimeout: 60 });
  });
  it.each([{}, { QueueArn: config.queueArn, FifoQueue: "false" }, { QueueArn: config.queueArn.replace("staging", "production"), FifoQueue: "true" }])("fails readiness when AWS queue identity or FIFO configuration is wrong", async (Attributes) => {
    send.mockResolvedValue({ Attributes });
    await expect(createJobTransport(config).probe()).rejects.toThrow("JOB_QUEUE_NOT_READY");
  });
  it("proves the authenticated queue read with the expected identity", async () => {
    send.mockResolvedValue({ Attributes: { QueueArn: config.queueArn, FifoQueue: "true" } });
    await expect(createJobTransport(config).probe()).resolves.toBeUndefined();
  });
});
