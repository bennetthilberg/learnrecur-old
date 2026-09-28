import "server-only";
import { createHash } from "node:crypto";
import {
  ChangeMessageVisibilityCommand,
  GetQueueAttributesCommand,
  SendMessageCommand,
  SQSClient,
} from "@aws-sdk/client-sqs";
import type { SQSRecord } from "aws-lambda";
import type { JobsConfig } from "./config";
import type { JobFailureCode } from "./worker";
import { withAbortableTimeout } from "./deadline";

export function createJobTransport(config: JobsConfig) {
  const client = new SQSClient({ region: config.region, maxAttempts: 3 });
  return {
    async deadLetter(record: SQSRecord, reason: JobFailureCode) {
      const group = record.attributes.MessageGroupId ?? "invalid";
      const response = await client.send(
        new SendMessageCommand({
          QueueUrl: config.queueUrl.replace("jobs.fifo", "jobs-dlq.fifo"),
          MessageBody: record.body,
          MessageGroupId: /^[a-f0-9]{64}$/.test(group)
            ? group
            : createHash("sha256").update(group).digest("hex"),
          MessageDeduplicationId: createHash("sha256")
            .update(record.messageId)
            .digest("hex"),
          MessageAttributes: {
            failureCode: { DataType: "String", StringValue: reason },
          },
        }),
      );
      if (!response.MessageId) throw new Error("JOB_DLQ_RECEIPT_MISSING");
    },
    async retry(record: SQSRecord, delaySeconds: number) {
      await withAbortableTimeout({
        timeoutMs: 5_000,
        stage: "job-visibility",
        message: "Queue visibility could not be updated in time.",
        run: (signal) =>
          client.send(
            new ChangeMessageVisibilityCommand({
              QueueUrl: config.queueUrl,
              ReceiptHandle: record.receiptHandle,
              VisibilityTimeout: Math.min(900, Math.max(1, delaySeconds)),
            }),
            { abortSignal: signal },
          ),
      });
    },
    async probe(signal?: AbortSignal) {
      const response = await client.send(
        new GetQueueAttributesCommand({
          QueueUrl: config.queueUrl,
          AttributeNames: ["QueueArn", "FifoQueue"],
        }),
        { abortSignal: signal },
      );
      if (
        response.Attributes?.QueueArn !== config.queueArn ||
        response.Attributes?.FifoQueue !== "true"
      ) {
        throw new Error("JOB_QUEUE_NOT_READY");
      }
    },
  };
}
