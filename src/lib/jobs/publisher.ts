import "server-only";
import { SendMessageCommand, SQSClient } from "@aws-sdk/client-sqs";

import { buildJobEnvelope, getJobMessageGroupId } from "./contracts";
import { getJobsConfig, type JobsConfig } from "./config";
import { reserveWorkerJobFollowUp } from "./publication-context";

export function createJobPublisher(config: JobsConfig) {
  const client = new SQSClient({ region: config.region, maxAttempts: 3 });
  return async (
    name: string,
    data: unknown,
    options?: { id?: string; signal?: AbortSignal },
  ): Promise<void> => {
    const continuationDepth = reserveWorkerJobFollowUp();
    const job = buildJobEnvelope(name, data, config.environment, options?.id, continuationDepth);
    try {
      const response = await client.send(new SendMessageCommand({
        QueueUrl: config.queueUrl,
        MessageBody: JSON.stringify(job),
        MessageGroupId: getJobMessageGroupId(job),
        // The SDK retries this command with the same ID; ambiguous network
        // responses cannot enqueue a second copy within SQS's deduplication window.
        MessageDeduplicationId: job.id,
      }), options?.signal ? { abortSignal: options.signal } : undefined);
      if (!response.MessageId) throw new Error("JOB_RECEIPT_MISSING");
    } catch (cause) {
      throw Object.assign(new Error("JOB_PUBLISH_FAILED", { cause }), { code: "JOB_PUBLISH_FAILED" });
    }
  };
}

let cached: { key: string; publish: ReturnType<typeof createJobPublisher> } | undefined;
export async function publishJob(
  name: string,
  data: unknown,
  options?: { id?: string; signal?: AbortSignal },
): Promise<void> {
  const config = getJobsConfig();
  const key = JSON.stringify(config);
  if (cached?.key !== key) cached = { key, publish: createJobPublisher(config) };
  await cached.publish(name, data, options);
}
