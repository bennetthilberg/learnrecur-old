import type { Context, SQSEvent, SQSBatchResponse } from "aws-lambda";
import { getPrisma } from "@/lib/prisma";
import { getJobsConfig } from "./config";
import { createJobDeliveryStore } from "./delivery-store";
import { loadWorkerEnvironment } from "./environment";
import { createJobTransport } from "./transport";
import { createJobWorker } from "./worker";
import { JOB_LEASE_SECONDS } from "./timing";

export { JOB_LEASE_SECONDS, JOB_TIMEOUT_SECONDS } from "./timing";

export async function createRuntimeWorker() {
  const config = getJobsConfig();
  const delivery = createJobDeliveryStore({ prisma: getPrisma(), now: () => new Date(), leaseSeconds: JOB_LEASE_SECONDS });
  const transport = createJobTransport(config);
  // Delay domain imports until the SSM environment has been loaded.
  const { executeJob } = await import("./dispatch");
  const { finalizeFailedJob } = await import("./recovery");
  return createJobWorker({
    ...config, ...delivery, ...transport,
    now: () => new Date(),
    finalize: finalizeFailedJob,
    async execute(job, context) {
      const result = await executeJob(job, context);
      if (job.name === "learnrecur/agent-access.maintenance") await delivery.purgeExpired();
      return result;
    },
    log(event) {
      console.info(JSON.stringify({ component: "background-jobs", environment: config.environment, ...event }));
    },
  });
}

let initialized: Promise<Awaited<ReturnType<typeof createRuntimeWorker>>> | undefined;
export async function handler(event: SQSEvent, context: Context): Promise<SQSBatchResponse> {
  context.callbackWaitsForEmptyEventLoop = false;
  try {
    initialized ??= loadWorkerEnvironment(getJobsConfig()).then(createRuntimeWorker).catch(() => {
      initialized = undefined;
      throw new Error("JOB_WORKER_INITIALIZATION_FAILED");
    });
    const worker = await initialized;
    return await worker(event);
  } catch {
    console.error(JSON.stringify({ component: "background-jobs", outcome: "JOB_WORKER_UNAVAILABLE" }));
    throw new Error("JOB_WORKER_UNAVAILABLE");
  }
}
