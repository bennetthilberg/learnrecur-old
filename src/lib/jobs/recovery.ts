import "server-only";

import { getPrisma } from "@/lib/prisma";
import { markRefillJobRetryableFailure } from "@/lib/skills/refill-jobs";
import { recoverStaleAgentOperationItems } from "@/lib/agent-access/recovery";
import type { JobEnvelope } from "./contracts";
import type { JobFailureCode } from "./worker";
import { withBoundedActivationRead } from "@/lib/prisma-bounded-read";

/** Called after the delivery ledger has closed execution, including a killed last attempt. */
export async function finalizeFailedJob(
  job: JobEnvelope,
  reason: JobFailureCode,
): Promise<void> {
  if (
    ![
      "JOB_RETRIES_EXHAUSTED",
      "JOB_NON_RETRYABLE",
      "JOB_CONTINUATION_LIMIT_EXCEEDED",
    ].includes(reason)
  )
    return;
  switch (job.name) {
    case "learnrecur/choice-refill.requested":
    case "learnrecur/exact-input-refill.requested":
    case "learnrecur/math-refill.requested":
      await markRefillJobRetryableFailure({
        ...job.data,
        now: new Date(),
        terminal: true,
        error: new Error(
          "Background execution retries ended before refill completion.",
        ),
      });
      await withBoundedActivationRead((tx) =>
        tx.generationJob.updateMany({
          where: {
            id: job.data.generationJobId,
            userId: job.data.userId,
            skillId: job.data.skillId,
            status: "FAILED",
            checkpoint: "retryable-exception",
          },
          data: { checkpoint: "retry-exhausted", completedAt: new Date() },
        }),
      );
      return;
  }
}

// Call only after the delivery ledger grants a retry lease. The eleven-minute
// lease exceeds Lambda's ten-minute execution limit, so the previous process
// cannot still be writing. Completed domain records are deliberately untouched.
export async function recoverInterruptedJob(job: JobEnvelope): Promise<void> {
  switch (job.name) {
    case "learnrecur/choice-refill.requested":
    case "learnrecur/exact-input-refill.requested":
    case "learnrecur/math-refill.requested":
      await markRefillJobRetryableFailure({
        ...job.data,
        now: new Date(),
        error: new Error("Previous worker attempt ended before completion."),
      });
      return;
    case "learnrecur/source-upload-draft.requested": {
      const prisma = getPrisma();
      const where = {
        id: job.data.sourceFileId,
        userId: job.data.userId,
        materialRevisionId: null,
        status: "PROCESSING" as const,
      };
      // Drafts and READY are committed together by the upload handler. Existing
      // skill references in PROCESSING indicate an inconsistent record, which
      // must be inspected instead of generating another set of drafts.
      await prisma.sourceFile.updateMany({
        where: { ...where, skillRefs: { none: {} } },
        data: { status: "UPLOADED" },
      });
      if (await prisma.sourceFile.count({ where })) {
        throw Object.assign(new Error("JOB_SOURCE_RECOVERY_CONFLICT"), {
          retryable: false,
        });
      }
      return;
    }
    case "learnrecur/agent-skill-operation.requested":
      await recoverStaleAgentOperationItems({
        userId: job.data.userId,
        operationId: job.data.operationId,
        now: new Date(),
      });
      return;
  }
}
