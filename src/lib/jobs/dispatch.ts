import "server-only";

import { recoverRetryableAccountDeletionJobs, runAccountDeletionJob } from "@/lib/account-deletion";
import { runAgentAccessMaintenance, runAgentConnectionRevocationJob } from "@/lib/agent-access/settings";
import { runAgentSkillOperationJob } from "@/lib/agent-access/worker";
import { runMaterialIngestionJob } from "@/lib/materials/ingestion";
import { runMaterialCleanupJob } from "@/lib/materials/cleanup";
import { runMaterialBatchActivationJob, runMaterialDraftItemJob } from "@/lib/materials/batches";
import { processDueReminderBatch, resolveClerkReminderAccountEmail } from "@/lib/reminders";
import { markRefillJobRetryableFailure, runChoiceExerciseRefillJob, runExactInputExerciseRefillJob, runMathExerciseRefillJob } from "@/lib/skills/refill-jobs";
import { runQueuedSourceUploadDraftJob } from "@/lib/skills/uploads";
import type { JobEnvelope } from "./contracts";
import type { JobExecutionContext } from "./worker";
import { recoverInterruptedJob } from "./recovery";
import { withWorkerJobPublicationContext } from "./publication-context";
import { RetryableRefillError } from "./errors";

export function executeJob(job: JobEnvelope, context: JobExecutionContext): Promise<unknown> {
  return withWorkerJobPublicationContext(
    () => executeJobInPublicationContext(job, context),
    job.continuationDepth ?? 0,
  );
}

async function executeJobInPublicationContext(job: JobEnvelope, context: JobExecutionContext): Promise<unknown> {
  if (context.attempt > 0) await recoverInterruptedJob(job);
  switch (job.name) {
    case "learnrecur/choice-refill.requested":
    case "learnrecur/exact-input-refill.requested":
    case "learnrecur/math-refill.requested": {
      const run = job.name === "learnrecur/choice-refill.requested" ? runChoiceExerciseRefillJob
        : job.name === "learnrecur/exact-input-refill.requested" ? runExactInputExerciseRefillJob : runMathExerciseRefillJob;
      try {
        const result = await run({ ...job.data, now: new Date(), ...(context.deadlineAt ? { deadlineAt: context.deadlineAt } : {}) });
        if (result?.status === "not-refilled" && result.retryable && result.failureCode) {
          throw new RetryableRefillError(result.failureCode, result.generationAttempt);
        }
        return result;
      } catch (error) {
        try {
          await markRefillJobRetryableFailure({ ...job.data, now: new Date(), error, terminal: context.attempt + 1 >= context.maxAttempts });
        } catch {
          console.error(JSON.stringify({ component: "background-jobs", outcome: "REFILL_FAILURE_RECORDING_FAILED" }));
        }
        throw error;
      }
    }
    case "learnrecur/source-upload-draft.requested":
      return runQueuedSourceUploadDraftJob({ ...job.data, now: new Date() });
    case "learnrecur/material-ingestion.requested":
      return runMaterialIngestionJob({ userId: job.data.userId, materialRevisionId: job.data.materialRevisionId });
    case "learnrecur/material-cleanup.requested":
      return runMaterialCleanupJob(job.data);
    case "learnrecur/material-draft-item.requested":
      return runMaterialDraftItemJob({ ...job.data, ...context });
    case "learnrecur/material-batch-activation.requested":
      return runMaterialBatchActivationJob({ ...job.data, ...context });
    case "learnrecur/agent-skill-operation.requested":
      return runAgentSkillOperationJob({
        ...job.data,
        ...(context.deadlineAt ? { deadlineAt: context.deadlineAt } : {}),
        deliveryAttempt: { attempt: context.attempt, maxAttempts: context.maxAttempts },
      });
    case "learnrecur/agent-connection-revocation.requested":
      return runAgentConnectionRevocationJob(job.data);
    case "learnrecur/account-deletion.requested":
      return runAccountDeletionJob(job.data);
    case "learnrecur/account-deletion.recovery":
      return recoverRetryableAccountDeletionJobs({ now: new Date() });
    case "learnrecur/agent-access.maintenance":
      return context.deadlineAt ? runAgentAccessMaintenance(new Date(), { deadlineAt: context.deadlineAt }) : runAgentAccessMaintenance(new Date());
    case "learnrecur/practice-reminders.due":
      return processDueReminderBatch({ accountEmailResolver: resolveClerkReminderAccountEmail, now: new Date() });
    default: {
      const unsupported: never = job;
      throw new Error(`JOB_TYPE_UNSUPPORTED: ${unsupported}`);
    }
  }
}
