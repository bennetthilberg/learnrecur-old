import {
  getJobStageTimeoutMs,
  isJobStageTimeoutError,
  withAbortableTimeout,
} from "@/lib/jobs/deadline";
import {
  getSafeJobErrorDetails,
  isTransientJobError,
  type RefillFailureCode,
} from "@/lib/jobs/errors";

export type RefillRuntimeInput = {
  userId: string;
  skillId: string;
  deadlineAt?: Date;
  signal?: AbortSignal;
  eventNow?: () => Date;
};
export type RefillRetryMetadata = {
  retryable?: boolean;
  failureCode?: RefillFailureCode;
  generationAttempt?: number;
};
export type RefillStage =
  | "refill-load"
  | "refill-reservation"
  | "refill-source-evidence"
  | "refill-recent-history"
  | "refill-generation"
  | "refill-verification"
  | "refill-publication";
export const REFILL_CLEANUP_MARGIN_MS = 30_000;
const CAPS: Record<RefillStage, number> = {
  "refill-load": 15_000,
  "refill-reservation": 15_000,
  "refill-source-evidence": 30_000,
  "refill-recent-history": 15_000,
  "refill-generation": 105_000,
  "refill-verification": 195_000,
  "refill-publication": 30_000,
};

export function refillStageBudget(
  input: RefillRuntimeInput,
  stage: RefillStage,
): number {
  return getJobStageTimeoutMs({
    deadlineAt: input.deadlineAt,
    cleanupMarginMs: input.deadlineAt ? REFILL_CLEANUP_MARGIN_MS : 0,
    maxTimeoutMs: CAPS[stage],
    stage,
  });
}

export async function runRefillStage<T>(
  input: RefillRuntimeInput,
  stage: RefillStage,
  run: (signal: AbortSignal) => Promise<T>,
  generationJobId?: string,
): Promise<T> {
  const startedAt = Date.now();
  const fields = {
    component: "skill-refill",
    userId: input.userId,
    skillId: input.skillId,
    generationJobId,
    stage,
  };
  console.info(JSON.stringify({ ...fields, outcome: "started" }));
  try {
    const result = await withAbortableTimeout({
      run,
      timeoutMs: refillStageBudget(input, stage),
      message: `${stage} timed out`,
      stage,
      parentSignal: input.signal,
    });
    console.info(
      JSON.stringify({
        ...fields,
        outcome: "completed",
        durationMs: Date.now() - startedAt,
      }),
    );
    return result;
  } catch (error) {
    console.warn(
      JSON.stringify({
        ...fields,
        outcome: "failed",
        durationMs: Date.now() - startedAt,
        retryable: isTransientJobError(error),
        ...getSafeJobErrorDetails(error),
      }),
    );
    throw error;
  }
}

export function refillRetryMetadata(
  error: unknown,
  stage: "source" | "generation" | "verification",
  generationAttempt: number,
): RefillRetryMetadata {
  if (!isTransientJobError(error)) return {};
  const details = getSafeJobErrorDetails(error);
  const databaseFailure =
    details.sqlState !== undefined ||
    details.errorCode === "DB_CONNECTION_TIMEOUT" ||
    (details.errorCode !== undefined &&
      /^(?:P\d{4}|(?:08|40|53|55|57)[0-9A-Z]{3})$/.test(details.errorCode));
  return {
    retryable: true,
    failureCode: isJobStageTimeoutError(error)
      ? "REFILL_STAGE_TIMEOUT"
      : databaseFailure
        ? "REFILL_DATABASE_UNAVAILABLE"
        : stage === "source"
          ? "REFILL_SOURCE_UNAVAILABLE"
          : "REFILL_PROVIDER_UNAVAILABLE",
    generationAttempt,
  };
}
