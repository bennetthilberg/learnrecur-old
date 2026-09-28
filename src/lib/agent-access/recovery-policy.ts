import { createHash } from "node:crypto";
import type { Prisma } from "@/generated/prisma/client";
import {
  AGENT_SKILL_OPERATION_SOFT_DEADLINE_MS,
} from "@/lib/jobs/timing";
import { ACTIVATION_GENERATION_TIMEOUT_MS } from "@/lib/skills/activation-timing";
import { JOB_LEASE_SECONDS } from "@/lib/jobs/timing";

/**
 * Activation work must finish well before the Lambda hard timeout. The job
 * deadline is intentionally soft, so reserve the part of the activation
 * budget that can occur after that deadline plus the cleanup margin.
 */
export const AGENT_OPERATION_CLEANUP_MARGIN_MS = 60_000;
export const AGENT_OPERATION_SOFT_DEADLINE_MS = AGENT_SKILL_OPERATION_SOFT_DEADLINE_MS;
export const AGENT_OPERATION_STALE_AFTER_MS = JOB_LEASE_SECONDS * 1_000;
export const AGENT_OPERATION_ITEM_RETRY_LIMIT = 4;
export const AGENT_OPERATION_RETRY_BASE_DELAY_MS = 30_000;
export const AGENT_OPERATION_RETRY_MAX_DELAY_MS = 2 * 60_000;
export const AGENT_OPERATION_ACTIVATION_RESERVE_MS =
  ACTIVATION_GENERATION_TIMEOUT_MS + AGENT_OPERATION_CLEANUP_MARGIN_MS;
export const AGENT_OPERATION_CANDIDATE_VERIFICATION_RESERVE_MS =
  AGENT_OPERATION_CLEANUP_MARGIN_MS;

export function isAgentOperationClaimStale(claimedAt: Date, now: Date): boolean {
  return now.getTime() - claimedAt.getTime() >= AGENT_OPERATION_STALE_AFTER_MS;
}

export function shouldDeferAgentOperation(deadlineAt: Date, now: Date): boolean {
  return deadlineAt.getTime() - now.getTime() <= AGENT_OPERATION_ACTIVATION_RESERVE_MS;
}

export function getAgentOperationRetryDelayMs(retryCount: number): number {
  if (!Number.isSafeInteger(retryCount) || retryCount <= 0) return 0;
  return Math.min(
    AGENT_OPERATION_RETRY_MAX_DELAY_MS,
    AGENT_OPERATION_RETRY_BASE_DELAY_MS * 2 ** Math.min(retryCount - 1, 8),
  );
}

export function isAgentOperationRetryReady(input: {
  errorCode: string | null;
  retryCount: number;
  updatedAt: Date;
  now: Date;
}): boolean {
  if (input.errorCode !== "TRANSIENT_WORKER_FAILURE") return true;
  return input.updatedAt.getTime() + getAgentOperationRetryDelayMs(input.retryCount) <= input.now.getTime();
}

export function getAgentOperationRetryReadyWhere(
  now: Date,
): Prisma.AgentSkillOperationItemWhereInput {
  const transientError = "TRANSIENT_WORKER_FAILURE";
  const cutoff = (delayMs: number) => new Date(now.getTime() - delayMs);
  return {
    OR: [
      { errorCode: null },
      { errorCode: { not: transientError } },
      {
        AND: [
          { errorCode: transientError },
          { retryCount: { lte: 0 } },
          { updatedAt: { lte: now } },
        ],
      },
      {
        AND: [
          { errorCode: transientError },
          { retryCount: 1 },
          { updatedAt: { lte: cutoff(getAgentOperationRetryDelayMs(1)) } },
        ],
      },
      {
        AND: [
          { errorCode: transientError },
          { retryCount: 2 },
          { updatedAt: { lte: cutoff(getAgentOperationRetryDelayMs(2)) } },
        ],
      },
      {
        AND: [
          { errorCode: transientError },
          { retryCount: { gte: 3 } },
          { updatedAt: { lte: cutoff(getAgentOperationRetryDelayMs(3)) } },
        ],
      },
    ],
  };
}

export function buildAgentOperationContinuationCursor(input: {
  operationId: string;
  operationUpdatedAt: Date;
  items: readonly { id: string; retryCount: number; updatedAt: Date }[];
}) {
  const items = [...input.items]
    .sort((left, right) => left.id.localeCompare(right.id))
    .map((item) => [item.id, item.retryCount, item.updatedAt.toISOString()]);
  const updatedAt = input.items.reduce(
    (latest, item) => Math.max(latest, item.updatedAt.getTime()),
    0,
  );
  const digest = createHash("sha256")
    .update(JSON.stringify([input.operationId, input.operationUpdatedAt.toISOString(), items]))
    .digest("hex")
    .slice(0, 40);
  return {
    eventId: `agent-op-${digest}`,
    requestedAt: new Date(Math.max(updatedAt, input.operationUpdatedAt.getTime())).toISOString(),
  };
}
