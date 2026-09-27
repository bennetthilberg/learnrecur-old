import "server-only";

import { randomUUID } from "node:crypto";

import {
  AgentOperationItemStatus,
  AgentOperationKind,
  AgentOperationStatus,
  GenerationFailureCategory,
  GenerationJobKind,
  GenerationJobStage,
  GenerationJobStatus,
  Prisma,
  SkillDraftBatchItemStatus,
  SkillStatus,
} from "@/generated/prisma/client";
import { sendAgentSkillOperationRequested } from "@/lib/jobs/events";
import {
  isJobStageTimeoutError,
  withAbortableTimeout,
} from "@/lib/jobs/deadline";
import { reconcileMaterialDraftBatch } from "@/lib/materials/batches";
import { getPrisma } from "@/lib/prisma";
import {
  AGENT_OPERATION_STALE_AFTER_MS,
  AGENT_OPERATION_ITEM_RETRY_LIMIT,
  buildAgentOperationContinuationCursor,
  isAgentOperationClaimStale,
  isAgentOperationRetryReady,
  getAgentOperationRetryReadyWhere,
} from "./recovery-policy";
import { reconcileAgentOperation } from "./reconciliation";

const RECOVERY_BATCH_LIMIT = 25;
const RECOVERABLE_OPERATION_KINDS = [
  AgentOperationKind.SPEC_BATCH,
  AgentOperationKind.TEXT_SOURCE,
  AgentOperationKind.MATERIAL_BATCH,
  AgentOperationKind.QUICK_FILES,
] as const;
const IN_FLIGHT_ITEM_STATUSES = [
  AgentOperationItemStatus.GENERATING,
  AgentOperationItemStatus.VERIFYING,
  AgentOperationItemStatus.ACTIVATING,
] as const;
// Older workers incorrectly persisted this wait condition as FAILED. Keep a
// narrow compatibility branch for those rows; all newly produced wait states
// remain ACTIVATING and are handled by the in-flight branch above.
const LEGACY_ACTIVATION_WAIT_ERROR = "ACTIVATION_IN_PROGRESS";
// Stale claims already waited past their lease; keep their immediate retry
// distinct from ordinary worker timeouts, which continue to honor backoff.
const STALE_RECOVERY_ERROR = "STALE_WORKER_RECOVERY";
const STALE_RECOVERY_MESSAGE =
  "The activation worker stopped before completion. The item was returned to the queue.";
const STALE_GENERATION_MESSAGE =
  "The activation worker lease expired before completion. The generation attempt can be retried.";
const TERMINAL_OPERATION_STATUSES = [
  AgentOperationStatus.SUCCEEDED,
  AgentOperationStatus.PARTIAL,
  AgentOperationStatus.FAILED,
  AgentOperationStatus.CANCELED,
] as const;

function readObject(value: Prisma.JsonValue | null): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

type RecoveryCandidate = {
  id: string;
  userId: string;
  operationId: string;
  operationKind: AgentOperationKind;
  operationRequestPayload: Prisma.JsonValue | null;
  ordinal: number;
  status: AgentOperationItemStatus;
  workerClaimToken: string | null;
  workerClaimedAt: Date | null;
  updatedAt: Date;
  createdSkillId: string | null;
  resultSkillId: string | null;
  retryCount: number;
};

type RecoveryResult = "requeued" | "finalized" | "waiting" | "promoted" | "unchanged";

export async function recoverStaleAgentOperationItems(input: {
  now: Date;
  userId?: string;
  operationId?: string;
  limit?: number;
}) {
  const prisma = getPrisma();
  const staleBefore = new Date(input.now.getTime() - AGENT_OPERATION_STALE_AFTER_MS);
  const rawCandidates = await prisma.agentSkillOperationItem.findMany({
    where: {
      ...(input.userId ? { userId: input.userId } : {}),
      ...(input.operationId ? { operationId: input.operationId } : {}),
      operation: {
        kind: { in: [...RECOVERABLE_OPERATION_KINDS] },
        status: {
          notIn: [
            ...TERMINAL_OPERATION_STATUSES,
            AgentOperationStatus.AWAITING_UPLOAD,
          ],
        },
      },
      OR: [
        {
          status: { in: [...IN_FLIGHT_ITEM_STATUSES] },
          OR: [
            { workerClaimedAt: { lte: staleBefore } },
            { workerClaimedAt: null, updatedAt: { lte: staleBefore } },
          ],
        },
        {
          status: AgentOperationItemStatus.FAILED,
          errorCode: LEGACY_ACTIVATION_WAIT_ERROR,
          updatedAt: { lte: staleBefore },
        },
      ],
    },
    orderBy: { updatedAt: "asc" },
    take: Math.min(input.limit ?? RECOVERY_BATCH_LIMIT, RECOVERY_BATCH_LIMIT),
    select: {
      id: true,
      userId: true,
      operationId: true,
      status: true,
      workerClaimToken: true,
      workerClaimedAt: true,
      updatedAt: true,
      createdSkillId: true,
      resultSkillId: true,
      retryCount: true,
      ordinal: true,
      operation: { select: { kind: true, requestPayload: true } },
    },
  });
  const candidates: RecoveryCandidate[] = rawCandidates.map(({ operation, ...candidate }) => ({
    ...candidate,
    operationKind: operation.kind,
    operationRequestPayload: operation.requestPayload,
  }));

  const changedOperations = new Set<string>();
  const counts = {
    scanned: candidates.length,
    requeued: 0,
    finalized: 0,
    waiting: 0,
    unchanged: 0,
    legacyPromoted: 0,
    continuations: 0,
    continuationPublishFailures: 0,
  };

  for (const candidate of candidates) {
    const result = await recoverCandidate({ candidate, now: input.now });
    console.info(JSON.stringify({
      component: "agent-activation-recovery",
      operationId: candidate.operationId,
      operationItemId: candidate.id,
      skillId: candidate.createdSkillId,
      stage: candidate.status,
      decision: result,
      ageMs: input.now.getTime() - candidate.updatedAt.getTime(),
      retryCount: candidate.retryCount,
    }));
    if (result === "requeued") {
      counts.requeued += 1;
    }
    if (result === "finalized") counts.finalized += 1;
    if (result === "waiting") counts.waiting += 1;
    if (result === "promoted") {
      counts.waiting += 1;
      counts.legacyPromoted += 1;
    }
    if (result === "unchanged") counts.unchanged += 1;
    if (result !== "unchanged" && result !== "waiting") {
      changedOperations.add(candidate.operationId);
    }
  }

  const queuedRecoveryItems = await prisma.agentSkillOperationItem.findMany({
    where: {
      ...(input.userId ? { userId: input.userId } : {}),
      ...(input.operationId ? { operationId: input.operationId } : {}),
      status: AgentOperationItemStatus.QUEUED,
      AND: [getAgentOperationRetryReadyWhere(input.now)],
      operation: {
        kind: { in: [...RECOVERABLE_OPERATION_KINDS] },
        status: {
          notIn: [
            ...TERMINAL_OPERATION_STATUSES,
            AgentOperationStatus.AWAITING_UPLOAD,
          ],
        },
      },
    },
    orderBy: { updatedAt: "asc" },
    take: Math.min(input.limit ?? RECOVERY_BATCH_LIMIT, RECOVERY_BATCH_LIMIT),
    select: { id: true, operationId: true, userId: true, errorCode: true, retryCount: true, updatedAt: true },
  });
  const continuationOwners = new Map<string, { operationId: string; userId: string }>();
  for (const operationId of changedOperations) {
    const candidate = candidates.find((item) => item.operationId === operationId);
    if (candidate) continuationOwners.set(operationId, candidate);
  }
  for (const item of queuedRecoveryItems) {
    if (isAgentOperationRetryReady({ ...item, now: input.now })) {
      continuationOwners.set(item.operationId, item);
    }
  }

  for (const owner of continuationOwners.values()) {
    const operation = await prisma.agentSkillOperation.findFirst({
      where: {
        id: owner.operationId,
        userId: owner.userId,
        status: {
          notIn: [
            ...TERMINAL_OPERATION_STATUSES,
            AgentOperationStatus.AWAITING_UPLOAD,
          ],
        },
      },
      select: { id: true },
    });
    if (!operation) continue;
    await reconcileAgentOperation({
      operationId: owner.operationId,
      userId: owner.userId,
      now: input.now,
    });
    const currentOperation = await prisma.agentSkillOperation.findFirst({
      where: {
        id: owner.operationId,
        userId: owner.userId,
        status: {
          notIn: [
            ...TERMINAL_OPERATION_STATUSES,
            AgentOperationStatus.AWAITING_UPLOAD,
          ],
        },
      },
      select: { updatedAt: true },
    });
    if (!currentOperation) continue;
    const queued = await prisma.agentSkillOperationItem.findMany({
      where: {
        operationId: owner.operationId,
        userId: owner.userId,
        status: AgentOperationItemStatus.QUEUED,
      },
      select: { id: true, retryCount: true, updatedAt: true, errorCode: true },
    });
    const eligible = queued.filter((item) =>
      isAgentOperationRetryReady({ ...item, now: input.now }),
    );
    if (eligible.length > 0) {
      const cursor = buildAgentOperationContinuationCursor({
        operationId: owner.operationId,
        operationUpdatedAt: currentOperation.updatedAt,
        items: eligible,
      });
      try {
        await withAbortableTimeout({
          run: (signal) => sendAgentSkillOperationRequested({
            userId: owner.userId,
            operationId: owner.operationId,
            requestedAt: cursor.requestedAt,
          }, { eventId: cursor.eventId, signal }),
          timeoutMs: 5_000,
          message: "Agent recovery continuation publication timed out.",
          stage: "agent recovery continuation publish",
        });
        counts.continuations += 1;
        console.info(JSON.stringify({
          component: "agent-activation-recovery",
          operationId: owner.operationId,
          decision: "continuation-published",
          queuedItemCount: eligible.length,
        }));
      } catch (error) {
        if (!isJobStageTimeoutError(error) && !isContinuationPublishFailure(error)) throw error;
        counts.continuationPublishFailures += 1;
        console.warn(JSON.stringify({
          component: "agent-activation-recovery",
          operationId: owner.operationId,
          decision: "continuation-publish-failed",
          cause: isJobStageTimeoutError(error) ? "stage-timeout" : "publish-error",
        }));
      }
    }
  }

  return counts;
}

function isContinuationPublishFailure(error: unknown) {
  return error instanceof Error && error.message === "JOB_PUBLISH_FAILED";
}

async function recoverCandidate(input: {
  candidate: RecoveryCandidate;
  now: Date;
}): Promise<RecoveryResult> {
  const prisma = getPrisma();
  const recoveryToken = randomUUID();
  const isLegacyCandidate = input.candidate.status === AgentOperationItemStatus.FAILED;
  const claimWhere: Prisma.AgentSkillOperationItemWhereInput = {
    id: input.candidate.id,
    userId: input.candidate.userId,
    operation: { status: { notIn: [...TERMINAL_OPERATION_STATUSES] } },
    ...(isLegacyCandidate
      ? { status: AgentOperationItemStatus.FAILED, errorCode: LEGACY_ACTIVATION_WAIT_ERROR }
      : { status: { in: [...IN_FLIGHT_ITEM_STATUSES] } }),
    ...(input.candidate.workerClaimToken
      ? { workerClaimToken: input.candidate.workerClaimToken }
      : { workerClaimToken: null }),
    ...(input.candidate.workerClaimedAt
      ? { workerClaimedAt: input.candidate.workerClaimedAt }
      : { workerClaimedAt: null, updatedAt: input.candidate.updatedAt }),
  };
  const claimed = await prisma.agentSkillOperationItem.updateMany({
    where: claimWhere,
    data: { workerClaimToken: recoveryToken, workerClaimedAt: input.now },
  });
  if (claimed.count !== 1) return "unchanged";

  const current = await prisma.agentSkillOperationItem.findFirst({
    where: { id: input.candidate.id, userId: input.candidate.userId },
    select: {
      id: true,
      createdSkillId: true,
      resultSkillId: true,
      status: true,
      errorCode: true,
      operationId: true,
    },
  });
  const isLegacyWaiting =
    current?.status === AgentOperationItemStatus.FAILED &&
    current.errorCode === LEGACY_ACTIVATION_WAIT_ERROR;
  if (
    !current ||
    (!IN_FLIGHT_ITEM_STATUSES.includes(current.status as (typeof IN_FLIGHT_ITEM_STATUSES)[number]) &&
      !isLegacyWaiting)
  ) {
    return "unchanged";
  }

  const skillIds = [current.createdSkillId, current.resultSkillId].filter(
    (value): value is string => Boolean(value),
  );
  const publishedSkill = skillIds.length
    ? await prisma.skill.findFirst({
        where: {
          id: { in: skillIds },
          userId: input.candidate.userId,
          status: { in: [SkillStatus.ACTIVE, SkillStatus.PAUSED] },
        },
        select: { id: true },
      })
    : null;
  if (publishedSkill) {
    return finalizePublishedItem({
      itemId: input.candidate.id,
      userId: input.candidate.userId,
      recoveryToken,
      skillId: publishedSkill.id,
      now: input.now,
    });
  }

  const generationJob = current.createdSkillId
    ? await prisma.generationJob.findFirst({
        where: {
          userId: input.candidate.userId,
          skillId: current.createdSkillId,
          kind: GenerationJobKind.SKILL_ACTIVATION,
          status: {
            in: [
              GenerationJobStatus.PENDING,
              GenerationJobStatus.RUNNING,
              GenerationJobStatus.SUCCEEDED,
              GenerationJobStatus.FAILED,
            ],
          },
        },
        orderBy: [{ updatedAt: "desc" }, { createdAt: "desc" }],
        select: { id: true, status: true, updatedAt: true },
      })
    : null;

  if (
    generationJob &&
    (generationJob.status === GenerationJobStatus.PENDING ||
      generationJob.status === GenerationJobStatus.RUNNING) &&
    !isAgentOperationClaimStale(generationJob.updatedAt, input.now)
  ) {
    if (isLegacyWaiting) {
      return promoteLegacyActivationWait({
        itemId: input.candidate.id,
        userId: input.candidate.userId,
        recoveryToken,
        now: input.now,
      });
    }
    await releaseRecoveryClaim({
      candidate: input.candidate,
      recoveryToken,
      now: input.now,
    });
    return "waiting";
  }

  if (
    generationJob &&
    (generationJob.status === GenerationJobStatus.PENDING ||
      generationJob.status === GenerationJobStatus.RUNNING)
  ) {
    const failed = await prisma.generationJob.updateMany({
      where: {
        id: generationJob.id,
        userId: input.candidate.userId,
        kind: GenerationJobKind.SKILL_ACTIVATION,
        status: generationJob.status,
        updatedAt: generationJob.updatedAt,
      },
      data: {
        status: GenerationJobStatus.FAILED,
        stage: GenerationJobStage.FAILED,
        failureCategory: GenerationFailureCategory.TIMEOUT,
        errorMessage: STALE_GENERATION_MESSAGE,
        completedAt: input.now,
      },
    });
    if (failed.count === 0) {
      const currentJob = await prisma.generationJob.findUnique({
        where: { id: generationJob.id },
        select: { status: true, updatedAt: true },
      });
      if (
        currentJob?.status === GenerationJobStatus.PENDING ||
        currentJob?.status === GenerationJobStatus.RUNNING
      ) {
        if (!isAgentOperationClaimStale(currentJob.updatedAt, input.now)) {
          if (isLegacyWaiting) {
            return promoteLegacyActivationWait({
              itemId: input.candidate.id,
              userId: input.candidate.userId,
              recoveryToken,
              now: input.now,
            });
          }
          await releaseRecoveryClaim({
            candidate: input.candidate,
            recoveryToken,
            now: input.now,
          });
          return "waiting";
        }
      }
    }
  }

  const publishedAfterJobRace = current.createdSkillId
    ? await prisma.skill.findFirst({
        where: {
          id: current.createdSkillId,
          userId: input.candidate.userId,
          status: { in: [SkillStatus.ACTIVE, SkillStatus.PAUSED] },
        },
        select: { id: true },
      })
    : null;
  if (publishedAfterJobRace) {
    return finalizePublishedItem({
      itemId: input.candidate.id,
      userId: input.candidate.userId,
      recoveryToken,
      skillId: publishedAfterJobRace.id,
      now: input.now,
    });
  }

  const retryExhausted =
    input.candidate.retryCount + 1 >= AGENT_OPERATION_ITEM_RETRY_LIMIT;
  const staleBefore = new Date(input.now.getTime() - AGENT_OPERATION_STALE_AFTER_MS);
  const recovery = await prisma.$transaction(async (tx) => {
    const payload = readObject(input.candidate.operationRequestPayload);
    const materialBatchId =
      input.candidate.operationKind === AgentOperationKind.MATERIAL_BATCH &&
      typeof payload.materialBatchId === "string"
        ? payload.materialBatchId
        : null;
    let synchronizedMaterialBatchId: string | null = null;
    let linkedMaterialSkillId: string | null = null;
    if (materialBatchId) {
      const materialItem = await tx.skillDraftBatchItem.findFirst({
        where: {
          batchId: materialBatchId,
          userId: input.candidate.userId,
          ordinal: input.candidate.ordinal,
        },
        select: { id: true, status: true, updatedAt: true, skillId: true },
      });
      if (
        materialItem?.status === SkillDraftBatchItemStatus.GENERATING &&
        materialItem.updatedAt > staleBefore
      ) {
        return { count: 0, freshMaterialGeneration: true, synchronizedMaterialBatchId: null };
      }
      if (materialItem?.status === SkillDraftBatchItemStatus.GENERATING) {
        const synchronized = await tx.skillDraftBatchItem.updateMany({
          where: {
            id: materialItem.id,
            userId: input.candidate.userId,
            status: SkillDraftBatchItemStatus.GENERATING,
            updatedAt: materialItem.updatedAt,
          },
          data: retryExhausted
            ? {
                status: SkillDraftBatchItemStatus.FAILED,
                generationClaimId: null,
                errorCode: "STALE_GENERATION_CLAIM",
                errorMessage: "Draft generation stopped before it finished. Retry this item.",
              }
            : {
                status: SkillDraftBatchItemStatus.PLANNED,
                generationClaimId: null,
                errorCode: null,
                errorMessage: null,
              },
        });
        if (synchronized.count === 0) {
          return { count: 0, freshMaterialGeneration: true, synchronizedMaterialBatchId: null };
        }
        synchronizedMaterialBatchId = materialBatchId;
      } else if (materialItem?.status === SkillDraftBatchItemStatus.READY) {
        if (materialItem.skillId) {
          // The worker can be terminated after material generation commits but
          // before it copies the generated draft id onto the agent item.
          // Preserve that durable result so the next delivery activates it
          // instead of trying to generate the material item again.
          linkedMaterialSkillId = materialItem.skillId;
        } else {
          const reset = await tx.skillDraftBatchItem.updateMany({
            where: {
              id: materialItem.id,
              userId: input.candidate.userId,
              status: SkillDraftBatchItemStatus.READY,
              updatedAt: materialItem.updatedAt,
            },
            data: {
              status: SkillDraftBatchItemStatus.PLANNED,
              generationClaimId: null,
              errorCode: null,
              errorMessage: null,
            },
          });
          if (reset.count === 0) {
            return { count: 0, freshMaterialGeneration: true, synchronizedMaterialBatchId: null };
          }
          synchronizedMaterialBatchId = materialBatchId;
        }
      }
    }

    const updated = await tx.agentSkillOperationItem.updateMany({
      where: {
        id: input.candidate.id,
        userId: input.candidate.userId,
        operation: { status: { notIn: [...TERMINAL_OPERATION_STATUSES] } },
        workerClaimToken: recoveryToken,
        retryCount: retryExhausted
          ? { gte: AGENT_OPERATION_ITEM_RETRY_LIMIT - 1 }
          : { lt: AGENT_OPERATION_ITEM_RETRY_LIMIT - 1 },
        OR: [
          { status: { in: [...IN_FLIGHT_ITEM_STATUSES] } },
          { status: AgentOperationItemStatus.FAILED, errorCode: LEGACY_ACTIVATION_WAIT_ERROR },
        ],
      },
      data: {
        status: retryExhausted
          ? AgentOperationItemStatus.FAILED
          : AgentOperationItemStatus.QUEUED,
        activationReservedAt: null,
        errorCode: STALE_RECOVERY_ERROR,
        errorMessage: retryExhausted
          ? `${STALE_RECOVERY_MESSAGE} Automatic retries are exhausted; retry this item manually.`
          : STALE_RECOVERY_MESSAGE,
        retryCount: { increment: 1 },
        completedAt: retryExhausted ? input.now : null,
        workerClaimToken: null,
        workerClaimedAt: null,
        ...(linkedMaterialSkillId && !input.candidate.createdSkillId
          ? { createdSkillId: linkedMaterialSkillId }
          : {}),
      },
    });
    return {
      count: updated.count,
      freshMaterialGeneration: false,
      synchronizedMaterialBatchId: updated.count === 1 ? synchronizedMaterialBatchId : null,
    };
  });
  if (recovery.freshMaterialGeneration) {
    await releaseRecoveryClaim({
      candidate: input.candidate,
      recoveryToken,
      now: input.now,
    });
    return "waiting";
  }
  if (recovery.synchronizedMaterialBatchId) {
    await reconcileMaterialDraftBatch({
      userId: input.candidate.userId,
      batchId: recovery.synchronizedMaterialBatchId,
      now: input.now,
    });
  }
  return recovery.count === 1 ? "requeued" : "unchanged";
}

async function promoteLegacyActivationWait(input: {
  itemId: string;
  userId: string;
  recoveryToken: string;
  now: Date;
}): Promise<RecoveryResult> {
  const promoted = await getPrisma().agentSkillOperationItem.updateMany({
    where: {
      id: input.itemId,
      userId: input.userId,
      operation: { status: { notIn: [...TERMINAL_OPERATION_STATUSES] } },
      workerClaimToken: input.recoveryToken,
      status: AgentOperationItemStatus.FAILED,
      errorCode: LEGACY_ACTIVATION_WAIT_ERROR,
    },
    data: {
      status: AgentOperationItemStatus.ACTIVATING,
      errorCode: "ACTIVATION_WAITING",
      errorMessage: "Another activation worker is still completing this draft.",
      completedAt: null,
      workerClaimToken: null,
      workerClaimedAt: null,
    },
  });
  return promoted.count === 1 ? "promoted" : "unchanged";
}

async function finalizePublishedItem(input: {
  itemId: string;
  userId: string;
  recoveryToken: string;
  skillId: string;
  now: Date;
}): Promise<RecoveryResult> {
  const finalized = await getPrisma().agentSkillOperationItem.updateMany({
    where: {
      id: input.itemId,
      userId: input.userId,
      operation: { status: { notIn: [...TERMINAL_OPERATION_STATUSES] } },
      workerClaimToken: input.recoveryToken,
      OR: [
        { status: { in: [...IN_FLIGHT_ITEM_STATUSES] } },
        { status: AgentOperationItemStatus.FAILED, errorCode: LEGACY_ACTIVATION_WAIT_ERROR },
      ],
    },
    data: {
      status: AgentOperationItemStatus.ACTIVE,
      resultSkillId: input.skillId,
      activationReservedAt: null,
      errorCode: null,
      errorMessage: null,
      completedAt: input.now,
      workerClaimToken: null,
      workerClaimedAt: null,
    },
  });
  return finalized.count === 1 ? "finalized" : "unchanged";
}

async function releaseRecoveryClaim(input: {
  candidate: RecoveryCandidate;
  recoveryToken: string;
  now: Date;
}) {
  await getPrisma().agentSkillOperationItem.updateMany({
    where: {
      id: input.candidate.id,
      userId: input.candidate.userId,
      workerClaimToken: input.recoveryToken,
      status: { in: [...IN_FLIGHT_ITEM_STATUSES] },
    },
    data: {
      workerClaimToken: input.candidate.workerClaimToken,
      workerClaimedAt: input.now,
    },
  });
}
