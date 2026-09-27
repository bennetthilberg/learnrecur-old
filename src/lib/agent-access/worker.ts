import { practicePreferenceOverrideSchema, textPolicySchema, type PracticePreference, type TextPolicy } from "@/lib/practice/policies";
import "server-only";
import { randomUUID } from "node:crypto";

import {
  AgentCandidateStatus,
  AgentOperationItemStatus,
  AgentOperationKind,
  AgentOperationStatus,
  GenerationJobKind,
  GenerationJobStatus,
  Prisma,
  SkillDraftBatchStatus,
  SkillDraftBatchItemStatus,
  SkillStatus,
} from "@/generated/prisma/client";
import {
  createSkillDraft,
  createSkillDraftFromSource,
  activateSkillDraft,
  verifyUntrustedAgentExerciseCandidates,
  type ActivateSkillDraftInput,
} from "@/lib/skills";
import {
  buildSkillDuplicateCandidateFingerprint,
  findSimilarSkillsForUser,
  type SkillSimilarityMatch,
} from "@/lib/skills/similarity";
import { getPrisma } from "@/lib/prisma";
import {
  MAX_IMPORT_BATCH_ITEMS,
  MAX_INLINE_OPERATION_ITEMS_PER_DELIVERY,
} from "@/lib/import-limits";
import { getSkillActivationUsage } from "@/lib/usage-limits";
import {
  getJobStageTimeoutMs,
  isJobStageTimeoutError,
  withAbortableTimeout,
} from "@/lib/jobs/deadline";
import { JobContinuationLimitError } from "@/lib/jobs/publication-context";
import {
  enqueueOperation,
} from "@/lib/agent-access/operations";
import { reconcileAgentOperation } from "@/lib/agent-access/reconciliation";
import {
  AGENT_OPERATION_CLEANUP_MARGIN_MS,
  AGENT_OPERATION_ITEM_RETRY_LIMIT,
  AGENT_OPERATION_CANDIDATE_VERIFICATION_RESERVE_MS,
  AGENT_OPERATION_SOFT_DEADLINE_MS,
  buildAgentOperationContinuationCursor,
  getAgentOperationRetryReadyWhere,
  isAgentOperationRetryReady,
  shouldDeferAgentOperation,
} from "@/lib/agent-access/recovery-policy";
import { completeSourceUploadDrafts } from "@/lib/skills/uploads";
import {
  confirmMaterialPlan,
  MaterialDraftGenerationError,
  planMaterialSkills,
  replanMaterialSkills,
  runMaterialDraftItemJob,
} from "@/lib/materials/batches";
import { buildAgentCandidateDuplicateKey } from "@/lib/agent-access/contracts";
import { CHOICE_VERIFICATION_TIMEOUT_MS } from "@/lib/skills/activation-timing";
import {
  MaterialSourceReferenceError,
  attachMaterialSourceReferencesToSkill,
  parseMaterialSourceReferences,
  type MaterialSourceReference,
} from "@/lib/materials/source-references";

const AGENT_OPERATION_INCLUDE = {
  items: { orderBy: { ordinal: "asc" as const }, include: { candidates: true } },
  sourceFile: true,
  sources: { orderBy: { ordinal: "asc" as const }, include: { sourceFile: true } },
  materialRevision: { select: { materialId: true, status: true } },
} satisfies Prisma.AgentSkillOperationInclude;

export type AgentSkillWorkerDependencies = {
  verifyAgentCandidates?: typeof verifyUntrustedAgentExerciseCandidates;
  activateDraft?: typeof activateSkillDraft;
  activationOptions?: Partial<ActivateSkillDraftInput>;
};

type AgentOperationWithItems = Prisma.AgentSkillOperationGetPayload<{
  include: typeof AGENT_OPERATION_INCLUDE;
}>;

export class AgentSkillWorkerError extends Error {
  constructor(message: string, readonly retryable: boolean) {
    super(message);
    this.name = "AgentSkillWorkerError";
  }
}

export function isMaterialLibraryOperation(operation: {
  kind: AgentOperationKind;
  toolName: string;
}) {
  return operation.kind === AgentOperationKind.MATERIAL_BATCH && operation.toolName.startsWith("materials.");
}

export function classifyAgentDuplicate(match: SkillSimilarityMatch | null) {
  if (!match) return { action: "create" as const, confidence: null };
  if (
    match.confidence === "exact" &&
    (match.skill.status === SkillStatus.ACTIVE || match.skill.status === SkillStatus.PAUSED)
  ) {
    return { action: "reuse" as const, confidence: match.confidence, skillId: match.skill.id };
  }
  return {
    action: "review" as const,
    confidence: match.confidence,
    skillId: match.skill.id,
  };
}

export function selectAgentOperationItemsForDelivery<
  T extends {
    status: AgentOperationItemStatus;
    errorCode?: string | null;
    retryCount?: number;
    updatedAt?: Date;
  },
>(
  items: readonly T[],
  operationKind: AgentOperationKind = AgentOperationKind.SPEC_BATCH,
  now = new Date(),
): T[] {
  const limit = operationKind === AgentOperationKind.MATERIAL_BATCH
    ? MAX_INLINE_OPERATION_ITEMS_PER_DELIVERY
    : 1;
  return items
    .filter((item) =>
      item.status === AgentOperationItemStatus.QUEUED &&
      isAgentOperationRetryReady({
        errorCode: item.errorCode ?? null,
        retryCount: item.retryCount ?? 0,
        updatedAt: item.updatedAt ?? now,
        now,
      }),
    )
    .slice(0, limit);
}

export async function runAgentSkillOperationJob(
  input: {
    userId: string;
    operationId: string;
    now?: Date;
    deadlineAt?: Date;
    deliveryAttempt?: { attempt: number; maxAttempts: number };
  },
  dependencies: AgentSkillWorkerDependencies = {},
) {
  const prisma = getPrisma();
  const now = input.now ?? new Date();
  const deadlineAt = input.deadlineAt ?? new Date(Date.now() + AGENT_OPERATION_SOFT_DEADLINE_MS);
  const clock = input.now ? () => now : () => new Date();
  const operation = await prisma.agentSkillOperation.findFirst({
    where: { id: input.operationId, userId: input.userId },
    include: AGENT_OPERATION_INCLUDE,
  });
  if (!operation) return { status: "not-found" as const };
  if (
    operation.status === AgentOperationStatus.SUCCEEDED ||
    operation.status === AgentOperationStatus.PARTIAL ||
    operation.status === AgentOperationStatus.FAILED ||
    operation.status === AgentOperationStatus.CANCELED
  ) {
    return { status: "complete" as const, operationId: operation.id };
  }
  // Material-library ingestion uses the native material-ingestion job and
  // shares AgentSkillOperation only for durable ownership/idempotency records.
  // A misplaced skill-operation event must never send its URL/PDF payload into
  // the skills.add_from_material planner.
  if (isMaterialLibraryOperation(operation)) {
    return { status: "delegated" as const, operationId: operation.id };
  }

  await prisma.agentSkillOperation.update({
    where: { id: operation.id },
    data: {
      startedAt: operation.startedAt ?? now,
      updatedAt: new Date(Math.max(Date.now(), operation.updatedAt.getTime() + 1)),
    },
  });

  const claims = new Map<string, string>();
  try {
  if (operation.kind === AgentOperationKind.MATERIAL_BATCH) {
    const shouldReconcile = await processMaterialOperation({
      operation,
      now,
      deadlineAt,
      clock,
      claims,
      dependencies,
    });
    if (shouldReconcile) {
      await reconcileAgentOperation({ operationId: operation.id, userId: input.userId, now: new Date() });
      await queueAgentOperationContinuation(operation.id, input.userId, clock(), deadlineAt);
    }
    return { status: "processed" as const, operationId: operation.id };
  }

  if (operation.kind === AgentOperationKind.QUICK_FILES) {
    await processFileOperation({ operation, now, deadlineAt, clock, claims, dependencies });
  } else if (operation.kind === AgentOperationKind.TEXT_SOURCE) {
    await processTextOperation({ operation, now, deadlineAt, clock, claims, dependencies });
  } else {
    const itemsForDelivery = selectAgentOperationItemsForDelivery(operation.items, operation.kind, clock());
    const candidates = itemsForDelivery.flatMap((item) => {
      const snapshot = parseSkillSnapshot(item.skillSnapshot);
      return snapshot ? [{ key: item.id, title: snapshot.title, objective: snapshot.objective }] : [];
    });
    const similarities = await findSimilarSkillsForUser({
      userId: input.userId,
      candidates,
      limitPerCandidate: 3,
      deadlineAt,
    });
    const byItem = new Map(similarities.candidates.map((candidate) => [candidate.key, candidate]));
    for (const item of itemsForDelivery) {
      if (shouldDeferAgentOperation(deadlineAt, clock()) && !item.createdSkillId) break;
      const claimToken = await claimQueuedAgentOperationItem({
        itemId: item.id,
        userId: input.userId,
        now,
      });
      if (!claimToken) continue;
      claims.set(item.id, claimToken);
      if (item.createdSkillId) {
        const reserved = await reserveAgentActivation(input.userId, item.id, now, claimToken);
        if (!reserved) {
          await failItem(item.id, input.userId, "QUOTA_EXCEEDED", now, claimToken);
          continue;
        }
        await activateCreatedDraft(input.userId, item.id, item.createdSkillId, now, claimToken, {
          deadlineAt,
          clock,
          dependencies,
        });
        continue;
      }
      const snapshot = parseSkillSnapshot(item.skillSnapshot);
      if (!snapshot) {
        await failItem(item.id, input.userId, "INVALID_SKILL_SNAPSHOT", now, claimToken);
        continue;
      }
      const duplicate = classifyAgentDuplicate(byItem.get(item.id)?.bestMatch ?? null);
      if (duplicate.action === "reuse") {
        let sourceReferenceOutcome: AgentSourceReferenceOutcome | null = null;
        if (snapshot.source_refs?.length) {
          try {
            sourceReferenceOutcome = summarizeAgentSourceReferenceOutcome(
              await attachMaterialSourceReferencesToSkill({
                userId: input.userId,
                skillId: duplicate.skillId,
                sourceRefs: snapshot.source_refs,
              }),
            );
          } catch (error) {
            await failItem(
              item.id,
              input.userId,
              sourceReferenceErrorCode(error),
              now,
              claimToken,
            );
            continue;
          }
        }
        await updateClaimedAgentItem({
          itemId: item.id,
          userId: input.userId,
          claimToken,
          now,
          data: {
            status: AgentOperationItemStatus.REUSED,
            resultSkillId: duplicate.skillId,
            duplicateConfidence: duplicate.confidence,
            duplicateLibraryFingerprint: similarities.duplicateLibraryFingerprint,
            sourceReferenceOutcome: sourceReferenceOutcome ? toJson(sourceReferenceOutcome) : undefined,
            completedAt: now,
            workerClaimToken: null,
            workerClaimedAt: null,
          },
        });
        continue;
      }
      if (duplicate.action === "review" && !item.duplicateOverrideApprovedAt) {
        await updateClaimedAgentItem({
          itemId: item.id,
          userId: input.userId,
          claimToken,
          now,
          data: {
            status: AgentOperationItemStatus.NEEDS_REVIEW,
            resultSkillId: duplicate.skillId,
            duplicateConfidence: duplicate.confidence,
            duplicateLibraryFingerprint: similarities.duplicateLibraryFingerprint,
            workerClaimToken: null,
            workerClaimedAt: null,
          },
        });
        continue;
      }
      await createAndActivateItem({
        userId: input.userId,
        itemId: item.id,
        snapshot,
        now,
        claimToken,
        deadlineAt,
        clock,
        dependencies,
      });
    }
  }

  await reconcileAgentOperation({ operationId: operation.id, userId: input.userId, now: new Date() });
  await queueAgentOperationContinuation(operation.id, input.userId, clock(), deadlineAt);
  return { status: "processed" as const, operationId: operation.id };
  } catch (error) {
    for (const [itemId, claimToken] of claims) {
      await releaseClaimForRetry({ itemId, userId: input.userId, now: new Date(), claimToken });
    }
    if (error instanceof JobContinuationLimitError) {
      await reconcileAgentOperation({ operationId: operation.id, userId: input.userId, now: new Date() });
      throw error;
    }
    if (isRecoverableWorkerTimeout(error)) {
      if (operation.kind === AgentOperationKind.MATERIAL_BATCH && claims.size === 0) {
        const currentItemCount = await prisma.agentSkillOperationItem.count({
          where: { operationId: operation.id, userId: input.userId },
        });
        if (currentItemCount === 0) {
          const attempt = input.deliveryAttempt?.attempt ?? 0;
          const maxAttempts = input.deliveryAttempt?.maxAttempts ?? 4;
          if (attempt >= maxAttempts - 1) {
            await failTimedOutAgentMaterialPlanning({
              operationId: operation.id,
              userId: input.userId,
              requestPayload: operation.requestPayload,
              now,
            });
            throw new AgentSkillWorkerError(
              "MATERIAL_PLANNING_TIMEOUT_RETRIES_EXHAUSTED",
              false,
            );
          }
          throw new AgentSkillWorkerError("MATERIAL_PLANNING_RETRYABLE_TIMEOUT", true);
        }
      }
      if (claims.size === 0) {
        await retryQueuedAgentItemsAfterPreclaimFailure({
          operationId: operation.id,
          userId: input.userId,
          limit: operation.kind === AgentOperationKind.MATERIAL_BATCH
            ? MAX_INLINE_OPERATION_ITEMS_PER_DELIVERY
            : 1,
          now: clock(),
        });
      }
      await reconcileAgentOperation({ operationId: operation.id, userId: input.userId, now: new Date() });
      if (!isContinuationPublishFailure(error)) {
        await queueAgentOperationContinuation(operation.id, input.userId, clock(), deadlineAt);
      }
      return { status: "retry-scheduled" as const, operationId: operation.id };
    }
    await reconcileAgentOperation({ operationId: operation.id, userId: input.userId, now: new Date() });
    throw new AgentSkillWorkerError(
      error instanceof Error ? error.message : "Agent skill processing failed.",
      true,
    );
  }
}

async function retryQueuedAgentItemsAfterPreclaimFailure(input: {
  operationId: string;
  userId: string;
  limit: number;
  now: Date;
}) {
  const prisma = getPrisma();
  const queuedItems = await prisma.agentSkillOperationItem.findMany({
    where: {
      operationId: input.operationId,
      userId: input.userId,
      status: AgentOperationItemStatus.QUEUED,
      AND: [getAgentOperationRetryReadyWhere(input.now)],
    },
    orderBy: { ordinal: "asc" },
    take: input.limit,
    select: { id: true, retryCount: true, updatedAt: true },
  });

  for (const item of queuedItems) {
    const exhausted = item.retryCount >= AGENT_OPERATION_ITEM_RETRY_LIMIT - 1;
    await prisma.agentSkillOperationItem.updateMany({
      where: {
        id: item.id,
        userId: input.userId,
        status: AgentOperationItemStatus.QUEUED,
        retryCount: item.retryCount,
        updatedAt: item.updatedAt,
      },
      data: {
        status: exhausted ? AgentOperationItemStatus.FAILED : AgentOperationItemStatus.QUEUED,
        activationReservedAt: null,
        errorCode: "TRANSIENT_WORKER_FAILURE",
        errorMessage: exhausted
          ? "The worker repeatedly stopped before processing this item. Retry it manually."
          : "The worker stopped before processing this item; it will be retried.",
        retryCount: { increment: 1 },
        completedAt: exhausted ? input.now : null,
        workerClaimToken: null,
        workerClaimedAt: null,
      },
    });
  }
}

async function failTimedOutAgentMaterialPlanning(input: {
  operationId: string;
  userId: string;
  requestPayload: Prisma.JsonValue | null;
  now: Date;
}) {
  const prisma = getPrisma();
  const payload = parseRecord(input.requestPayload);
  const batchId = typeof payload.materialBatchId === "string" ? payload.materialBatchId : null;
  const errorMessage =
    "Material planning timed out after the background job retry limit. Try the request again.";

  await prisma.$transaction(async (tx) => {
    const failed = await tx.agentSkillOperation.updateMany({
      where: {
        id: input.operationId,
        userId: input.userId,
        status: {
          notIn: [
            AgentOperationStatus.SUCCEEDED,
            AgentOperationStatus.PARTIAL,
            AgentOperationStatus.FAILED,
            AgentOperationStatus.CANCELED,
          ],
        },
        items: { none: {} },
      },
      data: {
        status: AgentOperationStatus.FAILED,
        errorCode: "MATERIAL_PLANNING_TIMEOUT_RETRIES_EXHAUSTED",
        errorMessage,
        completedAt: input.now,
      },
    });
    if (failed.count !== 1) return;

    await tx.skillDraftBatch.updateMany({
      where: {
        ...(batchId ? { id: batchId } : { idempotencyKey: `agent-${input.operationId}` }),
        userId: input.userId,
        status: SkillDraftBatchStatus.PLANNING,
        items: { none: {} },
      },
      data: {
        status: SkillDraftBatchStatus.FAILED,
        errorCode: "PLANNING_TIMEOUT_RETRIES_EXHAUSTED",
        errorMessage,
        completedAt: input.now,
      },
    });
  });
}

function isRecoverableWorkerTimeout(error: unknown) {
  if (isJobStageTimeoutError(error)) return true;
  if (typeof error !== "object" || error === null || !("code" in error)) return false;
  return ["P2024", "P2028", "P2034", "P1002", "P1008"].includes(String(error.code));
}

function isContinuationPublishFailure(error: unknown) {
  return error instanceof Error && error.message === "JOB_PUBLISH_FAILED";
}

const CLAIMED_ITEM_STATUSES = [
  AgentOperationItemStatus.GENERATING,
  AgentOperationItemStatus.VERIFYING,
  AgentOperationItemStatus.ACTIVATING,
] as const;

export async function claimQueuedAgentOperationItem(input: {
  itemId: string;
  userId: string;
  now: Date;
}): Promise<string | null> {
  const claimToken = randomUUID();
  const claimed = await getPrisma().agentSkillOperationItem.updateMany({
    where: {
      id: input.itemId,
      userId: input.userId,
      status: AgentOperationItemStatus.QUEUED,
      workerClaimToken: null,
    },
    data: {
      status: AgentOperationItemStatus.GENERATING,
      workerClaimToken: claimToken,
      workerClaimedAt: input.now,
      startedAt: input.now,
      completedAt: null,
      errorCode: null,
      errorMessage: null,
    },
  });
  return claimed.count === 1 ? claimToken : null;
}

async function updateClaimedAgentItem(input: {
  itemId: string;
  userId: string;
  claimToken: string;
  now: Date;
  data: Prisma.AgentSkillOperationItemUncheckedUpdateManyInput;
}) {
  const result = await getPrisma().agentSkillOperationItem.updateMany({
    where: {
      id: input.itemId,
      userId: input.userId,
      workerClaimToken: input.claimToken,
      status: { in: [...CLAIMED_ITEM_STATUSES] },
    },
    data: {
      ...input.data,
      workerClaimedAt: input.data.workerClaimToken === null ? null : input.now,
    },
  });
  if (result.count !== 1) {
    throw new AgentSkillWorkerError("AGENT_ITEM_CLAIM_LOST", true);
  }
}

async function touchClaimedAgentItem(input: {
  itemId: string;
  userId: string;
  claimToken: string;
  now: Date;
  status?: AgentOperationItemStatus;
}) {
  await updateClaimedAgentItem({
    ...input,
    data: {
      ...(input.status ? { status: input.status } : {}),
      workerClaimedAt: input.now,
    },
  });
}

async function releaseClaimForRetry(input: {
  itemId: string;
  userId: string;
  claimToken: string;
  now: Date;
}) {
  const prisma = getPrisma();
  const where = {
    id: input.itemId,
    userId: input.userId,
    workerClaimToken: input.claimToken,
    status: { in: [...CLAIMED_ITEM_STATUSES] },
  } satisfies Prisma.AgentSkillOperationItemWhereInput;
  const exhausted = await prisma.agentSkillOperationItem.updateMany({
    where: {
      ...where,
      retryCount: { gte: AGENT_OPERATION_ITEM_RETRY_LIMIT - 1 },
    },
    data: {
      status: AgentOperationItemStatus.FAILED,
      activationReservedAt: null,
      errorCode: "TRANSIENT_WORKER_FAILURE",
      errorMessage:
        "The worker stopped before the item completed and automatic retries are exhausted. Retry this item manually.",
      retryCount: { increment: 1 },
      completedAt: input.now,
      workerClaimToken: null,
      workerClaimedAt: null,
    },
  });
  if (exhausted.count === 1) return;
  await prisma.agentSkillOperationItem.updateMany({
    where: {
      ...where,
      retryCount: { lt: AGENT_OPERATION_ITEM_RETRY_LIMIT - 1 },
    },
    data: {
      status: AgentOperationItemStatus.QUEUED,
      activationReservedAt: null,
      errorCode: "TRANSIENT_WORKER_FAILURE",
      errorMessage: "The worker stopped before the item completed; it will be retried.",
      retryCount: { increment: 1 },
      completedAt: null,
      workerClaimToken: null,
      workerClaimedAt: null,
    },
  });
}

async function deferClaimedAgentItem(input: {
  itemId: string;
  userId: string;
  claimToken: string;
  now: Date;
}) {
  await updateClaimedAgentItem({
    ...input,
    data: {
      status: AgentOperationItemStatus.QUEUED,
      activationReservedAt: null,
      errorCode: "DELIVERY_DEFERRED",
      errorMessage: "The worker saved its progress and continued in a fresh delivery.",
      completedAt: null,
      workerClaimToken: null,
      workerClaimedAt: null,
    },
  });
}

async function waitForActivation(input: {
  itemId: string;
  userId: string;
  claimToken: string;
  now: Date;
}) {
  await updateClaimedAgentItem({
    ...input,
    data: {
      status: AgentOperationItemStatus.ACTIVATING,
      errorCode: "ACTIVATION_WAITING",
      errorMessage: "Another activation worker is still completing this draft.",
      workerClaimToken: null,
      workerClaimedAt: null,
    },
  });
}

async function processMaterialOperation(input: {
  operation: AgentOperationWithItems;
  now: Date;
  deadlineAt: Date;
  clock: () => Date;
  claims: Map<string, string>;
  dependencies: AgentSkillWorkerDependencies;
}): Promise<boolean> {
  const prisma = getPrisma();
  if (shouldDeferAgentOperation(input.deadlineAt, input.clock())) return true;
  const payload = parseRecord(input.operation.requestPayload);
  const instruction = buildMaterialOperationInstruction(payload);
  const maxSkills =
    typeof payload.maxSkills === "number" && Number.isInteger(payload.maxSkills)
      ? Math.min(MAX_IMPORT_BATCH_ITEMS, Math.max(1, payload.maxSkills))
      : 10;
  const sectionIds = stringArray(payload.sectionIds);
  const materialId = input.operation.materialRevision?.materialId;
  if (!materialId || !input.operation.materialRevisionId || !instruction) {
    await prisma.agentSkillOperation.update({
      where: { id: input.operation.id },
      data: { status: AgentOperationStatus.FAILED, errorCode: "INVALID_MATERIAL_REQUEST", completedAt: input.now },
    });
    return false;
  }
  const sectionTitles = sectionIds.length
    ? await prisma.materialSection.findMany({
        where: {
          id: { in: sectionIds },
          userId: input.operation.userId,
          materialRevisionId: input.operation.materialRevisionId,
        },
        orderBy: { ordinal: "asc" },
        select: { title: true },
      })
    : [];
  if (sectionTitles.length !== sectionIds.length) {
    await prisma.agentSkillOperation.update({
      where: { id: input.operation.id },
      data: { status: AgentOperationStatus.FAILED, errorCode: "MATERIAL_SECTION_NOT_FOUND", completedAt: input.now },
    });
    return false;
  }
  const planningInstruction = boundedMaterialInstruction(
    instruction,
    maxSkills,
    sectionTitles.map((section) => section.title),
  );

  let batchId = typeof payload.materialBatchId === "string" ? payload.materialBatchId : null;
  if (input.operation.items.length === 0) {
    let planning;
    if (batchId) {
      planning = await replanMaterialSkills({
        userId: input.operation.userId,
        now: input.now,
        deadlineAt: input.deadlineAt,
        preservePlanningOnTimeout: true,
        input: {
          batchId,
          instruction: planningInstruction,
          ...(sectionIds.length > 0 ? { sectionIds } : {}),
        },
      });
    } else {
      planning = await planMaterialSkills({
        userId: input.operation.userId,
        now: input.now,
        deadlineAt: input.deadlineAt,
        preservePlanningOnTimeout: true,
        input: {
          materialId,
          materialRevisionId: input.operation.materialRevisionId,
          instruction: planningInstruction,
          idempotencyKey: `agent-${input.operation.id}`,
          ...(sectionIds.length > 0 ? { sectionIds } : {}),
        },
      });
    }
    if (planning.status === "needs-scope") {
      const clarificationPayload: Record<string, unknown> = {
        ...payload,
        materialBatchId: planning.batchId,
      };
      if (sectionIds.length > 0) {
        clarificationPayload.originalSectionIds ??= sectionIds;
        delete clarificationPayload.sectionIds;
      }
      await prisma.agentSkillOperation.update({
        where: { id: input.operation.id },
        data: {
          status: AgentOperationStatus.NEEDS_INPUT,
          requestPayload: toJson(clarificationPayload),
          errorCode: "MATERIAL_SCOPE_NEEDS_INPUT",
          errorMessage: "Clarify the chapters, sections, or concepts to cover.",
        },
      });
      return false;
    }
    if (planning.status !== "planned") {
    await prisma.agentSkillOperation.update({
      where: { id: input.operation.id },
      data: {
        status: AgentOperationStatus.FAILED,
        errorCode: planning.status === "not-found" ? "MATERIAL_NOT_FOUND" : "MATERIAL_PLANNING_FAILED",
        errorMessage:
          "message" in planning ? planning.message : "Material planning could not finish.",
        completedAt: input.now,
      },
    });
      return false;
    }
    batchId = planning.batchId;
    if (planning.plan.items.length > maxSkills) {
    await prisma.agentSkillOperation.update({
      where: { id: input.operation.id },
      data: {
        status: AgentOperationStatus.NEEDS_INPUT,
        requestPayload: toJson({ ...payload, materialBatchId: batchId }),
        errorCode: "MATERIAL_PLAN_TOO_BROAD",
        errorMessage: `Clarify a narrower scope that yields no more than ${maxSkills} skills.`,
      },
    });
      return false;
    }
    const confirmed = await confirmMaterialPlan({
    userId: input.operation.userId,
    now: input.now,
    input: { batchId, plan: planning.plan },
    eventSender: { async sendMaterialDraftItemRequested() { return; } },
  });
    if (confirmed.status !== "queued" && confirmed.status !== "partial") {
    await prisma.agentSkillOperation.update({
      where: { id: input.operation.id },
      data: {
        status: AgentOperationStatus.FAILED,
        errorCode: "MATERIAL_CONFIRMATION_FAILED",
        errorMessage:
          "message" in confirmed ? confirmed.message : "The material plan could not be confirmed.",
        completedAt: input.now,
      },
    });
      return false;
    }
  }

  if (!batchId) {
    await prisma.agentSkillOperation.update({
      where: { id: input.operation.id },
      data: {
        status: AgentOperationStatus.FAILED,
        errorCode: "MATERIAL_BATCH_NOT_FOUND",
        completedAt: input.now,
      },
    });
    return false;
  }
  const materialItems = await prisma.skillDraftBatchItem.findMany({
    where: { batchId, userId: input.operation.userId },
    orderBy: { ordinal: "asc" },
    select: {
      id: true,
      ordinal: true,
      status: true,
      proposedTitle: true,
      proposedObjective: true,
      overlapSkillId: true,
    },
  });
  if (input.operation.items.length === 0) {
    await prisma.agentSkillOperation.update({
      where: { id: input.operation.id },
      data: {
        requestedCount: materialItems.length,
        requestPayload: toJson({ ...payload, materialBatchId: batchId }),
        items: {
          create: materialItems.map((item) => ({
            ordinal: item.ordinal,
            clientReference: `material-${item.ordinal + 1}`,
            proposedTitle: item.proposedTitle,
            proposedObjective: item.proposedObjective,
            status: item.overlapSkillId
              ? AgentOperationItemStatus.REUSED
              : AgentOperationItemStatus.QUEUED,
            resultSkillId: item.overlapSkillId,
            duplicateConfidence: item.overlapSkillId ? "exact" : null,
            completedAt: item.overlapSkillId ? input.now : null,
          })),
        },
      },
    });
  }
  const agentItems = await prisma.agentSkillOperationItem.findMany({
    where: { operationId: input.operation.id, userId: input.operation.userId },
    orderBy: { ordinal: "asc" },
  });
  const materialByOrdinal = new Map(materialItems.map((item) => [item.ordinal, item]));
  const itemsForDelivery = selectAgentOperationItemsForDelivery(
    agentItems,
    AgentOperationKind.MATERIAL_BATCH,
    input.clock(),
  );
  for (const agentItem of itemsForDelivery) {
    if (shouldDeferAgentOperation(input.deadlineAt, input.clock()) && !agentItem.createdSkillId) break;
    const claimToken = await claimQueuedAgentOperationItem({
      itemId: agentItem.id,
      userId: input.operation.userId,
      now: input.now,
    });
    if (!claimToken) continue;
    input.claims.set(agentItem.id, claimToken);
    if (agentItem.createdSkillId) {
      const reserved = await reserveAgentActivation(
        input.operation.userId,
        agentItem.id,
        input.now,
        claimToken,
      );
      if (!reserved) {
        await failItem(agentItem.id, input.operation.userId, "QUOTA_EXCEEDED", input.now, claimToken);
        continue;
      }
      await activateCreatedDraft(
        input.operation.userId,
        agentItem.id,
        agentItem.createdSkillId,
        input.now,
        claimToken,
        { deadlineAt: input.deadlineAt, clock: input.clock, dependencies: input.dependencies },
      );
      continue;
    }
    const materialItem = materialByOrdinal.get(agentItem.ordinal);
    if (!materialItem || materialItem.status !== SkillDraftBatchItemStatus.PLANNED) {
      await failItem(agentItem.id, input.operation.userId, "MATERIAL_ITEM_NOT_READY", input.now, claimToken);
      continue;
    }
    const reserved = await reserveAgentActivation(
      input.operation.userId,
      agentItem.id,
      input.now,
      claimToken,
    );
    if (!reserved) {
      await failItem(agentItem.id, input.operation.userId, "QUOTA_EXCEEDED", input.now, claimToken);
      continue;
    }
    let generated;
    try {
      generated = await runMaterialDraftItemJob({
        userId: input.operation.userId,
        batchId,
        itemId: materialItem.id,
        now: input.now,
        deadlineAt: input.deadlineAt,
        retryTransientOnWorkerDeadline: true,
      });
    } catch (error) {
      if (error instanceof MaterialDraftGenerationError) {
        if (error.retryable) {
          await releaseClaimForRetry({
            itemId: agentItem.id,
            userId: input.operation.userId,
            claimToken,
            now: input.now,
          });
          return true;
        }
        await failItem(agentItem.id, input.operation.userId, "MATERIAL_DRAFT_FAILED", input.now, claimToken);
        continue;
      }
      await releaseClaimForRetry({
        itemId: agentItem.id,
        userId: input.operation.userId,
        claimToken,
        now: input.now,
      });
      throw error;
    }
    if (generated.status === "excluded") {
      await updateClaimedAgentItem({
        itemId: agentItem.id,
        userId: input.operation.userId,
        claimToken,
        now: input.now,
        data: {
          status: AgentOperationItemStatus.REUSED,
          resultSkillId: generated.duplicateSkillId,
          duplicateConfidence: "exact",
          activationReservedAt: null,
          completedAt: input.now,
          workerClaimToken: null,
        },
      });
      continue;
    }
    if (generated.status !== "ready" || !generated.skillId) {
      await failItem(agentItem.id, input.operation.userId, "MATERIAL_DRAFT_FAILED", input.now, claimToken);
      continue;
    }
    const skill = await prisma.skill.findFirst({
      where: { id: generated.skillId, userId: input.operation.userId, status: SkillStatus.DRAFT },
    });
    if (!skill) {
      await failItem(agentItem.id, input.operation.userId, "DRAFT_NOT_FOUND", input.now, claimToken);
      continue;
    }
    await updateClaimedAgentItem({
      itemId: agentItem.id,
      userId: input.operation.userId,
      claimToken,
      now: input.now,
      data: {
        createdSkillId: skill.id,
        candidateFingerprint: buildSkillDuplicateCandidateFingerprint(skill),
        proposedTitle: skill.title,
        proposedObjective: skill.objective,
        skillSnapshot: toJson({
          title: skill.title,
          objective: skill.objective ?? "",
          rules: skill.rules,
          examples: skill.examples,
          exerciseConstraints: skill.exerciseConstraints,
          tags: skill.tags,
        }),
      },
    });
      await activateCreatedDraft(input.operation.userId, agentItem.id, skill.id, input.now, claimToken, {
        deadlineAt: input.deadlineAt,
        clock: input.clock,
        dependencies: input.dependencies,
      });
  }
  return true;
}

export function buildMaterialOperationInstruction(payload: Record<string, unknown>) {
  const instruction = typeof payload.instruction === "string" ? payload.instruction.trim() : "";
  const clarification =
    typeof payload.clarification === "string" ? payload.clarification.trim() : "";
  if (!instruction || !clarification) return instruction;
  return `${instruction}\n\nClarification: ${clarification}`;
}

function boundedMaterialInstruction(
  instruction: string,
  maxSkills: number,
  sectionTitles: readonly string[],
) {
  const constraints = [
    `Create no more than ${maxSkills} distinct skills.`,
    ...(sectionTitles.length
      ? [`Use only these selected sections: ${sectionTitles.join("; ")}.`]
      : []),
  ].join(" ").slice(0, 1_800);
  const instructionLimit = Math.max(3, 3_998 - constraints.length);
  return `${instruction.slice(0, instructionLimit)}\n\n${constraints}`;
}

async function processFileOperation(input: {
  operation: AgentOperationWithItems;
  now: Date;
  deadlineAt: Date;
  clock: () => Date;
  claims: Map<string, string>;
  dependencies: AgentSkillWorkerDependencies;
}) {
  const item = input.operation.items[0];
  const sourceFileIds = input.operation.sources.map((source) => source.sourceFileId);
  if (!item || sourceFileIds.length === 0) {
    if (item) await failItem(item.id, input.operation.userId, "SOURCE_NOT_READY", input.now);
    return;
  }
  if (item.status !== AgentOperationItemStatus.QUEUED) return;
  if (shouldDeferAgentOperation(input.deadlineAt, input.clock()) && !item.createdSkillId) return;
  const claimToken = await claimQueuedAgentOperationItem({
    itemId: item.id,
    userId: input.operation.userId,
    now: input.now,
  });
  if (!claimToken) return;
  input.claims.set(item.id, claimToken);
  if (item.createdSkillId) {
    const reserved = await reserveAgentActivation(
      input.operation.userId,
      item.id,
      input.now,
      claimToken,
    );
    if (!reserved) {
      await failItem(item.id, input.operation.userId, "QUOTA_EXCEEDED", input.now, claimToken);
      return;
    }
    await activateCreatedDraft(input.operation.userId, item.id, item.createdSkillId, input.now, claimToken, {
      deadlineAt: input.deadlineAt,
      clock: input.clock,
      dependencies: input.dependencies,
    });
    return;
  }
  const reserved = await reserveAgentActivation(
    input.operation.userId,
    item.id,
    input.now,
    claimToken,
  );
  if (!reserved) {
    await failItem(item.id, input.operation.userId, "QUOTA_EXCEEDED", input.now, claimToken);
    return;
  }
  const result = await completeSourceUploadDrafts({
    userId: input.operation.userId,
    sourceFileId: sourceFileIds[0],
    sourceFileIds,
    now: input.now,
  });
  if (result.status !== "created" || !result.skills[0]) {
    const code = result.status === "not-created" ? result.reason : "SOURCE_NOT_FOUND";
    await failItem(item.id, input.operation.userId, code, input.now, claimToken);
    return;
  }
  const generated = result.skills[0];
  await updateClaimedAgentItem({
    itemId: item.id,
    userId: input.operation.userId,
    claimToken,
    now: input.now,
    data: {
      createdSkillId: generated.id,
      candidateFingerprint: buildSkillDuplicateCandidateFingerprint(generated),
      proposedTitle: generated.title,
      proposedObjective: generated.objective,
      skillSnapshot: toJson({
        title: generated.title,
        objective: generated.objective ?? "",
        rules: generated.rules,
        examples: generated.examples,
        exerciseConstraints: generated.exerciseConstraints,
        tags: generated.tags,
      }),
    },
  });
  await activateCreatedDraft(input.operation.userId, item.id, generated.id, input.now, claimToken, {
    deadlineAt: input.deadlineAt,
    clock: input.clock,
    dependencies: input.dependencies,
  });
}

async function processTextOperation(input: {
  operation: AgentOperationWithItems;
  now: Date;
  deadlineAt: Date;
  clock: () => Date;
  claims: Map<string, string>;
  dependencies: AgentSkillWorkerDependencies;
}) {
  const item = input.operation.items[0];
  const source = input.operation.sourceFile;
  if (!item || !source?.extractedText) {
    if (item) await failItem(item.id, input.operation.userId, "SOURCE_NOT_READY", input.now);
    return;
  }
  if (item.status !== AgentOperationItemStatus.QUEUED) return;
  if (shouldDeferAgentOperation(input.deadlineAt, input.clock()) && !item.createdSkillId) return;
  const claimToken = await claimQueuedAgentOperationItem({
    itemId: item.id,
    userId: input.operation.userId,
    now: input.now,
  });
  if (!claimToken) return;
  input.claims.set(item.id, claimToken);
  if (item.createdSkillId) {
    const reserved = await reserveAgentActivation(
      input.operation.userId,
      item.id,
      input.now,
      claimToken,
    );
    if (!reserved) {
      await failItem(item.id, input.operation.userId, "QUOTA_EXCEEDED", input.now, claimToken);
      return;
    }
    await activateCreatedDraft(
      input.operation.userId,
      item.id,
      item.createdSkillId,
      input.now,
      claimToken,
      { deadlineAt: input.deadlineAt, clock: input.clock, dependencies: input.dependencies },
    );
    return;
  }
  const payload = parseRecord(input.operation.requestPayload);
  const result = await createSkillDraftFromSource({
    userId: input.operation.userId,
    now: input.now,
    skipUsageLimitCheck: true,
    persistFailedSource: false,
    input: {
      sourceText: source.extractedText,
      sourceLabel: source.originalName,
      focusNote: typeof payload.intent === "string" ? payload.intent : null,
      collectionName: typeof payload.collection === "string" ? payload.collection : null,
      tags: Array.isArray(payload.tags) ? payload.tags : [],
    },
    deadlineAt: input.deadlineAt,
  });
  if (result.status !== "created" || !result.skills[0]) {
    if (result.status === "not-created" && result.retryable) {
      await releaseClaimForRetry({ itemId: item.id, userId: input.operation.userId, now: input.now, claimToken });
      return;
    }
    await failItem(
      item.id,
      input.operation.userId,
      result.status === "not-created" ? result.reason : "DRAFT_GENERATION_FAILED",
      input.now,
      claimToken,
    );
    return;
  }
  const generated = result.skills[0];
  await updateClaimedAgentItem({
    itemId: item.id,
    userId: input.operation.userId,
    claimToken,
    now: input.now,
    data: {
      createdSkillId: generated.id,
      candidateFingerprint: buildSkillDuplicateCandidateFingerprint(generated),
      proposedTitle: generated.title,
      proposedObjective: generated.objective,
      skillSnapshot: toJson({
        title: generated.title,
        objective: generated.objective ?? "",
        rules: generated.rules,
        examples: generated.examples,
        exerciseConstraints: generated.exerciseConstraints,
        tags: generated.tags,
      }),
    },
  });
  const similar = await findSimilarSkillsForUser({
    userId: input.operation.userId,
    candidates: [{ key: item.id, skillId: generated.id, title: generated.title, objective: generated.objective }],
    limitPerCandidate: 3,
    deadlineAt: input.deadlineAt,
  });
  const duplicate = classifyAgentDuplicate(similar.candidates[0]?.bestMatch ?? null);
  if (duplicate.action !== "create") {
    await updateClaimedAgentItem({
      itemId: item.id,
      userId: input.operation.userId,
      claimToken,
      now: input.now,
      data: {
        status: duplicate.action === "reuse" ? AgentOperationItemStatus.REUSED : AgentOperationItemStatus.NEEDS_REVIEW,
        resultSkillId: duplicate.skillId,
        duplicateConfidence: duplicate.confidence,
        duplicateLibraryFingerprint: similar.duplicateLibraryFingerprint,
        completedAt: duplicate.action === "reuse" ? input.now : null,
        workerClaimToken: null,
      },
    });
    return;
  }
  const reserved = await reserveAgentActivation(
    input.operation.userId,
    item.id,
    input.now,
    claimToken,
  );
  if (!reserved) {
    await failItem(item.id, input.operation.userId, "QUOTA_EXCEEDED", input.now, claimToken);
    return;
  }
  await activateCreatedDraft(input.operation.userId, item.id, generated.id, input.now, claimToken, {
    deadlineAt: input.deadlineAt,
    clock: input.clock,
    dependencies: input.dependencies,
  });
}

async function createAndActivateItem(input: {
  userId: string;
  itemId: string;
  snapshot: SkillSnapshot;
  now: Date;
  claimToken: string;
  deadlineAt: Date;
  clock: () => Date;
  dependencies: AgentSkillWorkerDependencies;
}) {
  const reserved = await reserveAgentActivation(
    input.userId,
    input.itemId,
    input.now,
    input.claimToken,
  );
  if (!reserved) {
    await failItem(input.itemId, input.userId, "QUOTA_EXCEEDED", input.now, input.claimToken);
    return;
  }
  const draft = await createSkillDraft({
    userId: input.userId,
    input: buildSkillDraftInputFromSnapshot(input.snapshot),
  });
  if (draft.status !== "created") {
    await failItem(input.itemId, input.userId, "DRAFT_CREATE_FAILED", input.now, input.claimToken);
    return;
  }
  await updateClaimedAgentItem({
    itemId: input.itemId,
    userId: input.userId,
    claimToken: input.claimToken,
    now: input.now,
    data: {
      createdSkillId: draft.skill.id,
      candidateFingerprint: buildSkillDuplicateCandidateFingerprint(draft.skill),
    },
  });
  await activateCreatedDraft(
    input.userId,
    input.itemId,
    draft.skill.id,
    input.now,
    input.claimToken,
    { deadlineAt: input.deadlineAt, clock: input.clock, dependencies: input.dependencies },
  );
}

async function activateCreatedDraft(
  userId: string,
  itemId: string,
  skillId: string,
  now: Date,
  claimToken: string,
  context: {
    deadlineAt: Date;
    clock: () => Date;
    dependencies: AgentSkillWorkerDependencies;
  },
) {
  const prisma = getPrisma();
  await touchClaimedAgentItem({ itemId, userId, claimToken, now });
  const draft = await prisma.skill.findFirst({
    where: { id: skillId, userId },
    select: { id: true, title: true, objective: true, status: true },
  });
  if (!draft) {
    await failItem(itemId, userId, "DRAFT_NOT_FOUND", now, claimToken);
    return;
  }
  if (draft.status === SkillStatus.ACTIVE || draft.status === SkillStatus.PAUSED) {
    await finalizeAlreadyActivatedItem({ itemId, userId, skillId, now: new Date(), claimToken });
    return;
  }
  if (draft.status !== SkillStatus.DRAFT) {
    await failItem(itemId, userId, "DRAFT_NOT_FOUND", now, claimToken);
    return;
  }
  const operationItem = await prisma.agentSkillOperationItem.findFirst({
    where: { id: itemId, userId, createdSkillId: skillId },
    select: { candidateFingerprint: true, duplicateLibraryFingerprint: true, skillSnapshot: true },
  });
  const snapshot = parseSkillSnapshot(operationItem?.skillSnapshot);
  let sourceReferenceOutcome: AgentSourceReferenceOutcome | null = null;
  if (snapshot?.source_refs?.length) {
    if (shouldDeferAgentOperation(context.deadlineAt, context.clock())) {
      await deferClaimedAgentItem({ itemId, userId, claimToken, now });
      return;
    }
    try {
      sourceReferenceOutcome = summarizeAgentSourceReferenceOutcome(
        await attachMaterialSourceReferencesToSkill({
          userId,
          skillId,
          sourceRefs: snapshot.source_refs,
        }),
      );
    } catch (error) {
      await prisma.skill.deleteMany({ where: { id: skillId, userId, status: SkillStatus.DRAFT } });
      await failItem(
        itemId,
        userId,
        sourceReferenceErrorCode(error),
        now,
        claimToken,
      );
      return;
    }
  }
  const duplicateResult = await findSimilarSkillsForUser({
    userId,
    candidates: [{ key: itemId, skillId, title: draft.title, objective: draft.objective }],
    limitPerCandidate: 3,
    deadlineAt: context.deadlineAt,
  });
  const duplicate = classifyAgentDuplicate(duplicateResult.candidates[0]?.bestMatch ?? null);
  const reviewItem = await prisma.agentSkillOperationItem.findFirst({
    where: { id: itemId, userId },
    select: { duplicateOverrideApprovedAt: true },
  });
  const shouldStopForDuplicate =
    duplicate.action === "reuse" ||
    (duplicate.action === "review" && !reviewItem?.duplicateOverrideApprovedAt);
  if (shouldStopForDuplicate) {
    if (duplicate.action === "reuse" && snapshot?.source_refs?.length) {
      try {
        sourceReferenceOutcome = summarizeAgentSourceReferenceOutcome(
          await attachMaterialSourceReferencesToSkill({
            userId,
            skillId: duplicate.skillId,
            sourceRefs: snapshot.source_refs,
          }),
        );
      } catch (error) {
        await prisma.skill.deleteMany({ where: { id: skillId, userId, status: SkillStatus.DRAFT } });
        await failItem(itemId, userId, sourceReferenceErrorCode(error), now, claimToken);
        return;
      }
    }
    await updateClaimedAgentItem({
      itemId,
      userId,
      claimToken,
      now,
      data: {
        status: duplicate.action === "reuse" ? AgentOperationItemStatus.REUSED : AgentOperationItemStatus.NEEDS_REVIEW,
        resultSkillId: duplicate.skillId,
        duplicateConfidence: duplicate.confidence,
        duplicateLibraryFingerprint: duplicateResult.duplicateLibraryFingerprint,
        activationReservedAt: null,
        sourceReferenceOutcome: sourceReferenceOutcome ? toJson(sourceReferenceOutcome) : undefined,
        completedAt: duplicate.action === "reuse" ? now : null,
        workerClaimToken: null,
      },
    });
    if (duplicate.action === "reuse") {
      await prisma.skill.deleteMany({ where: { id: skillId, userId, status: SkillStatus.DRAFT } });
    }
    return;
  }
  const candidates = await prisma.agentExerciseCandidate.findMany({
    where: { operationItemId: itemId, userId, status: AgentCandidateStatus.VALIDATED },
    orderBy: { ordinal: "asc" },
    select: { id: true, normalizedPayload: true },
  });
  const seen = new Set<string>();
  const uniqueCandidates = candidates.filter((candidate) => {
    const key = buildAgentCandidateDuplicateKey(candidate.normalizedPayload);
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
  const duplicateIds = candidates
    .filter((candidate) => !uniqueCandidates.some((unique) => unique.id === candidate.id))
    .map((candidate) => candidate.id);
  if (duplicateIds.length) {
    await prisma.agentExerciseCandidate.updateMany({
      where: { id: { in: duplicateIds }, userId, operationItemId: itemId },
      data: { status: AgentCandidateStatus.REJECTED, verifierReason: "DUPLICATE" },
    });
  }
  if (uniqueCandidates.length) {
    if (context.deadlineAt.getTime() - context.clock().getTime() <= AGENT_OPERATION_CANDIDATE_VERIFICATION_RESERVE_MS) {
      await deferClaimedAgentItem({ itemId, userId, claimToken, now });
      return;
    }
    await touchClaimedAgentItem({
      itemId,
      userId,
      claimToken,
      now,
      status: AgentOperationItemStatus.VERIFYING,
    });
    const verification = await withAbortableTimeout({
      run: (signal) => (context.dependencies.verifyAgentCandidates ?? verifyUntrustedAgentExerciseCandidates)({
        userId,
        skillId,
        now,
        deadlineAt: context.deadlineAt,
        signal,
        candidates: uniqueCandidates.map((candidate) => ({
          candidateId: parseRecord(candidate.normalizedPayload).candidateId as string,
          normalizedPayload: candidate.normalizedPayload,
        })),
      }),
      timeoutMs: getJobStageTimeoutMs({
        deadlineAt: context.deadlineAt,
        cleanupMarginMs: AGENT_OPERATION_CLEANUP_MARGIN_MS,
        maxTimeoutMs: CHOICE_VERIFICATION_TIMEOUT_MS,
        stage: "agent candidate verification",
      }),
      message: "agent candidate verification timed out",
      stage: "agent candidate verification",
    });
    if (verification.status === "not-verified" && verification.retryable) {
      await releaseClaimForRetry({ itemId, userId, claimToken, now: new Date() });
      return;
    }
    if (verification.status === "verified") {
      for (const decision of verification.decisions) {
        const candidate = uniqueCandidates.find(
          (value) => parseRecord(value.normalizedPayload).candidateId === decision.candidateId,
        );
        if (!candidate) continue;
        await prisma.agentExerciseCandidate.updateMany({
          where: { id: candidate.id, userId, operationItemId: itemId },
          data: {
            status: decision.verdict === "verified" ? AgentCandidateStatus.VERIFIED : AgentCandidateStatus.REJECTED,
            verifierReason: decision.reason?.toUpperCase() ?? null,
            verifierNote: decision.note,
          },
        });
      }
    } else {
      await prisma.agentExerciseCandidate.updateMany({
        where: { id: { in: uniqueCandidates.map((candidate) => candidate.id) }, userId, operationItemId: itemId },
        data: { status: AgentCandidateStatus.REJECTED, verifierReason: verification.reason.toUpperCase() },
      });
    }
    await touchClaimedAgentItem({
      itemId,
      userId,
      claimToken,
      now,
      status: AgentOperationItemStatus.ACTIVATING,
    });
    if (shouldDeferAgentOperation(context.deadlineAt, context.clock())) {
      await deferClaimedAgentItem({ itemId, userId, claimToken, now });
      return;
    }
  }
  if (shouldDeferAgentOperation(context.deadlineAt, context.clock())) {
    await deferClaimedAgentItem({ itemId, userId, claimToken, now });
    return;
  }
  await updateClaimedAgentItem({
    itemId,
    userId,
    claimToken,
    now,
    data: { duplicateLibraryFingerprint: duplicateResult.duplicateLibraryFingerprint },
  });
  await touchClaimedAgentItem({
    itemId,
    userId,
    claimToken,
    now,
    status: AgentOperationItemStatus.VERIFYING,
  });
  let generationJobId: string | undefined;
  let result: Awaited<ReturnType<typeof activateSkillDraft>>;
  try {
    result = await (context.dependencies.activateDraft ?? activateSkillDraft)({
      ...context.dependencies.activationOptions,
      userId,
      skillId,
      now,
      eventNow: () => new Date(),
      onGenerationJobClaimed: (id) => { generationJobId = id; },
      deadlineAt: context.deadlineAt,
      skipUsageLimitCheck: true,
      verifiedAgentCandidateItemId: itemId,
      expectedDraftFingerprint: operationItem?.candidateFingerprint ?? undefined,
      expectedDuplicateLibraryFingerprint: duplicateResult.duplicateLibraryFingerprint ?? undefined,
    });
  } catch (error) {
    if (generationJobId) {
      try {
        await failClaimedActivationJob({
          itemId,
          userId,
          skillId,
          claimToken,
          generationJobId,
          now: new Date(),
          timedOut: isJobStageTimeoutError(error),
        });
      } catch {
        console.warn(JSON.stringify({
          component: "agent-activation",
          operationItemId: itemId,
          skillId,
          generationJobId,
          stage: "failure-recording",
          outcome: "failed",
        }));
      }
    }
    throw error;
  }
  if (result.status !== "activated") {
    if (result.reason === "activation-superseded") {
      const currentSkill = await prisma.skill.findFirst({
        where: { id: skillId, userId },
        select: { status: true },
      });
      if (currentSkill?.status === SkillStatus.ACTIVE || currentSkill?.status === SkillStatus.PAUSED) {
        await finalizeAlreadyActivatedItem({ itemId, userId, skillId, now: new Date(), claimToken });
        return;
      }
      const runningActivation = await prisma.generationJob.findFirst({
        where: {
          skillId,
          userId,
          kind: GenerationJobKind.SKILL_ACTIVATION,
          status: GenerationJobStatus.RUNNING,
        },
        select: { id: true },
      });
      if (!runningActivation) {
        await releaseClaimForRetry({ itemId, userId, claimToken, now: new Date() });
        return;
      }
    }
    if (result.reason === "activation-in-progress" || result.reason === "activation-superseded") {
      await waitForActivation({ itemId, userId, claimToken, now });
      return;
    }
    if ("retryable" in result && result.retryable) {
      await releaseClaimForRetry({ itemId, userId, claimToken, now: new Date() });
      return;
    }
    await failItem(itemId, userId, result.reason, now, claimToken);
    return;
  }
  await finalizeAlreadyActivatedItem({ itemId, userId, skillId, now: new Date(), claimToken });
}

async function failClaimedActivationJob(input: {
  itemId: string;
  userId: string;
  skillId: string;
  claimToken: string;
  generationJobId: string;
  now: Date;
  timedOut: boolean;
}) {
  const prisma = getPrisma();
  const failed = await prisma.$transaction(async (tx) => {
    const item = await tx.agentSkillOperationItem.findFirst({
      where: {
        id: input.itemId,
        userId: input.userId,
        createdSkillId: input.skillId,
        workerClaimToken: input.claimToken,
        status: { in: [...CLAIMED_ITEM_STATUSES] },
      },
      select: { id: true },
    });
    if (!item) return false;
    const draft = await tx.skill.findFirst({
      where: { id: input.skillId, userId: input.userId, status: SkillStatus.DRAFT },
      select: { id: true },
    });
    if (!draft) return false;
    const updated = await tx.generationJob.updateMany({
      where: {
        id: input.generationJobId,
        userId: input.userId,
        skillId: input.skillId,
        kind: GenerationJobKind.SKILL_ACTIVATION,
        status: GenerationJobStatus.RUNNING,
      },
      data: {
        status: GenerationJobStatus.FAILED,
        stage: "FAILED",
        checkpoint: "worker-exception",
        failureCategory: input.timedOut ? "TIMEOUT" : "UNKNOWN",
        errorMessage: input.timedOut
          ? "The activation stage exceeded its deadline. The attempt will be retried."
          : "The activation worker stopped unexpectedly. The attempt will be retried.",
        completedAt: input.now,
      },
    });
    return updated.count === 1;
  }, { maxWait: 5_000, timeout: 15_000 });
  console.info(JSON.stringify({
    component: "agent-activation",
    operationItemId: input.itemId,
    skillId: input.skillId,
    generationJobId: input.generationJobId,
    stage: "failure-recording",
    outcome: failed ? "recorded" : "superseded",
    cause: input.timedOut ? "stage-timeout" : "worker-exception",
  }));
}

async function finalizeAlreadyActivatedItem(input: {
  itemId: string;
  userId: string;
  skillId: string;
  now: Date;
  claimToken: string;
}) {
  await getPrisma().$transaction(async (tx) => {
    const itemUpdate = await tx.agentSkillOperationItem.updateMany({
      where: {
        id: input.itemId,
        userId: input.userId,
        workerClaimToken: input.claimToken,
        status: { in: [...CLAIMED_ITEM_STATUSES] },
      },
      data: {
        status: AgentOperationItemStatus.ACTIVE,
        resultSkillId: input.skillId,
        activationReservedAt: null,
        completedAt: input.now,
        errorCode: null,
        errorMessage: null,
        workerClaimToken: null,
        workerClaimedAt: null,
      },
    });
    if (itemUpdate.count !== 1) throw new AgentSkillWorkerError("AGENT_ITEM_CLAIM_LOST", true);
    await tx.agentExerciseCandidate.updateMany({
      where: {
        operationItemId: input.itemId,
        userId: input.userId,
        status: AgentCandidateStatus.VALIDATED,
      },
      data: { status: AgentCandidateStatus.NOT_PROCESSED, verifierReason: "NOT_SELECTED" },
    });
    await tx.agentExerciseCandidate.updateMany({
      where: {
        operationItemId: input.itemId,
        userId: input.userId,
        status: AgentCandidateStatus.VERIFIED,
        exerciseId: null,
      },
      data: { status: AgentCandidateStatus.NOT_PROCESSED, verifierReason: "NOT_SELECTED" },
    });
  }, { maxWait: 5_000, timeout: 15_000 });
}

/**
 * A delivery intentionally handles only a small prefix of queued items. The
 * remaining rows are the durable continuation cursor; enqueueing another
 * event gives them a fresh delivery identity without replaying terminal rows.
 */
async function queueAgentOperationContinuation(
  operationId: string,
  userId: string,
  now = new Date(),
  deadlineAt?: Date,
): Promise<boolean> {
  try {
    getJobStageTimeoutMs({
      deadlineAt,
      cleanupMarginMs: deadlineAt ? AGENT_OPERATION_CLEANUP_MARGIN_MS : 0,
      maxTimeoutMs: 5_000,
      stage: "agent continuation publish",
    });
  } catch (error) {
    if (isJobStageTimeoutError(error)) {
      console.info(JSON.stringify({ component: "agent-activation", operationId, stage: "continuation", outcome: "deferred-no-time" }));
      return false;
    }
    throw error;
  }
  const prisma = getPrisma();
  const [operation, queuedItems] = await Promise.all([
    prisma.agentSkillOperation.findFirst({
      where: { id: operationId, userId },
      select: { updatedAt: true },
    }),
    prisma.agentSkillOperationItem.findMany({
      where: {
        operationId,
        userId,
        status: AgentOperationItemStatus.QUEUED,
      },
      select: { id: true, retryCount: true, updatedAt: true, errorCode: true },
    }),
  ]);
  if (!operation) return false;
  const eligible = queuedItems.filter((item) => isAgentOperationRetryReady({
    errorCode: item.errorCode,
    retryCount: item.retryCount,
    updatedAt: item.updatedAt,
    now,
  }));
  if (eligible.length === 0) return false;
  const cursor = buildAgentOperationContinuationCursor({
    operationId,
    operationUpdatedAt: operation.updatedAt,
    items: eligible,
  });
  try {
    await withAbortableTimeout({
      run: (signal) => enqueueOperation(userId, operationId, { ...cursor, signal }),
      timeoutMs: getJobStageTimeoutMs({
        deadlineAt,
        cleanupMarginMs: deadlineAt ? AGENT_OPERATION_CLEANUP_MARGIN_MS : 0,
        maxTimeoutMs: 5_000,
        stage: "agent continuation publish",
      }),
      message: "Agent continuation publication timed out.",
      stage: "agent continuation publish",
    });
    console.info(JSON.stringify({ component: "agent-activation", operationId, stage: "continuation", outcome: "published", queuedItemCount: eligible.length }));
    return true;
  } catch (error) {
    if (isJobStageTimeoutError(error) || isContinuationPublishFailure(error)) {
      console.warn(JSON.stringify({ component: "agent-activation", operationId, stage: "continuation", outcome: "publish-failed", cause: isJobStageTimeoutError(error) ? "stage-timeout" : "publish-error" }));
      throw new AgentSkillWorkerError("AGENT_CONTINUATION_PUBLISH_FAILED", true);
    }
    throw error;
  }
}

export async function reserveAgentActivation(
  userId: string,
  itemId: string,
  now: Date,
  claimToken?: string,
) {
  const prisma = getPrisma();
  for (let attempt = 0; attempt < 3; attempt += 1) {
    try {
      return await prisma.$transaction(async (tx) => {
        await tx.$queryRaw`SELECT "id" FROM "users" WHERE "id" = ${userId} FOR UPDATE`;
        const usage = await getSkillActivationUsage({ userId, now, prisma: tx });
        if (
          usage.countedSkillCount >= usage.activeSkillLimit ||
          usage.activationsUsedToday >= usage.dailyActivationLimit
        ) {
          return false;
        }
        const claimed = await tx.agentSkillOperationItem.updateMany({
          where: {
            id: itemId,
            userId,
            activationReservedAt: null,
            ...(claimToken ? { workerClaimToken: claimToken } : { workerClaimToken: null }),
            status: {
              in: [AgentOperationItemStatus.QUEUED, AgentOperationItemStatus.GENERATING],
            },
          },
          data: {
            activationReservedAt: now,
            errorCode: null,
            errorMessage: null,
            ...(claimToken ? { workerClaimedAt: now } : {}),
          },
        });
        return claimed.count === 1;
      }, {
        isolationLevel: Prisma.TransactionIsolationLevel.Serializable,
        maxWait: 5_000,
        timeout: 15_000,
      });
    } catch (error) {
      if (
        !(error instanceof Prisma.PrismaClientKnownRequestError) ||
        error.code !== "P2034" ||
        attempt === 2
      ) {
        throw error;
      }
    }
  }
  return false;
}

async function failItem(
  itemId: string,
  userId: string,
  errorCode: string,
  now: Date,
  claimToken?: string,
) {
  await getPrisma().agentSkillOperationItem.updateMany({
    where: {
      id: itemId,
      userId,
      ...(claimToken
        ? {
            workerClaimToken: claimToken,
            status: { in: [...CLAIMED_ITEM_STATUSES] },
          }
        : { status: AgentOperationItemStatus.QUEUED }),
    },
    data: {
      status: AgentOperationItemStatus.FAILED,
      errorCode: normalizeAgentItemErrorCode(errorCode),
      activationReservedAt: null,
      completedAt: now,
      workerClaimToken: null,
      workerClaimedAt: null,
    },
  });
}

export function normalizeAgentItemErrorCode(errorCode: string) {
  return errorCode.toUpperCase().replace(/[^A-Z0-9]+/g, "_");
}

type SkillSnapshot = {
  alreadyStudied?: boolean;
  practicePreference?: PracticePreference | null;
  textPolicy?: TextPolicy | null;
  title: string;
  objective: string;
  rules: string[];
  examples: string[];
  exerciseConstraints: string;
  tags: string[];
  collection?: string;
  source_refs?: MaterialSourceReference[];
};

export type AgentSourceReferenceOutcome = {
  status: "attached" | "merged" | "preserved";
  attachedCount: number;
  mergedCount: number;
  unchangedCount: number;
};

export function summarizeAgentSourceReferenceOutcome(input: {
  attachedCount: number;
  mergedCount: number;
  unchangedCount: number;
}): AgentSourceReferenceOutcome {
  return {
    status: input.attachedCount > 0
      ? "attached"
      : input.mergedCount > 0
        ? "merged"
        : "preserved",
    attachedCount: input.attachedCount,
    mergedCount: input.mergedCount,
    unchangedCount: input.unchangedCount,
  };
}

function sourceReferenceErrorCode(error: unknown) {
  return error instanceof MaterialSourceReferenceError
    ? `SOURCE_REFERENCE_${error.code}`
    : "SOURCE_REFERENCE_INVALID";
}

export function buildSkillDraftInputFromSnapshot(snapshot: SkillSnapshot) {
  return {
    ...(snapshot.alreadyStudied !== undefined ? {alreadyStudied:snapshot.alreadyStudied} : {}),
    ...(snapshot.practicePreference !== undefined ? {practicePreference:snapshot.practicePreference} : {}),
    ...(snapshot.textPolicy !== undefined ? {textPolicy:snapshot.textPolicy} : {}),
    title: snapshot.title,
    objective: snapshot.objective,
    rules: snapshot.rules.join("\n"),
    examples: snapshot.examples.join("\n"),
    exerciseConstraints: snapshot.exerciseConstraints,
    tags: snapshot.tags,
    collectionName: snapshot.collection,
  };
}

export function parseSkillSnapshot(value: unknown): SkillSnapshot | null {
  const record = parseRecord(value);
  if (typeof record.title !== "string" || typeof record.objective !== "string") return null;
  const practicePreference = practicePreferenceOverrideSchema.optional().safeParse(record.practicePreference);
  const textPolicy = textPolicySchema.nullable().optional().safeParse(record.textPolicy);
  if (!practicePreference.success || !textPolicy.success ||
      (record.alreadyStudied !== undefined && typeof record.alreadyStudied !== "boolean")) return null;
  let sourceRefs: MaterialSourceReference[] | undefined;
  if (record.source_refs !== undefined) {
    try {
      sourceRefs = parseMaterialSourceReferences(record.source_refs);
    } catch {
      return null;
    }
  }
  return {
    ...(typeof record.alreadyStudied === "boolean" ? {alreadyStudied:record.alreadyStudied} : {}),
    ...(practicePreference.data !== undefined ? {practicePreference:practicePreference.data} : {}),
    ...(textPolicy.data !== undefined ? {textPolicy:textPolicy.data} : {}),
    title: record.title,
    objective: record.objective,
    rules: stringArray(record.rules),
    examples: stringArray(record.examples),
    exerciseConstraints: typeof record.exerciseConstraints === "string" ? record.exerciseConstraints : "",
    tags: stringArray(record.tags),
    collection: typeof record.collection === "string" ? record.collection : undefined,
    ...(sourceRefs !== undefined ? { source_refs: sourceRefs } : {}),
  };
}

function parseRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
}

function stringArray(value: unknown) {
  return Array.isArray(value) ? value.filter((item): item is string => typeof item === "string") : [];
}

function toJson(value: unknown): Prisma.InputJsonValue {
  return JSON.parse(JSON.stringify(value)) as Prisma.InputJsonValue;
}
