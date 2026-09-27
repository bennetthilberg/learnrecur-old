import "server-only";

import { createHash, randomUUID } from "node:crypto";
import { isDeepStrictEqual } from "node:util";

import {
  GenerationFailureCategory,
  GenerationJobKind,
  GenerationJobStage,
  GenerationJobStatus,
  MaterialPageTextStatus,
  MaterialRevisionStatus,
  Prisma,
  SkillDraftBatchItemStatus,
  SkillDraftBatchStatus,
  SkillStatus,
  SourceFileStatus,
  StudyMaterialStatus,
} from "@/generated/prisma/client";
import { getJobsEnvStatus } from "@/lib/jobs/config";
import {
  JobStageTimeoutError,
  getJobStageTimeoutMs,
  isJobStageTimeoutError,
  withAbortableTimeout,
} from "@/lib/jobs/deadline";
import { AGENT_OPERATION_CLEANUP_MARGIN_MS } from "@/lib/agent-access/recovery-policy";
import {
  awsMaterialBatchActivationEventSender,
  awsMaterialDraftItemEventSender,
  type MaterialBatchActivationEventSender,
  type MaterialDraftItemEventSender,
} from "@/lib/jobs/events";
import {
  resolveMaterialDraftAiSetup,
  type MaterialDraftAiSetup,
} from "@/lib/materials/ai";
import {
  MAX_SKILLS_PER_BATCH,
  activateBatchInputSchema,
  batchItemMutationInputSchema,
  confirmMaterialPlanInputSchema,
  materialScopePlanSchema,
  materialScopeResolutionSchema,
  planMaterialSkillsInputSchema,
  replanMaterialSkillsInputSchema,
  skillSourceLocatorSchema,
  type MaterialScopeResolution,
} from "@/lib/materials/contracts";
import {
  annotateMaterialPlanOverlaps,
  expandPlanningChunkNeighbors,
  generateValidatedMaterialScopePlan,
  generateVerifiedMaterialDraft,
  repairMaterialDraftTarget,
  recoverBackMatterMaterialScope,
  buildMaterialTopicRecoveryQuery,
  getMaterialTopicRecoveryGroupCount,
  resolveMaterialTopicSearchQuery,
  resolveExplicitMaterialSectionScope,
  resolveStructuralMaterialScope,
  selectFocusedMaterialTopicRecoveryChunks,
  selectMaterialTopicRetrievalChunks,
  type MaterialPlanningSection,
} from "@/lib/materials/drafting";
import { summarizeMaterialDraftBatch } from "@/lib/materials/batch-summary";
import {
  createGeminiMaterialEmbeddingGenerator,
  type MaterialEmbeddingGenerator,
} from "@/lib/materials/embeddings";
import {
  ensureMaterialPageOcr,
  loadLocalizedMaterialEvidence,
  type MaterialOcrGenerator,
} from "@/lib/materials/evidence";
import {
  materialPageEvidenceId,
  parseMaterialPageEvidenceId,
} from "@/lib/materials/evidence-ids";
import { createIdempotentDraftBatch } from "@/lib/materials/lifecycle";
import {
  searchMaterialChunks,
  searchMaterialChunksLexical,
  type MaterialChunkSearchResult,
} from "@/lib/materials/retrieval";
import {
  MuseRetrievalCapacityError,
  createMuseScanSignal,
  getMuseScanBudgetMs,
  scanMaterialChunksWithMuse,
  type MuseChunkRanker,
} from "@/lib/materials/muse-retrieval";
import { getPrisma } from "@/lib/prisma";
import type { SourceObjectStorage } from "@/lib/storage/s3";
import {
  ACTIVATION_GENERATION_STALE_AFTER_MS,
  ACTIVATION_STALE_JOB_MESSAGE,
  ACTIVATION_SUPERSEDED_JOB_MESSAGE,
  activateSkillDraft,
  GEMINI_PROVIDER,
  REQUESTED_ACTIVATION_EXERCISES,
  SKILL_MCQ_PROMPT_VERSION,
  type ChoiceExerciseGenerator,
  type ChoiceExerciseVerifier,
  type SkillSourceEvidenceLoader,
  type SourceMediaContextLoader,
} from "@/lib/skills";
import {
  buildSkillDuplicateCandidateFingerprint,
  buildSkillDuplicateLibraryFingerprint,
  buildSkillDuplicateReviewFingerprint,
  findSimilarSkillsForUser,
  normalizeSkillSimilarityText,
  rankSkillSimilarityMatches,
  type SkillSimilarityEmbeddingGenerator,
  type SkillSimilarityMatch,
  type SkillSimilarityPreview,
} from "@/lib/skills/similarity";
import {
  DEFAULT_GEMINI_MODEL,
  getPublicGeminiScopePlanningFailureMessage,
} from "@/lib/gemini";
import { ACTIVATION_PROVIDER_CHAIN_TIMEOUT_MS } from "@/lib/skills/activation-timing";
import {
  getSkillActivationUsage,
} from "@/lib/usage-limits";

const PLANNING_CHUNK_LIMIT = 60;
const GENERATION_EVIDENCE_CHARACTER_LIMIT = 24_000;
// Keep this above the five-minute function ceiling so a slow but healthy model call
// is not surfaced as failed while its worker can still complete.
const MATERIAL_DRAFT_CLAIM_STALE_MS = 10 * 60 * 1_000;
const MATERIAL_OCR_ABORT_SETTLE_GRACE_MS = 5_000;
// The worker can spend two provider budgets on generation and two more on verification.
// Only recover a claim after the five-minute function window has elapsed.
const MATERIAL_BATCH_ACTIVATION_CLAIM_STALE_MS = 5 * 60 * 1_000;
const MAX_AUTOMATIC_TARGET_REPAIRS = 2;

function throwIfMaterialPlanningAborted(signal?: AbortSignal): void {
  if (signal?.aborted) {
    throw signal.reason instanceof Error
      ? signal.reason
      : new Error("Material planning was canceled.");
  }
}

function withMaterialPlanningStageTimeout<T>(input: {
  deadlineAt?: Date;
  stage: string;
  message: string;
  settleGraceMs?: number;
  run(signal: AbortSignal): Promise<T>;
}): Promise<T> {
  return withAbortableTimeout({
    run: input.run,
    settleGraceMs: input.settleGraceMs,
    timeoutMs: getJobStageTimeoutMs({
      deadlineAt: input.deadlineAt,
      cleanupMarginMs: input.deadlineAt ? AGENT_OPERATION_CLEANUP_MARGIN_MS : 0,
      maxTimeoutMs: ACTIVATION_PROVIDER_CHAIN_TIMEOUT_MS,
      stage: input.stage,
    }),
    message: input.message,
    stage: input.stage,
  });
}

function isSupersededActivationJobMessage(message: string | null): boolean {
  return (
    message === ACTIVATION_SUPERSEDED_JOB_MESSAGE ||
    message === ACTIVATION_STALE_JOB_MESSAGE
  );
}

export class MaterialDraftGenerationError extends Error {
  readonly retryable: boolean;

  constructor(message: string, options: { retryable: boolean; cause?: unknown }) {
    super(message, { cause: options.cause });
    this.name = "MaterialDraftGenerationError";
    this.retryable = options.retryable;
  }
}

export class MaterialBatchActivationError extends Error {
  readonly retryable: boolean;

  constructor(message: string, options: { retryable: boolean; cause?: unknown }) {
    super(message, { cause: options.cause });
    this.name = "MaterialBatchActivationError";
    this.retryable = options.retryable;
  }
}

class MaterialDraftClaimSupersededError extends Error {}

export async function planMaterialSkills(input: {
  userId: string;
  input: unknown;
  now: Date;
  deadlineAt?: Date;
  preservePlanningOnTimeout?: boolean;
  aiSetup?: MaterialDraftAiSetup;
  embeddingGenerator?: MaterialEmbeddingGenerator | null;
  ocrGenerator?: MaterialOcrGenerator | null;
  ocrStorage?: SourceObjectStorage;
}) {
  const parsed = planMaterialSkillsInputSchema.safeParse(input.input);
  if (!parsed.success) {
    return {
      status: "invalid" as const,
      message: "Describe the material scope before planning skills.",
      fieldErrors: parsed.error.flatten().fieldErrors,
    };
  }
  const prisma = getPrisma();
  const revision = await prisma.materialRevision.findFirst({
    where: {
      id: parsed.data.materialRevisionId,
      materialId: parsed.data.materialId,
      userId: input.userId,
      status: MaterialRevisionStatus.READY,
      material: { status: { not: "DELETING" } },
    },
    select: { id: true },
  });
  if (!revision) {
    return { status: "not-found" as const, message: "Ready material revision was not found." };
  }

  let batch;
  try {
    batch = await createIdempotentDraftBatch({
      userId: input.userId,
      materialRevisionId: parsed.data.materialRevisionId,
      instruction: parsed.data.instruction,
      idempotencyKey: parsed.data.idempotencyKey,
      selectedSectionIds: parsed.data.sectionIds,
    });
  } catch (error) {
    return {
      status: "invalid" as const,
      message: error instanceof Error ? error.message : "This planning request could not be reused.",
    };
  }

  const existingPlan = materialScopeResolutionSchema.safeParse(batch.proposedPlan);
  if (existingPlan.success) {
    return planResult(batch.id, existingPlan.data);
  }

  return planExistingMaterialBatch({
    userId: input.userId,
    batchId: batch.id,
    now: input.now,
    deadlineAt: input.deadlineAt,
    preservePlanningOnTimeout: input.preservePlanningOnTimeout,
    aiSetup: input.aiSetup,
    embeddingGenerator: input.embeddingGenerator,
    ocrGenerator: input.ocrGenerator,
    ocrStorage: input.ocrStorage,
    sectionIds: parsed.data.sectionIds,
  });
}

export async function replanMaterialSkills(input: {
  userId: string;
  input: unknown;
  now: Date;
  deadlineAt?: Date;
  preservePlanningOnTimeout?: boolean;
  aiSetup?: MaterialDraftAiSetup;
  embeddingGenerator?: MaterialEmbeddingGenerator | null;
  ocrGenerator?: MaterialOcrGenerator | null;
  ocrStorage?: SourceObjectStorage;
}) {
  const parsed = replanMaterialSkillsInputSchema.safeParse(input.input);
  if (!parsed.success) {
    return {
      status: "invalid" as const,
      message: "Clarify which chapters, sections, or concepts you want.",
      fieldErrors: parsed.error.flatten().fieldErrors,
    };
  }
  const prisma = getPrisma();
  const updated = await prisma.skillDraftBatch.updateMany({
    where: {
      id: parsed.data.batchId,
      userId: input.userId,
      confirmedAt: null,
      status: {
        in: [
          SkillDraftBatchStatus.NEEDS_SCOPE,
          SkillDraftBatchStatus.PLANNED,
          SkillDraftBatchStatus.PLANNING,
        ],
      },
      items: { none: {} },
    },
    data: {
      instruction: parsed.data.instruction,
      proposedPlan: Prisma.JsonNull,
      planningMetadata: Prisma.JsonNull,
      status: SkillDraftBatchStatus.PLANNING,
      requestedCount: 0,
      errorCode: null,
      errorMessage: null,
      completedAt: null,
    },
  });
  if (updated.count !== 1) {
    return { status: "not-found" as const, message: "Editable scope plan was not found." };
  }
  return planExistingMaterialBatch({
    userId: input.userId,
    batchId: parsed.data.batchId,
    now: input.now,
    deadlineAt: input.deadlineAt,
    preservePlanningOnTimeout: input.preservePlanningOnTimeout,
    aiSetup: input.aiSetup,
    embeddingGenerator: input.embeddingGenerator,
    ocrGenerator: input.ocrGenerator,
    ocrStorage: input.ocrStorage,
    sectionIds: parsed.data.sectionIds,
  });
}

export async function confirmMaterialPlan(input: {
  userId: string;
  input: unknown;
  now: Date;
  eventSender?: MaterialDraftItemEventSender;
}) {
  const parsed = confirmMaterialPlanInputSchema.safeParse(input.input);
  if (!parsed.success) {
    return { status: "invalid" as const, message: "The scope plan changed or is incomplete." };
  }
  const prisma = getPrisma();
  const batch = await prisma.skillDraftBatch.findFirst({
    where: { id: parsed.data.batchId, userId: input.userId },
    include: { items: { orderBy: { ordinal: "asc" } } },
  });
  if (!batch) {
    return { status: "not-found" as const, message: "Material skill batch was not found." };
  }
  const alreadyConfirmed = Boolean(batch.confirmedAt && batch.items.length > 0);
  const proposed = materialScopePlanSchema.safeParse(
    alreadyConfirmed ? batch.confirmedPlan ?? batch.proposedPlan : batch.proposedPlan,
  );
  if (!proposed.success || !isDeepStrictEqual(proposed.data, parsed.data.plan)) {
    return {
      status: "invalid" as const,
      message: "The submitted scope no longer matches the reviewed plan. Review it again.",
    };
  }
  const createSeparatelyTargetKeys = new Set(
    parsed.data.createSeparatelyTargetKeys ?? [],
  );
  const env = getJobsEnvStatus();

  const confirmation = await prisma.$transaction(async (tx) => {
    await tx.$queryRaw`
      SELECT "id" FROM "skill_draft_batches"
      WHERE "id" = ${batch.id} AND "userId" = ${input.userId}
      FOR UPDATE
    `;
    const lockedBatch = await tx.skillDraftBatch.findFirst({
      where: { id: batch.id, userId: input.userId },
      include: { items: { orderBy: { ordinal: "asc" } } },
    });
    if (!lockedBatch) {
      return { status: "not-found" as const };
    }
    const lockedAlreadyConfirmed = Boolean(
      lockedBatch.confirmedAt && lockedBatch.items.length > 0,
    );
    const lockedPlan = materialScopePlanSchema.safeParse(
      lockedAlreadyConfirmed
        ? lockedBatch.confirmedPlan ?? lockedBatch.proposedPlan
        : lockedBatch.proposedPlan,
    );
    if (!lockedPlan.success || !isDeepStrictEqual(lockedPlan.data, parsed.data.plan)) {
      return { status: "invalid" as const };
    }
    if (lockedBatch.items.length > 0) {
      if (
        lockedBatch.items.some(
          (item) => item.status === SkillDraftBatchItemStatus.PLANNED,
        ) &&
        env.status === "missing-env" &&
        !input.eventSender
      ) {
        return { status: "not-queued" as const };
      }
      return {
        status: "ready" as const,
        items: lockedBatch.items,
        alreadyConfirmed: lockedAlreadyConfirmed,
      };
    }
    const overlapSkillIds = [
      ...new Set(
        lockedPlan.data.items.flatMap((item) =>
          item.overlapSkillId ? [item.overlapSkillId] : [],
        ),
      ),
    ];
    const lockedOverlapSkills =
      overlapSkillIds.length > 0
        ? await tx.$queryRaw<
            Array<{
              id: string;
              title: string;
              objective: string | null;
              collectionId: string | null;
              rules: unknown;
              examples: unknown;
              exerciseConstraints: unknown;
              tags: string[];
              status: SkillStatus;
            }>
          >(
            Prisma.sql`
              SELECT
                skill."id",
                skill."title",
                skill."objective",
                skill."collectionId",
                skill."rules",
                skill."examples",
                skill."exerciseConstraints",
                skill."tags",
                skill."status"
              FROM "skills" AS skill
              WHERE skill."userId" = ${input.userId}
                AND skill."id" IN (${Prisma.join(overlapSkillIds)})
              ORDER BY skill."id"
              FOR UPDATE OF skill
            `,
          )
        : [];
    const currentOverlapSkillById = new Map(
      lockedOverlapSkills.map((skill) => [skill.id, skill]),
    );
    const willQueueItem = lockedPlan.data.items.some((item) => {
      if (!item.overlapSkillId || !item.overlapSkillFingerprint) {
        return true;
      }
      const currentSkill = currentOverlapSkillById.get(item.overlapSkillId);
      return (
        !currentSkill ||
        buildSkillDuplicateReviewFingerprint(currentSkill) !==
          item.overlapSkillFingerprint ||
        createSeparatelyTargetKeys.has(item.key)
      );
    });
    if (
      willQueueItem &&
      env.status === "missing-env" &&
      !input.eventSender
    ) {
      return { status: "not-queued" as const };
    }
    const rows = [];
    for (const [ordinal, item] of lockedPlan.data.items.entries()) {
      const currentOverlapSkill = item.overlapSkillId
        ? currentOverlapSkillById.get(item.overlapSkillId)
        : undefined;
      const overlapSkill =
        currentOverlapSkill &&
        item.overlapSkillFingerprint &&
        buildSkillDuplicateReviewFingerprint(currentOverlapSkill) ===
          item.overlapSkillFingerprint
          ? currentOverlapSkill
          : null;
      const overlapSkillId = overlapSkill?.id ?? null;
      const createSeparatelyRequested =
        createSeparatelyTargetKeys.has(item.key);
      const userChoseCurrentMatchSeparately =
        createSeparatelyRequested && Boolean(overlapSkill);
      const preserveGeneratedDraft =
        createSeparatelyRequested ||
        Boolean(item.overlapSkillId && !overlapSkillId);
      const useExisting = Boolean(
        overlapSkillId && !preserveGeneratedDraft,
      );
      const duplicateMatchSkill =
        overlapSkill ??
        (preserveGeneratedDraft ? currentOverlapSkill : null);
      rows.push(
        await tx.skillDraftBatchItem.create({
          data: {
            userId: input.userId,
            batchId: batch.id,
            ordinal,
            targetKey: item.key,
            proposedTitle: item.title,
            proposedObjective: item.objective,
            locator: toInputJson(item.locator),
            status: useExisting
              ? SkillDraftBatchItemStatus.EXCLUDED
              : SkillDraftBatchItemStatus.PLANNED,
            overlapSkillId,
            errorCode: useExisting ? "DUPLICATE_USE_EXISTING" : null,
            errorMessage:
              useExisting && overlapSkill
                ? duplicateUseExistingMessage(overlapSkill)
                : null,
            generationMetadata: {
              scopeBoundaries: {
                includeConcepts: item.includeConcepts ?? [],
                excludeConcepts: item.excludeConcepts ?? [],
              },
              ...(duplicateMatchSkill
                ? {
                    duplicateMatch: {
                      skillId: duplicateMatchSkill.id,
                      skillFingerprint:
                        buildSkillDuplicateReviewFingerprint(
                          duplicateMatchSkill,
                        ),
                      confidence: item.overlapConfidence ?? "exact",
                      score: item.overlapScore ?? 1,
                      userOverride: userChoseCurrentMatchSeparately,
                      ...(preserveGeneratedDraft &&
                        !userChoseCurrentMatchSeparately
                        ? { preserveGeneratedDraft: true }
                        : {}),
                    },
                  }
                : {}),
            },
          },
        }),
      );
    }
    const excludedCount = rows.filter(
      (item) => item.status === SkillDraftBatchItemStatus.EXCLUDED,
    ).length;
    await tx.skillDraftBatch.update({
      where: { id: batch.id },
      data: {
        confirmedPlan: toInputJson(lockedPlan.data),
        confirmedAt: input.now,
        status:
          excludedCount === rows.length
            ? SkillDraftBatchStatus.READY
            : SkillDraftBatchStatus.GENERATING,
        requestedCount: rows.length,
        excludedCount,
        completedAt: excludedCount === rows.length ? input.now : null,
      },
    });
    return { status: "ready" as const, items: rows, alreadyConfirmed: false };
  });

  if (confirmation.status === "not-found") {
    return { status: "not-found" as const, message: "Material skill batch was not found." };
  }
  if (confirmation.status === "invalid") {
    return {
      status: "invalid" as const,
      message: "The submitted scope no longer matches the reviewed plan. Review it again.",
    };
  }
  if (confirmation.status === "not-queued") {
    return {
      status: "not-queued" as const,
      message:
        env.status === "missing-env"
          ? env.message
          : "Background processing is not available. Try again shortly.",
    };
  }
  const createdItems = confirmation.items;

  const queuedItems = createdItems.filter(
    (item) => item.status === SkillDraftBatchItemStatus.PLANNED,
  );
  const sender = input.eventSender ?? awsMaterialDraftItemEventSender;
  const sendResults = await Promise.allSettled(
    queuedItems.map((item) =>
      sender.sendMaterialDraftItemRequested({
        userId: input.userId,
        batchId: batch.id,
        itemId: item.id,
        requestedAt: input.now.toISOString(),
      }),
    ),
  );
  const failedItemIds = sendResults.flatMap((result, index) => {
    if (result.status !== "rejected") {
      return [];
    }
    const itemId = queuedItems[index].id;
    console.error("[background-jobs] material draft event send failed", {
      batchId: batch.id,
      itemId,
      error: getEventSendErrorLogDetails(result.reason),
    });
    return [itemId];
  });
  if (failedItemIds.length > 0) {
    await prisma.skillDraftBatchItem.updateMany({
      where: {
        id: { in: failedItemIds },
        userId: input.userId,
        status: SkillDraftBatchItemStatus.PLANNED,
      },
      data: {
        status: SkillDraftBatchItemStatus.FAILED,
        errorCode: "EVENT_SEND_FAILED",
        errorMessage: "Background processing was unavailable. Retry this item.",
      },
    });
    await reconcileMaterialDraftBatch({ userId: input.userId, batchId: batch.id, now: input.now });
  }

  return {
    status: failedItemIds.length === 0 ? ("queued" as const) : ("partial" as const),
    batchId: batch.id,
    queuedItemIds: queuedItems
      .map((item) => item.id)
      .filter((itemId) => !failedItemIds.includes(itemId)),
    failedItemIds,
    alreadyConfirmed: confirmation.alreadyConfirmed,
  };
}

export async function runMaterialDraftItemJob(input: {
  userId: string;
  batchId: string;
  itemId: string;
  requestedAt?: string;
  attempt?: number;
  maxAttempts?: number;
  now?: Date;
  deadlineAt?: Date;
  retryTransientOnWorkerDeadline?: boolean;
  signal?: AbortSignal;
  aiSetup?: MaterialDraftAiSetup;
  sourceStorage?: SourceObjectStorage;
  sourceEvidenceLoader?: SkillSourceEvidenceLoader;
}) {
  const prisma = getPrisma();
  const now = input.now ?? new Date();
  const staleBefore = new Date(now.getTime() - MATERIAL_DRAFT_CLAIM_STALE_MS);
  const item = await prisma.skillDraftBatchItem.findFirst({
    where: { id: input.itemId, batchId: input.batchId, userId: input.userId },
    select: {
      id: true,
      status: true,
      updatedAt: true,
      proposedTitle: true,
      proposedObjective: true,
      locator: true,
      generationMetadata: true,
      generationClaimId: true,
      overlapSkillId: true,
      skillId: true,
      skill: { select: { id: true } },
      batch: {
        select: {
          id: true,
          materialRevisionId: true,
          materialRevision: {
            select: {
              materialId: true,
              status: true,
              material: { select: { title: true, collectionId: true, status: true } },
              sourceFiles: {
                where: { status: SourceFileStatus.READY },
                orderBy: { createdAt: "asc" },
                take: 1,
                select: {
                  id: true,
                  materialRevisionId: true,
                  kind: true,
                  status: true,
                  originalName: true,
                  mimeType: true,
                  storageBucket: true,
                  storageKey: true,
                },
              },
            },
          },
        },
      },
    },
  });
  if (!item) {
    return { status: "not-found" as const };
  }
  if (item.status === SkillDraftBatchItemStatus.READY) {
    if (item.skill) {
      return { status: "ready" as const, alreadyGenerated: true };
    }
    const recovered = await prisma.skillDraftBatchItem.updateMany({
      where: {
        id: item.id,
        userId: input.userId,
        status: SkillDraftBatchItemStatus.READY,
        skillId: null,
      },
      data: {
        status: SkillDraftBatchItemStatus.FAILED,
        errorCode: "DRAFT_SKILL_DELETED",
        errorMessage: "The generated draft was deleted. Retry this item to create it again.",
      },
    });
    if (recovered.count !== 1) {
      return { status: "not-claimed" as const };
    }
    await reconcileMaterialDraftBatch({ userId: input.userId, batchId: input.batchId, now });
    return { status: "failed" as const, reason: "draft-skill-deleted" as const };
  }
  if (item.status === SkillDraftBatchItemStatus.EXCLUDED) {
    return { status: "excluded" as const };
  }

  const claimId = getMaterialDraftGenerationClaimId({
    itemId: input.itemId,
    requestedAt: input.requestedAt,
    status: item.status,
    existingClaimId: item.generationClaimId,
  });
  const claimed = await prisma.skillDraftBatchItem.updateMany({
    where: {
      id: item.id,
      userId: input.userId,
      batch: {
        materialRevision: {
          status: MaterialRevisionStatus.READY,
          material: { status: StudyMaterialStatus.ACTIVE },
        },
      },
      OR: [
        {
          status: SkillDraftBatchItemStatus.PLANNED,
          OR: [{ generationClaimId: null }, { generationClaimId: claimId }],
        },
        { status: SkillDraftBatchItemStatus.GENERATING, updatedAt: { lt: staleBefore } },
      ],
    },
    data: {
      status: SkillDraftBatchItemStatus.GENERATING,
      generationClaimId: claimId,
      errorCode: null,
      errorMessage: null,
    },
  });
  if (claimed.count !== 1) {
    if (
      item.status === SkillDraftBatchItemStatus.GENERATING &&
      item.updatedAt >= staleBefore
    ) {
      throw new MaterialDraftGenerationError("Draft generation is still running.", {
        retryable: true,
      });
    }
    return { status: "not-claimed" as const };
  }

  const locator = skillSourceLocatorSchema.safeParse(item.locator);
  const sourceFile = item.batch.materialRevision.sourceFiles[0];
  if (!locator.success || !sourceFile) {
    const marked = await markMaterialDraftItemFailed({
      userId: input.userId,
      itemId: item.id,
      claimId,
      code: "INVALID_CONFIRMED_SCOPE",
      message: "The confirmed material evidence is no longer available.",
    });
    if (!marked) {
      return { status: "not-claimed" as const };
    }
    await reconcileMaterialDraftBatch({ userId: input.userId, batchId: input.batchId, now });
    return { status: "failed" as const, reason: "invalid-scope" as const };
  }

  try {
    const localizedEvidence = input.sourceEvidenceLoader
      ? await input.sourceEvidenceLoader({
          userId: input.userId,
          sourceRefs: [{ locator: locator.data, sourceFile }],
        })
      : await loadLocalizedMaterialEvidence({
          userId: input.userId,
          sourceRefs: [{ locator: locator.data, sourceFile }],
          storage: input.sourceStorage,
        });
    const evidenceText = localizedEvidence.sourceContext
      ?.slice(0, GENERATION_EVIDENCE_CHARACTER_LIMIT)
      .trim();
    if (!evidenceText) {
      throw new MaterialDraftGenerationError("Confirmed evidence chunks were not found.", {
        retryable: false,
      });
    }
    const ai = input.aiSetup ?? resolveMaterialDraftAiSetup();
    const baseGenerationMetadata = readJsonObject(item.generationMetadata);
    let scopeBoundaries = readScopeBoundaries(item.generationMetadata);
    let target = {
      title: item.proposedTitle,
      objective: item.proposedObjective,
      ...scopeBoundaries,
    };
    let targetRepairState = readJsonObject(baseGenerationMetadata.targetRepair);
    let targetRepairAttempts = 0;
    let totalDraftAttempts = 0;
    const targetRepairContext = readTargetRepairContext(item.generationMetadata);
    const sourceMedia = localizedEvidence.sourceMedia.map((media) => ({
      sourceFileId: media.sourceFileId,
      label: media.originalName,
      mimeType: "application/pdf" as const,
      bytes: media.bytes,
    }));
    const applyAutomaticTargetRepair = async (verificationNote: string) => {
      if (!ai.repairTarget || targetRepairAttempts >= MAX_AUTOMATIC_TARGET_REPAIRS) {
        return { status: "unavailable" as const };
      }
      targetRepairAttempts += 1;
      targetRepairState = {
        status: "adjusting",
        attempts: targetRepairAttempts,
        verificationNote,
      };
      const staged = await prisma.skillDraftBatchItem.updateMany({
        where: {
          id: item.id,
          userId: input.userId,
          status: SkillDraftBatchItemStatus.GENERATING,
          generationClaimId: claimId,
        },
        data: {
          generationMetadata: toInputJson({
            ...baseGenerationMetadata,
            scopeBoundaries,
            targetRepair: targetRepairState,
          }),
        },
      });
      if (staged.count !== 1) {
        return { status: "not-claimed" as const };
      }
      const repaired = await repairMaterialDraftTarget({
        target,
        materialTitle: item.batch.materialRevision.material.title,
        evidenceText,
        verificationNote,
        repairTarget: ai.repairTarget,
        deadlineAt: input.deadlineAt,
        cleanupMarginMs: input.deadlineAt ? AGENT_OPERATION_CLEANUP_MARGIN_MS : 0,
        signal: input.signal,
        sourceMedia,
      });
      if (repaired.status === "failed") {
        return { status: "failed" as const, message: repaired.message };
      }
      target = repaired.target;
      scopeBoundaries = {
        includeConcepts: repaired.target.includeConcepts ?? [],
        excludeConcepts: repaired.target.excludeConcepts ?? [],
      };
      targetRepairState = {
        status: "regenerating",
        attempts: targetRepairAttempts,
        note: repaired.note,
        previousVerificationNote: verificationNote,
      };
      const targetUpdated = await prisma.skillDraftBatchItem.updateMany({
        where: {
          id: item.id,
          userId: input.userId,
          status: SkillDraftBatchItemStatus.GENERATING,
          generationClaimId: claimId,
        },
        data: {
          proposedTitle: repaired.target.title,
          proposedObjective: repaired.target.objective,
          generationMetadata: toInputJson({
            ...baseGenerationMetadata,
            scopeBoundaries,
            targetRepair: targetRepairState,
          }),
        },
      });
      if (targetUpdated.count !== 1) {
        return { status: "not-claimed" as const };
      }
      return { status: "repaired" as const };
    };
    const finishTargetRepairFailure = async (message: string) => {
      const marked = await markMaterialDraftItemFailed({
        userId: input.userId,
        itemId: item.id,
        claimId,
        code: "TARGET_REPAIR_FAILED",
        message,
        generationMetadata: toInputJson({
          ...baseGenerationMetadata,
          scopeBoundaries,
          targetRepair: {
            status: "exhausted",
            attempts: targetRepairAttempts,
            note: message,
          },
        }),
      });
      if (!marked) {
        return { status: "not-claimed" as const };
      }
      await reconcileMaterialDraftBatch({ userId: input.userId, batchId: input.batchId, now });
      return { status: "failed" as const, reason: "target-repair-failed" as const };
    };

    if (targetRepairContext && ai.repairTarget) {
      const initialRepair = await applyAutomaticTargetRepair(
        targetRepairContext.verificationNote,
      );
      if (initialRepair.status === "not-claimed") {
        return { status: "not-claimed" as const };
      }
      if (initialRepair.status === "failed") {
        return finishTargetRepairFailure(initialRepair.message);
      }
    }

    let generated: Awaited<ReturnType<typeof generateVerifiedMaterialDraft>> | null = null;
    while (true) {
      generated = await generateVerifiedMaterialDraft({
        target,
        materialTitle: item.batch.materialRevision.material.title,
        evidenceText,
        sourceMedia,
        generateDraft: async (draftInput) => {
          const attempt = await prisma.skillDraftBatchItem.updateMany({
            where: {
              id: item.id,
              userId: input.userId,
              status: SkillDraftBatchItemStatus.GENERATING,
              generationClaimId: claimId,
            },
            data: { generationAttempts: { increment: 1 } },
          });
          if (attempt.count !== 1) {
            throw new MaterialDraftClaimSupersededError();
          }
          return ai.generateDraft(draftInput);
        },
        verifyDraft: ai.verifyDraft,
        deadlineAt: input.deadlineAt,
        cleanupMarginMs: input.deadlineAt ? AGENT_OPERATION_CLEANUP_MARGIN_MS : 0,
        signal: input.signal,
      });
      totalDraftAttempts += generated.attempts;
      if (generated.status === "ready") {
        break;
      }
      if (
        generated.reason === "verification-rejected" &&
        ai.repairTarget &&
        targetRepairAttempts < MAX_AUTOMATIC_TARGET_REPAIRS
      ) {
        const repair = await applyAutomaticTargetRepair(generated.message);
        if (repair.status === "not-claimed") {
          return { status: "not-claimed" as const };
        }
        if (repair.status === "failed") {
          return finishTargetRepairFailure(repair.message);
        }
        if (repair.status === "repaired") {
          continue;
        }
      }

      const marked = await markMaterialDraftItemFailed({
        userId: input.userId,
        itemId: item.id,
        claimId,
        code: generated.reason.toUpperCase().replaceAll("-", "_"),
        message: generated.message,
        generationMetadata: toInputJson({
          ...baseGenerationMetadata,
          model: ai.model,
          verification: "rejected",
          attemptsThisRun: totalDraftAttempts,
          scopeBoundaries,
          recovery: generated.recovery ?? "clarify_scope",
          reasons: generated.reasons ?? [],
          verificationNote: generated.message,
          targetRepair: {
            status:
              generated.reason === "verification-rejected"
                ? "exhausted"
                : targetRepairState.status,
            attempts: targetRepairAttempts,
            verificationNote: generated.message,
          },
        }),
      });
      if (!marked) {
        return { status: "not-claimed" as const };
      }
      await reconcileMaterialDraftBatch({ userId: input.userId, batchId: input.batchId, now });
      return { status: "failed" as const, reason: generated.reason };
    }

    if (!generated || generated.status !== "ready") {
      throw new Error("Material draft generation ended without a verified draft.");
    }
    if (targetRepairAttempts > 0) {
      targetRepairState = {
        ...targetRepairState,
        status: "completed",
        attempts: targetRepairAttempts,
      };
    }

    const saved = await prisma.$transaction(async (tx) => {
      await tx.$queryRaw`
        SELECT "id" FROM "users"
        WHERE "id" = ${input.userId}
        FOR UPDATE
      `;
      await tx.$queryRaw`
        SELECT "id" FROM "skill_draft_batch_items"
        WHERE "id" = ${item.id} AND "userId" = ${input.userId}
        FOR UPDATE
      `;
      const activeClaim = await tx.skillDraftBatchItem.findFirst({
        where: {
          id: item.id,
          userId: input.userId,
          status: SkillDraftBatchItemStatus.GENERATING,
          generationClaimId: claimId,
        },
        select: { id: true },
      });
      if (!activeClaim) {
        return { status: "superseded" as const };
      }
      await tx.$queryRaw`
        SELECT "id" FROM "study_materials"
        WHERE "id" = ${item.batch.materialRevision.materialId}
          AND "userId" = ${input.userId}
        FOR UPDATE
      `;
      const availableRevision = await tx.materialRevision.findFirst({
        where: {
          id: item.batch.materialRevisionId,
          userId: input.userId,
          status: MaterialRevisionStatus.READY,
          material: { status: StudyMaterialStatus.ACTIVE },
        },
        select: { id: true },
      });
      if (!availableRevision) {
        return { status: "superseded" as const };
      }
      await tx.$queryRaw`
        SELECT pg_advisory_xact_lock(hashtext(${item.batch.materialRevision.materialId}))::text AS "lock"
      `;
      const existingSkills = await tx.$queryRaw<
        Array<{
          id: string;
          title: string;
          objective: string | null;
          collectionId: string | null;
          rules: unknown;
          examples: unknown;
          exerciseConstraints: unknown;
          tags: string[];
          status: SkillStatus;
        }>
      >`
        SELECT
          skill."id",
          skill."title",
          skill."objective",
          skill."collectionId",
          skill."rules",
          skill."examples",
          skill."exerciseConstraints",
          skill."tags",
          skill."status"
        FROM "skills" AS skill
        WHERE skill."userId" = ${input.userId}
        ORDER BY skill."id"
        FOR UPDATE OF skill
      `;
      const exactDuplicates = existingSkills
        .filter(
          (skill) =>
            normalizeSkillSimilarityText(skill.title) ===
              normalizeSkillSimilarityText(generated.draft.title) &&
            normalizeSkillSimilarityText(skill.objective ?? "") ===
              normalizeSkillSimilarityText(generated.draft.objective),
        )
        .toSorted((left, right) => left.id.localeCompare(right.id));
      const duplicate = exactDuplicates[0];
      const storedDuplicateMatch = readJsonObject(
        baseGenerationMetadata.duplicateMatch,
      );
      const preserveReviewedSeparateDraft =
        (storedDuplicateMatch.userOverride === true ||
          storedDuplicateMatch.preserveGeneratedDraft === true) &&
        typeof storedDuplicateMatch.skillId === "string" &&
        exactDuplicates.some(
          (skill) =>
            skill.id === storedDuplicateMatch.skillId &&
            storedDuplicateMatch.skillFingerprint ===
              buildSkillDuplicateReviewFingerprint(skill),
        );
      if (duplicate && !preserveReviewedSeparateDraft) {
        await tx.skillDraftBatchItem.update({
          where: { id: item.id },
          data: {
            status: SkillDraftBatchItemStatus.EXCLUDED,
            generationClaimId: null,
            overlapSkillId: duplicate.id,
            errorCode: "DUPLICATE_USE_EXISTING",
            errorMessage: duplicateUseExistingMessage(duplicate),
            generationMetadata: toInputJson({
              ...baseGenerationMetadata,
              model: ai.model,
              verification: "verified",
              duplicatePrevented: true,
              duplicateMatch: {
                skillId: duplicate.id,
                skillFingerprint:
                  buildSkillDuplicateReviewFingerprint(duplicate),
                confidence: "exact",
                score: 1,
                userOverride: false,
              },
              scopeBoundaries,
              targetRepair: targetRepairState,
            }),
          },
        });
        return { status: "duplicate" as const, skillId: duplicate.id };
      }
      const skill = await tx.skill.create({
        data: {
          userId: input.userId,
          collectionId: item.batch.materialRevision.material.collectionId,
          title: generated.draft.title,
          objective: generated.draft.objective,
          rules: { items: generated.draft.rules },
          examples: { items: generated.draft.examples },
          exerciseConstraints: {
            notes: generated.draft.exerciseConstraints,
            answerKind: "choice",
            requestedCount: REQUESTED_ACTIVATION_EXERCISES,
          },
          tags: generated.draft.tags,
          status: SkillStatus.DRAFT,
        },
      });
      await tx.skillSourceRef.create({
        data: {
          userId: input.userId,
          skillId: skill.id,
          sourceFileId: sourceFile.id,
          locator: toInputJson(locator.data),
        },
      });
      await tx.skillDraftBatchItem.update({
        where: { id: item.id },
        data: {
          skillId: skill.id,
          status: SkillDraftBatchItemStatus.READY,
          generationClaimId: null,
          generationMetadata: toInputJson({
            ...baseGenerationMetadata,
            model: ai.model,
            verification: "verified",
            attemptsThisRun: totalDraftAttempts,
            scopeBoundaries,
            targetRepair: targetRepairState,
          }),
        },
      });
      await tx.studyMaterial.update({
        where: { id: item.batch.materialRevision.materialId },
        data: { lastUsedAt: now },
      });
      return { status: "created" as const, skillId: skill.id };
    });
    if (saved.status === "superseded") {
      return { status: "not-claimed" as const };
    }
    await reconcileMaterialDraftBatch({ userId: input.userId, batchId: input.batchId, now });
    return saved.status === "duplicate"
      ? { status: "excluded" as const, duplicateSkillId: saved.skillId }
      : { status: "ready" as const, skillId: saved.skillId, alreadyGenerated: false };
  } catch (error) {
    if (error instanceof MaterialDraftClaimSupersededError) {
      return { status: "not-claimed" as const };
    }
    const normalized = normalizeMaterialDraftError(error);
    const releaseForRetry = shouldRetryMaterialDraftGeneration({
      retryable: normalized.retryable,
      attempt: input.attempt,
      maxAttempts: input.maxAttempts,
      retryTransientOnWorkerDeadline: input.retryTransientOnWorkerDeadline,
    });
    const marked = releaseForRetry
      ? await releaseMaterialDraftItemForRetry({
          userId: input.userId,
          itemId: item.id,
          claimId,
          code: "TRANSIENT_GENERATION_FAILURE",
          message: normalized.message,
        })
      : await markMaterialDraftItemFailed({
          userId: input.userId,
          itemId: item.id,
          claimId,
          code: normalized.retryable ? "TRANSIENT_GENERATION_FAILURE" : "GENERATION_REJECTED",
          message: normalized.message,
        });
    if (!marked) {
      return { status: "not-claimed" as const };
    }
    await reconcileMaterialDraftBatch({ userId: input.userId, batchId: input.batchId, now });
    throw normalized;
  }
}

export function getMaterialDraftGenerationClaimId(input: {
  itemId: string;
  requestedAt?: string;
  status: SkillDraftBatchItemStatus;
  existingClaimId?: string | null;
}) {
  if (input.requestedAt) {
    return createHash("sha256")
      .update(`${input.itemId}\u0000${input.requestedAt}`)
      .digest("hex");
  }
  if (
    input.status === SkillDraftBatchItemStatus.PLANNED &&
    input.existingClaimId
  ) {
    return input.existingClaimId;
  }
  return randomUUID();
}

export function shouldRetryMaterialDraftGeneration(input: {
  retryable: boolean;
  attempt?: number;
  maxAttempts?: number;
  retryTransientOnWorkerDeadline?: boolean;
}) {
  return input.retryable && (
    input.retryTransientOnWorkerDeadline === true ||
    (input.attempt !== undefined &&
      input.maxAttempts !== undefined &&
      input.attempt + 1 < input.maxAttempts)
  );
}

export async function retryMaterialDraftItem(input: {
  userId: string;
  batchId: string;
  itemId: string;
  now: Date;
  automatic?: boolean;
  eventSender?: MaterialDraftItemEventSender;
}) {
  const env = getJobsEnvStatus();
  if (env.status === "missing-env" && !input.eventSender) {
    return { status: "not-queued" as const, message: env.message };
  }
  const prisma = getPrisma();
  const failedItem = await prisma.skillDraftBatchItem.findFirst({
    where: {
      id: input.itemId,
      batchId: input.batchId,
      userId: input.userId,
      status: SkillDraftBatchItemStatus.FAILED,
      NOT: { errorCode: { startsWith: "ACTIVATION_" } },
    },
    select: { errorCode: true, errorMessage: true, generationMetadata: true },
  });
  if (!failedItem) {
    return { status: "not-found" as const, message: "Failed draft item was not found." };
  }
  const generationMetadata = readJsonObject(failedItem.generationMetadata);
  const targetRepairMetadata = readJsonObject(generationMetadata.targetRepair);
  if (
    input.automatic &&
    (failedItem.errorCode !== "VERIFICATION_REJECTED" ||
      targetRepairMetadata.status === "exhausted")
  ) {
    return { status: "not-found" as const, message: "Automatic repair is no longer pending." };
  }
  const repairMetadata =
    failedItem.errorCode === "VERIFICATION_REJECTED" && failedItem.errorMessage
      ? toInputJson({
          ...generationMetadata,
          targetRepair: {
            required: true,
            verificationNote: failedItem.errorMessage,
          },
        })
      : null;
  const updated = await prisma.skillDraftBatchItem.updateMany({
    where: {
      id: input.itemId,
      batchId: input.batchId,
      userId: input.userId,
      status: SkillDraftBatchItemStatus.FAILED,
      NOT: { errorCode: { startsWith: "ACTIVATION_" } },
    },
    data: {
      status: SkillDraftBatchItemStatus.PLANNED,
      generationClaimId: null,
      errorCode: null,
      errorMessage: null,
      ...(repairMetadata ? { generationMetadata: repairMetadata } : {}),
    },
  });
  if (updated.count !== 1) {
    return { status: "not-found" as const, message: "Failed draft item was not found." };
  }
  try {
    await (input.eventSender ?? awsMaterialDraftItemEventSender).sendMaterialDraftItemRequested({
      userId: input.userId,
      batchId: input.batchId,
      itemId: input.itemId,
      requestedAt: input.now.toISOString(),
    });
  } catch (error) {
    console.error("[background-jobs] material draft event send failed", {
      batchId: input.batchId,
      itemId: input.itemId,
      error: getEventSendErrorLogDetails(error),
    });
    await markMaterialDraftItemFailed({
      userId: input.userId,
      itemId: input.itemId,
      code: "EVENT_SEND_FAILED",
      message: "Background processing was unavailable. Retry this item.",
    });
    return {
      status: "not-queued" as const,
      message: "Background processing was unavailable. Try again in a moment.",
    };
  }
  await reconcileMaterialDraftBatch({ userId: input.userId, batchId: input.batchId, now: input.now });
  return { status: "queued" as const };
}

export async function queueMaterialBatchActivation(input: {
  userId: string;
  input: unknown;
  now: Date;
  eventSender?: MaterialBatchActivationEventSender;
  embeddingGenerator?: SkillSimilarityEmbeddingGenerator | null;
}) {
  const parsed = activateBatchInputSchema.safeParse(input.input);
  if (!parsed.success) {
    return {
      status: "invalid" as const,
      message: `Choose between one and ${MAX_SKILLS_PER_BATCH} ready skills to add.`,
      fieldErrors: parsed.error.flatten().fieldErrors,
    };
  }
  const env = getJobsEnvStatus();
  if (env.status === "missing-env" && !input.eventSender) {
    return { status: "not-queued" as const, message: env.message };
  }
  const prisma = getPrisma();
  const candidateItems = await prisma.skillDraftBatchItem.findMany({
    where: {
      id: { in: parsed.data.itemIds },
      batchId: parsed.data.batchId,
      userId: input.userId,
      status: SkillDraftBatchItemStatus.READY,
      skill: { status: SkillStatus.DRAFT },
    },
    select: {
      id: true,
      skill: {
        select: { id: true, title: true, objective: true },
      },
    },
  });
  const similarity = await findSimilarSkillsForUser({
    userId: input.userId,
    candidates: candidateItems.flatMap((item) =>
      item.skill
        ? [
            {
              key: item.id,
              skillId: item.skill.id,
              title: item.skill.title,
              objective: item.skill.objective,
            },
          ]
        : [],
    ),
    embeddingGenerator: input.embeddingGenerator,
  });
  const preflightMatchByItemId = new Map(
    similarity.candidates.map((candidate) => [
      candidate.key,
      candidate.bestMatch,
    ]),
  );
  const candidateSnapshotByItemId = new Map(
    candidateItems.flatMap((item) =>
      item.skill
        ? [
            [
              item.id,
              {
                title: item.skill.title,
                objective: item.skill.objective,
              },
            ] as const,
          ]
        : [],
    ),
  );
  const createSeparatelyMatchByItemId = new Map(
    (parsed.data.createSeparatelyMatches ?? []).map((match) => [
      match.itemId,
      match,
    ]),
  );
  const reserve = () => prisma.$transaction(async (tx) => {
    await tx.$queryRaw`
      SELECT "id" FROM "users"
      WHERE "id" = ${input.userId}
      FOR UPDATE
    `;
    const batch = await tx.skillDraftBatch.findFirst({
      where: { id: parsed.data.batchId, userId: input.userId, confirmedAt: { not: null } },
      select: { id: true },
    });
    if (!batch) {
      return { status: "not-found" as const, message: "Ready material batch was not found." };
    }
    await tx.$queryRaw(
      Prisma.sql`
        SELECT skill."id"
        FROM "skills" AS skill
        INNER JOIN "skill_draft_batch_items" AS item
          ON item."skillId" = skill."id"
        WHERE item."id" IN (${Prisma.join(parsed.data.itemIds)})
          AND item."batchId" = ${batch.id}
          AND item."userId" = ${input.userId}
        ORDER BY skill."id"
        FOR UPDATE OF skill
      `,
    );
    const items = await tx.skillDraftBatchItem.findMany({
      where: {
        id: { in: parsed.data.itemIds },
        batchId: batch.id,
        userId: input.userId,
      },
      select: {
        id: true,
        status: true,
        overlapSkillId: true,
        errorCode: true,
        generationMetadata: true,
        skill: {
          select: {
            id: true,
            title: true,
            objective: true,
            collectionId: true,
            rules: true,
            examples: true,
            exerciseConstraints: true,
            tags: true,
            status: true,
          },
        },
      },
    });
    if (items.length !== parsed.data.itemIds.length) {
      return {
        status: "invalid" as const,
        message: "Every selected item must still belong to this batch.",
      };
    }
    const readyItems = items.filter(
      (item) =>
        item.status === SkillDraftBatchItemStatus.READY &&
        item.skill?.status === SkillStatus.DRAFT,
    );
    const handledItems = items.filter(
      (item) =>
        (item.skill?.status === SkillStatus.ACTIVE &&
          (item.status === SkillDraftBatchItemStatus.READY ||
            item.status === SkillDraftBatchItemStatus.ACTIVATING ||
            item.status === SkillDraftBatchItemStatus.ACTIVE)) ||
        (item.status === SkillDraftBatchItemStatus.ACTIVATING &&
          item.skill?.status === SkillStatus.DRAFT),
    );
    if (readyItems.length + handledItems.length !== items.length) {
      return {
        status: "invalid" as const,
        message: "Every selected item must still be a ready draft in this batch.",
      };
    }
    if (readyItems.length === 0) {
      return {
        status: "already-queued" as const,
        batchId: batch.id,
        message: "These skills are already being added or are active.",
      };
    }
    const candidateInputsStillCurrent = readyItems.every((item) => {
      const snapshot = candidateSnapshotByItemId.get(item.id);
      return (
        snapshot &&
        item.skill &&
        snapshot.title === item.skill.title &&
        snapshot.objective === item.skill.objective
      );
    });
    if (!candidateInputsStillCurrent) {
      return {
        status: "invalid" as const,
        message:
          "A selected draft changed while LearnRecur checked for similar skills. Review the latest draft, then add it again.",
      };
    }
    const storedSkills = await tx.skill.findMany({
      where: { userId: input.userId },
      select: {
        id: true,
        title: true,
        objective: true,
        collectionId: true,
        rules: true,
        examples: true,
        exerciseConstraints: true,
        status: true,
        tags: true,
        collection: { select: { name: true } },
      },
    });
    const duplicateLibraryFingerprint =
      buildSkillDuplicateLibraryFingerprint(storedSkills);
    if (
      similarity.duplicateLibraryFingerprint &&
      duplicateLibraryFingerprint !==
        similarity.duplicateLibraryFingerprint
    ) {
      return {
        status: "invalid" as const,
        message:
          "Your skill library changed while LearnRecur checked for similar skills. Try adding these drafts again to review the latest matches.",
      };
    }
    const similarityPreviews: SkillSimilarityPreview[] = storedSkills.map(
      (skill) => ({
        id: skill.id,
        title: skill.title,
        objective: skill.objective,
        status: skill.status,
        tags: skill.tags,
        collectionName: skill.collection?.name ?? null,
        contentFingerprint:
          buildSkillDuplicateReviewFingerprint(skill),
      }),
    );
    const previewById = new Map(
      similarityPreviews.map((skill) => [skill.id, skill]),
    );
    const reviewItems: Array<{
      item: (typeof readyItems)[number];
      match: SkillSimilarityMatch;
    }> = [];
    const eligibleReadyItems: (typeof readyItems)[number][] = [];
    for (const item of readyItems) {
      if (!item.skill) {
        continue;
      }
      const createSeparatelyMatch =
        createSeparatelyMatchByItemId.get(item.id);
      const candidateFingerprint =
        buildSkillDuplicateCandidateFingerprint(item.skill);
      if (
        createSeparatelyMatch &&
        createSeparatelyMatch.candidateFingerprint !== candidateFingerprint
      ) {
        return {
          status: "invalid" as const,
          message:
            "This draft changed after duplicate review. Review the latest draft, then choose again.",
        };
      }
      const lexicalMatch =
        rankSkillSimilarityMatches({
          candidate: {
            title: item.skill.title,
            objective: item.skill.objective,
          },
          skills: similarityPreviews.filter(
            (skill) => skill.id !== item.skill?.id,
          ),
          limit: 1,
        })[0] ?? null;
      const snapshot = candidateSnapshotByItemId.get(item.id);
      const preflightMatch = preflightMatchByItemId.get(item.id) ?? null;
      const preflightStillCurrent =
        snapshot?.title === item.skill.title &&
        snapshot.objective === item.skill.objective &&
        preflightMatch &&
        previewById.get(preflightMatch.skill.id)?.title ===
          preflightMatch.skill.title &&
        previewById.get(preflightMatch.skill.id)?.objective ===
          preflightMatch.skill.objective &&
        previewById.get(preflightMatch.skill.id)?.contentFingerprint ===
          preflightMatch.skill.contentFingerprint;
      const match = chooseStrongerSkillSimilarityMatch(
        lexicalMatch,
        preflightStillCurrent ? preflightMatch : null,
      );
      if (!match) {
        if (
          item.errorCode === "DUPLICATE_REVIEW_REQUIRED" ||
          item.overlapSkillId
        ) {
          await tx.skillDraftBatchItem.update({
            where: { id: item.id },
            data: {
              overlapSkillId: null,
              errorCode: null,
              errorMessage: null,
              generationMetadata: toInputJson(
                withoutDuplicateMatchMetadata(item.generationMetadata),
              ),
            },
          });
        }
        eligibleReadyItems.push(item);
        continue;
      }
      const storedDuplicateMatch = readJsonObject(
        readJsonObject(item.generationMetadata).duplicateMatch,
      );
      const submittedOverrideStillCurrent =
        createSeparatelyMatch?.skillId === match.skill.id &&
        createSeparatelyMatch.skillFingerprint ===
          match.skill.contentFingerprint &&
        createSeparatelyMatch.candidateFingerprint === candidateFingerprint;
      const previouslyOverridden =
        storedDuplicateMatch.userOverride === true &&
        storedDuplicateMatch.skillId === match.skill.id &&
        storedDuplicateMatch.skillFingerprint ===
          match.skill.contentFingerprint &&
        (storedDuplicateMatch.candidateFingerprint === undefined ||
          (storedDuplicateMatch.candidateSkillId === item.skill.id &&
            storedDuplicateMatch.candidateFingerprint ===
              buildSkillDuplicateCandidateFingerprint(item.skill)));
      if (
        submittedOverrideStillCurrent ||
        previouslyOverridden
      ) {
        eligibleReadyItems.push(item);
        await tx.skillDraftBatchItem.update({
          where: { id: item.id },
          data: {
            overlapSkillId: match.skill.id,
            generationMetadata: toInputJson(
              withDuplicateMatchMetadata(
                item.generationMetadata,
                match,
                item.skill,
                true,
              ),
            ),
          },
        });
        continue;
      }
      reviewItems.push({ item, match });
      await tx.skillDraftBatchItem.update({
        where: { id: item.id },
        data: {
          overlapSkillId: match.skill.id,
          errorCode: "DUPLICATE_REVIEW_REQUIRED",
          errorMessage: duplicateReviewMessage(match),
          generationMetadata: toInputJson(
            withDuplicateMatchMetadata(
              item.generationMetadata,
              match,
              item.skill,
              false,
            ),
          ),
        },
      });
    }
    if (eligibleReadyItems.length === 0) {
      await reconcileMaterialDraftBatchWithClient(tx, {
        userId: input.userId,
        batchId: batch.id,
        now: input.now,
      });
      return {
        status: "review-required" as const,
        batchId: batch.id,
        reviewItemIds: reviewItems.map(({ item }) => item.id),
        message:
          reviewItems.length === 1
            ? "Review the similar skill before adding this draft."
            : "Review the similar skills before adding these drafts.",
      };
    }
    const activeJobs = await tx.generationJob.findMany({
      where: {
        userId: input.userId,
        skillId: {
          in: eligibleReadyItems.flatMap((item) =>
            item.skill ? [item.skill.id] : [],
          ),
        },
        kind: GenerationJobKind.SKILL_ACTIVATION,
        status: { in: [GenerationJobStatus.PENDING, GenerationJobStatus.RUNNING] },
      },
      select: { id: true, skillId: true },
    });
    const activeJobBySkillId = new Map(
      activeJobs.flatMap((job) => (job.skillId ? [[job.skillId, job] as const] : [])),
    );
    const alreadyActivatingItems = eligibleReadyItems.filter(
      (item) => item.skill && activeJobBySkillId.has(item.skill.id),
    );
    const reservableItems = eligibleReadyItems.filter(
      (item) => !item.skill || !activeJobBySkillId.has(item.skill.id),
    );
    for (const item of alreadyActivatingItems) {
      const activeJob = item.skill ? activeJobBySkillId.get(item.skill.id) : null;
      if (!activeJob) {
        continue;
      }
      await tx.skillDraftBatchItem.updateMany({
        where: {
          id: item.id,
          userId: input.userId,
          status: SkillDraftBatchItemStatus.READY,
        },
        data: {
          status: SkillDraftBatchItemStatus.ACTIVATING,
          generationClaimId: `existing-job:${activeJob.id}`,
          errorCode: null,
          errorMessage: null,
        },
      });
    }
    if (reservableItems.length === 0) {
      await reconcileMaterialDraftBatchWithClient(tx, {
        userId: input.userId,
        batchId: batch.id,
        now: input.now,
      });
      return {
        status: "already-queued" as const,
        batchId: batch.id,
        message: "These skills are already being added or are active.",
      };
    }
    const usage = await getSkillActivationUsage({
      userId: input.userId,
      now: input.now,
      prisma: tx,
    });
    if (usage.countedSkillCount + reservableItems.length > usage.activeSkillLimit) {
      await reconcileMaterialDraftBatchWithClient(tx, {
        userId: input.userId,
        batchId: batch.id,
        now: input.now,
      });
      return {
        status: "limited" as const,
        code: "active-skill-limit" as const,
        message: `Adding ${reservableItems.length} skill${reservableItems.length === 1 ? "" : "s"} would exceed the ${usage.activeSkillLimit}-skill library limit, including skills already reserved. Exclude some drafts or archive existing skills.`,
      };
    }
    if (usage.activationsUsedToday + reservableItems.length > usage.dailyActivationLimit) {
      const remaining = Math.max(
        0,
        usage.dailyActivationLimit - usage.activationsUsedToday,
      );
      await reconcileMaterialDraftBatchWithClient(tx, {
        userId: input.userId,
        batchId: batch.id,
        now: input.now,
      });
      return {
        status: "limited" as const,
        code: "daily-activation-limit" as const,
        message: `Only ${remaining} activation${remaining === 1 ? "" : "s"} remain today. Select fewer skills or try again after ${usage.nextUtcResetAt.toISOString()} UTC.`,
      };
    }

    const model = process.env.GEMINI_MODEL?.trim() || DEFAULT_GEMINI_MODEL;
    const currentReservationMetadata = new Map(
      (
        await tx.skillDraftBatchItem.findMany({
          where: {
            id: { in: reservableItems.map((item) => item.id) },
            batchId: batch.id,
            userId: input.userId,
          },
          select: { id: true, generationMetadata: true },
        })
      ).map((item) => [item.id, item.generationMetadata]),
    );
    const byId = new Map(reservableItems.map((item) => [item.id, item]));
    const reservations = [];
    for (const itemId of parsed.data.itemIds) {
      const item = byId.get(itemId);
      if (!item) {
        continue;
      }
      if (!item.skill) {
        throw new Error("Selected activation skill disappeared while reserving quota.");
      }
      const generationJob = await tx.generationJob.create({
        data: {
          userId: input.userId,
          skillId: item.skill.id,
          kind: GenerationJobKind.SKILL_ACTIVATION,
          status: GenerationJobStatus.PENDING,
          provider: GEMINI_PROVIDER,
          model,
          promptVersion: SKILL_MCQ_PROMPT_VERSION,
          requestedCount: REQUESTED_ACTIVATION_EXERCISES,
          createdAt: input.now,
        },
        select: { id: true },
      });
      await tx.skillDraftBatchItem.update({
        where: { id: item.id },
        data: {
          status: SkillDraftBatchItemStatus.ACTIVATING,
          errorCode: null,
          errorMessage: null,
          generationMetadata: toInputJson({
            ...readJsonObject(currentReservationMetadata.get(item.id)),
            activationDraftFingerprint:
              buildSkillDuplicateCandidateFingerprint(item.skill),
            activationDuplicateLibraryFingerprint:
              duplicateLibraryFingerprint,
          }),
        },
      });
      reservations.push({ itemId: item.id, generationJobId: generationJob.id });
    }
    await tx.skillDraftBatch.update({
      where: { id: batch.id },
      data: {
        status: SkillDraftBatchStatus.ACTIVATING,
        completedAt: null,
        errorCode: null,
        errorMessage: null,
      },
    });
    await reconcileMaterialDraftBatchWithClient(tx, {
      userId: input.userId,
      batchId: batch.id,
      now: input.now,
    });
    return {
      status: "reserved" as const,
      batchId: batch.id,
      reservations,
      reviewItemIds: reviewItems.map(({ item }) => item.id),
    };
  });
  let reservation: Awaited<ReturnType<typeof reserve>>;
  try {
    reservation = await reserve();
  } catch (error) {
    if (!isPrismaUniqueConstraintError(error)) {
      throw error;
    }
    reservation = await reserve();
  }

  if (reservation.status !== "reserved") {
    return reservation;
  }
  const sender = input.eventSender ?? awsMaterialBatchActivationEventSender;
  const payloads = reservation.reservations.map((item) => ({
    userId: input.userId,
    batchId: reservation.batchId,
    itemId: item.itemId,
    generationJobId: item.generationJobId,
    requestedAt: input.now.toISOString(),
  }));
  const sendResults = await Promise.allSettled(
    payloads.map((payload) => sender.sendMaterialBatchActivationRequested(payload)),
  );
  const failed = sendResults.flatMap((result, index) =>
    result.status === "rejected" ? [payloads[index]] : [],
  );
  let failedItemIds: string[] = [];
  if (failed.length > 0) {
    failedItemIds = await failUnclaimedMaterialBatchActivationReservations({
      userId: input.userId,
      batchId: reservation.batchId,
      reservations: failed,
      message: "Activation could not be queued. Retry this item.",
      now: input.now,
    });
  }
  return {
    status:
      failedItemIds.length > 0 || reservation.reviewItemIds.length > 0
        ? ("partial" as const)
        : ("queued" as const),
    batchId: reservation.batchId,
    queuedItemIds: payloads
      .map((payload) => payload.itemId)
      .filter((itemId) => !failedItemIds.includes(itemId)),
    failedItemIds,
    reviewItemIds: reservation.reviewItemIds,
  };
}

export async function runMaterialBatchActivationJob(input: {
  userId: string;
  batchId: string;
  itemId: string;
  generationJobId: string;
  requestedAt?: string;
  attempt?: number;
  maxAttempts?: number;
  now?: Date;
  deadlineAt?: Date;
  generateChoiceExercises?: ChoiceExerciseGenerator;
  verifyChoiceExercises?: ChoiceExerciseVerifier;
  sourceMediaLoader?: SourceMediaContextLoader;
  sourceEvidenceLoader?: SkillSourceEvidenceLoader;
  sourceStorage?: SourceObjectStorage;
  model?: string;
}) {
  const prisma = getPrisma();
  const now = input.now ?? new Date();
  const item = await prisma.skillDraftBatchItem.findFirst({
    where: { id: input.itemId, batchId: input.batchId, userId: input.userId },
    select: {
      id: true,
      status: true,
      errorCode: true,
      skill: { select: { id: true, status: true } },
    },
  });
  if (!item?.skill) {
    return { status: "not-found" as const };
  }
  if (item.status === SkillDraftBatchItemStatus.ACTIVE && item.skill.status === SkillStatus.ACTIVE) {
    return { status: "active" as const, alreadyActivated: true, skillId: item.skill.id };
  }
  if (item.skill.status === SkillStatus.ACTIVE) {
    await markMaterialBatchItemActive({
      userId: input.userId,
      batchId: input.batchId,
      itemId: item.id,
      now,
    });
    return { status: "active" as const, alreadyActivated: true, skillId: item.skill.id };
  }
  const slot = await claimMaterialBatchActivationSlot({
    userId: input.userId,
    batchId: input.batchId,
    itemId: item.id,
    generationJobId: input.generationJobId,
    now,
  });
  if (slot.status === "limited") {
    await reconcileMaterialDraftBatch({
      userId: input.userId,
      batchId: input.batchId,
      now,
    });
    throw new MaterialBatchActivationError(slot.message, { retryable: true });
  }
  if (slot.status !== "ready") {
    return { status: "not-claimed" as const };
  }

  let result: Awaited<ReturnType<typeof activateSkillDraft>>;
  try {
    result = await activateSkillDraft({
      userId: input.userId,
      skillId: slot.skillId,
      generationJobId: slot.generationJobId,
      now,
      deadlineAt: input.deadlineAt,
      generateChoiceExercises: input.generateChoiceExercises,
      verifyChoiceExercises: input.verifyChoiceExercises,
      sourceMediaLoader: input.sourceMediaLoader,
      sourceEvidenceLoader:
        input.sourceEvidenceLoader ??
        ((evidenceInput) =>
          loadLocalizedMaterialEvidence({
            ...evidenceInput,
            storage: input.sourceStorage,
          })),
      model: input.model,
      expectedDraftFingerprint: slot.expectedDraftFingerprint,
      expectedDuplicateLibraryFingerprint:
        slot.expectedDuplicateLibraryFingerprint,
      expectedDuplicateMatch: slot.expectedDuplicateMatch,
      skipUsageLimitCheck: true,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Skill activation failed.";
    await prisma.generationJob.updateMany({
      where: {
        id: slot.generationJobId,
        userId: input.userId,
        status: { in: [GenerationJobStatus.PENDING, GenerationJobStatus.RUNNING] },
      },
      data: {
        status: GenerationJobStatus.FAILED,
        stage: GenerationJobStage.FAILED,
        failureCategory: GenerationFailureCategory.UNKNOWN,
        errorMessage: message.slice(0, 1_000),
        completedAt: now,
      },
    });
    const marked = hasAutomaticActivationRetryRemaining(input)
      ? await releaseMaterialBatchActivationForRetry({
          userId: input.userId,
          batchId: input.batchId,
          itemId: item.id,
          claimId: slot.claimId,
          message,
          now,
        })
      : await markMaterialBatchActivationFailed({
          userId: input.userId,
          batchId: input.batchId,
          itemId: item.id,
          claimId: slot.claimId,
          code: "ACTIVATION_RETRYABLE_UNEXPECTED_FAILURE",
          message,
          now,
        });
    if (!marked) {
      return { status: "not-claimed" as const };
    }
    throw new MaterialBatchActivationError(message, { retryable: true, cause: error });
  }
  if (result.status === "activated") {
    return {
      status: "active" as const,
      alreadyActivated: false,
      skillId: result.skillId,
      exerciseCount: result.exerciseCount,
    };
  }
  if (result.reason === "draft-changed") {
    const message =
      "This draft changed after it was queued. Review the latest version, then add it again. LearnRecur will check for duplicates first.";
    const released = await releaseMaterialBatchActivationForDraftReview({
      userId: input.userId,
      batchId: input.batchId,
      itemId: item.id,
      skillId: slot.skillId,
      generationJobId:
        result.generationJobId ?? slot.generationJobId,
      claimId: slot.claimId,
      message,
      now,
    });
    if (!released) {
      return { status: "not-claimed" as const };
    }
    return {
      status: "failed" as const,
      reason: result.reason,
      message,
    };
  }
  if (result.reason === "duplicate-review-changed") {
    const message =
      "Your skill library changed after the duplicate review. Compare the latest matches, then choose again.";
    const released = await releaseMaterialBatchActivationForDraftReview({
      userId: input.userId,
      batchId: input.batchId,
      itemId: item.id,
      skillId: slot.skillId,
      generationJobId:
        result.generationJobId ?? slot.generationJobId,
      claimId: slot.claimId,
      errorCode: "DUPLICATE_REVIEW_REQUIRED",
      message,
      now,
    });
    if (!released) {
      return { status: "not-claimed" as const };
    }
    return {
      status: "failed" as const,
      reason: result.reason,
      message,
    };
  }
  if (result.reason === "activation-superseded") {
    await releaseMaterialBatchActivationAfterSuperseded({
      userId: input.userId,
      batchId: input.batchId,
      itemId: item.id,
      claimId: slot.claimId,
      message:
        "Another activation attempt took over. You can wait for it to finish or add this draft again.",
      now,
    });
    return { status: "not-claimed" as const };
  }
  const reason = result.reason.toUpperCase().replaceAll("-", "_");
  const retryable = [
    "generation-failed",
    "verification-failed",
    "activation-in-progress",
  ].includes(result.reason);
  const marked =
    retryable && hasAutomaticActivationRetryRemaining(input)
      ? await releaseMaterialBatchActivationForRetry({
          userId: input.userId,
          batchId: input.batchId,
          itemId: item.id,
          claimId: slot.claimId,
          message: result.message,
          now,
        })
      : await markMaterialBatchActivationFailed({
          userId: input.userId,
          batchId: input.batchId,
          itemId: item.id,
          claimId: slot.claimId,
          code: `ACTIVATION_${retryable ? "RETRYABLE_" : ""}${reason}`,
          message: result.message,
          now,
        });
  if (!marked) {
    return { status: "not-claimed" as const };
  }
  if (retryable) {
    throw new MaterialBatchActivationError(result.message, { retryable: true });
  }
  return { status: "failed" as const, reason: result.reason, message: result.message };
}

async function claimMaterialBatchActivationSlot(input: {
  userId: string;
  batchId: string;
  itemId: string;
  generationJobId: string;
  now: Date;
}) {
  return getPrisma().$transaction(async (tx) => {
    await tx.$queryRaw`
      SELECT "id" FROM "users"
      WHERE "id" = ${input.userId}
      FOR UPDATE
    `;
    const item = await tx.skillDraftBatchItem.findFirst({
      where: { id: input.itemId, batchId: input.batchId, userId: input.userId },
      select: {
        id: true,
        status: true,
        errorCode: true,
        generationClaimId: true,
        generationMetadata: true,
        updatedAt: true,
        skill: { select: { id: true, status: true } },
      },
    });
    if (!item?.skill || item.skill.status !== SkillStatus.DRAFT) {
      return { status: "not-claimed" as const };
    }
    const retryingTransientFailure =
      item.status === SkillDraftBatchItemStatus.FAILED &&
      item.errorCode?.startsWith("ACTIVATION_RETRYABLE_");
    if (item.status !== SkillDraftBatchItemStatus.ACTIVATING && !retryingTransientFailure) {
      return { status: "not-claimed" as const };
    }
    if (
      item.status === SkillDraftBatchItemStatus.ACTIVATING &&
      item.generationClaimId &&
      input.now.getTime() - item.updatedAt.getTime() < MATERIAL_BATCH_ACTIVATION_CLAIM_STALE_MS
    ) {
      return { status: "not-claimed" as const };
    }
    let job = await tx.generationJob.findFirst({
      where: {
        id: input.generationJobId,
        userId: input.userId,
        skillId: item.skill.id,
        kind: GenerationJobKind.SKILL_ACTIVATION,
        status: {
          in: [
            GenerationJobStatus.PENDING,
            GenerationJobStatus.RUNNING,
            GenerationJobStatus.FAILED,
          ],
        },
      },
      select: {
        id: true,
        status: true,
        startedAt: true,
        errorMessage: true,
        provider: true,
        model: true,
        promptVersion: true,
        requestedCount: true,
      },
    });
    let supersededJobSource: typeof job = null;
    if (
      job?.status === GenerationJobStatus.FAILED &&
      isSupersededActivationJobMessage(job.errorMessage)
    ) {
      supersededJobSource = job;
      job = await tx.generationJob.findFirst({
        where: {
          userId: input.userId,
          skillId: item.skill.id,
          kind: GenerationJobKind.SKILL_ACTIVATION,
          id: { not: job.id },
          status: {
            in: [
              GenerationJobStatus.PENDING,
              GenerationJobStatus.RUNNING,
              GenerationJobStatus.FAILED,
            ],
          },
        },
        orderBy: [{ createdAt: "desc" }, { id: "desc" }],
        select: {
          id: true,
          status: true,
          startedAt: true,
          errorMessage: true,
          provider: true,
          model: true,
          promptVersion: true,
          requestedCount: true,
        },
      });
      if (
        job?.status === GenerationJobStatus.FAILED &&
        isSupersededActivationJobMessage(job.errorMessage)
      ) {
        supersededJobSource = job;
        job = null;
      }
    }
    if (
      (!job && !supersededJobSource) ||
      (job?.status === GenerationJobStatus.RUNNING &&
        job.startedAt &&
        input.now.getTime() - job.startedAt.getTime() <
          MATERIAL_BATCH_ACTIVATION_CLAIM_STALE_MS)
    ) {
      return { status: "not-claimed" as const };
    }
    const usage = await getSkillActivationUsage({
      userId: input.userId,
      now: input.now,
      prisma: tx,
    });
    const projectedSkillCount =
      usage.countedSkillCount + (retryingTransientFailure ? 1 : 0);
    if (projectedSkillCount > usage.activeSkillLimit) {
      if (item.status === SkillDraftBatchItemStatus.ACTIVATING) {
        await tx.skillDraftBatchItem.update({
          where: { id: item.id },
          data: {
            status: SkillDraftBatchItemStatus.FAILED,
            errorCode: "ACTIVATION_RETRYABLE_ACTIVE_SKILL_LIMIT",
            errorMessage: `Activation would exceed the ${usage.activeSkillLimit}-skill library limit. Archive a skill and retry.`,
          },
        });
      }
      if (job) {
        await tx.generationJob.update({
          where: { id: job.id },
          data: {
            status: GenerationJobStatus.FAILED,
            stage: GenerationJobStage.FAILED,
            failureCategory: GenerationFailureCategory.CANCELED,
            errorMessage:
              job.status === GenerationJobStatus.RUNNING
                ? ACTIVATION_SUPERSEDED_JOB_MESSAGE
                : `Activation would exceed the ${usage.activeSkillLimit}-skill library limit.`,
            completedAt: input.now,
          },
        });
      }
      return {
        status: "limited" as const,
        message: `Activation would exceed the ${usage.activeSkillLimit}-skill library limit. Archive a skill and retry.`,
      };
    }
    let claimedGenerationJobId: string;
    if (!job) {
      if (!supersededJobSource) {
        return { status: "not-claimed" as const };
      }
      const replacement = await tx.generationJob.create({
        data: {
          userId: input.userId,
          skillId: item.skill.id,
          kind: GenerationJobKind.SKILL_ACTIVATION,
          status: GenerationJobStatus.PENDING,
          provider: supersededJobSource.provider,
          model: supersededJobSource.model,
          promptVersion: supersededJobSource.promptVersion,
          requestedCount: supersededJobSource.requestedCount,
          createdAt: input.now,
        },
        select: { id: true },
      });
      claimedGenerationJobId = replacement.id;
    } else if (job.status === GenerationJobStatus.RUNNING) {
      const superseded = await tx.generationJob.updateMany({
        where: {
          id: job.id,
          userId: input.userId,
          skillId: item.skill.id,
          kind: GenerationJobKind.SKILL_ACTIVATION,
          status: GenerationJobStatus.RUNNING,
        },
        data: {
          status: GenerationJobStatus.FAILED,
          stage: GenerationJobStage.FAILED,
          failureCategory: GenerationFailureCategory.CANCELED,
          errorMessage: ACTIVATION_SUPERSEDED_JOB_MESSAGE,
          completedAt: input.now,
        },
      });
      if (superseded.count !== 1) {
        return { status: "not-claimed" as const };
      }
      const replacement = await tx.generationJob.create({
        data: {
          userId: input.userId,
          skillId: item.skill.id,
          kind: GenerationJobKind.SKILL_ACTIVATION,
          status: GenerationJobStatus.PENDING,
          provider: job.provider,
          model: job.model,
          promptVersion: job.promptVersion,
          requestedCount: job.requestedCount,
          createdAt: input.now,
        },
        select: { id: true },
      });
      claimedGenerationJobId = replacement.id;
    } else {
      claimedGenerationJobId = job.id;
    }
    const claimId = randomUUID();
    const keepAutomaticRetryState =
      item.errorCode === "ACTIVATION_RETRYING_TRANSIENT_FAILURE";
    await tx.skillDraftBatchItem.update({
      where: { id: item.id },
      data: {
        status: SkillDraftBatchItemStatus.ACTIVATING,
        generationClaimId: claimId,
        ...(keepAutomaticRetryState ? {} : { errorCode: null, errorMessage: null }),
      },
    });
    const generationMetadata = readJsonObject(item.generationMetadata);
    const activationDraftFingerprint =
      generationMetadata.activationDraftFingerprint;
    const activationDuplicateLibraryFingerprint =
      generationMetadata.activationDuplicateLibraryFingerprint;
    const storedDuplicateMatch = readJsonObject(
      generationMetadata.duplicateMatch,
    );
    const expectedDuplicateMatch =
      storedDuplicateMatch.userOverride === true &&
      typeof storedDuplicateMatch.skillId === "string" &&
      typeof storedDuplicateMatch.skillFingerprint === "string"
        ? {
            skillId: storedDuplicateMatch.skillId,
            fingerprint: storedDuplicateMatch.skillFingerprint,
          }
        : undefined;
    return {
      status: "ready" as const,
      skillId: item.skill.id,
      claimId,
      generationJobId: claimedGenerationJobId,
      expectedDraftFingerprint:
        typeof activationDraftFingerprint === "string"
          ? activationDraftFingerprint
          : undefined,
      expectedDuplicateLibraryFingerprint:
        typeof activationDuplicateLibraryFingerprint === "string"
          ? activationDuplicateLibraryFingerprint
          : undefined,
      expectedDuplicateMatch,
    };
  });
}

export async function retryMaterialBatchActivationItem(input: {
  userId: string;
  batchId: string;
  itemId: string;
  now: Date;
  eventSender?: MaterialBatchActivationEventSender;
}) {
  const parsed = batchItemMutationInputSchema.safeParse({
    batchId: input.batchId,
    itemId: input.itemId,
  });
  if (!parsed.success) {
    return { status: "invalid" as const, message: "Activation item was invalid." };
  }
  const env = getJobsEnvStatus();
  if (env.status === "missing-env" && !input.eventSender) {
    return { status: "not-queued" as const, message: env.message };
  }
  const prisma = getPrisma();
  const retry = await prisma.$transaction(async (tx) => {
    await tx.$queryRaw`
      SELECT "id" FROM "users"
      WHERE "id" = ${input.userId}
      FOR UPDATE
    `;
    const item = await tx.skillDraftBatchItem.findFirst({
      where: {
        id: parsed.data.itemId,
        batchId: parsed.data.batchId,
        userId: input.userId,
        status: SkillDraftBatchItemStatus.FAILED,
        errorCode: { startsWith: "ACTIVATION_" },
      },
      select: { id: true, skill: { select: { id: true, status: true } } },
    });
    if (!item?.skill || item.skill.status !== SkillStatus.DRAFT) {
      return { status: "not-found" as const };
    }
    const job = await tx.generationJob.findFirst({
      where: {
        userId: input.userId,
        skillId: item.skill.id,
        kind: GenerationJobKind.SKILL_ACTIVATION,
        status: {
          in: [
            GenerationJobStatus.FAILED,
            GenerationJobStatus.PENDING,
            GenerationJobStatus.RUNNING,
          ],
        },
      },
      orderBy: { createdAt: "desc" },
      select: {
        id: true,
        status: true,
        startedAt: true,
        updatedAt: true,
        errorMessage: true,
        provider: true,
        model: true,
        promptVersion: true,
        requestedCount: true,
      },
    });
    if (!job) {
      return { status: "not-found" as const };
    }
    const freshRunningJob =
      job.status === GenerationJobStatus.RUNNING &&
      job.startedAt !== null &&
      input.now.getTime() - job.startedAt.getTime() <
        MATERIAL_BATCH_ACTIVATION_CLAIM_STALE_MS;
    if (freshRunningJob) {
      const message =
        "Another activation attempt is already running. You can wait for it to finish or add this draft again.";
      await tx.skillDraftBatchItem.update({
        where: { id: item.id },
        data: {
          status: SkillDraftBatchItemStatus.READY,
          generationClaimId: null,
          errorCode: "ACTIVATION_SUPERSEDED",
          errorMessage: message,
        },
      });
      await reconcileMaterialDraftBatchWithClient(tx, {
        userId: input.userId,
        batchId: parsed.data.batchId,
        now: input.now,
      });
      return {
        status: "already-running" as const,
        itemId: item.id,
        generationJobId: job.id,
        message,
      };
    }
    const usage = await getSkillActivationUsage({
      userId: input.userId,
      now: input.now,
      prisma: tx,
    });
    if (usage.countedSkillCount >= usage.activeSkillLimit) {
      return {
        status: "limited" as const,
        message: `This retry would exceed the ${usage.activeSkillLimit}-skill library limit, including skills already reserved. Archive a skill or wait for an in-progress activation to finish.`,
      };
    }
    if (usage.activationsUsedToday >= usage.dailyActivationLimit) {
      return {
        status: "limited" as const,
        code: "daily-activation-limit" as const,
        message: `No activation attempts remain today. Try this draft again after ${usage.nextUtcResetAt.toISOString()} UTC.`,
      };
    }
    if (
      job.status === GenerationJobStatus.PENDING ||
      job.status === GenerationJobStatus.RUNNING
    ) {
      const superseded = await tx.generationJob.updateMany({
        where: {
          id: job.id,
          userId: input.userId,
          skillId: item.skill.id,
          kind: GenerationJobKind.SKILL_ACTIVATION,
          status: job.status,
          updatedAt: job.updatedAt,
        },
        data: {
          status: GenerationJobStatus.FAILED,
          stage: GenerationJobStage.FAILED,
          failureCategory: GenerationFailureCategory.CANCELED,
          errorMessage: ACTIVATION_SUPERSEDED_JOB_MESSAGE,
          completedAt: input.now,
        },
      });
      if (superseded.count !== 1) {
        return {
          status: "already-running" as const,
          itemId: item.id,
          generationJobId: job.id,
          message:
            "Another activation attempt took over. Wait for it to finish, then refresh this batch.",
        };
      }
    }
    const replacement = await tx.generationJob.create({
      data: {
        userId: input.userId,
        skillId: item.skill.id,
        kind: GenerationJobKind.SKILL_ACTIVATION,
        status: GenerationJobStatus.PENDING,
        provider: job.provider,
        model: job.model,
        promptVersion: job.promptVersion,
        requestedCount: job.requestedCount,
        createdAt: input.now,
      },
      select: { id: true },
    });
    await tx.skillDraftBatchItem.update({
      where: { id: item.id },
      data: {
        status: SkillDraftBatchItemStatus.ACTIVATING,
        generationClaimId: null,
        errorCode: null,
        errorMessage: null,
      },
    });
    await tx.skillDraftBatch.update({
      where: { id: parsed.data.batchId },
      data: { status: SkillDraftBatchStatus.ACTIVATING, completedAt: null },
    });
    return {
      status: "reserved" as const,
      itemId: item.id,
      generationJobId: replacement.id,
    };
  });
  if (retry.status === "not-found") {
    return { status: "not-found" as const, message: "Retryable activation was not found." };
  }
  if (retry.status === "limited") {
    return retry;
  }
  if (retry.status === "already-running") {
    return retry;
  }
  const payload = {
    userId: input.userId,
    batchId: parsed.data.batchId,
    itemId: retry.itemId,
    generationJobId: retry.generationJobId,
    requestedAt: input.now.toISOString(),
  };
  try {
    await (input.eventSender ?? awsMaterialBatchActivationEventSender)
      .sendMaterialBatchActivationRequested(payload);
  } catch {
    const [failedItemId] =
      await failUnclaimedMaterialBatchActivationReservations({
        userId: input.userId,
        batchId: parsed.data.batchId,
        reservations: [
          {
            itemId: retry.itemId,
            generationJobId: retry.generationJobId,
          },
        ],
        message: "Activation could not be queued. Try again.",
        now: input.now,
      });
    if (!failedItemId) {
      return {
        status: "already-running" as const,
        itemId: retry.itemId,
        generationJobId: retry.generationJobId,
        message:
          "Activation is already being processed. Wait for it to finish, then refresh this batch.",
      };
    }
    return { status: "not-queued" as const, message: "Activation could not be queued." };
  }
  return {
    status: "queued" as const,
    itemId: retry.itemId,
    generationJobId: retry.generationJobId,
  };
}

async function markMaterialBatchItemActive(input: {
  userId: string;
  batchId: string;
  itemId: string;
  now: Date;
}) {
  await getPrisma().skillDraftBatchItem.updateMany({
    where: { id: input.itemId, batchId: input.batchId, userId: input.userId },
    data: {
      status: SkillDraftBatchItemStatus.ACTIVE,
      generationClaimId: null,
      errorCode: null,
      errorMessage: null,
    },
  });
  await reconcileMaterialDraftBatch(input);
}

async function failUnclaimedMaterialBatchActivationReservations(input: {
  userId: string;
  batchId: string;
  reservations: Array<{ itemId: string; generationJobId: string }>;
  message: string;
  now: Date;
}) {
  if (input.reservations.length === 0) {
    return [];
  }
  return getPrisma().$transaction(async (tx) => {
    await tx.$queryRaw`
      SELECT "id"
      FROM "users"
      WHERE "id" = ${input.userId}
      FOR UPDATE
    `;
    await tx.$queryRaw(
      Prisma.sql`
        SELECT item."id"
        FROM "skill_draft_batch_items" AS item
        WHERE item."id" IN (${Prisma.join(
          input.reservations.map((reservation) => reservation.itemId),
        )})
          AND item."batchId" = ${input.batchId}
          AND item."userId" = ${input.userId}
        ORDER BY item."id"
        FOR UPDATE OF item
      `,
    );
    const items = await tx.skillDraftBatchItem.findMany({
      where: {
        id: {
          in: input.reservations.map((reservation) => reservation.itemId),
        },
        batchId: input.batchId,
        userId: input.userId,
        status: SkillDraftBatchItemStatus.ACTIVATING,
        generationClaimId: null,
      },
      select: { id: true, skillId: true },
    });
    const itemById = new Map(items.map((item) => [item.id, item]));
    const failedItemIds: string[] = [];
    for (const reservation of input.reservations) {
      const item = itemById.get(reservation.itemId);
      if (!item?.skillId) {
        continue;
      }
      const failedJob = await tx.generationJob.updateMany({
        where: {
          id: reservation.generationJobId,
          userId: input.userId,
          skillId: item.skillId,
          kind: GenerationJobKind.SKILL_ACTIVATION,
          status: GenerationJobStatus.PENDING,
        },
        data: {
          status: GenerationJobStatus.FAILED,
          stage: GenerationJobStage.FAILED,
          failureCategory: GenerationFailureCategory.CANCELED,
          errorMessage: "Activation could not be queued.",
          completedAt: input.now,
        },
      });
      if (failedJob.count !== 1) {
        continue;
      }
      const failedItem = await tx.skillDraftBatchItem.updateMany({
        where: {
          id: reservation.itemId,
          batchId: input.batchId,
          userId: input.userId,
          skillId: item.skillId,
          status: SkillDraftBatchItemStatus.ACTIVATING,
          generationClaimId: null,
        },
        data: {
          status: SkillDraftBatchItemStatus.FAILED,
          errorCode: "ACTIVATION_EVENT_SEND_FAILED",
          errorMessage: input.message.slice(0, 1_000),
        },
      });
      if (failedItem.count !== 1) {
        throw new Error(
          "Activation reservation changed while its send failure was recorded.",
        );
      }
      failedItemIds.push(reservation.itemId);
    }
    if (failedItemIds.length > 0) {
      await reconcileMaterialDraftBatchWithClient(tx, {
        userId: input.userId,
        batchId: input.batchId,
        now: input.now,
      });
    }
    return failedItemIds;
  });
}

async function markMaterialBatchActivationFailed(input: {
  userId: string;
  batchId: string;
  itemId: string;
  claimId?: string;
  code: string;
  message: string;
  now: Date;
}) {
  const updated = await getPrisma().skillDraftBatchItem.updateMany({
    where: {
      id: input.itemId,
      batchId: input.batchId,
      userId: input.userId,
      ...(input.claimId
        ? {
            status: SkillDraftBatchItemStatus.ACTIVATING,
            generationClaimId: input.claimId,
          }
        : {}),
    },
    data: {
      status: SkillDraftBatchItemStatus.FAILED,
      generationClaimId: null,
      errorCode: input.code,
      errorMessage: input.message.slice(0, 1_000),
    },
  });
  if (updated.count === 1) {
    await reconcileMaterialDraftBatch(input);
  }
  return updated.count === 1;
}

async function releaseMaterialBatchActivationForDraftReview(input: {
  userId: string;
  batchId: string;
  itemId: string;
  skillId: string;
  generationJobId: string;
  claimId: string;
  errorCode?: "ACTIVATION_DRAFT_CHANGED" | "DUPLICATE_REVIEW_REQUIRED";
  message: string;
  now: Date;
}) {
  const released = await getPrisma().$transaction(async (tx) => {
    await tx.$queryRaw`
      SELECT "id"
      FROM "users"
      WHERE "id" = ${input.userId}
      FOR UPDATE
    `;
    const updated = await tx.skillDraftBatchItem.updateMany({
      where: {
        id: input.itemId,
        batchId: input.batchId,
        userId: input.userId,
        skillId: input.skillId,
        status: SkillDraftBatchItemStatus.ACTIVATING,
        generationClaimId: input.claimId,
      },
      data: {
        status: SkillDraftBatchItemStatus.READY,
        generationClaimId: null,
        errorCode: input.errorCode ?? "ACTIVATION_DRAFT_CHANGED",
        errorMessage: input.message,
      },
    });
    if (updated.count !== 1) {
      return false;
    }
    await tx.generationJob.updateMany({
      where: {
        id: input.generationJobId,
        userId: input.userId,
        skillId: input.skillId,
        kind: GenerationJobKind.SKILL_ACTIVATION,
        status: {
          in: [
            GenerationJobStatus.PENDING,
            GenerationJobStatus.RUNNING,
            GenerationJobStatus.FAILED,
          ],
        },
      },
      data: {
        status: GenerationJobStatus.FAILED,
        stage: GenerationJobStage.FAILED,
        failureCategory: GenerationFailureCategory.UNKNOWN,
        errorMessage: input.message,
        completedAt: input.now,
      },
    });
    return true;
  });
  if (released) {
    await reconcileMaterialDraftBatch({
      userId: input.userId,
      batchId: input.batchId,
      now: input.now,
    });
  }
  return released;
}

async function releaseMaterialBatchActivationAfterSuperseded(input: {
  userId: string;
  batchId: string;
  itemId: string;
  claimId: string;
  message: string;
  now: Date;
}) {
  const released = await getPrisma().$transaction(async (tx) => {
    await tx.$queryRaw`
      SELECT "id"
      FROM "users"
      WHERE "id" = ${input.userId}
      FOR UPDATE
    `;
    const updated = await tx.skillDraftBatchItem.updateMany({
      where: {
        id: input.itemId,
        batchId: input.batchId,
        userId: input.userId,
        status: SkillDraftBatchItemStatus.ACTIVATING,
        generationClaimId: input.claimId,
      },
      data: {
        status: SkillDraftBatchItemStatus.READY,
        generationClaimId: null,
        errorCode: "ACTIVATION_SUPERSEDED",
        errorMessage: input.message,
      },
    });
    return updated.count === 1;
  });
  if (released) {
    await reconcileMaterialDraftBatch({
      userId: input.userId,
      batchId: input.batchId,
      now: input.now,
    });
  }
  return released;
}

async function releaseMaterialBatchActivationForRetry(input: {
  userId: string;
  batchId: string;
  itemId: string;
  claimId: string;
  message: string;
  now: Date;
}) {
  const updated = await getPrisma().skillDraftBatchItem.updateMany({
    where: {
      id: input.itemId,
      batchId: input.batchId,
      userId: input.userId,
      status: SkillDraftBatchItemStatus.ACTIVATING,
      generationClaimId: input.claimId,
    },
    data: {
      status: SkillDraftBatchItemStatus.ACTIVATING,
      generationClaimId: null,
      errorCode: "ACTIVATION_RETRYING_TRANSIENT_FAILURE",
      errorMessage: input.message.slice(0, 1_000),
    },
  });
  if (updated.count === 1) {
    await reconcileMaterialDraftBatch(input);
  }
  return updated.count === 1;
}

function hasAutomaticActivationRetryRemaining(input: {
  attempt?: number;
  maxAttempts?: number;
}) {
  return (
    input.attempt !== undefined &&
    input.maxAttempts !== undefined &&
    input.attempt + 1 < input.maxAttempts
  );
}

export async function excludeMaterialDraftItem(input: {
  userId: string;
  batchId: string;
  itemId: string;
  now: Date;
} & (
  | {
      intent?: "exclude";
      expectedCandidateId?: never;
      expectedCandidateFingerprint?: never;
      expectedMatchId?: never;
      expectedMatchFingerprint?: never;
    }
  | {
      intent: "use-existing";
      expectedCandidateId: string;
      expectedCandidateFingerprint: string;
      expectedMatchId: string;
      expectedMatchFingerprint: string;
    }
)) {
  const prisma = getPrisma();
  const useExisting = input.intent === "use-existing";
  const expectedMatchId = useExisting ? input.expectedMatchId : null;
  const expectedMatchFingerprint = useExisting
    ? input.expectedMatchFingerprint
    : null;
  const expectedCandidateId = useExisting
    ? input.expectedCandidateId
    : null;
  const expectedCandidateFingerprint = useExisting
    ? input.expectedCandidateFingerprint
    : null;
  const result = await prisma.$transaction(async (tx) => {
    await tx.$queryRaw`
      SELECT "id" FROM "users"
      WHERE "id" = ${input.userId}
      FOR UPDATE
    `;
    await tx.$queryRaw`
      SELECT "id" FROM "skill_draft_batch_items"
      WHERE "id" = ${input.itemId}
        AND "batchId" = ${input.batchId}
        AND "userId" = ${input.userId}
      FOR UPDATE
    `;
    const item = await tx.skillDraftBatchItem.findFirst({
      where: {
        id: input.itemId,
        batchId: input.batchId,
        userId: input.userId,
        status: { in: [SkillDraftBatchItemStatus.READY, SkillDraftBatchItemStatus.FAILED] },
      },
      select: {
        id: true,
        skillId: true,
        overlapSkillId: true,
        errorCode: true,
        generationMetadata: true,
        skill: { select: { id: true, status: true } },
      },
    });
    if (!item) {
      return { status: "not-found" as const };
    }
    let existingMatch:
      | {
          id: string;
          title: string;
          objective: string | null;
          status: SkillStatus;
        }
      | undefined;
    let lockedDraftSkillStatus: SkillStatus | undefined;
    if (useExisting) {
      const storedMatch = readJsonObject(
        readJsonObject(item.generationMetadata).duplicateMatch,
      );
      if (
        item.errorCode !== "DUPLICATE_REVIEW_REQUIRED" ||
        !item.overlapSkillId ||
        item.overlapSkillId !== expectedMatchId ||
        storedMatch.skillId !== expectedMatchId ||
        storedMatch.skillFingerprint !== expectedMatchFingerprint
      ) {
        return { status: "match-changed" as const };
      }
      if (
        !item.skillId ||
        item.skillId !== expectedCandidateId ||
        storedMatch.candidateSkillId !== expectedCandidateId ||
        storedMatch.candidateFingerprint !== expectedCandidateFingerprint
      ) {
        return { status: "draft-changed" as const };
      }
      const skillIdsToLock = [
        ...new Set(
          [expectedMatchId, item.skillId].filter(
            (skillId): skillId is string => Boolean(skillId),
          ),
        ),
      ].sort();
      const lockedSkills = await tx.$queryRaw<
        Array<{
          id: string;
          title: string;
          objective: string | null;
          collectionId: string | null;
          rules: unknown;
          examples: unknown;
          exerciseConstraints: unknown;
          tags: string[];
          status: SkillStatus;
        }>
      >(
        Prisma.sql`
          SELECT
            skill."id",
            skill."title",
            skill."objective",
            skill."collectionId",
            skill."rules",
            skill."examples",
            skill."exerciseConstraints",
            skill."tags",
            skill."status"
          FROM "skills" AS skill
          WHERE skill."userId" = ${input.userId}
            AND skill."id" IN (${Prisma.join(skillIdsToLock)})
          ORDER BY skill."id"
          FOR UPDATE OF skill
        `,
      );
      existingMatch = lockedSkills.find(
        (skill) => skill.id === expectedMatchId,
      );
      lockedDraftSkillStatus = lockedSkills.find(
        (skill) => skill.id === item.skillId,
      )?.status;
      if (!existingMatch) {
        return { status: "match-not-found" as const };
      }
      if (
        buildSkillDuplicateReviewFingerprint(existingMatch) !==
        expectedMatchFingerprint
      ) {
        return { status: "match-changed" as const };
      }
      const lockedDraftSkill = lockedSkills.find(
        (skill) => skill.id === expectedCandidateId,
      );
      if (
        !lockedDraftSkill ||
        buildSkillDuplicateCandidateFingerprint(lockedDraftSkill) !==
          expectedCandidateFingerprint
      ) {
        return { status: "draft-changed" as const };
      }
    }
    if (item.skillId) {
      const lockedSkillStatus =
        lockedDraftSkillStatus ??
        (
          await tx.$queryRaw<Array<{ status: SkillStatus }>>`
            SELECT "status" FROM "skills"
            WHERE "id" = ${item.skillId} AND "userId" = ${input.userId}
            FOR UPDATE
          `
        )[0]?.status;
      if (
        !item.skill ||
        item.skill.status !== SkillStatus.DRAFT ||
        lockedSkillStatus !== SkillStatus.DRAFT
      ) {
        return { status: "skill-not-draft" as const };
      }
      const activationStaleBefore = new Date(
        input.now.getTime() - ACTIVATION_GENERATION_STALE_AFTER_MS,
      );
      const activeActivation = await tx.generationJob.count({
        where: {
          userId: input.userId,
          skillId: item.skillId,
          kind: GenerationJobKind.SKILL_ACTIVATION,
          OR: [
            {
              status: GenerationJobStatus.PENDING,
              createdAt: { gte: activationStaleBefore },
            },
            {
              status: GenerationJobStatus.RUNNING,
              startedAt: { gte: activationStaleBefore },
            },
          ],
        },
      });
      if (activeActivation > 0) {
        return { status: "activation-in-progress" as const };
      }
      const deletedSkill = await tx.skill.deleteMany({
        where: { id: item.skillId, userId: input.userId, status: SkillStatus.DRAFT },
      });
      if (deletedSkill.count !== 1) {
        return { status: "skill-not-draft" as const };
      }
    }
    await tx.skillDraftBatchItem.update({
      where: { id: item.id },
      data: {
        skillId: null,
        status: SkillDraftBatchItemStatus.EXCLUDED,
        overlapSkillId: useExisting ? expectedMatchId : null,
        errorCode: useExisting ? "DUPLICATE_USE_EXISTING" : null,
        errorMessage:
          useExisting && existingMatch
            ? duplicateUseExistingMessage(existingMatch)
            : null,
        ...(!useExisting
          ? {
              generationMetadata: toInputJson(
                withoutDuplicateMatchMetadata(item.generationMetadata),
              ),
            }
          : {}),
      },
    });
    return { status: "excluded" as const };
  });
  if (result.status === "not-found") {
    return { status: "not-found" as const };
  }
  if (result.status === "skill-not-draft") {
    return {
      status: "not-excluded" as const,
      reason: "skill-not-draft" as const,
      message: "This skill is already active or changed outside the batch and cannot be excluded.",
    };
  }
  if (result.status === "match-not-found") {
    return {
      status: "not-excluded" as const,
      reason: "match-not-found" as const,
      message:
        "The existing skill is no longer available. Your generated draft was kept; check and add it if it is distinct.",
    };
  }
  if (result.status === "match-changed") {
    return {
      status: "not-excluded" as const,
      reason: "match-changed" as const,
      message:
        "The existing skill changed after you reviewed it. Your draft was kept; refresh the batch and compare them again.",
    };
  }
  if (result.status === "draft-changed") {
    return {
      status: "not-excluded" as const,
      reason: "draft-changed" as const,
      message:
        "This draft changed after the duplicate check. It was kept; refresh the batch and check it again.",
    };
  }
  if (result.status === "activation-in-progress") {
    return {
      status: "not-excluded" as const,
      reason: "activation-in-progress" as const,
      message: "This skill is being activated and cannot be excluded until activation finishes.",
    };
  }
  await reconcileMaterialDraftBatch({ userId: input.userId, batchId: input.batchId, now: input.now });
  return { status: "excluded" as const };
}

export async function getMaterialDraftBatch(input: { userId: string; batchId: string }) {
  const prisma = getPrisma();
  const now = new Date();
  const staleBefore = new Date(now.getTime() - MATERIAL_DRAFT_CLAIM_STALE_MS);
  const activationStaleBefore = new Date(
    now.getTime() - MATERIAL_BATCH_ACTIVATION_CLAIM_STALE_MS,
  );
  const recovered = await prisma.skillDraftBatchItem.updateMany({
    where: {
      batchId: input.batchId,
      userId: input.userId,
      status: SkillDraftBatchItemStatus.GENERATING,
      updatedAt: { lt: staleBefore },
    },
    data: {
      status: SkillDraftBatchItemStatus.FAILED,
      generationClaimId: null,
      errorCode: "STALE_GENERATION_CLAIM",
      errorMessage: "Draft generation stopped before it finished. Retry this item.",
    },
  });
  const { recoveredActivations, retiredActivationJobs } = await prisma.$transaction(
    async (tx) => {
      await tx.$queryRaw`
        SELECT "id"
        FROM "users"
        WHERE "id" = ${input.userId}
        FOR UPDATE
      `;
      const [staleItems, takeoverItems] = await Promise.all([
        tx.skillDraftBatchItem.findMany({
          where: {
            batchId: input.batchId,
            userId: input.userId,
            status: SkillDraftBatchItemStatus.ACTIVATING,
            generationClaimId: { not: null },
            updatedAt: { lt: activationStaleBefore },
            skill: { status: SkillStatus.DRAFT },
          },
          select: { id: true, skillId: true },
        }),
        tx.skillDraftBatchItem.findMany({
          where: {
            batchId: input.batchId,
            userId: input.userId,
            status: SkillDraftBatchItemStatus.READY,
            errorCode: "ACTIVATION_SUPERSEDED",
            skill: { status: SkillStatus.DRAFT },
          },
          select: { skillId: true },
        }),
      ]);
      if (staleItems.length === 0 && takeoverItems.length === 0) {
        return {
          recoveredActivations: { count: 0 },
          retiredActivationJobs: { count: 0 },
        };
      }
      const candidateSkillIds = [
        ...new Set(
          [...staleItems, ...takeoverItems].flatMap((item) =>
            item.skillId ? [item.skillId] : [],
          ),
        ),
      ];
      const retiredJobs = await tx.generationJob.updateMany({
        where: {
          userId: input.userId,
          skillId: { in: candidateSkillIds },
          kind: GenerationJobKind.SKILL_ACTIVATION,
          OR: [
            {
              status: GenerationJobStatus.PENDING,
              createdAt: { lt: activationStaleBefore },
            },
            {
              status: GenerationJobStatus.RUNNING,
              OR: [
                { startedAt: { lt: activationStaleBefore } },
                {
                  startedAt: null,
                  updatedAt: { lt: activationStaleBefore },
                },
              ],
            },
          ],
        },
        data: {
          status: GenerationJobStatus.FAILED,
          stage: GenerationJobStage.FAILED,
          failureCategory: GenerationFailureCategory.CANCELED,
          errorMessage: ACTIVATION_SUPERSEDED_JOB_MESSAGE,
          completedAt: now,
        },
      });
      const recoveredItems =
        staleItems.length > 0
          ? await tx.skillDraftBatchItem.updateMany({
              where: {
                id: { in: staleItems.map((item) => item.id) },
                userId: input.userId,
                status: SkillDraftBatchItemStatus.ACTIVATING,
                generationClaimId: { not: null },
                updatedAt: { lt: activationStaleBefore },
                skill: { status: SkillStatus.DRAFT },
              },
              data: {
                status: SkillDraftBatchItemStatus.FAILED,
                generationClaimId: null,
                errorCode: "ACTIVATION_RETRYABLE_STALE_CLAIM",
                errorMessage:
                  "Activation stopped before it finished. Retry or exclude this item.",
              },
            })
          : { count: 0 };
      return {
        recoveredActivations: recoveredItems,
        retiredActivationJobs: retiredJobs,
      };
    },
  );
  const synchronizedActive = await prisma.skillDraftBatchItem.updateMany({
    where: {
      batchId: input.batchId,
      userId: input.userId,
      status: {
        in: [
          SkillDraftBatchItemStatus.READY,
          SkillDraftBatchItemStatus.ACTIVATING,
          SkillDraftBatchItemStatus.FAILED,
        ],
      },
      skill: { status: SkillStatus.ACTIVE },
    },
    data: {
      status: SkillDraftBatchItemStatus.ACTIVE,
      generationClaimId: null,
      errorCode: null,
      errorMessage: null,
    },
  });
  if (
    recovered.count > 0 ||
    recoveredActivations.count > 0 ||
    retiredActivationJobs.count > 0 ||
    synchronizedActive.count > 0
  ) {
    await reconcileMaterialDraftBatch({
      userId: input.userId,
      batchId: input.batchId,
      now,
    });
  }

  return prisma.skillDraftBatch.findFirst({
    where: { id: input.batchId, userId: input.userId },
    select: {
      id: true,
      instruction: true,
      proposedPlan: true,
      confirmedPlan: true,
      status: true,
      requestedCount: true,
      readyCount: true,
      failedCount: true,
      excludedCount: true,
      activatedCount: true,
      errorMessage: true,
      createdAt: true,
      updatedAt: true,
      confirmedAt: true,
      completedAt: true,
      materialRevision: {
        select: {
          id: true,
          revisionNumber: true,
          material: { select: { id: true, title: true, kind: true } },
        },
      },
      items: {
        orderBy: { ordinal: "asc" },
        select: {
          id: true,
          ordinal: true,
          targetKey: true,
          proposedTitle: true,
          proposedObjective: true,
          locator: true,
          status: true,
          overlapSkillId: true,
          errorCode: true,
          errorMessage: true,
          generationAttempts: true,
          generationMetadata: true,
          skill: {
            select: {
              id: true,
              title: true,
              objective: true,
              collectionId: true,
              collection: { select: { name: true } },
              rules: true,
              examples: true,
              exerciseConstraints: true,
              tags: true,
              status: true,
              generationJobs: {
                where: {
                  kind: GenerationJobKind.SKILL_ACTIVATION,
                  status: {
                    in: [
                      GenerationJobStatus.PENDING,
                      GenerationJobStatus.RUNNING,
                    ],
                  },
                },
                select: { id: true },
                take: 1,
              },
            },
          },
        },
      },
    },
  });
}

export async function getMaterialDuplicateSkillPreviews(input: {
  userId: string;
  skillIds: readonly string[];
}) {
  const skillIds = [...new Set(input.skillIds.filter(Boolean))];
  if (skillIds.length === 0) {
    return [];
  }
  return getPrisma().skill.findMany({
    where: {
      userId: input.userId,
      id: { in: skillIds },
    },
    select: {
      id: true,
      title: true,
      objective: true,
      collectionId: true,
      rules: true,
      examples: true,
      exerciseConstraints: true,
      status: true,
      tags: true,
      collection: { select: { name: true } },
    },
  });
}

export async function reconcileMaterialDraftBatch(input: {
  userId: string;
  batchId: string;
  now?: Date;
}) {
  const prisma = getPrisma();
  const items = await prisma.skillDraftBatchItem.findMany({
    where: { batchId: input.batchId, userId: input.userId },
    select: { status: true },
  });
  if (items.length === 0) {
    return null;
  }
  const summary = summarizeMaterialDraftBatch(items.map((item) => item.status));
  await prisma.skillDraftBatch.updateMany({
    where: { id: input.batchId, userId: input.userId },
    data: {
      status: SkillDraftBatchStatus[summary.status],
      readyCount: summary.readyCount,
      failedCount: summary.failedCount,
      excludedCount: summary.excludedCount,
      activatedCount: summary.activatedCount,
      completedAt: summary.terminal ? input.now ?? new Date() : null,
    },
  });
  return summary;
}

async function reconcileMaterialDraftBatchWithClient(
  prisma: Prisma.TransactionClient,
  input: { userId: string; batchId: string; now: Date },
) {
  const items = await prisma.skillDraftBatchItem.findMany({
    where: { batchId: input.batchId, userId: input.userId },
    select: { status: true },
  });
  if (items.length === 0) {
    return null;
  }
  const summary = summarizeMaterialDraftBatch(items.map((item) => item.status));
  await prisma.skillDraftBatch.updateMany({
    where: { id: input.batchId, userId: input.userId },
    data: {
      status: SkillDraftBatchStatus[summary.status],
      readyCount: summary.readyCount,
      failedCount: summary.failedCount,
      excludedCount: summary.excludedCount,
      activatedCount: summary.activatedCount,
      completedAt: summary.terminal ? input.now : null,
    },
  });
  return summary;
}

async function planExistingMaterialBatch(input: {
  userId: string;
  batchId: string;
  now: Date;
  deadlineAt?: Date;
  preservePlanningOnTimeout?: boolean;
  aiSetup?: MaterialDraftAiSetup;
  embeddingGenerator?: MaterialEmbeddingGenerator | null;
  ocrGenerator?: MaterialOcrGenerator | null;
  ocrStorage?: SourceObjectStorage;
  sectionIds?: string[];
}) {
  const prisma = getPrisma();
  const batch = await prisma.skillDraftBatch.findFirst({
    where: { id: input.batchId, userId: input.userId, confirmedAt: null },
    select: {
      id: true,
      instruction: true,
      updatedAt: true,
      materialRevisionId: true,
      materialRevision: {
        select: {
          status: true,
          material: { select: { id: true, title: true, kind: true } },
          sourceFiles: {
            where: { status: SourceFileStatus.READY },
            orderBy: { createdAt: "asc" },
            take: 1,
            select: {
              id: true,
              materialRevisionId: true,
              kind: true,
              status: true,
              originalName: true,
              mimeType: true,
              storageBucket: true,
              storageKey: true,
            },
          },
          sections: {
            orderBy: { ordinal: "asc" },
            select: {
              id: true,
              parentId: true,
              ordinal: true,
              level: true,
              title: true,
              pageStart: true,
              pageEnd: true,
              url: true,
              anchor: true,
            },
          },
        },
      },
    },
  });
  if (!batch || batch.materialRevision.status !== MaterialRevisionStatus.READY) {
    return { status: "not-found" as const, message: "Ready material batch was not found." };
  }
  const sections = batch.materialRevision.sections satisfies MaterialPlanningSection[];
  const structural = input.sectionIds
    ? resolveExplicitMaterialSectionScope({ sectionIds: input.sectionIds, sections })
    : resolveStructuralMaterialScope({ instruction: batch.instruction, sections });
  if (structural.missingReferences.length > 0) {
    const plan = materialScopeResolutionSchema.parse({
      version: 1,
      materialRevisionId: batch.materialRevisionId,
      instruction: batch.instruction,
      resolutionStatus: "ambiguous",
      resolvedScopeLabel: "The requested chapter or section was not found in this revision.",
      warnings: [],
      clarification: `Check the material outline and clarify ${structural.missingReferences.join(
        " and ",
      )}.`,
      items: [],
    });
    return saveProposedMaterialPlan({
      batchId: batch.id,
      userId: input.userId,
      plan,
      model: null,
      structural,
      selectedSectionIds: input.sectionIds ?? [],
      now: input.now,
      expectedInstruction: batch.instruction,
      expectedUpdatedAt: batch.updatedAt,
    });
  }
  if (structural.candidateSectionIds.length === 0) {
    const plan = materialScopeResolutionSchema.parse({
      version: 1,
      materialRevisionId: batch.materialRevisionId,
      instruction: batch.instruction,
      resolutionStatus: "ambiguous",
      resolvedScopeLabel: "This material does not have a usable outline yet.",
      warnings: [],
      clarification: "Wait for indexing to finish or choose a material with resolved sections.",
      items: [],
    });
    return saveProposedMaterialPlan({
      batchId: batch.id,
      userId: input.userId,
      plan,
      model: null,
      structural,
      selectedSectionIds: input.sectionIds ?? [],
      now: input.now,
      expectedInstruction: batch.instruction,
      expectedUpdatedAt: batch.updatedAt,
    });
  }

  try {
    const topicSearchQuery =
      structural.references.length === 0
        ? resolveMaterialTopicSearchQuery(batch.instruction)
        : null;
    const ai = input.aiSetup ?? resolveMaterialDraftAiSetup();
    const sourceFile = batch.materialRevision.sourceFiles[0];
    if (batch.materialRevision.material.kind === "PDF" && sourceFile) {
      const pageRanges = sections
        .filter((section) => structural.candidateSectionIds.includes(section.id))
        .flatMap((section) =>
          section.pageStart !== null && section.pageEnd !== null
            ? [{ start: section.pageStart, end: section.pageEnd }]
            : [],
        );
      if (pageRanges.length > 0) {
        const ocr = await withMaterialPlanningStageTimeout({
          deadlineAt: input.deadlineAt,
          stage: "material page OCR",
          message: "Material page OCR timed out.",
          settleGraceMs: MATERIAL_OCR_ABORT_SETTLE_GRACE_MS,
          run: (signal) => ensureMaterialPageOcr({
            userId: input.userId,
            materialRevisionId: batch.materialRevisionId,
            sourceFile,
            pageRanges,
            now: input.now,
            storage: input.ocrStorage,
            ocrGenerator: input.ocrGenerator,
            signal,
          }),
        });
        if (ocr.status === "in-progress") {
          throw new JobStageTimeoutError(
            "Material page OCR is still processing relevant pages.",
            "material page OCR",
          );
        }
      }
    }
    const retrieval = await withMaterialPlanningStageTimeout({
      deadlineAt: input.deadlineAt,
      stage: "material scope retrieval",
      message: "Material scope retrieval timed out.",
      run: (signal) => retrievePlanningChunks({
        userId: input.userId,
        materialRevisionId: batch.materialRevisionId,
        instruction: batch.instruction,
        topicSearchQuery,
        sectionIds: structural.candidateSectionIds,
        sections: sections.filter((section) =>
          structural.candidateSectionIds.includes(section.id),
        ),
        embeddingGenerator: input.embeddingGenerator,
        rankChunks: ai.rankChunks,
        deadlineAt: input.deadlineAt,
        signal,
      }),
    });
    let chunks = retrieval.chunks;
    if (chunks.length === 0) {
      const plan = materialScopeResolutionSchema.parse({
        version: 1,
        materialRevisionId: batch.materialRevisionId,
        instruction: batch.instruction,
        resolutionStatus: "ambiguous",
        resolvedScopeLabel: "The requested scope has no indexed text yet.",
        warnings: retrieval.retrievalWarning ? [retrieval.retrievalWarning] : [],
        clarification: "Choose another section or retry after scanned pages finish OCR.",
        items: [],
      });
      return saveProposedMaterialPlan({
        batchId: batch.id,
        userId: input.userId,
        plan,
        model: null,
        structural,
        selectedSectionIds: input.sectionIds ?? [],
        now: input.now,
        expectedInstruction: batch.instruction,
        expectedUpdatedAt: batch.updatedAt,
      });
    }
    const recoveredScope = await withMaterialPlanningStageTimeout({
      deadlineAt: input.deadlineAt,
      stage: "material scope evidence recovery",
      message: "Material scope evidence recovery timed out.",
      run: async (signal) => recoverBackMatterMaterialScope({
        sections,
        sectionIds: structural.candidateSectionIds,
        chunks,
        retrieveRevisionChunks: async ({ query }) => {
          throwIfMaterialPlanningAborted(signal);
          const result = await retrieveBackMatterRecoveryCandidates({
            userId: input.userId,
            materialRevisionId: batch.materialRevisionId,
            query,
          });
          throwIfMaterialPlanningAborted(signal);
          return result;
        },
        retrieveSectionChunks: async ({ sectionIds, anchorChunkIds }) => {
          throwIfMaterialPlanningAborted(signal);
          const result = await retrieveMaterialSectionChunks({
            userId: input.userId,
            materialRevisionId: batch.materialRevisionId,
            sectionIds,
            anchorChunkIds,
          });
          throwIfMaterialPlanningAborted(signal);
          return result;
        },
      }),
    });
    if (
      recoveredScope.status === "ambiguous" ||
      (input.sectionIds !== undefined && recoveredScope.status === "recovered")
    ) {
      const explicitSelection = input.sectionIds !== undefined;
      const plan = materialScopeResolutionSchema.parse({
        version: 1,
        materialRevisionId: batch.materialRevisionId,
        instruction: batch.instruction,
        resolutionStatus: "ambiguous",
        resolvedScopeLabel: explicitSelection
          ? "The selected section appears to contain answer-key or back-matter content."
          : "The requested chapter was found only in answer-key or back-matter pages.",
        warnings: explicitSelection
          ? ["The selected source scope does not contain confidently identified instructional content."]
          : ["LearnRecur could not confidently locate the instructional chapter."],
        clarification: explicitSelection
          ? "Select an instructional section from this material before planning skills."
          : "Choose the instructional page range or name a more specific section from the material.",
        items: [],
      });
      return saveProposedMaterialPlan({
        batchId: batch.id,
        userId: input.userId,
        plan,
        model: null,
        structural,
        selectedSectionIds: input.sectionIds ?? [],
        now: input.now,
        expectedInstruction: batch.instruction,
        expectedUpdatedAt: batch.updatedAt,
      });
    }
    const planningSectionIds =
      retrieval.focusedTopic && recoveredScope.status === "not-needed"
        ? [
            ...new Set(
              recoveredScope.chunks.flatMap((chunk) =>
                chunk.materialSectionId ? [chunk.materialSectionId] : [],
              ),
            ),
          ]
        : recoveredScope.sectionIds;
    chunks = recoveredScope.chunks;
    const planningStructural =
      recoveredScope.status === "recovered" || retrieval.focusedTopic
      ? {
          ...structural,
          references: structural.references.map((reference) => ({
            ...reference,
            sectionIds: planningSectionIds,
          })),
          candidateSectionIds: planningSectionIds,
        }
      : structural;
    const allowedSections = sections.filter((section) =>
      planningSectionIds.includes(section.id),
    );
    const scopePlanningInput = {
      materialTitle: batch.materialRevision.material.title,
      materialKind: batch.materialRevision.material.kind,
      instruction: batch.instruction,
      structuralReferences: planningStructural.references,
      sections: allowedSections,
      chunks,
    };
    let validation = await withMaterialPlanningStageTimeout({
      deadlineAt: input.deadlineAt,
      stage: "material scope planning",
      message: "Material scope planning timed out.",
      run: (signal) => generateValidatedMaterialScopePlan({
        generate: (validationFeedback) =>
          ai.planScope({ ...scopePlanningInput, signal, validationFeedback }),
        materialRevisionId: batch.materialRevisionId,
        instruction: batch.instruction,
        kind: batch.materialRevision.material.kind,
        allowedSections,
        allowedChunks: chunks,
      }),
    });
    const reviewScope = ai.reviewScope;
    if (
      validation.status === "ready" &&
      validation.plan.resolutionStatus === "resolved" &&
      reviewScope
    ) {
      const candidatePlan = validation.plan;
      validation = await withMaterialPlanningStageTimeout({
        deadlineAt: input.deadlineAt,
        stage: "material scope review",
        message: "Material scope review timed out.",
        run: (signal) => generateValidatedMaterialScopePlan({
          generate: (validationFeedback) =>
            reviewScope({
              ...scopePlanningInput,
              signal,
              candidatePlan,
              validationFeedback,
            }),
          materialRevisionId: batch.materialRevisionId,
          instruction: batch.instruction,
          kind: batch.materialRevision.material.kind,
          allowedSections,
          allowedChunks: chunks,
        }),
      });
    }
    if (validation.status !== "ready") {
      const markedFailed = await markMaterialBatchPlanningFailed({
        userId: input.userId,
        batchId: batch.id,
        code: validation.reason.toUpperCase().replaceAll("-", "_"),
        message: validation.message,
        expectedInstruction: batch.instruction,
        expectedUpdatedAt: batch.updatedAt,
      });
      if (!markedFailed) {
        return readCurrentMaterialPlanningResult({
          userId: input.userId,
          batchId: batch.id,
        });
      }
      return { status: "failed" as const, batchId: batch.id, message: validation.message };
    }
    const similarity = await withMaterialPlanningStageTimeout({
      deadlineAt: input.deadlineAt,
      stage: "material skill similarity",
      message: "Material skill similarity lookup timed out.",
      run: (signal) => findSimilarSkillsForUser({
        userId: input.userId,
        candidates: validation.plan.items.map((item) => ({
          key: item.key,
          title: item.title,
          objective: item.objective,
        })),
        embeddingGenerator:
          input.embeddingGenerator as SkillSimilarityEmbeddingGenerator | null | undefined,
        deadlineAt: input.deadlineAt,
        signal,
      }),
    });
    const annotatedPlan = annotateMaterialPlanOverlaps(validation.plan, similarity.candidates);
    const plan = retrieval.retrievalWarning
      ? {
          ...annotatedPlan,
          warnings: [
            ...new Set(annotatedPlan.warnings.filter(
              (warning) => warning !== retrieval.retrievalWarning,
            )),
          ].slice(0, 19).concat(retrieval.retrievalWarning),
        }
      : annotatedPlan;
    return saveProposedMaterialPlan({
      batchId: batch.id,
      userId: input.userId,
      plan,
      model: ai.model,
      structural: planningStructural,
      selectedSectionIds: input.sectionIds ?? [],
      now: input.now,
      expectedInstruction: batch.instruction,
      expectedUpdatedAt: batch.updatedAt,
    });
  } catch (error) {
    if (isJobStageTimeoutError(error) && input.preservePlanningOnTimeout) {
      // Keep the idempotent planning row in PLANNING so the same background
      // delivery can retry the provider call without turning a transient stall
      // into a permanent material-planning failure.
      throw error;
    }
    console.error("[materials] scope planning failed", {
      materialRevisionId: batch.materialRevisionId,
      error: error instanceof Error ? error.message : "Unknown scope planning error",
    });
    const message = getPublicGeminiScopePlanningFailureMessage(error);
    const markedFailed = await markMaterialBatchPlanningFailed({
      userId: input.userId,
      batchId: batch.id,
      code: "PLANNING_FAILED",
      message,
      expectedInstruction: batch.instruction,
      expectedUpdatedAt: batch.updatedAt,
    });
    if (!markedFailed) {
      return readCurrentMaterialPlanningResult({
        userId: input.userId,
        batchId: batch.id,
      });
    }
    return { status: "failed" as const, batchId: batch.id, message };
  }
}

async function retrievePlanningChunks(input: {
  userId: string;
  materialRevisionId: string;
  instruction: string;
  topicSearchQuery?: string | null;
  sectionIds: string[];
  sections: MaterialPlanningSection[];
  embeddingGenerator?: MaterialEmbeddingGenerator | null;
  rankChunks?: MuseChunkRanker;
  deadlineAt?: Date;
  signal?: AbortSignal;
}) {
  throwIfMaterialPlanningAborted(input.signal);
  const stageRemainingMs = getJobStageTimeoutMs({
    deadlineAt: input.deadlineAt,
    cleanupMarginMs: input.deadlineAt ? AGENT_OPERATION_CLEANUP_MARGIN_MS : 0,
    maxTimeoutMs: ACTIVATION_PROVIDER_CHAIN_TIMEOUT_MS,
    stage: "material scope retrieval",
  });
  const stageDeadlineAt = Date.now() + stageRemainingMs;
  const prisma = getPrisma();
  let ranked: MaterialChunkSearchResult[] = [];
  let focusedTopic = false;
  const retrievalQuery = input.topicSearchQuery ?? input.instruction;
  const embeddingGenerator =
    input.embeddingGenerator === undefined
      ? safelyResolveEmbeddingGenerator()
      : input.embeddingGenerator;
  let embeddingFailed = false;
  if (embeddingGenerator) {
    try {
      const [embedding] = await embeddingGenerator({
        texts: [retrievalQuery],
        titles: ["Material skill request"],
        signal: input.signal,
      });
      throwIfMaterialPlanningAborted(input.signal);
      ranked = (
        await searchMaterialChunks({
          userId: input.userId,
          materialRevisionId: input.materialRevisionId,
          embedding,
          query: retrievalQuery,
          materialSectionIds: input.sectionIds,
          limit: 48,
        })
      ).filter((chunk) => chunk.vectorScore > 0 || chunk.lexicalScore > 0);
    } catch (error) {
      throwIfMaterialPlanningAborted(input.signal);
      embeddingFailed = true;
      console.warn("[materials] semantic scope retrieval unavailable", {
        materialRevisionId: input.materialRevisionId,
        error: error instanceof Error ? error.message : "Unknown retrieval error",
      });
    }
  }
  const hasVectorMatch = ranked.some((chunk) => chunk.vectorScore > 0);
  let hasMissingEmbeddings = false;
  if (hasVectorMatch) {
    const coverage = await prisma.$queryRaw<Array<{ hasMissingEmbeddings: boolean }>>`
      SELECT EXISTS (
        SELECT 1 FROM "material_chunks"
        WHERE "userId" = ${input.userId}
          AND "materialRevisionId" = ${input.materialRevisionId}
          AND "materialSectionId" IN (${Prisma.join(input.sectionIds)})
          AND "embedding" IS NULL
      ) AS "hasMissingEmbeddings"
    `;
    throwIfMaterialPlanningAborted(input.signal);
    hasMissingEmbeddings = coverage[0]?.hasMissingEmbeddings ?? false;
  }
  let hasOcrEvidence = false;
  if (hasVectorMatch) {
    const boundedSections = input.sections.filter(
      (section) => section.pageStart !== null && section.pageEnd !== null,
    );
    if (boundedSections.length > 0) {
      const ocrPage = await prisma.materialPage.findFirst({
        where: {
          userId: input.userId,
          materialRevisionId: input.materialRevisionId,
          textStatus: MaterialPageTextStatus.OCR_READY,
          ocrText: { not: null },
          OR: boundedSections.map((section) => ({
            pageNumber: { gte: section.pageStart!, lte: section.pageEnd! },
          })),
        },
        select: { id: true },
      });
      throwIfMaterialPlanningAborted(input.signal);
      hasOcrEvidence = ocrPage !== null;
    }
  }
  let museMatched = false;
  let retrievalWarning: string | null = null;
  let museOcrChunks: MaterialChunkSearchResult[] | null = null;
  if ((!hasVectorMatch || hasMissingEmbeddings || hasOcrEvidence) && input.rankChunks) {
    try {
      const scanSignal = createMuseScanSignal(
        input.signal,
        getMuseScanBudgetMs(stageDeadlineAt - Date.now()),
      );
      museOcrChunks = await retrieveOcrPlanningChunks({
        userId: input.userId,
        materialRevisionId: input.materialRevisionId,
        instruction: retrievalQuery,
        sections: input.sections,
      });
      throwIfMaterialPlanningAborted(scanSignal);
      const scan = await scanMaterialChunksWithMuse({
        query: retrievalQuery,
        signal: scanSignal,
        rank: input.rankChunks,
        supplementalChunks: museOcrChunks,
        loadPage: async (afterOrdinal, limit) =>
          prisma.materialChunk.findMany({
            where: {
              userId: input.userId,
              materialRevisionId: input.materialRevisionId,
              materialSectionId: { in: input.sectionIds },
              ordinal: afterOrdinal === Number.MIN_SAFE_INTEGER
                ? undefined
                : { gt: afterOrdinal },
            },
            orderBy: { ordinal: "asc" },
            take: limit,
            select: {
              id: true,
              materialRevisionId: true,
              materialSectionId: true,
              sourceFileId: true,
              ordinal: true,
              text: true,
              tokenEstimate: true,
              locator: true,
              headingText: true,
            },
          }),
      });
      throwIfMaterialPlanningAborted(input.signal);
      if (scan.matches.length > 0) {
        ranked = uniqueById([...scan.matches, ...ranked]);
        museMatched = true;
      }
      console.info("[materials] muse scope retrieval completed", {
        materialRevisionId: input.materialRevisionId,
        scannedChunkCount: scan.scannedChunkCount,
        matchedChunkCount: scan.matches.length,
      });
    } catch (error) {
      throwIfMaterialPlanningAborted(input.signal);
      retrievalWarning = error instanceof MuseRetrievalCapacityError
        ? "This material is too large for a complete semantic fallback scan. Choose a narrower section or review the cited passages carefully."
        : "Muse could not scan all source text. Word-based evidence may miss related passages; retry or review the cited passages carefully.";
      console.warn("[materials] muse scope retrieval unavailable", {
        materialRevisionId: input.materialRevisionId,
        error: error instanceof Error ? error.message : "Unknown retrieval error",
      });
    }
  }
  if (embeddingFailed && !input.rankChunks) {
    retrievalWarning = "Semantic retrieval is unavailable. Word-based evidence may miss related passages; review the cited passages carefully.";
  } else if ((hasMissingEmbeddings || hasOcrEvidence) && !input.rankChunks) {
    retrievalWarning = "Some source passages have no embeddings or come from OCR pages, so semantic coverage may be incomplete. Review the cited passages carefully.";
  }
  let strictLexicalMatched = false;
  if (input.topicSearchQuery) {
    const lexicalMatches = await searchMaterialChunksLexical({
      userId: input.userId,
      materialRevisionId: input.materialRevisionId,
      query: input.topicSearchQuery,
      materialSectionIds: input.sectionIds,
      limit: 48,
      prefixMatching: true,
      excludeLikelyBackMatter: true,
    });
    throwIfMaterialPlanningAborted(input.signal);
    const strictLexical = selectMaterialTopicRetrievalChunks({
      semantic: [],
      lexical: lexicalMatches,
    });
    strictLexicalMatched = strictLexical.chunks.length > 0;
    const selected = selectMaterialTopicRetrievalChunks({
      semantic: ranked,
      lexical: lexicalMatches,
    });
    if (selected.chunks.length > 0) {
      ranked = uniqueById([...selected.chunks, ...strictLexical.chunks]);
      focusedTopic = selected.focused || strictLexical.focused;
    }
  }
  if (input.topicSearchQuery && !strictLexicalMatched && !museMatched) {
    const recoveryQuery = buildMaterialTopicRecoveryQuery(input.topicSearchQuery);
    if (recoveryQuery) {
      const minimumPrefixMatches = getMaterialTopicRecoveryGroupCount(recoveryQuery);
      const recoveryMatches = (
        await searchMaterialChunksLexical({
          userId: input.userId,
          materialRevisionId: input.materialRevisionId,
          query: recoveryQuery,
          materialSectionIds: input.sectionIds,
          limit: 80,
          prefixMatching: true,
          prefixOperator: "or",
          minimumSectionPrefixMatches: minimumPrefixMatches,
          excludeLikelyBackMatter: true,
        })
      );
      throwIfMaterialPlanningAborted(input.signal);
      const recovered = selectFocusedMaterialTopicRecoveryChunks(recoveryMatches);
      if (recovered.length > 0) {
        ranked = recovered;
        focusedTopic = true;
      }
    }
  }
  if (ranked.length === 0) {
    ranked = (
      await searchMaterialChunksLexical({
        userId: input.userId,
        materialRevisionId: input.materialRevisionId,
        query: retrievalQuery,
        materialSectionIds: input.sectionIds,
        limit: 48,
      })
    ).filter((chunk) => chunk.lexicalScore > 0);
    throwIfMaterialPlanningAborted(input.signal);
  }
  const ocrSections = focusedTopic
    ? input.sections.filter((section) =>
        ranked.some((chunk) => chunk.materialSectionId === section.id),
      )
    : input.sections;
  const ocrSectionIds = new Set(ocrSections.map((section) => section.id));
  const ocrChunks = museOcrChunks
    ? museOcrChunks.filter((chunk) =>
        chunk.materialSectionId !== null && ocrSectionIds.has(chunk.materialSectionId),
      )
    : await retrieveOcrPlanningChunks({
        userId: input.userId,
        materialRevisionId: input.materialRevisionId,
        instruction: retrievalQuery,
        sections: ocrSections,
      });
  throwIfMaterialPlanningAborted(input.signal);
  const matchedOcrChunks = ocrChunks.filter((chunk) => chunk.lexicalScore > 0);
  const reservedOcrChunks = uniqueById([...matchedOcrChunks, ...ocrChunks]).slice(0, 8);
  ranked = uniqueById([
    ...matchedOcrChunks,
    ...ranked.slice(0, Math.max(0, 48 - reservedOcrChunks.length)),
    ...reservedOcrChunks,
  ]).slice(0, 48);
  const persistedRanked = ranked.filter(
    (chunk) => chunk.materialSectionId !== null && !parseMaterialPageEvidenceId(chunk.id),
  );
  if (persistedRanked.length > 0) {
    const neighboringRows = await prisma.materialChunk.findMany({
      where: {
        userId: input.userId,
        materialRevisionId: input.materialRevisionId,
        OR: persistedRanked.map((chunk) => ({
          materialSectionId: chunk.materialSectionId,
          ordinal: { gte: Math.max(0, chunk.ordinal - 1), lte: chunk.ordinal + 1 },
        })),
      },
      orderBy: [{ materialSectionId: "asc" }, { ordinal: "asc" }],
      select: {
        id: true,
        materialRevisionId: true,
        materialSectionId: true,
        sourceFileId: true,
        ordinal: true,
        text: true,
        tokenEstimate: true,
        locator: true,
        headingText: true,
      },
    });
    throwIfMaterialPlanningAborted(input.signal);
    const neighboringChunks = neighboringRows.map((chunk) => ({
      ...chunk,
      vectorScore: 0,
      lexicalScore: 0,
      score: 0,
    }));
    ranked = uniqueById([
      ...expandPlanningChunkNeighbors(persistedRanked, neighboringChunks, 48),
      ...ranked,
    ]).slice(0, 48);
  }
  const firstBySection = await prisma.$queryRaw<MaterialChunkSearchResult[]>`
    WITH section_chunks AS (
      SELECT
        "id",
        "materialRevisionId",
        "materialSectionId",
        "sourceFileId",
        "ordinal",
        "text",
        "tokenEstimate",
        "locator",
        "headingText",
        ROW_NUMBER() OVER (
          PARTITION BY "materialSectionId"
          ORDER BY "ordinal" ASC
        ) AS "sectionRank"
      FROM "material_chunks"
      WHERE "userId" = ${input.userId}
        AND "materialRevisionId" = ${input.materialRevisionId}
        AND "materialSectionId" IN (${Prisma.join(input.sectionIds)})
    )
    SELECT
      "id",
      "materialRevisionId",
      "materialSectionId",
      "sourceFileId",
      "ordinal",
      "text",
      "tokenEstimate",
      "locator",
      "headingText",
      0::double precision AS "vectorScore",
      0::double precision AS "lexicalScore",
      0::double precision AS "score"
    FROM section_chunks
    WHERE "sectionRank" = 1
    ORDER BY array_position(
      ARRAY[${Prisma.join(input.sectionIds)}]::text[],
      "materialSectionId"
    )
    LIMIT ${PLANNING_CHUNK_LIMIT}
  `;
  throwIfMaterialPlanningAborted(input.signal);
  const firstOcrBySection = uniqueBy(
    ocrChunks.filter((chunk) => chunk.materialSectionId !== null),
    (chunk) => chunk.materialSectionId,
  );
  const rankedChunks = uniqueById(ranked);
  const rankedSectionIds = new Set(
    rankedChunks.flatMap((chunk) =>
      chunk.materialSectionId ? [chunk.materialSectionId] : [],
    ),
  );
  const uncoveredSectionFallbacks = focusedTopic
    ? []
    : uniqueById([...firstBySection, ...firstOcrBySection]).filter(
        (chunk) =>
          chunk.materialSectionId !== null && !rankedSectionIds.has(chunk.materialSectionId),
      );
  const rankedMinimum = rankedChunks.length > 0 ? 1 : 0;
  const fallbackSlots = Math.min(
    uncoveredSectionFallbacks.length,
    PLANNING_CHUNK_LIMIT - rankedMinimum,
  );
  const rankedSlots = PLANNING_CHUNK_LIMIT - fallbackSlots;
  return {
    chunks: uniqueById([
      ...rankedChunks.slice(0, rankedSlots),
      ...uncoveredSectionFallbacks.slice(0, fallbackSlots),
    ]),
    focusedTopic,
    retrievalWarning,
  };
}

async function retrieveBackMatterRecoveryCandidates(input: {
  userId: string;
  materialRevisionId: string;
  query: string;
}) {
  return (
    await searchMaterialChunksLexical({
      userId: input.userId,
      materialRevisionId: input.materialRevisionId,
      query: input.query,
      limit: 48,
    })
  ).filter((chunk) => chunk.lexicalScore > 0);
}

async function retrieveMaterialSectionChunks(input: {
  userId: string;
  materialRevisionId: string;
  sectionIds: string[];
  anchorChunkIds: string[];
}): Promise<MaterialChunkSearchResult[]> {
  if (input.sectionIds.length === 0 || input.anchorChunkIds.length === 0) {
    return [];
  }
  const prisma = getPrisma();
  const anchors = await prisma.materialChunk.findMany({
    where: {
      userId: input.userId,
      materialRevisionId: input.materialRevisionId,
      materialSectionId: { in: input.sectionIds },
      id: { in: input.anchorChunkIds },
    },
    select: { materialSectionId: true, ordinal: true },
  });
  const minimumOrdinalBySection = new Map<string, number>();
  for (const anchor of anchors) {
    if (!anchor.materialSectionId) {
      continue;
    }
    const current = minimumOrdinalBySection.get(anchor.materialSectionId);
    minimumOrdinalBySection.set(
      anchor.materialSectionId,
      current === undefined ? anchor.ordinal : Math.min(current, anchor.ordinal),
    );
  }
  const sectionWindows = input.sectionIds.flatMap((sectionId) => {
    const minimumOrdinal = minimumOrdinalBySection.get(sectionId);
    return minimumOrdinal === undefined
      ? []
      : [{ materialSectionId: sectionId, ordinal: { gte: minimumOrdinal } }];
  });
  if (sectionWindows.length === 0) {
    return [];
  }
  const chunks = await prisma.materialChunk.findMany({
    where: {
      userId: input.userId,
      materialRevisionId: input.materialRevisionId,
      OR: sectionWindows,
    },
    orderBy: { ordinal: "asc" },
    take: PLANNING_CHUNK_LIMIT,
    select: {
      id: true,
      materialRevisionId: true,
      materialSectionId: true,
      sourceFileId: true,
      ordinal: true,
      text: true,
      tokenEstimate: true,
      locator: true,
      headingText: true,
    },
  });
  return chunks.map((chunk) => ({
    ...chunk,
    vectorScore: 0,
    lexicalScore: 0,
    score: 0,
  }));
}

async function retrieveOcrPlanningChunks(input: {
  userId: string;
  materialRevisionId: string;
  instruction: string;
  sections: MaterialPlanningSection[];
}): Promise<MaterialChunkSearchResult[]> {
  const boundedSections = input.sections.filter(
    (section) => section.pageStart !== null && section.pageEnd !== null,
  );
  if (boundedSections.length === 0) {
    return [];
  }
  const pages = await getPrisma().materialPage.findMany({
    where: {
      userId: input.userId,
      materialRevisionId: input.materialRevisionId,
      textStatus: MaterialPageTextStatus.OCR_READY,
      ocrText: { not: null },
      OR: boundedSections.map((section) => ({
        pageNumber: { gte: section.pageStart!, lte: section.pageEnd! },
      })),
    },
    orderBy: { pageNumber: "asc" },
    select: {
      id: true,
      pageNumber: true,
      ocrText: true,
      tokenEstimate: true,
    },
  });
  return pages.flatMap((page) => {
    if (!page.ocrText) {
      return [];
    }
    const section = boundedSections
      .filter(
        (candidate) =>
          candidate.pageStart! <= page.pageNumber && candidate.pageEnd! >= page.pageNumber,
      )
      .sort(
        (left, right) =>
          right.level - left.level ||
          left.pageEnd! - left.pageStart! - (right.pageEnd! - right.pageStart!) ||
          left.ordinal - right.ordinal,
      )[0];
    if (!section) {
      return [];
    }
    const lexicalScore = planningLexicalScore(input.instruction, page.ocrText);
    return [
      {
        id: materialPageEvidenceId(page.id),
        materialRevisionId: input.materialRevisionId,
        materialSectionId: section.id,
        sourceFileId: null,
        ordinal: page.pageNumber,
        text: page.ocrText,
        tokenEstimate: page.tokenEstimate,
        locator: {
          kind: "pdf",
          pageRange: { start: page.pageNumber, end: page.pageNumber },
        },
        headingText: `${section.title} · page ${page.pageNumber}`,
        vectorScore: 0,
        lexicalScore,
        score: lexicalScore,
      },
    ];
  });
}

function planningLexicalScore(query: string, text: string) {
  const terms = new Set(normalizeComparableText(query).split(" ").filter((term) => term.length > 1));
  if (terms.size === 0) {
    return 0;
  }
  const textTerms = new Set(normalizeComparableText(text).split(" "));
  return [...terms].filter((term) => textTerms.has(term)).length / terms.size;
}

function readJsonObject(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value)
    ? { ...(value as Record<string, unknown>) }
    : {};
}

function chooseStrongerSkillSimilarityMatch(
  first: SkillSimilarityMatch | null,
  second: SkillSimilarityMatch | null,
) {
  if (!first) {
    return second;
  }
  if (!second) {
    return first;
  }
  const confidenceRank = { exact: 3, likely: 2, possible: 1 } as const;
  const confidenceDifference =
    confidenceRank[first.confidence] - confidenceRank[second.confidence];
  if (confidenceDifference !== 0) {
    return confidenceDifference > 0 ? first : second;
  }
  return first.score >= second.score ? first : second;
}

function withDuplicateMatchMetadata(
  value: unknown,
  match: SkillSimilarityMatch,
  candidate: {
    id: string;
    title: string;
    objective: string | null;
    collectionId: string | null;
    rules: unknown;
    examples: unknown;
    exerciseConstraints: unknown;
    tags: readonly string[];
  },
  userOverride: boolean,
) {
  return {
    ...readJsonObject(value),
    duplicateMatch: {
      skillId: match.skill.id,
      skillFingerprint: match.skill.contentFingerprint,
      candidateSkillId: candidate.id,
      candidateFingerprint:
        buildSkillDuplicateCandidateFingerprint(candidate),
      confidence: match.confidence,
      score: match.score,
      userOverride,
    },
  };
}

function withoutDuplicateMatchMetadata(value: unknown) {
  const metadata = readJsonObject(value);
  delete metadata.duplicateMatch;
  return metadata;
}

function duplicateReviewMessage(match: SkillSimilarityMatch) {
  return match.confidence === "exact"
    ? `This draft matches ${match.skill.title}. Review the existing skill before adding a separate copy.`
    : match.confidence === "likely"
      ? `This draft appears to cover the same skill as ${match.skill.title}. Review it before adding a separate copy.`
      : `This draft may overlap with ${match.skill.title}. Review it before adding a separate copy.`;
}

function duplicateUseExistingMessage(skill: {
  title: string;
  status: SkillStatus;
}) {
  switch (skill.status) {
    case SkillStatus.ACTIVE:
      return `Kept ${skill.title}, which is already active. The duplicate draft was removed.`;
    case SkillStatus.DRAFT:
      return `Kept the existing draft ${skill.title}. Open it when you are ready to add it to practice.`;
    case SkillStatus.PAUSED:
      return `Kept ${skill.title} paused. Open it when you are ready to resume practice.`;
    case SkillStatus.ARCHIVED:
      return `Kept ${skill.title} archived. Open it if you want to restore it.`;
  }
}

function readTargetRepairContext(value: unknown) {
  const repair = readJsonObject(readJsonObject(value).targetRepair);
  return repair.required === true && typeof repair.verificationNote === "string"
    ? { verificationNote: repair.verificationNote }
    : null;
}

function readScopeBoundaries(value: unknown) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return { includeConcepts: [] as string[], excludeConcepts: [] as string[] };
  }
  const boundaries = (value as { scopeBoundaries?: unknown }).scopeBoundaries;
  if (!boundaries || typeof boundaries !== "object" || Array.isArray(boundaries)) {
    return { includeConcepts: [] as string[], excludeConcepts: [] as string[] };
  }
  const record = boundaries as {
    includeConcepts?: unknown;
    excludeConcepts?: unknown;
  };
  const readConcepts = (concepts: unknown) =>
    Array.isArray(concepts)
      ? concepts.filter((concept): concept is string => typeof concept === "string")
      : [];
  return {
    includeConcepts: readConcepts(record.includeConcepts),
    excludeConcepts: readConcepts(record.excludeConcepts),
  };
}

async function saveProposedMaterialPlan(input: {
  batchId: string;
  userId: string;
  plan: MaterialScopeResolution;
  model: string | null;
  structural: ReturnType<typeof resolveStructuralMaterialScope>;
  selectedSectionIds: string[];
  now: Date;
  expectedInstruction: string;
  expectedUpdatedAt: Date;
}) {
  const updated = await getPrisma().skillDraftBatch.updateMany({
    where: {
      id: input.batchId,
      userId: input.userId,
      confirmedAt: null,
      status: SkillDraftBatchStatus.PLANNING,
      instruction: input.expectedInstruction,
      updatedAt: input.expectedUpdatedAt,
    },
    data: {
      proposedPlan: toInputJson(input.plan),
      planningMetadata: {
        model: input.model,
        selectedSectionIds: input.selectedSectionIds,
        structuralReferences: input.structural.references.map((reference) => reference.label),
        candidateSectionCount: input.structural.candidateSectionIds.length,
        plannedAt: input.now.toISOString(),
      },
      status:
        input.plan.resolutionStatus === "ambiguous"
          ? SkillDraftBatchStatus.NEEDS_SCOPE
          : SkillDraftBatchStatus.PLANNED,
      requestedCount: input.plan.items.length,
      errorCode: null,
      errorMessage: null,
    },
  });
  if (updated.count === 1) {
    return planResult(input.batchId, input.plan);
  }
  return readCurrentMaterialPlanningResult({
    userId: input.userId,
    batchId: input.batchId,
  });
}

async function markMaterialBatchPlanningFailed(input: {
  userId: string;
  batchId: string;
  code: string;
  message: string;
  expectedInstruction: string;
  expectedUpdatedAt: Date;
}) {
  const updated = await getPrisma().skillDraftBatch.updateMany({
    where: {
      id: input.batchId,
      userId: input.userId,
      confirmedAt: null,
      status: SkillDraftBatchStatus.PLANNING,
      instruction: input.expectedInstruction,
      updatedAt: input.expectedUpdatedAt,
    },
    data: {
      status: SkillDraftBatchStatus.FAILED,
      errorCode: input.code,
      errorMessage: input.message.slice(0, 1_000),
      completedAt: new Date(),
    },
  });
  return updated.count === 1;
}

async function readCurrentMaterialPlanningResult(input: {
  userId: string;
  batchId: string;
}) {
  const current = await getPrisma().skillDraftBatch.findFirst({
    where: { id: input.batchId, userId: input.userId },
    select: { proposedPlan: true, status: true, errorMessage: true },
  });
  if (!current) {
    return { status: "not-found" as const, message: "Material skill batch was not found." };
  }
  const plan = materialScopeResolutionSchema.safeParse(current.proposedPlan);
  if (plan.success) {
    return planResult(input.batchId, plan.data);
  }
  if (current.status === SkillDraftBatchStatus.FAILED) {
    return {
      status: "failed" as const,
      batchId: input.batchId,
      message: current.errorMessage ?? "Material scope planning failed.",
    };
  }
  return {
    status: "superseded" as const,
    batchId: input.batchId,
    message: "A newer scope request is still being planned. Refresh this batch shortly.",
  };
}

async function markMaterialDraftItemFailed(input: {
  userId: string;
  itemId: string;
  claimId?: string;
  code: string;
  message: string;
  generationMetadata?: Prisma.InputJsonValue;
}) {
  const updated = await getPrisma().skillDraftBatchItem.updateMany({
    where: {
      id: input.itemId,
      userId: input.userId,
      ...(input.claimId
        ? {
            status: SkillDraftBatchItemStatus.GENERATING,
            generationClaimId: input.claimId,
          }
        : {}),
    },
    data: {
      status: SkillDraftBatchItemStatus.FAILED,
      generationClaimId: null,
      errorCode: input.code,
      errorMessage: input.message.slice(0, 1_000),
      ...(input.generationMetadata ? { generationMetadata: input.generationMetadata } : {}),
    },
  });
  return updated.count === 1;
}

async function releaseMaterialDraftItemForRetry(input: {
  userId: string;
  itemId: string;
  claimId: string;
  code: string;
  message: string;
}) {
  const updated = await getPrisma().skillDraftBatchItem.updateMany({
    where: {
      id: input.itemId,
      userId: input.userId,
      status: SkillDraftBatchItemStatus.GENERATING,
      generationClaimId: input.claimId,
    },
    data: {
      status: SkillDraftBatchItemStatus.PLANNED,
      generationClaimId: input.claimId,
      errorCode: input.code,
      errorMessage: input.message.slice(0, 1_000),
    },
  });
  return updated.count === 1;
}

function planResult(batchId: string, plan: MaterialScopeResolution) {
  return {
    status: plan.resolutionStatus === "ambiguous" ? ("needs-scope" as const) : ("planned" as const),
    batchId,
    plan,
  };
}

function normalizeMaterialDraftError(error: unknown) {
  if (error instanceof MaterialDraftGenerationError) {
    return error;
  }
  const message = error instanceof Error ? error.message : "Material draft generation failed.";
  const retryable = !/invalid|not found|no longer available|unsupported|confirmed evidence/i.test(
    message,
  );
  return new MaterialDraftGenerationError(message, { retryable, cause: error });
}

function getEventSendErrorLogDetails(error: unknown) {
  if (error instanceof Error) {
    return { name: error.name, message: error.message };
  }
  if (typeof error === "string" && error.trim()) {
    return { name: "Error", message: error };
  }
  return { name: "UnknownError", message: "Unknown event send failure." };
}

function isPrismaUniqueConstraintError(error: unknown) {
  return error instanceof Prisma.PrismaClientKnownRequestError && error.code === "P2002";
}

function safelyResolveEmbeddingGenerator() {
  try {
    return createGeminiMaterialEmbeddingGenerator();
  } catch {
    return null;
  }
}

function toInputJson(value: unknown): Prisma.InputJsonValue {
  return JSON.parse(JSON.stringify(value)) as Prisma.InputJsonValue;
}

function normalizeComparableText(value: string) {
  return value
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLocaleLowerCase()
    .replace(/[^\p{L}\p{N}]+/gu, " ")
    .trim();
}

function uniqueById<T extends { id: string }>(values: T[]) {
  return [...new Map(values.map((value) => [value.id, value])).values()];
}

function uniqueBy<T, K>(values: T[], key: (value: T) => K) {
  return [...new Map(values.map((value) => [key(value), value])).values()];
}
