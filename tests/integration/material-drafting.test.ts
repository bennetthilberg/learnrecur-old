import { randomUUID } from "node:crypto";

import { PDFDocument } from "pdf-lib";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";

import {
  GenerationJobKind,
  GenerationJobStatus,
  MaterialPageTextStatus,
  SkillDraftBatchItemStatus,
  SkillDraftBatchStatus,
  SkillStatus,
  SourceFileKind,
  SourceFileStatus,
  StudyMaterialKind,
} from "@/generated/prisma/client";
import type { MaterialDraftAiSetup } from "@/lib/materials/ai";
import {
  MaterialBatchActivationError,
  confirmMaterialPlan,
  excludeMaterialDraftItem,
  getMaterialDraftBatch,
  MaterialDraftGenerationError,
  planMaterialSkills,
  replanMaterialSkills,
  queueMaterialBatchActivation as queueMaterialBatchActivationService,
  retryMaterialBatchActivationItem,
  retryMaterialDraftItem,
  runMaterialBatchActivationJob,
  runMaterialDraftItemJob,
} from "@/lib/materials/batches";
import {
  createMaterialWithInitialRevision,
  finalizeMaterialRevision,
  requestMaterialDeletion,
} from "@/lib/materials/lifecycle";
import {
  MATERIAL_EMBEDDING_DIMENSIONS,
  searchMaterialChunksLexical,
  storeMaterialChunkEmbedding,
} from "@/lib/materials/retrieval";
import { recoverBackMatterMaterialScope } from "@/lib/materials/drafting";
import { materialPageEvidenceId } from "@/lib/materials/evidence-ids";
import * as museRetrieval from "@/lib/materials/muse-retrieval";
import { JobStageTimeoutError } from "@/lib/jobs/deadline";
import { loadLocalizedMaterialEvidence } from "@/lib/materials/evidence";
import * as materialEvidence from "@/lib/materials/evidence";
import * as materialDrafting from "@/lib/materials/drafting";
import { getPrisma } from "@/lib/prisma";
import {
  ACTIVATION_GENERATION_STALE_AFTER_MS,
  ACTIVATION_SUPERSEDED_JOB_MESSAGE,
  activateSkillDraft,
  refillChoiceExercisesForSkill,
  updateSkillDraft,
  type SkillSourceEvidenceLoader,
} from "@/lib/skills";
import { deleteSkillPermanently } from "@/lib/skills/delete";
import {
  SKILL_SIMILARITY_EMBEDDING_DIMENSIONS,
  buildSkillDuplicateCandidateFingerprint,
  buildSkillDuplicateReviewFingerprint,
} from "@/lib/skills/similarity";
import * as skillSimilarity from "@/lib/skills/similarity";
import type { SourceObjectStorage } from "@/lib/storage/s3";
import {
  ALPHA_SKILL_ACTIVATIONS_PER_DAY,
  getSkillActivationUsage,
} from "@/lib/usage-limits";

const runDatabaseTests = process.env.RUN_DATABASE_TESTS === "1";
const describeDatabase = runDatabaseTests ? describe : describe.skip;
const runId = `material_drafting_${randomUUID()}`;
const queueMaterialBatchActivation = (
  input: Parameters<typeof queueMaterialBatchActivationService>[0],
) =>
  queueMaterialBatchActivationService({
    embeddingGenerator: null,
    ...input,
  });
type MaterialBatchActivationEvent = Pick<
  Parameters<typeof runMaterialBatchActivationJob>[0],
  "userId" | "batchId" | "itemId" | "generationJobId"
>;

describeDatabase("material multi-skill drafting", () => {
  const prisma = getPrisma();
  const userId = `${runId}_owner`;
  const otherUserId = `${runId}_other`;
  let materialId = "";
  let materialRevisionId = "";
  let directSectionId = "";
  let indirectSectionId = "";
  let directChunkId = "";
  let indirectChunkId = "";
  let sourcePdfBytes = Buffer.alloc(0);
  let sourceStorage: SourceObjectStorage;
  let sourceEvidenceLoader: SkillSourceEvidenceLoader;

  beforeAll(async () => {
    const document = await PDFDocument.create();
    for (let page = 0; page < 220; page += 1) {
      document.addPage([500, 700]);
    }
    sourcePdfBytes = Buffer.from(await document.save());
    sourceStorage = createSourceStorage(() => sourcePdfBytes);
    sourceEvidenceLoader = createCachedSourceEvidenceLoader(sourceStorage);
    await prisma.user.createMany({
      data: [
        { id: userId, email: `${userId}@example.com` },
        { id: otherUserId, email: `${otherUserId}@example.com` },
      ],
    });
    const { material, revision } = await createMaterialWithInitialRevision({
      userId,
      title: "Spanish Grammar Atlas",
      kind: StudyMaterialKind.PDF,
    });
    materialId = material.id;
    materialRevisionId = revision.id;
    const chapter = await prisma.materialSection.create({
      data: {
        userId,
        materialRevisionId,
        ordinal: 0,
        level: 1,
        title: "Chapter 4 · Object pronouns",
        normalizedTitle: "chapter 4 object pronouns",
        pageStart: 90,
        pageEnd: 128,
        headingPath: ["Chapter 4"],
      },
    });
    const direct = await prisma.materialSection.create({
      data: {
        id: `${runId}_section_a_direct`,
        userId,
        materialRevisionId,
        parentId: chapter.id,
        ordinal: 1,
        level: 2,
        title: "4.1 Direct object pronouns",
        normalizedTitle: "4 1 direct object pronouns",
        pageStart: 94,
        pageEnd: 105,
        headingPath: ["Chapter 4", "Direct object pronouns"],
      },
    });
    const indirect = await prisma.materialSection.create({
      data: {
        id: `${runId}_section_z_indirect`,
        userId,
        materialRevisionId,
        parentId: chapter.id,
        ordinal: 2,
        level: 2,
        title: "4.2 Indirect object pronouns",
        normalizedTitle: "4 2 indirect object pronouns",
        pageStart: 106,
        pageEnd: 116,
        headingPath: ["Chapter 4", "Indirect object pronouns"],
      },
    });
    directSectionId = direct.id;
    indirectSectionId = indirect.id;
    const chunks = await Promise.all([
      prisma.materialChunk.create({
        data: {
          userId,
          materialRevisionId,
          materialSectionId: direct.id,
          ordinal: 0,
          text: "Direct object pronouns replace nouns receiving the action and precede a conjugated verb.",
          tokenEstimate: 15,
          contentHash: `sha256:${runId}:direct`,
          headingText: direct.title,
          locator: { kind: "pdf", pageRange: { start: 94, end: 105 } },
        },
      }),
      prisma.materialChunk.create({
        data: {
          userId,
          materialRevisionId,
          materialSectionId: indirect.id,
          ordinal: 80,
          text: "Indirect object pronouns identify the recipient and distinguish singular le from plural les.",
          tokenEstimate: 15,
          contentHash: `sha256:${runId}:indirect`,
          headingText: indirect.title,
          locator: { kind: "pdf", pageRange: { start: 106, end: 116 } },
        },
      }),
    ]);
    directChunkId = chunks[0].id;
    indirectChunkId = chunks[1].id;
    await prisma.materialChunk.createMany({
      data: Array.from({ length: 79 }, (_, index) => ({
        userId,
        materialRevisionId,
        materialSectionId: direct.id,
        ordinal: index + 1,
        text: `Direct object pronoun filler excerpt ${index + 1}.`,
        tokenEstimate: 8,
        contentHash: `sha256:${runId}:direct-filler-${index + 1}`,
        headingText: direct.title,
        locator: { kind: "pdf", pageRange: { start: 94, end: 105 } },
      })),
    });
    await prisma.sourceFile.create({
      data: {
        userId,
        materialRevisionId,
        kind: SourceFileKind.PDF,
        status: SourceFileStatus.READY,
        originalName: "spanish-grammar-atlas.pdf",
        mimeType: "application/pdf",
        storageBucket: "test-materials",
        storageKey: `${runId}/spanish.pdf`,
        extractedText:
          "UNRELATED CHAPTER SIX SOURCE TEXT. This whole-book excerpt must not ground chapter four exercises.",
      },
    });
    await finalizeMaterialRevision({
      userId,
      materialId,
      materialRevisionId,
      contentHash: `sha256:${runId}`,
      byteSize: 8_192,
      pageCount: 220,
      storageBucket: "test-materials",
      storageKey: `${runId}/spanish.pdf`,
    });
  });

  afterAll(async () => {
    await prisma.user.deleteMany({ where: { id: { in: [userId, otherUserId] } } });
    await prisma.$disconnect();
  });

  it("plans idempotently, warns on an exact saved duplicate, and confirms only new items", async () => {
    const existing = await prisma.skill.create({
      data: {
        userId,
        title: "Direct object pronouns",
        objective: "Replace direct objects with the correct Spanish pronoun in short sentences.",
        tags: ["spanish"],
        status: SkillStatus.DRAFT,
      },
    });
    const planScope = vi.fn<MaterialDraftAiSetup["planScope"]>(async () => ({
      resolutionStatus: "resolved",
      resolvedScopeLabel: "Chapter 4, pages 90–128",
      clarification: null,
      warnings: [],
      items: [
        {
          key: "direct-object-pronouns",
          title: "Direct object pronouns",
          objective: "Replace direct objects with the correct Spanish pronoun in short sentences.",
          materialSectionIds: [directSectionId],
          evidenceChunkIds: [directChunkId],
        },
        {
          key: "indirect-object-pronouns",
          title: "Indirect object pronouns",
          objective: "Choose the correct indirect object pronoun for a recipient in a short sentence.",
          materialSectionIds: [indirectSectionId],
          evidenceChunkIds: [indirectChunkId],
        },
      ],
    }));
    const aiSetup = createAiSetup({ planScope });
    const request = {
      materialId,
      materialRevisionId,
      instruction: "Make two skills from chapter four.",
      idempotencyKey: `${runId}_duplicate_plan`,
    };
    const first = await planMaterialSkills({
      userId,
      input: request,
      now: new Date(),
      aiSetup,
      embeddingGenerator: null,
    });
    const retry = await planMaterialSkills({
      userId,
      input: request,
      now: new Date(),
      aiSetup,
      embeddingGenerator: null,
    });
    expect(first.status).toBe("planned");
    expect(retry).toEqual(first);
    expect(planScope).toHaveBeenCalledTimes(1);
    if (first.status !== "planned") {
      throw new Error("expected planned batch");
    }
    expect(first.plan.items[0]).toMatchObject({ overlapSkillId: existing.id });
    expect(
      await planMaterialSkills({
        userId: otherUserId,
        input: { ...request, idempotencyKey: `${runId}_foreign_plan` },
        now: new Date(),
        aiSetup,
        embeddingGenerator: null,
      }),
    ).toMatchObject({ status: "not-found" });

    const events: string[] = [];
    expect(
      await confirmMaterialPlan({
        userId,
        input: {
          batchId: first.batchId,
          plan: {
            ...first.plan,
            items: first.plan.items.map((item, index) =>
              index === 0 ? { ...item, title: "Tampered title" } : item,
            ),
          },
        },
        now: new Date(),
        eventSender: { async sendMaterialDraftItemRequested() {} },
      }),
    ).toMatchObject({ status: "invalid" });
    const confirmed = await confirmMaterialPlan({
      userId,
      input: { batchId: first.batchId, plan: first.plan },
      now: new Date(),
      eventSender: {
        async sendMaterialDraftItemRequested(payload) {
          events.push(payload.itemId);
        },
      },
    });
    expect(confirmed).toMatchObject({ status: "queued" });
    const batch = await getMaterialDraftBatch({ userId, batchId: first.batchId });
    expect(batch?.items.map((item) => item.status)).toEqual([
      SkillDraftBatchItemStatus.EXCLUDED,
      SkillDraftBatchItemStatus.PLANNED,
    ]);
    expect(events).toEqual([batch?.items[1].id]);

    const repeatedEvents: string[] = [];
    const repeated = await confirmMaterialPlan({
      userId,
      input: { batchId: first.batchId, plan: first.plan },
      now: new Date(),
      eventSender: {
        async sendMaterialDraftItemRequested(payload) {
          repeatedEvents.push(payload.itemId);
        },
      },
    });
    expect(repeated).toMatchObject({
      status: "queued",
      alreadyConfirmed: true,
      queuedItemIds: [batch?.items[1].id],
    });
    expect(repeatedEvents).toEqual([batch?.items[1].id]);

    await expect(
      prisma.skillDraftBatchItem.update({
        where: { id: batch?.items[1].id },
        data: {
          skillId: (
            await prisma.skill.create({
              data: { userId: otherUserId, title: "Foreign skill", tags: [] },
            })
          ).id,
        },
      }),
    ).rejects.toThrow();
  });

  it("keeps a planning batch retryable after a provider timeout when requested by a worker", async () => {
    const request = {
      materialId,
      materialRevisionId,
      instruction: "Create one skill from direct object pronouns in chapter four.",
      idempotencyKey: `${runId}_planning_timeout_retry`,
    };

    await expect(planMaterialSkills({
      userId,
      input: request,
      now: new Date(),
      preservePlanningOnTimeout: true,
      aiSetup: createAiSetup({
        planScope: async () => {
          throw new JobStageTimeoutError("Scope planner timed out.", "material scope planning");
        },
      }),
      embeddingGenerator: null,
    })).rejects.toMatchObject({ name: "JobStageTimeoutError", retryable: true });

    const batch = await prisma.skillDraftBatch.findFirstOrThrow({
      where: { userId, idempotencyKey: request.idempotencyKey },
      select: { id: true, status: true, errorCode: true },
    });
    expect(batch).toMatchObject({ status: SkillDraftBatchStatus.PLANNING, errorCode: null });

    const retry = await planMaterialSkills({
      userId,
      input: request,
      now: new Date(),
      aiSetup: createAiSetup({
        planScope: async () => ({
          resolutionStatus: "resolved",
          resolvedScopeLabel: "Chapter four direct object pronouns",
          clarification: null,
          warnings: [],
          items: [
            {
              key: "direct-object-pronouns-timeout-retry",
              title: "Direct object pronouns",
              objective: "Replace direct objects with the correct Spanish pronoun in short sentences.",
              materialSectionIds: [directSectionId],
              evidenceChunkIds: [directChunkId],
            },
          ],
        }),
      }),
      embeddingGenerator: null,
    });

    expect(retry).toMatchObject({ status: "planned", batchId: batch.id });
  }, 60_000);

  it("aborts a material scope planner that never resolves before the worker deadline", async () => {
    const request = {
      materialId,
      materialRevisionId,
      instruction: "Create one skill from direct object pronouns in chapter four.",
      idempotencyKey: `${runId}_planning_hang_deadline`,
    };
    let plannerSignal: AbortSignal | undefined;

    await expect(planMaterialSkills({
      userId,
      input: request,
      now: new Date(),
      deadlineAt: new Date(Date.now() + 66_000),
      preservePlanningOnTimeout: true,
      aiSetup: createAiSetup({
        planScope: ({ signal }) => {
          plannerSignal = signal;
          return new Promise(() => {});
        },
      }),
      embeddingGenerator: null,
    })).rejects.toMatchObject({
      name: "JobStageTimeoutError",
      stage: "material scope planning",
      retryable: true,
    });

    expect(plannerSignal?.aborted).toBe(true);
    await expect(prisma.skillDraftBatch.findFirstOrThrow({
      where: { userId, idempotencyKey: request.idempotencyKey },
      select: { status: true, errorCode: true },
    })).resolves.toMatchObject({ status: SkillDraftBatchStatus.PLANNING, errorCode: null });
  }, 60_000);

  it("aborts a material scope reviewer that never resolves before the worker deadline", async () => {
    const request = {
      materialId,
      materialRevisionId,
      instruction: "Create one skill from direct object pronouns in chapter four.",
      idempotencyKey: `${runId}_review_hang_deadline`,
    };
    let reviewSignal: AbortSignal | undefined;

    await expect(planMaterialSkills({
      userId,
      input: request,
      now: new Date(),
      deadlineAt: new Date(Date.now() + 66_000),
      preservePlanningOnTimeout: true,
      aiSetup: createAiSetup({
        planScope: async () => ({
          resolutionStatus: "resolved",
          resolvedScopeLabel: "Chapter four direct object pronouns",
          clarification: null,
          warnings: [],
          items: [{
            key: "direct-object-pronouns-review-hang",
            title: "Direct object pronouns",
            objective: "Replace direct objects with the correct Spanish pronoun in short sentences.",
            materialSectionIds: [directSectionId],
            evidenceChunkIds: [directChunkId],
          }],
        }),
        reviewScope: ({ signal }) => {
          reviewSignal = signal;
          return new Promise(() => {});
        },
      }),
      embeddingGenerator: null,
    })).rejects.toMatchObject({
      name: "JobStageTimeoutError",
      stage: "material scope review",
      retryable: true,
    });

    expect(reviewSignal?.aborted).toBe(true);
    await expect(prisma.skillDraftBatch.findFirstOrThrow({
      where: { userId, idempotencyKey: request.idempotencyKey },
      select: { status: true, errorCode: true },
    })).resolves.toMatchObject({ status: SkillDraftBatchStatus.PLANNING, errorCode: null });
  }, 60_000);

  it("aborts lazy PDF OCR that stalls before material scope retrieval", async () => {
    const request = {
      materialId,
      materialRevisionId,
      instruction: "Create one skill from direct object pronouns in chapter four.",
      idempotencyKey: `${runId}_ocr_hang_deadline`,
    };
    const page = await prisma.materialPage.create({
      data: {
        userId,
        materialRevisionId,
        pageNumber: 100,
        textStatus: MaterialPageTextStatus.NEEDS_OCR,
        contentHash: `sha256:${runId}:ocr-hang-deadline`,
      },
    });
    let ocrSignal: AbortSignal | undefined;
    const ocrGenerator = vi.fn(({ signal }: { signal?: AbortSignal }) => {
        ocrSignal = signal;
        return new Promise<never>((_, reject) => {
          signal?.addEventListener("abort", () => reject(signal.reason), { once: true });
        });
      });

    try {
      await expect(planMaterialSkills({
        userId,
        input: request,
        now: new Date(),
        deadlineAt: new Date(Date.now() + 66_000),
        preservePlanningOnTimeout: true,
        aiSetup: createAiSetup(),
        embeddingGenerator: null,
        ocrStorage: sourceStorage,
        ocrGenerator,
      })).rejects.toMatchObject({
        name: "JobStageTimeoutError",
        stage: "material page OCR",
        retryable: true,
      });

      expect(ocrGenerator).toHaveBeenCalledOnce();
      expect(ocrSignal?.aborted).toBe(true);
      await expect(prisma.materialPage.findUniqueOrThrow({ where: { id: page.id } }))
        .resolves.toMatchObject({
          textStatus: MaterialPageTextStatus.NEEDS_OCR,
          metadata: { reason: "worker-deadline", retryable: true },
        });
      await expect(prisma.skillDraftBatch.findFirstOrThrow({
        where: { userId, idempotencyKey: request.idempotencyKey },
        select: { status: true, errorCode: true },
      })).resolves.toMatchObject({ status: SkillDraftBatchStatus.PLANNING, errorCode: null });
    } finally {
      await prisma.materialPage.delete({ where: { id: page.id } });
    }
  }, 60_000);

  it("does not save a plan while relevant OCR pages still have active claims", async () => {
    const request = {
      materialId,
      materialRevisionId,
      instruction: "Create one skill from direct object pronouns in chapter four.",
      idempotencyKey: `${runId}_ocr_claim_still_active`,
    };
    const ocr = vi
      .spyOn(materialEvidence, "ensureMaterialPageOcr")
      .mockResolvedValue({ status: "in-progress", processedPageCount: 0 });
    const planScope = vi.fn<MaterialDraftAiSetup["planScope"]>(async () => ({
      resolutionStatus: "ambiguous",
      resolvedScopeLabel: "Unexpected plan",
      clarification: null,
      warnings: [],
      items: [],
    }));

    try {
      await expect(planMaterialSkills({
        userId,
        input: request,
        now: new Date(),
        deadlineAt: new Date(Date.now() + 66_000),
        preservePlanningOnTimeout: true,
        aiSetup: createAiSetup({ planScope }),
        embeddingGenerator: null,
      })).rejects.toMatchObject({
        name: "JobStageTimeoutError",
        stage: "material page OCR",
        retryable: true,
      });

      expect(planScope).not.toHaveBeenCalled();
      await expect(prisma.skillDraftBatch.findFirstOrThrow({
        where: { userId, idempotencyKey: request.idempotencyKey },
        select: { status: true, errorCode: true, proposedPlan: true },
      })).resolves.toMatchObject({
        status: SkillDraftBatchStatus.PLANNING,
        errorCode: null,
        proposedPlan: null,
      });
    } finally {
      ocr.mockRestore();
    }
  }, 60_000);

  it("aborts a material retrieval embedding that stalls before scope planning", async () => {
    const request = {
      materialId,
      materialRevisionId,
      instruction: "Create one skill from direct object pronouns in chapter four.",
      idempotencyKey: `${runId}_retrieval_hang_deadline`,
    };
    let embeddingSignal: AbortSignal | undefined;
    const ocr = vi
      .spyOn(materialEvidence, "ensureMaterialPageOcr")
      .mockResolvedValue({ status: "not-needed", processedPageCount: 0 });

    try {
      await expect(planMaterialSkills({
        userId,
        input: request,
        now: new Date(),
        deadlineAt: new Date(Date.now() + 66_000),
        preservePlanningOnTimeout: true,
        aiSetup: createAiSetup(),
        embeddingGenerator: ({ signal }) => {
          embeddingSignal = signal;
          return new Promise(() => {});
        },
      })).rejects.toMatchObject({
        name: "JobStageTimeoutError",
        stage: "material scope retrieval",
        retryable: true,
      });

      expect(embeddingSignal?.aborted).toBe(true);
      await expect(prisma.skillDraftBatch.findFirstOrThrow({
        where: { userId, idempotencyKey: request.idempotencyKey },
        select: { status: true, errorCode: true },
      })).resolves.toMatchObject({ status: SkillDraftBatchStatus.PLANNING, errorCode: null });
    } finally {
      ocr.mockRestore();
    }
  }, 60_000);

  it("bounds back-matter evidence recovery inside the planning deadline", async () => {
    const request = {
      materialId,
      materialRevisionId,
      instruction: "Create one skill from direct object pronouns in chapter four.",
      idempotencyKey: `${runId}_evidence_recovery_hang_deadline`,
    };
    const recovery = vi
      .spyOn(materialDrafting, "recoverBackMatterMaterialScope")
      .mockImplementation(() => new Promise(() => {}));

    try {
      await expect(planMaterialSkills({
        userId,
        input: request,
        now: new Date(),
        deadlineAt: new Date(Date.now() + 66_000),
        preservePlanningOnTimeout: true,
        aiSetup: createAiSetup(),
        embeddingGenerator: null,
      })).rejects.toMatchObject({
        name: "JobStageTimeoutError",
        stage: "material scope evidence recovery",
        retryable: true,
      });

      await expect(prisma.skillDraftBatch.findFirstOrThrow({
        where: { userId, idempotencyKey: request.idempotencyKey },
        select: { status: true, errorCode: true },
      })).resolves.toMatchObject({ status: SkillDraftBatchStatus.PLANNING, errorCode: null });
    } finally {
      recovery.mockRestore();
    }
  }, 60_000);

  it("aborts material skill similarity that stalls after scope review", async () => {
    const request = {
      materialId,
      materialRevisionId,
      instruction: "Create one skill from direct object pronouns in chapter four.",
      idempotencyKey: `${runId}_similarity_hang_deadline`,
    };
    let similaritySignal: AbortSignal | undefined;
    const ocr = vi
      .spyOn(materialEvidence, "ensureMaterialPageOcr")
      .mockResolvedValue({ status: "not-needed", processedPageCount: 0 });
    const similarity = vi
      .spyOn(skillSimilarity, "findSimilarSkillsForUser")
      .mockImplementation(({ signal }) => {
        similaritySignal = signal;
        return new Promise(() => {});
      });

    try {
      await expect(planMaterialSkills({
        userId,
        input: request,
        now: new Date(),
        deadlineAt: new Date(Date.now() + 66_000),
        preservePlanningOnTimeout: true,
        aiSetup: createAiSetup({
          planScope: async () => ({
            resolutionStatus: "resolved",
            resolvedScopeLabel: "Chapter four direct object pronouns",
            clarification: null,
            warnings: [],
            items: [{
              key: "similarity-deadline",
              title: "Direct object pronouns",
              objective: "Replace direct objects with the correct Spanish pronoun in short sentences.",
              materialSectionIds: [directSectionId],
              evidenceChunkIds: [directChunkId],
            }],
          }),
        }),
        embeddingGenerator: null,
      })).rejects.toMatchObject({
        name: "JobStageTimeoutError",
        stage: "material skill similarity",
        retryable: true,
      });

      expect(similaritySignal?.aborted).toBe(true);
      await expect(prisma.skillDraftBatch.findFirstOrThrow({
        where: { userId, idempotencyKey: request.idempotencyKey },
        select: { status: true, errorCode: true },
      })).resolves.toMatchObject({ status: SkillDraftBatchStatus.PLANNING, errorCode: null });
    } finally {
      similarity.mockRestore();
      ocr.mockRestore();
    }
  }, 60_000);

  it("returns a failed planning result for synchronous planning timeouts", async () => {
    const request = {
      materialId,
      materialRevisionId,
      instruction: "Create one skill from indirect object pronouns in chapter four.",
      idempotencyKey: `${runId}_planning_timeout_sync`,
    };

    await expect(planMaterialSkills({
      userId,
      input: request,
      now: new Date(),
      aiSetup: createAiSetup({
        planScope: async () => {
          throw new JobStageTimeoutError("Scope planner timed out.", "material scope planning");
        },
      }),
      embeddingGenerator: null,
    })).resolves.toMatchObject({ status: "failed" });

    await expect(prisma.skillDraftBatch.findFirstOrThrow({
      where: { userId, idempotencyKey: request.idempotencyKey },
      select: { status: true, errorCode: true, errorMessage: true, completedAt: true },
    })).resolves.toMatchObject({
      status: SkillDraftBatchStatus.FAILED,
      errorCode: "PLANNING_FAILED",
      completedAt: expect.any(Date),
    });

    const planned = await planMaterialSkills({
      userId,
      input: {
        materialId,
        materialRevisionId,
        instruction: "Plan a recoverable indirect object pronoun scope.",
        idempotencyKey: `${runId}_replan_timeout_sync`,
      },
      now: new Date(),
      aiSetup: createAiSetup({
        planScope: async () => ({
          resolutionStatus: "resolved",
          resolvedScopeLabel: "Chapter four indirect object pronouns",
          clarification: null,
          warnings: [],
          items: [{
            key: "indirect-object-pronouns-sync-replan",
            title: "Indirect object pronouns",
            objective: "Choose the correct indirect object pronoun for a recipient in a short sentence.",
            materialSectionIds: [indirectSectionId],
            evidenceChunkIds: [indirectChunkId],
          }],
        }),
      }),
      embeddingGenerator: null,
    });
    expect(planned.status).toBe("planned");
    if (planned.status !== "planned") throw new Error("expected a planned batch before replan");

    await expect(replanMaterialSkills({
      userId,
      input: { batchId: planned.batchId, instruction: "Refine the same indirect pronoun scope." },
      now: new Date(),
      aiSetup: createAiSetup({
        planScope: async () => {
          throw new JobStageTimeoutError("Scope planner timed out.", "material scope planning");
        },
      }),
      embeddingGenerator: null,
    })).resolves.toMatchObject({ status: "failed" });
    await expect(prisma.skillDraftBatch.findUniqueOrThrow({
      where: { id: planned.batchId },
      select: { status: true, errorCode: true },
    })).resolves.toMatchObject({
      status: SkillDraftBatchStatus.FAILED,
      errorCode: "PLANNING_FAILED",
    });
  }, 60_000);

  it("creates the planned skill when its reviewed duplicate disappears before confirmation", async () => {
    const fixtureId = randomUUID();
    const title = `Stale plan duplicate ${fixtureId}`;
    const objective =
      `Choose a direct object pronoun for a noun in fixture ${fixtureId}.`;
    const existing = await prisma.skill.create({
      data: {
        userId,
        title,
        objective,
        tags: ["stale-plan-match"],
        status: SkillStatus.DRAFT,
      },
    });
    const planned = await planMaterialSkills({
      userId,
      input: {
        materialId,
        materialRevisionId,
        instruction: "Make the stale plan duplicate fixture.",
        idempotencyKey: `${runId}_stale_plan_match`,
      },
      now: new Date(),
      aiSetup: createAiSetup({
        planScope: async () => ({
          resolutionStatus: "resolved",
          resolvedScopeLabel: "Chapter 4",
          clarification: null,
          warnings: [],
          items: [
            {
              key: "stale-plan-match",
              title,
              objective,
              materialSectionIds: [directSectionId],
              evidenceChunkIds: [directChunkId],
            },
          ],
        }),
      }),
      embeddingGenerator: null,
    });
    if (planned.status !== "planned") {
      throw new Error("expected a stale duplicate plan");
    }
    expect(planned.plan.items[0]).toMatchObject({
      overlapSkillId: existing.id,
      overlapConfidence: "exact",
    });

    await prisma.skill.delete({ where: { id: existing.id } });
    const events: string[] = [];
    const confirmed = await confirmMaterialPlan({
      userId,
      input: { batchId: planned.batchId, plan: planned.plan },
      now: new Date(),
      eventSender: {
        async sendMaterialDraftItemRequested(payload) {
          events.push(payload.itemId);
        },
      },
    });

    expect(confirmed).toMatchObject({
      status: "queued",
      queuedItemIds: [expect.any(String)],
    });
    expect(events).toHaveLength(1);
    const item = await prisma.skillDraftBatchItem.findUniqueOrThrow({
      where: { id: events[0] },
      select: {
        status: true,
        overlapSkillId: true,
        errorCode: true,
        generationMetadata: true,
      },
    });
    expect(item).toMatchObject({
      status: SkillDraftBatchItemStatus.PLANNED,
      overlapSkillId: null,
      errorCode: null,
    });
    expect(item.generationMetadata).not.toHaveProperty("duplicateMatch");
  });

  it("keeps a separate draft but requires current duplicate review when its saved match changes", async () => {
    const fixtureId = randomUUID();
    const title = `Edited plan duplicate ${fixtureId}`;
    const objective =
      `Choose a direct object pronoun for a noun in fixture ${fixtureId}.`;
    const existing = await prisma.skill.create({
      data: {
        userId,
        title,
        objective,
        tags: ["edited-plan-match"],
        status: SkillStatus.DRAFT,
      },
    });
    const planned = await planMaterialSkills({
      userId,
      input: {
        materialId,
        materialRevisionId,
        instruction: "Make the edited plan duplicate fixture.",
        idempotencyKey: `${runId}_edited_plan_match`,
      },
      now: new Date(),
      aiSetup: createAiSetup({
        planScope: async () => ({
          resolutionStatus: "resolved",
          resolvedScopeLabel: "Chapter 4",
          clarification: null,
          warnings: [],
          items: [
            {
              key: "edited-plan-match",
              title,
              objective,
              materialSectionIds: [directSectionId],
              evidenceChunkIds: [directChunkId],
            },
          ],
        }),
      }),
      embeddingGenerator: null,
    });
    if (planned.status !== "planned") {
      throw new Error("expected an edited duplicate plan");
    }
    expect(planned.plan.items[0]).toMatchObject({
      overlapSkillId: existing.id,
      overlapSkillFingerprint:
        buildSkillDuplicateReviewFingerprint(existing),
    });
    const changedExisting = await prisma.skill.update({
      where: { id: existing.id },
      data: {
        tags: ["edited-plan-match", "changed-after-review"],
      },
    });
    const events: string[] = [];

    await expect(
      confirmMaterialPlan({
        userId,
        input: { batchId: planned.batchId, plan: planned.plan },
        now: new Date(),
        eventSender: {
          async sendMaterialDraftItemRequested(payload) {
            events.push(payload.itemId);
          },
        },
      }),
    ).resolves.toMatchObject({ status: "queued" });
    expect(events).toHaveLength(1);
    expect(
      await prisma.skillDraftBatchItem.findUnique({
        where: { id: events[0] },
        select: {
          status: true,
          overlapSkillId: true,
          generationMetadata: true,
        },
      }),
    ).toMatchObject({
      status: SkillDraftBatchItemStatus.PLANNED,
      overlapSkillId: null,
      generationMetadata: {
        scopeBoundaries: expect.any(Object),
        duplicateMatch: {
          skillId: existing.id,
          skillFingerprint:
            buildSkillDuplicateReviewFingerprint(changedExisting),
          preserveGeneratedDraft: true,
          userOverride: false,
        },
      },
    });
    await expect(
      runMaterialDraftItemJob({
        userId,
        batchId: planned.batchId,
        itemId: events[0],
        aiSetup: createAiSetup({
          draftObjectiveByTitle: new Map([[title, objective]]),
        }),
        sourceEvidenceLoader,
      }),
    ).resolves.toMatchObject({ status: "ready" });
    await expect(
      prisma.skill.count({ where: { userId, title, objective } }),
    ).resolves.toBe(2);
    const savedItem = await prisma.skillDraftBatchItem.findUniqueOrThrow({
      where: { id: events[0] },
      select: { status: true, skillId: true },
    });
    expect(savedItem).toMatchObject({
      status: SkillDraftBatchItemStatus.READY,
      skillId: expect.any(String),
    });
    expect(savedItem.skillId).not.toBe(existing.id);
    const activationEvents: string[] = [];
    await expect(
      queueMaterialBatchActivation({
        userId,
        input: {
          batchId: planned.batchId,
          itemIds: [events[0]],
        },
        now: new Date("2032-03-01T12:00:00.000Z"),
        embeddingGenerator: null,
        eventSender: {
          async sendMaterialBatchActivationRequested(payload) {
            activationEvents.push(payload.itemId);
          },
        },
      }),
    ).resolves.toMatchObject({
      status: "review-required",
      reviewItemIds: [events[0]],
    });
    expect(activationEvents).toEqual([]);
    await expect(
      prisma.skillDraftBatchItem.findUniqueOrThrow({
        where: { id: events[0] },
      }),
    ).resolves.toMatchObject({
      status: SkillDraftBatchItemStatus.READY,
      errorCode: "DUPLICATE_REVIEW_REQUIRED",
    });
  });

  it("does not carry a stale plan-stage separate choice into activation consent", async () => {
    const fixtureId = randomUUID();
    const targetKey = `stale-explicit-match-${fixtureId}`;
    const title = `Stale explicit duplicate ${fixtureId}`;
    const objective =
      `Choose a direct object pronoun in stale fixture ${fixtureId}.`;
    const existing = await prisma.skill.create({
      data: {
        userId,
        title,
        objective,
        tags: ["stale-explicit-match"],
        status: SkillStatus.DRAFT,
      },
    });
    const planned = await planMaterialSkills({
      userId,
      input: {
        materialId,
        materialRevisionId,
        instruction: "Make the stale explicit duplicate fixture.",
        idempotencyKey: `${runId}_stale_explicit_${fixtureId}`,
      },
      now: new Date(),
      aiSetup: createAiSetup({
        planScope: async () => ({
          resolutionStatus: "resolved",
          resolvedScopeLabel: "Chapter 4",
          clarification: null,
          warnings: [],
          items: [
            {
              key: targetKey,
              title,
              objective,
              materialSectionIds: [directSectionId],
              evidenceChunkIds: [directChunkId],
            },
          ],
        }),
      }),
      embeddingGenerator: null,
    });
    if (planned.status !== "planned") {
      throw new Error("expected a stale explicit duplicate plan");
    }
    const changedExisting = await prisma.skill.update({
      where: { id: existing.id },
      data: {
        rules: { items: ["Changed after the learner reviewed this match."] },
      },
    });

    const draftEvents: string[] = [];
    await expect(
      confirmMaterialPlan({
        userId,
        input: {
          batchId: planned.batchId,
          plan: planned.plan,
          createSeparatelyTargetKeys: [targetKey],
        },
        now: new Date(),
        eventSender: {
          async sendMaterialDraftItemRequested(payload) {
            draftEvents.push(payload.itemId);
          },
        },
      }),
    ).resolves.toMatchObject({ status: "queued" });
    expect(draftEvents).toHaveLength(1);
    const queuedItem = await prisma.skillDraftBatchItem.findUniqueOrThrow({
      where: { id: draftEvents[0] },
      select: { generationMetadata: true, status: true },
    });
    expect(queuedItem).toMatchObject({
      status: SkillDraftBatchItemStatus.PLANNED,
      generationMetadata: {
        duplicateMatch: {
          skillId: existing.id,
          skillFingerprint:
            buildSkillDuplicateReviewFingerprint(changedExisting),
          preserveGeneratedDraft: true,
          userOverride: false,
        },
      },
    });
    await expect(
      runMaterialDraftItemJob({
        userId,
        batchId: planned.batchId,
        itemId: draftEvents[0],
        aiSetup: createAiSetup({
          draftObjectiveByTitle: new Map([[title, objective]]),
        }),
        sourceEvidenceLoader,
      }),
    ).resolves.toMatchObject({ status: "ready" });
    await expect(
      prisma.skill.count({ where: { userId, title, objective } }),
    ).resolves.toBe(2);
    const generatedItem =
      await prisma.skillDraftBatchItem.findUniqueOrThrow({
        where: { id: draftEvents[0] },
        select: { skillId: true, status: true },
      });
    expect(generatedItem).toMatchObject({
      skillId: expect.any(String),
      status: SkillDraftBatchItemStatus.READY,
    });
    expect(generatedItem.skillId).not.toBe(existing.id);

    const activationEvents: string[] = [];
    await expect(
      queueMaterialBatchActivation({
        userId,
        input: {
          batchId: planned.batchId,
          itemIds: [draftEvents[0]],
        },
        now: new Date("2032-03-01T12:00:00.000Z"),
        embeddingGenerator: null,
        eventSender: {
          async sendMaterialBatchActivationRequested(payload) {
            activationEvents.push(payload.itemId);
          },
        },
      }),
    ).resolves.toMatchObject({
      status: "review-required",
      reviewItemIds: [draftEvents[0]],
    });
    expect(activationEvents).toEqual([]);
    await expect(
      prisma.skillDraftBatchItem.findUniqueOrThrow({
        where: { id: draftEvents[0] },
      }),
    ).resolves.toMatchObject({
      status: SkillDraftBatchItemStatus.READY,
      errorCode: "DUPLICATE_REVIEW_REQUIRED",
    });
  });

  it("uses the current existing-skill status when confirming an unchanged match", async () => {
    const fixtureId = randomUUID();
    const title = `Status-updated plan duplicate ${fixtureId}`;
    const objective =
      `Choose the matching direct object pronoun in fixture ${fixtureId}.`;
    const existing = await prisma.skill.create({
      data: {
        userId,
        title,
        objective,
        tags: ["status-updated-plan-match"],
        status: SkillStatus.DRAFT,
      },
    });
    const planned = await planMaterialSkills({
      userId,
      input: {
        materialId,
        materialRevisionId,
        instruction: "Make the status-updated duplicate fixture.",
        idempotencyKey: `${runId}_status_updated_plan_match`,
      },
      now: new Date(),
      aiSetup: createAiSetup({
        planScope: async () => ({
          resolutionStatus: "resolved",
          resolvedScopeLabel: "Chapter 4",
          clarification: null,
          warnings: [],
          items: [
            {
              key: "status-updated-plan-match",
              title,
              objective,
              materialSectionIds: [directSectionId],
              evidenceChunkIds: [directChunkId],
            },
          ],
        }),
      }),
      embeddingGenerator: null,
    });
    if (planned.status !== "planned") {
      throw new Error("expected a status-updated duplicate plan");
    }
    await prisma.skill.update({
      where: { id: existing.id },
      data: { status: SkillStatus.PAUSED },
    });

    await expect(
      confirmMaterialPlan({
        userId,
        input: { batchId: planned.batchId, plan: planned.plan },
        now: new Date(),
        eventSender: { async sendMaterialDraftItemRequested() {} },
      }),
    ).resolves.toMatchObject({
      status: "queued",
      queuedItemIds: [],
    });
    expect(
      await prisma.skillDraftBatchItem.findFirstOrThrow({
        where: { batchId: planned.batchId, userId },
        select: { status: true, overlapSkillId: true, errorCode: true, errorMessage: true },
      }),
    ).toMatchObject({
      status: SkillDraftBatchItemStatus.EXCLUDED,
      overlapSkillId: existing.id,
      errorCode: "DUPLICATE_USE_EXISTING",
      errorMessage: expect.stringMatching(/paused/i),
    });
  });

  it("preflights a narrow proposal against the original request before confirmation", async () => {
    const planScope = vi
      .fn()
      .mockResolvedValueOnce({ resolutionStatus: "resolved" })
      .mockResolvedValueOnce({
        resolutionStatus: "resolved",
        resolvedScopeLabel: "Chapter 4 object pronouns",
        clarification: null,
        warnings: [],
        items: [
          {
            key: "direct-pronouns",
            title: "Direct object pronouns",
            objective: "Choose a direct object pronoun for one noun in a Spanish sentence.",
            materialSectionIds: [directSectionId],
            evidenceChunkIds: [directChunkId],
          },
        ],
      });
    const reviewScope = vi.fn(async () => ({
      resolutionStatus: "resolved",
      resolvedScopeLabel: "Chapter 4 direct and indirect object pronouns",
      clarification: null,
      warnings: [],
      items: [
        {
          key: "direct-pronouns",
          title: "Direct object pronouns",
          objective: "Choose a direct object pronoun for one noun in a Spanish sentence.",
          includeConcepts: ["direct object pronoun selection"],
          excludeConcepts: ["indirect object pronouns"],
          materialSectionIds: [directSectionId],
          evidenceChunkIds: [directChunkId],
        },
        {
          key: "indirect-pronouns",
          title: "Indirect object pronouns",
          objective: "Choose an indirect object pronoun for a recipient in a Spanish sentence.",
          includeConcepts: ["indirect object pronoun selection"],
          excludeConcepts: ["direct object pronouns"],
          materialSectionIds: [indirectSectionId],
          evidenceChunkIds: [indirectChunkId],
        },
      ],
    }));
    const result = await planMaterialSkills({
      userId,
      input: {
        materialId,
        materialRevisionId,
        instruction: "Make skills for the object pronouns in chapter four.",
        idempotencyKey: `${runId}_scope_preflight`,
      },
      now: new Date(),
      aiSetup: createAiSetup({ planScope, reviewScope }),
      embeddingGenerator: null,
    });

    expect(result).toMatchObject({
      status: "planned",
      plan: {
        items: [
          { key: "direct-pronouns", excludeConcepts: ["indirect object pronouns"] },
          { key: "indirect-pronouns", excludeConcepts: ["direct object pronouns"] },
        ],
      },
    });
    expect(reviewScope).toHaveBeenCalledWith(
      expect.objectContaining({
        instruction: "Make skills for the object pronouns in chapter four.",
        candidatePlan: expect.objectContaining({ items: [expect.any(Object)] }),
      }),
    );
    expect(planScope).toHaveBeenCalledTimes(2);
  });

  it("keeps an explicit plan-stage duplicate override through the final draft save guard", async () => {
    const title = "Explicit duplicate override fixture";
    const objective =
      "Place an object pronoun correctly before a conjugated Spanish verb.";
    await prisma.skill.create({
      data: {
        id: `zz_${runId}`,
        userId,
        title,
        objective,
        tags: ["duplicate-override-shadow"],
        status: SkillStatus.PAUSED,
      },
    });
    const existing = await prisma.skill.create({
      data: {
        id: `aa_${runId}`,
        userId,
        title,
        objective,
        tags: ["duplicate-override"],
        status: SkillStatus.ACTIVE,
      },
    });
    const planned = await planMaterialSkills({
      userId,
      input: {
        materialId,
        materialRevisionId,
        instruction: "Make the explicit duplicate override fixture.",
        idempotencyKey: `${runId}_duplicate_override`,
      },
      now: new Date(),
      aiSetup: createAiSetup({
        planScope: async () => ({
          resolutionStatus: "resolved",
          resolvedScopeLabel: "Chapter 4",
          clarification: null,
          warnings: [],
          items: [
            {
              key: "explicit-duplicate-override",
              title,
              objective,
              materialSectionIds: [directSectionId],
              evidenceChunkIds: [directChunkId],
            },
          ],
        }),
      }),
      embeddingGenerator: null,
    });
    if (planned.status !== "planned") {
      throw new Error("expected a duplicate override plan");
    }
    expect(planned.plan.items[0]).toMatchObject({
      overlapSkillId: existing.id,
      overlapConfidence: "exact",
    });
    const events: string[] = [];
    const confirmed = await confirmMaterialPlan({
      userId,
      input: {
        batchId: planned.batchId,
        plan: planned.plan,
        createSeparatelyTargetKeys: ["explicit-duplicate-override"],
      },
      now: new Date(),
      eventSender: {
        async sendMaterialDraftItemRequested(payload) {
          events.push(payload.itemId);
        },
      },
    });
    expect(confirmed).toMatchObject({ status: "queued" });
    expect(events).toHaveLength(1);

    await expect(
      runMaterialDraftItemJob({
        userId,
        batchId: planned.batchId,
        itemId: events[0],
        aiSetup: createAiSetup({ rejectTitle: title }),
        sourceEvidenceLoader,
      }),
    ).resolves.toMatchObject({ status: "failed" });
    const failedItem = await prisma.skillDraftBatchItem.findFirstOrThrow({
      where: { id: events[0], userId },
      select: { generationMetadata: true },
    });
    expect(failedItem.generationMetadata).toMatchObject({
      duplicateMatch: {
        skillId: existing.id,
        userOverride: true,
      },
    });
    await expect(
      retryMaterialDraftItem({
        userId,
        batchId: planned.batchId,
        itemId: events[0],
        now: new Date(),
        eventSender: {
          async sendMaterialDraftItemRequested(payload) {
            events.push(payload.itemId);
          },
        },
      }),
    ).resolves.toMatchObject({ status: "queued" });
    await expect(
      runMaterialDraftItemJob({
        userId,
        batchId: planned.batchId,
        itemId: events[1],
        aiSetup: createAiSetup(),
        sourceEvidenceLoader,
      }),
    ).resolves.toMatchObject({ status: "ready" });
    await expect(
      prisma.skill.count({ where: { userId, title, objective } }),
    ).resolves.toBe(3);
  });

  it("requires clarification without calling the semantic planner when a chapter is absent", async () => {
    const planScope = vi.fn<MaterialDraftAiSetup["planScope"]>(async () => {
      throw new Error("semantic planner should not run");
    });
    const result = await planMaterialSkills({
      userId,
      input: {
        materialId,
        materialRevisionId,
        instruction: "Make the first concept in chapter nine.",
        idempotencyKey: `${runId}_missing_chapter`,
      },
      now: new Date(),
      aiSetup: createAiSetup({ planScope }),
      embeddingGenerator: null,
    });

    expect(result).toMatchObject({
      status: "needs-scope",
      plan: {
        resolutionStatus: "ambiguous",
        items: [],
        clarification: expect.stringMatching(/chapter nine/i),
      },
    });
    expect(planScope).not.toHaveBeenCalled();
    if (result.status !== "needs-scope") {
      throw new Error("expected ambiguous scope");
    }
    expect(
      await prisma.skillDraftBatch.findUnique({
        where: { id: result.batchId },
        select: { status: true, proposedPlan: true, confirmedAt: true },
      }),
    ).toMatchObject({
      status: SkillDraftBatchStatus.NEEDS_SCOPE,
      proposedPlan: expect.any(Object),
      confirmedAt: null,
    });
  });

  it("records structured diagnostics when draft events cannot be queued", async () => {
    const planScope = vi.fn<MaterialDraftAiSetup["planScope"]>(async () => ({
      resolutionStatus: "resolved" as const,
      resolvedScopeLabel: "Chapter 4",
      clarification: null,
      warnings: [],
      items: [
        {
          key: "queue-diagnostic",
          title: "Queue diagnostic skill",
          objective: "Choose a direct object pronoun in one focused sentence.",
          materialSectionIds: [directSectionId],
          evidenceChunkIds: [directChunkId],
        },
      ],
    }));
    const planned = await planMaterialSkills({
      userId,
      input: {
        materialId,
        materialRevisionId,
        instruction: "Make one diagnostic skill from chapter four.",
        idempotencyKey: `${runId}_queue_diagnostic`,
      },
      now: new Date(),
      aiSetup: createAiSetup({ planScope }),
      embeddingGenerator: null,
    });
    if (planned.status !== "planned") {
      throw new Error("expected planned batch");
    }
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});

    const result = await confirmMaterialPlan({
      userId,
      input: { batchId: planned.batchId, plan: planned.plan },
      now: new Date(),
      eventSender: {
        async sendMaterialDraftItemRequested() {
          throw new Error("connect ECONNREFUSED 127.0.0.1:8288");
        },
      },
    });

    expect(result).toMatchObject({ status: "partial", failedItemIds: [expect.any(String)] });
    if (result.status !== "partial" || !result.failedItemIds?.[0]) {
      throw new Error("expected one failed item id");
    }
    const failedItemId = result.failedItemIds[0];
    expect(errorSpy).toHaveBeenCalledWith(
      "[background-jobs] material draft event send failed",
      expect.objectContaining({
        batchId: planned.batchId,
        itemId: failedItemId,
        error: expect.objectContaining({
          message: "connect ECONNREFUSED 127.0.0.1:8288",
        }),
      }),
    );
    errorSpy.mockRestore();
  });

  it("stores and returns a public message when Gemini scope planning fails", async () => {
    const providerError = JSON.stringify({
      error: {
        code: 400,
        message: "Request contains an invalid argument.",
        status: "INVALID_ARGUMENT",
      },
    });
    const result = await planMaterialSkills({
      userId,
      input: {
        materialId,
        materialRevisionId,
        instruction: "Make one skill from chapter four.",
        idempotencyKey: `${runId}_provider_failure`,
      },
      now: new Date(),
      aiSetup: createAiSetup({
        planScope: vi.fn(async () => {
          throw new Error(providerError);
        }),
      }),
      embeddingGenerator: null,
    });

    expect(result).toMatchObject({
      status: "failed",
      message: "LearnRecur could not review that scope. Check the request and try again.",
    });
    expect(JSON.stringify(result)).not.toContain("INVALID_ARGUMENT");
    if (!("batchId" in result)) {
      throw new Error("expected failed batch id");
    }
    expect(
      await prisma.skillDraftBatch.findUnique({
        where: { id: result.batchId },
        select: { errorCode: true, errorMessage: true },
      }),
    ).toEqual({
      errorCode: "PLANNING_FAILED",
      errorMessage: "LearnRecur could not review that scope. Check the request and try again.",
    });
  });

  it("does not let a slower duplicate planner overwrite the plan that finished first", async () => {
    let releaseFirstPlanner = () => {};
    const firstPlannerMayFinish = new Promise<void>((resolve) => {
      releaseFirstPlanner = resolve;
    });
    let markFirstPlannerStarted = () => {};
    const firstPlannerStarted = new Promise<void>((resolve) => {
      markFirstPlannerStarted = resolve;
    });
    let callCount = 0;
    const planScope = vi.fn<MaterialDraftAiSetup["planScope"]>(async () => {
      callCount += 1;
      const call = callCount;
      if (call === 1) {
        markFirstPlannerStarted();
        await firstPlannerMayFinish;
      }
      return {
        resolutionStatus: "resolved" as const,
        resolvedScopeLabel: "Chapter 4",
        clarification: null,
        warnings: [],
        items: [
          {
            key: "duplicate-planner-fence",
            title: call === 1 ? "Stale slower plan" : "Winning faster plan",
            objective: "Choose a direct object pronoun in one focused sentence.",
            materialSectionIds: [directSectionId],
            evidenceChunkIds: [directChunkId],
          },
        ],
      };
    });
    const request = {
      materialId,
      materialRevisionId,
      instruction: "Make one skill from chapter four.",
      idempotencyKey: `${runId}_concurrent_plan_fence`,
    };
    const firstRequest = planMaterialSkills({
      userId,
      input: request,
      now: new Date("2026-07-09T11:00:00.000Z"),
      aiSetup: createAiSetup({ planScope }),
      embeddingGenerator: null,
    });
    await firstPlannerStarted;
    const secondResult = await planMaterialSkills({
      userId,
      input: request,
      now: new Date("2026-07-09T11:00:01.000Z"),
      aiSetup: createAiSetup({ planScope }),
      embeddingGenerator: null,
    });
    releaseFirstPlanner();
    const firstResult = await firstRequest;

    expect(planScope).toHaveBeenCalledTimes(2);
    expect(secondResult).toMatchObject({
      status: "planned",
      plan: { items: [{ title: "Winning faster plan" }] },
    });
    expect(firstResult).toEqual(secondResult);
  });

  it("rechecks the reviewed plan after acquiring the confirmation lock", async () => {
    const planScope = vi.fn<MaterialDraftAiSetup["planScope"]>(async () => ({
      resolutionStatus: "resolved" as const,
      resolvedScopeLabel: "Chapter 4",
      clarification: null,
      warnings: [],
      items: [
        {
          key: "confirmation-fence",
          title: "Original reviewed plan",
          objective: "Choose a direct object pronoun in one focused sentence.",
          materialSectionIds: [directSectionId],
          evidenceChunkIds: [directChunkId],
        },
      ],
    }));
    const planned = await planMaterialSkills({
      userId,
      input: {
        materialId,
        materialRevisionId,
        instruction: "Make one skill from chapter four.",
        idempotencyKey: `${runId}_confirmation_fence`,
      },
      now: new Date(),
      aiSetup: createAiSetup({ planScope }),
      embeddingGenerator: null,
    });
    if (planned.status !== "planned") {
      throw new Error("expected planned confirmation fence batch");
    }
    const replacementPlan = {
      ...planned.plan,
      items: planned.plan.items.map((item) => ({ ...item, title: "Newer replacement plan" })),
    };
    let confirmation: ReturnType<typeof confirmMaterialPlan> | null = null;

    await prisma.$transaction(async (tx) => {
      await tx.$queryRaw`
        SELECT "id" FROM "skill_draft_batches"
        WHERE "id" = ${planned.batchId} AND "userId" = ${userId}
        FOR UPDATE
      `;
      confirmation = confirmMaterialPlan({
        userId,
        input: { batchId: planned.batchId, plan: planned.plan },
        now: new Date(),
        eventSender: { async sendMaterialDraftItemRequested() {} },
      });
      await new Promise((resolve) => setTimeout(resolve, 100));
      await tx.skillDraftBatch.update({
        where: { id: planned.batchId },
        data: { proposedPlan: JSON.parse(JSON.stringify(replacementPlan)) },
      });
    });

    expect(await confirmation).toMatchObject({ status: "invalid" });
    expect(
      await prisma.skillDraftBatchItem.count({ where: { batchId: planned.batchId } }),
    ).toBe(0);
  });

  it("gives the planner a fallback chunk from every candidate section before the cap", async () => {
    const planScope = vi.fn<MaterialDraftAiSetup["planScope"]>(async () => ({
      resolutionStatus: "resolved",
      resolvedScopeLabel: "Chapter 4",
      clarification: null,
      warnings: [],
      items: [
        {
          key: "balanced-fallback",
          title: "Balanced pronoun evidence",
          objective: "Distinguish direct and indirect object pronoun evidence in short examples.",
          materialSectionIds: [directSectionId, indirectSectionId],
          evidenceChunkIds: [directChunkId, indirectChunkId],
        },
      ],
    }));
    const result = await planMaterialSkills({
      userId,
      input: {
        materialId,
        materialRevisionId,
        instruction: "Make one skill for each section in chapter four.",
        idempotencyKey: `${runId}_balanced_fallback`,
      },
      now: new Date(),
      aiSetup: createAiSetup({ planScope }),
      embeddingGenerator: null,
    });

    expect(result.status).toBe("planned");
    const planningInput = planScope.mock.calls[0]?.[0];
    expect(planningInput?.chunks.map((chunk) => chunk.id)).toEqual(
      expect.arrayContaining([directChunkId, indirectChunkId]),
    );
  });

  async function createDuplicateLessonFixture(label: string) {
    const { material, revision } = await createMaterialWithInitialRevision({
      userId,
      title: `Duplicate lesson fixture ${label}`,
      kind: StudyMaterialKind.PDF,
    });
    const firstLesson = await prisma.materialSection.create({
      data: {
        userId,
        materialRevisionId: revision.id,
        ordinal: 0,
        level: 1,
        title: "Lesson L",
        normalizedTitle: "lesson l",
        pageStart: 30,
        pageEnd: 70,
        headingPath: ["Lesson L"],
      },
    });
    const selectedLesson = await prisma.materialSection.create({
      data: {
        userId,
        materialRevisionId: revision.id,
        ordinal: 1,
        level: 1,
        title: "Lesson L",
        normalizedTitle: "lesson l",
        pageStart: 369,
        pageEnd: 424,
        headingPath: ["Lesson L"],
      },
    });
    const selectedSubsection = await prisma.materialSection.create({
      data: {
        userId,
        materialRevisionId: revision.id,
        parentId: selectedLesson.id,
        ordinal: 2,
        level: 2,
        title: "Affirmative tú commands",
        normalizedTitle: "affirmative tu commands",
        pageStart: 411,
        pageEnd: 424,
        headingPath: ["Lesson L", "Affirmative tú commands"],
      },
    });
    const firstChunk = `${runId}_${label}_first_chunk`;
    const selectedChunk = `${runId}_${label}_selected_chunk`;
    await prisma.materialChunk.createMany({
      data: [
        {
          id: firstChunk,
          userId,
          materialRevisionId: revision.id,
          materialSectionId: firstLesson.id,
          ordinal: 0,
          text: "Noun gender and plural article forms use matching endings.",
          tokenEstimate: 10,
          contentHash: `sha256:${firstChunk}`,
          headingText: firstLesson.title,
          locator: { kind: "pdf", pageRange: { start: 30, end: 30 } },
        },
        {
          id: selectedChunk,
          userId,
          materialRevisionId: revision.id,
          materialSectionId: selectedSubsection.id,
          ordinal: 1,
          text: "Affirmative tú reflexive commands attach the pronoun and accent the command; irse is the exception.",
          tokenEstimate: 14,
          contentHash: `sha256:${selectedChunk}`,
          headingText: selectedSubsection.title,
          locator: { kind: "pdf", pageRange: { start: 411, end: 414 } },
        },
      ],
    });
    await finalizeMaterialRevision({
      userId,
      materialId: material.id,
      materialRevisionId: revision.id,
      contentHash: `sha256:${runId}:${label}`,
      byteSize: 32_768,
      pageCount: 628,
      storageBucket: "test-materials",
      storageKey: `${runId}/${label}.pdf`,
    });
    return {
      material,
      revision,
      firstLesson,
      selectedLesson,
      selectedSubsection,
      firstChunk,
      selectedChunk,
    };
  }

  it("uses the selected section ID when duplicate lesson titles exist", async () => {
    const fixture = await createDuplicateLessonFixture("explicit-section-plan");
    const planScope = vi.fn<MaterialDraftAiSetup["planScope"]>(async () => ({
      resolutionStatus: "resolved",
      resolvedScopeLabel: "Affirmative tú reflexive commands",
      clarification: null,
      warnings: [],
      items: [
        {
          key: "affirmative-tu-reflexive-command",
          title: "Affirmative tú reflexive commands",
          objective: "Form affirmative tú reflexive commands with correct accent placement, including irse.",
          materialSectionIds: [fixture.selectedSubsection.id],
          evidenceChunkIds: [fixture.selectedChunk],
        },
      ],
    }));

    const result = await planMaterialSkills({
      userId,
      input: {
        materialId: fixture.material.id,
        materialRevisionId: fixture.revision.id,
        instruction:
          "Create one skill from Lesson L about affirmative tú reflexive commands, accent placement, and the irse exception.",
        idempotencyKey: `${runId}_explicit_section_plan`,
        sectionIds: [fixture.selectedLesson.id],
      },
      now: new Date(),
      aiSetup: createAiSetup({ planScope }),
      embeddingGenerator: null,
    });

    expect(result.status).toBe("planned");
    const sameSelectionRetry = await planMaterialSkills({
      userId,
      input: {
        materialId: fixture.material.id,
        materialRevisionId: fixture.revision.id,
        instruction:
          "Create one skill from Lesson L about affirmative tú reflexive commands, accent placement, and the irse exception.",
        idempotencyKey: `${runId}_explicit_section_plan`,
        sectionIds: [fixture.selectedLesson.id],
      },
      now: new Date(),
      aiSetup: createAiSetup({ planScope }),
      embeddingGenerator: null,
    });
    const changedSelectionRetry = await planMaterialSkills({
      userId,
      input: {
        materialId: fixture.material.id,
        materialRevisionId: fixture.revision.id,
        instruction:
          "Create one skill from Lesson L about affirmative tú reflexive commands, accent placement, and the irse exception.",
        idempotencyKey: `${runId}_explicit_section_plan`,
        sectionIds: [fixture.firstLesson.id],
      },
      now: new Date(),
      aiSetup: createAiSetup({ planScope }),
      embeddingGenerator: null,
    });
    expect(sameSelectionRetry).toEqual(result);
    expect(changedSelectionRetry).toMatchObject({
      status: "invalid",
      message: expect.stringContaining("different selected section scope"),
    });
    expect(planScope).toHaveBeenCalledTimes(1);
    const planningInput = planScope.mock.calls[0]?.[0];
    expect(planningInput?.sections.map((section) => section.id)).toEqual([
      fixture.selectedLesson.id,
      fixture.selectedSubsection.id,
    ]);
    expect(planningInput?.chunks.map((chunk) => chunk.id)).toContain(fixture.selectedChunk);
    expect(planningInput?.chunks.map((chunk) => chunk.id)).not.toContain(fixture.firstChunk);
  });

  it("preserves selected section IDs when replanning a material batch", async () => {
    const fixture = await createDuplicateLessonFixture("explicit-section-replan");
    const needsScope = await planMaterialSkills({
      userId,
      input: {
        materialId: fixture.material.id,
        materialRevisionId: fixture.revision.id,
        instruction: "Choose a section for a Spanish grammar skill.",
        idempotencyKey: `${runId}_explicit_section_replan`,
      },
      now: new Date(),
      aiSetup: createAiSetup({
        planScope: async () => ({
          resolutionStatus: "ambiguous",
          resolvedScopeLabel: "Choose a lesson",
          clarification: "Select one lesson.",
          clarificationOptions: [],
          warnings: [],
          items: [],
        }),
      }),
      embeddingGenerator: null,
    });
    expect(needsScope.status).toBe("needs-scope");
    if (needsScope.status !== "needs-scope") {
      throw new Error("expected an editable material scope");
    }

    const planScope = vi.fn<MaterialDraftAiSetup["planScope"]>(async () => ({
      resolutionStatus: "resolved",
      resolvedScopeLabel: "Affirmative tú reflexive commands",
      clarification: null,
      warnings: [],
      items: [
        {
          key: "affirmative-tu-reflexive-command",
          title: "Affirmative tú reflexive commands",
          objective: "Form affirmative tú reflexive commands with correct accent placement, including irse.",
          materialSectionIds: [fixture.selectedSubsection.id],
          evidenceChunkIds: [fixture.selectedChunk],
        },
      ],
    }));
    const result = await replanMaterialSkills({
      userId,
      input: {
        batchId: needsScope.batchId,
        instruction:
          "Create one skill from Lesson L about affirmative tú reflexive commands, accent placement, and the irse exception.",
        sectionIds: [fixture.selectedLesson.id],
      },
      now: new Date(),
      aiSetup: createAiSetup({ planScope }),
      embeddingGenerator: null,
    });

    expect(result.status).toBe("planned");
    const planningInput = planScope.mock.calls[0]?.[0];
    expect(planningInput?.sections.map((section) => section.id)).toEqual([
      fixture.selectedLesson.id,
      fixture.selectedSubsection.id,
    ]);
    expect(planningInput?.chunks.map((chunk) => chunk.id)).toContain(fixture.selectedChunk);
    expect(planningInput?.chunks.map((chunk) => chunk.id)).not.toContain(fixture.firstChunk);
  });

  it("retrieves the reflexive-verb lesson when semantic search is unavailable", async () => {
    const { material, revision } = await createMaterialWithInitialRevision({
      userId,
      title: "Reflexive verb retrieval fixture",
      kind: StudyMaterialKind.PDF,
    });
    const frontMatter = await prisma.materialSection.create({
      data: {
        userId,
        materialRevisionId: revision.id,
        ordinal: 0,
        level: 1,
        title: "Front matter",
        normalizedTitle: "front matter",
        pageStart: 1,
        pageEnd: 12,
        headingPath: ["Front matter"],
      },
    });
    const lesson = await prisma.materialSection.create({
      data: {
        userId,
        materialRevisionId: revision.id,
        ordinal: 1,
        level: 1,
        title: "Lesson 12",
        normalizedTitle: "lesson 12",
        pageStart: 193,
        pageEnd: 205,
        headingPath: ["Lesson 12"],
      },
    });
    const unrelatedSectionIds = Array.from(
      { length: 12 },
      (_, index) => `${runId}_reflexive_unrelated_${index}`,
    );
    await prisma.materialSection.createMany({
      data: unrelatedSectionIds.map((id, index) => ({
        id,
        userId,
        materialRevisionId: revision.id,
        ordinal: index + 2,
        level: 1,
        title: `Unrelated lesson ${index + 1}`,
        normalizedTitle: `unrelated lesson ${index + 1}`,
        pageStart: index + 20,
        pageEnd: index + 20,
        headingPath: [`Unrelated lesson ${index + 1}`],
      })),
    });
    const teachingChunkIds = [
      `${runId}_reflexive_teaching_1`,
      `${runId}_reflexive_teaching_2`,
      `${runId}_reflexive_teaching_3`,
    ];
    const accentChunkId = `${runId}_accented_topic`;
    await prisma.materialChunk.createMany({
      data: [
        {
          id: `${runId}_reflexive_toc`,
          userId,
          materialRevisionId: revision.id,
          materialSectionId: frontMatter.id,
          ordinal: 0,
          text: "Contents: Reflexive Verbs 193. Reflexive pronouns 193.",
          tokenEstimate: 8,
          contentHash: `sha256:${runId}:reflexive-toc`,
          headingText: "Front matter",
          locator: { kind: "pdf", pageRange: { start: 8, end: 8 } },
        },
        ...teachingChunkIds.map((id, index) => ({
          id,
          userId,
          materialRevisionId: revision.id,
          materialSectionId: lesson.id,
          ordinal: index + 1,
          text:
            index === 0
              ? "Reflexive verbs use reflexive pronouns that agree with the subject."
              : index === 1
                ? "With a conjugated reflexive verb, the reflexive pronoun normally comes before the verb."
                : "Reflexive verbs can express routines, movement, emotion, and reciprocal actions.",
          tokenEstimate: 12,
          contentHash: `sha256:${id}`,
          headingText: "Lesson 12",
          locator: {
            kind: "pdf",
            pageRange: { start: 193 + index, end: 193 + index },
          },
        })),
        ...unrelatedSectionIds.map((sectionId, index) => ({
          id: `${runId}_reflexive_unrelated_chunk_${index}`,
          userId,
          materialRevisionId: revision.id,
          materialSectionId: sectionId,
          ordinal: index + 20,
          text: `General unrelated grammar material ${index + 1}.`,
          tokenEstimate: 8,
          contentHash: `sha256:${runId}:reflexive-unrelated-${index}`,
          headingText: `Unrelated lesson ${index + 1}`,
          locator: {
            kind: "pdf",
            pageRange: { start: index + 20, end: index + 20 },
          },
        })),
        {
          id: accentChunkId,
          userId,
          materialRevisionId: revision.id,
          materialSectionId: unrelatedSectionIds[0],
          ordinal: 50,
          text: "Números cardinales del 21 al 99.",
          tokenEstimate: 7,
          contentHash: `sha256:${runId}:accented-topic`,
          headingText: "Números",
          locator: { kind: "pdf", pageRange: { start: 20, end: 20 } },
        },
      ],
    });
    await prisma.materialPage.createMany({
      data: [
        {
          userId,
          materialRevisionId: revision.id,
          pageNumber: 194,
          ocrText: "Reflexive verbs place reflexive pronouns according to the verb form.",
          textStatus: MaterialPageTextStatus.OCR_READY,
          contentHash: `sha256:${runId}:reflexive-ocr`,
          tokenEstimate: 10,
        },
        {
          userId,
          materialRevisionId: revision.id,
          pageNumber: 20,
          ocrText: "An unrelated scanned worksheet with no relevant topic.",
          textStatus: MaterialPageTextStatus.OCR_READY,
          contentHash: `sha256:${runId}:unrelated-ocr`,
          tokenEstimate: 9,
        },
      ],
    });
    await finalizeMaterialRevision({
      userId,
      materialId: material.id,
      materialRevisionId: revision.id,
      contentHash: `sha256:${runId}:reflexive-retrieval`,
      byteSize: 16_384,
      pageCount: 205,
      storageBucket: "test-materials",
      storageKey: `${runId}/reflexive-retrieval.pdf`,
    });
    const planScope = vi.fn<MaterialDraftAiSetup["planScope"]>(async () => ({
      resolutionStatus: "resolved" as const,
      resolvedScopeLabel: "Reflexive verb rules",
      clarification: null,
      clarificationOptions: [],
      warnings: [],
      items: [
        {
          key: "reflexive-pronoun-placement",
          title: "Reflexive pronoun placement",
          objective: "Place reflexive pronouns correctly with conjugated Spanish verbs.",
          includeConcepts: ["reflexive pronoun placement"],
          excludeConcepts: ["commands"],
          materialSectionIds: [lesson.id],
          evidenceChunkIds: [teachingChunkIds[1]],
        },
      ],
    }));
    const lexicalMatches = await searchMaterialChunksLexical({
      userId,
      materialRevisionId: revision.id,
      query: "reflexive verb",
      prefixMatching: true,
      limit: 48,
    });
    expect(
      lexicalMatches.filter((chunk) => chunk.lexicalScore > 0).map((chunk) => chunk.id),
    ).toEqual(expect.arrayContaining(teachingChunkIds));
    const accentedMatches = await searchMaterialChunksLexical({
      userId,
      materialRevisionId: revision.id,
      query: "números",
      prefixMatching: true,
      limit: 10,
    });
    expect(
      accentedMatches.filter((chunk) => chunk.lexicalScore > 0).map((chunk) => chunk.id),
    ).toContain(accentChunkId);

    const result = await planMaterialSkills({
      userId,
      input: {
        materialId: material.id,
        materialRevisionId: revision.id,
        instruction: "make skills for the reflexive verb rules",
        idempotencyKey: `${runId}_reflexive_retrieval`,
      },
      now: new Date(),
      aiSetup: createAiSetup({ planScope }),
      embeddingGenerator: async () => {
        throw Object.assign(new Error("Not Found"), { status: 404 });
      },
    });

    expect(
      result.status,
      `planning result: ${JSON.stringify(result)}; planner calls: ${planScope.mock.calls.length}`,
    ).toBe("planned");
    const planningInput = planScope.mock.calls[0]?.[0];
    expect(planningInput?.sections.map((section) => section.id)).toEqual([lesson.id]);
    expect(planningInput?.chunks.map((chunk) => chunk.id)).toEqual(
      expect.arrayContaining(teachingChunkIds),
    );
    expect(planningInput?.chunks.every((chunk) => chunk.materialSectionId === lesson.id)).toBe(
      true,
    );
  });

  it("recovers preterit teaching chunks from a malformed outline without embeddings", async () => {
    const { material, revision } = await createMaterialWithInitialRevision({
      userId,
      title: "Malformed preterit outline fixture",
      kind: StudyMaterialKind.PDF,
    });
    const broadPart = await prisma.materialSection.create({
      data: {
        userId,
        materialRevisionId: revision.id,
        ordinal: 0,
        level: 1,
        title: "Part II",
        normalizedTitle: "part ii",
        pageStart: 255,
        pageEnd: 299,
        headingPath: ["Part II"],
      },
    });
    const answerKey = await prisma.materialSection.create({
      data: {
        userId,
        materialRevisionId: revision.id,
        ordinal: 1,
        level: 1,
        title: "Chapter 14 The Preterit Tense",
        normalizedTitle: "chapter 14 the preterit tense",
        pageStart: 601,
        pageEnd: 601,
        headingPath: ["Chapter 14 The Preterit Tense"],
      },
    });
    const incidentalSection = await prisma.materialSection.create({
      data: {
        userId,
        materialRevisionId: revision.id,
        ordinal: 2,
        level: 1,
        title: "Earlier lessons",
        normalizedTitle: "earlier lessons",
        pageStart: 50,
        pageEnd: 140,
        headingPath: ["Earlier lessons"],
      },
    });
    const teachingChunkIds = [
      `${runId}_preterit_teaching_ar`,
      `${runId}_preterit_teaching_er_ir`,
    ];
    const answerKeyChunkIds = Array.from(
      { length: 85 },
      (_, index) => `${runId}_preterit_answer_key_${index}`,
    );
    const answerKeyChunkId = answerKeyChunkIds[0];
    await prisma.materialChunk.createMany({
      data: [
        {
          id: `${runId}_part_two_lead_in`,
          userId,
          materialRevisionId: revision.id,
          materialSectionId: broadPart.id,
          ordinal: 0,
          text: "The previous lesson concludes with a reading comprehension exercise.",
          tokenEstimate: 10,
          contentHash: `sha256:${runId}:part-two-lead-in`,
          headingText: broadPart.title,
          locator: { kind: "pdf", pageRange: { start: 257, end: 261 } },
        },
        {
          id: teachingChunkIds[0],
          userId,
          materialRevisionId: revision.id,
          materialSectionId: broadPart.id,
          ordinal: 1,
          text: "Formation of the preterit. To conjugate regular -ar verbs, drop the ending and add the regular preterit endings.",
          tokenEstimate: 18,
          contentHash: `sha256:${runId}:preterit-ar`,
          headingText: broadPart.title,
          locator: { kind: "pdf", pageRange: { start: 262, end: 264 } },
        },
        {
          id: teachingChunkIds[1],
          userId,
          materialRevisionId: revision.id,
          materialSectionId: broadPart.id,
          ordinal: 2,
          text: "Regular -er and -ir verbs use the same preterit endings. Conjugate each verb by replacing its infinitive ending.",
          tokenEstimate: 19,
          contentHash: `sha256:${runId}:preterit-er-ir`,
          headingText: broadPart.title,
          locator: { kind: "pdf", pageRange: { start: 264, end: 267 } },
        },
        ...answerKeyChunkIds.map((id, index) => ({
          id,
          userId,
          materialRevisionId: revision.id,
          materialSectionId: answerKey.id,
          ordinal: index + 3,
          text: `${"Answer Key regular preterit conjugate ar er ir verb. ".repeat(8)}Entry ${index + 1}.`,
          tokenEstimate: 80,
          contentHash: `sha256:${runId}:preterit-answer-key:${index}`,
          headingText: answerKey.title,
          locator: { kind: "pdf", pageRange: { start: 601, end: 601 } },
        })),
        ...Array.from({ length: 85 }, (_, index) => ({
          id: `${runId}_preterit_incidental_${index}`,
          userId,
          materialRevisionId: revision.id,
          materialSectionId: incidentalSection.id,
          ordinal: index + 88,
          text: `${"preterit ".repeat(40)}reference note ${index}`,
          tokenEstimate: 42,
          contentHash: `sha256:${runId}:preterit-incidental:${index}`,
          headingText: incidentalSection.title,
          locator: { kind: "pdf", pageRange: { start: 50 + index, end: 50 + index } },
        })),
      ],
    });
    const partialEmbedding = Array.from(
      { length: MATERIAL_EMBEDDING_DIMENSIONS },
      (_, index) => (index === 0 ? 1 : 0),
    );
    await storeMaterialChunkEmbedding({
      userId,
      materialRevisionId: revision.id,
      chunkId: `${runId}_preterit_incidental_0`,
      embedding: partialEmbedding,
    });
    await finalizeMaterialRevision({
      userId,
      materialId: material.id,
      materialRevisionId: revision.id,
      contentHash: `sha256:${runId}:malformed-preterit`,
      byteSize: 25_963_871,
      pageCount: 628,
      storageBucket: "test-materials",
      storageKey: `${runId}/malformed-preterit.pdf`,
      processingMetadata: { embeddingStatus: "unavailable" },
    });
    const planScope = vi.fn<MaterialDraftAiSetup["planScope"]>(async () => ({
      resolutionStatus: "resolved" as const,
      resolvedScopeLabel: "Regular preterit conjugations",
      clarification: null,
      warnings: [],
      items: [
        {
          key: "regular-preterit-endings",
          title: "Regular preterit endings",
          objective: "Conjugate regular -ar, -er, and -ir verbs in the preterit.",
          materialSectionIds: [broadPart.id],
          evidenceChunkIds: teachingChunkIds,
        },
      ],
    }));

    const result = await planMaterialSkills({
      userId,
      input: {
        materialId: material.id,
        materialRevisionId: revision.id,
        instruction: "make skills for conjugating -ar, -er, and -ir verbs in the preterit",
        idempotencyKey: `${runId}_malformed_preterit_recovery`,
      },
      now: new Date(),
      aiSetup: createAiSetup({ planScope }),
      embeddingGenerator: async () => [partialEmbedding],
    });

    expect(result.status).toBe("planned");
    const planningInput = planScope.mock.calls[0]?.[0];
    expect(planningInput?.sections.map((section) => section.id)).toEqual([broadPart.id]);
    expect(planningInput?.chunks.map((chunk) => chunk.id)).toEqual(
      expect.arrayContaining(teachingChunkIds),
    );
    expect(planningInput?.chunks.map((chunk) => chunk.id)).not.toContain(answerKeyChunkId);
    expect(
      planningInput?.chunks
        .map((chunk) => chunk.id)
        .filter((id) => answerKeyChunkIds.includes(id)),
    ).toEqual([]);

    const rejectedScopePlanner = vi.fn<MaterialDraftAiSetup["planScope"]>();
    const selectedAnswerKey = await planMaterialSkills({
      userId,
      input: {
        materialId: material.id,
        materialRevisionId: revision.id,
        instruction: "Create a skill from the preterit tense rules in chapter fourteen.",
        idempotencyKey: `${runId}_selected_answer_key_scope`,
        sectionIds: [answerKey.id],
      },
      now: new Date(),
      aiSetup: createAiSetup({ planScope: rejectedScopePlanner }),
      embeddingGenerator: null,
    });

    expect(selectedAnswerKey.status).toBe("needs-scope");
    expect(rejectedScopePlanner).not.toHaveBeenCalled();
  });

  it("does not widen an explicit section scope during back-matter recovery", async () => {
    const { material, revision } = await createMaterialWithInitialRevision({
      userId,
      title: "Selected back-matter fixture",
      kind: StudyMaterialKind.PDF,
    });
    const answerKeySection = await prisma.materialSection.create({
      data: {
        userId,
        materialRevisionId: revision.id,
        ordinal: 0,
        level: 1,
        title: "Chapter 14 The Preterit Tense",
        normalizedTitle: "chapter 14 the preterit tense",
        pageStart: 601,
        pageEnd: 601,
        headingPath: ["Chapter 14 The Preterit Tense"],
      },
    });
    const instructionalSection = await prisma.materialSection.create({
      data: {
        userId,
        materialRevisionId: revision.id,
        ordinal: 1,
        level: 1,
        title: "Regular preterit conjugations",
        normalizedTitle: "regular preterit conjugations",
        pageStart: 255,
        pageEnd: 299,
        headingPath: ["Regular preterit conjugations"],
      },
    });
    const answerKeyChunkIds = Array.from(
      { length: 3 },
      (_, index) => `${runId}_selected_back_matter_${index}`,
    );
    const teachingChunkIds = [
      `${runId}_unselected_preterit_teaching_1`,
      `${runId}_unselected_preterit_teaching_2`,
    ];
    const answerKeyEvidence = {
      id: answerKeyChunkIds[0],
      materialSectionId: answerKeySection.id,
      headingText: answerKeySection.title,
      text: "Answer key for chapter 14: The preterit tense forms completed past actions.",
    };
    const instructionalEvidence = teachingChunkIds.map((id, index) => ({
      id,
      materialSectionId: instructionalSection.id,
      headingText: instructionalSection.title,
      text:
        index === 0
          ? "The preterit tense is used for completed past actions. Drop the infinitive ending and add the regular -ar endings."
          : "The preterit tense uses the same endings for regular -er and -ir verbs. Replace the infinitive ending.",
    }));
    await prisma.materialChunk.createMany({
      data: [
        ...answerKeyChunkIds.map((id, index) => ({
          id,
          userId,
          materialRevisionId: revision.id,
          materialSectionId: answerKeySection.id,
          ordinal: index,
          text: `Answer key: Chapter 14 The Preterit Tense. Entries and exercise answers ${index + 1}.`,
          tokenEstimate: 12,
          contentHash: `sha256:${id}`,
          headingText: answerKeySection.title,
          locator: { kind: "pdf", pageRange: { start: 601, end: 601 } },
        })),
        ...instructionalEvidence.map((chunk, index) => ({
          id: chunk.id,
          userId,
          materialRevisionId: revision.id,
          materialSectionId: instructionalSection.id,
          ordinal: index + answerKeyChunkIds.length,
          text: chunk.text,
          tokenEstimate: 18,
          contentHash: `sha256:${chunk.id}`,
          headingText: instructionalSection.title,
          locator: { kind: "pdf", pageRange: { start: 260 + index, end: 260 + index } },
        })),
      ],
    });
    await finalizeMaterialRevision({
      userId,
      materialId: material.id,
      materialRevisionId: revision.id,
      contentHash: `sha256:${runId}:selected-back-matter`,
      byteSize: 16_384,
      pageCount: 628,
      storageBucket: "test-materials",
      storageKey: `${runId}/selected-back-matter.pdf`,
    });

    const recoveredScope = await recoverBackMatterMaterialScope({
      sections: [answerKeySection, instructionalSection],
      sectionIds: [answerKeySection.id],
      chunks: [answerKeyEvidence],
      retrieveRevisionChunks: async () => instructionalEvidence,
      retrieveSectionChunks: async () => instructionalEvidence,
    });
    expect(recoveredScope.status).toBe("recovered");
    if (recoveredScope.status !== "recovered") {
      throw new Error("expected the fixture to expose an out-of-scope recovery candidate");
    }
    expect(recoveredScope.sectionIds).toEqual([instructionalSection.id]);

    const planScope = vi.fn<MaterialDraftAiSetup["planScope"]>(async () => ({
      resolutionStatus: "resolved",
      resolvedScopeLabel: "Regular preterit conjugations",
      clarification: null,
      warnings: [],
      items: [
        {
          key: "regular-preterit-endings",
          title: "Regular preterit endings",
          objective: "Conjugate regular -ar, -er, and -ir verbs in the preterit.",
          materialSectionIds: [instructionalSection.id],
          evidenceChunkIds: teachingChunkIds,
        },
      ],
    }));
    const result = await planMaterialSkills({
      userId,
      input: {
        materialId: material.id,
        materialRevisionId: revision.id,
        instruction: "Make one skill from chapter fourteen's preterit tense rules.",
        idempotencyKey: `${runId}_bounded_back_matter_scope`,
        sectionIds: [answerKeySection.id],
      },
      now: new Date(),
      aiSetup: createAiSetup({ planScope }),
      embeddingGenerator: null,
    });

    expect(result.status).toBe("needs-scope");
    expect(planScope).not.toHaveBeenCalled();
  });

  it("recovers comparison terms split across a section despite a semantic decoy", async () => {
    const { material, revision } = await createMaterialWithInitialRevision({
      userId,
      title: "Split comparison fixture",
      kind: StudyMaterialKind.PDF,
    });
    const comparisonSection = await prisma.materialSection.create({
      data: {
        userId,
        materialRevisionId: revision.id,
        ordinal: 0,
        level: 1,
        title: "Copular verbs",
        normalizedTitle: "copular verbs",
        pageStart: 30,
        pageEnd: 34,
        headingPath: ["Copular verbs"],
      },
    });
    const decoySection = await prisma.materialSection.create({
      data: {
        userId,
        materialRevisionId: revision.id,
        ordinal: 1,
        level: 1,
        title: "Verb reference",
        normalizedTitle: "verb reference",
        pageStart: 100,
        pageEnd: 190,
        headingPath: ["Verb reference"],
      },
    });
    const teachingChunkIds = [
      `${runId}_split_ser_teaching`,
      `${runId}_split_estar_teaching`,
    ];
    const decoyChunkIds = Array.from(
      { length: 85 },
      (_, index) => `${runId}_split_comparison_decoy_${index}`,
    );
    await prisma.materialChunk.createMany({
      data: [
        {
          id: teachingChunkIds[0],
          userId,
          materialRevisionId: revision.id,
          materialSectionId: comparisonSection.id,
          ordinal: 0,
          text: "Ser identifies an essential characteristic or identity.",
          tokenEstimate: 8,
          contentHash: `sha256:${runId}:split-ser`,
          headingText: comparisonSection.title,
          locator: { kind: "pdf", pageRange: { start: 30, end: 31 } },
        },
        {
          id: teachingChunkIds[1],
          userId,
          materialRevisionId: revision.id,
          materialSectionId: comparisonSection.id,
          ordinal: 1,
          text: "Estar describes a condition or location.",
          tokenEstimate: 7,
          contentHash: `sha256:${runId}:split-estar`,
          headingText: comparisonSection.title,
          locator: { kind: "pdf", pageRange: { start: 32, end: 34 } },
        },
        ...decoyChunkIds.map((id, index) => ({
          id,
          userId,
          materialRevisionId: revision.id,
          materialSectionId: decoySection.id,
          ordinal: index + 2,
          text: `Ser reference list entry ${index + 1}.`,
          tokenEstimate: 6,
          contentHash: `sha256:${runId}:split-decoy:${index}`,
          headingText: decoySection.title,
          locator: {
            kind: "pdf" as const,
            pageRange: { start: 100 + index, end: 100 + index },
          },
        })),
      ],
    });
    const semanticEmbedding = Array.from(
      { length: MATERIAL_EMBEDDING_DIMENSIONS },
      (_, index) => (index === 0 ? 1 : 0),
    );
    await storeMaterialChunkEmbedding({
      userId,
      materialRevisionId: revision.id,
      chunkId: decoyChunkIds[0],
      embedding: semanticEmbedding,
    });
    await finalizeMaterialRevision({
      userId,
      materialId: material.id,
      materialRevisionId: revision.id,
      contentHash: `sha256:${runId}:split-comparison`,
      byteSize: 32_768,
      pageCount: 190,
      storageBucket: "test-materials",
      storageKey: `${runId}/split-comparison.pdf`,
      processingMetadata: { embeddingStatus: "ready" },
    });
    const planScope = vi.fn<MaterialDraftAiSetup["planScope"]>(async () => ({
      resolutionStatus: "resolved" as const,
      resolvedScopeLabel: "Ser and estar",
      clarification: null,
      warnings: [],
      items: [
        {
          key: "ser-estar-comparison",
          title: "Ser and estar",
          objective: "Choose between ser and estar for identity, condition, and location.",
          materialSectionIds: [comparisonSection.id],
          evidenceChunkIds: teachingChunkIds,
        },
      ],
    }));

    const result = await planMaterialSkills({
      userId,
      input: {
        materialId: material.id,
        materialRevisionId: revision.id,
        instruction: "make skills for ser and estar",
        idempotencyKey: `${runId}_split_comparison_recovery`,
      },
      now: new Date(),
      aiSetup: createAiSetup({ planScope }),
      embeddingGenerator: async () => [semanticEmbedding],
    });

    expect(result.status).toBe("planned");
    const planningInput = planScope.mock.calls[0]?.[0];
    expect(planningInput?.sections.map((section) => section.id)).toEqual([
      comparisonSection.id,
    ]);
    expect(planningInput?.chunks.map((chunk) => chunk.id)).toEqual(
      expect.arrayContaining(teachingChunkIds),
    );
    expect(planningInput?.chunks.map((chunk) => chunk.id)).not.toContain(
      decoyChunkIds[0],
    );
  });

  it("uses a complete Muse scan when Gemini embeddings fail and words do not match", async () => {
    const rankedIds: string[] = [];
    const rankChunks = vi.fn<NonNullable<MaterialDraftAiSetup["rankChunks"]>>(
      async ({ chunks }) => {
        rankedIds.push(...chunks.map((chunk) => chunk.id));
        return {
          scores: chunks.map((chunk) => ({
            id: chunk.id,
            relevance: chunk.id === indirectChunkId ? 3 : 0,
          })),
        };
      },
    );
    const planScope = vi.fn<MaterialDraftAiSetup["planScope"]>(async () => ({
      resolutionStatus: "resolved",
      resolvedScopeLabel: "Dative clitics",
      clarification: null,
      clarificationOptions: [],
      warnings: [],
      items: [{
        key: "dative-clitics",
        title: "Choosing le and les",
        objective: "Choose the correct pronoun for one or several recipients.",
        includeConcepts: ["recipient number"],
        excludeConcepts: ["direct object pronouns"],
        materialSectionIds: [indirectSectionId],
        evidenceChunkIds: [indirectChunkId],
      }],
    }));

    const result = await planMaterialSkills({
      userId,
      input: {
        materialId,
        materialRevisionId,
        instruction: "make skills for dative clitics",
        idempotencyKey: `${runId}_muse_semantic_retrieval`,
      },
      now: new Date(),
      aiSetup: createAiSetup({ planScope, rankChunks }),
      embeddingGenerator: async () => {
        throw new Error("Gemini embeddings unavailable");
      },
    });

    expect(result.status).toBe("planned");
    expect(rankChunks).toHaveBeenCalled();
    expect(rankedIds).toContain(indirectChunkId);
    expect(new Set(rankedIds).size).toBe(81);
    expect(planScope.mock.calls[0]?.[0].chunks.map((chunk) => chunk.id)).toContain(indirectChunkId);
    expect(planScope.mock.calls[0]?.[0].sections.map((section) => section.id)).toEqual([
      indirectSectionId,
    ]);
  });

  it("scans unembedded passages even when another chunk has a vector match", async () => {
    const { material, revision } = await createMaterialWithInitialRevision({
      userId,
      title: "Partially embedded lesson",
      kind: StudyMaterialKind.PDF,
    });
    const section = await prisma.materialSection.create({
      data: {
        userId,
        materialRevisionId: revision.id,
        ordinal: 0,
        level: 1,
        title: "Lesson 1",
        normalizedTitle: "lesson 1",
        pageStart: 1,
        pageEnd: 2,
        headingPath: ["Lesson 1"],
      },
    });
    const stored = await Promise.all([
      prisma.materialChunk.create({
        data: {
          userId,
          materialRevisionId: revision.id,
          materialSectionId: section.id,
          ordinal: 0,
          text: "Noun plurals add s after a vowel.",
          tokenEstimate: 10,
          contentHash: `sha256:${runId}:partial-embedding-0`,
          headingText: section.title,
          locator: { kind: "pdf", pageRange: { start: 1, end: 1 } },
        },
      }),
      prisma.materialChunk.create({
        data: {
          userId,
          materialRevisionId: revision.id,
          materialSectionId: section.id,
          ordinal: 1,
          text: "La casa blanca.",
          tokenEstimate: 8,
          contentHash: `sha256:${runId}:partial-embedding-1`,
          headingText: section.title,
          locator: { kind: "pdf", pageRange: { start: 2, end: 2 } },
        },
      }),
    ]);
    const vector = Array.from({ length: MATERIAL_EMBEDDING_DIMENSIONS },
      (_, index) => index === 0 ? 1 : 0);
    await storeMaterialChunkEmbedding({
      userId,
      materialRevisionId: revision.id,
      chunkId: stored[0].id,
      embedding: vector,
    });
    await finalizeMaterialRevision({
      userId,
      materialId: material.id,
      materialRevisionId: revision.id,
      contentHash: `sha256:${runId}:partial-embedding`,
      byteSize: 4_096,
      pageCount: 2,
      storageBucket: "test-materials",
      storageKey: `${runId}/partial-embedding.pdf`,
    });
    const rankChunks = vi.fn<NonNullable<MaterialDraftAiSetup["rankChunks"]>>(
      async ({ chunks }) => ({
        scores: chunks.map((chunk) => ({
          id: chunk.id,
          relevance: chunk.id === stored[1].id ? 3 : 0,
        })),
      }),
    );
    const planScope = vi.fn<MaterialDraftAiSetup["planScope"]>(async () => ({
      resolutionStatus: "resolved",
      resolvedScopeLabel: "Gender concord",
      clarification: null,
      clarificationOptions: [],
      warnings: [],
      items: [{
        key: "gender-concord",
        title: "Matching adjective gender",
        objective: "Select an adjective form that matches a feminine noun.",
        includeConcepts: ["feminine adjective agreement"],
        excludeConcepts: ["noun plurals"],
        materialSectionIds: [section.id],
        evidenceChunkIds: [stored[1].id],
      }],
    }));

    const result = await planMaterialSkills({
      userId,
      input: {
        materialId: material.id,
        materialRevisionId: revision.id,
        instruction: "make skills for gender concord",
        idempotencyKey: `${runId}_partial_embedding_muse_scan`,
      },
      now: new Date(),
      aiSetup: createAiSetup({ planScope, rankChunks }),
      embeddingGenerator: async () => [vector],
    });

    expect(result.status).toBe("planned");
    expect(rankChunks).toHaveBeenCalled();
    expect(planScope.mock.calls[0]?.[0].chunks.map((chunk) => chunk.id))
      .toContain(stored[1].id);
  });

  it("keeps lexical evidence and warns when Muse cannot complete its scan", async () => {
    const planScope = vi.fn<MaterialDraftAiSetup["planScope"]>(async () => ({
      resolutionStatus: "resolved",
      resolvedScopeLabel: "Direct object pronouns",
      clarification: null,
      clarificationOptions: [],
      warnings: Array.from({ length: 20 }, (_, index) => `Fixture warning ${index + 1}`),
      items: [{
        key: "direct-object-pronouns",
        title: "Placing direct object pronouns",
        objective: "Place a direct object pronoun before a conjugated verb.",
        includeConcepts: ["pronoun placement"],
        excludeConcepts: ["indirect object pronouns"],
        materialSectionIds: [directSectionId],
        evidenceChunkIds: [directChunkId],
      }],
    }));
    const rankChunks = vi.fn<NonNullable<MaterialDraftAiSetup["rankChunks"]>>(
      async () => { throw new Error("Meta rate limit"); },
    );

    const result = await planMaterialSkills({
      userId,
      input: {
        materialId,
        materialRevisionId,
        instruction: "make skills for direct object pronouns",
        idempotencyKey: `${runId}_muse_lexical_degradation`,
      },
      now: new Date(),
      aiSetup: createAiSetup({ planScope, rankChunks }),
      embeddingGenerator: async () => { throw new Error("Gemini embeddings unavailable"); },
    });

    expect(result.status).toBe("planned");
    if (result.status !== "planned") return;
    expect(rankChunks).toHaveBeenCalled();
    expect(result.plan.warnings).toEqual(expect.arrayContaining([
      expect.stringContaining("Muse could not scan all source text"),
    ]));
    expect(result.plan.warnings).toHaveLength(20);
    expect(planScope.mock.calls[0]?.[0].chunks.map((chunk) => chunk.id)).toContain(directChunkId);
  });

  it("reserves time for lexical retrieval after a Muse scan timeout", async () => {
    const budget = vi.spyOn(museRetrieval, "getMuseScanBudgetMs").mockReturnValue(10);
    let abortReason: unknown;
    const rankChunks = vi.fn<NonNullable<MaterialDraftAiSetup["rankChunks"]>>(
      async ({ signal }) => new Promise((_, reject) => {
        const watchdog = setTimeout(() => reject(new Error("Muse child did not time out")), 500);
        signal?.addEventListener("abort", () => {
          clearTimeout(watchdog);
          abortReason = signal.reason;
          reject(signal.reason);
        }, { once: true });
      }),
    );
    const planScope = vi.fn<MaterialDraftAiSetup["planScope"]>(async () => ({
      resolutionStatus: "resolved",
      resolvedScopeLabel: "Direct object pronouns",
      clarification: null,
      clarificationOptions: [],
      warnings: [],
      items: [{
        key: "direct-object-pronoun-timeout",
        title: "Placing direct object pronouns",
        objective: "Place a direct object pronoun before a conjugated verb.",
        includeConcepts: ["pronoun placement"],
        excludeConcepts: ["indirect object pronouns"],
        materialSectionIds: [directSectionId],
        evidenceChunkIds: [directChunkId],
      }],
    }));
    try {
      const result = await planMaterialSkills({
        userId,
        input: {
          materialId,
          materialRevisionId,
          instruction: "make skills for direct object pronouns",
          idempotencyKey: `${runId}_muse_scan_timeout`,
        },
        now: new Date(),
        aiSetup: createAiSetup({ planScope, rankChunks }),
        embeddingGenerator: null,
      });
      expect(result.status).toBe("planned");
      if (result.status !== "planned") return;
      expect(rankChunks).toHaveBeenCalled();
      expect((abortReason as Error).name).toBe("TimeoutError");
      expect(result.plan.warnings).toEqual(expect.arrayContaining([
        expect.stringContaining("Muse could not scan all source text"),
      ]));
      expect(planScope.mock.calls[0]?.[0].chunks.map((chunk) => chunk.id))
        .toContain(directChunkId);
    } finally {
      budget.mockRestore();
    }
  });

  it("includes later OCR-only pages despite complete chunk embedding coverage", async () => {
    const { material, revision } = await createMaterialWithInitialRevision({
      userId,
      title: "OCR-only lesson fixture",
      kind: StudyMaterialKind.PDF,
    });
    const section = await prisma.materialSection.create({
      data: {
        userId,
        materialRevisionId: revision.id,
        ordinal: 0,
        level: 1,
        title: "Lesson 1",
        normalizedTitle: "lesson 1",
        pageStart: 20,
        pageEnd: 30,
        headingPath: ["Lesson 1"],
      },
    });
    const storedChunk = await prisma.materialChunk.create({
      data: {
        userId,
        materialRevisionId: revision.id,
        materialSectionId: section.id,
        ordinal: 0,
        text: "Noun plurals add s after a vowel.",
        tokenEstimate: 10,
        contentHash: `sha256:${runId}:ocr-only-stored`,
        headingText: section.title,
        locator: { kind: "pdf", pageRange: { start: 20, end: 20 } },
      },
    });
    const vector = Array.from({ length: MATERIAL_EMBEDDING_DIMENSIONS },
      (_, index) => index === 0 ? 1 : 0);
    await storeMaterialChunkEmbedding({
      userId,
      materialRevisionId: revision.id,
      chunkId: storedChunk.id,
      embedding: vector,
    });
    const targetPageId = `${runId}_ocr_only_target`;
    await prisma.materialPage.createMany({
      data: Array.from({ length: 11 }, (_, index) => ({
        id: index === 10 ? targetPageId : `${runId}_ocr_only_${index}`,
        userId,
        materialRevisionId: revision.id,
        pageNumber: index + 20,
        ocrText: index === 10
          ? "Yo me levanto cada mañana."
          : `Unrelated passage ${index + 1}.`,
        textStatus: MaterialPageTextStatus.OCR_READY,
        contentHash: `sha256:${runId}:ocr-only-page-${index}`,
        tokenEstimate: 10,
      })),
    });
    await finalizeMaterialRevision({
      userId,
      materialId: material.id,
      materialRevisionId: revision.id,
      contentHash: `sha256:${runId}:ocr-only-material`,
      byteSize: 16_384,
      pageCount: 30,
      storageBucket: "test-materials",
      storageKey: `${runId}/ocr-only.pdf`,
    });
    const targetEvidenceId = materialPageEvidenceId(targetPageId);
    const rankChunks = vi.fn<NonNullable<MaterialDraftAiSetup["rankChunks"]>>(
      async ({ chunks }) => ({
        scores: chunks.map((chunk) => ({
          id: chunk.id,
          relevance: chunk.id === targetEvidenceId ? 3 : 0,
        })),
      }),
    );
    const planScope = vi.fn<MaterialDraftAiSetup["planScope"]>(async () => ({
      resolutionStatus: "resolved",
      resolvedScopeLabel: "Personal routines",
      clarification: null,
      clarificationOptions: [],
      warnings: [],
      items: [{
        key: "personal-routines",
        title: "Describing personal routines",
        objective: "Choose the matching first-person routine expression.",
        includeConcepts: ["first-person routine expression"],
        excludeConcepts: ["noun plurals"],
        materialSectionIds: [section.id],
        evidenceChunkIds: [targetEvidenceId],
      }],
    }));

    const result = await planMaterialSkills({
      userId,
      input: {
        materialId: material.id,
        materialRevisionId: revision.id,
        instruction: "make skills for personal routines",
        idempotencyKey: `${runId}_muse_ocr_only`,
      },
      now: new Date(),
      aiSetup: createAiSetup({ planScope, rankChunks }),
      embeddingGenerator: async () => [vector],
    });

    expect(result.status).toBe("planned");
    expect(rankChunks.mock.calls.flatMap(([call]) => call.chunks.map((chunk) => chunk.id)))
      .toContain(targetEvidenceId);
    expect(planScope.mock.calls[0]?.[0].chunks.map((chunk) => chunk.id))
      .toContain(targetEvidenceId);
  });

  it("reserves fallback evidence for later sections before truncating ranked chunks", async () => {
    const { material, revision } = await createMaterialWithInitialRevision({
      userId,
      title: "Balanced retrieval fixture",
      kind: StudyMaterialKind.PDF,
    });
    const sectionIds = Array.from(
      { length: 20 },
      (_, index) => `${runId}_balanced_section_${String(index).padStart(2, "0")}`,
    );
    await prisma.materialSection.createMany({
      data: sectionIds.map((id, index) => ({
        id,
        userId,
        materialRevisionId: revision.id,
        ordinal: index,
        level: 1,
        title: `Balanced topic ${index + 1}`,
        normalizedTitle: `balanced topic ${index + 1}`,
        pageStart: index + 1,
        pageEnd: index + 1,
        headingPath: [`Balanced topic ${index + 1}`],
      })),
    });
    const firstSectionChunkIds = Array.from(
      { length: 48 },
      (_, index) => `${runId}_ranked_chunk_${String(index).padStart(2, "0")}`,
    );
    const fallbackChunkIds = sectionIds.slice(1).map(
      (_, index) => `${runId}_fallback_chunk_${String(index).padStart(2, "0")}`,
    );
    await prisma.materialChunk.createMany({
      data: [
        ...firstSectionChunkIds.map((id, index) => ({
          id,
          userId,
          materialRevisionId: revision.id,
          materialSectionId: sectionIds[0],
          ordinal: index,
          text: `Ranked grammar request evidence ${index + 1}.`,
          tokenEstimate: 8,
          contentHash: `sha256:${id}`,
          headingText: "Ranked grammar request",
          locator: { kind: "pdf", pageRange: { start: 1, end: 1 } },
        })),
        ...fallbackChunkIds.map((id, index) => ({
          id,
          userId,
          materialRevisionId: revision.id,
          materialSectionId: sectionIds[index + 1],
          ordinal: firstSectionChunkIds.length + index,
          text: `Fallback evidence for balanced topic ${index + 2}.`,
          tokenEstimate: 8,
          contentHash: `sha256:${id}`,
          headingText: `Balanced topic ${index + 2}`,
          locator: { kind: "pdf", pageRange: { start: index + 2, end: index + 2 } },
        })),
      ],
    });
    await finalizeMaterialRevision({
      userId,
      materialId: material.id,
      materialRevisionId: revision.id,
      contentHash: `sha256:${runId}:balanced-retrieval`,
      byteSize: 16_384,
      pageCount: 20,
      storageBucket: "test-materials",
      storageKey: `${runId}/balanced-retrieval.pdf`,
    });
    const lastChunkId = fallbackChunkIds.at(-1);
    if (!lastChunkId) {
      throw new Error("expected a later fallback chunk");
    }
    const planScope = vi.fn<MaterialDraftAiSetup["planScope"]>(async () => ({
      resolutionStatus: "resolved" as const,
      resolvedScopeLabel: "Balanced topic 20",
      clarification: null,
      warnings: [],
      items: [
        {
          key: "later-balanced-topic",
          title: "Later balanced topic",
          objective: "Recognize the evidence from the final balanced topic.",
          materialSectionIds: [sectionIds.at(-1)!],
          evidenceChunkIds: [lastChunkId],
        },
      ],
    }));

    const result = await planMaterialSkills({
      userId,
      input: {
        materialId: material.id,
        materialRevisionId: revision.id,
        instruction: "ranked grammar request",
        idempotencyKey: `${runId}_reserved_fallbacks`,
      },
      now: new Date(),
      aiSetup: createAiSetup({ planScope }),
      embeddingGenerator: null,
    });

    expect(result.status).toBe("planned");
    expect(planScope.mock.calls[0]?.[0].chunks.map((chunk) => chunk.id)).toContain(lastChunkId);
  });

  it("keeps a later semantic hit when broad-scope fallbacks fill the planning cap", async () => {
    const { material, revision } = await createMaterialWithInitialRevision({
      userId,
      title: "Large retrieval fixture",
      kind: StudyMaterialKind.PDF,
    });
    const sectionIds = Array.from(
      { length: 61 },
      (_, index) => `${runId}_retrieval_section_${String(index).padStart(2, "0")}`,
    );
    const chunkIds = Array.from(
      { length: 61 },
      (_, index) => `${runId}_retrieval_chunk_${String(index).padStart(2, "0")}`,
    );
    await prisma.materialSection.createMany({
      data: sectionIds.map((id, index) => ({
        id,
        userId,
        materialRevisionId: revision.id,
        ordinal: index,
        level: 1,
        title: `Topic ${index + 1}`,
        normalizedTitle: `topic ${index + 1}`,
        pageStart: index + 1,
        pageEnd: index + 1,
        headingPath: [`Topic ${index + 1}`],
      })),
    });
    await prisma.materialChunk.createMany({
      data: chunkIds.map((id, index) => ({
        id,
        userId,
        materialRevisionId: revision.id,
        materialSectionId: sectionIds[index],
        ordinal: index,
        text:
          index === 60
            ? "The zygomatic conjugation sentinel is the uniquely requested concept."
            : `General grammar overview for topic ${index + 1}.`,
        tokenEstimate: 10,
        contentHash: `sha256:${runId}:retrieval-${index}`,
        headingText: `Topic ${index + 1}`,
        locator: { kind: "pdf", pageRange: { start: index + 1, end: index + 1 } },
      })),
    });
    await finalizeMaterialRevision({
      userId,
      materialId: material.id,
      materialRevisionId: revision.id,
      contentHash: `sha256:${runId}:large-retrieval`,
      byteSize: 16_384,
      pageCount: 61,
      storageBucket: "test-materials",
      storageKey: `${runId}/large-retrieval.pdf`,
    });
    const planScope = vi.fn<MaterialDraftAiSetup["planScope"]>(async () => ({
      resolutionStatus: "resolved" as const,
      resolvedScopeLabel: "Topic 61",
      clarification: null,
      warnings: [],
      items: [
        {
          key: "later-semantic-hit",
          title: "Zygomatic conjugation sentinel",
          objective: "Recognize the uniquely requested conjugation sentinel concept.",
          materialSectionIds: [sectionIds[60]],
          evidenceChunkIds: [chunkIds[60]],
        },
      ],
    }));

    const result = await planMaterialSkills({
      userId,
      input: {
        materialId: material.id,
        materialRevisionId: revision.id,
        instruction: "zygomatic conjugation sentinel",
        idempotencyKey: `${runId}_later_semantic_hit`,
      },
      now: new Date(),
      aiSetup: createAiSetup({ planScope }),
      embeddingGenerator: null,
    });

    expect(result.status).toBe("planned");
    expect(planScope.mock.calls[0]?.[0].chunks.map((chunk) => chunk.id)).toContain(chunkIds[60]);
  });

  it("keeps a verified draft when a sibling exhausts its bounded regeneration", async () => {
    const planScope = vi.fn<MaterialDraftAiSetup["planScope"]>(async () => ({
      resolutionStatus: "resolved",
      resolvedScopeLabel: "Chapter 4, pages 90–128",
      clarification: null,
      warnings: [],
      items: [
        {
          key: "pronoun-placement",
          title: "Object pronoun placement",
          objective: "Place an object pronoun correctly before a conjugated Spanish verb.",
          materialSectionIds: [directSectionId],
          evidenceChunkIds: [directChunkId],
        },
        {
          key: "le-versus-les",
          title: "Choosing le versus les",
          objective: "Choose le or les from the number of recipients in a Spanish sentence.",
          materialSectionIds: [indirectSectionId],
          evidenceChunkIds: [indirectChunkId],
        },
      ],
    }));
    const planned = await planMaterialSkills({
      userId,
      input: {
        materialId,
        materialRevisionId,
        instruction: "Make two more skills from chapter four.",
        idempotencyKey: `${runId}_partial_plan`,
      },
      now: new Date(),
      aiSetup: createAiSetup({ planScope }),
      embeddingGenerator: null,
    });
    if (planned.status !== "planned") {
      throw new Error("expected planned batch");
    }
    const events: string[] = [];
    await confirmMaterialPlan({
      userId,
      input: { batchId: planned.batchId, plan: planned.plan },
      now: new Date(),
      eventSender: {
        async sendMaterialDraftItemRequested(payload) {
          events.push(payload.itemId);
        },
      },
    });
    const batch = await getMaterialDraftBatch({ userId, batchId: planned.batchId });
    expect(events).toHaveLength(2);
    const firstItem = batch?.items[0];
    const secondItem = batch?.items[1];
    if (!firstItem || !secondItem) {
      throw new Error("expected two planned items");
    }

    // Stay strictly beyond the ten-minute lease, even within one clock tick.
    const staleUpdatedAt = new Date(Date.now() - 11 * 60 * 1_000);
    await prisma.skillDraftBatchItem.update({
      where: { id: firstItem.id },
      data: { status: SkillDraftBatchItemStatus.GENERATING, updatedAt: staleUpdatedAt },
    });

    expect(
      await runMaterialDraftItemJob({
        userId,
        batchId: planned.batchId,
        itemId: firstItem.id,
        aiSetup: createAiSetup(),
        sourceEvidenceLoader,
      }),
    ).toMatchObject({ status: "ready" });

    await prisma.skillDraftBatchItem.update({
      where: { id: secondItem.id },
      data: { status: SkillDraftBatchItemStatus.GENERATING, updatedAt: staleUpdatedAt },
    });
    const recovered = await getMaterialDraftBatch({ userId, batchId: planned.batchId });
    expect(recovered?.items[1]).toMatchObject({
      status: SkillDraftBatchItemStatus.FAILED,
      errorCode: "STALE_GENERATION_CLAIM",
      errorMessage: expect.stringMatching(/retry/i),
    });
    expect(
      await runMaterialDraftItemJob({
        userId,
        batchId: planned.batchId,
        itemId: secondItem.id,
        aiSetup: createAiSetup(),
        sourceEvidenceLoader,
      }),
    ).toMatchObject({ status: "not-claimed" });
    await retryMaterialDraftItem({
      userId,
      batchId: planned.batchId,
      itemId: secondItem.id,
      now: new Date(),
      eventSender: { async sendMaterialDraftItemRequested() {} },
    });
    expect(
      await runMaterialDraftItemJob({
        userId,
        batchId: planned.batchId,
        itemId: secondItem.id,
        aiSetup: createAiSetup({ rejectTitle: "Choosing le versus les" }),
        sourceEvidenceLoader,
      }),
    ).toMatchObject({ status: "failed", reason: "verification-rejected" });

    const completed = await getMaterialDraftBatch({ userId, batchId: planned.batchId });
    expect(completed).toMatchObject({
      status: SkillDraftBatchStatus.PARTIAL,
      readyCount: 1,
      failedCount: 1,
    });
    expect(completed?.items[0]).toMatchObject({
      status: SkillDraftBatchItemStatus.READY,
      skill: { status: SkillStatus.DRAFT },
    });
    expect(completed?.items[1]).toMatchObject({
      status: SkillDraftBatchItemStatus.FAILED,
      generationAttempts: 2,
    });
    expect(
      await retryMaterialDraftItem({
        userId,
        batchId: planned.batchId,
        itemId: secondItem.id,
        now: new Date(),
        automatic: true,
        eventSender: { async sendMaterialDraftItemRequested() {} },
      }),
    ).toMatchObject({ status: "not-found" });

    const readySkill = completed?.items[0].skill;
    if (!readySkill) {
      throw new Error("expected a ready material draft");
    }
    let activationSourceContext: string | null = null;
    const activated = await activateSkillDraft({
      userId,
      skillId: readySkill.id,
      now: new Date(),
      model: "fixture-model",
      generateChoiceExercises: async (generationInput) => {
        activationSourceContext = generationInput.sourceContext;
        return {
          exercises: [1, 2, 3].map((number) => ({
            prompt: `Choose the direct object pronoun in example ${number}.`,
            choices: [
              { id: "correct", label: "lo" },
              { id: "wrong-a", label: "le" },
              { id: "wrong-b", label: "les" },
            ],
            correctChoiceId: "correct",
            explanation: "The noun receives the action directly.",
            difficulty: 2,
            expectedSeconds: 30,
          })),
        };
      },
      verifyChoiceExercises: async (verificationInput) => ({
        verifications: verificationInput.candidates.map((candidate) => ({
          candidateId: candidate.candidateId,
          verdict: "verified",
        })),
      }),
      sourceEvidenceLoader,
    });
    expect(activated.status).toBe("activated");
    expect(activationSourceContext).toContain("Direct object pronouns replace nouns");
    expect(activationSourceContext).not.toContain("UNRELATED CHAPTER SIX SOURCE TEXT");
    expect(activationSourceContext).not.toContain("Indirect object pronouns identify");
    expect(await getMaterialDraftBatch({ userId, batchId: planned.batchId })).toMatchObject({
      status: SkillDraftBatchStatus.PARTIAL,
      readyCount: 0,
      activatedCount: 1,
      items: [
        { status: SkillDraftBatchItemStatus.ACTIVE },
        { status: SkillDraftBatchItemStatus.FAILED },
      ],
    });

    let refillSourceContext: string | null = null;
    const refilled = await refillChoiceExercisesForSkill({
      userId,
      skillId: readySkill.id,
      now: new Date(),
      model: "fixture-model",
      targetReadyCount: 5,
      generateChoiceExercises: async (generationInput) => {
        refillSourceContext = generationInput.sourceContext;
        return {
          exercises: [4, 5].map((number) => ({
            prompt: `Choose the direct object pronoun in refill example ${number}.`,
            choices: [
              { id: "correct", label: "lo" },
              { id: "wrong-a", label: "le" },
              { id: "wrong-b", label: "les" },
            ],
            correctChoiceId: "correct",
            explanation: "The noun receives the action directly.",
            difficulty: 2,
            expectedSeconds: 30,
          })),
        };
      },
      verifyChoiceExercises: async (verificationInput) => ({
        verifications: verificationInput.candidates.map((candidate) => ({
          candidateId: candidate.candidateId,
          verdict: "verified",
        })),
      }),
      sourceEvidenceLoader,
    });
    expect(refilled.status).toBe("refilled");
    expect(refillSourceContext).toContain("Direct object pronouns replace nouns");
    expect(refillSourceContext).not.toContain("UNRELATED CHAPTER SIX SOURCE TEXT");
    expect(refillSourceContext).not.toContain("Indirect object pronouns identify");

    await expect(
      excludeMaterialDraftItem({
        userId,
        batchId: planned.batchId,
        itemId: firstItem.id,
        now: new Date(),
      }),
    ).resolves.toMatchObject({ status: "not-found" });
    expect(
      await prisma.skill.findUniqueOrThrow({
        where: { id: readySkill.id },
        select: { status: true },
      }),
    ).toEqual({ status: SkillStatus.ACTIVE });
  });

  it("repairs a rejected target against its evidence automatically", async () => {
    const planned = await planMaterialSkills({
      userId,
      input: {
        materialId,
        materialRevisionId,
        instruction: "Make one skill for choosing le versus les.",
        idempotencyKey: `${runId}_target_repair`,
      },
      now: new Date(),
      aiSetup: createAiSetup({
        planScope: async () => ({
          resolutionStatus: "resolved",
          resolvedScopeLabel: "Indirect object pronouns",
          clarification: null,
          warnings: [],
          items: [
            {
              key: "le-versus-les-repair",
              title: "Choosing le versus les",
              objective: "Choose le or les and apply an unsupported pluralization rule.",
              includeConcepts: ["choose le or les from the number of recipients"],
              excludeConcepts: ["direct object pronouns"],
              materialSectionIds: [indirectSectionId],
              evidenceChunkIds: [indirectChunkId],
            },
          ],
        }),
      }),
      embeddingGenerator: null,
    });
    if (planned.status !== "planned") {
      throw new Error("expected target repair plan");
    }
    await confirmMaterialPlan({
      userId,
      input: { batchId: planned.batchId, plan: planned.plan },
      now: new Date(),
      eventSender: { async sendMaterialDraftItemRequested() {} },
    });
    const pending = await getMaterialDraftBatch({ userId, batchId: planned.batchId });
    const itemId = pending?.items[0]?.id;
    if (!itemId) {
      throw new Error("expected target repair item");
    }
    const repairTarget = vi.fn().mockResolvedValue({
      status: "repaired",
      title: "Choosing le or les by recipient count",
      objective: "Choose le for one recipient and les for multiple recipients.",
      includeConcepts: ["le for one recipient", "les for multiple recipients"],
      excludeConcepts: ["unsupported pluralization rule"],
      note: "Removed the unsupported rule.",
    });
    expect(
      await runMaterialDraftItemJob({
        userId,
        batchId: planned.batchId,
        itemId,
        aiSetup: createAiSetup({
          rejectTitle: "Choosing le versus les",
          repairTarget,
        }),
        sourceEvidenceLoader,
      }),
    ).toMatchObject({ status: "ready" });
    expect(repairTarget).toHaveBeenCalledWith(
      expect.objectContaining({
        verificationNote: expect.stringMatching(/fixture rejects/i),
      }),
    );
    expect(await getMaterialDraftBatch({ userId, batchId: planned.batchId })).toMatchObject({
      status: SkillDraftBatchStatus.READY,
      items: [
        {
          status: SkillDraftBatchItemStatus.READY,
          proposedTitle: "Choosing le or les by recipient count",
          proposedObjective: "Choose le for one recipient and les for multiple recipients.",
          generationMetadata: {
            targetRepair: { status: "completed" },
          },
        },
      ],
    });
  });

  it("repairs a contradictory repaired target again before showing a failure", async () => {
    const planned = await planMaterialSkills({
      userId,
      input: {
        materialId,
        materialRevisionId,
        instruction: "Make a skill for cardinal numbers above 29.",
        idempotencyKey: `${runId}_second_target_repair`,
      },
      now: new Date(),
      aiSetup: createAiSetup({
        planScope: async () => ({
          resolutionStatus: "resolved",
          resolvedScopeLabel: "Cardinal numbers above 29",
          clarification: null,
          warnings: [],
          items: [
            {
              key: "cardinals-above-29",
              title: "Spanish cardinal numbers above 29",
              objective: "Form Spanish cardinal numbers from 30 up to millions.",
              includeConcepts: ["cardinal numbers from 30 upward"],
              excludeConcepts: ["ordinal numbers"],
              materialSectionIds: [indirectSectionId],
              evidenceChunkIds: [indirectChunkId],
            },
          ],
        }),
      }),
      embeddingGenerator: null,
    });
    if (planned.status !== "planned") {
      throw new Error("expected second target repair plan");
    }
    await confirmMaterialPlan({
      userId,
      input: { batchId: planned.batchId, plan: planned.plan },
      now: new Date(),
      eventSender: { async sendMaterialDraftItemRequested() {} },
    });
    const pending = await getMaterialDraftBatch({ userId, batchId: planned.batchId });
    const itemId = pending?.items[0]?.id;
    if (!itemId) {
      throw new Error("expected second target repair item");
    }
    const repairTarget = vi
      .fn()
      .mockResolvedValueOnce({
        status: "repaired",
        title: "Spanish cardinal numbers from 30 to 100",
        objective: "Form Spanish cardinal numbers from 30 to 100.",
        includeConcepts: ["cardinal numbers from 30 to 100"],
        excludeConcepts: ["cien and ciento patterns"],
        note: "Narrowed the range to the cited pages.",
      })
      .mockResolvedValueOnce({
        status: "repaired",
        title: "Spanish cardinal numbers from 30 to 99",
        objective: "Form Spanish cardinal numbers from 30 to 99.",
        includeConcepts: ["cardinal numbers from 30 to 99"],
        excludeConcepts: ["100 and higher cardinal numbers"],
        note: "Removed the contradictory endpoint.",
      });
    const verifyDraft = vi.fn(async (verificationInput) => {
      if (verificationInput.target.objective.includes("millions")) {
        return {
          verdict: "rejected",
          reasons: ["unsupported_detail"],
          note: "The cited pages do not support millions.",
        };
      }
      if (verificationInput.target.objective.includes("30 to 100")) {
        return {
          verdict: "rejected",
          reasons: ["too_broad"],
          note: "The draft includes 100 even though the target excludes cien and ciento.",
        };
      }
      return { verdict: "verified", reasons: [], note: null };
    });

    expect(
      await runMaterialDraftItemJob({
        userId,
        batchId: planned.batchId,
        itemId,
        aiSetup: createAiSetup({ repairTarget, verifyDraft }),
        sourceEvidenceLoader,
      }),
    ).toMatchObject({ status: "ready" });
    expect(repairTarget).toHaveBeenCalledTimes(2);
    expect(repairTarget.mock.calls[1]?.[0]).toMatchObject({
      target: { objective: "Form Spanish cardinal numbers from 30 to 100." },
      verificationNote: expect.stringMatching(/includes 100/i),
    });
    expect(await getMaterialDraftBatch({ userId, batchId: planned.batchId })).toMatchObject({
      status: SkillDraftBatchStatus.READY,
      items: [
        {
          status: SkillDraftBatchItemStatus.READY,
          proposedTitle: "Spanish cardinal numbers from 30 to 99",
          proposedObjective: "Form Spanish cardinal numbers from 30 to 99.",
          generationMetadata: {
            targetRepair: { status: "completed", attempts: 2 },
          },
        },
      ],
    });
  });

  it("does not let a stale worker overwrite a newer successful claim", async () => {
    const planScope = vi.fn<MaterialDraftAiSetup["planScope"]>(async () => ({
      resolutionStatus: "resolved" as const,
      resolvedScopeLabel: "Chapter 4",
      clarification: null,
      warnings: [],
      items: [
        {
          key: "generation-claim-fence",
          title: "Generation claim fence",
          objective: "Place a direct object pronoun in one claim-fence example.",
          materialSectionIds: [directSectionId],
          evidenceChunkIds: [directChunkId],
        },
      ],
    }));
    const planned = await planMaterialSkills({
      userId,
      input: {
        materialId,
        materialRevisionId,
        instruction: "Make a claim-fence skill from chapter four.",
        idempotencyKey: `${runId}_generation_claim_fence`,
      },
      now: new Date(),
      aiSetup: createAiSetup({ planScope }),
      embeddingGenerator: null,
    });
    if (planned.status !== "planned") {
      throw new Error("expected planned generation claim fence batch");
    }
    await confirmMaterialPlan({
      userId,
      input: { batchId: planned.batchId, plan: planned.plan },
      now: new Date(),
      eventSender: { async sendMaterialDraftItemRequested() {} },
    });
    const item = await prisma.skillDraftBatchItem.findFirstOrThrow({
      where: { batchId: planned.batchId },
    });
    let markSlowWorkerStarted = () => {};
    const slowWorkerStarted = new Promise<void>((resolve) => {
      markSlowWorkerStarted = resolve;
    });
    let releaseSlowWorker = () => {};
    const slowWorkerMayFinish = new Promise<void>((resolve) => {
      releaseSlowWorker = resolve;
    });
    const slowAi = createAiSetup();
    slowAi.generateDraft = async () => {
      markSlowWorkerStarted();
      await slowWorkerMayFinish;
      throw new Error("stale worker failed after its claim was replaced");
    };

    const staleWorker = runMaterialDraftItemJob({
      userId,
      batchId: planned.batchId,
      itemId: item.id,
      now: new Date(),
      aiSetup: slowAi,
      sourceEvidenceLoader,
    });
    await slowWorkerStarted;
    await prisma.skillDraftBatchItem.update({
      where: { id: item.id },
      data: { updatedAt: new Date(Date.now() - 11 * 60 * 1_000) },
    });
    expect(
      await runMaterialDraftItemJob({
        userId,
        batchId: planned.batchId,
        itemId: item.id,
        now: new Date(),
        aiSetup: createAiSetup(),
        sourceEvidenceLoader,
      }),
    ).toMatchObject({ status: "ready" });
    releaseSlowWorker();
    expect(await staleWorker).toMatchObject({ status: "not-claimed" });
    expect(
      await prisma.skillDraftBatchItem.findUnique({
        where: { id: item.id },
        select: { status: true, skill: { select: { status: true } } },
      }),
    ).toEqual({ status: SkillDraftBatchItemStatus.READY, skill: { status: SkillStatus.DRAFT } });
  });

  it("recovers a ready batch item after its linked draft skill is deleted", async () => {
    const skill = await prisma.skill.create({
      data: { userId, title: "Deleted batch draft", tags: [], status: SkillStatus.DRAFT },
    });
    const batch = await prisma.skillDraftBatch.create({
      data: {
        userId,
        materialRevisionId,
        instruction: "Deleted ready draft fixture",
        idempotencyKey: `${runId}_deleted_ready_draft`,
        status: SkillDraftBatchStatus.READY,
        requestedCount: 1,
        readyCount: 1,
      },
    });
    const item = await prisma.skillDraftBatchItem.create({
      data: {
        userId,
        batchId: batch.id,
        skillId: skill.id,
        ordinal: 0,
        targetKey: "deleted-ready-draft",
        proposedTitle: "Deleted batch draft replacement",
        proposedObjective: "Place a direct object pronoun in one short sentence.",
        locator: pdfLocator({ materialRevisionId, directSectionId, directChunkId }),
        status: SkillDraftBatchItemStatus.READY,
      },
    });
    await expect(
      deleteSkillPermanently({
        userId,
        skillId: skill.id,
        confirmationTitle: skill.title,
      }),
    ).resolves.toMatchObject({ status: "deleted" });

    await expect(
      runMaterialDraftItemJob({
        userId,
        batchId: batch.id,
        itemId: item.id,
        aiSetup: createAiSetup(),
        sourceEvidenceLoader,
      }),
    ).resolves.toMatchObject({ status: "failed", reason: "draft-skill-deleted" });
    await expect(
      prisma.skillDraftBatchItem.findUnique({ where: { id: item.id } }),
    ).resolves.toMatchObject({
      status: SkillDraftBatchItemStatus.FAILED,
      skillId: null,
      errorCode: "DRAFT_SKILL_DELETED",
    });

    await retryMaterialDraftItem({
      userId,
      batchId: batch.id,
      itemId: item.id,
      now: new Date(),
      eventSender: { async sendMaterialDraftItemRequested() {} },
    });
    await expect(
      runMaterialDraftItemJob({
        userId,
        batchId: batch.id,
        itemId: item.id,
        aiSetup: createAiSetup(),
        sourceEvidenceLoader,
      }),
    ).resolves.toMatchObject({ status: "ready", alreadyGenerated: false });
  });

  it("keeps retryable generation failures claimable for the same AWS background jobs event", async () => {
    const batch = await prisma.skillDraftBatch.create({
      data: {
        userId,
        materialRevisionId,
        instruction: "Transient retry fixture",
        idempotencyKey: `${runId}_transient_retry`,
        status: SkillDraftBatchStatus.GENERATING,
        requestedCount: 1,
      },
    });
    const item = await prisma.skillDraftBatchItem.create({
      data: {
        userId,
        batchId: batch.id,
        ordinal: 0,
        targetKey: "transient-retry",
        proposedTitle: "Transient retry draft",
        proposedObjective: "Place a direct object pronoun in one short sentence.",
        locator: pdfLocator({ materialRevisionId, directSectionId, directChunkId }),
        status: SkillDraftBatchItemStatus.PLANNED,
      },
    });
    const requestedAt = new Date().toISOString();
    const failingAi = createAiSetup();
    failingAi.generateDraft = async () => {
      throw new MaterialDraftGenerationError("Temporary provider timeout.", { retryable: true });
    };

    await expect(
      runMaterialDraftItemJob({
        userId,
        batchId: batch.id,
        itemId: item.id,
        requestedAt,
        attempt: 0,
        maxAttempts: 4,
        aiSetup: failingAi,
        sourceEvidenceLoader,
      }),
    ).rejects.toThrow(/temporary provider timeout/i);
    await expect(
      prisma.skillDraftBatchItem.findUnique({ where: { id: item.id } }),
    ).resolves.toMatchObject({
      status: SkillDraftBatchItemStatus.PLANNED,
      generationClaimId: expect.any(String),
      errorCode: "TRANSIENT_GENERATION_FAILURE",
    });

    await expect(
      runMaterialDraftItemJob({
        userId,
        batchId: batch.id,
        itemId: item.id,
        requestedAt,
        attempt: 1,
        maxAttempts: 4,
        aiSetup: createAiSetup(),
        sourceEvidenceLoader,
      }),
    ).resolves.toMatchObject({ status: "ready", alreadyGenerated: false });
  });

  it("treats deleted localized evidence as a permanent draft failure", async () => {
    const batch = await prisma.skillDraftBatch.create({
      data: {
        userId,
        materialRevisionId,
        instruction: "Missing localized evidence fixture",
        idempotencyKey: `${runId}_missing_localized_evidence`,
        status: SkillDraftBatchStatus.GENERATING,
        requestedCount: 1,
      },
    });
    const missingChunkId = `${runId}_deleted_evidence_chunk`;
    const item = await prisma.skillDraftBatchItem.create({
      data: {
        userId,
        batchId: batch.id,
        ordinal: 0,
        targetKey: "missing-localized-evidence",
        proposedTitle: "Missing localized evidence",
        proposedObjective: "Use evidence that has been permanently removed.",
        locator: pdfLocator({
          materialRevisionId,
          directSectionId,
          directChunkId: missingChunkId,
        }),
        status: SkillDraftBatchItemStatus.PLANNED,
      },
    });

    await expect(
      runMaterialDraftItemJob({
        userId,
        batchId: batch.id,
        itemId: item.id,
        requestedAt: new Date().toISOString(),
        attempt: 0,
        maxAttempts: 4,
        aiSetup: createAiSetup(),
        sourceEvidenceLoader,
      }),
    ).rejects.toMatchObject({ retryable: false });
    await expect(
      prisma.skillDraftBatchItem.findUnique({ where: { id: item.id } }),
    ).resolves.toMatchObject({
      status: SkillDraftBatchItemStatus.FAILED,
      generationClaimId: null,
      errorCode: "GENERATION_REJECTED",
      errorMessage: expect.stringMatching(/no longer available/i),
    });
  });

  it("does not save a draft after material deletion wins the generation race", async () => {
    const fixtureTitle = "Deleting generation fixture";
    const { material, revision } = await createMaterialWithInitialRevision({
      userId,
      title: fixtureTitle,
      kind: StudyMaterialKind.PDF,
    });
    const section = await prisma.materialSection.create({
      data: {
        userId,
        materialRevisionId: revision.id,
        ordinal: 0,
        title: "Deletion race section",
        normalizedTitle: "deletion race section",
        pageStart: 1,
        pageEnd: 1,
        headingPath: ["Deletion race section"],
      },
    });
    const chunk = await prisma.materialChunk.create({
      data: {
        userId,
        materialRevisionId: revision.id,
        materialSectionId: section.id,
        ordinal: 0,
        text: "A deletion race must not leave a newly generated draft without its material evidence.",
        tokenEstimate: 14,
        contentHash: `sha256:${runId}:deletion-race`,
        headingText: section.title,
        locator: { kind: "pdf", pageRange: { start: 1, end: 1 } },
      },
    });
    await prisma.sourceFile.create({
      data: {
        userId,
        materialRevisionId: revision.id,
        kind: SourceFileKind.PDF,
        status: SourceFileStatus.READY,
        originalName: "deletion-race.pdf",
        mimeType: "application/pdf",
        storageBucket: "test-materials",
        storageKey: `${runId}/deletion-race.pdf`,
      },
    });
    await finalizeMaterialRevision({
      userId,
      materialId: material.id,
      materialRevisionId: revision.id,
      contentHash: `sha256:${runId}:deletion-race`,
      byteSize: 1_024,
      pageCount: 1,
      storageBucket: "test-materials",
      storageKey: `${runId}/deletion-race.pdf`,
    });
    const batch = await prisma.skillDraftBatch.create({
      data: {
        userId,
        materialRevisionId: revision.id,
        instruction: "Deletion race fixture",
        idempotencyKey: `${runId}_deletion_race`,
        status: SkillDraftBatchStatus.GENERATING,
        requestedCount: 1,
      },
    });
    const item = await prisma.skillDraftBatchItem.create({
      data: {
        userId,
        batchId: batch.id,
        ordinal: 0,
        targetKey: "deletion-race",
        proposedTitle: "Deletion race draft",
        proposedObjective: "Recognize the deletion race evidence boundary.",
        locator: pdfLocator({
          materialRevisionId: revision.id,
          directSectionId: section.id,
          directChunkId: chunk.id,
        }),
        status: SkillDraftBatchItemStatus.PLANNED,
      },
    });
    const deletingAi = createAiSetup();
    const generateDraft = deletingAi.generateDraft;
    deletingAi.generateDraft = async (generationInput) => {
      await requestMaterialDeletion({
        userId,
        materialId: material.id,
        confirmationTitle: fixtureTitle,
      });
      return generateDraft(generationInput);
    };

    await expect(
      runMaterialDraftItemJob({
        userId,
        batchId: batch.id,
        itemId: item.id,
        aiSetup: deletingAi,
        sourceEvidenceLoader,
      }),
    ).resolves.toMatchObject({ status: "not-claimed" });
    await expect(
      prisma.skill.count({ where: { userId, title: "Deletion race draft" } }),
    ).resolves.toBe(0);
  });

  it("blocks exclusion while a linked skill activation job is running", async () => {
    const now = new Date();
    const skill = await prisma.skill.create({
      data: { userId, title: "Activation exclusion fence", tags: [], status: SkillStatus.DRAFT },
    });
    const batch = await prisma.skillDraftBatch.create({
      data: {
        userId,
        materialRevisionId,
        instruction: "Activation exclusion fixture",
        idempotencyKey: `${runId}_activation_exclusion`,
        status: SkillDraftBatchStatus.READY,
        requestedCount: 1,
        readyCount: 1,
      },
    });
    const item = await prisma.skillDraftBatchItem.create({
      data: {
        userId,
        batchId: batch.id,
        skillId: skill.id,
        ordinal: 0,
        targetKey: "activation-exclusion",
        proposedTitle: skill.title,
        proposedObjective: "Block exclusion while activation generates exercises.",
        locator: {},
        status: SkillDraftBatchItemStatus.READY,
      },
    });
    await prisma.generationJob.create({
      data: {
        userId,
        skillId: skill.id,
        kind: GenerationJobKind.SKILL_ACTIVATION,
        status: GenerationJobStatus.RUNNING,
        provider: "gemini",
        model: "fixture-model",
        promptVersion: "fixture-v1",
        requestedCount: 3,
        startedAt: now,
      },
    });

    expect(
      await excludeMaterialDraftItem({
        userId,
        batchId: batch.id,
        itemId: item.id,
        now,
      }),
    ).toMatchObject({ status: "not-excluded", reason: "activation-in-progress" });
    expect(await prisma.skill.count({ where: { id: skill.id } })).toBe(1);
    expect(await prisma.skillDraftBatchItem.findUnique({ where: { id: item.id } })).toMatchObject({
      status: SkillDraftBatchItemStatus.READY,
      skillId: skill.id,
    });

    await prisma.generationJob.updateMany({
      where: { userId, skillId: skill.id, kind: GenerationJobKind.SKILL_ACTIVATION },
      data: { startedAt: new Date(now.getTime() - 12 * 60 * 1_000) },
    });
    await expect(
      excludeMaterialDraftItem({
        userId,
        batchId: batch.id,
        itemId: item.id,
        now,
      }),
    ).resolves.toMatchObject({ status: "excluded" });
    expect(await prisma.skill.count({ where: { id: skill.id } })).toBe(0);
  });

  it("reserves quota once and activates a ready item from its exact stored locator", async () => {
    const ready = await createReadyBatch([
      {
        key: "localized-activation",
        title: "Localized object pronoun practice",
        objective: "Place a direct object pronoun before a conjugated Spanish verb.",
        materialSectionIds: [directSectionId],
        evidenceChunkIds: [directChunkId],
      },
    ]);
    const events: MaterialBatchActivationEvent[] = [];
    const queued = await queueMaterialBatchActivation({
      userId,
      input: { batchId: ready.id, itemIds: [ready.items[0].id] },
      now: new Date("2026-07-09T12:00:00.000Z"),
      eventSender: {
        async sendMaterialBatchActivationRequested(payload) {
          events.push(payload);
        },
      },
    });

    expect(queued).toMatchObject({ status: "queued", queuedItemIds: [ready.items[0].id] });
    expect(events).toHaveLength(1);
    expect(await getMaterialDraftBatch({ userId, batchId: ready.id })).toMatchObject({
      status: SkillDraftBatchStatus.ACTIVATING,
      readyCount: 0,
      items: [{ status: SkillDraftBatchItemStatus.ACTIVATING }],
    });
    expect(
      await queueMaterialBatchActivation({
        userId,
        input: { batchId: ready.id, itemIds: [ready.items[0].id] },
        now: new Date("2026-07-09T12:00:30.000Z"),
        eventSender: {
          async sendMaterialBatchActivationRequested(payload) {
            events.push(payload);
          },
        },
      }),
    ).toMatchObject({ status: "already-queued" });
    expect(events).toHaveLength(1);
    await expect(
      prisma.generationJob.findUniqueOrThrow({ where: { id: events[0].generationJobId } }),
    ).resolves.toMatchObject({
      kind: GenerationJobKind.SKILL_ACTIVATION,
      status: GenerationJobStatus.PENDING,
      skillId: ready.items[0].skill?.id,
    });

    let receivedSourceContext: string | null | undefined;
    const activated = await runMaterialBatchActivationJob({
      userId,
      batchId: ready.id,
      itemId: ready.items[0].id,
      generationJobId: events[0].generationJobId,
      now: new Date("2026-07-09T12:01:00.000Z"),
      generateChoiceExercises: async (input) => {
        receivedSourceContext = input.sourceContext;
        return {
          exercises: [
            generatedChoiceExercise(1),
            generatedChoiceExercise(2),
            generatedChoiceExercise(3),
          ],
        };
      },
      verifyChoiceExercises: async (input) => ({
        verifications: input.candidates.map((candidate) => ({
          candidateId: candidate.candidateId,
          verdict: "verified",
        })),
      }),
      model: "fixture-model",
      sourceEvidenceLoader,
    });

    expect(activated).toMatchObject({ status: "active", alreadyActivated: false });
    expect(receivedSourceContext).toContain("Direct object pronouns replace nouns");
    expect(receivedSourceContext).not.toContain("Indirect object pronouns identify");
    expect(receivedSourceContext).not.toContain("UNRELATED CHAPTER SIX SOURCE TEXT");
    expect(await getMaterialDraftBatch({ userId, batchId: ready.id })).toMatchObject({
      status: SkillDraftBatchStatus.COMPLETE,
      activatedCount: 1,
      items: [{ status: SkillDraftBatchItemStatus.ACTIVE, skill: { status: SkillStatus.ACTIVE } }],
    });

    let refillSourceContext: string | null | undefined;
    const refilled = await refillChoiceExercisesForSkill({
      userId,
      skillId: ready.items[0].skill?.id ?? "missing-skill",
      now: new Date("2026-07-09T12:02:00.000Z"),
      targetReadyCount: 5,
      generateChoiceExercises: async (input) => {
        refillSourceContext = input.sourceContext;
        return {
          exercises: [generatedChoiceExercise(10), generatedChoiceExercise(11)],
        };
      },
      verifyChoiceExercises: acceptAllChoiceExercises,
      model: "fixture-model",
      sourceEvidenceLoader,
    });
    expect(refilled).toMatchObject({ status: "refilled", exerciseCount: 2 });
    expect(refillSourceContext).toContain("Direct object pronouns replace nouns");
    expect(refillSourceContext).not.toContain("UNRELATED CHAPTER SIX SOURCE TEXT");
  });

  it("keeps material activation inside the worker cleanup margin", async () => {
    const ready = await createReadyBatch([{
      key: "material-activation-deadline",
      title: "Material activation deadline fixture",
      objective: "Choose the correct object pronoun from the cited material.",
      materialSectionIds: [directSectionId],
      evidenceChunkIds: [directChunkId],
    }]);
    const events: MaterialBatchActivationEvent[] = [];
    await queueMaterialBatchActivation({
      userId,
      input: { batchId: ready.id, itemIds: [ready.items[0].id] },
      now: new Date(),
      eventSender: {
        async sendMaterialBatchActivationRequested(payload) { events.push(payload); },
      },
    });
    const generator = vi.fn(async () => ({ exercises: [generatedChoiceExercise(91)] }));

    await expect(runMaterialBatchActivationJob({
      userId,
      batchId: ready.id,
      itemId: ready.items[0].id,
      generationJobId: events[0].generationJobId,
      now: new Date(),
      deadlineAt: new Date(Date.now() + 59_000),
      attempt: 0,
      maxAttempts: 2,
      generateChoiceExercises: generator,
      verifyChoiceExercises: acceptAllChoiceExercises,
      model: "fixture-model",
      sourceEvidenceLoader,
    })).rejects.toMatchObject({ retryable: true });
    expect(generator).not.toHaveBeenCalled();
    await expect(prisma.skillDraftBatchItem.findUniqueOrThrow({ where: { id: ready.items[0].id } }))
      .resolves.toMatchObject({
        status: SkillDraftBatchItemStatus.ACTIVATING,
        generationClaimId: null,
        errorCode: "ACTIVATION_RETRYING_TRANSIENT_FAILURE",
      });
  });

  it("returns an edited batch draft to review before its reserved activation starts", async () => {
    const ready = await createReadyBatch([
      {
        key: "activation-reservation-snapshot",
        title: "Reserved pronoun placement fixture",
        objective: "Place a direct object pronoun before a conjugated verb.",
        materialSectionIds: [directSectionId],
        evidenceChunkIds: [directChunkId],
      },
    ]);
    const events: MaterialBatchActivationEvent[] = [];
    const queued = await queueMaterialBatchActivation({
      userId,
      input: { batchId: ready.id, itemIds: [ready.items[0].id] },
      now: new Date("2026-07-09T13:00:00.000Z"),
      eventSender: {
        async sendMaterialBatchActivationRequested(payload) {
          events.push(payload);
        },
      },
    });
    expect(queued).toMatchObject({
      status: "queued",
      queuedItemIds: [ready.items[0].id],
    });
    expect(events).toHaveLength(1);

    const updated = await updateSkillDraft({
      userId,
      skillId: ready.items[0].skill?.id ?? "missing-skill",
      input: {
        title: "Edited reserved pronoun placement fixture",
        objective:
          "Distinguish direct and indirect pronoun placement after reviewing the broader scope.",
        collectionName: "",
        tags: "pronouns, edited",
      },
    });
    expect(updated.status).toBe("updated");
    const generator = vi.fn(async () => ({
      exercises: [
        generatedChoiceExercise(1),
        generatedChoiceExercise(2),
        generatedChoiceExercise(3),
      ],
    }));

    const result = await runMaterialBatchActivationJob({
      userId,
      batchId: ready.id,
      itemId: events[0].itemId,
      generationJobId: events[0].generationJobId,
      now: new Date("2026-07-09T13:01:00.000Z"),
      generateChoiceExercises: generator,
      verifyChoiceExercises: acceptAllChoiceExercises,
      model: "fixture-model",
      sourceEvidenceLoader,
    });

    expect(result).toMatchObject({
      status: "failed",
      reason: "draft-changed",
    });
    expect(generator).not.toHaveBeenCalled();
    const [batch, generationJob, exerciseCount] = await Promise.all([
      getMaterialDraftBatch({ userId, batchId: ready.id }),
      prisma.generationJob.findUniqueOrThrow({
        where: { id: events[0].generationJobId },
      }),
      prisma.exercise.count({
        where: {
          userId,
          skillId: ready.items[0].skill?.id,
        },
      }),
    ]);
    expect(batch).toMatchObject({
      status: SkillDraftBatchStatus.READY,
      readyCount: 1,
      failedCount: 0,
      items: [
        {
          status: SkillDraftBatchItemStatus.READY,
          errorCode: "ACTIVATION_DRAFT_CHANGED",
          skill: {
            title: "Edited reserved pronoun placement fixture",
            status: SkillStatus.DRAFT,
          },
        },
      ],
    });
    expect(generationJob).toMatchObject({
      status: GenerationJobStatus.FAILED,
      errorMessage:
        "This draft changed after it was queued. Review the latest version, then add it again. LearnRecur will check for duplicates first.",
    });
    expect(exerciseCount).toBe(0);
  });

  it("does not reserve a batch when the title-objective library changes during semantic preflight", async () => {
    const fixtureId = randomUUID();
    const ready = await createReadyBatch([
      {
        key: `semantic-preflight-race-${fixtureId}`,
        title: `Position clitics ${fixtureId}`,
        objective:
          `Arrange Spanish object markers relative to a finite predicate for case ${fixtureId}.`,
        materialSectionIds: [directSectionId],
        evidenceChunkIds: [directChunkId],
      },
    ]);
    const mutableSkill = await prisma.skill.create({
      data: {
        userId,
        title: `Triangle altitude ${fixtureId}`,
        objective:
          `Calculate a triangle height from its area and base for case ${fixtureId}.`,
        status: SkillStatus.DRAFT,
      },
    });
    const vectorByText = new Map<string, number[]>();
    let libraryChanged = false;
    const events: string[] = [];

    const result = await queueMaterialBatchActivationService({
      userId,
      input: {
        batchId: ready.id,
        itemIds: [ready.items[0].id],
      },
      now: new Date("2026-07-22T11:00:00.000Z"),
      embeddingGenerator: async ({ texts }) => {
        if (!libraryChanged) {
          const updated = await updateSkillDraft({
            userId,
            skillId: mutableSkill.id,
            input: {
              title: `Unstressed accusatives ${fixtureId}`,
              objective:
                `Decide where lo, la, los, and las belong around conjugated verb forms in scenario ${fixtureId}.`,
              collectionName: "",
              tags: "",
            },
          });
          expect(updated.status).toBe("updated");
          libraryChanged = true;
        }

        return texts.map((text) => {
          const existing = vectorByText.get(text);
          if (existing) {
            return existing;
          }
          const vector = new Array<number>(
            SKILL_SIMILARITY_EMBEDDING_DIMENSIONS,
          ).fill(0);
          vector[
            vectorByText.size % SKILL_SIMILARITY_EMBEDDING_DIMENSIONS
          ] = 1;
          vectorByText.set(text, vector);
          return vector;
        });
      },
      eventSender: {
        async sendMaterialBatchActivationRequested(payload) {
          events.push(payload.itemId);
        },
      },
    });

    expect(libraryChanged).toBe(true);
    expect(result).toMatchObject({
      status: "invalid",
      message:
        "Your skill library changed while LearnRecur checked for similar skills. Try adding these drafts again to review the latest matches.",
    });
    expect(events).toEqual([]);
    await expect(
      prisma.generationJob.count({
        where: {
          userId,
          skillId: ready.items[0].skill?.id,
          kind: GenerationJobKind.SKILL_ACTIVATION,
        },
      }),
    ).resolves.toBe(0);
    await expect(
      prisma.skillDraftBatchItem.findUniqueOrThrow({
        where: { id: ready.items[0].id },
      }),
    ).resolves.toMatchObject({
      status: SkillDraftBatchItemStatus.READY,
    });
  });

  it("returns a batch draft to duplicate review when the library changes during generation", async () => {
    const fixtureId = randomUUID();
    const ready = await createReadyBatch([
      {
        key: `semantic-generation-race-${fixtureId}`,
        title: `Position clitics ${fixtureId}`,
        objective:
          `Arrange Spanish object markers relative to a finite predicate for case ${fixtureId}.`,
        materialSectionIds: [directSectionId],
        evidenceChunkIds: [directChunkId],
      },
    ]);
    const mutableSkill = await prisma.skill.create({
      data: {
        userId,
        title: `Triangle altitude generation ${fixtureId}`,
        objective:
          `Calculate a triangle height from its area and base for case ${fixtureId}.`,
        status: SkillStatus.DRAFT,
      },
    });
    const events: MaterialBatchActivationEvent[] = [];
    const queued = await queueMaterialBatchActivation({
      userId,
      input: {
        batchId: ready.id,
        itemIds: [ready.items[0].id],
      },
      now: new Date("2026-07-22T11:15:00.000Z"),
      eventSender: {
        async sendMaterialBatchActivationRequested(payload) {
          events.push(payload);
        },
      },
    });

    expect(queued).toMatchObject({
      status: "queued",
      queuedItemIds: [ready.items[0].id],
    });
    expect(events).toHaveLength(1);
    await expect(
      prisma.skillDraftBatchItem.findUniqueOrThrow({
        where: { id: ready.items[0].id },
        select: { generationMetadata: true },
      }),
    ).resolves.toMatchObject({
      generationMetadata: {
        activationDuplicateLibraryFingerprint: expect.any(String),
      },
    });

    const result = await runMaterialBatchActivationJob({
      userId,
      batchId: ready.id,
      itemId: events[0].itemId,
      generationJobId: events[0].generationJobId,
      now: new Date("2026-07-22T11:16:00.000Z"),
      generateChoiceExercises: async () => {
        const updated = await updateSkillDraft({
          userId,
          skillId: mutableSkill.id,
          input: {
            title: `Unstressed accusatives ${fixtureId}`,
            objective:
              `Decide where lo, la, los, and las belong around conjugated verb forms in scenario ${fixtureId}.`,
            collectionName: "",
            tags: "",
          },
        });
        expect(updated.status).toBe("updated");
        return {
          exercises: [
            generatedChoiceExercise(1),
            generatedChoiceExercise(2),
            generatedChoiceExercise(3),
          ],
        };
      },
      verifyChoiceExercises: acceptAllChoiceExercises,
      model: "fixture-model",
      sourceEvidenceLoader,
    });

    expect(result).toMatchObject({
      status: "failed",
      reason: "duplicate-review-changed",
      message:
        "Your skill library changed after the duplicate review. Compare the latest matches, then choose again.",
    });
    const [batch, candidate, exerciseCount] = await Promise.all([
      getMaterialDraftBatch({ userId, batchId: ready.id }),
      prisma.skill.findUniqueOrThrow({
        where: { id: ready.items[0].skill?.id },
      }),
      prisma.exercise.count({
        where: {
          userId,
          skillId: ready.items[0].skill?.id,
        },
      }),
    ]);
    expect(batch).toMatchObject({
      status: SkillDraftBatchStatus.READY,
      items: [
        {
          status: SkillDraftBatchItemStatus.READY,
          errorCode: "DUPLICATE_REVIEW_REQUIRED",
        },
      ],
    });
    expect(candidate.status).toBe(SkillStatus.DRAFT);
    expect(exerciseCount).toBe(0);
  });

  it("rechecks edited drafts for duplicates before activation and honors an explicit override", async () => {
    const ready = await createReadyBatch([
      {
        key: "activation-duplicate-gate",
        title: "Contrastive pronoun placement fixture",
        objective: "Place one contrastive pronoun before a conjugated verb.",
        materialSectionIds: [directSectionId],
        evidenceChunkIds: [directChunkId],
      },
    ]);
    const duplicate = await prisma.skill.create({
      data: {
        userId,
        title: "Contrastive pronoun placement fixture",
        objective: "Place one contrastive pronoun before a conjugated verb.",
        tags: ["duplicate-gate"],
        status: SkillStatus.PAUSED,
      },
    });
    const events: string[] = [];
    const reviewed = await queueMaterialBatchActivation({
      userId,
      input: { batchId: ready.id, itemIds: [ready.items[0].id] },
      now: new Date("2026-07-22T12:00:00.000Z"),
      embeddingGenerator: null,
      eventSender: {
        async sendMaterialBatchActivationRequested(payload) {
          events.push(payload.itemId);
        },
      },
    });

    expect(reviewed).toMatchObject({
      status: "review-required",
      reviewItemIds: [ready.items[0].id],
    });
    expect(events).toEqual([]);
    expect(await getMaterialDraftBatch({ userId, batchId: ready.id })).toMatchObject({
      items: [
        {
          status: SkillDraftBatchItemStatus.READY,
          overlapSkillId: duplicate.id,
          errorCode: "DUPLICATE_REVIEW_REQUIRED",
        },
      ],
    });

    const reviewedCandidateFingerprint =
      buildSkillDuplicateCandidateFingerprint(ready.items[0].skill!);
    const editedCandidate = await prisma.skill.update({
      where: { id: ready.items[0].skill!.id },
      data: { tags: ["edited-after-duplicate-review"] },
    });
    await expect(
      queueMaterialBatchActivation({
        userId,
        input: {
          batchId: ready.id,
          itemIds: [ready.items[0].id],
          createSeparatelyMatches: [
            {
              itemId: ready.items[0].id,
              skillId: duplicate.id,
              candidateFingerprint: reviewedCandidateFingerprint,
              skillFingerprint:
                buildSkillDuplicateReviewFingerprint(duplicate),
            },
          ],
        },
        now: new Date("2026-07-22T12:00:15.000Z"),
        embeddingGenerator: null,
        eventSender: {
          async sendMaterialBatchActivationRequested(payload) {
            events.push(payload.itemId);
          },
        },
      }),
    ).resolves.toMatchObject({
      status: "invalid",
      message:
        "This draft changed after duplicate review. Review the latest draft, then choose again.",
    });
    expect(events).toEqual([]);

    const staleOverride = await queueMaterialBatchActivation({
      userId,
      input: {
        batchId: ready.id,
        itemIds: [ready.items[0].id],
        createSeparatelyMatches: [
          {
            itemId: ready.items[0].id,
            skillId: "stale-match-id",
            candidateFingerprint:
              buildSkillDuplicateCandidateFingerprint(editedCandidate),
            skillFingerprint:
              buildSkillDuplicateReviewFingerprint(duplicate),
          },
        ],
      },
      now: new Date("2026-07-22T12:00:30.000Z"),
      embeddingGenerator: null,
      eventSender: {
        async sendMaterialBatchActivationRequested(payload) {
          events.push(payload.itemId);
        },
      },
    });
    expect(staleOverride).toMatchObject({
      status: "review-required",
      reviewItemIds: [ready.items[0].id],
    });
    expect(events).toEqual([]);

    const overridden = await queueMaterialBatchActivation({
      userId,
      input: {
        batchId: ready.id,
        itemIds: [ready.items[0].id],
        createSeparatelyMatches: [
          {
            itemId: ready.items[0].id,
            skillId: duplicate.id,
            candidateFingerprint:
              buildSkillDuplicateCandidateFingerprint(editedCandidate),
            skillFingerprint:
              buildSkillDuplicateReviewFingerprint(duplicate),
          },
        ],
      },
      now: new Date("2026-07-22T12:01:00.000Z"),
      embeddingGenerator: null,
      eventSender: {
        async sendMaterialBatchActivationRequested(payload) {
          events.push(payload.itemId);
        },
      },
    });
    expect(overridden).toMatchObject({
      status: "queued",
      queuedItemIds: [ready.items[0].id],
    });
    expect(events).toEqual([ready.items[0].id]);
    const activationJob = await prisma.generationJob.findFirstOrThrow({
      where: {
        userId,
        skillId: ready.items[0].skill?.id,
        kind: GenerationJobKind.SKILL_ACTIVATION,
        status: GenerationJobStatus.PENDING,
      },
    });

    const activationResult = await runMaterialBatchActivationJob({
      userId,
      batchId: ready.id,
      itemId: ready.items[0].id,
      generationJobId: activationJob.id,
      now: new Date("2026-07-22T12:02:00.000Z"),
      generateChoiceExercises: async () => {
        await prisma.skill.update({
          where: { id: duplicate.id },
          data: {
            tags: ["duplicate-gate", "changed-during-generation"],
          },
        });
        return {
          exercises: [
            generatedChoiceExercise(1),
            generatedChoiceExercise(2),
            generatedChoiceExercise(3),
          ],
        };
      },
      verifyChoiceExercises: acceptAllChoiceExercises,
      model: "fixture-model",
      sourceEvidenceLoader,
    });

    expect(activationResult).toMatchObject({
      status: "failed",
      reason: "duplicate-review-changed",
    });
    const [batchAfterChange, candidateAfterChange, exerciseCount] =
      await Promise.all([
        getMaterialDraftBatch({ userId, batchId: ready.id }),
        prisma.skill.findUniqueOrThrow({
          where: { id: ready.items[0].skill?.id },
        }),
        prisma.exercise.count({
          where: {
            userId,
            skillId: ready.items[0].skill?.id,
          },
        }),
      ]);
    expect(batchAfterChange).toMatchObject({
      status: SkillDraftBatchStatus.READY,
      items: [
        {
          status: SkillDraftBatchItemStatus.READY,
          errorCode: "DUPLICATE_REVIEW_REQUIRED",
        },
      ],
    });
    expect(candidateAfterChange.status).toBe(SkillStatus.DRAFT);
    expect(exerciseCount).toBe(0);
  });

  it("persists the keep-existing choice after removing a duplicate generated draft", async () => {
    const fixtureId = randomUUID();
    const title = `Keep existing outcome ${fixtureId}`;
    const objective =
      `Place a direct object pronoun before the verb in fixture ${fixtureId}.`;
    const ready = await createReadyBatch([
      {
        key: "keep-existing-outcome",
        title,
        objective,
        materialSectionIds: [directSectionId],
        evidenceChunkIds: [directChunkId],
      },
    ]);
    const generatedSkillId = ready.items[0].skill?.id;
    if (!generatedSkillId) {
      throw new Error("expected a generated duplicate draft");
    }
    const existing = await prisma.skill.create({
      data: {
        userId,
        title,
        objective,
        tags: ["keep-existing"],
        status: SkillStatus.PAUSED,
      },
    });
    await expect(
      queueMaterialBatchActivation({
        userId,
        input: { batchId: ready.id, itemIds: [ready.items[0].id] },
        now: new Date(),
        embeddingGenerator: null,
        eventSender: { async sendMaterialBatchActivationRequested() {} },
      }),
    ).resolves.toMatchObject({ status: "review-required" });

    await expect(
      excludeMaterialDraftItem({
        userId,
        batchId: ready.id,
        itemId: ready.items[0].id,
        intent: "use-existing",
        expectedCandidateId: generatedSkillId,
        expectedCandidateFingerprint:
          buildSkillDuplicateCandidateFingerprint(ready.items[0].skill!),
        expectedMatchId: existing.id,
        expectedMatchFingerprint:
          buildSkillDuplicateReviewFingerprint(existing),
        now: new Date(),
      }),
    ).resolves.toMatchObject({ status: "excluded" });

    expect(await prisma.skill.count({ where: { id: generatedSkillId } })).toBe(0);
    expect(await prisma.skill.findUnique({ where: { id: existing.id } })).toMatchObject({
      status: SkillStatus.PAUSED,
    });
    expect(
      await prisma.skillDraftBatchItem.findUnique({
        where: { id: ready.items[0].id },
        select: { skillId: true },
      }),
    ).toEqual({ skillId: null });
    expect(await getMaterialDraftBatch({ userId, batchId: ready.id })).toMatchObject({
      items: [
        {
          status: SkillDraftBatchItemStatus.EXCLUDED,
          overlapSkillId: existing.id,
          errorCode: "DUPLICATE_USE_EXISTING",
          errorMessage: expect.stringMatching(/paused/i),
        },
      ],
    });
  });

  it("keeps the generated draft when the reviewed match metadata or identity changes", async () => {
    const fixtureId = randomUUID();
    const title = `Changed reviewed match ${fixtureId}`;
    const objective =
      `Place a direct object pronoun before the verb in fixture ${fixtureId}.`;
    const ready = await createReadyBatch([
      {
        key: "changed-reviewed-match",
        title,
        objective,
        materialSectionIds: [directSectionId],
        evidenceChunkIds: [directChunkId],
      },
    ]);
    const generatedSkillId = ready.items[0].skill?.id;
    if (!generatedSkillId) {
      throw new Error("expected a generated draft");
    }
    const existing = await prisma.skill.create({
      data: {
        userId,
        title,
        objective,
        tags: ["changed-reviewed-match"],
        status: SkillStatus.ACTIVE,
      },
    });
    const replacement = await prisma.skill.create({
      data: {
        userId,
        title: `Replacement match ${runId}`,
        objective:
          "Distinguish a direct object from an indirect object in a sentence.",
        tags: ["replacement-reviewed-match"],
        status: SkillStatus.ACTIVE,
      },
    });
    await expect(
      queueMaterialBatchActivation({
        userId,
        input: { batchId: ready.id, itemIds: [ready.items[0].id] },
        now: new Date(),
        embeddingGenerator: null,
        eventSender: { async sendMaterialBatchActivationRequested() {} },
      }),
    ).resolves.toMatchObject({ status: "review-required" });
    const reviewedFingerprint =
      buildSkillDuplicateReviewFingerprint(existing);
    await prisma.skill.update({
      where: { id: existing.id },
      data: {
        tags: ["changed-reviewed-match", "changed-after-review"],
      },
    });

    await expect(
      excludeMaterialDraftItem({
        userId,
        batchId: ready.id,
        itemId: ready.items[0].id,
        intent: "use-existing",
        expectedCandidateId: generatedSkillId,
        expectedCandidateFingerprint:
          buildSkillDuplicateCandidateFingerprint(ready.items[0].skill!),
        expectedMatchId: existing.id,
        expectedMatchFingerprint: reviewedFingerprint,
        now: new Date(),
      }),
    ).resolves.toMatchObject({
      status: "not-excluded",
      reason: "match-changed",
    });
    expect(await prisma.skill.count({ where: { id: generatedSkillId } })).toBe(1);

    await prisma.skillDraftBatchItem.update({
      where: { id: ready.items[0].id },
      data: { overlapSkillId: replacement.id },
    });
    await expect(
      excludeMaterialDraftItem({
        userId,
        batchId: ready.id,
        itemId: ready.items[0].id,
        intent: "use-existing",
        expectedCandidateId: generatedSkillId,
        expectedCandidateFingerprint:
          buildSkillDuplicateCandidateFingerprint(ready.items[0].skill!),
        expectedMatchId: existing.id,
        expectedMatchFingerprint: reviewedFingerprint,
        now: new Date(),
      }),
    ).resolves.toMatchObject({
      status: "not-excluded",
      reason: "match-changed",
    });
    expect(await prisma.skill.count({ where: { id: generatedSkillId } })).toBe(1);
  });

  it("keeps an edited generated draft when use-existing was submitted from a stale preview", async () => {
    const fixtureId = randomUUID();
    const title = `Edited candidate duplicate ${fixtureId}`;
    const objective =
      `Replace one direct object with a pronoun in fixture ${fixtureId}.`;
    const ready = await createReadyBatch([
      {
        key: "edited-candidate-duplicate",
        title,
        objective,
        materialSectionIds: [directSectionId],
        evidenceChunkIds: [directChunkId],
      },
    ]);
    const generatedSkillId = ready.items[0].skill?.id;
    if (!generatedSkillId) {
      throw new Error("expected an editable generated draft");
    }
    const existing = await prisma.skill.create({
      data: {
        userId,
        title,
        objective,
        tags: ["edited-candidate-duplicate"],
        status: SkillStatus.ACTIVE,
      },
    });
    await expect(
      queueMaterialBatchActivation({
        userId,
        input: { batchId: ready.id, itemIds: [ready.items[0].id] },
        now: new Date(),
        embeddingGenerator: null,
        eventSender: { async sendMaterialBatchActivationRequested() {} },
      }),
    ).resolves.toMatchObject({ status: "review-required" });
    const reviewedCandidateFingerprint =
      buildSkillDuplicateCandidateFingerprint(ready.items[0].skill!);
    const editedTitle = `Distinct edited candidate ${fixtureId}`;
    const editedObjective =
      "Find the area of a triangle from its base and perpendicular height.";
    await prisma.skill.update({
      where: { id: generatedSkillId },
      data: { title: editedTitle, objective: editedObjective },
    });

    await expect(
      excludeMaterialDraftItem({
        userId,
        batchId: ready.id,
        itemId: ready.items[0].id,
        intent: "use-existing",
        expectedCandidateId: generatedSkillId,
        expectedCandidateFingerprint: reviewedCandidateFingerprint,
        expectedMatchId: existing.id,
        expectedMatchFingerprint:
          buildSkillDuplicateReviewFingerprint(existing),
        now: new Date(),
      }),
    ).resolves.toMatchObject({
      status: "not-excluded",
      reason: "draft-changed",
    });
    expect(
      await prisma.skill.findUnique({
        where: { id: generatedSkillId },
        select: { title: true, objective: true, status: true },
      }),
    ).toEqual({
      title: editedTitle,
      objective: editedObjective,
      status: SkillStatus.DRAFT,
    });
    expect(
      await prisma.skillDraftBatchItem.findUnique({
        where: { id: ready.items[0].id },
        select: { status: true, skillId: true, errorCode: true },
      }),
    ).toEqual({
      status: SkillDraftBatchItemStatus.READY,
      skillId: generatedSkillId,
      errorCode: "DUPLICATE_REVIEW_REQUIRED",
    });
  });

  it("serializes simultaneous keep-existing and create-separately choices", async () => {
    const fixtureId = randomUUID();
    const title = `Concurrent duplicate choice ${fixtureId}`;
    const objective =
      `Replace one direct object with a pronoun in fixture ${fixtureId}.`;
    const ready = await createReadyBatch([
      {
        key: "concurrent-duplicate-choice",
        title,
        objective,
        materialSectionIds: [directSectionId],
        evidenceChunkIds: [directChunkId],
      },
    ]);
    const generatedSkillId = ready.items[0].skill?.id;
    if (!generatedSkillId) {
      throw new Error("expected a concurrent-choice generated draft");
    }
    const existing = await prisma.skill.create({
      data: {
        userId,
        title,
        objective,
        tags: ["concurrent-duplicate-choice"],
        status: SkillStatus.ACTIVE,
      },
    });
    await expect(
      queueMaterialBatchActivation({
        userId,
        input: { batchId: ready.id, itemIds: [ready.items[0].id] },
        now: new Date(),
        embeddingGenerator: null,
        eventSender: { async sendMaterialBatchActivationRequested() {} },
      }),
    ).resolves.toMatchObject({ status: "review-required" });
    const reviewedFingerprint =
      buildSkillDuplicateReviewFingerprint(existing);

    await Promise.all([
      excludeMaterialDraftItem({
        userId,
        batchId: ready.id,
        itemId: ready.items[0].id,
        intent: "use-existing",
        expectedCandidateId: generatedSkillId,
        expectedCandidateFingerprint:
          buildSkillDuplicateCandidateFingerprint(ready.items[0].skill!),
        expectedMatchId: existing.id,
        expectedMatchFingerprint: reviewedFingerprint,
        now: new Date(),
      }),
      queueMaterialBatchActivation({
        userId,
        input: {
          batchId: ready.id,
          itemIds: [ready.items[0].id],
          createSeparatelyMatches: [
            {
              itemId: ready.items[0].id,
              skillId: existing.id,
              candidateFingerprint:
                buildSkillDuplicateCandidateFingerprint(
                  ready.items[0].skill!,
                ),
              skillFingerprint: reviewedFingerprint,
            },
          ],
        },
        now: new Date(),
        embeddingGenerator: null,
        eventSender: { async sendMaterialBatchActivationRequested() {} },
      }),
    ]);

    const settledItem = await prisma.skillDraftBatchItem.findUniqueOrThrow({
      where: { id: ready.items[0].id },
      select: {
        status: true,
        skillId: true,
        overlapSkillId: true,
        errorCode: true,
      },
    });
    if (settledItem.status === SkillDraftBatchItemStatus.EXCLUDED) {
      expect(settledItem).toMatchObject({
        skillId: null,
        overlapSkillId: existing.id,
        errorCode: "DUPLICATE_USE_EXISTING",
      });
      expect(await prisma.skill.count({ where: { id: generatedSkillId } })).toBe(0);
    } else {
      expect(settledItem).toMatchObject({
        status: SkillDraftBatchItemStatus.ACTIVATING,
        skillId: generatedSkillId,
        errorCode: null,
      });
      expect(await prisma.skill.count({ where: { id: generatedSkillId } })).toBe(1);
      expect(
        await prisma.generationJob.count({
          where: {
            userId,
            skillId: generatedSkillId,
            kind: GenerationJobKind.SKILL_ACTIVATION,
            status: GenerationJobStatus.PENDING,
          },
        }),
      ).toBe(1);
    }

    await prisma.skillDraftBatch.delete({ where: { id: ready.id } });
    await prisma.skill.deleteMany({
      where: { id: { in: [generatedSkillId, existing.id] }, userId },
    });
  });

  it("keeps the generated draft when a reviewed existing skill disappears", async () => {
    const fixtureId = randomUUID();
    const title = `Deleted activation match ${fixtureId}`;
    const objective =
      `Choose the correct direct object pronoun in fixture ${fixtureId}.`;
    const ready = await createReadyBatch([
      {
        key: "deleted-activation-match",
        title,
        objective,
        materialSectionIds: [directSectionId],
        evidenceChunkIds: [directChunkId],
      },
    ]);
    const generatedSkillId = ready.items[0].skill?.id;
    if (!generatedSkillId) {
      throw new Error("expected a generated draft");
    }
    const existing = await prisma.skill.create({
      data: {
        userId,
        title,
        objective,
        tags: ["deleted-activation-match"],
        status: SkillStatus.ACTIVE,
      },
    });
    await expect(
      queueMaterialBatchActivation({
        userId,
        input: { batchId: ready.id, itemIds: [ready.items[0].id] },
        now: new Date(),
        embeddingGenerator: null,
        eventSender: { async sendMaterialBatchActivationRequested() {} },
      }),
    ).resolves.toMatchObject({ status: "review-required" });
    const expectedMatchFingerprint =
      buildSkillDuplicateReviewFingerprint(existing);
    await prisma.skill.delete({ where: { id: existing.id } });

    await expect(
      excludeMaterialDraftItem({
        userId,
        batchId: ready.id,
        itemId: ready.items[0].id,
        intent: "use-existing",
        expectedCandidateId: generatedSkillId,
        expectedCandidateFingerprint:
          buildSkillDuplicateCandidateFingerprint(ready.items[0].skill!),
        expectedMatchId: existing.id,
        expectedMatchFingerprint,
        now: new Date(),
      }),
    ).resolves.toMatchObject({
      status: "not-excluded",
      reason: "match-not-found",
    });
    expect(await prisma.skill.count({ where: { id: generatedSkillId } })).toBe(1);
    expect(
      await prisma.skillDraftBatchItem.findUnique({
        where: { id: ready.items[0].id },
        select: { skillId: true },
      }),
    ).toEqual({ skillId: generatedSkillId });
    expect(await getMaterialDraftBatch({ userId, batchId: ready.id })).toMatchObject({
      items: [
        {
          status: SkillDraftBatchItemStatus.READY,
          overlapSkillId: existing.id,
          errorCode: "DUPLICATE_REVIEW_REQUIRED",
        },
      ],
    });
  });

  it("reuses an existing activation job instead of inserting a conflicting reservation", async () => {
    const ready = await createReadyBatch([
      {
        key: "existing-activation-job",
        title: "Existing activation job fixture",
        objective: "Choose a direct object pronoun while activation is already running.",
        materialSectionIds: [directSectionId],
        evidenceChunkIds: [directChunkId],
      },
    ]);
    const skillId = ready.items[0].skill?.id;
    if (!skillId) {
      throw new Error("expected an existing activation fixture skill");
    }
    const existingJob = await prisma.generationJob.create({
      data: {
        userId,
        skillId,
        kind: GenerationJobKind.SKILL_ACTIVATION,
        status: GenerationJobStatus.RUNNING,
        provider: "gemini",
        model: "fixture-model",
        promptVersion: "skill-mcq-v0",
        requestedCount: 3,
        startedAt: new Date(),
      },
    });

    await expect(
      queueMaterialBatchActivation({
        userId,
        input: { batchId: ready.id, itemIds: [ready.items[0].id] },
        now: new Date(),
        eventSender: { async sendMaterialBatchActivationRequested() {} },
      }),
    ).resolves.toMatchObject({ status: "already-queued" });
    await expect(
      prisma.generationJob.count({
        where: { userId, skillId, kind: GenerationJobKind.SKILL_ACTIVATION },
      }),
    ).resolves.toBe(1);
    expect(await getMaterialDraftBatch({ userId, batchId: ready.id })).toMatchObject({
      readyCount: 0,
      items: [{ status: SkillDraftBatchItemStatus.ACTIVATING }],
    });

    await prisma.$transaction([
      prisma.generationJob.update({
        where: { id: existingJob.id },
        data: { status: GenerationJobStatus.FAILED, completedAt: new Date() },
      }),
      prisma.skillDraftBatchItem.update({
        where: { id: ready.items[0].id },
        data: {
          status: SkillDraftBatchItemStatus.FAILED,
          errorCode: "ACTIVATION_RETRYABLE_TEST_CLEANUP",
          errorMessage: "released after existing job test",
        },
      }),
    ]);
  });

  it("does not let a stale activation worker overwrite a newer claim", async () => {
    const ready = await createReadyBatch([
      {
        key: "activation-claim-fence",
        title: "Activation claim fence fixture",
        objective: "Choose a direct object pronoun while activation claims race.",
        materialSectionIds: [directSectionId],
        evidenceChunkIds: [directChunkId],
      },
    ]);
    const events: MaterialBatchActivationEvent[] = [];
    await queueMaterialBatchActivation({
      userId,
      input: { batchId: ready.id, itemIds: [ready.items[0].id] },
      now: new Date("2026-07-17T12:00:00.000Z"),
      eventSender: {
        async sendMaterialBatchActivationRequested(payload) {
          events.push(payload);
        },
      },
    });
    let markWorkerStarted = () => {};
    const workerStarted = new Promise<void>((resolve) => {
      markWorkerStarted = resolve;
    });
    let releaseWorker = () => {};
    const workerMayFail = new Promise<void>((resolve) => {
      releaseWorker = resolve;
    });
    const staleWorker = runMaterialBatchActivationJob({
      ...events[0],
      now: new Date("2026-07-17T12:01:00.000Z"),
      generateChoiceExercises: async () => {
        markWorkerStarted();
        await workerMayFail;
        throw new Error("stale activation worker failed");
      },
      verifyChoiceExercises: acceptAllChoiceExercises,
      model: "fixture-model",
      sourceEvidenceLoader,
    });
    await workerStarted;
    await prisma.skillDraftBatchItem.update({
      where: { id: ready.items[0].id },
      data: {
        status: SkillDraftBatchItemStatus.ACTIVATING,
        generationClaimId: "newer-activation-claim",
      },
    });
    releaseWorker();

    await expect(staleWorker).resolves.toMatchObject({ status: "not-claimed" });
    await expect(
      prisma.skillDraftBatchItem.findUnique({ where: { id: ready.items[0].id } }),
    ).resolves.toMatchObject({
      status: SkillDraftBatchItemStatus.ACTIVATING,
      generationClaimId: "newer-activation-claim",
    });
    await prisma.skillDraftBatchItem.update({
      where: { id: ready.items[0].id },
      data: {
        status: SkillDraftBatchItemStatus.FAILED,
        generationClaimId: null,
        errorCode: "ACTIVATION_RETRYABLE_TEST_CLEANUP",
        errorMessage: "released after activation claim test",
      },
    });
  });

  it("keeps a batch draft recoverable when a standalone activation supersedes its worker", async () => {
    const batchNow = new Date();
    const ready = await createReadyBatch([
      {
        key: "cross-flow-activation-fence",
        title: "Cross-flow activation fence fixture",
        objective: "Choose a direct object pronoun while two activation flows overlap.",
        materialSectionIds: [directSectionId],
        evidenceChunkIds: [directChunkId],
      },
    ]);
    const events: MaterialBatchActivationEvent[] = [];
    await queueMaterialBatchActivation({
      userId,
      input: { batchId: ready.id, itemIds: [ready.items[0].id] },
      now: batchNow,
      eventSender: {
        async sendMaterialBatchActivationRequested(payload) {
          events.push(payload);
        },
      },
    });
    if (!ready.items[0].skill || !events[0]) {
      throw new Error("Expected a queued cross-flow activation fixture.");
    }

    let signalBatchWorkerStarted: (() => void) | undefined;
    const batchWorkerStarted = new Promise<void>((resolve) => {
      signalBatchWorkerStarted = resolve;
    });
    let releaseBatchWorker: (() => void) | undefined;
    const batchWorkerCanFail = new Promise<void>((resolve) => {
      releaseBatchWorker = resolve;
    });
    const batchWorker = runMaterialBatchActivationJob({
      userId,
      batchId: ready.id,
      itemId: events[0].itemId,
      generationJobId: events[0].generationJobId,
      now: new Date(batchNow.getTime() + 60_000),
      generateChoiceExercises: async () => {
        signalBatchWorkerStarted?.();
        await batchWorkerCanFail;
        throw new Error("superseded batch worker fixture");
      },
      verifyChoiceExercises: acceptAllChoiceExercises,
      model: "batch-fixture-model",
      sourceEvidenceLoader,
    });
    await batchWorkerStarted;

    let signalStandaloneStarted: (() => void) | undefined;
    const standaloneStarted = new Promise<void>((resolve) => {
      signalStandaloneStarted = resolve;
    });
    let releaseStandalone: (() => void) | undefined;
    const standaloneCanFinish = new Promise<void>((resolve) => {
      releaseStandalone = resolve;
    });
    const standaloneNow = new Date(batchNow.getTime() + 60_000 + ACTIVATION_GENERATION_STALE_AFTER_MS + 1_000);
    const standalone = activateSkillDraft({
      userId,
      skillId: ready.items[0].skill.id,
      now: standaloneNow,
      eventNow: () => standaloneNow,
      generateChoiceExercises: async () => {
        signalStandaloneStarted?.();
        await standaloneCanFinish;
        return {
          exercises: [
            generatedChoiceExercise(51),
            generatedChoiceExercise(52),
            generatedChoiceExercise(53),
          ],
        };
      },
      verifyChoiceExercises: acceptAllChoiceExercises,
      model: "standalone-fixture-model",
      sourceEvidenceLoader,
      skipUsageLimitCheck: true,
    });
    await standaloneStarted;

    releaseBatchWorker?.();
    await expect(batchWorker).resolves.toMatchObject({
      status: "not-claimed",
    });
    await expect(
      prisma.skillDraftBatchItem.findUniqueOrThrow({
        where: { id: ready.items[0].id },
      }),
    ).resolves.toMatchObject({
      status: SkillDraftBatchItemStatus.READY,
      generationClaimId: null,
      errorCode: "ACTIVATION_SUPERSEDED",
    });

    releaseStandalone?.();
    const standaloneResult = await standalone;
    expect(standaloneResult).toMatchObject({
      status: "activated",
      skillId: ready.items[0].skill.id,
    });
    if (standaloneResult.status !== "activated") {
      throw new Error("Expected the standalone replacement to activate the skill.");
    }
    await expect(
      getMaterialDraftBatch({ userId, batchId: ready.id }),
    ).resolves.toMatchObject({
      status: SkillDraftBatchStatus.COMPLETE,
      activatedCount: 1,
      items: [
        {
          status: SkillDraftBatchItemStatus.ACTIVE,
          errorCode: null,
          skill: { status: SkillStatus.ACTIVE },
        },
      ],
    });
    const jobs = await prisma.generationJob.findMany({
      where: {
        userId,
        skillId: ready.items[0].skill.id,
        kind: GenerationJobKind.SKILL_ACTIVATION,
      },
      orderBy: { createdAt: "asc" },
    });
    expect(jobs).toHaveLength(2);
    expect(
      jobs.find((job) => job.id === events[0].generationJobId),
    ).toMatchObject({
      status: GenerationJobStatus.FAILED,
    });
    expect(
      jobs.find((job) => job.id === standaloneResult.generationJobId),
    ).toMatchObject({
      status: GenerationJobStatus.SUCCEEDED,
    });
  });

  it("does not strand a retry when another activation is already running", async () => {
    const ready = await createReadyBatch([
      {
        key: "cross-flow-retry-release",
        title: "Cross-flow retry release fixture",
        objective: "Choose a direct object pronoun after another activation attempt.",
        materialSectionIds: [directSectionId],
        evidenceChunkIds: [directChunkId],
      },
    ]);
    const skillId = ready.items[0].skill?.id;
    if (!skillId) {
      throw new Error("expected a cross-flow retry fixture skill");
    }
    await prisma.skillDraftBatchItem.update({
      where: { id: ready.items[0].id },
      data: {
        status: SkillDraftBatchItemStatus.FAILED,
        errorCode: "ACTIVATION_RETRYABLE_STALE_CLAIM",
        errorMessage: "Activation stopped before it finished. Retry or exclude this item.",
      },
    });

    let signalStandaloneStarted: (() => void) | undefined;
    const standaloneStarted = new Promise<void>((resolve) => {
      signalStandaloneStarted = resolve;
    });
    let releaseStandalone: (() => void) | undefined;
    const standaloneCanFail = new Promise<void>((resolve) => {
      releaseStandalone = resolve;
    });
    const standalone = activateSkillDraft({
      userId,
      skillId,
      now: new Date(),
      generateChoiceExercises: async () => {
        signalStandaloneStarted?.();
        await standaloneCanFail;
        throw new Error("standalone activation fixture failed");
      },
      verifyChoiceExercises: acceptAllChoiceExercises,
      model: "standalone-failure-model",
      sourceEvidenceLoader,
      skipUsageLimitCheck: true,
    });
    await standaloneStarted;

    const retrySender = vi.fn(async () => {});
    await expect(
      retryMaterialBatchActivationItem({
        userId,
        batchId: ready.id,
        itemId: ready.items[0].id,
        now: new Date(),
        eventSender: {
          sendMaterialBatchActivationRequested: retrySender,
        },
      }),
    ).resolves.toMatchObject({
      status: "already-running",
      message:
        "Another activation attempt is already running. You can wait for it to finish or add this draft again.",
    });
    expect(retrySender).not.toHaveBeenCalled();
    await expect(
      getMaterialDraftBatch({ userId, batchId: ready.id }),
    ).resolves.toMatchObject({
      status: SkillDraftBatchStatus.READY,
      items: [
        {
          status: SkillDraftBatchItemStatus.READY,
          errorCode: "ACTIVATION_SUPERSEDED",
          skill: {
            generationJobs: [expect.objectContaining({ id: expect.any(String) })],
          },
        },
      ],
    });
    await expect(
      prisma.skillDraftBatchItem.findUniqueOrThrow({
        where: { id: ready.items[0].id },
      }),
    ).resolves.toMatchObject({
      status: SkillDraftBatchItemStatus.READY,
      generationClaimId: null,
    });

    releaseStandalone?.();
    await expect(standalone).resolves.toMatchObject({
      status: "not-activated",
      reason: "generation-failed",
    });
    await expect(
      getMaterialDraftBatch({ userId, batchId: ready.id }),
    ).resolves.toMatchObject({
      status: SkillDraftBatchStatus.READY,
      items: [
        {
          status: SkillDraftBatchItemStatus.READY,
          errorCode: "ACTIVATION_SUPERSEDED",
          skill: {
            status: SkillStatus.DRAFT,
            generationJobs: [],
          },
        },
      ],
    });
    await expect(
      prisma.skillDraftBatchItem.findUniqueOrThrow({
        where: { id: ready.items[0].id },
      }),
    ).resolves.toMatchObject({
      status: SkillDraftBatchItemStatus.READY,
      generationClaimId: null,
    });
  });

  it("retires an abandoned activation while keeping its batch draft ready", async () => {
    const ready = await createReadyBatch([
      {
        key: "cross-flow-abandoned-owner",
        title: "Abandoned activation owner fixture",
        objective: "Choose a direct object pronoun after an abandoned activation.",
        materialSectionIds: [directSectionId],
        evidenceChunkIds: [directChunkId],
      },
    ]);
    const skillId = ready.items[0].skill?.id;
    if (!skillId) {
      throw new Error("expected an abandoned activation fixture skill");
    }
    await prisma.skillDraftBatchItem.update({
      where: { id: ready.items[0].id },
      data: {
        status: SkillDraftBatchItemStatus.FAILED,
        errorCode: "ACTIVATION_RETRYABLE_STALE_CLAIM",
        errorMessage: "Activation stopped before it finished. Retry or exclude this item.",
      },
    });

    let signalStandaloneStarted: (() => void) | undefined;
    const standaloneStarted = new Promise<void>((resolve) => {
      signalStandaloneStarted = resolve;
    });
    let releaseStandalone: (() => void) | undefined;
    const standaloneCanFinish = new Promise<void>((resolve) => {
      releaseStandalone = resolve;
    });
    const standalone = activateSkillDraft({
      userId,
      skillId,
      now: new Date(),
      generateChoiceExercises: async () => {
        signalStandaloneStarted?.();
        await standaloneCanFinish;
        return {
          exercises: [
            generatedChoiceExercise(71),
            generatedChoiceExercise(72),
            generatedChoiceExercise(73),
          ],
        };
      },
      verifyChoiceExercises: acceptAllChoiceExercises,
      model: "abandoned-owner-model",
      sourceEvidenceLoader,
      skipUsageLimitCheck: true,
    });
    await standaloneStarted;

    const retrySender = vi.fn(async () => {});
    await expect(
      retryMaterialBatchActivationItem({
        userId,
        batchId: ready.id,
        itemId: ready.items[0].id,
        now: new Date(),
        eventSender: {
          sendMaterialBatchActivationRequested: retrySender,
        },
      }),
    ).resolves.toMatchObject({ status: "already-running" });
    expect(retrySender).not.toHaveBeenCalled();
    const abandonedJob = await prisma.generationJob.findFirstOrThrow({
      where: {
        userId,
        skillId,
        kind: GenerationJobKind.SKILL_ACTIVATION,
        status: GenerationJobStatus.RUNNING,
      },
    });
    await prisma.generationJob.update({
      where: { id: abandonedJob.id },
      data: { startedAt: new Date(Date.now() - 6 * 60 * 1_000) },
    });

    await expect(
      getMaterialDraftBatch({ userId, batchId: ready.id }),
    ).resolves.toMatchObject({
      status: SkillDraftBatchStatus.READY,
      items: [
        {
          status: SkillDraftBatchItemStatus.READY,
          errorCode: "ACTIVATION_SUPERSEDED",
          skill: {
            status: SkillStatus.DRAFT,
            generationJobs: [],
          },
        },
      ],
    });
    await expect(
      prisma.generationJob.findUniqueOrThrow({ where: { id: abandonedJob.id } }),
    ).resolves.toMatchObject({
      status: GenerationJobStatus.FAILED,
      errorMessage: ACTIVATION_SUPERSEDED_JOB_MESSAGE,
    });

    releaseStandalone?.();
    await expect(standalone).resolves.toMatchObject({
      status: "not-activated",
      reason: "activation-superseded",
    });
    await expect(
      prisma.skillDraftBatchItem.findUniqueOrThrow({
        where: { id: ready.items[0].id },
      }),
    ).resolves.toMatchObject({
      status: SkillDraftBatchItemStatus.READY,
      generationClaimId: null,
      errorCode: "ACTIVATION_SUPERSEDED",
    });
    await expect(
      prisma.exercise.count({ where: { userId, skillId } }),
    ).resolves.toBe(0);
  });

  it("recovers stale activation claims when the batch is opened", async () => {
    const ready = await createReadyBatch([
      {
        key: "stale-activation-recovery",
        title: "Stale activation recovery fixture",
        objective: "Choose a direct object pronoun after a stopped activation.",
        materialSectionIds: [directSectionId],
        evidenceChunkIds: [directChunkId],
      },
    ]);
    const skillId = ready.items[0].skill?.id;
    if (!skillId) {
      throw new Error("expected a stale activation fixture skill");
    }
    const staleAt = new Date(Date.now() - 6 * 60 * 1_000);
    const job = await prisma.generationJob.create({
      data: {
        userId,
        skillId,
        kind: GenerationJobKind.SKILL_ACTIVATION,
        status: GenerationJobStatus.RUNNING,
        provider: "gemini",
        model: "fixture-model",
        promptVersion: "skill-mcq-v0",
        requestedCount: 3,
        startedAt: staleAt,
      },
    });
    await prisma.skillDraftBatchItem.update({
      where: { id: ready.items[0].id },
      data: {
        status: SkillDraftBatchItemStatus.ACTIVATING,
        generationClaimId: "abandoned-activation-claim",
        updatedAt: staleAt,
      },
    });

    expect(await getMaterialDraftBatch({ userId, batchId: ready.id })).toMatchObject({
      failedCount: 1,
      readyCount: 0,
      items: [
        {
          status: SkillDraftBatchItemStatus.FAILED,
          errorCode: "ACTIVATION_RETRYABLE_STALE_CLAIM",
        },
      ],
    });
    await expect(
      prisma.skillDraftBatchItem.findUnique({ where: { id: ready.items[0].id } }),
    ).resolves.toMatchObject({ generationClaimId: null });
    await expect(
      prisma.generationJob.findUnique({ where: { id: job.id } }),
    ).resolves.toMatchObject({
      status: GenerationJobStatus.FAILED,
      errorMessage: ACTIVATION_SUPERSEDED_JOB_MESSAGE,
    });
  });

  it("permanently fences a stale worker before retrying its activation", async () => {
    const ready = await createReadyBatch([
      {
        key: "stale-activation-retry-fence",
        title: "Stale activation retry fence fixture",
        objective: "Choose a direct object pronoun while a stale activation is retried.",
        materialSectionIds: [directSectionId],
        evidenceChunkIds: [directChunkId],
      },
    ]);
    const skillId = ready.items[0].skill?.id;
    if (!skillId) {
      throw new Error("expected a stale activation retry fixture skill");
    }
    const queuedEvents: MaterialBatchActivationEvent[] = [];
    await queueMaterialBatchActivation({
      userId,
      input: { batchId: ready.id, itemIds: [ready.items[0].id] },
      now: new Date(),
      eventSender: {
        async sendMaterialBatchActivationRequested(payload) {
          queuedEvents.push(payload);
        },
      },
    });
    if (!queuedEvents[0]) {
      throw new Error("expected an initial stale activation event");
    }

    let signalStaleWorkerStarted: (() => void) | undefined;
    const staleWorkerStarted = new Promise<void>((resolve) => {
      signalStaleWorkerStarted = resolve;
    });
    let releaseStaleWorker: (() => void) | undefined;
    const staleWorkerCanFail = new Promise<void>((resolve) => {
      releaseStaleWorker = resolve;
    });
    const staleWorker = runMaterialBatchActivationJob({
      ...queuedEvents[0],
      now: new Date(),
      generateChoiceExercises: async () => {
        signalStaleWorkerStarted?.();
        await staleWorkerCanFail;
        throw new Error("late stale activation failure");
      },
      verifyChoiceExercises: acceptAllChoiceExercises,
      model: "stale-worker-model",
      sourceEvidenceLoader,
    });
    await staleWorkerStarted;

    const staleAt = new Date(Date.now() - 6 * 60 * 1_000);
    await prisma.$transaction([
      prisma.generationJob.update({
        where: { id: queuedEvents[0].generationJobId },
        data: { startedAt: staleAt },
      }),
      prisma.skillDraftBatchItem.update({
        where: { id: ready.items[0].id },
        data: { updatedAt: staleAt },
      }),
    ]);
    await expect(
      getMaterialDraftBatch({ userId, batchId: ready.id }),
    ).resolves.toMatchObject({
      items: [
        {
          status: SkillDraftBatchItemStatus.FAILED,
          errorCode: "ACTIVATION_RETRYABLE_STALE_CLAIM",
        },
      ],
    });
    await expect(
      prisma.generationJob.findUniqueOrThrow({
        where: { id: queuedEvents[0].generationJobId },
      }),
    ).resolves.toMatchObject({
      status: GenerationJobStatus.FAILED,
      errorMessage: ACTIVATION_SUPERSEDED_JOB_MESSAGE,
    });

    const retryEvents: MaterialBatchActivationEvent[] = [];
    await expect(
      retryMaterialBatchActivationItem({
        userId,
        batchId: ready.id,
        itemId: ready.items[0].id,
        now: new Date(),
        eventSender: {
          async sendMaterialBatchActivationRequested(payload) {
            retryEvents.push(payload);
          },
        },
      }),
    ).resolves.toMatchObject({ status: "queued" });
    expect(retryEvents).toEqual([
      expect.objectContaining({
        itemId: ready.items[0].id,
      }),
    ]);
    expect(retryEvents[0].generationJobId).not.toBe(
      queuedEvents[0].generationJobId,
    );

    let signalReplacementStarted: (() => void) | undefined;
    const replacementStarted = new Promise<void>((resolve) => {
      signalReplacementStarted = resolve;
    });
    let releaseReplacement: (() => void) | undefined;
    const replacementCanFinish = new Promise<void>((resolve) => {
      releaseReplacement = resolve;
    });
    const replacementWorker = runMaterialBatchActivationJob({
      ...retryEvents[0],
      now: new Date(),
      generateChoiceExercises: async () => {
        signalReplacementStarted?.();
        await replacementCanFinish;
        return {
          exercises: [
            generatedChoiceExercise(61),
            generatedChoiceExercise(62),
            generatedChoiceExercise(63),
          ],
        };
      },
      verifyChoiceExercises: acceptAllChoiceExercises,
      model: "replacement-worker-model",
      sourceEvidenceLoader,
    });
    await replacementStarted;
    const replacementJob = await prisma.generationJob.findFirstOrThrow({
      where: {
        userId,
        skillId,
        kind: GenerationJobKind.SKILL_ACTIVATION,
        status: GenerationJobStatus.RUNNING,
        id: { not: queuedEvents[0].generationJobId },
      },
    });
    const replacementItem = await prisma.skillDraftBatchItem.findUniqueOrThrow({
      where: { id: ready.items[0].id },
    });

    releaseStaleWorker?.();
    await expect(staleWorker).resolves.toMatchObject({ status: "not-claimed" });
    await expect(
      prisma.generationJob.findUniqueOrThrow({ where: { id: replacementJob.id } }),
    ).resolves.toMatchObject({ status: GenerationJobStatus.RUNNING });
    await expect(
      prisma.skillDraftBatchItem.findUniqueOrThrow({
        where: { id: ready.items[0].id },
      }),
    ).resolves.toMatchObject({
      status: SkillDraftBatchItemStatus.ACTIVATING,
      generationClaimId: replacementItem.generationClaimId,
    });

    releaseReplacement?.();
    await expect(replacementWorker).resolves.toMatchObject({
      status: "active",
      skillId,
      exerciseCount: 3,
    });
    await expect(
      prisma.exercise.count({ where: { userId, skillId } }),
    ).resolves.toBe(3);
    await expect(
      prisma.skillDraftBatchItem.findUniqueOrThrow({
        where: { id: ready.items[0].id },
      }),
    ).resolves.toMatchObject({ status: SkillDraftBatchItemStatus.ACTIVE });
    const jobs = await prisma.generationJob.findMany({
      where: { userId, skillId, kind: GenerationJobKind.SKILL_ACTIVATION },
      orderBy: { createdAt: "asc" },
    });
    expect(jobs).toHaveLength(2);
    expect(jobs.find((job) => job.id === queuedEvents[0].generationJobId)).toMatchObject({
      status: GenerationJobStatus.FAILED,
      errorMessage: ACTIVATION_SUPERSEDED_JOB_MESSAGE,
    });
    expect(jobs.find((job) => job.id === replacementJob.id)).toMatchObject({
      status: GenerationJobStatus.SUCCEEDED,
    });
  });

  it("keeps successful activations when a sibling fails and retries only the failed item", async () => {
    const ready = await createReadyBatch([
      {
        key: "activation-success",
        title: "Activation success fixture",
        objective: "Choose a direct object pronoun in one short sentence.",
        materialSectionIds: [directSectionId],
        evidenceChunkIds: [directChunkId],
      },
      {
        key: "activation-retry",
        title: "Activation retry fixture",
        objective: "Choose le or les for one recipient phrase.",
        materialSectionIds: [indirectSectionId],
        evidenceChunkIds: [indirectChunkId],
      },
    ]);
    const events: MaterialBatchActivationEvent[] = [];
    await queueMaterialBatchActivation({
      userId,
      input: { batchId: ready.id, itemIds: ready.items.map((item) => item.id) },
      now: new Date("2026-07-09T13:00:00.000Z"),
      eventSender: {
        async sendMaterialBatchActivationRequested(payload) {
          events.push(payload);
        },
      },
    });
    const firstEvent = events.find((event) => event.itemId === ready.items[0].id);
    const secondEvent = events.find((event) => event.itemId === ready.items[1].id);
    if (!firstEvent || !secondEvent) {
      throw new Error("expected two activation events");
    }

    await runMaterialBatchActivationJob({
      userId,
      batchId: ready.id,
      itemId: firstEvent.itemId,
      generationJobId: firstEvent.generationJobId,
      now: new Date("2026-07-09T13:01:00.000Z"),
      generateChoiceExercises: async () => ({
        exercises: [
          generatedChoiceExercise(4),
          generatedChoiceExercise(5),
          generatedChoiceExercise(6),
        ],
      }),
      verifyChoiceExercises: acceptAllChoiceExercises,
      model: "fixture-model",
      sourceEvidenceLoader,
    });
    await expect(
      runMaterialBatchActivationJob({
        userId,
        batchId: ready.id,
        itemId: secondEvent.itemId,
        generationJobId: secondEvent.generationJobId,
        now: new Date("2026-07-09T13:01:00.000Z"),
        generateChoiceExercises: async () => {
          throw new Error("temporary fixture outage");
        },
        verifyChoiceExercises: acceptAllChoiceExercises,
        model: "fixture-model",
        sourceEvidenceLoader,
      }),
    ).rejects.toBeInstanceOf(MaterialBatchActivationError);

    expect(await getMaterialDraftBatch({ userId, batchId: ready.id })).toMatchObject({
      status: SkillDraftBatchStatus.PARTIAL,
      activatedCount: 1,
      failedCount: 1,
    });
    const retryEvents: typeof events = [];
    expect(
      await retryMaterialBatchActivationItem({
        userId,
        batchId: ready.id,
        itemId: secondEvent.itemId,
        now: new Date("2026-07-09T13:02:00.000Z"),
        eventSender: {
          async sendMaterialBatchActivationRequested(payload) {
            retryEvents.push(payload);
          },
        },
      }),
    ).toMatchObject({ status: "queued" });
    expect(retryEvents).toEqual([
      expect.objectContaining({
        itemId: secondEvent.itemId,
      }),
    ]);
    expect(retryEvents[0].generationJobId).not.toBe(
      secondEvent.generationJobId,
    );

    await runMaterialBatchActivationJob({
      ...retryEvents[0],
      now: new Date("2026-07-09T13:03:00.000Z"),
      generateChoiceExercises: async () => ({
        exercises: [
          generatedChoiceExercise(7),
          generatedChoiceExercise(8),
          generatedChoiceExercise(9),
        ],
      }),
      verifyChoiceExercises: acceptAllChoiceExercises,
      model: "fixture-model",
      sourceEvidenceLoader,
    });
    expect(await getMaterialDraftBatch({ userId, batchId: ready.id })).toMatchObject({
      status: SkillDraftBatchStatus.COMPLETE,
      activatedCount: 2,
      failedCount: 0,
    });
    await expect(
      prisma.generationJob.count({
        where: {
          userId,
          skillId: ready.items[1].skill?.id,
          kind: GenerationJobKind.SKILL_ACTIVATION,
        },
      }),
    ).resolves.toBe(2);
  });

  it("keeps a transient activation failure in progress while AWS background jobs retries remain", async () => {
    const ready = await createReadyBatch([
      {
        key: "automatic-activation-retry",
        title: "Automatic activation retry fixture",
        objective: "Choose a direct object pronoun after a transient provider failure.",
        materialSectionIds: [directSectionId],
        evidenceChunkIds: [directChunkId],
      },
    ]);
    const events: MaterialBatchActivationEvent[] = [];
    await queueMaterialBatchActivation({
      userId,
      input: { batchId: ready.id, itemIds: [ready.items[0].id] },
      now: new Date("2026-07-21T12:00:00.000Z"),
      eventSender: {
        async sendMaterialBatchActivationRequested(payload) {
          events.push(payload);
        },
      },
    });

    await expect(
      runMaterialBatchActivationJob({
        ...events[0],
        attempt: 0,
        maxAttempts: 4,
        now: new Date("2026-07-21T12:01:00.000Z"),
        generateChoiceExercises: async () => {
          throw new Error("temporary provider timeout");
        },
        verifyChoiceExercises: acceptAllChoiceExercises,
        model: "fixture-model",
        sourceEvidenceLoader,
      }),
    ).rejects.toBeInstanceOf(MaterialBatchActivationError);

    expect(await getMaterialDraftBatch({ userId, batchId: ready.id })).toMatchObject({
      status: SkillDraftBatchStatus.ACTIVATING,
      failedCount: 0,
      items: [
        {
          status: SkillDraftBatchItemStatus.ACTIVATING,
          errorCode: "ACTIVATION_RETRYING_TRANSIENT_FAILURE",
        },
      ],
    });

    await expect(
      runMaterialBatchActivationJob({
        ...events[0],
        attempt: 1,
        maxAttempts: 4,
        now: new Date("2026-07-21T12:02:00.000Z"),
        generateChoiceExercises: async () => ({
          exercises: [
            generatedChoiceExercise(41),
            generatedChoiceExercise(42),
            generatedChoiceExercise(43),
          ],
        }),
        verifyChoiceExercises: acceptAllChoiceExercises,
        model: "fixture-model",
        sourceEvidenceLoader,
      }),
    ).resolves.toMatchObject({ status: "active" });

    expect(await getMaterialDraftBatch({ userId, batchId: ready.id })).toMatchObject({
      status: SkillDraftBatchStatus.COMPLETE,
      failedCount: 0,
      activatedCount: 1,
      items: [{ status: SkillDraftBatchItemStatus.ACTIVE, errorCode: null }],
    });
  });

  it("keeps queued siblings when one activation event cannot be sent", async () => {
    const ready = await createReadyBatch([
      {
        key: "event-success",
        title: "Activation event success fixture",
        objective: "Choose a direct object pronoun after an event is queued.",
        materialSectionIds: [directSectionId],
        evidenceChunkIds: [directChunkId],
      },
      {
        key: "event-failure",
        title: "Activation event failure fixture",
        objective: "Choose an indirect object pronoun after an event retry.",
        materialSectionIds: [indirectSectionId],
        evidenceChunkIds: [indirectChunkId],
      },
    ]);
    const result = await queueMaterialBatchActivation({
      userId,
      input: { batchId: ready.id, itemIds: ready.items.map((item) => item.id) },
      now: new Date("2026-07-09T13:30:00.000Z"),
      eventSender: {
        async sendMaterialBatchActivationRequested(payload) {
          if (payload.itemId === ready.items[1].id) {
            throw new Error("fixture event transport failure");
          }
        },
      },
    });

    expect(result).toMatchObject({
      status: "partial",
      queuedItemIds: [ready.items[0].id],
      failedItemIds: [ready.items[1].id],
    });
    const batch = await getMaterialDraftBatch({ userId, batchId: ready.id });
    expect(batch).toMatchObject({
      status: SkillDraftBatchStatus.ACTIVATING,
      failedCount: 1,
      items: [
        { status: SkillDraftBatchItemStatus.ACTIVATING },
        {
          status: SkillDraftBatchItemStatus.FAILED,
          errorCode: "ACTIVATION_EVENT_SEND_FAILED",
        },
      ],
    });
    const jobs = await prisma.generationJob.findMany({
      where: {
        userId,
        skillId: { in: ready.items.flatMap((item) => (item.skill ? [item.skill.id] : [])) },
        kind: GenerationJobKind.SKILL_ACTIVATION,
      },
    });
    const statusBySkillId = new Map(jobs.map((job) => [job.skillId, job.status]));
    expect(statusBySkillId.get(ready.items[0].skill?.id ?? "missing")).toBe(
      GenerationJobStatus.PENDING,
    );
    expect(statusBySkillId.get(ready.items[1].skill?.id ?? "missing")).toBe(
      GenerationJobStatus.FAILED,
    );
  });

  it("preserves a claimed activation when the event sender reports a late failure", async () => {
    const ready = await createReadyBatch([
      {
        key: "late-event-failure",
        title: "Late activation acknowledgement fixture",
        objective: "Choose a direct object pronoun after a delayed event acknowledgement.",
        materialSectionIds: [directSectionId],
        evidenceChunkIds: [directChunkId],
      },
    ]);
    let signalWorkerStarted: (() => void) | undefined;
    const workerStarted = new Promise<void>((resolve) => {
      signalWorkerStarted = resolve;
    });
    let releaseWorker: (() => void) | undefined;
    const workerCanFinish = new Promise<void>((resolve) => {
      releaseWorker = resolve;
    });
    let worker:
      | ReturnType<typeof runMaterialBatchActivationJob>
      | undefined;

    const result = await queueMaterialBatchActivation({
      userId,
      input: { batchId: ready.id, itemIds: [ready.items[0].id] },
      now: new Date("2032-02-01T13:40:00.000Z"),
      eventSender: {
        async sendMaterialBatchActivationRequested(payload) {
          worker = runMaterialBatchActivationJob({
            ...payload,
            now: new Date("2032-02-01T13:40:01.000Z"),
            generateChoiceExercises: async () => {
              signalWorkerStarted?.();
              await workerCanFinish;
              return {
                exercises: [
                  generatedChoiceExercise(81),
                  generatedChoiceExercise(82),
                  generatedChoiceExercise(83),
                ],
              };
            },
            verifyChoiceExercises: acceptAllChoiceExercises,
            model: "fixture-model",
            sourceEvidenceLoader,
          });
          await workerStarted;
          throw new Error("fixture acknowledgement failed after delivery");
        },
      },
    });

    expect(result).toMatchObject({
      status: "queued",
      queuedItemIds: [ready.items[0].id],
      failedItemIds: [],
    });
    await expect(
      prisma.skillDraftBatchItem.findUniqueOrThrow({
        where: { id: ready.items[0].id },
      }),
    ).resolves.toMatchObject({
      status: SkillDraftBatchItemStatus.ACTIVATING,
      generationClaimId: expect.any(String),
    });
    if (!worker) {
      throw new Error("expected the activation worker to start");
    }
    releaseWorker?.();
    await expect(worker).resolves.toMatchObject({ status: "active" });
    await expect(
      prisma.skillDraftBatchItem.findUniqueOrThrow({
        where: { id: ready.items[0].id },
      }),
    ).resolves.toMatchObject({
      status: SkillDraftBatchItemStatus.ACTIVE,
      generationClaimId: null,
    });
    await prisma.skill.delete({
      where: { id: ready.items[0].skill!.id },
    });
  });

  it("preserves a claimed retry when the event sender reports a late failure", async () => {
    const ready = await createReadyBatch([
      {
        key: "late-retry-event-failure",
        title: "Late retry acknowledgement fixture",
        objective: "Choose an indirect object pronoun after a delayed retry acknowledgement.",
        materialSectionIds: [indirectSectionId],
        evidenceChunkIds: [indirectChunkId],
      },
    ]);
    const events: MaterialBatchActivationEvent[] = [];
    await queueMaterialBatchActivation({
      userId,
      input: { batchId: ready.id, itemIds: [ready.items[0].id] },
      now: new Date("2032-02-02T13:45:00.000Z"),
      eventSender: {
        async sendMaterialBatchActivationRequested(payload) {
          events.push(payload);
        },
      },
    });
    await prisma.$transaction([
      prisma.skillDraftBatchItem.update({
        where: { id: ready.items[0].id },
        data: {
          status: SkillDraftBatchItemStatus.FAILED,
          generationClaimId: null,
          errorCode: "ACTIVATION_RETRYABLE_FIXTURE",
          errorMessage: "Retry fixture.",
        },
      }),
      prisma.generationJob.update({
        where: { id: events[0].generationJobId },
        data: {
          status: GenerationJobStatus.FAILED,
          errorMessage: "Retry fixture.",
          completedAt: new Date("2032-02-02T13:45:30.000Z"),
        },
      }),
    ]);
    let signalWorkerStarted: (() => void) | undefined;
    const workerStarted = new Promise<void>((resolve) => {
      signalWorkerStarted = resolve;
    });
    let releaseWorker: (() => void) | undefined;
    const workerCanFinish = new Promise<void>((resolve) => {
      releaseWorker = resolve;
    });
    let worker:
      | ReturnType<typeof runMaterialBatchActivationJob>
      | undefined;
    let retryGenerationJobId: string | undefined;

    const result = await retryMaterialBatchActivationItem({
      userId,
      batchId: ready.id,
      itemId: ready.items[0].id,
      now: new Date("2032-02-02T13:46:00.000Z"),
      eventSender: {
        async sendMaterialBatchActivationRequested(payload) {
          retryGenerationJobId = payload.generationJobId;
          worker = runMaterialBatchActivationJob({
            ...payload,
            now: new Date("2032-02-02T13:46:01.000Z"),
            generateChoiceExercises: async () => {
              signalWorkerStarted?.();
              await workerCanFinish;
              return {
                exercises: [
                  generatedChoiceExercise(84),
                  generatedChoiceExercise(85),
                  generatedChoiceExercise(86),
                ],
              };
            },
            verifyChoiceExercises: acceptAllChoiceExercises,
            model: "fixture-model",
            sourceEvidenceLoader,
          });
          await workerStarted;
          throw new Error("fixture retry acknowledgement failed after delivery");
        },
      },
    });

    if (!worker || !retryGenerationJobId) {
      throw new Error("expected the retry worker to start");
    }
    const claimedRetryGenerationJobId = retryGenerationJobId;
    expect(result).toMatchObject({
      status: "already-running",
      itemId: ready.items[0].id,
      generationJobId: claimedRetryGenerationJobId,
    });
    expect(claimedRetryGenerationJobId).not.toBe(
      events[0].generationJobId,
    );
    await expect(
      prisma.skillDraftBatchItem.findUniqueOrThrow({
        where: { id: ready.items[0].id },
      }),
    ).resolves.toMatchObject({
      status: SkillDraftBatchItemStatus.ACTIVATING,
      generationClaimId: expect.any(String),
    });
    releaseWorker?.();
    await expect(worker).resolves.toMatchObject({ status: "active" });
    await expect(
      prisma.generationJob.findUniqueOrThrow({
        where: { id: claimedRetryGenerationJobId },
      }),
    ).resolves.toMatchObject({ status: GenerationJobStatus.SUCCEEDED });
    await prisma.skill.delete({
      where: { id: ready.items[0].skill!.id },
    });
  });

  it("does not let an old sender failure cancel a newer retry reservation", async () => {
    const ready = await createReadyBatch([
      {
        key: "old-sender-new-retry",
        title: "Delayed sender retry identity fixture",
        objective: "Choose a direct object pronoun after a delayed sender failure.",
        materialSectionIds: [directSectionId],
        evidenceChunkIds: [directChunkId],
      },
    ]);
    let originalEvent:
      | { itemId: string; generationJobId: string }
      | undefined;
    let signalOriginalWorkerFailed: (() => void) | undefined;
    const originalWorkerFailed = new Promise<void>((resolve) => {
      signalOriginalWorkerFailed = resolve;
    });
    let releaseOriginalSender: (() => void) | undefined;
    const originalSenderCanReject = new Promise<void>((resolve) => {
      releaseOriginalSender = resolve;
    });
    const originalQueue = queueMaterialBatchActivation({
      userId,
      input: { batchId: ready.id, itemIds: [ready.items[0].id] },
      now: new Date("2032-02-03T13:50:00.000Z"),
      eventSender: {
        async sendMaterialBatchActivationRequested(payload) {
          originalEvent = payload;
          await expect(
            runMaterialBatchActivationJob({
              ...payload,
              now: new Date("2032-02-03T13:50:01.000Z"),
              generateChoiceExercises: async () => {
                throw new Error("fixture worker failure before acknowledgement");
              },
              verifyChoiceExercises: acceptAllChoiceExercises,
              model: "fixture-model",
              sourceEvidenceLoader,
            }),
          ).rejects.toBeInstanceOf(MaterialBatchActivationError);
          signalOriginalWorkerFailed?.();
          await originalSenderCanReject;
          throw new Error("fixture original sender rejected late");
        },
      },
    });
    const originalStart = await Promise.race([
      originalWorkerFailed.then(() => ({ status: "worker-started" as const })),
      originalQueue.then((result) => ({
        status: "queue-finished" as const,
        result,
      })),
    ]);
    if (originalStart.status === "queue-finished") {
      throw new Error(
        `expected the original worker to start, but queue finished with ${originalStart.result.status}`,
      );
    }
    if (!originalEvent) {
      throw new Error("expected the original activation event");
    }

    const retryEvents: MaterialBatchActivationEvent[] = [];
    await expect(
      retryMaterialBatchActivationItem({
        userId,
        batchId: ready.id,
        itemId: ready.items[0].id,
        now: new Date("2032-02-03T13:51:00.000Z"),
        eventSender: {
          async sendMaterialBatchActivationRequested(payload) {
            retryEvents.push(payload);
          },
        },
      }),
    ).resolves.toMatchObject({ status: "queued" });
    expect(retryEvents).toHaveLength(1);
    expect(retryEvents[0].generationJobId).not.toBe(
      originalEvent.generationJobId,
    );

    releaseOriginalSender?.();
    await expect(originalQueue).resolves.toMatchObject({
      status: "queued",
      failedItemIds: [],
    });
    await expect(
      prisma.skillDraftBatchItem.findUniqueOrThrow({
        where: { id: ready.items[0].id },
      }),
    ).resolves.toMatchObject({
      status: SkillDraftBatchItemStatus.ACTIVATING,
      generationClaimId: null,
    });
    await expect(
      prisma.generationJob.findUniqueOrThrow({
        where: { id: retryEvents[0].generationJobId },
      }),
    ).resolves.toMatchObject({ status: GenerationJobStatus.PENDING });

    await expect(
      runMaterialBatchActivationJob({
        ...retryEvents[0],
        now: new Date("2032-02-03T13:52:00.000Z"),
        generateChoiceExercises: async () => ({
          exercises: [
            generatedChoiceExercise(87),
            generatedChoiceExercise(88),
            generatedChoiceExercise(89),
          ],
        }),
        verifyChoiceExercises: acceptAllChoiceExercises,
        model: "fixture-model",
        sourceEvidenceLoader,
      }),
    ).resolves.toMatchObject({ status: "active" });
    await prisma.skill.delete({
      where: { id: ready.items[0].skill!.id },
    });
  });

  it("fails only the fresh retry reservation when retry event delivery fails", async () => {
    const ready = await createReadyBatch([
      {
        key: "retry-send-failure",
        title: "Retry send failure fixture",
        objective: "Choose an indirect object pronoun after an event delivery retry.",
        materialSectionIds: [indirectSectionId],
        evidenceChunkIds: [indirectChunkId],
      },
    ]);
    const originalEvents: MaterialBatchActivationEvent[] = [];
    await expect(queueMaterialBatchActivation({
      userId,
      input: { batchId: ready.id, itemIds: [ready.items[0].id] },
      now: new Date("2032-02-04T13:55:00.000Z"),
      eventSender: {
        async sendMaterialBatchActivationRequested(payload) {
          originalEvents.push(payload);
        },
      },
    })).resolves.toMatchObject({ status: "queued" });
    expect(originalEvents).toHaveLength(1);
    await prisma.$transaction([
      prisma.skillDraftBatchItem.update({
        where: { id: ready.items[0].id },
        data: {
          status: SkillDraftBatchItemStatus.FAILED,
          generationClaimId: null,
          errorCode: "ACTIVATION_RETRYABLE_FIXTURE",
          errorMessage: "Retry fixture.",
        },
      }),
      prisma.generationJob.update({
        where: { id: originalEvents[0].generationJobId },
        data: {
          status: GenerationJobStatus.FAILED,
          errorMessage: "Retry fixture.",
          completedAt: new Date("2032-02-04T13:55:30.000Z"),
        },
      }),
    ]);
    let rejectedRetryJobId: string | undefined;

    await expect(
      retryMaterialBatchActivationItem({
        userId,
        batchId: ready.id,
        itemId: ready.items[0].id,
        now: new Date("2032-02-04T13:56:00.000Z"),
        eventSender: {
          async sendMaterialBatchActivationRequested(payload) {
            rejectedRetryJobId = payload.generationJobId;
            throw new Error("fixture retry event delivery failure");
          },
        },
      }),
    ).resolves.toMatchObject({
      status: "not-queued",
      message: "Activation could not be queued.",
    });
    if (!rejectedRetryJobId) {
      throw new Error("expected a rejected retry reservation");
    }
    expect(rejectedRetryJobId).not.toBe(originalEvents[0].generationJobId);
    await expect(
      prisma.skillDraftBatchItem.findUniqueOrThrow({
        where: { id: ready.items[0].id },
      }),
    ).resolves.toMatchObject({
      status: SkillDraftBatchItemStatus.FAILED,
      generationClaimId: null,
      errorCode: "ACTIVATION_EVENT_SEND_FAILED",
    });
    await expect(
      prisma.generationJob.findUniqueOrThrow({
        where: { id: rejectedRetryJobId },
      }),
    ).resolves.toMatchObject({ status: GenerationJobStatus.FAILED });

    const nextRetryEvents: MaterialBatchActivationEvent[] = [];
    await expect(
      retryMaterialBatchActivationItem({
        userId,
        batchId: ready.id,
        itemId: ready.items[0].id,
        now: new Date("2032-02-04T13:57:00.000Z"),
        eventSender: {
          async sendMaterialBatchActivationRequested(payload) {
            nextRetryEvents.push(payload);
          },
        },
      }),
    ).resolves.toMatchObject({ status: "queued" });
    expect(nextRetryEvents).toHaveLength(1);
    expect(nextRetryEvents[0].generationJobId).not.toBe(rejectedRetryJobId);
    await expect(
      runMaterialBatchActivationJob({
        ...nextRetryEvents[0],
        now: new Date("2032-02-04T13:58:00.000Z"),
        generateChoiceExercises: async () => ({
          exercises: [
            generatedChoiceExercise(90),
            generatedChoiceExercise(91),
            generatedChoiceExercise(92),
          ],
        }),
        verifyChoiceExercises: acceptAllChoiceExercises,
        model: "fixture-model",
        sourceEvidenceLoader,
      }),
    ).resolves.toMatchObject({ status: "active" });
    await prisma.skill.delete({
      where: { id: ready.items[0].skill!.id },
    });
  });

  it("enforces the daily activation-attempt limit before reserving a retry", async () => {
    const ready = await createReadyBatch([
      {
        key: "retry-daily-limit",
        title: "Retry daily activation limit fixture",
        objective: "Choose a direct object pronoun at the retry-attempt limit.",
        materialSectionIds: [directSectionId],
        evidenceChunkIds: [directChunkId],
      },
    ]);
    const originalEvents: MaterialBatchActivationEvent[] = [];
    const retryDayStart = new Date("2032-02-05T00:00:00.000Z");
    const retryDayEnd = new Date("2032-02-06T00:00:00.000Z");
    await expect(
      queueMaterialBatchActivation({
        userId,
        input: { batchId: ready.id, itemIds: [ready.items[0].id] },
        now: new Date("2032-02-05T10:00:00.000Z"),
        eventSender: {
          async sendMaterialBatchActivationRequested(payload) {
            originalEvents.push(payload);
          },
        },
      }),
    ).resolves.toMatchObject({ status: "queued" });
    expect(originalEvents).toHaveLength(1);
    await prisma.$transaction([
      prisma.skillDraftBatchItem.update({
        where: { id: ready.items[0].id },
        data: {
          status: SkillDraftBatchItemStatus.FAILED,
          generationClaimId: null,
          errorCode: "ACTIVATION_RETRYABLE_FIXTURE",
          errorMessage: "Retry fixture.",
        },
      }),
      prisma.generationJob.update({
        where: { id: originalEvents[0].generationJobId },
        data: {
          status: GenerationJobStatus.FAILED,
          errorMessage: "Retry fixture.",
          completedAt: new Date("2032-02-05T10:00:30.000Z"),
        },
      }),
    ]);
    const retryQuotaPrefix = `Retry daily quota ${randomUUID()}`;
    const existingAttempts = await prisma.generationJob.count({
      where: {
        userId,
        kind: GenerationJobKind.SKILL_ACTIVATION,
        createdAt: { gte: retryDayStart, lt: retryDayEnd },
      },
    });
    for (
      let index = existingAttempts;
      index < ALPHA_SKILL_ACTIVATIONS_PER_DAY;
      index += 1
    ) {
      const fillerSkill = await prisma.skill.create({
        data: {
          userId,
          title: `${retryQuotaPrefix} ${index}`,
          tags: [],
          status: SkillStatus.DRAFT,
        },
      });
      await prisma.generationJob.create({
        data: {
          userId,
          skillId: fillerSkill.id,
          kind: GenerationJobKind.SKILL_ACTIVATION,
          status: GenerationJobStatus.FAILED,
          provider: "google",
          model: "fixture-model",
          promptVersion: "skill-mcq-v0",
          requestedCount: 5,
          errorMessage: "retry quota fixture",
          createdAt: new Date(retryDayStart.getTime() + (index + 1) * 60_000),
        },
      });
    }
    const retrySender = vi.fn(async () => {});

    await expect(
      retryMaterialBatchActivationItem({
        userId,
        batchId: ready.id,
        itemId: ready.items[0].id,
        now: new Date("2032-02-05T12:00:00.000Z"),
        eventSender: {
          sendMaterialBatchActivationRequested: retrySender,
        },
      }),
    ).resolves.toMatchObject({
      status: "limited",
      code: "daily-activation-limit",
      message:
        "No activation attempts remain today. Try this draft again after 2032-02-06T00:00:00.000Z UTC.",
    });
    expect(retrySender).not.toHaveBeenCalled();
    await expect(
      prisma.generationJob.count({
        where: {
          userId,
          kind: GenerationJobKind.SKILL_ACTIVATION,
          createdAt: { gte: retryDayStart, lt: retryDayEnd },
        },
      }),
    ).resolves.toBe(ALPHA_SKILL_ACTIVATIONS_PER_DAY);
    await expect(
      prisma.generationJob.findUniqueOrThrow({
        where: { id: originalEvents[0].generationJobId },
      }),
    ).resolves.toMatchObject({ status: GenerationJobStatus.FAILED });
    await expect(
      prisma.skillDraftBatchItem.findUniqueOrThrow({
        where: { id: ready.items[0].id },
      }),
    ).resolves.toMatchObject({
      status: SkillDraftBatchItemStatus.FAILED,
      generationClaimId: null,
    });

    await prisma.skill.deleteMany({
      where: {
        userId,
        OR: [
          { id: ready.items[0].skill!.id },
          { title: { startsWith: retryQuotaPrefix } },
        ],
      },
    });
  });

  it("serializes concurrent batch reservations so the daily quota cannot be oversubscribed", async () => {
    const first = await createReadyBatch([
      {
        key: "quota-first",
        title: "Quota boundary first fixture",
        objective: "Choose one direct object pronoun at the quota boundary.",
        materialSectionIds: [directSectionId],
        evidenceChunkIds: [directChunkId],
      },
    ]);
    const second = await createReadyBatch([
      {
        key: "quota-second",
        title: "Quota boundary second fixture",
        objective: "Choose one indirect object pronoun at the quota boundary.",
        materialSectionIds: [indirectSectionId],
        evidenceChunkIds: [indirectChunkId],
      },
    ]);
    expect(
      await queueMaterialBatchActivation({
        userId: otherUserId,
        input: { batchId: first.id, itemIds: [first.items[0].id] },
        now: new Date("2030-01-15T14:00:00.000Z"),
        eventSender: { async sendMaterialBatchActivationRequested() {} },
      }),
    ).toMatchObject({ status: "not-found" });

    const dayStart = new Date("2030-01-15T00:00:00.000Z");
    const existingCount = await prisma.generationJob.count({
      where: {
        userId,
        kind: GenerationJobKind.SKILL_ACTIVATION,
        createdAt: { gte: dayStart },
      },
    });
    for (
      let index = existingCount;
      index < ALPHA_SKILL_ACTIVATIONS_PER_DAY - 1;
      index += 1
    ) {
      const skill = await prisma.skill.create({
        data: {
          userId,
          title: `Quota reservation fixture ${index}`,
          tags: [],
          status: SkillStatus.DRAFT,
        },
      });
      await prisma.generationJob.create({
        data: {
          userId,
          skillId: skill.id,
          kind: GenerationJobKind.SKILL_ACTIVATION,
          status: GenerationJobStatus.FAILED,
          provider: "google",
          model: "fixture-model",
          promptVersion: "skill-mcq-v0",
          requestedCount: 5,
          errorMessage: "quota fixture",
          createdAt: new Date(dayStart.getTime() + (index + 1) * 60_000),
        },
      });
    }

    const results = await Promise.all([
      queueMaterialBatchActivation({
        userId,
        input: { batchId: first.id, itemIds: [first.items[0].id] },
        now: new Date("2030-01-15T14:00:00.000Z"),
        eventSender: { async sendMaterialBatchActivationRequested() {} },
      }),
      queueMaterialBatchActivation({
        userId,
        input: { batchId: second.id, itemIds: [second.items[0].id] },
        now: new Date("2030-01-15T14:00:00.000Z"),
        eventSender: { async sendMaterialBatchActivationRequested() {} },
      }),
    ]);

    expect(results.map((result) => result.status).sort()).toEqual(["limited", "queued"]);
    await expect(
      prisma.generationJob.count({
        where: {
          userId,
          kind: GenerationJobKind.SKILL_ACTIVATION,
          createdAt: { gte: dayStart },
        },
      }),
    ).resolves.toBe(ALPHA_SKILL_ACTIVATIONS_PER_DAY);
  });

  it("skips a ready batch item whose skill was already activated elsewhere", async () => {
    const ready = await createReadyBatch([
      {
        key: "already-active-selection",
        title: "Already active selection fixture",
        objective: "Choose a direct object pronoun in one short sentence.",
        materialSectionIds: [directSectionId],
        evidenceChunkIds: [directChunkId],
      },
      {
        key: "remaining-ready-selection",
        title: "Remaining ready selection fixture",
        objective: "Choose an indirect object pronoun in one short sentence.",
        materialSectionIds: [indirectSectionId],
        evidenceChunkIds: [indirectChunkId],
      },
    ]);
    const activeSkillId = ready.items[0].skill?.id;
    if (!activeSkillId) {
      throw new Error("expected the first batch skill");
    }
    await prisma.skill.update({
      where: { id: activeSkillId },
      data: { status: SkillStatus.ACTIVE },
    });
    const events: MaterialBatchActivationEvent[] = [];

    const result = await queueMaterialBatchActivation({
      userId,
      input: { batchId: ready.id, itemIds: ready.items.map((item) => item.id) },
      now: new Date("2026-07-10T12:00:00.000Z"),
      eventSender: {
        async sendMaterialBatchActivationRequested(payload) {
          events.push(payload);
        },
      },
    });

    expect(result).toMatchObject({
      status: "queued",
      queuedItemIds: [ready.items[1].id],
    });
    expect(events).toEqual([
      expect.objectContaining({ itemId: ready.items[1].id }),
    ]);
    expect(await getMaterialDraftBatch({ userId, batchId: ready.id })).toMatchObject({
      activatedCount: 1,
      items: [
        { status: SkillDraftBatchItemStatus.ACTIVE },
        { status: SkillDraftBatchItemStatus.ACTIVATING },
      ],
    });

    await prisma.$transaction([
      prisma.skillDraftBatchItem.update({
        where: { id: ready.items[1].id },
        data: {
          status: SkillDraftBatchItemStatus.FAILED,
          errorCode: "ACTIVATION_RETRYABLE_TEST_CLEANUP",
          errorMessage: "released after selection test",
        },
      }),
      prisma.generationJob.update({
        where: { id: events[0].generationJobId },
        data: { status: GenerationJobStatus.FAILED, completedAt: new Date() },
      }),
    ]);
  });

  it("rechecks active-skill capacity before consuming a queued reservation", async () => {
    await resetActivationCapacityFixtures();
    const ready = await createReadyBatch([
      {
        key: "queued-slot-recheck",
        title: "Queued slot recheck fixture",
        objective: "Choose a direct object pronoun after a queued slot is rechecked.",
        materialSectionIds: [directSectionId],
        evidenceChunkIds: [directChunkId],
      },
    ]);
    const usage = await getSkillActivationUsage({
      userId,
      now: new Date("2026-07-12T12:01:00.000Z"),
      prisma,
    });
    const fillerPrefix = `Queued slot recheck filler ${randomUUID()}`;
    await prisma.skill.createMany({
      data: Array.from({ length: Math.max(0, usage.activeSkillLimit - usage.countedSkillCount) }, (_, index) => ({
        userId,
        title: `${fillerPrefix} ${index}`,
        tags: [],
        status: SkillStatus.ARCHIVED,
      })),
    });
    const events: MaterialBatchActivationEvent[] = [];
    expect(
      await queueMaterialBatchActivation({
        userId,
        input: { batchId: ready.id, itemIds: [ready.items[0].id] },
        now: new Date("2026-07-12T12:00:00.000Z"),
        eventSender: {
          async sendMaterialBatchActivationRequested(payload) {
            events.push(payload);
          },
        },
      }),
    ).toMatchObject({ status: "queued" });
    await prisma.skill.updateMany({
      where: { userId, title: { startsWith: fillerPrefix } },
      data: { status: SkillStatus.ACTIVE },
    });
    const generateChoiceExercises = vi.fn(async () => ({
      exercises: [generatedChoiceExercise(31), generatedChoiceExercise(32), generatedChoiceExercise(33)],
    }));

    await expect(
      runMaterialBatchActivationJob({
        userId,
        batchId: ready.id,
        itemId: ready.items[0].id,
        generationJobId: events[0].generationJobId,
        now: new Date("2026-07-12T12:01:00.000Z"),
        generateChoiceExercises,
        verifyChoiceExercises: acceptAllChoiceExercises,
        model: "fixture-model",
      }),
    ).rejects.toBeInstanceOf(MaterialBatchActivationError);
    expect(generateChoiceExercises).not.toHaveBeenCalled();
    expect(await getMaterialDraftBatch({ userId, batchId: ready.id })).toMatchObject({
      failedCount: 1,
      items: [{ status: SkillDraftBatchItemStatus.FAILED }],
    });
    await prisma.skill.deleteMany({ where: { userId, title: { startsWith: fillerPrefix } } });
  });

  it("reserves active-skill slots for queued activations and retry attempts", async () => {
    await resetActivationCapacityFixtures();
    const first = await createReadyBatch([
      {
        key: "active-slot-first",
        title: "Active slot first fixture",
        objective: "Choose a direct object pronoun at the active-skill boundary.",
        materialSectionIds: [directSectionId],
        evidenceChunkIds: [directChunkId],
      },
    ]);
    const second = await createReadyBatch([
      {
        key: "active-slot-second",
        title: "Active slot second fixture",
        objective: "Choose an indirect object pronoun at the active-skill boundary.",
        materialSectionIds: [indirectSectionId],
        evidenceChunkIds: [indirectChunkId],
      },
    ]);
    await prisma.skillDraftBatchItem.updateMany({
      where: { userId, status: SkillDraftBatchItemStatus.ACTIVATING },
      data: {
        status: SkillDraftBatchItemStatus.FAILED,
        errorCode: "ACTIVATION_RETRYABLE_TEST_CLEANUP",
        errorMessage: "released before active-slot test",
      },
    });
    const usage = await getSkillActivationUsage({
      userId,
      now: new Date("2026-07-11T12:00:00.000Z"),
      prisma,
    });
    await prisma.skill.createMany({
      data: Array.from(
        { length: Math.max(0, usage.activeSkillLimit - 1 - usage.countedSkillCount) },
        (_, index) => ({
          userId,
          title: `Active slot filler ${index}`,
          tags: [],
          status: SkillStatus.ACTIVE,
        }),
      ),
    });
    const firstEvents: MaterialBatchActivationEvent[] = [];
    expect(
      await queueMaterialBatchActivation({
        userId,
        input: { batchId: first.id, itemIds: [first.items[0].id] },
        now: new Date("2026-07-11T12:00:00.000Z"),
        eventSender: {
          async sendMaterialBatchActivationRequested(payload) {
            firstEvents.push(payload);
          },
        },
      }),
    ).toMatchObject({ status: "queued" });

    expect(
      await queueMaterialBatchActivation({
        userId,
        input: { batchId: second.id, itemIds: [second.items[0].id] },
        now: new Date("2026-07-11T12:01:00.000Z"),
        eventSender: { async sendMaterialBatchActivationRequested() {} },
      }),
    ).toMatchObject({ status: "limited", code: "active-skill-limit" });

    await prisma.skillDraftBatchItem.update({
      where: { id: first.items[0].id },
      data: {
        status: SkillDraftBatchItemStatus.FAILED,
        errorCode: "ACTIVATION_RETRYABLE_TEST_FAILURE",
        errorMessage: "retry fixture",
      },
    });
    await prisma.generationJob.update({
      where: { id: firstEvents[0].generationJobId },
      data: { status: GenerationJobStatus.FAILED, completedAt: new Date() },
    });
    expect(
      await queueMaterialBatchActivation({
        userId,
        input: { batchId: second.id, itemIds: [second.items[0].id] },
        now: new Date("2026-07-11T12:02:00.000Z"),
        eventSender: { async sendMaterialBatchActivationRequested() {} },
      }),
    ).toMatchObject({ status: "queued" });
    expect(
      await retryMaterialBatchActivationItem({
        userId,
        batchId: first.id,
        itemId: first.items[0].id,
        now: new Date("2026-07-11T12:03:00.000Z"),
        eventSender: { async sendMaterialBatchActivationRequested() {} },
      }),
    ).toMatchObject({ status: "limited" });
    expect(firstEvents).toHaveLength(1);

    const generateChoiceExercises = vi.fn(async () => ({
      exercises: [generatedChoiceExercise(21), generatedChoiceExercise(22), generatedChoiceExercise(23)],
    }));
    await expect(
      runMaterialBatchActivationJob({
        userId,
        batchId: first.id,
        itemId: first.items[0].id,
        generationJobId: firstEvents[0].generationJobId,
        now: new Date("2026-07-11T12:04:00.000Z"),
        generateChoiceExercises,
        verifyChoiceExercises: acceptAllChoiceExercises,
        model: "fixture-model",
      }),
    ).rejects.toBeInstanceOf(MaterialBatchActivationError);
    expect(generateChoiceExercises).not.toHaveBeenCalled();
    expect(
      await prisma.skillDraftBatchItem.findUnique({
        where: { id: first.items[0].id },
        select: { status: true, skill: { select: { status: true } } },
      }),
    ).toEqual({
      status: SkillDraftBatchItemStatus.FAILED,
      skill: { status: SkillStatus.DRAFT },
    });
  });

  async function resetActivationCapacityFixtures() {
    await prisma.skill.updateMany({
      where: { userId, status: { in: [SkillStatus.ACTIVE, SkillStatus.PAUSED] } },
      data: { status: SkillStatus.ARCHIVED },
    });
    await prisma.skillDraftBatchItem.updateMany({
      where: { userId, status: SkillDraftBatchItemStatus.ACTIVATING },
      data: {
        status: SkillDraftBatchItemStatus.FAILED,
        generationClaimId: null,
        errorCode: "ACTIVATION_CAPACITY_TEST_CLEANUP",
        errorMessage: "released before active-capacity test",
      },
    });
    await prisma.generationJob.updateMany({
      where: {
        userId,
        kind: GenerationJobKind.SKILL_ACTIVATION,
        status: { in: [GenerationJobStatus.PENDING, GenerationJobStatus.RUNNING] },
      },
      data: { status: GenerationJobStatus.FAILED, completedAt: new Date() },
    });
  }

  it("resynchronizes a failed batch item whose skill became active elsewhere", async () => {
    const ready = await createReadyBatch([
      {
        key: "failed-active-resync",
        title: "Failed active resync fixture",
        objective: "Choose a direct object pronoun after external activation.",
        materialSectionIds: [directSectionId],
        evidenceChunkIds: [directChunkId],
      },
    ]);
    const skillId = ready.items[0].skill?.id;
    if (!skillId) {
      throw new Error("expected failed active resync skill");
    }
    await prisma.$transaction([
      prisma.skillDraftBatchItem.update({
        where: { id: ready.items[0].id },
        data: {
          status: SkillDraftBatchItemStatus.FAILED,
          generationClaimId: "stale-active-resync-claim",
          errorCode: "ACTIVATION_RETRYABLE_FIXTURE",
          errorMessage: "fixture failure before external activation",
        },
      }),
      prisma.skill.update({
        where: { id: skillId },
        data: { status: SkillStatus.ACTIVE },
      }),
    ]);

    expect(await getMaterialDraftBatch({ userId, batchId: ready.id })).toMatchObject({
      activatedCount: 1,
      failedCount: 0,
      items: [{ status: SkillDraftBatchItemStatus.ACTIVE }],
    });
    await expect(
      prisma.skillDraftBatchItem.findUniqueOrThrow({
        where: { id: ready.items[0].id },
      }),
    ).resolves.toMatchObject({ generationClaimId: null });
  });

  async function createReadyBatch(
    targets: Array<{
      key: string;
      title: string;
      objective: string;
      materialSectionIds: string[];
      evidenceChunkIds: string[];
    }>,
  ) {
    const planned = await planMaterialSkills({
      userId,
      input: {
        materialId,
        materialRevisionId,
        instruction: "Make focused skills from chapter four.",
        idempotencyKey: `${runId}_${randomUUID()}`,
      },
      now: new Date(),
      aiSetup: createAiSetup({
        planScope: async () => ({
          resolutionStatus: "resolved",
          resolvedScopeLabel: "Chapter 4",
          clarification: null,
          warnings: [],
          items: targets,
        }),
      }),
      embeddingGenerator: null,
    });
    if (planned.status !== "planned") {
      throw new Error("expected activation fixture plan");
    }
    await confirmMaterialPlan({
      userId,
      input: { batchId: planned.batchId, plan: planned.plan },
      now: new Date(),
      eventSender: { async sendMaterialDraftItemRequested() {} },
    });
    const pending = await getMaterialDraftBatch({ userId, batchId: planned.batchId });
    if (!pending) {
      throw new Error("expected activation fixture batch");
    }
    for (const item of pending.items) {
      const result = await runMaterialDraftItemJob({
        userId,
        batchId: planned.batchId,
        itemId: item.id,
        aiSetup: createAiSetup({
          draftObjectiveByTitle: new Map(
            targets.map((target) => [target.title, target.objective]),
          ),
        }),
        sourceEvidenceLoader,
      });
      if (result.status !== "ready") {
        throw new Error("expected activation fixture draft");
      }
    }
    const ready = await getMaterialDraftBatch({ userId, batchId: planned.batchId });
    if (!ready || ready.items.some((item) => item.status !== SkillDraftBatchItemStatus.READY)) {
      throw new Error("expected ready activation fixture items");
    }
    return ready;
  }
});

const generatedChoiceExercise = (id: number) => ({
  prompt: `Choose the grounded answer for item ${id}.`,
  choices: [
    { id: "correct", label: "Correct" },
    { id: "close", label: "Close" },
    { id: "wrong", label: "Wrong" },
  ],
  correctChoiceId: "correct",
  explanation: "This answer follows the cited material.",
  difficulty: 2,
  expectedSeconds: 30,
});

const acceptAllChoiceExercises = async (input: {
  candidates: Array<{ candidateId: string }>;
}) => ({
  verifications: input.candidates.map((candidate) => ({
    candidateId: candidate.candidateId,
    verdict: "verified",
  })),
});

function pdfLocator(input: {
  materialRevisionId: string;
  directSectionId: string;
  directChunkId: string;
}) {
  return {
    version: 1,
    materialRevisionId: input.materialRevisionId,
    materialSectionIds: [input.directSectionId],
    evidenceChunkIds: [input.directChunkId],
    source: { kind: "pdf", pageRanges: [{ start: 1, end: 1 }] },
  };
}

function createAiSetup(input: {
  rankChunks?: MaterialDraftAiSetup["rankChunks"];
  planScope?: MaterialDraftAiSetup["planScope"];
  reviewScope?: MaterialDraftAiSetup["reviewScope"];
  repairTarget?: MaterialDraftAiSetup["repairTarget"];
  verifyDraft?: MaterialDraftAiSetup["verifyDraft"];
  draftObjectiveByTitle?: ReadonlyMap<string, string>;
  rejectTitle?: string;
} = {}): MaterialDraftAiSetup {
  return {
    model: "fixture-model",
    ...(input.rankChunks ? { rankChunks: input.rankChunks } : {}),
    planScope:
      input.planScope ??
      (async () => ({
        resolutionStatus: "ambiguous",
        resolvedScopeLabel: "Fixture",
        clarification: "Clarify the fixture.",
        warnings: [],
        items: [],
      })),
    ...(input.reviewScope ? { reviewScope: input.reviewScope } : {}),
    ...(input.repairTarget ? { repairTarget: input.repairTarget } : {}),
    async generateDraft(draftInput) {
      const title = draftInput.focusNote?.match(/Create exactly this target: ([^.]+(?:\.)?)/)?.[1]
        ?.replace(/\.$/, "")
        .trim() || "Object pronoun placement";
      return {
        drafts: [
          {
            title,
            objective:
              input.draftObjectiveByTitle?.get(title) ??
              (title === "Choosing le versus les"
                ? "Choose le or les from the number of recipients in a Spanish sentence."
                : "Place an object pronoun correctly before a conjugated Spanish verb."),
            rules: ["Place the pronoun before a conjugated verb."],
            examples: ["Veo el libro. → Lo veo."],
            exerciseConstraints: "Use short sentences with one unambiguous recipient or object.",
            tags: ["spanish", "pronouns"],
          },
        ],
      };
    },
    verifyDraft: input.verifyDraft ?? (async (verificationInput) => {
      return verificationInput.target.title === input.rejectTitle
        ? {
            verdict: "rejected",
            reasons: ["not_grounded"],
            note: "The fixture rejects this target.",
          }
        : { verdict: "verified", reasons: [], note: null };
    }),
  };
}

function createSourceStorage(readBytes: () => Buffer): SourceObjectStorage {
  return {
    bucketName: "test-materials",
    async createPresignedUploadUrl() {
      return "https://uploads.example.test/material.pdf";
    },
    async headObject() {
      const bytes = readBytes();
      return { byteSize: bytes.byteLength, mimeType: "application/pdf" };
    },
    async getObjectBytes() {
      return readBytes();
    },
    async listObjects() {
      return [];
    },
    async deleteObject() {},
  };
}

function createCachedSourceEvidenceLoader(
  storage: SourceObjectStorage,
): SkillSourceEvidenceLoader {
  const cache = new Map<string, ReturnType<SkillSourceEvidenceLoader>>();
  return async (input) => {
    const key = JSON.stringify(
      input.sourceRefs.map((sourceRef) => ({
        sourceFileId: sourceRef.sourceFile.id,
        locator: sourceRef.locator,
      })),
    );
    const existing = cache.get(key);
    if (existing) {
      return existing;
    }
    const pending = loadLocalizedMaterialEvidence({ ...input, storage });
    cache.set(key, pending);
    try {
      return await pending;
    } catch (error) {
      cache.delete(key);
      throw error;
    }
  };
}
