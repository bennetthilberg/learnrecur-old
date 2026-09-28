import { randomUUID } from "node:crypto";

import { PDFDocument } from "pdf-lib";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";

import {
  MaterialPageTextStatus,
  Prisma,
  SourceFileKind,
  SourceFileStatus,
  StudyMaterialKind,
  type SourceFile,
} from "@/generated/prisma/client";
import {
  ensureMaterialPageOcr,
  loadLocalizedMaterialEvidence,
  MaterialEvidencePendingError,
} from "@/lib/materials/evidence";
import { planMaterialSkills } from "@/lib/materials/batches";
import {
  createMaterialWithInitialRevision,
  createNextMaterialRevision,
  finalizeMaterialRevision,
} from "@/lib/materials/lifecycle";
import { getPrisma } from "@/lib/prisma";
import type { SourceObjectStorage } from "@/lib/storage/s3";

const runDatabaseTests = process.env.RUN_DATABASE_TESTS === "1";
const describeDatabase = runDatabaseTests ? describe : describe.skip;
const runId = `material_evidence_${randomUUID()}`;

describeDatabase("localized material OCR evidence", () => {
  const prisma = getPrisma();
  const userId = `${runId}_owner`;
  let materialId = "";
  let materialRevisionId = "";
  let materialSectionId = "";
  let seedChunkId = "";
  let sourceFile: SourceFile;
  let pdfBytes = Buffer.alloc(0);

  beforeAll(async () => {
    await prisma.user.create({ data: { id: userId, email: `${userId}@example.com` } });
    const document = await PDFDocument.create();
    for (let page = 0; page < 10; page += 1) {
      document.addPage([500 + page, 700]);
    }
    pdfBytes = Buffer.from(await document.save());
    const { material, revision } = await createMaterialWithInitialRevision({
      userId,
      title: "Scanned grammar workbook",
      kind: StudyMaterialKind.PDF,
    });
    materialId = material.id;
    materialRevisionId = revision.id;
    const section = await prisma.materialSection.create({
      data: {
        userId,
        materialRevisionId,
        ordinal: 0,
        level: 1,
        title: "Document",
        normalizedTitle: "document",
        pageStart: 1,
        pageEnd: 10,
        headingPath: ["Document"],
      },
    });
    materialSectionId = section.id;
    sourceFile = await prisma.sourceFile.create({
      data: {
        userId,
        materialRevisionId,
        kind: SourceFileKind.PDF,
        status: SourceFileStatus.READY,
        originalName: "scanned.pdf",
        mimeType: "application/pdf",
        storageBucket: "test-materials",
        storageKey: `${runId}/scanned.pdf`,
      },
    });
    await prisma.materialPage.createMany({
      data: Array.from({ length: 10 }, (_, index) => ({
        userId,
        materialRevisionId,
        pageNumber: index + 1,
        textStatus: MaterialPageTextStatus.NEEDS_OCR,
        contentHash: `visual:${index + 1}`,
      })),
    });
    const seedChunk = await prisma.materialChunk.create({
      data: {
        userId,
        materialRevisionId,
        materialSectionId,
        sourceFileId: sourceFile.id,
        ordinal: 0,
        text: "The selected scanned pages contain the visual grammar evidence.",
        tokenEstimate: 12,
        contentHash: `seed:${runId}`,
        headingText: "Document",
        locator: { version: 1, kind: "pdf", pageRange: { start: 1, end: 10 } },
      },
    });
    seedChunkId = seedChunk.id;
    await finalizeMaterialRevision({
      userId,
      materialId: material.id,
      materialRevisionId,
      contentHash: `sha256:${runId}`,
      byteSize: pdfBytes.byteLength,
      pageCount: 10,
      storageBucket: "test-materials",
      storageKey: `${runId}/scanned.pdf`,
    });
  });

  afterAll(async () => {
    await prisma.user.delete({ where: { id: userId } });
    await prisma.$disconnect();
  });

  it("OCRs selected scanned pages in bounded cached groups without mutating snapshot chunks", async () => {
    const generator = vi.fn(async ({ pageNumbers }: { pageNumbers: number[] }) => ({
      pages: pageNumbers.map((pageNumber) => ({
        pageNumber,
        text: `Scanned page ${pageNumber} explains a grounded grammar concept.`,
      })),
    }));
    const storage = createStorage(pdfBytes);
    const first = await ensureMaterialPageOcr({
      userId,
      materialRevisionId,
      sourceFile,
      pageRanges: [{ start: 1, end: 10 }],
      storage,
      ocrGenerator: generator,
      now: new Date("2026-07-09T15:00:00.000Z"),
    });

    expect(first).toMatchObject({ status: "processed", processedPageCount: 8 });
    expect(generator.mock.calls[0][0].pageNumbers).toEqual([1, 2, 3, 4, 5, 6, 7, 8]);
    await expect(
      prisma.materialPage.count({
        where: { userId, materialRevisionId, textStatus: MaterialPageTextStatus.OCR_READY },
      }),
    ).resolves.toBe(8);
    await expect(
      prisma.materialChunk.count({ where: { userId, materialRevisionId } }),
    ).resolves.toBe(1);

    const second = await ensureMaterialPageOcr({
      userId,
      materialRevisionId,
      sourceFile,
      pageRanges: [{ start: 1, end: 10 }],
      storage,
      ocrGenerator: generator,
      now: new Date("2026-07-09T15:01:00.000Z"),
    });
    expect(second).toMatchObject({ status: "processed", processedPageCount: 2 });
    expect(generator.mock.calls[1][0].pageNumbers).toEqual([9, 10]);

    const evidence = await loadLocalizedMaterialEvidence({
      userId,
      storage,
      sourceRefs: [
        {
          locator: {
            version: 1,
            materialRevisionId,
            materialSectionIds: [materialSectionId],
            evidenceChunkIds: [seedChunkId],
            source: { kind: "pdf", pageRanges: [{ start: 1, end: 2 }] },
          },
          sourceFile,
        },
      ],
    });
    expect(evidence.sourceContext).toContain("Scanned page 1 explains");
    expect(evidence.sourceContext).toContain("Visual page 2");
    expect(evidence.sourceMedia).toHaveLength(1);
    await expect(PDFDocument.load(evidence.sourceMedia[0].bytes)).resolves.toHaveProperty(
      "getPageCount",
    );
    expect((await PDFDocument.load(evidence.sourceMedia[0].bytes)).getPageCount()).toBe(2);

    const pageTwo = await prisma.materialPage.findFirstOrThrow({
      where: { userId, materialRevisionId, pageNumber: 2 },
      select: { id: true },
    });
    const pageEvidenceId = `material-page:${pageTwo.id}`;
    const planScope = vi.fn(async (planningInput: { chunks: Array<{ id: string; text: string }> }) => {
      const ocrChunk = planningInput.chunks.find((chunk) => chunk.id === pageEvidenceId);
      expect(ocrChunk?.text).toContain("Scanned page 2 explains");
      return {
        resolutionStatus: "resolved" as const,
        resolvedScopeLabel: "Scanned page 2",
        clarification: null,
        warnings: [],
        items: [
          {
            key: "scanned-page-two",
            title: "Scanned page two grammar",
            objective: "Apply the grammar concept explained on scanned page two.",
            materialSectionIds: [materialSectionId],
            evidenceChunkIds: [pageEvidenceId],
          },
        ],
      };
    });
    const planned = await planMaterialSkills({
      userId,
      input: {
        materialId,
        materialRevisionId,
        instruction: "Make a skill from the grammar concept on scanned page 2.",
        idempotencyKey: `${runId}_ocr_planning`,
      },
      now: new Date("2026-07-09T15:02:00.000Z"),
      aiSetup: {
        model: "fixture-model",
        planScope,
        async generateDraft() {
          throw new Error("not used");
        },
        async verifyDraft() {
          throw new Error("not used");
        },
      },
      embeddingGenerator: null,
      ocrGenerator: null,
    });
    expect(planned).toMatchObject({
      status: "planned",
      plan: {
        items: [
          {
            evidenceChunkIds: [pageEvidenceId],
            locator: { source: { kind: "pdf", pageRanges: [{ start: 2, end: 2 }] } },
          },
        ],
      },
    });

    const pageEvidence = await loadLocalizedMaterialEvidence({
      userId,
      storage,
      sourceRefs: [
        {
          locator: {
            version: 1,
            materialRevisionId,
            materialSectionIds: [materialSectionId],
            evidenceChunkIds: [pageEvidenceId],
            source: { kind: "pdf", pageRanges: [{ start: 2, end: 2 }] },
          },
          sourceFile,
        },
      ],
    });
    expect(pageEvidence.sourceContext).toContain("Scanned page 2 explains");
  });

  it("lets only one concurrent worker claim each OCR page", async () => {
    await prisma.materialPage.updateMany({
      where: { userId, materialRevisionId, pageNumber: { in: [1, 2] } },
      data: {
        textStatus: MaterialPageTextStatus.NEEDS_OCR,
        ocrText: null,
        metadata: {},
      },
    });
    const requestedPages: number[] = [];
    const generator = vi.fn(async ({ pageNumbers }: { pageNumbers: number[] }) => {
      requestedPages.push(...pageNumbers);
      await new Promise((resolve) => setTimeout(resolve, 10));
      return {
        pages: pageNumbers.map((pageNumber) => ({
          pageNumber,
          text: `Concurrent OCR page ${pageNumber}.`,
        })),
      };
    });
    const input = {
      userId,
      materialRevisionId,
      sourceFile,
      pageRanges: [{ start: 1, end: 2 }],
      storage: createStorage(pdfBytes),
      ocrGenerator: generator,
      now: new Date("2026-07-09T16:00:00.000Z"),
    };

    const results = await Promise.all([
      ensureMaterialPageOcr(input),
      ensureMaterialPageOcr(input),
    ]);

    expect(results.some((result) => result.status === "processed")).toBe(true);
    expect(requestedPages.sort((left, right) => left - right)).toEqual([1, 2]);
    await expect(
      prisma.materialPage.count({
        where: {
          userId,
          materialRevisionId,
          pageNumber: { in: [1, 2] },
          textStatus: MaterialPageTextStatus.OCR_READY,
        },
      }),
    ).resolves.toBe(2);
    await expect(
      prisma.materialChunk.count({ where: { userId, materialRevisionId } }),
    ).resolves.toBe(1);
  });

  it("releases claimed OCR pages when the worker deadline aborts its provider", async () => {
    const page = await prisma.materialPage.findFirstOrThrow({
      where: { userId, materialRevisionId, pageNumber: 1 },
    });
    const previousPageState = {
      textStatus: page.textStatus,
      ocrText: page.ocrText,
      contentHash: page.contentHash,
      tokenEstimate: page.tokenEstimate,
      metadata:
        page.metadata === null
          ? Prisma.JsonNull
          : page.metadata as Prisma.InputJsonValue,
    };
    await prisma.materialPage.update({
      where: { id: page.id },
      data: {
        textStatus: MaterialPageTextStatus.NEEDS_OCR,
        ocrText: null,
        metadata: {},
      },
    });

    const controller = new AbortController();
    const canceled = new Error("material OCR worker deadline reached");
    let ocrSignal: AbortSignal | undefined;
    let markOcrStarted!: () => void;
    const ocrStarted = new Promise<void>((resolve) => {
      markOcrStarted = resolve;
    });

    try {
      const ocrPromise = ensureMaterialPageOcr({
        userId,
        materialRevisionId,
        sourceFile,
        pageRanges: [{ start: 1, end: 1 }],
        storage: createStorage(pdfBytes),
        signal: controller.signal,
        now: new Date(),
        ocrGenerator: ({ signal }) => {
          ocrSignal = signal;
          markOcrStarted();
          return new Promise((_, reject) => {
            signal?.addEventListener("abort", () => reject(signal.reason), { once: true });
          });
        },
      });
      await ocrStarted;
      controller.abort(canceled);

      await expect(ocrPromise).rejects.toBe(canceled);
      expect(ocrSignal).toBe(controller.signal);
      await expect(
        prisma.materialPage.findUniqueOrThrow({ where: { id: page.id } }),
      ).resolves.toMatchObject({
        textStatus: MaterialPageTextStatus.NEEDS_OCR,
        ocrText: null,
        metadata: { reason: "worker-deadline", retryable: true },
      });
    } finally {
      await prisma.materialPage.update({
        where: { id: page.id },
        data: previousPageState,
      });
    }
  });

  it("releases OCR claims when the worker deadline aborts during the claim transaction", async () => {
    const page = await prisma.materialPage.findFirstOrThrow({
      where: { userId, materialRevisionId, pageNumber: 4 },
    });
    const previousPageState = {
      textStatus: page.textStatus,
      ocrText: page.ocrText,
      contentHash: page.contentHash,
      tokenEstimate: page.tokenEstimate,
      metadata:
        page.metadata === null
          ? Prisma.JsonNull
          : page.metadata as Prisma.InputJsonValue,
    };
    await prisma.materialPage.update({
      where: { id: page.id },
      data: {
        textStatus: MaterialPageTextStatus.NEEDS_OCR,
        ocrText: null,
        metadata: {},
      },
    });

    const controller = new AbortController();
    const canceled = new Error("material OCR worker deadline reached");
    const originalTransaction = prisma.$transaction.bind(prisma) as unknown as (
      operation: unknown,
      options?: { maxWait?: number; timeout?: number },
    ) => Promise<unknown>;
    const transactionSpy = vi.spyOn(prisma, "$transaction");
    let abortedAfterClaim = false;
    transactionSpy.mockImplementation((async (...args: unknown[]) => {
      const operation = args[0];
      const options = args[1] as { maxWait?: number; timeout?: number } | undefined;
      if (!abortedAfterClaim && typeof operation === "function") {
        return originalTransaction(async (tx: unknown) => {
          const result = await (operation as (tx: unknown) => Promise<unknown>)(tx);
          abortedAfterClaim = true;
          controller.abort(canceled);
          return result;
        }, options);
      }
      return originalTransaction(operation, options);
    }) as typeof prisma.$transaction);
    const generator = vi.fn(async ({ pageNumbers }: { pageNumbers: number[] }) => ({
      pages: pageNumbers.map((pageNumber) => ({
        pageNumber,
        text: `OCR page ${pageNumber}.`,
      })),
    }));

    try {
      await expect(ensureMaterialPageOcr({
        userId,
        materialRevisionId,
        sourceFile,
        pageRanges: [{ start: 4, end: 4 }],
        storage: createStorage(pdfBytes),
        signal: controller.signal,
        ocrGenerator: generator,
      })).rejects.toBe(canceled);
      expect(abortedAfterClaim).toBe(true);
      expect(generator).not.toHaveBeenCalled();
      await expect(prisma.materialPage.findUniqueOrThrow({ where: { id: page.id } }))
        .resolves.toMatchObject({
          textStatus: MaterialPageTextStatus.NEEDS_OCR,
          metadata: { reason: "worker-deadline", retryable: true },
        });
    } finally {
      transactionSpy.mockRestore();
      await prisma.materialPage.update({
        where: { id: page.id },
        data: previousPageState,
      });
    }
  });

  it("reports a fresh OCR claim instead of treating it as completed work", async () => {
    const page = await prisma.materialPage.findFirstOrThrow({
      where: { userId, materialRevisionId, pageNumber: 3 },
    });
    const previousPageState = {
      textStatus: page.textStatus,
      ocrText: page.ocrText,
      contentHash: page.contentHash,
      tokenEstimate: page.tokenEstimate,
      metadata:
        page.metadata === null
          ? Prisma.JsonNull
          : page.metadata as Prisma.InputJsonValue,
    };
    const claimedAt = new Date();
    await prisma.materialPage.update({
      where: { id: page.id },
      data: {
        textStatus: MaterialPageTextStatus.OCR_PROCESSING,
        ocrText: null,
        updatedAt: claimedAt,
      },
    });
    const generator = vi.fn(async ({ pageNumbers }: { pageNumbers: number[] }) => ({
      pages: pageNumbers.map((pageNumber) => ({
        pageNumber,
        text: `OCR page ${pageNumber}.`,
      })),
    }));

    try {
      await expect(ensureMaterialPageOcr({
        userId,
        materialRevisionId,
        sourceFile,
        pageRanges: [{ start: 3, end: 3 }],
        storage: createStorage(pdfBytes),
        ocrGenerator: generator,
        now: new Date(claimedAt.getTime() + 5_000),
      })).resolves.toMatchObject({ status: "in-progress", processedPageCount: 0 });
      expect(generator).not.toHaveBeenCalled();
      await expect(prisma.materialPage.findUniqueOrThrow({ where: { id: page.id } }))
        .resolves.toMatchObject({ textStatus: MaterialPageTextStatus.OCR_PROCESSING });
      await expect(loadLocalizedMaterialEvidence({
        userId,
        storage: createStorage(pdfBytes),
        sourceRefs: [{
          sourceFile,
          locator: {
            version: 1,
            materialRevisionId,
            materialSectionIds: [materialSectionId],
            evidenceChunkIds: [seedChunkId],
            source: { kind: "pdf", pageRanges: [{ start: 3, end: 3 }] },
          },
        }],
      })).rejects.toBeInstanceOf(MaterialEvidencePendingError);
    } finally {
      await prisma.materialPage.update({
        where: { id: page.id },
        data: previousPageState,
      });
    }
  });

  it("releases an OCR claim when the generator ignores abort", async () => {
    const page = await prisma.materialPage.findFirstOrThrow({
      where: { userId, materialRevisionId, pageNumber: 5 },
    });
    const previousPageState = {
      textStatus: page.textStatus,
      ocrText: page.ocrText,
      contentHash: page.contentHash,
      tokenEstimate: page.tokenEstimate,
      metadata: page.metadata === null ? Prisma.JsonNull : page.metadata as Prisma.InputJsonValue,
    };
    await prisma.materialPage.update({
      where: { id: page.id },
      data: { textStatus: MaterialPageTextStatus.NEEDS_OCR, ocrText: null, metadata: {} },
    });
    const controller = new AbortController();
    const canceled = new Error("source stage timed out");
    let markStarted!: () => void;
    const started = new Promise<void>((resolve) => { markStarted = resolve; });
    let releaseGenerator!: () => void;
    const generatorResult = new Promise<unknown>((resolve) => {
      releaseGenerator = () => resolve({ pages: [{ pageNumber: 5, text: "late OCR result" }] });
    });
    try {
      const ocr = ensureMaterialPageOcr({
        userId,
        materialRevisionId,
        sourceFile,
        pageRanges: [{ start: 5, end: 5 }],
        storage: createStorage(pdfBytes),
        signal: controller.signal,
        ocrGenerator: async () => { markStarted(); return generatorResult; },
      });
      await started;
      controller.abort(canceled);
      await vi.waitFor(async () => {
        await expect(prisma.materialPage.findUniqueOrThrow({ where: { id: page.id } }))
          .resolves.toMatchObject({ textStatus: MaterialPageTextStatus.NEEDS_OCR });
      });
      releaseGenerator();
      await expect(ocr).rejects.toBe(canceled);
      await expect(prisma.materialPage.findUniqueOrThrow({ where: { id: page.id } }))
        .resolves.toMatchObject({ textStatus: MaterialPageTextStatus.NEEDS_OCR, ocrText: null });
    } finally {
      releaseGenerator();
      await prisma.materialPage.update({ where: { id: page.id }, data: previousPageState });
    }
  });

  it("OCRs a still-ready revision after a replacement becomes active", async () => {
    const { material, revision } = await createMaterialWithInitialRevision({
      userId,
      title: "Workbook with an existing draft batch",
      kind: StudyMaterialKind.PDF,
    });
    const objectKey = `${runId}/obsolete-ready-revision.pdf`;
    const obsoleteSource = await prisma.sourceFile.create({
      data: {
        userId,
        materialRevisionId: revision.id,
        kind: SourceFileKind.PDF,
        status: SourceFileStatus.READY,
        originalName: "obsolete-ready-revision.pdf",
        mimeType: "application/pdf",
        storageBucket: "test-materials",
        storageKey: objectKey,
      },
    });
    await prisma.materialPage.create({
      data: {
        userId,
        materialRevisionId: revision.id,
        pageNumber: 1,
        textStatus: MaterialPageTextStatus.NEEDS_OCR,
        contentHash: `visual:${runId}:obsolete-ready`,
      },
    });
    await finalizeMaterialRevision({
      userId,
      materialId: material.id,
      materialRevisionId: revision.id,
      contentHash: `sha256:${runId}:obsolete-ready`,
      byteSize: pdfBytes.byteLength,
      pageCount: 1,
      storageBucket: "test-materials",
      storageKey: objectKey,
    });
    const replacement = await createNextMaterialRevision({
      userId,
      materialId: material.id,
    });
    if (!replacement) {
      throw new Error("expected replacement revision");
    }
    await finalizeMaterialRevision({
      userId,
      materialId: material.id,
      materialRevisionId: replacement.id,
      contentHash: `sha256:${runId}:active-replacement`,
      byteSize: pdfBytes.byteLength,
      pageCount: 1,
      storageBucket: "test-materials",
      storageKey: objectKey,
    });

    await expect(
      ensureMaterialPageOcr({
        userId,
        materialRevisionId: revision.id,
        sourceFile: obsoleteSource,
        pageRanges: [{ start: 1, end: 1 }],
        storage: createStorage(pdfBytes),
        ocrGenerator: async ({ pageNumbers }) => ({
          pages: pageNumbers.map((pageNumber) => ({
            pageNumber,
            text: "OCR evidence for the still-ready revision batch.",
          })),
        }),
        now: new Date("2026-07-09T16:15:00.000Z"),
      }),
    ).resolves.toMatchObject({ status: "processed", processedPageCount: 1 });
  });

  it("releases stale OCR claims before activating a replacement", async () => {
    const claimTime = new Date("2026-07-09T16:20:00.000Z");
    const { material, revision } = await createMaterialWithInitialRevision({
      userId,
      title: "Workbook with a stale OCR claim",
      kind: StudyMaterialKind.PDF,
    });
    await prisma.materialPage.create({
      data: {
        userId,
        materialRevisionId: revision.id,
        pageNumber: 1,
        textStatus: MaterialPageTextStatus.OCR_PROCESSING,
        contentHash: `visual:${runId}:stale-claim`,
        updatedAt: claimTime,
      },
    });
    await finalizeMaterialRevision({
      userId,
      materialId: material.id,
      materialRevisionId: revision.id,
      contentHash: `sha256:${runId}:stale-claim`,
      byteSize: pdfBytes.byteLength,
      pageCount: 1,
      storageBucket: "test-materials",
      storageKey: `${runId}/stale-claim.pdf`,
    });
    const replacement = await createNextMaterialRevision({
      userId,
      materialId: material.id,
    });
    if (!replacement) {
      throw new Error("expected replacement revision");
    }

    await expect(
      finalizeMaterialRevision({
        userId,
        materialId: material.id,
        materialRevisionId: replacement.id,
        contentHash: `sha256:${runId}:stale-claim-replacement`,
        byteSize: pdfBytes.byteLength,
        pageCount: 1,
        storageBucket: "test-materials",
        storageKey: `${runId}/stale-claim.pdf`,
        copyReadyOcrFromRevisionId: revision.id,
        indexedOcrPageNumbers: [],
        now: new Date(claimTime.getTime() + 10 * 60 * 1_000 + 1),
      }),
    ).resolves.toMatchObject({ id: replacement.id });
    await expect(
      prisma.materialPage.findUniqueOrThrow({
        where: {
          materialRevisionId_pageNumber: {
            materialRevisionId: revision.id,
            pageNumber: 1,
          },
        },
        select: { textStatus: true },
      }),
    ).resolves.toEqual({ textStatus: MaterialPageTextStatus.OCR_FAILED });
  });

  it("defers replacement activation until in-flight OCR is indexed", async () => {
    const { material, revision } = await createMaterialWithInitialRevision({
      userId,
      title: "Rebuilding scanned handbook",
      kind: StudyMaterialKind.PDF,
    });
    const objectKey = `${runId}/rebuilding-scanned.pdf`;
    const rebuildingSource = await prisma.sourceFile.create({
      data: {
        userId,
        materialRevisionId: revision.id,
        kind: SourceFileKind.PDF,
        status: SourceFileStatus.READY,
        originalName: "rebuilding-scanned.pdf",
        mimeType: "application/pdf",
        storageBucket: "test-materials",
        storageKey: objectKey,
      },
    });
    await prisma.materialPage.create({
      data: {
        userId,
        materialRevisionId: revision.id,
        pageNumber: 1,
        textStatus: MaterialPageTextStatus.NEEDS_OCR,
        contentHash: `visual:${runId}:rebuild-race`,
      },
    });
    await finalizeMaterialRevision({
      userId,
      materialId: material.id,
      materialRevisionId: revision.id,
      contentHash: `sha256:${runId}:rebuild-race`,
      byteSize: pdfBytes.byteLength,
      pageCount: 10,
      storageBucket: "test-materials",
      storageKey: objectKey,
    });

    let releaseOcr!: () => void;
    let markOcrStarted!: () => void;
    const ocrStarted = new Promise<void>((resolve) => {
      markOcrStarted = resolve;
    });
    const ocrRelease = new Promise<void>((resolve) => {
      releaseOcr = resolve;
    });
    const ocrPromise = ensureMaterialPageOcr({
      userId,
      materialRevisionId: revision.id,
      sourceFile: rebuildingSource,
      pageRanges: [{ start: 1, end: 1 }],
      storage: createStorage(pdfBytes),
      ocrGenerator: async ({ pageNumbers }) => {
        markOcrStarted();
        await ocrRelease;
        return {
          pages: pageNumbers.map((pageNumber) => ({
            pageNumber,
            text: "OCR completed while the replacement rebuild waited for activation.",
          })),
        };
      },
      now: new Date("2026-07-09T16:30:00.000Z"),
    });
    await ocrStarted;

    const replacement = await createNextMaterialRevision({
      userId,
      materialId: material.id,
    });
    if (!replacement) {
      throw new Error("expected replacement revision");
    }
    await prisma.materialSection.create({
      data: {
        userId,
        materialRevisionId: replacement.id,
        ordinal: 0,
        title: "Document",
        normalizedTitle: "document",
        pageStart: 1,
        pageEnd: 10,
        headingPath: ["Document"],
      },
    });
    await prisma.sourceFile.create({
      data: {
        userId,
        materialRevisionId: replacement.id,
        kind: SourceFileKind.PDF,
        status: SourceFileStatus.READY,
        originalName: "rebuilt-scanned.pdf",
        mimeType: "application/pdf",
        storageBucket: "test-materials",
        storageKey: objectKey,
      },
    });
    await prisma.materialRevision.update({
      where: { id: replacement.id },
      data: { processingMetadata: { rebuildOfRevisionId: revision.id } },
    });
    const finalizeReplacement = (indexedOcrPageNumbers: number[]) =>
      finalizeMaterialRevision({
        userId,
        materialId: material.id,
        materialRevisionId: replacement.id,
        contentHash: `sha256:${runId}:replacement`,
        byteSize: pdfBytes.byteLength,
        pageCount: 10,
        storageBucket: "test-materials",
        storageKey: objectKey,
        processingMetadata: { rebuildOfRevisionId: revision.id },
        copyReadyOcrFromRevisionId: revision.id,
        indexedOcrPageNumbers,
        now: new Date("2026-07-09T16:35:00.000Z"),
      });
    await expect(finalizeReplacement([])).rejects.toThrow(/OCR is still processing/i);
    await expect(
      prisma.studyMaterial.findUniqueOrThrow({
        where: { id: material.id },
        select: { activeRevisionId: true },
      }),
    ).resolves.toEqual({ activeRevisionId: revision.id });

    releaseOcr();
    await expect(ocrPromise).resolves.toMatchObject({
      status: "processed",
      processedPageCount: 1,
    });
    await expect(finalizeReplacement([])).rejects.toThrow(/OCR became ready/i);
    const replacementSection = await prisma.materialSection.findFirstOrThrow({
      where: { userId, materialRevisionId: replacement.id },
      select: { id: true, title: true },
    });
    const replacementSource = await prisma.sourceFile.findFirstOrThrow({
      where: { userId, materialRevisionId: replacement.id },
      select: { id: true },
    });
    await prisma.materialChunk.create({
      data: {
        userId,
        materialRevisionId: replacement.id,
        materialSectionId: replacementSection.id,
        sourceFileId: replacementSource.id,
        ordinal: 0,
        text: "OCR completed while the replacement rebuild waited for activation.",
        tokenEstimate: 9,
        contentHash: `sha256:${runId}:replacement-ocr-chunk`,
        headingText: replacementSection.title,
        locator: {
          version: 1,
          kind: "pdf",
          sectionId: replacementSection.id,
          pageRange: { start: 1, end: 1 },
        },
      },
    });
    await expect(finalizeReplacement([1])).resolves.toMatchObject({ id: replacement.id });
    await expect(
      prisma.materialPage.findUniqueOrThrow({
        where: {
          materialRevisionId_pageNumber: {
            materialRevisionId: replacement.id,
            pageNumber: 1,
          },
        },
        select: { ocrText: true, textStatus: true },
      }),
    ).resolves.toEqual({
      ocrText: "OCR completed while the replacement rebuild waited for activation.",
      textStatus: MaterialPageTextStatus.OCR_READY,
    });
    await expect(
      prisma.materialChunk.findFirstOrThrow({
        where: {
          userId,
          materialRevisionId: replacement.id,
          text: { contains: "OCR completed while the replacement rebuild waited" },
        },
        select: { materialRevisionId: true, locator: true },
      }),
    ).resolves.toMatchObject({
      materialRevisionId: replacement.id,
      locator: { kind: "pdf", pageRange: { start: 1, end: 1 } },
    });
  });

  it("attaches cited pages from an ordinary text PDF without material-page OCR rows", async () => {
    const { material, revision } = await createMaterialWithInitialRevision({
      userId,
      title: "Text grammar handbook",
      kind: StudyMaterialKind.PDF,
    });
    const section = await prisma.materialSection.create({
      data: {
        userId,
        materialRevisionId: revision.id,
        ordinal: 0,
        level: 1,
        title: "Direct objects",
        normalizedTitle: "direct objects",
        pageStart: 4,
        pageEnd: 5,
        headingPath: ["Direct objects"],
      },
    });
    const ordinarySource = await prisma.sourceFile.create({
      data: {
        userId,
        materialRevisionId: revision.id,
        kind: SourceFileKind.PDF,
        status: SourceFileStatus.READY,
        originalName: "text-handbook.pdf",
        mimeType: "application/pdf",
        storageBucket: "test-materials",
        storageKey: `${runId}/text-handbook.pdf`,
      },
    });
    const chunk = await prisma.materialChunk.create({
      data: {
        userId,
        materialRevisionId: revision.id,
        materialSectionId: section.id,
        sourceFileId: ordinarySource.id,
        ordinal: 0,
        text: "Direct object pronouns replace nouns that receive the action.",
        tokenEstimate: 12,
        contentHash: `ordinary:${runId}`,
        headingText: "Direct objects",
        locator: { version: 1, kind: "pdf", pageRange: { start: 4, end: 5 } },
      },
    });
    await finalizeMaterialRevision({
      userId,
      materialId: material.id,
      materialRevisionId: revision.id,
      contentHash: `sha256:ordinary:${runId}`,
      byteSize: pdfBytes.byteLength,
      pageCount: 10,
      storageBucket: "test-materials",
      storageKey: `${runId}/text-handbook.pdf`,
    });

    await expect(
      prisma.materialPage.count({ where: { userId, materialRevisionId: revision.id } }),
    ).resolves.toBe(0);
    const evidence = await loadLocalizedMaterialEvidence({
      userId,
      storage: createStorage(pdfBytes),
      sourceRefs: [
        {
          sourceFile: ordinarySource,
          locator: {
            version: 1,
            materialRevisionId: revision.id,
            materialSectionIds: [section.id],
            evidenceChunkIds: [chunk.id],
            source: { kind: "pdf", pageRanges: [{ start: 4, end: 5 }] },
          },
        },
      ],
    });

    expect(evidence.sourceMedia).toHaveLength(1);
    expect((await PDFDocument.load(evidence.sourceMedia[0].bytes)).getPageCount()).toBe(2);
    expect(evidence.sourceMedia[0].originalName).toContain("pages-4-5");
  });
});

function createStorage(bytes: Buffer): SourceObjectStorage {
  return {
    bucketName: "test-materials",
    async createPresignedUploadUrl() {
      throw new Error("not used");
    },
    async headObject() {
      return { byteSize: bytes.byteLength, mimeType: "application/pdf" };
    },
    async getObjectBytes() {
      return bytes;
    },
    async listObjects() {
      return [];
    },
    async deleteObject() {},
  };
}
