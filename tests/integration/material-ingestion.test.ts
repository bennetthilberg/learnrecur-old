import { createHash, randomUUID } from "node:crypto";

import { PDFDocument, StandardFonts } from "pdf-lib";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import {
  MaterialPageTextStatus,
  MaterialRevisionStatus,
  SkillStatus,
  SourceFileKind,
  SourceFileStatus,
  StudyMaterialKind,
} from "@/generated/prisma/client";
import { queueMaterialDeletion, runMaterialCleanupJob } from "@/lib/materials/cleanup";
import {
  discardPreparedMaterialPdf,
  prepareMaterialPdf,
  queueMaterialPdfReindex,
  queueMaterialPdfIngestion,
  queueWebsiteMaterialImport,
  queueWebsiteMaterialRefresh,
  retryMaterialIngestion,
  runMaterialIngestionJob,
} from "@/lib/materials/ingestion";
import { MATERIAL_QUEUE_STALE_AFTER_MS } from "@/lib/materials/ingestion-status";
import { publishMaterialIngestion } from "@/lib/materials/ingestion-publication";
import { finalizeMaterialRevision } from "@/lib/materials/lifecycle";
import { getMaterialDetail, getMaterialLibrary } from "@/lib/materials/library";
import { MATERIAL_EMBEDDING_DIMENSIONS } from "@/lib/materials/retrieval";
import { getPrisma } from "@/lib/prisma";
import { prepareSourceUpload, queueSourceUploadDrafts } from "@/lib/skills/uploads";
import type { SourceObjectStorage } from "@/lib/storage/s3";
import { ALPHA_SOURCE_UPLOADS_PER_DAY, ALPHA_STORED_SOURCE_BYTES } from "@/lib/usage-limits";

const runDatabaseTests = process.env.RUN_DATABASE_TESTS === "1";
const describeDatabase = runDatabaseTests ? describe : describe.skip;
const runId = `material_ingestion_${randomUUID()}`;

describeDatabase("material ingestion", () => {
  const prisma = getPrisma();
  const userId = `${runId}_owner`;

  beforeAll(async () => {
    await prisma.user.create({ data: { id: userId, email: `${userId}@example.com` } });
  });

  afterAll(async () => {
    await prisma.user.deleteMany({ where: { id: userId } });
    await prisma.$disconnect();
  });

  it("prepares, verifies, and indexes a PDF while caching pages that need OCR", async () => {
    const document = await PDFDocument.create();
    const font = await document.embedFont(StandardFonts.Helvetica);
    document.addPage([612, 792]).drawText(
      "Chapter 4 Direct object pronouns replace nouns receiving an action in a sentence.",
      { x: 48, y: 720, size: 12, font },
    );
    document.addPage([612, 792]);
    const bytes = Buffer.from(await document.save());
    const storage = createMemoryStorage();

    const prepared = await prepareMaterialPdf({
      userId,
      now: new Date(),
      storage,
      input: {
        title: "Practical Spanish Grammar",
        originalName: "spanish-grammar.pdf",
        mimeType: "application/pdf",
        byteSize: String(bytes.byteLength),
      },
    });
    expect(prepared.status).toBe("prepared");
    if (prepared.status !== "prepared") {
      throw new Error("expected prepared upload");
    }
    expect(storage.lastPreparedKey).toMatch(
      new RegExp(`^source-uploads/materials/${userId}/`),
    );
    expect(prepared.headers).toEqual({ "Content-Type": "application/pdf" });
    storage.objects.set(storage.lastPreparedKey, bytes);

    const sentEvents: string[] = [];
    const queued = await queueMaterialPdfIngestion({
      userId,
      materialRevisionId: prepared.materialRevisionId,
      now: new Date(),
      storage,
      eventSender: {
        async sendMaterialIngestionRequested(payload) {
          sentEvents.push(payload.materialRevisionId);
        },
      },
    });
    expect(queued.status).toBe("queued");
    expect(sentEvents).toEqual([prepared.materialRevisionId]);

    const result = await runMaterialIngestionJob({
      userId,
      materialRevisionId: prepared.materialRevisionId,
      storage,
      summaryGenerator: async () => ({
        overview: "A practical Spanish grammar textbook for independent learners",
        coverage: "It introduces direct object pronouns through explanations and examples",
      }),
      embeddingGenerator: async ({ texts }) =>
        texts.map((_, textIndex) =>
          Array.from({ length: MATERIAL_EMBEDDING_DIMENSIONS }, (_, index) =>
            index === textIndex % MATERIAL_EMBEDDING_DIMENSIONS ? 1 : 0,
          ),
        ),
    });
    expect(result).toMatchObject({ status: "ready", pageCount: 2 });

    const revision = await prisma.materialRevision.findUniqueOrThrow({
      where: { id: prepared.materialRevisionId },
      include: { chunks: true, sections: true, pages: true, sourceFiles: true },
    });
    expect(revision.status).toBe(MaterialRevisionStatus.READY);
    expect(revision.summary).toBe(
      "A practical Spanish grammar textbook for independent learners. It introduces direct object pronouns through explanations and examples.",
    );
    expect(revision.sections.length).toBeGreaterThan(0);
    expect(revision.chunks.length).toBeGreaterThan(0);
    expect(revision.pages).toEqual([
      expect.objectContaining({ pageNumber: 2, textStatus: "NEEDS_OCR" }),
    ]);
    expect(revision.sourceFiles[0].status).toBe(SourceFileStatus.READY);
  });

  it("keeps a completed PDF when event delivery fails after the worker finishes", async () => {
    const document = await PDFDocument.create();
    document.addPage([612, 792]);
    const bytes = Buffer.from(await document.save());
    const storage = createMemoryStorage();
    const prepared = await prepareMaterialPdf({
      userId,
      now: new Date(),
      storage,
      input: {
        title: "Ambiguous PDF queue",
        originalName: "ambiguous.pdf",
        mimeType: "application/pdf",
        byteSize: bytes.byteLength,
      },
    });
    expect(prepared.status).toBe("prepared");
    if (prepared.status !== "prepared") throw new Error("expected prepared upload");
    storage.objects.set(storage.lastPreparedKey, bytes);

    const result = await queueMaterialPdfIngestion({
      userId,
      materialRevisionId: prepared.materialRevisionId,
      now: new Date(),
      storage,
      eventSender: {
        async sendMaterialIngestionRequested(payload) {
          await prisma.sourceFile.updateMany({
            where: { materialRevisionId: payload.materialRevisionId, userId },
            data: { status: SourceFileStatus.READY },
          });
          await prisma.materialRevision.update({
            where: { id: payload.materialRevisionId },
            data: { status: MaterialRevisionStatus.READY },
          });
          throw new Error("response lost after the worker finished");
        },
      },
    });

    expect(result).toMatchObject({ status: "queued" });
    await expect(
      prisma.materialRevision.findUniqueOrThrow({
        where: { id: prepared.materialRevisionId },
        select: { status: true, errorCode: true, sourceFiles: { select: { status: true } } },
      }),
    ).resolves.toEqual({
      status: MaterialRevisionStatus.READY,
      errorCode: null,
      sourceFiles: [{ status: SourceFileStatus.READY }],
    });
  });

  it("discards a prepared PDF when the direct upload does not finish", async () => {
    const storage = createMemoryStorage();
    const prepared = await prepareMaterialPdf({
      userId,
      now: new Date(),
      storage,
      input: {
        title: "Interrupted PDF upload",
        originalName: "interrupted.pdf",
        mimeType: "application/pdf",
        byteSize: "2048",
      },
    });
    expect(prepared.status).toBe("prepared");
    if (prepared.status !== "prepared") {
      throw new Error("expected prepared upload");
    }

    await expect(
      discardPreparedMaterialPdf({
        userId,
        materialId: prepared.materialId,
        materialRevisionId: prepared.materialRevisionId,
        storage,
      }),
    ).resolves.toEqual({ status: "discarded" });
    expect(storage.deleted).toContain(storage.lastPreparedKey);
    expect(await prisma.studyMaterial.count({ where: { id: prepared.materialId } })).toBe(0);
  });

  it("requeues a stalled revision without requiring another upload", async () => {
    const now = new Date("2026-07-10T18:00:00.000Z");
    const material = await prisma.studyMaterial.create({
      data: {
        userId,
        title: "Stalled reusable PDF",
        kind: StudyMaterialKind.PDF,
        revisions: {
          create: {
            revisionNumber: 1,
            status: MaterialRevisionStatus.QUEUED,
            updatedAt: new Date(now.getTime() - MATERIAL_QUEUE_STALE_AFTER_MS),
          },
        },
      },
      include: { revisions: true },
    });
    const revision = material.revisions[0];
    const sentRevisionIds: string[] = [];

    const result = await retryMaterialIngestion({
      userId,
      materialRevisionId: revision.id,
      now,
      eventSender: {
        async sendMaterialIngestionRequested(payload) {
          sentRevisionIds.push(payload.materialRevisionId);
        },
      },
    });

    expect(result).toMatchObject({
      status: "queued",
      materialId: material.id,
      materialRevisionId: revision.id,
    });
    expect(sentRevisionIds).toEqual([revision.id]);
    await expect(
      prisma.materialRevision.findUniqueOrThrow({
        where: { id: revision.id },
        select: { status: true, updatedAt: true },
      }),
    ).resolves.toMatchObject({
      status: MaterialRevisionStatus.QUEUED,
      updatedAt: expect.any(Date),
    });
  });

  it("reindexes a ready PDF from the preserved original into a new immutable revision", async () => {
    const document = await PDFDocument.create();
    const font = await document.embedFont(StandardFonts.Helvetica);
    document.addPage([612, 792]).drawText(
      "Embedded overview text that does not contain the cached visual lesson topic.",
      { x: 48, y: 720, size: 12, font },
    );
    document.addPage([612, 792]);
    const bytes = Buffer.from(await document.save());
    const storage = createMemoryStorage();
    const objectKey = `${runId}/reindex/original.pdf`;
    storage.objects.set(objectKey, bytes);
    const material = await prisma.studyMaterial.create({
      data: {
        userId,
        title: "Reindexable Spanish reference",
        kind: StudyMaterialKind.PDF,
        revisions: {
          create: {
            revisionNumber: 1,
            storageBucket: storage.bucketName,
            storageKey: objectKey,
            sourceFiles: {
              create: {
                kind: SourceFileKind.PDF,
                status: SourceFileStatus.READY,
                originalName: "spanish-reference.pdf",
                mimeType: "application/pdf",
                byteSize: bytes.byteLength,
                storageBucket: storage.bucketName,
                storageKey: objectKey,
              },
            },
          },
        },
      },
      include: { revisions: { include: { sourceFiles: true } } },
    });
    const activeRevision = material.revisions[0];
    await finalizeMaterialRevision({
      userId,
      materialId: material.id,
      materialRevisionId: activeRevision.id,
      contentHash: createHash("sha256").update(bytes).digest("hex"),
      byteSize: bytes.byteLength,
      pageCount: 2,
      storageBucket: storage.bucketName,
      storageKey: objectKey,
      processingMetadata: { embeddingStatus: "unavailable" },
    });
    await prisma.materialPage.create({
      data: {
        userId,
        materialRevisionId: activeRevision.id,
        pageNumber: 1,
        embeddedText: null,
        ocrText: "Cached OCR text from the preserved PDF page.",
        textStatus: MaterialPageTextStatus.OCR_READY,
        contentHash: `sha256:${runId}:cached-reindex-ocr`,
        tokenEstimate: 9,
      },
    });
    await prisma.materialPage.create({
      data: {
        userId,
        materialRevisionId: activeRevision.id,
        pageNumber: 2,
        embeddedText: null,
        ocrText: null,
        textStatus: MaterialPageTextStatus.OCR_PROCESSING,
        contentHash: `sha256:${runId}:in-flight-reindex-ocr`,
        tokenEstimate: 0,
        metadata: { reason: "lazy-ocr" },
      },
    });
    const linkedSkill = await prisma.skill.create({
      data: {
        userId,
        title: "Regular preterit endings",
        tags: [],
      },
    });
    await prisma.skillSourceRef.create({
      data: {
        userId,
        skillId: linkedSkill.id,
        sourceFileId: activeRevision.sourceFiles[0].id,
      },
    });
    const sentRevisionIds: string[] = [];

    const result = await queueMaterialPdfReindex({
      userId,
      materialId: material.id,
      now: new Date("2026-08-06T20:00:00.000Z"),
      storage,
      eventSender: {
        async sendMaterialIngestionRequested(payload) {
          sentRevisionIds.push(payload.materialRevisionId);
        },
      },
    });

    expect(result).toMatchObject({ status: "queued", materialId: material.id });
    if (result.status !== "queued") {
      throw new Error("expected queued PDF reindex");
    }
    expect(sentRevisionIds).toEqual([result.materialRevisionId]);
    const revisions = await prisma.materialRevision.findMany({
      where: { materialId: material.id },
      orderBy: { revisionNumber: "asc" },
      include: { sourceFiles: true },
    });
    expect(revisions.map((revision) => revision.revisionNumber)).toEqual([1, 2]);
    expect(revisions[1]).toMatchObject({
      status: MaterialRevisionStatus.QUEUED,
      storageBucket: storage.bucketName,
      storageKey: objectKey,
      processingMetadata: { rebuildOfRevisionId: activeRevision.id },
      sourceFiles: [
        expect.objectContaining({
          status: SourceFileStatus.UPLOADED,
          storageBucket: storage.bucketName,
          storageKey: objectKey,
          byteSize: null,
        }),
      ],
    });
    expect(
      await prisma.studyMaterial.findUniqueOrThrow({
        where: { id: material.id },
        select: { activeRevisionId: true },
      }),
    ).toEqual({ activeRevisionId: activeRevision.id });
    await expect(
      prisma.materialPage.findUniqueOrThrow({
        where: {
          materialRevisionId_pageNumber: {
            materialRevisionId: result.materialRevisionId,
            pageNumber: 1,
          },
        },
        select: {
          embeddedText: true,
          ocrText: true,
          textStatus: true,
          tokenEstimate: true,
        },
      }),
    ).resolves.toEqual({
      embeddedText: null,
      ocrText: "Cached OCR text from the preserved PDF page.",
      textStatus: MaterialPageTextStatus.OCR_READY,
      tokenEstimate: 9,
    });
    const copiedNulls = await prisma.$queryRaw<
      Array<{ pageMetadataIsNull: boolean; sourceMetadataIsNull: boolean }>
    >`
      SELECT
        page."metadata" IS NULL AS "pageMetadataIsNull",
        source."metadata" IS NULL AS "sourceMetadataIsNull"
      FROM "material_pages" page
      INNER JOIN "source_files" source
        ON source."materialRevisionId" = page."materialRevisionId"
      WHERE page."materialRevisionId" = ${result.materialRevisionId}
        AND page."pageNumber" = 1
      LIMIT 1
    `;
    expect(copiedNulls).toEqual([
      { pageMetadataIsNull: true, sourceMetadataIsNull: true },
    ]);
    await expect(
      prisma.materialPage.findUnique({
        where: {
          materialRevisionId_pageNumber: {
            materialRevisionId: result.materialRevisionId,
            pageNumber: 2,
          },
        },
      }),
    ).resolves.toBeNull();

    await prisma.materialPage.update({
      where: {
        materialRevisionId_pageNumber: {
          materialRevisionId: activeRevision.id,
          pageNumber: 2,
        },
      },
      data: {
        ocrText: "OCR text that finished while the replacement was rebuilding.",
        textStatus: MaterialPageTextStatus.OCR_READY,
        contentHash: `sha256:${runId}:finished-reindex-ocr`,
        tokenEstimate: 10,
        metadata: { reason: "lazy-ocr", completedDuringRebuild: true },
      },
    });

    await expect(
      queueMaterialPdfReindex({
        userId,
        materialId: material.id,
        now: new Date("2026-08-06T20:00:01.000Z"),
        storage,
        eventSender: { async sendMaterialIngestionRequested() {} },
      }),
    ).resolves.toMatchObject({
      status: "not-queued",
      message: expect.stringMatching(/already/i),
    });

    await expect(
      runMaterialIngestionJob({
        userId,
        materialRevisionId: result.materialRevisionId,
        storage,
        embeddingGenerator: async ({ texts }) =>
          texts.map(() =>
            Array.from({ length: MATERIAL_EMBEDDING_DIMENSIONS }, (_, index) =>
              index === 0 ? 1 : 0,
            ),
          ),
        summaryGenerator: null,
      }),
    ).resolves.toMatchObject({ status: "ready", pageCount: 2 });
    expect(
      await prisma.studyMaterial.findUniqueOrThrow({
        where: { id: material.id },
        select: { activeRevisionId: true },
      }),
    ).toEqual({ activeRevisionId: result.materialRevisionId });
    await expect(
      prisma.materialPage.findUniqueOrThrow({
        where: {
          materialRevisionId_pageNumber: {
            materialRevisionId: result.materialRevisionId,
            pageNumber: 1,
          },
        },
        select: {
          embeddedText: true,
          ocrText: true,
          textStatus: true,
          tokenEstimate: true,
        },
      }),
    ).resolves.toEqual({
      embeddedText:
        "Embedded overview text that does not contain the cached visual lesson topic.",
      ocrText: "Cached OCR text from the preserved PDF page.",
      textStatus: MaterialPageTextStatus.OCR_READY,
      tokenEstimate: 9,
    });
    await expect(
      prisma.materialPage.findUniqueOrThrow({
        where: {
          materialRevisionId_pageNumber: {
            materialRevisionId: result.materialRevisionId,
            pageNumber: 2,
          },
        },
        select: { ocrText: true, textStatus: true, tokenEstimate: true },
      }),
    ).resolves.toEqual({
      ocrText: "OCR text that finished while the replacement was rebuilding.",
      textStatus: MaterialPageTextStatus.OCR_READY,
      tokenEstimate: 10,
    });
    const rebuiltIndex = await prisma.materialRevision.findUniqueOrThrow({
      where: { id: result.materialRevisionId },
      select: { processingMetadata: true, chunks: { select: { text: true } } },
    });
    expect(rebuiltIndex.processingMetadata).toMatchObject({ embeddingStatus: "ready" });
    expect(rebuiltIndex.chunks.some((chunk) => chunk.text.includes("Cached OCR text"))).toBe(
      true,
    );
    expect(
      rebuiltIndex.chunks.some((chunk) =>
        chunk.text.includes("OCR text that finished while the replacement was rebuilding"),
      ),
    ).toBe(true);
    expect(
      (await getMaterialDetail({ userId, materialId: material.id }))?.linkedSkills.map(
        (skill) => skill.id,
      ),
    ).toContain(linkedSkill.id);
  });

  it("rejects a rebuild when the preserved PDF bytes changed after queueing", async () => {
    const original = await PDFDocument.create();
    original.addPage([612, 792]).drawText("Original preserved lesson.");
    const originalBytes = Buffer.from(await original.save());
    const replacement = await PDFDocument.create();
    replacement.addPage([612, 792]).drawText("Different replaced lesson.");
    const replacementBytes = Buffer.from(await replacement.save());
    const storage = createMemoryStorage();
    const objectKey = `${runId}/changed-reindex/original.pdf`;
    storage.objects.set(objectKey, originalBytes);
    const material = await prisma.studyMaterial.create({
      data: {
        userId,
        title: "Changed preserved PDF",
        kind: StudyMaterialKind.PDF,
        revisions: {
          create: {
            revisionNumber: 1,
            storageBucket: storage.bucketName,
            storageKey: objectKey,
            sourceFiles: {
              create: {
                kind: SourceFileKind.PDF,
                status: SourceFileStatus.READY,
                originalName: "changed-preserved.pdf",
                mimeType: "application/pdf",
                byteSize: originalBytes.byteLength,
                storageBucket: storage.bucketName,
                storageKey: objectKey,
              },
            },
          },
        },
      },
      include: { revisions: true },
    });
    const activeRevision = material.revisions[0];
    await finalizeMaterialRevision({
      userId,
      materialId: material.id,
      materialRevisionId: activeRevision.id,
      contentHash: createHash("sha256").update(originalBytes).digest("hex"),
      byteSize: originalBytes.byteLength,
      pageCount: 1,
      storageBucket: storage.bucketName,
      storageKey: objectKey,
    });
    const queued = await queueMaterialPdfReindex({
      userId,
      materialId: material.id,
      now: new Date("2026-08-06T22:00:00.000Z"),
      storage,
      eventSender: { async sendMaterialIngestionRequested() {} },
    });
    expect(queued.status).toBe("queued");
    if (queued.status !== "queued") {
      throw new Error("expected queued PDF reindex");
    }
    storage.objects.set(objectKey, replacementBytes);

    await expect(
      runMaterialIngestionJob({
        userId,
        materialRevisionId: queued.materialRevisionId,
        storage,
        embeddingGenerator: null,
        summaryGenerator: null,
      }),
    ).rejects.toThrow(/saved PDF changed/i);
    await expect(
      prisma.studyMaterial.findUniqueOrThrow({
        where: { id: material.id },
        select: { activeRevisionId: true },
      }),
    ).resolves.toEqual({ activeRevisionId: activeRevision.id });
  });

  it("allows a new PDF rebuild after publication retries exhaust", async () => {
    const document = await PDFDocument.create();
    document.addPage([612, 792]);
    const bytes = Buffer.from(await document.save());
    const storage = createMemoryStorage();
    const objectKey = `${runId}/failed-reindex/original.pdf`;
    storage.objects.set(objectKey, bytes);
    storage.headObject = async ({ key }) => {
      const stored = storage.objects.get(key);
      return {
        byteSize: stored?.byteLength ?? null,
        mimeType: stored ? " Application/PDF ; charset=binary" : null,
      };
    };
    const material = await prisma.studyMaterial.create({
      data: {
        userId,
        title: "Retryable failed PDF rebuild",
        kind: StudyMaterialKind.PDF,
        revisions: {
          create: {
            revisionNumber: 1,
            storageBucket: storage.bucketName,
            storageKey: objectKey,
            sourceFiles: {
              create: {
                kind: SourceFileKind.PDF,
                status: SourceFileStatus.READY,
                originalName: "retryable.pdf",
                mimeType: "application/pdf",
                byteSize: bytes.byteLength,
                storageBucket: storage.bucketName,
                storageKey: objectKey,
              },
            },
          },
        },
      },
      include: { revisions: true },
    });
    await finalizeMaterialRevision({
      userId,
      materialId: material.id,
      materialRevisionId: material.revisions[0].id,
      contentHash: `sha256:${runId}:retryable-rebuild`,
      byteSize: bytes.byteLength,
      pageCount: 1,
      storageBucket: storage.bucketName,
      storageKey: objectKey,
    });

    await expect(
      queueMaterialPdfReindex({
        userId,
        materialId: material.id,
        now: new Date("2026-08-06T23:00:00.000Z"),
        storage,
        eventSender: {
          async sendMaterialIngestionRequested() {
            throw new Error("fixture send failure");
          },
        },
      }),
    ).resolves.toMatchObject({ status: "queued" });

    const pendingRebuild = await prisma.materialRevision.findFirstOrThrow({ where: { materialId: material.id, revisionNumber: 2 } });
    for (let attempt = 1; attempt < 5; attempt += 1) {
      await publishMaterialIngestion({
        userId, materialRevisionId: pendingRebuild.id,
        now: new Date(new Date("2026-08-06T23:00:00Z").getTime() + attempt * 360_000),
        sender: { async sendMaterialIngestionRequested() { throw new Error("synthetic queue outage"); } },
      });
    }
    const sentRevisionIds: string[] = [];
    const retried = await queueMaterialPdfReindex({
      userId,
      materialId: material.id,
      now: new Date("2026-08-06T23:00:01.000Z"),
      storage,
      eventSender: {
        async sendMaterialIngestionRequested(payload) {
          sentRevisionIds.push(payload.materialRevisionId);
        },
      },
    });

    expect(retried).toMatchObject({ status: "queued", materialId: material.id });
    if (retried.status !== "queued") {
      throw new Error("expected replacement rebuild to queue");
    }
    expect(sentRevisionIds).toEqual([retried.materialRevisionId]);
    await expect(
      prisma.materialRevision.findMany({
        where: { materialId: material.id },
        orderBy: { revisionNumber: "asc" },
        select: { revisionNumber: true, status: true },
      }),
    ).resolves.toEqual([
      { revisionNumber: 1, status: MaterialRevisionStatus.READY },
      { revisionNumber: 2, status: MaterialRevisionStatus.FAILED },
      { revisionNumber: 3, status: MaterialRevisionStatus.QUEUED },
    ]);
  });

  it("preserves a rebuild source when the worker claims an ambiguously delivered event", async () => {
    const document = await PDFDocument.create();
    document.addPage([612, 792]);
    const bytes = Buffer.from(await document.save());
    const storage = createMemoryStorage();
    const objectKey = `${runId}/ambiguous-reindex/original.pdf`;
    storage.objects.set(objectKey, bytes);
    const material = await prisma.studyMaterial.create({
      data: {
        userId,
        title: "Ambiguously delivered PDF rebuild",
        kind: StudyMaterialKind.PDF,
        revisions: {
          create: {
            revisionNumber: 1,
            storageBucket: storage.bucketName,
            storageKey: objectKey,
            sourceFiles: {
              create: {
                kind: SourceFileKind.PDF,
                status: SourceFileStatus.READY,
                originalName: "ambiguous.pdf",
                mimeType: "application/pdf",
                byteSize: bytes.byteLength,
                storageBucket: storage.bucketName,
                storageKey: objectKey,
              },
            },
          },
        },
      },
      include: { revisions: true },
    });
    await finalizeMaterialRevision({
      userId,
      materialId: material.id,
      materialRevisionId: material.revisions[0].id,
      contentHash: createHash("sha256").update(bytes).digest("hex"),
      byteSize: bytes.byteLength,
      pageCount: 1,
      storageBucket: storage.bucketName,
      storageKey: objectKey,
    });

    const result = await queueMaterialPdfReindex({
      userId,
      materialId: material.id,
      now: new Date("2026-08-06T23:30:00.000Z"),
      storage,
      eventSender: {
        async sendMaterialIngestionRequested(payload) {
          await prisma.materialRevision.update({
            where: { id: payload.materialRevisionId },
            data: { status: MaterialRevisionStatus.PROCESSING },
          });
          throw new Error("response lost after the worker claimed the event");
        },
      },
    });

    expect(result).toMatchObject({ status: "queued" });
    const replacement = await prisma.materialRevision.findFirstOrThrow({
      where: { materialId: material.id, revisionNumber: 2 },
      include: { sourceFiles: true },
    });
    expect(replacement.status).toBe(MaterialRevisionStatus.PROCESSING);
    expect(replacement.sourceFiles).toEqual([
      expect.objectContaining({ status: SourceFileStatus.UPLOADED }),
    ]);
  });

  it("does not requeue a revision that is still within the worker pickup window", async () => {
    const now = new Date("2026-07-10T18:00:00.000Z");
    const material = await prisma.studyMaterial.create({
      data: {
        userId,
        title: "Fresh queued reusable PDF",
        kind: StudyMaterialKind.PDF,
        revisions: {
          create: {
            revisionNumber: 1,
            status: MaterialRevisionStatus.QUEUED,
            updatedAt: new Date(now.getTime() - MATERIAL_QUEUE_STALE_AFTER_MS + 1),
          },
        },
      },
      include: { revisions: true },
    });
    let eventCount = 0;

    await expect(
      retryMaterialIngestion({
        userId,
        materialRevisionId: material.revisions[0].id,
        now,
        eventSender: {
          async sendMaterialIngestionRequested() {
            eventCount += 1;
          },
        },
      }),
    ).resolves.toMatchObject({ status: "not-found" });
    expect(eventCount).toBe(0);
  });

  it("applies reusable PDF storage limits without consuming the quick-upload daily quota", async () => {
    const now = new Date();
    const quickUploadPrefix = `${runId}_quick_upload_`;
    await prisma.sourceFile.createMany({
      data: Array.from({ length: ALPHA_SOURCE_UPLOADS_PER_DAY }, (_, index) => ({
        userId,
        kind: SourceFileKind.PDF,
        status: SourceFileStatus.DRAFT,
        originalName: `${quickUploadPrefix}${index + 1}.pdf`,
        mimeType: "application/pdf",
        byteSize: 1,
        createdAt: now,
      })),
    });

    const storage = createMemoryStorage();
    try {
      const prepared = await prepareMaterialPdf({
        userId,
        now,
        storage,
        input: {
          title: "Reusable PDF after quick uploads",
          originalName: "reusable.pdf",
          mimeType: "application/pdf",
          byteSize: "2048",
        },
      });
      expect(prepared.status).toBe("prepared");
      if (prepared.status !== "prepared") {
        throw new Error("expected reusable PDF preparation");
      }
      await discardPreparedMaterialPdf({
        userId,
        materialId: prepared.materialId,
        materialRevisionId: prepared.materialRevisionId,
        storage,
      });
    } finally {
      await prisma.sourceFile.deleteMany({
        where: { userId, originalName: { startsWith: quickUploadPrefix } },
      });
    }

    const quotaMaterial = await prisma.studyMaterial.create({
      data: {
        userId,
        title: "Stored reusable PDF quota fixture",
        kind: StudyMaterialKind.PDF,
        revisions: {
          create: {
            revisionNumber: 1,
            sourceFiles: {
              create: {
                kind: SourceFileKind.PDF,
                status: SourceFileStatus.UPLOADED,
                originalName: "stored-material.pdf",
                mimeType: "application/pdf",
                byteSize: ALPHA_STORED_SOURCE_BYTES,
                storageBucket: "test-materials",
                storageKey: `materials/${userId}/stored-material.pdf`,
              },
            },
          },
        },
      },
    });

    try {
      await expect(
        prepareMaterialPdf({
          userId,
          now,
          storage,
          input: {
            title: "Over reusable storage quota",
            originalName: "over-quota.pdf",
            mimeType: "application/pdf",
            byteSize: "1",
          },
        }),
      ).resolves.toMatchObject({
        status: "not-prepared",
        message: expect.stringMatching(/250 MB/),
      });
    } finally {
      await prisma.studyMaterial.delete({ where: { id: quotaMaterial.id } });
    }
  });

  it("rejects a long PDF in quick create on the server", async () => {
    const document = await PDFDocument.create();
    for (let page = 0; page < 21; page += 1) {
      document.addPage();
    }
    const bytes = Buffer.from(await document.save());
    const storage = createMemoryStorage();
    const prepared = await prepareSourceUpload({
      userId,
      now: new Date(),
      storage,
      input: {
        originalName: "long-quick-create.pdf",
        mimeType: "application/pdf",
        byteSize: String(bytes.byteLength),
      },
    });
    expect(prepared.status).toBe("prepared");
    if (prepared.status !== "prepared") {
      throw new Error("expected prepared quick upload");
    }
    storage.objects.set(storage.lastPreparedKey, bytes);

    await expect(
      queueSourceUploadDrafts({
        userId,
        sourceFileId: prepared.sourceFileId,
        now: new Date(),
        storage,
        eventSender: { async sendSourceUploadDraftRequested() {} },
      }),
    ).resolves.toMatchObject({
      status: "not-queued",
      reason: "invalid-upload",
      message: expect.stringMatching(/over 20 pages.*Materials/i),
    });
    expect(await prisma.sourceFile.count({ where: { id: prepared.sourceFileId } })).toBe(0);
    expect(storage.deleted).toContain(prepared.objectKey);
  });

  it("keeps a completed website import when event delivery fails after the worker finishes", async () => {
    const storage = createMemoryStorage();
    const queued = await queueWebsiteMaterialImport({
      userId,
      now: new Date(),
      storage,
      eventSender: {
        async sendMaterialIngestionRequested(payload) {
          await prisma.sourceFile.updateMany({
            where: { materialRevisionId: payload.materialRevisionId, userId },
            data: { status: SourceFileStatus.READY },
          });
          await prisma.materialRevision.update({
            where: { id: payload.materialRevisionId },
            data: { status: MaterialRevisionStatus.READY },
          });
          throw new Error("response lost after the worker finished");
        },
      },
      input: {
        title: "Ambiguous website import",
        sourceUrl: "https://books.example/ambiguous-book",
        selectedUrls: ["https://books.example/ambiguous-chapter"],
      },
    });

    expect(queued).toMatchObject({ status: "queued" });
    const revision = await prisma.materialRevision.findFirstOrThrow({
      where: { userId, sourceUrl: "https://books.example/ambiguous-book" },
      select: { status: true, errorCode: true, sourceFiles: { select: { status: true } } },
    });
    expect(revision).toEqual({
      status: MaterialRevisionStatus.READY,
      errorCode: null,
      sourceFiles: [{ status: SourceFileStatus.READY }],
    });
  });

  it("keeps a completed website refresh when event delivery fails after the worker finishes", async () => {
    const storage = createMemoryStorage();
    const initial = await queueWebsiteMaterialImport({
      userId,
      now: new Date(),
      storage,
      eventSender: { async sendMaterialIngestionRequested() {} },
      input: {
        title: "Ambiguous website refresh",
        sourceUrl: "https://books.example/refresh-book",
        selectedUrls: ["https://books.example/refresh-chapter"],
      },
    });
    expect(initial.status).toBe("queued");
    if (initial.status !== "queued") throw new Error("expected queued website import");
    await prisma.$transaction([
      prisma.materialRevision.update({
        where: { id: initial.materialRevisionId },
        data: { status: MaterialRevisionStatus.READY },
      }),
      prisma.sourceFile.updateMany({
        where: { materialRevisionId: initial.materialRevisionId, userId },
        data: { status: SourceFileStatus.READY },
      }),
      prisma.studyMaterial.update({
        where: { id: initial.materialId },
        data: { activeRevisionId: initial.materialRevisionId },
      }),
    ]);

    const refreshed = await queueWebsiteMaterialRefresh({
      userId,
      materialId: initial.materialId,
      now: new Date(),
      storage,
      eventSender: {
        async sendMaterialIngestionRequested(payload) {
          await prisma.sourceFile.updateMany({
            where: { materialRevisionId: payload.materialRevisionId, userId },
            data: { status: SourceFileStatus.READY },
          });
          await prisma.materialRevision.update({
            where: { id: payload.materialRevisionId },
            data: { status: MaterialRevisionStatus.READY },
          });
          throw new Error("response lost after the worker finished");
        },
      },
    });

    expect(refreshed).toMatchObject({ status: "queued" });
    const revision = await prisma.materialRevision.findFirstOrThrow({
      where: { id: { not: initial.materialRevisionId }, materialId: initial.materialId },
      orderBy: { revisionNumber: "desc" },
      select: { status: true, errorCode: true, sourceFiles: { select: { status: true } } },
    });
    expect(revision).toEqual({
      status: MaterialRevisionStatus.READY,
      errorCode: null,
      sourceFiles: [{ status: SourceFileStatus.READY }],
    });
  });

  it("snapshots selected same-origin website pages and builds URL locators", async () => {
    const storage = createMemoryStorage();
    const sentEvents: string[] = [];
    const queued = await queueWebsiteMaterialImport({
      userId,
      now: new Date(),
      storage,
      eventSender: {
        async sendMaterialIngestionRequested(payload) {
          sentEvents.push(payload.materialRevisionId);
        },
      },
      input: {
        title: "Open Grammar",
        sourceUrl: "https://books.example/open-grammar",
        selectedUrls: [
          "https://books.example/chapter-1",
          "https://books.example/chapter-2",
        ],
      },
    });
    expect(queued.status).toBe("queued");
    if (queued.status !== "queued") {
      throw new Error("expected queued website import");
    }

    const result = await runMaterialIngestionJob({
      userId,
      materialRevisionId: queued.materialRevisionId,
      storage,
      summaryGenerator: null,
      resolveHostname: async () => ["93.184.216.34"],
      fetchResource: async (url) => ({
        url,
        contentType: "text/html",
        bytes: Buffer.from(`
          <html><head><title>Open Grammar</title></head><body>
            <main><h1 id="topic">${url.endsWith("1") ? "Noun agreement" : "Verb agreement"}</h1>
            <p>Agreement rules connect grammatical forms in complete sentences. This chapter
            provides several concrete examples and short explanations for practice.</p></main>
          </body></html>`),
      }),
      embeddingGenerator: null,
    });
    expect(result).toMatchObject({ status: "ready", pageCount: 2 });
    expect(sentEvents).toEqual([queued.materialRevisionId]);
    expect(storage.puts).toHaveLength(1);

    const revision = await prisma.materialRevision.findUniqueOrThrow({
      where: { id: queued.materialRevisionId },
      include: { chunks: true, sections: true },
    });
    expect(revision.status).toBe(MaterialRevisionStatus.READY);
    expect(revision.sections.map((section) => section.url)).toEqual([
      "https://books.example/chapter-1",
      "https://books.example/chapter-2",
    ]);
    expect(revision.chunks.map((chunk) => chunk.locator)).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          kind: "web",
          url: "https://books.example/chapter-1",
        }),
        expect.objectContaining({
          kind: "web",
          url: "https://books.example/chapter-2",
        }),
      ]),
    );
  });

  it("tracks a website snapshot before a later index failure and cleans it up", async () => {
    const storage = createMemoryStorage();
    const queued = await queueWebsiteMaterialImport({
      userId,
      now: new Date(),
      storage,
      eventSender: { async sendMaterialIngestionRequested() {} },
      input: {
        title: "Website index failure",
        sourceUrl: "https://books.example/index-failure-book",
        selectedUrls: ["https://books.example/index-failure-chapter"],
      },
    });
    expect(queued.status).toBe("queued");
    if (queued.status !== "queued") throw new Error("expected queued website import");

    const triggerBase = `test_midx_${randomUUID().replaceAll("-", "").slice(0, 24)}`;
    const functionName = `${triggerBase}_fn`;
    const triggerName = `${triggerBase}_trigger`;
    await prisma.$executeRawUnsafe(`
      CREATE FUNCTION "${functionName}"() RETURNS trigger
      LANGUAGE plpgsql AS $$
      BEGIN
        IF NEW."userId" = '${userId}' THEN
          RAISE EXCEPTION 'fixture website index failure';
        END IF;
        RETURN NEW;
      END;
      $$;
    `);
    await prisma.$executeRawUnsafe(`
      CREATE TRIGGER "${triggerName}"
      BEFORE INSERT ON "material_sections"
      FOR EACH ROW EXECUTE FUNCTION "${functionName}"();
    `);

    let writtenKey = "";
    try {
      await expect(
        runMaterialIngestionJob({
          userId,
          materialRevisionId: queued.materialRevisionId,
          storage,
          resolveHostname: async () => ["93.184.216.34"],
          fetchResource: async (url) => ({
            url,
            contentType: "text/html",
            bytes: Buffer.from(
              "<html><body><main><h1>Index failure chapter</h1><p>This page has enough readable textbook content to write the private snapshot before the index fixture fails.</p></main></body></html>",
            ),
          }),
          embeddingGenerator: null,
          summaryGenerator: null,
        }),
      ).rejects.toThrow(/fixture website index failure/i);

      expect(storage.puts).toHaveLength(1);
      writtenKey = storage.puts[0].key;
      expect(storage.objects.has(writtenKey)).toBe(true);
      const sourceFile = await prisma.sourceFile.findFirstOrThrow({
        where: { materialRevisionId: queued.materialRevisionId, userId },
        select: { status: true, byteSize: true, storageKey: true },
      });
      expect(sourceFile).toEqual({
        status: SourceFileStatus.FAILED,
        byteSize: storage.puts[0].bytes.byteLength,
        storageKey: writtenKey,
      });
    } finally {
      await prisma.$executeRawUnsafe(`DROP TRIGGER IF EXISTS "${triggerName}" ON "material_sections";`);
      await prisma.$executeRawUnsafe(`DROP FUNCTION IF EXISTS "${functionName}"();`);
    }

    const deletion = await queueMaterialDeletion({
      userId,
      materialId: queued.materialId,
      confirmationTitle: "Website index failure",
      now: new Date(),
      eventSender: { async sendMaterialCleanupRequested() {} },
    });
    expect(deletion.status).toBe("queued");
    if (deletion.status !== "queued") throw new Error("expected queued cleanup");
    await expect(
      runMaterialCleanupJob({
        userId,
        materialId: queued.materialId,
        cleanupJobId: deletion.cleanupJobId,
        storage,
      }),
    ).resolves.toMatchObject({ status: "deleted" });
    expect(storage.objects.has(writtenKey)).toBe(false);
  });

  it("serializes website snapshot quota reservations for concurrent workers", async () => {
    const storage = createMemoryStorage();
    const pageUrl = "https://books.example/quota-chapter";
    const pageHtml =
      "<html><body><main><h1>Quota chapter</h1><p>This page contains enough readable textbook content for the concurrent quota reservation fixture.</p></main></body></html>";
    const snapshotBytes = Buffer.from(
      JSON.stringify({
        version: 1,
        pages: [{ url: pageUrl, title: "Quota chapter", html: pageHtml }],
      }),
    );
    const existingStorage = await prisma.sourceFile.aggregate({
      where: { userId, storageKey: { not: null } },
      _sum: { byteSize: true },
    });
    const prefillByteSize =
      ALPHA_STORED_SOURCE_BYTES - (existingStorage._sum.byteSize ?? 0) - snapshotBytes.byteLength;
    expect(prefillByteSize).toBeGreaterThan(0);
    const prefill = await prisma.sourceFile.create({
      data: {
        userId,
        kind: SourceFileKind.OTHER,
        status: SourceFileStatus.READY,
        originalName: "quota-prefill.bin",
        mimeType: "application/octet-stream",
        byteSize: prefillByteSize,
        storageBucket: storage.bucketName,
        storageKey: `${runId}/quota-prefill.bin`,
      },
      select: { id: true },
    });

    try {
      const inputs = await Promise.all(
        ["one", "two"].map((suffix) =>
          queueWebsiteMaterialImport({
            userId,
            now: new Date(),
            storage,
            eventSender: { async sendMaterialIngestionRequested() {} },
            input: {
              title: `Concurrent quota ${suffix}`,
              sourceUrl: "https://books.example/quota-book",
              selectedUrls: [pageUrl],
            },
          }),
        ),
      );
      expect(inputs.every((input) => input.status === "queued")).toBe(true);
      const revisionIds = inputs.map((input) => {
        if (input.status !== "queued") throw new Error("expected both quota fixtures to queue");
        return input.materialRevisionId;
      });
      let fetchedCount = 0;
      let releaseFetch!: () => void;
      const fetchBarrier = new Promise<void>((resolve) => {
        releaseFetch = resolve;
      });
      const fetchResource = async (url: string) => {
        fetchedCount += 1;
        if (fetchedCount === revisionIds.length) releaseFetch();
        await fetchBarrier;
        return { url, contentType: "text/html", bytes: Buffer.from(pageHtml) };
      };

      const results = await Promise.allSettled(
        revisionIds.map((materialRevisionId) =>
          runMaterialIngestionJob({
            userId,
            materialRevisionId,
            storage,
            resolveHostname: async () => ["93.184.216.34"],
            fetchResource,
            embeddingGenerator: null,
            summaryGenerator: null,
          }),
        ),
      );
      expect(results.filter((result) => result.status === "fulfilled")).toHaveLength(1);
      expect(results.filter((result) => result.status === "rejected")).toHaveLength(1);
      const rejected = results.find((result) => result.status === "rejected");
      expect(rejected).toMatchObject({
        status: "rejected",
        reason: expect.objectContaining({ message: expect.stringMatching(/250 MB|store/i) }),
      });
      expect(storage.puts).toHaveLength(1);

      const sourceFiles = await prisma.sourceFile.findMany({
        where: { userId, materialRevisionId: { in: revisionIds } },
        select: { status: true, byteSize: true },
      });
      expect(sourceFiles).toEqual(
        expect.arrayContaining([
          { status: SourceFileStatus.READY, byteSize: snapshotBytes.byteLength },
          { status: SourceFileStatus.FAILED, byteSize: null },
        ]),
      );
    } finally {
      await prisma.sourceFile.delete({ where: { id: prefill.id } });
    }
  });

  it("retains a failed website write reservation until a retry replaces it", async () => {
    const storage = createMemoryStorage();
    const pageUrl = "https://books.example/retry-chapter";
    const pageHtml =
      "<html><body><main><h1>Retry chapter</h1><p>This page contains enough readable textbook content for a failed storage write and retry fixture.</p></main></body></html>";
    const snapshotBytes = Buffer.from(
      JSON.stringify({
        version: 1,
        pages: [{ url: pageUrl, title: "Retry chapter", html: pageHtml }],
      }),
    );
    const queued = await queueWebsiteMaterialImport({
      userId,
      now: new Date(),
      storage,
      eventSender: { async sendMaterialIngestionRequested() {} },
      input: {
        title: "Retry website write",
        sourceUrl: "https://books.example/retry-book",
        selectedUrls: [pageUrl],
      },
    });
    expect(queued.status).toBe("queued");
    if (queued.status !== "queued") throw new Error("expected queued website import");

    const successfulPut = storage.putObject?.bind(storage);
    if (!successfulPut) throw new Error("expected snapshot storage");
    storage.putObject = async () => {
      throw new Error("fixture storage write failed");
    };
    await expect(
      runMaterialIngestionJob({
        userId,
        materialRevisionId: queued.materialRevisionId,
        storage,
        resolveHostname: async () => ["93.184.216.34"],
        fetchResource: async (url) => ({
          url,
          contentType: "text/html",
          bytes: Buffer.from(pageHtml),
        }),
        embeddingGenerator: null,
        summaryGenerator: null,
      }),
    ).rejects.toThrow(/fixture storage write failed/i);

    const failedSource = await prisma.sourceFile.findFirstOrThrow({
      where: { materialRevisionId: queued.materialRevisionId, userId },
      select: { status: true, byteSize: true, storageKey: true },
    });
    expect(failedSource).toMatchObject({
      status: SourceFileStatus.FAILED,
      byteSize: snapshotBytes.byteLength,
    });
    expect(storage.objects.size).toBe(0);

    storage.putObject = successfulPut;
    await expect(
      runMaterialIngestionJob({
        userId,
        materialRevisionId: queued.materialRevisionId,
        storage,
        resolveHostname: async () => ["93.184.216.34"],
        fetchResource: async (url) => ({
          url,
          contentType: "text/html",
          bytes: Buffer.from(pageHtml),
        }),
        embeddingGenerator: null,
        summaryGenerator: null,
      }),
    ).resolves.toMatchObject({ status: "ready" });
    const completedSource = await prisma.sourceFile.findFirstOrThrow({
      where: { materialRevisionId: queued.materialRevisionId, userId },
      select: { status: true, byteSize: true, storageKey: true },
    });
    expect(completedSource).toMatchObject({
      status: SourceFileStatus.READY,
      byteSize: snapshotBytes.byteLength,
    });
    expect(storage.puts).toHaveLength(1);
    expect(storage.objects.has(completedSource.storageKey ?? "")).toBe(true);
  });

  it("fetches large website selections in bounded concurrent groups", async () => {
    const storage = createMemoryStorage();
    const selectedUrls = Array.from(
      { length: 20 },
      (_, index) => `https://books.example/concurrent-chapter-${index + 1}`,
    );
    const queued = await queueWebsiteMaterialImport({
      userId,
      now: new Date(),
      storage,
      eventSender: { async sendMaterialIngestionRequested() {} },
      input: {
        title: "Concurrent website import",
        sourceUrl: "https://books.example/concurrent-book",
        selectedUrls,
      },
    });
    expect(queued.status).toBe("queued");
    if (queued.status !== "queued") {
      throw new Error("expected queued website import");
    }

    let inFlight = 0;
    let maximumInFlight = 0;
    const result = await runMaterialIngestionJob({
      userId,
      materialRevisionId: queued.materialRevisionId,
      storage,
      summaryGenerator: null,
      resolveHostname: async () => ["93.184.216.34"],
      fetchResource: async (url, options) => {
        expect(options?.maximumBytes).toBe(5 * 1024 * 1024);
        inFlight += 1;
        maximumInFlight = Math.max(maximumInFlight, inFlight);
        await new Promise((resolve) => setTimeout(resolve, 10));
        inFlight -= 1;
        return {
          url,
          contentType: "text/html",
          bytes: Buffer.from(
            `<html><body><main><h1>${url.split("-").at(-1)}</h1><p>This chapter contains enough readable textbook content for a stable, bounded website import fixture.</p></main></body></html>`,
          ),
        };
      },
      embeddingGenerator: null,
    });

    expect(result).toMatchObject({ status: "ready", pageCount: 20 });
    expect(maximumInFlight).toBe(10);
    expect(
      (
        await prisma.materialSection.findMany({
          where: { materialRevisionId: queued.materialRevisionId },
          orderBy: { ordinal: "asc" },
          select: { url: true },
        })
      ).map((section) => section.url),
    ).toEqual(selectedUrls);
  });

  it("removes a website snapshot written after deletion begins", async () => {
    const storage = createMemoryStorage();
    const title = "Deleted during website snapshot";
    const queued = await queueWebsiteMaterialImport({
      userId,
      now: new Date(),
      storage,
      eventSender: { async sendMaterialIngestionRequested() {} },
      input: {
        title,
        sourceUrl: "https://books.example/deleted-book",
        selectedUrls: ["https://books.example/deleted-chapter"],
      },
    });
    expect(queued.status).toBe("queued");
    if (queued.status !== "queued") {
      throw new Error("expected queued website import");
    }

    const putObject = storage.putObject?.bind(storage);
    if (!putObject) {
      throw new Error("expected snapshot storage");
    }
    let writtenKey = "";
    storage.putObject = async (putInput) => {
      writtenKey = putInput.key;
      await queueMaterialDeletion({
        userId,
        materialId: queued.materialId,
        confirmationTitle: title,
        now: new Date(),
        eventSender: { async sendMaterialCleanupRequested() {} },
      });
      await putObject(putInput);
    };

    await expect(
      runMaterialIngestionJob({
        userId,
        materialRevisionId: queued.materialRevisionId,
        storage,
        summaryGenerator: null,
        resolveHostname: async () => ["93.184.216.34"],
        fetchResource: async (url) => ({
          url,
          contentType: "text/html",
          bytes: Buffer.from(
            "<html><body><main><h1>Deleted chapter</h1><p>This readable page is deleted while its private snapshot is being stored.</p></main></body></html>",
          ),
        }),
        embeddingGenerator: null,
      }),
    ).rejects.toThrow(/deleted/i);
    expect(writtenKey).not.toBe("");
    expect(storage.objects.has(writtenKey)).toBe(false);
    expect(storage.deleted).toContain(writtenKey);
  });

  it("does not write a website snapshot after an account deletion tombstone exists", async () => {
    const storage = createMemoryStorage();
    const queued = await queueWebsiteMaterialImport({
      userId,
      now: new Date(),
      storage,
      eventSender: { async sendMaterialIngestionRequested() {} },
      input: {
        title: "Account deletion blocks snapshot",
        sourceUrl: "https://books.example/account-deletion-book",
        selectedUrls: ["https://books.example/account-deletion-chapter"],
      },
    });
    expect(queued.status).toBe("queued");
    if (queued.status !== "queued") {
      throw new Error("expected queued website import");
    }
    const deletionJob = await prisma.accountDeletionJob.create({
      data: {
        userId,
        manifest: { version: 1, storageObjects: [], agentConnections: [] },
      },
    });

    try {
      await expect(
        runMaterialIngestionJob({
          userId,
          materialRevisionId: queued.materialRevisionId,
          storage,
          summaryGenerator: null,
          resolveHostname: async () => ["93.184.216.34"],
          fetchResource: async (url) => ({
            url,
            contentType: "text/html",
            bytes: Buffer.from(
              "<html><body><main><h1>Blocked chapter</h1><p>This private snapshot must never be written after account deletion starts.</p></main></body></html>",
            ),
          }),
          embeddingGenerator: null,
        }),
      ).rejects.toThrow(/deleted/i);
      expect(storage.puts).toHaveLength(0);
      expect(storage.objects.size).toBe(0);
    } finally {
      await prisma.accountDeletionJob.delete({ where: { id: deletionJob.id } });
    }
  });

  it("removes a website snapshot when account deletion starts during the storage write", async () => {
    const storage = createMemoryStorage();
    const queued = await queueWebsiteMaterialImport({
      userId,
      now: new Date(),
      storage,
      eventSender: { async sendMaterialIngestionRequested() {} },
      input: {
        title: "Concurrent account deletion snapshot",
        sourceUrl: "https://books.example/concurrent-deletion-book",
        selectedUrls: ["https://books.example/concurrent-deletion-chapter"],
      },
    });
    expect(queued.status).toBe("queued");
    if (queued.status !== "queued") throw new Error("expected queued website import");
    const sourceFile = await prisma.sourceFile.findFirstOrThrow({
      where: { materialRevisionId: queued.materialRevisionId, userId },
      select: { id: true },
    });
    const putObject = storage.putObject?.bind(storage);
    if (!putObject) throw new Error("expected snapshot storage");
    let deletionJobId: string | null = null;
    storage.putObject = async (putInput) => {
      const job = await prisma.accountDeletionJob.create({
        data: {
          userId,
          manifest: { version: 1, storageObjects: [], agentConnections: [] },
        },
      });
      deletionJobId = job.id;
      await putObject(putInput);
    };

    try {
      await expect(
        runMaterialIngestionJob({
          userId,
          materialRevisionId: queued.materialRevisionId,
          storage,
          summaryGenerator: null,
          resolveHostname: async () => ["93.184.216.34"],
          fetchResource: async (url) => ({
            url,
            contentType: "text/html",
            bytes: Buffer.from(
              "<html><body><main><h1>Concurrent deletion</h1><p>This snapshot is removed because account deletion starts during its write.</p></main></body></html>",
            ),
          }),
          embeddingGenerator: null,
        }),
      ).rejects.toThrow(/deleted/i);
      expect(storage.puts).toHaveLength(1);
      expect(storage.objects.size).toBe(0);
      await expect(
        prisma.sourceFile.findUniqueOrThrow({
          where: { id: sourceFile.id },
          select: { presignedUploadExpiresAt: true },
        }),
      ).resolves.toEqual({ presignedUploadExpiresAt: null });
    } finally {
      if (deletionJobId) {
        await prisma.accountDeletionJob.delete({ where: { id: deletionJobId } });
      }
    }
  });

  it("does not create a website refresh revision when the background queue is unavailable", async () => {
    const material = await prisma.studyMaterial.findFirstOrThrow({
      where: { userId, title: "Open Grammar" },
    });
    const revisionCount = await prisma.materialRevision.count({
      where: { materialId: material.id },
    });
    const previousEnv = {
      NODE_ENV: process.env.NODE_ENV,
      JOBS_QUEUE_URL: process.env.JOBS_QUEUE_URL,
    };
    Object.assign(process.env, { NODE_ENV: "production" });
    delete process.env.JOBS_QUEUE_URL;

    try {
      await expect(
        queueWebsiteMaterialRefresh({
          userId,
          materialId: material.id,
          now: new Date(),
          storage: createMemoryStorage(),
        }),
      ).resolves.toMatchObject({ status: "not-queued", message: expect.stringMatching(/Background jobs/) });
      await expect(
        prisma.materialRevision.count({ where: { materialId: material.id } }),
      ).resolves.toBe(revisionCount);
    } finally {
      restoreEnv("NODE_ENV", previousEnv.NODE_ENV);
      restoreEnv("JOBS_QUEUE_URL", previousEnv.JOBS_QUEUE_URL);
    }
  });

  it("refreshes a website into a new immutable revision without moving existing links", async () => {
    const material = await prisma.studyMaterial.findFirstOrThrow({
      where: { userId, title: "Open Grammar" },
      include: { activeRevision: { include: { sourceFiles: true } } },
    });
    const activeRevisionId = material.activeRevisionId;
    const sourceFile = material.activeRevision?.sourceFiles[0];
    if (!activeRevisionId || !sourceFile) {
      throw new Error("expected active website revision");
    }
    const skill = await prisma.skill.create({
      data: { userId, title: "Web revision link", tags: [], status: SkillStatus.DRAFT },
    });
    await prisma.skillSourceRef.create({
      data: { userId, skillId: skill.id, sourceFileId: sourceFile.id },
    });
    const refreshed = await queueWebsiteMaterialRefresh({
      userId,
      materialId: material.id,
      now: new Date(),
      storage: createMemoryStorage(),
      eventSender: { async sendMaterialIngestionRequested() {} },
    });
    expect(refreshed).toMatchObject({ status: "queued" });
    if (refreshed.status !== "queued") {
      throw new Error("expected queued refresh");
    }
    const revisions = await prisma.materialRevision.findMany({
      where: { materialId: material.id },
      orderBy: { revisionNumber: "asc" },
    });
    expect(revisions.map((revision) => revision.revisionNumber)).toEqual([1, 2]);
    const detail = await getMaterialDetail({ userId, materialId: material.id });
    expect(detail?.activeRevision?.id).toBe(activeRevisionId);
    expect(detail?.currentRevision).toMatchObject({
      id: refreshed.materialRevisionId,
      status: MaterialRevisionStatus.QUEUED,
    });
    expect(
      (await getMaterialLibrary({ userId })).find((item) => item.id === material.id),
    ).toMatchObject({
      revisionNumber: 1,
      revisionStatus: MaterialRevisionStatus.READY,
      linkedSkillCount: 1,
    });
    expect(
      await prisma.skillSourceRef.findFirstOrThrow({
        where: { skillId: skill.id },
        select: { sourceFile: { select: { materialRevisionId: true } } },
      }),
    ).toEqual({ sourceFile: { materialRevisionId: activeRevisionId } });
  });

  it("omits materials that do not have a ready active revision", async () => {
    const material = await prisma.studyMaterial.create({
      data: {
        userId,
        title: "Unfinished hidden material",
        kind: StudyMaterialKind.PDF,
        revisions: {
          create: {
            revisionNumber: 1,
            status: MaterialRevisionStatus.FAILED,
            errorCode: "EXTRACTION_FAILED",
            errorMessage: "The test import did not finish.",
          },
        },
      },
      select: { id: true },
    });

    expect(
      (await getMaterialLibrary({ userId })).some((item) => item.id === material.id),
    ).toBe(false);
  });

  it("deletes material objects and source links idempotently without deleting linked skills", async () => {
    const material = await prisma.studyMaterial.findFirstOrThrow({
      where: { userId, title: "Open Grammar" },
      include: { activeRevision: { include: { sourceFiles: true } } },
    });
    const sourceFile = material.activeRevision?.sourceFiles[0];
    if (!sourceFile) {
      throw new Error("expected material source");
    }
    const skill = await prisma.skill.create({
      data: { userId, title: "Agreement rules", tags: [], status: SkillStatus.ACTIVE },
    });
    await prisma.skillSourceRef.create({
      data: { userId, skillId: skill.id, sourceFileId: sourceFile.id },
    });
    const storage = createMemoryStorage();
    const cleanupEvents: Array<{ materialId: string; cleanupJobId: string }> = [];
    const queued = await queueMaterialDeletion({
      userId,
      materialId: material.id,
      confirmationTitle: material.title,
      now: new Date(),
      eventSender: {
        async sendMaterialCleanupRequested(payload) {
          cleanupEvents.push(payload);
        },
      },
    });
    expect(queued.status).toBe("queued");
    if (queued.status !== "queued") {
      throw new Error("expected queued cleanup");
    }

    expect(
      await runMaterialCleanupJob({
        userId,
        materialId: material.id,
        cleanupJobId: queued.cleanupJobId,
        storage,
      }),
    ).toMatchObject({ status: "deleted" });
    expect(await prisma.skill.count({ where: { id: skill.id } })).toBe(1);
    expect(await prisma.skillSourceRef.count({ where: { skillId: skill.id } })).toBe(0);
    expect(
      await runMaterialCleanupJob({
        userId,
        materialId: material.id,
        cleanupJobId: queued.cleanupJobId,
        storage,
      }),
    ).toEqual({ status: "already-clean" });
  });

  it("restores a material when cleanup event delivery fails", async () => {
    const material = await prisma.studyMaterial.findFirstOrThrow({
      where: { userId, title: "Practical Spanish Grammar" },
      include: { activeRevision: true },
    });
    const activeRevision = material.activeRevision;
    if (!activeRevision) {
      throw new Error("expected active PDF revision");
    }

    await expect(
      queueMaterialDeletion({
        userId,
        materialId: material.id,
        confirmationTitle: material.title,
        now: new Date(),
        eventSender: {
          async sendMaterialCleanupRequested() {
            throw new Error("AWS background jobs unavailable");
          },
        },
      }),
    ).resolves.toMatchObject({ status: "not-deleted", reason: "queue-unavailable" });
    await expect(
      prisma.studyMaterial.findUniqueOrThrow({
        where: { id: material.id },
        select: { status: true, activeRevisionId: true, cleanupJob: true },
      }),
    ).resolves.toMatchObject({
      status: material.status,
      activeRevisionId: activeRevision.id,
      cleanupJob: null,
    });
    await expect(
      prisma.materialRevision.findUniqueOrThrow({
        where: { id: activeRevision.id },
        select: { status: true },
      }),
    ).resolves.toEqual({ status: activeRevision.status });
  });

});

type MemoryStorage = SourceObjectStorage & {
  objects: Map<string, Buffer>;
  puts: Array<{ key: string; bytes: Buffer }>;
  deleted: string[];
  lastPreparedKey: string;
};

function createMemoryStorage(): MemoryStorage {
  const objects = new Map<string, Buffer>();
  const puts: Array<{ key: string; bytes: Buffer }> = [];
  const deleted: string[] = [];
  const storage: MemoryStorage = {
    bucketName: "test-materials",
    objects,
    puts,
    deleted,
    lastPreparedKey: "",
    async createPresignedUploadUrl({ key }) {
      storage.lastPreparedKey = key;
      return `https://uploads.example/${encodeURIComponent(key)}`;
    },
    async headObject({ key }) {
      const bytes = objects.get(key);
      return { byteSize: bytes?.byteLength ?? null, mimeType: bytes ? "application/pdf" : null };
    },
    async getObjectBytes({ key }) {
      const bytes = objects.get(key);
      if (!bytes) {
        throw new Error("missing test object");
      }
      return bytes;
    },
    async putObject({ key, bytes }) {
      objects.set(key, bytes);
      puts.push({ key, bytes });
    },
    async listObjects() {
      return [...objects.keys()];
    },
    async deleteObject({ key }) {
      objects.delete(key);
      deleted.push(key);
    },
  };
  return storage;
}

function restoreEnv(name: string, value: string | undefined) {
  if (value === undefined) {
    delete process.env[name];
    return;
  }
  process.env[name] = value;
}
