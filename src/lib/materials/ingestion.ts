import "server-only";

import { createHash, randomUUID } from "node:crypto";

import {
  MaterialPageTextStatus,
  MaterialRevisionStatus,
  Prisma,
  SourceFileKind,
  SourceFileStatus,
  StudyMaterialKind,
  StudyMaterialStatus,
} from "@/generated/prisma/client";
import { getJobsEnvStatus } from "@/lib/jobs/config";
import {
  type MaterialIngestionEventSender,
} from "@/lib/jobs/events";
import {
  MAX_MATERIAL_PDF_BYTES,
  MAX_MATERIAL_PDF_PAGES,
  MAX_WEBSITE_REVISION_BYTES,
  confirmWebsiteImportInputSchema,
  prepareMaterialPdfInputSchema,
} from "@/lib/materials/contracts";
import { MATERIAL_QUEUE_STALE_AFTER_MS } from "@/lib/materials/ingestion-status";
import { createMaterialIngestionDelivery, publishMaterialIngestion } from "@/lib/materials/ingestion-publication";
import {
  createGeminiMaterialEmbeddingGenerator,
  type MaterialEmbeddingGenerator,
} from "@/lib/materials/embeddings";
import {
  createNextMaterialRevision,
  finalizeMaterialRevision,
} from "@/lib/materials/lifecycle";
import { readyMaterialOcrPageQuery } from "@/lib/materials/ocr-cache";
import {
  buildPdfIndex,
  chunkSectionText,
  estimateTokens,
  extractPdfPages,
  type ExtractedPdfPage,
} from "@/lib/materials/pdf";
import { materialPdfErrorMessage } from "@/lib/materials/pdf-upload";
import { storeMaterialChunkEmbedding } from "@/lib/materials/retrieval";
import { createGeminiMaterialSummaryGenerator } from "@/lib/materials/summary-ai";
import {
  buildStoredMaterialSummary,
  materialSummaryResponseSchema,
  type MaterialSummaryGenerator,
} from "@/lib/materials/summary";
import {
  extractReadableWebPage,
  fetchPublicWebResource,
  validatePublicHttpsUrl,
  type FetchWebResource,
  type ResolveHostname,
} from "@/lib/materials/web";
import { getPrisma } from "@/lib/prisma";
import {
  resolveS3SourceObjectStorage,
  type SourceObjectStorage,
} from "@/lib/storage/s3";
import {
  checkSourceStorageUsageLimit,
} from "@/lib/usage-limits";

const MATERIAL_STORAGE_PREFIX = "source-uploads/materials";
const MATERIAL_UPLOAD_URL_EXPIRES_IN_SECONDS = 600;
const MATERIAL_UPLOAD_LEASE_SAFETY_MS = 30_000;
const MATERIAL_EMBEDDING_BATCH_SIZE = 32;
const MATERIAL_SOURCE_EXCERPT_LIMIT = 4_000;
const MATERIAL_WEBSITE_FETCH_CONCURRENCY = 16;
const MATERIAL_WEBSITE_PAGE_FETCH_BYTES = 5 * 1024 * 1024;

export class MaterialIngestionError extends Error {
  readonly retryable: boolean;

  constructor(message: string, options: { retryable: boolean; cause?: unknown }) {
    super(message, { cause: options.cause });
    this.name = "MaterialIngestionError";
    this.retryable = options.retryable;
  }
}

export type PrepareMaterialPdfResult =
  | {
      status: "prepared";
      materialId: string;
      materialRevisionId: string;
      sourceFileId: string;
      uploadUrl: string;
      headers: Record<string, string>;
      expiresInSeconds: number;
    }
  | {
      status: "invalid" | "not-prepared";
      message: string;
      fieldErrors?: Record<string, string[]>;
    };

export type QueueMaterialIngestionResult =
  | {
      status: "queued";
      materialId: string;
      materialRevisionId: string;
      message: string;
    }
  | {
      status: "not-found" | "not-queued";
      message: string;
    };

export async function prepareMaterialPdf(input: {
  userId: string;
  now: Date;
  input: unknown;
  storage?: SourceObjectStorage;
}): Promise<PrepareMaterialPdfResult> {
  const parsed = prepareMaterialPdfInputSchema.safeParse(input.input);
  if (!parsed.success) {
    const fieldErrors = parsed.error.flatten().fieldErrors;
    return {
      status: "invalid",
      message: materialPdfErrorMessage(
        fieldErrors,
        "Check the PDF details shown below and try again.",
      ),
      fieldErrors,
    };
  }

  const storageSetup = resolveMaterialStorage(input.storage);
  if (storageSetup.status === "missing-env") {
    return { status: "not-prepared", message: storageSetup.message };
  }

  const prisma = getPrisma();
  const expiresInSeconds = MATERIAL_UPLOAD_URL_EXPIRES_IN_SECONDS;
  const presignedUploadExpiresAt = new Date(
    input.now.getTime() + expiresInSeconds * 1_000 + MATERIAL_UPLOAD_LEASE_SAFETY_MS,
  );
  const created = await prisma.$transaction(
    async (tx) => {
      await tx.$queryRaw`SELECT "id" FROM "users" WHERE "id" = ${input.userId} FOR UPDATE`;
      const deletionJob = await tx.accountDeletionJob.findUnique({
        where: { userId: input.userId },
        select: { id: true },
      });
      if (deletionJob) {
        return {
          status: "account-deletion" as const,
          message: "Uploads are disabled while account deletion is in progress.",
        };
      }

      const quota = await checkSourceStorageUsageLimit({
        userId: input.userId,
        byteSize: parsed.data.byteSize,
        prisma: tx,
      });
      if (quota.status === "limited") {
        return { status: "limited" as const, message: quota.message };
      }

      const material = await tx.studyMaterial.create({
        data: {
          userId: input.userId,
          collectionId: parsed.data.collectionId ?? null,
          title: parsed.data.title,
          kind: StudyMaterialKind.PDF,
        },
        select: { id: true },
      });
      const revision = await tx.materialRevision.create({
        data: {
          userId: input.userId,
          materialId: material.id,
          revisionNumber: 1,
          storageBucket: storageSetup.storage.bucketName,
        },
        select: { id: true },
      });
      const objectKey = buildMaterialObjectKey({
        userId: input.userId,
        materialId: material.id,
        materialRevisionId: revision.id,
        fileName: parsed.data.originalName,
      });
      const sourceFile = await tx.sourceFile.create({
        data: {
          userId: input.userId,
          collectionId: parsed.data.collectionId ?? null,
          materialRevisionId: revision.id,
          kind: SourceFileKind.PDF,
          status: SourceFileStatus.DRAFT,
          originalName: parsed.data.originalName,
          mimeType: parsed.data.mimeType,
          byteSize: parsed.data.byteSize,
          storageBucket: storageSetup.storage.bucketName,
          storageKey: objectKey,
          presignedUploadExpiresAt,
        },
        select: { id: true },
      });
      await tx.materialRevision.update({
        where: { id: revision.id },
        data: { storageKey: objectKey },
      });

      return {
        status: "created" as const,
        materialId: material.id,
        materialRevisionId: revision.id,
        sourceFileId: sourceFile.id,
        objectKey,
      };
    },
    { isolationLevel: Prisma.TransactionIsolationLevel.Serializable },
  );

  if (created.status === "limited" || created.status === "account-deletion") {
    return { status: "not-prepared", message: created.message };
  }

  try {
    const uploadUrl = await storageSetup.storage.createPresignedUploadUrl({
      key: created.objectKey,
      mimeType: parsed.data.mimeType,
      byteSize: parsed.data.byteSize,
      maxBytes: MAX_MATERIAL_PDF_BYTES,
      expiresInSeconds,
    });

    return {
      status: "prepared",
      materialId: created.materialId,
      materialRevisionId: created.materialRevisionId,
      sourceFileId: created.sourceFileId,
      uploadUrl,
      headers: {
        "Content-Type": parsed.data.mimeType,
      },
      expiresInSeconds,
    };
  } catch (error) {
    await prisma.studyMaterial.deleteMany({
      where: { id: created.materialId, userId: input.userId },
    });
    return {
      status: "not-prepared",
      message: error instanceof Error ? error.message : "Could not prepare the private PDF upload.",
    };
  }
}

export async function discardPreparedMaterialPdf(input: {
  userId: string;
  materialId: string;
  materialRevisionId: string;
  storage?: SourceObjectStorage;
}) {
  const prisma = getPrisma();
  const claimed = await prisma.$transaction(async (tx) => {
    const revision = await tx.materialRevision.findFirst({
      where: {
        id: input.materialRevisionId,
        materialId: input.materialId,
        userId: input.userId,
        status: MaterialRevisionStatus.PENDING_UPLOAD,
        material: { kind: StudyMaterialKind.PDF },
      },
      select: {
        id: true,
        sourceFiles: {
          where: { kind: SourceFileKind.PDF },
          take: 1,
          select: { id: true, storageBucket: true, storageKey: true },
        },
      },
    });
    const sourceFile = revision?.sourceFiles[0];
    if (!revision || !sourceFile) {
      return null;
    }

    const claimedRevision = await tx.materialRevision.updateMany({
      where: {
        id: revision.id,
        materialId: input.materialId,
        userId: input.userId,
        status: MaterialRevisionStatus.PENDING_UPLOAD,
      },
      data: {
        status: MaterialRevisionStatus.FAILED,
        errorCode: "UPLOAD_ABANDONED",
        errorMessage: "The private PDF upload did not finish.",
      },
    });
    if (claimedRevision.count !== 1) {
      return null;
    }

    await tx.sourceFile.updateMany({
      where: {
        id: sourceFile.id,
        userId: input.userId,
        materialRevisionId: revision.id,
        status: SourceFileStatus.DRAFT,
      },
      data: { status: SourceFileStatus.FAILED },
    });

    return sourceFile;
  });

  if (!claimed) {
    return { status: "already-handled" as const };
  }

  const storageSetup = resolveMaterialStorage(input.storage);
  if (storageSetup.status === "missing-env") {
    return {
      status: "retained" as const,
      message:
        "The upload failed and the material was marked for attention, but storage cleanup is unavailable.",
    };
  }

  try {
    if (claimed.storageKey && claimed.storageBucket) {
      await storageSetup.storage.deleteObject({
        key: claimed.storageKey,
        bucket: claimed.storageBucket,
      });
    }
  } catch {
    return {
      status: "retained" as const,
      message:
        "The upload failed and the material was marked for attention because storage cleanup did not finish.",
    };
  }

  const deletedMaterial = await prisma.studyMaterial.deleteMany({
    where: {
      id: input.materialId,
      userId: input.userId,
      activeRevisionId: null,
      revisions: {
        every: {
          id: input.materialRevisionId,
          status: MaterialRevisionStatus.FAILED,
          errorCode: "UPLOAD_ABANDONED",
        },
      },
    },
  });

  return deletedMaterial.count === 1
    ? { status: "discarded" as const }
    : {
        status: "retained" as const,
        message:
          "The upload failed and was marked for attention because the material changed during cleanup.",
      };
}

export async function queueMaterialPdfIngestion(input: {
  userId: string;
  materialRevisionId: string;
  now: Date;
  storage?: SourceObjectStorage;
  eventSender?: MaterialIngestionEventSender;
}): Promise<QueueMaterialIngestionResult> {
  const envStatus = getJobsEnvStatus();
  if (envStatus.status === "missing-env" && !input.eventSender) {
    return { status: "not-queued", message: envStatus.message };
  }
  const storageSetup = resolveMaterialStorage(input.storage);
  if (storageSetup.status === "missing-env") {
    return { status: "not-queued", message: storageSetup.message };
  }

  const prisma = getPrisma();
  const revision = await prisma.materialRevision.findFirst({
    where: {
      id: input.materialRevisionId,
      userId: input.userId,
      status: {
        in: [
          MaterialRevisionStatus.PENDING_UPLOAD,
          MaterialRevisionStatus.QUEUED,
          MaterialRevisionStatus.PROCESSING,
          MaterialRevisionStatus.READY,
        ],
      },
      material: { kind: StudyMaterialKind.PDF },
    },
    select: {
      id: true,
      materialId: true,
      status: true,
      processingMetadata: true,
      sourceFiles: {
        where: { kind: SourceFileKind.PDF },
        take: 1,
        select: {
          id: true,
          byteSize: true,
          mimeType: true,
          storageBucket: true,
          storageKey: true,
        },
      },
    },
  });
  const sourceFile = revision?.sourceFiles[0];
  if (!revision || !sourceFile?.storageKey || !sourceFile.storageBucket) {
    return {
      status: "not-found",
      message: "Material PDF upload was not found.",
    };
  }
  if (revision.status !== MaterialRevisionStatus.PENDING_UPLOAD) {
    return {
      status: "queued",
      materialId: revision.materialId,
      materialRevisionId: revision.id,
      message:
        revision.status === MaterialRevisionStatus.READY
          ? "Material is already ready."
          : "Material processing has already started.",
    };
  }

  try {
    const head = await storageSetup.storage.headObject({
      key: sourceFile.storageKey,
      bucket: sourceFile.storageBucket,
    });
    if (
      head.byteSize !== sourceFile.byteSize ||
      head.mimeType?.split(";")[0] !== "application/pdf"
    ) {
      return {
        status: "not-queued",
        message:
          "The uploaded PDF did not match the prepared file. Choose it again.",
      };
    }
  } catch {
    return {
      status: "not-queued",
      message:
        "The private PDF upload could not be verified. Try uploading it again.",
    };
  }

  const queued = await prisma.$transaction(async (tx) => {
    const changed = await tx.materialRevision.updateMany({
      where: {
        id: revision.id,
        userId: input.userId,
        status: MaterialRevisionStatus.PENDING_UPLOAD,
        material: { status: StudyMaterialStatus.ACTIVE },
      },
      data: {
        status: MaterialRevisionStatus.QUEUED,
        errorCode: null,
        errorMessage: null,
        processingMetadata: {
          ...(revision.processingMetadata &&
          typeof revision.processingMetadata === "object" &&
          !Array.isArray(revision.processingMetadata)
            ? revision.processingMetadata
            : {}),
          ingestionDelivery: createMaterialIngestionDelivery(input.now),
        },
      },
    });
    if (changed.count !== 1) return false;
    await tx.sourceFile.updateMany({
      where: {
        id: sourceFile.id,
        userId: input.userId,
        materialRevisionId: revision.id,
      },
      data: { status: SourceFileStatus.UPLOADED },
    });
    return true;
  });
  if (!queued)
    return {
      status: "queued",
      materialId: revision.materialId,
      materialRevisionId: revision.id,
      message: "Material processing has already started.",
    };

  const publication = await publishMaterialIngestion({
    userId: input.userId,
    materialRevisionId: revision.id,
    now: input.now,
    sender: input.eventSender,
  });
  if (publication === "terminal")
    return {
      status: "not-queued",
      message:
        "Background processing could not be delivered. Retry the import.",
    };

  return {
    status: "queued",
    materialId: revision.materialId,
    materialRevisionId: revision.id,
    message: "PDF uploaded. LearnRecur is building its outline.",
  };
}

export async function queueWebsiteMaterialImport(input: {
  userId: string;
  now: Date;
  input: unknown;
  storage?: SourceObjectStorage;
  eventSender?: MaterialIngestionEventSender;
}): Promise<
  | QueueMaterialIngestionResult
  | {
      status: "invalid";
      message: string;
      fieldErrors: Record<string, string[]>;
    }
> {
  const parsed = confirmWebsiteImportInputSchema.safeParse(input.input);
  if (!parsed.success) {
    return {
      status: "invalid",
      message: "Website import details need a little attention.",
      fieldErrors: parsed.error.flatten().fieldErrors,
    };
  }
  const source = new URL(parsed.data.sourceUrl);
  const selectedUrls = [...new Set(parsed.data.selectedUrls)];
  if (selectedUrls.some((value) => new URL(value).origin !== source.origin)) {
    return {
      status: "invalid",
      message: "Website pages must come from the same textbook site.",
      fieldErrors: {
        selectedUrls: ["Choose only pages from the discovered textbook."],
      },
    };
  }

  const storageSetup = resolveMaterialStorage(input.storage);
  if (storageSetup.status === "missing-env") {
    return { status: "not-queued", message: storageSetup.message };
  }
  const envStatus = getJobsEnvStatus();
  if (envStatus.status === "missing-env" && !input.eventSender) {
    return { status: "not-queued", message: envStatus.message };
  }

  const prisma = getPrisma();
  const created = await prisma.$transaction(async (tx) => {
    const material = await tx.studyMaterial.create({
      data: {
        userId: input.userId,
        collectionId: parsed.data.collectionId ?? null,
        title: parsed.data.title,
        kind: StudyMaterialKind.WEB,
      },
      select: { id: true },
    });
    const revision = await tx.materialRevision.create({
      data: {
        userId: input.userId,
        materialId: material.id,
        revisionNumber: 1,
        status: MaterialRevisionStatus.QUEUED,
        processingMetadata: {
          ingestionDelivery: createMaterialIngestionDelivery(input.now),
        },
        sourceUrl: parsed.data.sourceUrl,
        storageBucket: storageSetup.storage.bucketName,
      },
      select: { id: true },
    });
    const storageKey = buildMaterialObjectKey({
      userId: input.userId,
      materialId: material.id,
      materialRevisionId: revision.id,
      fileName: "website-snapshot.json",
    });
    const sourceFile = await tx.sourceFile.create({
      data: {
        userId: input.userId,
        collectionId: parsed.data.collectionId ?? null,
        materialRevisionId: revision.id,
        kind: SourceFileKind.URL,
        status: SourceFileStatus.UPLOADED,
        originalName: parsed.data.title,
        mimeType: "application/json",
        storageBucket: storageSetup.storage.bucketName,
        storageKey,
        metadata: {
          sourceUrl: parsed.data.sourceUrl,
          selectedUrls,
        },
      },
      select: { id: true },
    });
    await tx.materialRevision.update({
      where: { id: revision.id },
      data: { storageKey },
    });
    return {
      materialId: material.id,
      materialRevisionId: revision.id,
      sourceFileId: sourceFile.id,
    };
  });

  const publication = await publishMaterialIngestion({
    userId: input.userId,
    materialRevisionId: created.materialRevisionId,
    now: input.now,
    sender: input.eventSender,
  });
  if (publication === "terminal")
    return {
      status: "not-queued",
      message:
        "Background processing could not be delivered. Retry the import.",
    };

  return {
    status: "queued",
    materialId: created.materialId,
    materialRevisionId: created.materialRevisionId,
    message:
      "Website pages saved for import. LearnRecur is building the outline.",
  };
}

export async function retryMaterialIngestion(input: {
  userId: string;
  materialRevisionId: string;
  now: Date;
  eventSender?: MaterialIngestionEventSender;
}): Promise<QueueMaterialIngestionResult> {
  const prisma = getPrisma();
  const staleBefore = new Date(
    input.now.getTime() - MATERIAL_QUEUE_STALE_AFTER_MS,
  );
  const retryableStatus = {
    OR: [
      { status: MaterialRevisionStatus.FAILED },
      {
        status: MaterialRevisionStatus.QUEUED,
        updatedAt: { lte: staleBefore },
      },
    ],
  };
  const revision = await prisma.materialRevision.findFirst({
    where: {
      id: input.materialRevisionId,
      userId: input.userId,
      ...retryableStatus,
      material: { status: StudyMaterialStatus.ACTIVE },
    },
    select: {
      id: true,
      materialId: true,
      updatedAt: true,
      processingMetadata: true,
    },
  });
  if (!revision) {
    return {
      status: "not-found",
      message:
        "Material processing is already running or is not ready to retry.",
    };
  }

  const claimed = await prisma.materialRevision.updateMany({
    where: {
      id: revision.id,
      userId: input.userId,
      ...retryableStatus,
      updatedAt: revision.updatedAt,
      material: { status: StudyMaterialStatus.ACTIVE },
    },
    data: {
      status: MaterialRevisionStatus.QUEUED,
      errorCode: null,
      errorMessage: null,
      updatedAt: input.now,
      processingMetadata: {
        ...(revision.processingMetadata &&
        typeof revision.processingMetadata === "object" &&
        !Array.isArray(revision.processingMetadata)
          ? revision.processingMetadata
          : {}),
        ingestionDelivery: createMaterialIngestionDelivery(input.now),
      },
    },
  });
  if (claimed.count !== 1) {
    return {
      status: "not-found",
      message: "Material processing already resumed before this retry.",
    };
  }
  const publication = await publishMaterialIngestion({
    userId: input.userId,
    materialRevisionId: revision.id,
    now: input.now,
    sender: input.eventSender,
  });
  if (publication === "terminal")
    return {
      status: "not-queued",
      message:
        "Background processing could not be delivered. Retry the import.",
    };

  return {
    status: "queued",
    materialId: revision.materialId,
    materialRevisionId: revision.id,
    message: "Material processing queued again.",
  };
}

export async function queueMaterialPdfReindex(input: {
  userId: string;
  materialId: string;
  now: Date;
  storage?: SourceObjectStorage;
  eventSender?: MaterialIngestionEventSender;
}): Promise<QueueMaterialIngestionResult> {
  const storageSetup = resolveMaterialStorage(input.storage);
  if (storageSetup.status === "missing-env") {
    return { status: "not-queued", message: storageSetup.message };
  }
  const envStatus = getJobsEnvStatus();
  if (envStatus.status === "missing-env" && !input.eventSender) {
    return { status: "not-queued", message: envStatus.message };
  }

  const prisma = getPrisma();
  const material = await prisma.studyMaterial.findFirst({
    where: {
      id: input.materialId,
      userId: input.userId,
      kind: StudyMaterialKind.PDF,
      status: StudyMaterialStatus.ACTIVE,
    },
    select: {
      id: true,
      collectionId: true,
      activeRevision: {
        select: {
          id: true,
          status: true,
          byteSize: true,
          sourceFiles: {
            where: { kind: SourceFileKind.PDF },
            orderBy: { createdAt: "asc" },
            take: 1,
            select: {
              status: true,
              originalName: true,
              mimeType: true,
              byteSize: true,
              storageBucket: true,
              storageKey: true,
              metadata: true,
            },
          },
        },
      },
      revisions: {
        orderBy: { revisionNumber: "desc" },
        take: 1,
        select: { id: true, status: true },
      },
    },
  });
  const activeRevision = material?.activeRevision;
  const sourceFile = activeRevision?.sourceFiles[0];
  if (
    !material ||
    !activeRevision ||
    activeRevision.status !== MaterialRevisionStatus.READY ||
    sourceFile?.status !== SourceFileStatus.READY ||
    !sourceFile.storageBucket ||
    !sourceFile.storageKey ||
    !sourceFile.mimeType
  ) {
    return {
      status: "not-found",
      message: "Ready PDF material was not found.",
    };
  }
  const latestRevision = material.revisions[0];
  if (
    latestRevision?.id !== activeRevision.id &&
    latestRevision?.status !== MaterialRevisionStatus.FAILED
  ) {
    return {
      status: "not-queued",
      message: "This material is already rebuilding.",
    };
  }
  if (storageSetup.storage.bucketName !== sourceFile.storageBucket) {
    return {
      status: "not-queued",
      message: "The saved PDF storage location is unavailable.",
    };
  }

  try {
    const head = await storageSetup.storage.headObject({
      key: sourceFile.storageKey,
      bucket: sourceFile.storageBucket,
    });
    const expectedByteSize = sourceFile.byteSize ?? activeRevision.byteSize;
    if (
      (expectedByteSize !== null && head.byteSize !== expectedByteSize) ||
      head.mimeType?.split(";")[0]?.trim().toLowerCase() !== "application/pdf"
    ) {
      return {
        status: "not-queued",
        message: "The saved PDF could not be verified.",
      };
    }
  } catch {
    return {
      status: "not-queued",
      message: "The saved PDF could not be verified.",
    };
  }

  const created = await prisma.$transaction(async (tx) => {
    await tx.$queryRaw`
      SELECT "id"
      FROM "study_materials"
      WHERE "id" = ${material.id} AND "userId" = ${input.userId}
      FOR UPDATE
    `;
    const locked = await tx.studyMaterial.findFirst({
      where: {
        id: material.id,
        userId: input.userId,
        kind: StudyMaterialKind.PDF,
        status: StudyMaterialStatus.ACTIVE,
      },
      select: {
        collectionId: true,
        activeRevisionId: true,
        revisions: {
          orderBy: { revisionNumber: "desc" },
          take: 1,
          select: { id: true, revisionNumber: true, status: true },
        },
      },
    });
    const latestRevision = locked?.revisions[0];
    if (
      !locked ||
      !latestRevision ||
      locked.activeRevisionId !== activeRevision.id ||
      (latestRevision?.id !== activeRevision.id &&
        latestRevision?.status !== MaterialRevisionStatus.FAILED)
    ) {
      return { status: "already-running" as const };
    }
    const revision = await tx.materialRevision.create({
      data: {
        userId: input.userId,
        materialId: material.id,
        revisionNumber: latestRevision.revisionNumber + 1,
        status: MaterialRevisionStatus.QUEUED,
        storageBucket: sourceFile.storageBucket,
        storageKey: sourceFile.storageKey,
        processingMetadata: {
          rebuildOfRevisionId: activeRevision.id,
          ingestionDelivery: createMaterialIngestionDelivery(input.now),
        },
      },
      select: { id: true },
    });
    const nextSourceFile = await tx.sourceFile.create({
      data: {
        userId: input.userId,
        collectionId: locked.collectionId,
        materialRevisionId: revision.id,
        kind: SourceFileKind.PDF,
        status: SourceFileStatus.UPLOADED,
        originalName: sourceFile.originalName,
        mimeType: sourceFile.mimeType,
        // The original source row already accounts for this shared object in storage quotas.
        byteSize: null,
        storageBucket: sourceFile.storageBucket,
        storageKey: sourceFile.storageKey,
        metadata: sourceFile.metadata ?? Prisma.DbNull,
      },
      select: { id: true },
    });
    const cachedOcrPages = await tx.materialPage.findMany({
      ...readyMaterialOcrPageQuery({
        userId: input.userId,
        materialRevisionId: activeRevision.id,
      }),
      orderBy: { pageNumber: "asc" },
    });
    if (cachedOcrPages.length > 0) {
      await tx.materialPage.createMany({
        data: cachedOcrPages.map((page) => ({
          userId: input.userId,
          materialRevisionId: revision.id,
          pageNumber: page.pageNumber,
          embeddedText: page.embeddedText,
          ocrText: page.ocrText,
          textStatus: page.textStatus,
          contentHash: page.contentHash,
          tokenEstimate: page.tokenEstimate,
          metadata: page.metadata ?? Prisma.DbNull,
        })),
      });
    }
    return {
      status: "created" as const,
      materialRevisionId: revision.id,
      sourceFileId: nextSourceFile.id,
    };
  });
  if (created.status === "already-running") {
    return {
      status: "not-queued",
      message: "This material is already rebuilding.",
    };
  }

  const publication = await publishMaterialIngestion({
    userId: input.userId,
    materialRevisionId: created.materialRevisionId,
    now: input.now,
    sender: input.eventSender,
  });
  if (publication === "terminal")
    return {
      status: "not-queued",
      message:
        "Background processing could not be delivered. Retry the import.",
    };

  return {
    status: "queued",
    materialId: material.id,
    materialRevisionId: created.materialRevisionId,
    message: "Search rebuild queued from the saved PDF.",
  };
}

export async function queueWebsiteMaterialRefresh(input: {
  userId: string;
  materialId: string;
  now: Date;
  storage?: SourceObjectStorage;
  eventSender?: MaterialIngestionEventSender;
}): Promise<QueueMaterialIngestionResult> {
  const storageSetup = resolveMaterialStorage(input.storage);
  if (storageSetup.status === "missing-env") {
    return { status: "not-queued", message: storageSetup.message };
  }
  const envStatus = getJobsEnvStatus();
  if (envStatus.status === "missing-env" && !input.eventSender) {
    return { status: "not-queued", message: envStatus.message };
  }
  const prisma = getPrisma();
  const material = await prisma.studyMaterial.findFirst({
    where: {
      id: input.materialId,
      userId: input.userId,
      kind: StudyMaterialKind.WEB,
    },
    select: {
      id: true,
      collectionId: true,
      title: true,
      activeRevision: {
        select: {
          sourceUrl: true,
          sourceFiles: {
            where: { kind: SourceFileKind.URL },
            take: 1,
            select: { metadata: true },
          },
        },
      },
    },
  });
  const sourceUrl = material?.activeRevision?.sourceUrl;
  const metadata = material?.activeRevision?.sourceFiles[0]?.metadata;
  if (
    !material ||
    !sourceUrl ||
    readStringArray(metadata ?? null, "selectedUrls").length === 0
  ) {
    return {
      status: "not-found",
      message: "Ready website material was not found.",
    };
  }
  const revision = await createNextMaterialRevision({
    userId: input.userId,
    materialId: material.id,
    sourceUrl,
  });
  if (!revision) {
    return { status: "not-found", message: "Website material was not found." };
  }
  const storageKey = buildMaterialObjectKey({
    userId: input.userId,
    materialId: material.id,
    materialRevisionId: revision.id,
    fileName: "website-snapshot.json",
  });
  await prisma.$transaction([
    prisma.materialRevision.update({
      where: { id: revision.id },
      data: {
        status: MaterialRevisionStatus.QUEUED,
        processingMetadata: {
          ingestionDelivery: createMaterialIngestionDelivery(input.now),
        },
        storageBucket: storageSetup.storage.bucketName,
        storageKey,
      },
    }),
    prisma.sourceFile.create({
      data: {
        userId: input.userId,
        collectionId: material.collectionId,
        materialRevisionId: revision.id,
        kind: SourceFileKind.URL,
        status: SourceFileStatus.UPLOADED,
        originalName: material.title,
        mimeType: "application/json",
        storageBucket: storageSetup.storage.bucketName,
        storageKey,
        metadata: metadata ?? Prisma.JsonNull,
      },
      select: { id: true },
    }),
  ]);

  const publication = await publishMaterialIngestion({
    userId: input.userId,
    materialRevisionId: revision.id,
    now: input.now,
    sender: input.eventSender,
  });
  if (publication === "terminal")
    return {
      status: "not-queued",
      message:
        "Background processing could not be delivered. Retry the import.",
    };

  return {
    status: "queued",
    materialId: material.id,
    materialRevisionId: revision.id,
    message: "Website refresh queued as a new revision.",
  };
}

export async function runMaterialIngestionJob(input: {
  userId: string;
  materialRevisionId: string;
  storage?: SourceObjectStorage;
  embeddingGenerator?: MaterialEmbeddingGenerator | null;
  summaryGenerator?: MaterialSummaryGenerator | null;
  fetchResource?: FetchWebResource;
  resolveHostname?: ResolveHostname;
}) {
  const prisma = getPrisma();
  const existing = await prisma.materialRevision.findFirst({
    where: { id: input.materialRevisionId, userId: input.userId },
    select: { status: true },
  });
  if (!existing) {
    return { status: "not-found" as const };
  }
  if (existing.status === MaterialRevisionStatus.READY) {
    return { status: "ready" as const, alreadyProcessed: true };
  }

  const claimed = await prisma.materialRevision.updateMany({
    where: {
      id: input.materialRevisionId,
      userId: input.userId,
      status: {
        in: [
          MaterialRevisionStatus.QUEUED,
          MaterialRevisionStatus.FAILED,
          MaterialRevisionStatus.PROCESSING,
        ],
      },
    },
    data: {
      status: MaterialRevisionStatus.PROCESSING,
      errorCode: null,
      errorMessage: null,
    },
  });
  if (claimed.count !== 1) {
    return { status: "not-claimed" as const };
  }

  try {
    const revision = await prisma.materialRevision.findFirstOrThrow({
      where: { id: input.materialRevisionId, userId: input.userId },
      select: {
        id: true,
        materialId: true,
        sourceUrl: true,
        storageBucket: true,
        storageKey: true,
        processingMetadata: true,
        material: { select: { kind: true, title: true } },
        sourceFiles: {
          orderBy: { createdAt: "asc" },
          take: 1,
          select: {
            id: true,
            byteSize: true,
            mimeType: true,
            storageBucket: true,
            storageKey: true,
            metadata: true,
          },
        },
      },
    });
    const sourceFile = revision.sourceFiles[0];
    if (!sourceFile) {
      throw new MaterialIngestionError("Material source record is missing.", { retryable: false });
    }
    const storageSetup = resolveMaterialStorage(input.storage);
    if (storageSetup.status === "missing-env") {
      throw new MaterialIngestionError(storageSetup.message, { retryable: true });
    }
    const embeddingGenerator =
      input.embeddingGenerator === undefined
        ? safelyResolveEmbeddingGenerator()
        : input.embeddingGenerator;
    const summaryGenerator =
      input.summaryGenerator === undefined
        ? safelyResolveSummaryGenerator()
        : input.summaryGenerator;

    const result = revision.material.kind === StudyMaterialKind.PDF
      ? await ingestPdfRevision({
          userId: input.userId,
          materialId: revision.materialId,
          materialRevisionId: revision.id,
          sourceFile,
          storage: storageSetup.storage,
          embeddingGenerator,
          summaryGenerator,
          materialTitle: revision.material.title,
          rebuildOfRevisionId: readRebuildOfRevisionId(revision.processingMetadata),
        })
      : await ingestWebsiteRevision({
          userId: input.userId,
          materialId: revision.materialId,
          materialRevisionId: revision.id,
          sourceUrl: revision.sourceUrl,
          sourceFile,
          storage: storageSetup.storage,
          embeddingGenerator,
          summaryGenerator,
          materialTitle: revision.material.title,
          fetchResource: input.fetchResource,
          resolveHostname: input.resolveHostname,
        });

    return { status: "ready" as const, alreadyProcessed: false, ...result };
  } catch (error) {
    const ingestionError = normalizeIngestionError(error);
    await prisma.$transaction([
      prisma.materialRevision.updateMany({
        where: { id: input.materialRevisionId, userId: input.userId },
        data: {
          status: MaterialRevisionStatus.FAILED,
          errorCode: ingestionError.retryable ? "TRANSIENT_INGESTION_FAILURE" : "INGESTION_REJECTED",
          errorMessage: ingestionError.message.slice(0, 1_000),
        },
      }),
      prisma.sourceFile.updateMany({
        where: { materialRevisionId: input.materialRevisionId, userId: input.userId },
        data: { status: SourceFileStatus.FAILED },
      }),
    ]);
    throw ingestionError;
  }
}

async function ingestPdfRevision(input: {
  userId: string;
  materialId: string;
  materialRevisionId: string;
  sourceFile: MaterialSourceFile;
  storage: SourceObjectStorage;
  embeddingGenerator: MaterialEmbeddingGenerator | null;
  summaryGenerator: MaterialSummaryGenerator | null;
  materialTitle: string;
  rebuildOfRevisionId: string | null;
}) {
  if (!input.sourceFile.storageKey || !input.sourceFile.storageBucket) {
    throw new MaterialIngestionError("Material PDF storage location is missing.", { retryable: false });
  }
  const bytes = await input.storage.getObjectBytes({
    key: input.sourceFile.storageKey,
    bucket: input.sourceFile.storageBucket,
    maxBytes: MAX_MATERIAL_PDF_BYTES,
  });
  const contentHash = sha256(bytes);
  if (input.rebuildOfRevisionId) {
    const sourceRevision = await getPrisma().materialRevision.findFirst({
      where: {
        id: input.rebuildOfRevisionId,
        userId: input.userId,
        materialId: input.materialId,
        status: MaterialRevisionStatus.READY,
      },
      select: { contentHash: true },
    });
    if (!sourceRevision?.contentHash || sourceRevision.contentHash !== contentHash) {
      throw new MaterialIngestionError(
        "The saved PDF changed after it was uploaded, so LearnRecur did not reuse its cached text.",
        { retryable: false },
      );
    }
  }
  const extracted = await extractPdfPages(bytes, { maximumPages: MAX_MATERIAL_PDF_PAGES });
  const indexedPages = await mergeCachedOcrIntoExtractedPdfPages({
    userId: input.userId,
    materialRevisionId: input.materialRevisionId,
    rebuildOfRevisionId: input.rebuildOfRevisionId,
    pages: extracted.pages,
  });
  if (indexedPages.every((page) => page.needsOcr)) {
    // Keep the revision usable: selected pages can be OCRed lazily and cached.
    // A placeholder section gives scope planning a stable page range meanwhile.
    indexedPages[0] = { ...indexedPages[0], text: "Scanned document", needsOcr: true };
  }
  const index = buildPdfIndex(indexedPages);
  if (index.sections.length === 0) {
    index.sections.push({
      ordinal: 0,
      level: 1,
      title: "Document",
      normalizedTitle: "document",
      pageStart: 1,
      pageEnd: extracted.pageCount,
      headingPath: ["Document"],
    });
  }
  const persisted = await persistPdfIndex({
    userId: input.userId,
    materialRevisionId: input.materialRevisionId,
    sourceFileId: input.sourceFile.id,
    pages: indexedPages,
    sections: index.sections,
    chunks: index.chunks,
  });
  const embedded = await embedPersistedChunks({
    userId: input.userId,
    materialRevisionId: input.materialRevisionId,
    chunks: persisted.chunks,
    embeddingGenerator: input.embeddingGenerator,
  });
  const excerpt = indexedPages
    .filter((page) => !page.needsOcr)
    .map((page) => page.text)
    .join("\n\n")
    .slice(0, MATERIAL_SOURCE_EXCERPT_LIMIT);
  const summary = await generateStoredMaterialSummary({
    generator: input.summaryGenerator,
    materialTitle: input.materialTitle,
    materialKind: "PDF",
    outlineTitles: index.sections.map((section) => section.title),
    excerpt,
  });
  const prisma = getPrisma();
  await prisma.sourceFile.update({
    where: { id: input.sourceFile.id },
    data: { status: SourceFileStatus.READY, extractedText: excerpt || null },
  });
  await finalizeMaterialRevision({
    userId: input.userId,
    materialId: input.materialId,
    materialRevisionId: input.materialRevisionId,
    contentHash,
    byteSize: bytes.byteLength,
    pageCount: extracted.pageCount,
    summary,
    storageBucket: input.sourceFile.storageBucket,
    storageKey: input.sourceFile.storageKey,
    processingMetadata: {
      parser: "pdfjs",
      chunkTargetTokens: 800,
      chunkOverlapTokens: 120,
      pagesRequiringOcr: index.pagesRequiringOcr,
      embeddingStatus: embedded ? "ready" : "unavailable",
      ...(input.rebuildOfRevisionId
        ? { rebuildOfRevisionId: input.rebuildOfRevisionId }
        : {}),
    },
    copyReadyOcrFromRevisionId: input.rebuildOfRevisionId,
    indexedOcrPageNumbers: indexedPages
      .filter((page) => page.extractedText !== undefined)
      .map((page) => page.pageNumber),
  });

  return { pageCount: extracted.pageCount, chunkCount: persisted.chunks.length };
}

async function mergeCachedOcrIntoExtractedPdfPages(input: {
  userId: string;
  materialRevisionId: string;
  rebuildOfRevisionId: string | null;
  pages: ExtractedPdfPage[];
}): Promise<IndexedPdfPage[]> {
  const prisma = getPrisma();
  const readyPageQuery = (materialRevisionId: string) =>
    prisma.materialPage.findMany({
      ...readyMaterialOcrPageQuery({
        userId: input.userId,
        materialRevisionId,
      }),
    });
  const [preservedPages, newlyReadySourcePages] = await Promise.all([
    readyPageQuery(input.materialRevisionId),
    input.rebuildOfRevisionId
      ? readyPageQuery(input.rebuildOfRevisionId)
      : Promise.resolve([]),
  ]);
  const ocrByPage = new Map(
    [...preservedPages, ...newlyReadySourcePages].flatMap((page) =>
      page.ocrText?.trim() ? [[page.pageNumber, page.ocrText.trim()] as const] : [],
    ),
  );
  return input.pages.map((page) => {
    const ocrText = ocrByPage.get(page.pageNumber);
    if (!ocrText) {
      return page;
    }
    const embeddedText = page.text.trim();
    return {
      ...page,
      extractedText: page.text,
      text:
        embeddedText
          ? embeddedText.includes(ocrText)
            ? embeddedText
            : `${embeddedText}\n\n${ocrText}`
          : ocrText,
      needsOcr: false,
      hasVisualContent: true,
    };
  });
}

function readRebuildOfRevisionId(metadata: Prisma.JsonValue | null) {
  if (!metadata || typeof metadata !== "object" || Array.isArray(metadata)) {
    return null;
  }
  const revisionId = metadata.rebuildOfRevisionId;
  return typeof revisionId === "string" && revisionId.length > 0 ? revisionId : null;
}

async function ingestWebsiteRevision(input: {
  userId: string;
  materialId: string;
  materialRevisionId: string;
  sourceUrl: string | null;
  sourceFile: MaterialSourceFile;
  storage: SourceObjectStorage;
  embeddingGenerator: MaterialEmbeddingGenerator | null;
  summaryGenerator: MaterialSummaryGenerator | null;
  materialTitle: string;
  fetchResource?: FetchWebResource;
  resolveHostname?: ResolveHostname;
}) {
  if (!input.sourceUrl || !input.sourceFile.storageKey || !input.sourceFile.storageBucket) {
    throw new MaterialIngestionError("Website material source details are missing.", { retryable: false });
  }
  if (!input.storage.putObject) {
    throw new MaterialIngestionError("Private website snapshot storage is unavailable.", {
      retryable: true,
    });
  }
  const storageKey = input.sourceFile.storageKey;
  const storageBucket = input.sourceFile.storageBucket;
  const putObject = input.storage.putObject;
  const base = await validatePublicHttpsUrl(input.sourceUrl, input.resolveHostname);
  const selectedUrls = readStringArray(input.sourceFile.metadata, "selectedUrls");
  if (selectedUrls.length === 0) {
    throw new MaterialIngestionError("No website pages were selected for import.", { retryable: false });
  }
  const fetchResource = input.fetchResource ?? fetchPublicWebResource;
  const snapshots: Array<{ url: string; title: string; html: string }> = [];
  let fetchedBytes = 0;

  for (let start = 0; start < selectedUrls.length; ) {
    const remaining = MAX_WEBSITE_REVISION_BYTES - fetchedBytes;
    if (remaining <= 0) {
      throw new MaterialIngestionError("Website revision exceeded the 50 MB fetched-content limit.", {
        retryable: false,
      });
    }
    const quotaBoundConcurrency = Math.max(
      1,
      Math.min(
        MATERIAL_WEBSITE_FETCH_CONCURRENCY,
        Math.floor(remaining / MATERIAL_WEBSITE_PAGE_FETCH_BYTES),
      ),
    );
    const batch = selectedUrls.slice(start, start + quotaBoundConcurrency);
    const maximumBytesPerPage = Math.min(
      MATERIAL_WEBSITE_PAGE_FETCH_BYTES,
      Math.floor(remaining / batch.length),
    );
    const fetchedBatch = await Promise.all(
      batch.map(async (selectedUrl) => {
        const requested = await validatePublicHttpsUrl(selectedUrl, input.resolveHostname);
        if (requested.origin !== base.origin) {
          throw new MaterialIngestionError(
            "Website imports can only follow pages from the same origin.",
            { retryable: false },
          );
        }
        const resource = await fetchResource(requested.toString(), {
          maximumBytes: maximumBytesPerPage,
          requiredOrigin: base.origin,
        });
        const finalUrl = await validatePublicHttpsUrl(resource.url, input.resolveHostname);
        if (finalUrl.origin !== base.origin) {
          throw new MaterialIngestionError(
            "Website imports can only follow pages from the same origin.",
            { retryable: false },
          );
        }
        if (!resource.contentType.toLowerCase().includes("text/html")) {
          throw new MaterialIngestionError("A selected website page was not readable HTML.", {
            retryable: false,
          });
        }
        const html = resource.bytes.toString("utf8");
        const readable = extractReadableWebPage(html, new URL(resource.url).pathname);
        return {
          byteSize: resource.bytes.byteLength,
          snapshot: { url: finalUrl.toString(), title: readable.title, html },
        };
      }),
    );
    const batchBytes = fetchedBatch.reduce((sum, page) => sum + page.byteSize, 0);
    if (batchBytes > remaining) {
      throw new MaterialIngestionError("Website revision exceeded the 50 MB fetched-content limit.", {
        retryable: false,
      });
    }
    fetchedBytes += batchBytes;
    snapshots.push(...fetchedBatch.map((page) => page.snapshot));
    start += batch.length;
  }

  const snapshotBytes = Buffer.from(JSON.stringify({ version: 1, pages: snapshots }));
  if (snapshotBytes.byteLength > MAX_WEBSITE_REVISION_BYTES) {
    throw new MaterialIngestionError("Website revision exceeded the 50 MB snapshot limit.", {
      retryable: false,
    });
  }
  const writePrisma = getPrisma();
  const leaseExpiresAt = new Date(
    Date.now() + MATERIAL_UPLOAD_URL_EXPIRES_IN_SECONDS * 1_000 + MATERIAL_UPLOAD_LEASE_SAFETY_MS,
  );
  await writePrisma.$transaction(async (transaction) => {
    await transaction.$queryRaw`SELECT "id" FROM "users" WHERE "id" = ${input.userId} FOR UPDATE`;
    if (!(await canWriteWebsiteSnapshot(input, transaction))) {
      throw websiteSnapshotDeletionError();
    }
    const currentSourceFile = await transaction.sourceFile.findFirst({
      where: {
        id: input.sourceFile.id,
        userId: input.userId,
        materialRevisionId: input.materialRevisionId,
      },
      select: { byteSize: true },
    });
    if (!currentSourceFile) {
      throw websiteSnapshotDeletionError();
    }
    const reservedByteSize = Math.max(
      currentSourceFile.byteSize ?? 0,
      snapshotBytes.byteLength,
    );
    const quota = await checkSourceStorageUsageLimit({
      userId: input.userId,
      byteSize: reservedByteSize,
      replaceSourceFileId: input.sourceFile.id,
      prisma: transaction,
    });
    if (quota.status === "limited") {
      throw new MaterialIngestionError(quota.message, { retryable: false });
    }
    const leased = await transaction.sourceFile.updateMany({
      where: {
        id: input.sourceFile.id,
        userId: input.userId,
        materialRevisionId: input.materialRevisionId,
      },
      data: {
        byteSize: reservedByteSize,
        presignedUploadExpiresAt: leaseExpiresAt,
      },
    });
    if (leased.count !== 1) throw websiteSnapshotDeletionError();
  });

  try {
    await putObject({
      key: storageKey,
      bucket: storageBucket,
      bytes: snapshotBytes,
      mimeType: "application/json",
    });
    if (!(await canWriteWebsiteSnapshot(input))) {
      await discardWebsiteSnapshot(input.storage, storageKey, storageBucket);
      throw websiteSnapshotDeletionError();
    }
    const finalizedSource = await writePrisma.sourceFile.updateMany({
      where: {
        id: input.sourceFile.id,
        userId: input.userId,
        materialRevisionId: input.materialRevisionId,
        presignedUploadExpiresAt: leaseExpiresAt,
      },
      data: { byteSize: snapshotBytes.byteLength },
    });
    if (finalizedSource.count !== 1) {
      await discardWebsiteSnapshot(input.storage, storageKey, storageBucket);
      throw websiteSnapshotDeletionError();
    }
  } finally {
    await writePrisma.sourceFile.updateMany({
      where: {
        id: input.sourceFile.id,
        userId: input.userId,
        materialRevisionId: input.materialRevisionId,
        presignedUploadExpiresAt: leaseExpiresAt,
      },
      data: { presignedUploadExpiresAt: null },
    });
  }

  const persisted = await persistWebsiteIndex({
    userId: input.userId,
    materialRevisionId: input.materialRevisionId,
    sourceFileId: input.sourceFile.id,
    snapshots,
  });
  const embedded = await embedPersistedChunks({
    userId: input.userId,
    materialRevisionId: input.materialRevisionId,
    chunks: persisted.chunks,
    embeddingGenerator: input.embeddingGenerator,
  });
  const excerpt = persisted.chunks
    .map((chunk) => chunk.text)
    .join("\n\n")
    .slice(0, MATERIAL_SOURCE_EXCERPT_LIMIT);
  const summary = await generateStoredMaterialSummary({
    generator: input.summaryGenerator,
    materialTitle: input.materialTitle,
    materialKind: "WEB",
    outlineTitles: persisted.sections.map((section) => section.title),
    excerpt,
  });
  const prisma = getPrisma();
  await prisma.sourceFile.update({
    where: { id: input.sourceFile.id },
    data: {
      status: SourceFileStatus.READY,
      byteSize: snapshotBytes.byteLength,
      extractedText: excerpt,
    },
  });
  await finalizeMaterialRevision({
    userId: input.userId,
    materialId: input.materialId,
    materialRevisionId: input.materialRevisionId,
    contentHash: sha256(snapshotBytes),
    byteSize: snapshotBytes.byteLength,
    fetchedPageCount: snapshots.length,
    summary,
    storageBucket: input.sourceFile.storageBucket,
    storageKey: input.sourceFile.storageKey,
    processingMetadata: {
      parser: "cheerio",
      fetchedBytes,
      chunkTargetTokens: 800,
      chunkOverlapTokens: 120,
      embeddingStatus: embedded ? "ready" : "unavailable",
    },
  });

  return { pageCount: snapshots.length, chunkCount: persisted.chunks.length };
}

async function canWriteWebsiteSnapshot(input: {
  userId: string;
  materialId: string;
  materialRevisionId: string;
  sourceFile: MaterialSourceFile;
}, prisma: Pick<Prisma.TransactionClient, "materialRevision" | "accountDeletionJob"> = getPrisma()) {
  const [matchingRevisionCount, deletionJob] = await Promise.all([
    prisma.materialRevision.count({
      where: {
        id: input.materialRevisionId,
        materialId: input.materialId,
        userId: input.userId,
        status: MaterialRevisionStatus.PROCESSING,
        material: { status: StudyMaterialStatus.ACTIVE },
        sourceFiles: { some: { id: input.sourceFile.id, userId: input.userId } },
      },
    }),
    prisma.accountDeletionJob.findUnique({
      where: { userId: input.userId },
      select: { id: true },
    }),
  ]);
  return matchingRevisionCount === 1 && !deletionJob;
}

function websiteSnapshotDeletionError() {
  return new MaterialIngestionError(
    "The material was deleted before its website snapshot could be finalized.",
    { retryable: false },
  );
}

async function discardWebsiteSnapshot(
  storage: SourceObjectStorage,
  key: string,
  bucket: string,
) {
  try {
    await storage.deleteObject({ key, bucket });
  } catch {
    // Preserve the structured deletion error even if best-effort object cleanup fails.
  }
}

type MaterialSourceFile = {
  id: string;
  byteSize: number | null;
  mimeType: string | null;
  storageBucket: string | null;
  storageKey: string | null;
  metadata: Prisma.JsonValue | null;
};

type IndexedPdfPage = ExtractedPdfPage & {
  extractedText?: string;
};

async function persistPdfIndex(input: {
  userId: string;
  materialRevisionId: string;
  sourceFileId: string;
  pages: IndexedPdfPage[];
  sections: ReturnType<typeof buildPdfIndex>["sections"];
  chunks: ReturnType<typeof buildPdfIndex>["chunks"];
}) {
  const prisma = getPrisma();
  const sections = input.sections.map((section) => ({ ...section, id: randomUUID() }));
  const sectionIds = new Map(sections.map((section) => [section.ordinal, section.id]));
  const chunks = input.chunks.map((chunk) => {
    const sectionId = sectionIds.get(chunk.sectionOrdinal);
    if (!sectionId) {
      throw new MaterialIngestionError("PDF chunk did not resolve to an outline section.", {
        retryable: false,
      });
    }
    return { ...chunk, id: randomUUID(), sectionId };
  });

  await prisma.$transaction(async (tx) => {
    const cachedOcrPages = await tx.materialPage.findMany({
      ...readyMaterialOcrPageQuery({
        materialRevisionId: input.materialRevisionId,
        userId: input.userId,
      }),
    });
    await tx.materialChunk.deleteMany({ where: { materialRevisionId: input.materialRevisionId, userId: input.userId } });
    await tx.materialPage.deleteMany({ where: { materialRevisionId: input.materialRevisionId, userId: input.userId } });
    await tx.materialSection.deleteMany({ where: { materialRevisionId: input.materialRevisionId, userId: input.userId } });
    await tx.materialSection.createMany({
      data: sections.map((section) => ({
        id: section.id,
        userId: input.userId,
        materialRevisionId: input.materialRevisionId,
        ordinal: section.ordinal,
        level: section.level,
        title: section.title,
        normalizedTitle: section.normalizedTitle,
        pageStart: section.pageStart,
        pageEnd: section.pageEnd,
        headingPath: section.headingPath,
      })),
    });
    const visualPages = input.pages.filter((page) => page.needsOcr || page.hasVisualContent);
    const pagesToPersist = new Map<
      number,
      {
        pageNumber: number;
        embeddedText: string | null;
        ocrText: string | null;
        textStatus: MaterialPageTextStatus;
        contentHash: string;
        tokenEstimate: number;
        metadata: Prisma.InputJsonValue | typeof Prisma.DbNull;
      }
    >();
    for (const page of visualPages) {
      pagesToPersist.set(page.pageNumber, {
        pageNumber: page.pageNumber,
        embeddedText: (page.extractedText ?? page.text) || null,
        ocrText: null,
        textStatus: page.needsOcr
          ? MaterialPageTextStatus.NEEDS_OCR
          : MaterialPageTextStatus.OCR_READY,
        contentHash: sha256(Buffer.from(page.text || `visual-page-${page.pageNumber}`)),
        tokenEstimate: page.text ? estimateTokens(page.text) : 0,
        metadata: {
          reason: page.needsOcr ? "insufficient-embedded-text" : "visual-content",
        },
      });
    }
    for (const page of cachedOcrPages) {
      const extracted = pagesToPersist.get(page.pageNumber);
      pagesToPersist.set(page.pageNumber, {
        pageNumber: page.pageNumber,
        embeddedText: extracted?.embeddedText ?? page.embeddedText,
        ocrText: page.ocrText,
        textStatus: page.textStatus,
        contentHash: page.contentHash,
        tokenEstimate: page.tokenEstimate,
        metadata: page.metadata ?? Prisma.DbNull,
      });
    }
    if (pagesToPersist.size > 0) {
      await tx.materialPage.createMany({
        data: [...pagesToPersist.values()].map((page) => ({
          userId: input.userId,
          materialRevisionId: input.materialRevisionId,
          pageNumber: page.pageNumber,
          embeddedText: page.embeddedText,
          ocrText: page.ocrText,
          textStatus: page.textStatus,
          contentHash: page.contentHash,
          tokenEstimate: page.tokenEstimate,
          metadata: page.metadata,
        })),
      });
    }
    if (chunks.length > 0) {
      await tx.materialChunk.createMany({
        data: chunks.map((chunk) => ({
          id: chunk.id,
          userId: input.userId,
          materialRevisionId: input.materialRevisionId,
          materialSectionId: chunk.sectionId,
          sourceFileId: input.sourceFileId,
          ordinal: chunk.ordinal,
          text: chunk.text,
          tokenEstimate: chunk.tokenEstimate,
          contentHash: chunk.contentHash,
          headingText: chunk.headingText,
          locator: {
            version: 1,
            kind: "pdf",
            sectionId: chunk.sectionId,
            pageRange: { start: chunk.pageStart, end: chunk.pageEnd },
          },
        })),
      });
    }
  });

  return { sections, chunks };
}

async function persistWebsiteIndex(input: {
  userId: string;
  materialRevisionId: string;
  sourceFileId: string;
  snapshots: Array<{ url: string; title: string; html: string }>;
}) {
  const sections = input.snapshots.map((snapshot, ordinal) => ({
    id: randomUUID(),
    ordinal,
    title: snapshot.title,
    normalizedTitle: normalizeHeading(snapshot.title),
    url: snapshot.url,
    readable: extractReadableWebPage(snapshot.html, snapshot.title),
  }));
  const chunks = sections.flatMap((section) =>
    chunkSectionText(section.readable.text).map((chunk) => ({
      ...chunk,
      id: randomUUID(),
      sectionId: section.id,
      headingText: section.title,
      url: section.url,
      anchor: section.readable.headings[0]?.anchor ?? null,
    })),
  ).map((chunk, ordinal) => ({ ...chunk, ordinal }));
  const prisma = getPrisma();

  await prisma.$transaction(async (tx) => {
    await tx.materialChunk.deleteMany({ where: { materialRevisionId: input.materialRevisionId, userId: input.userId } });
    await tx.materialPage.deleteMany({ where: { materialRevisionId: input.materialRevisionId, userId: input.userId } });
    await tx.materialSection.deleteMany({ where: { materialRevisionId: input.materialRevisionId, userId: input.userId } });
    await tx.materialSection.createMany({
      data: sections.map((section) => ({
        id: section.id,
        userId: input.userId,
        materialRevisionId: input.materialRevisionId,
        ordinal: section.ordinal,
        level: 1,
        title: section.title,
        normalizedTitle: section.normalizedTitle,
        url: section.url,
        anchor: section.readable.headings[0]?.anchor ?? null,
        headingPath: [section.title],
        metadata: { headings: section.readable.headings.slice(0, 100) },
      })),
    });
    if (chunks.length > 0) {
      await tx.materialChunk.createMany({
        data: chunks.map((chunk) => ({
          id: chunk.id,
          userId: input.userId,
          materialRevisionId: input.materialRevisionId,
          materialSectionId: chunk.sectionId,
          sourceFileId: input.sourceFileId,
          ordinal: chunk.ordinal,
          text: chunk.text,
          tokenEstimate: chunk.tokenEstimate,
          contentHash: chunk.contentHash,
          headingText: chunk.headingText,
          locator: {
            version: 1,
            kind: "web",
            sectionId: chunk.sectionId,
            url: chunk.url,
            anchor: chunk.anchor,
          },
        })),
      });
    }
  });

  return { sections, chunks };
}

async function embedPersistedChunks(input: {
  userId: string;
  materialRevisionId: string;
  chunks: Array<{ id: string; text: string; headingText: string }>;
  embeddingGenerator: MaterialEmbeddingGenerator | null;
}) {
  if (!input.embeddingGenerator || input.chunks.length === 0) {
    return false;
  }

  try {
    for (let start = 0; start < input.chunks.length; start += MATERIAL_EMBEDDING_BATCH_SIZE) {
      const batch = input.chunks.slice(start, start + MATERIAL_EMBEDDING_BATCH_SIZE);
      const embeddings = await input.embeddingGenerator({
        texts: batch.map((chunk) => chunk.text),
        titles: batch.map((chunk) => chunk.headingText),
      });
      if (embeddings.length !== batch.length) {
        throw new Error("Embedding generator returned an incomplete batch.");
      }
      await Promise.all(
        batch.map((chunk, index) =>
          storeMaterialChunkEmbedding({
            userId: input.userId,
            materialRevisionId: input.materialRevisionId,
            chunkId: chunk.id,
            embedding: embeddings[index],
          }),
        ),
      );
    }
    return true;
  } catch (error) {
    console.warn("[materials] chunk embeddings unavailable", {
      materialRevisionId: input.materialRevisionId,
      message: error instanceof Error ? error.message : "Unknown embedding error",
    });
    return false;
  }
}

export function buildMaterialObjectKey(input: {
  userId: string;
  materialId: string;
  materialRevisionId: string;
  fileName: string;
}) {
  return [
    MATERIAL_STORAGE_PREFIX,
    sanitizeKeySegment(input.userId),
    sanitizeKeySegment(input.materialId),
    sanitizeKeySegment(input.materialRevisionId),
    sanitizeFileName(input.fileName),
  ].join("/");
}

function resolveMaterialStorage(storage?: SourceObjectStorage) {
  if (storage) {
    return { status: "ready" as const, storage };
  }
  return resolveS3SourceObjectStorage();
}

function safelyResolveEmbeddingGenerator() {
  try {
    return createGeminiMaterialEmbeddingGenerator();
  } catch {
    return null;
  }
}

function safelyResolveSummaryGenerator() {
  try {
    return createGeminiMaterialSummaryGenerator();
  } catch {
    return null;
  }
}

async function generateStoredMaterialSummary(input: {
  generator: MaterialSummaryGenerator | null;
  materialTitle: string;
  materialKind: "PDF" | "WEB";
  outlineTitles: string[];
  excerpt: string;
}) {
  if (!input.generator) {
    return null;
  }

  try {
    const response = materialSummaryResponseSchema.parse(await input.generator({
      materialTitle: input.materialTitle,
      materialKind: input.materialKind,
      outlineTitles: input.outlineTitles,
      excerpt: input.excerpt,
    }));
    return buildStoredMaterialSummary(response);
  } catch (error) {
    console.warn("[ai] material summary unavailable", {
      materialKind: input.materialKind,
      outlineCount: input.outlineTitles.length,
      error: error instanceof Error ? error.message : "Unknown summary error",
    });
    return null;
  }
}

function normalizeIngestionError(error: unknown) {
  if (error instanceof MaterialIngestionError) {
    return error;
  }
  const message = error instanceof Error ? error.message : "Material processing failed.";
  const retryable = !/limit|at most|not readable|not contain enough|missing|invalid|unsupported/i.test(message);
  return new MaterialIngestionError(message, { retryable, cause: error });
}

function readStringArray(value: Prisma.JsonValue | null, key: string) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return [];
  }
  const candidate = value[key];
  return Array.isArray(candidate)
    ? candidate.filter((item): item is string => typeof item === "string" && item.length > 0)
    : [];
}

function normalizeHeading(value: string) {
  return value
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLocaleLowerCase()
    .replace(/[^\p{L}\p{N}]+/gu, " ")
    .trim();
}

function sanitizeKeySegment(value: string) {
  return value.replace(/[^A-Za-z0-9_-]/g, "_").slice(0, 120) || "unknown";
}

function sanitizeFileName(value: string) {
  return value
    .replace(/[\\/]/g, "_")
    .replace(/[^A-Za-z0-9._ -]/g, "_")
    .trim()
    .slice(0, 180) || "material";
}

function sha256(bytes: Buffer) {
  return createHash("sha256").update(bytes).digest("hex");
}
