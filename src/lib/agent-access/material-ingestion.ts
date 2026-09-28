import "server-only";

import {
  AgentOperationKind,
  AgentOperationStatus,
  MaterialRevisionStatus,
  Prisma,
  SourceFileKind,
  SourceFileStatus,
  StudyMaterialKind,
  StudyMaterialStatus,
} from "@/generated/prisma/client";
import type { AgentAuthContext } from "@/lib/agent-access/auth";
import {
  authorizeAgentRead,
  withAgentMutation,
} from "@/lib/agent-access/access";
import { AgentOperationError } from "@/lib/agent-access/operations";
import { buildAgentPayloadHash } from "@/lib/agent-access/contracts";
import {
  agentCompleteMaterialUploadSchema,
  agentGetMaterialStatusSchema,
  agentImportMaterialUrlSchema,
  agentPrepareMaterialUploadSchema,
  agentRetryMaterialIngestionSchema,
  type AgentCompleteMaterialUploadInput,
  type AgentImportMaterialUrlInput,
} from "@/lib/agent-access/material-ingestion-contracts";
import type { MaterialIngestionEventSender } from "@/lib/jobs/events";
import {
  createMaterialIngestionDelivery,
  publishMaterialIngestion,
} from "@/lib/materials/ingestion-publication";
import {
  buildMaterialObjectKey,
  retryMaterialIngestion,
} from "@/lib/materials/ingestion";
import {
  MAX_MATERIAL_PDF_BYTES,
  truncateMaterialTitle,
} from "@/lib/materials/pdf-upload";
import { MAX_WEBSITE_REVISION_PAGES } from "@/lib/materials/contracts";
import {
  discoverBookWebsite,
  validatePublicHttpsUrl,
  type ResolveHostname,
  type WebsiteDiscovery,
} from "@/lib/materials/web";
import { MATERIAL_QUEUE_STALE_AFTER_MS } from "@/lib/materials/ingestion-status";
import { getPrisma } from "@/lib/prisma";
import {
  resolveS3SourceObjectStorage,
  type SourceObjectStorage,
} from "@/lib/storage/s3";
import { checkSourceStorageUsageLimit } from "@/lib/usage-limits";

const MATERIAL_PREPARE_TOOL = "materials.prepare_upload";
const MATERIAL_COMPLETE_TOOL = "materials.complete_upload";
const MATERIAL_IMPORT_TOOL = "materials.import_url";
const MATERIAL_RETRY_TOOL = "materials.retry_ingestion";
const MATERIAL_OPERATION_TOOLS = [
  MATERIAL_PREPARE_TOOL,
  MATERIAL_COMPLETE_TOOL,
  MATERIAL_IMPORT_TOOL,
  MATERIAL_RETRY_TOOL,
] as const;
const MATERIAL_UPLOAD_URL_EXPIRES_IN_SECONDS = 600;
const MATERIAL_UPLOAD_LEASE_SAFETY_MS = 30_000;
const MATERIAL_OPERATION_PAYLOAD_RETENTION_MS = 30 * 24 * 60 * 60 * 1_000;

type AgentMaterialIngestionErrorCode =
  | "idempotency_conflict"
  | "permission_denied"
  | "material_not_found"
  | "stale_material_revision"
  | "invalid_source_url"
  | "material_not_ready"
  | "upload_not_available"
  | "upload_preparation_failed"
  | "event_queue_failed"
  | "operation_not_ready"
  | "operation_not_retryable"
  | "rate_limited";

export class AgentMaterialIngestionError extends Error {
  constructor(
    readonly code: AgentMaterialIngestionErrorCode,
    message: string,
    readonly retryable = false,
  ) {
    super(message);
    this.name = "AgentMaterialIngestionError";
  }
}

export type AgentMaterialIngestionDependencies = {
  storage?: SourceObjectStorage;
  eventSender?: MaterialIngestionEventSender;
  discoverWebsite?: typeof discoverBookWebsite;
  resolveHostname?: ResolveHostname;
};

export type AgentMaterialOperationStatus = {
  operation_id: string;
  operation_uri: string;
  status: string;
  material_id: string | null;
  material_revision_id: string | null;
  source_file_id: string | null;
  error_code: string | null;
  error_message: string | null;
  created_at: string;
  updated_at: string;
  completed_at: string | null;
  retryable: boolean;
};

export type AgentMaterialUploadResponse = AgentMaterialOperationStatus & {
  upload_url: string | null;
  headers: Record<string, string> | null;
  expires_in_seconds: number | null;
};

export type AgentMaterialStatusResponse = {
  material_id: string;
  title: string;
  kind: "pdf" | "web";
  status: string;
  material_revision_id: string;
  revision_number: number;
  revision_status: string;
  source_file_status: string | null;
  is_active_revision: boolean;
  ready: boolean;
  processing: boolean;
  page_count: number | null;
  fetched_page_count: number | null;
  summary: string | null;
  error_code: string | null;
  error_message: string | null;
  created_at: string;
  updated_at: string;
  finalized_at: string | null;
};

/**
 * Reconcile the durable operation row with the native material pipeline.
 *
 * Material ingestion uses AgentSkillOperation for ownership, idempotency, and
 * polling, while MaterialRevision is the source of truth for processing. The
 * MCP operations.get handler calls this before its generic skill operation
 * lookup. Returning null for non-material rows keeps the generic operation
 * contract unchanged; a matching material row is always returned, including
 * when its revision was deleted and the relation is now gone.
 */
export async function getAgentMaterialOperationStatus(
  auth: AgentAuthContext,
  operationId: string,
): Promise<AgentMaterialOperationStatus | null> {
  const prisma = getPrisma();
  const candidate = await prisma.agentSkillOperation.findFirst({
    where: {
      id: operationId,
      userId: auth.userId,
      connectionId: auth.connectionId,
      kind: AgentOperationKind.MATERIAL_BATCH,
      toolName: { in: [...MATERIAL_OPERATION_TOOLS] },
    },
    select: { id: true },
  });
  if (!candidate) return null;

  await authorizeMaterialOperationRead(auth);
  const operation = await prisma.agentSkillOperation.findFirst({
    where: {
      id: candidate.id,
      userId: auth.userId,
      connectionId: auth.connectionId,
      kind: AgentOperationKind.MATERIAL_BATCH,
      toolName: { in: [...MATERIAL_OPERATION_TOOLS] },
    },
    select: MATERIAL_OPERATION_SELECT,
  });
  if (!operation) return null;
  return serializeMaterialOperation(operation);
}

/**
 * Prepare an owned reusable PDF material and an S3 upload lease.
 *
 * The operation row is created in the same serializable transaction as the
 * material, revision, and source row. That gives the agent an idempotency
 * fence without adding a schema migration or exposing storage identifiers.
 */
export async function prepareAgentMaterialUpload(
  auth: AgentAuthContext,
  rawInput: unknown,
  dependencies: AgentMaterialIngestionDependencies = {},
): Promise<AgentMaterialUploadResponse> {
  requireSourceUploadScope(auth);
  const input = agentPrepareMaterialUploadSchema.parse(rawInput);
  const payloadHash = buildAgentPayloadHash(input);
  const storage = resolveMaterialStorage(dependencies.storage);
  if (storage.status === "missing-env") {
    throw new AgentMaterialIngestionError(
      "upload_preparation_failed",
      storage.message,
      true,
    );
  }

  const claim = await runMaterialMutation(auth, async (tx) => {
    const existing = await tx.agentSkillOperation.findUnique({
      where: {
        connectionId_toolName_idempotencyKey: {
          connectionId: auth.connectionId,
          toolName: MATERIAL_PREPARE_TOOL,
          idempotencyKey: input.idempotency_key,
        },
      },
      select: MATERIAL_OPERATION_SELECT,
    });
    if (existing) {
      assertPayloadHash(existing.payloadHash, payloadHash);
      return { created: false as const, operation: existing };
    }

    const quota = await checkSourceStorageUsageLimit({
      userId: auth.userId,
      byteSize: input.byte_size,
      prisma: tx,
    });
    if (quota.status === "limited") {
      throw new AgentMaterialIngestionError(
        "upload_preparation_failed",
        quota.message,
      );
    }

    await assertOwnedCollection(tx, auth.userId, input.collection_id);
    const material = await tx.studyMaterial.create({
      data: {
        userId: auth.userId,
        collectionId: input.collection_id ?? null,
        title: input.title,
        kind: StudyMaterialKind.PDF,
      },
      select: { id: true },
    });
    const revision = await tx.materialRevision.create({
      data: {
        userId: auth.userId,
        materialId: material.id,
        revisionNumber: 1,
        status: MaterialRevisionStatus.PENDING_UPLOAD,
        storageBucket: storage.storage.bucketName,
      },
      select: { id: true },
    });
    const objectKey = buildMaterialObjectKey({
      userId: auth.userId,
      materialId: material.id,
      materialRevisionId: revision.id,
      fileName: input.original_name,
    });
    const expiresAt = new Date(
      Date.now() +
        MATERIAL_UPLOAD_URL_EXPIRES_IN_SECONDS * 1_000 +
        MATERIAL_UPLOAD_LEASE_SAFETY_MS,
    );
    const sourceFile = await tx.sourceFile.create({
      data: {
        userId: auth.userId,
        collectionId: input.collection_id ?? null,
        materialRevisionId: revision.id,
        kind: SourceFileKind.PDF,
        status: SourceFileStatus.DRAFT,
        originalName: input.original_name,
        mimeType: input.mime_type,
        byteSize: input.byte_size,
        storageBucket: storage.storage.bucketName,
        storageKey: objectKey,
        presignedUploadExpiresAt: expiresAt,
      },
      select: { id: true },
    });
    await tx.materialRevision.update({
      where: { id: revision.id },
      data: { storageKey: objectKey },
    });
    const operation = await tx.agentSkillOperation.create({
      data: {
        userId: auth.userId,
        connectionId: auth.connectionId,
        kind: AgentOperationKind.MATERIAL_BATCH,
        toolName: MATERIAL_PREPARE_TOOL,
        status: AgentOperationStatus.AWAITING_UPLOAD,
        idempotencyKey: input.idempotency_key,
        payloadHash,
        requestPayload: toJson(input),
        payloadExpiresAt: new Date(
          Date.now() + MATERIAL_OPERATION_PAYLOAD_RETENTION_MS,
        ),
        materialRevisionId: revision.id,
        sourceFileId: sourceFile.id,
      },
      select: MATERIAL_OPERATION_SELECT,
    });
    return {
      created: true as const,
      operation,
      objectKey,
      sourceFileId: sourceFile.id,
      materialRevisionId: revision.id,
      materialId: material.id,
    };
  });

  if (!claim.created) {
    return refreshOrSerializePreparedUpload({
      auth,
      operation: claim.operation,
      storage: storage.storage,
    });
  }

  try {
    const uploadUrl = await storage.storage.createPresignedUploadUrl({
      key: claim.objectKey,
      mimeType: input.mime_type,
      byteSize: input.byte_size,
      maxBytes: MAX_MATERIAL_PDF_BYTES,
      expiresInSeconds: MATERIAL_UPLOAD_URL_EXPIRES_IN_SECONDS,
    });
    const updated = await getPrisma().agentSkillOperation.findUniqueOrThrow({
      where: { id: claim.operation.id },
      select: MATERIAL_OPERATION_SELECT,
    });
    return buildUploadResponse(updated, {
      uploadUrl,
      expiresInSeconds: MATERIAL_UPLOAD_URL_EXPIRES_IN_SECONDS,
    });
  } catch {
    await markMaterialPreparationFailed({
      auth,
      operationId: claim.operation.id,
      materialId: claim.materialId,
      message: "Could not prepare the private PDF upload.",
    });
    throw new AgentMaterialIngestionError(
      "upload_preparation_failed",
      "The private material upload could not be prepared.",
      true,
    );
  }
}

/**
 * Verify an uploaded PDF, transition its revision to queued, and enqueue the
 * existing material ingestion worker. Storage verification happens before the
 * database mutation fence; the fenced claim still re-reads the owned source
 * snapshot so a concurrent change cannot enqueue the wrong object.
 */
export async function completeAgentMaterialUpload(
  auth: AgentAuthContext,
  rawInput: unknown,
  dependencies: AgentMaterialIngestionDependencies = {},
): Promise<AgentMaterialOperationStatus> {
  requireSourceUploadScope(auth);
  const input = agentCompleteMaterialUploadSchema.parse(rawInput);
  const payloadHash = buildAgentPayloadHash(input);
  await authorizeAgentRead(auth, "sources:upload");

  const replay = await findMaterialCompletionReplay(auth, input, payloadHash);
  if (replay) return serializeMaterialOperation(replay);

  const storage = resolveMaterialStorage(dependencies.storage);
  if (storage.status === "missing-env") {
    throw new AgentMaterialIngestionError(
      "upload_preparation_failed",
      storage.message,
      true,
    );
  }

  const preparedOperation = await findPreparedMaterialOperation(
    getPrisma(),
    auth,
    input,
  );
  if (!preparedOperation) {
    throw new AgentMaterialIngestionError(
      "material_not_found",
      "The prepared material upload was not found.",
    );
  }
  assertMaterialOperationIdentifiers(preparedOperation, input);
  const preparedSnapshot = await readPreparedMaterialUploadSnapshot(
    getPrisma(),
    auth,
    input,
    preparedOperation,
  );
  const verifiedHead =
    preparedSnapshot.revision.status === MaterialRevisionStatus.PENDING_UPLOAD
      ? await verifyPreparedMaterialUpload(
          storage.storage,
          preparedSnapshot.sourceFile,
        )
      : null;

  const claim = await runMaterialMutation(auth, async (tx) => {
    const replay = await tx.agentOperationAction.findUnique({
      where: {
        connectionId_toolName_idempotencyKey: {
          connectionId: auth.connectionId,
          toolName: MATERIAL_COMPLETE_TOOL,
          idempotencyKey: input.idempotency_key,
        },
      },
      select: { operationId: true, userId: true, payloadHash: true },
    });
    if (replay) {
      if (replay.userId !== auth.userId) {
        throw new AgentMaterialIngestionError(
          "idempotency_conflict",
          "That idempotency key was already used for a different material operation.",
        );
      }
      assertPayloadHash(replay.payloadHash, payloadHash);
      return {
        operation: await refreshMaterialOperation(tx, replay.operationId),
        enqueue: false as const,
      };
    }

    const operation = await findPreparedMaterialOperation(tx, auth, input);
    if (!operation) {
      throw new AgentMaterialIngestionError(
        "material_not_found",
        "The prepared material upload was not found.",
      );
    }

    assertMaterialOperationIdentifiers(operation, input);
    const currentSnapshot = await readPreparedMaterialUploadSnapshot(
      tx,
      auth,
      input,
      operation,
    );

    if (
      currentSnapshot.revision.status !== MaterialRevisionStatus.PENDING_UPLOAD
    ) {
      await recordMaterialAction(
        tx,
        auth,
        operation.id,
        MATERIAL_COMPLETE_TOOL,
        input.idempotency_key,
        payloadHash,
      );
      return {
        operation: await refreshMaterialOperation(tx, operation.id),
        enqueue: false as const,
      };
    }
    if (
      verifiedHead === null ||
      !samePreparedMaterialUploadSnapshot(preparedSnapshot, currentSnapshot)
    ) {
      throw new AgentMaterialIngestionError(
        "stale_material_revision",
        "The prepared material upload changed while it was being verified. Prepare a new material upload.",
      );
    }
    const sourceFile = currentSnapshot.sourceFile;
    const revision = currentSnapshot.revision;
    if (
      sourceFile.status !== SourceFileStatus.DRAFT ||
      !sourceFile.storageBucket ||
      !sourceFile.storageKey ||
      sourceFile.mimeType !== "application/pdf" ||
      sourceFile.byteSize === null
    ) {
      throw new AgentMaterialIngestionError(
        "upload_not_available",
        "The prepared PDF upload is no longer available. Prepare a new material upload.",
      );
    }

    const claimed = await tx.materialRevision.updateMany({
      where: {
        id: revision.id,
        userId: auth.userId,
        materialId: revision.materialId,
        status: MaterialRevisionStatus.PENDING_UPLOAD,
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
          ingestionDelivery: createMaterialIngestionDelivery(new Date()),
        },
      },
    });
    if (claimed.count !== 1) {
      const current = await refreshMaterialOperation(tx, operation.id);
      await recordMaterialAction(
        tx,
        auth,
        operation.id,
        MATERIAL_COMPLETE_TOOL,
        input.idempotency_key,
        payloadHash,
      );
      return { operation: current, enqueue: false as const };
    }
    const uploaded = await tx.sourceFile.updateMany({
      where: {
        id: sourceFile.id,
        userId: auth.userId,
        materialRevisionId: revision.id,
        status: SourceFileStatus.DRAFT,
        storageBucket: sourceFile.storageBucket,
        storageKey: sourceFile.storageKey,
        mimeType: sourceFile.mimeType,
        byteSize: sourceFile.byteSize,
      },
      data: { status: SourceFileStatus.UPLOADED },
    });
    if (uploaded.count !== 1) {
      throw new AgentMaterialIngestionError(
        "stale_material_revision",
        "The prepared material upload changed while it was being verified. Prepare a new material upload.",
      );
    }
    await tx.agentSkillOperation.update({
      where: { id: operation.id },
      data: {
        status: AgentOperationStatus.QUEUED,
        errorCode: null,
        errorMessage: null,
        completedAt: null,
      },
    });
    await recordMaterialAction(
      tx,
      auth,
      operation.id,
      MATERIAL_COMPLETE_TOOL,
      input.idempotency_key,
      payloadHash,
    );
    return {
      operation: await refreshMaterialOperation(tx, operation.id),
      enqueue: true as const,
    };
  });

  if (!claim.enqueue) return serializeMaterialOperation(claim.operation);
  const queued = await sendMaterialIngestionEvent({
    auth,
    operation: claim.operation,
    eventSender: dependencies.eventSender,
  });
  return serializeMaterialOperation(queued);
}

/**
 * Import a public, same-origin set of textbook pages into the reusable
 * material library. Discovery is deliberately bounded and uses the same SSRF
 * protections as the native web importer.
 */
export async function importAgentMaterialUrl(
  auth: AgentAuthContext,
  rawInput: unknown,
  dependencies: AgentMaterialIngestionDependencies = {},
): Promise<AgentMaterialOperationStatus> {
  requireSourceUploadScope(auth);
  const input = agentImportMaterialUrlSchema.parse(rawInput);
  const payloadHash = buildAgentPayloadHash(input);

  // Idempotency must fence the request before URL discovery or DNS
  // validation. A replay should return the owned operation even when the
  // source site is unavailable now. The create transaction below repeats this
  // lookup so two concurrent requests cannot create two materials.
  await authorizeAgentRead(auth, "sources:upload");
  const replay = await getPrisma().agentSkillOperation.findUnique({
    where: {
      connectionId_toolName_idempotencyKey: {
        connectionId: auth.connectionId,
        toolName: MATERIAL_IMPORT_TOOL,
        idempotencyKey: input.idempotency_key,
      },
    },
    select: MATERIAL_OPERATION_SELECT,
  });
  if (replay) {
    assertPayloadHash(replay.payloadHash, payloadHash);
    return serializeMaterialOperation(replay);
  }

  const storage = resolveMaterialStorage(dependencies.storage);
  if (storage.status === "missing-env") {
    throw new AgentMaterialIngestionError(
      "upload_preparation_failed",
      storage.message,
      true,
    );
  }

  const discovered = await resolveWebsiteImport(input, dependencies);
  const claim = await runMaterialMutation(auth, async (tx) => {
    const replayAfterDiscovery = await tx.agentSkillOperation.findUnique({
      where: {
        connectionId_toolName_idempotencyKey: {
          connectionId: auth.connectionId,
          toolName: MATERIAL_IMPORT_TOOL,
          idempotencyKey: input.idempotency_key,
        },
      },
      select: MATERIAL_OPERATION_SELECT,
    });
    if (replayAfterDiscovery) {
      assertPayloadHash(replayAfterDiscovery.payloadHash, payloadHash);
      return { created: false as const, operation: replayAfterDiscovery };
    }

    await assertOwnedCollection(tx, auth.userId, input.collection_id);
    const material = await tx.studyMaterial.create({
      data: {
        userId: auth.userId,
        collectionId: input.collection_id ?? null,
        title: discovered.title,
        kind: StudyMaterialKind.WEB,
      },
      select: { id: true },
    });
    const revision = await tx.materialRevision.create({
      data: {
        userId: auth.userId,
        materialId: material.id,
        revisionNumber: 1,
        status: MaterialRevisionStatus.QUEUED,
        sourceUrl: discovered.sourceUrl,
        storageBucket: storage.storage.bucketName,
        processingMetadata: {
          ingestionDelivery: createMaterialIngestionDelivery(new Date()),
        },
      },
      select: { id: true },
    });
    const storageKey = buildMaterialObjectKey({
      userId: auth.userId,
      materialId: material.id,
      materialRevisionId: revision.id,
      fileName: "website-snapshot.json",
    });
    await tx.materialRevision.update({
      where: { id: revision.id },
      data: { storageKey },
    });
    const sourceFile = await tx.sourceFile.create({
      data: {
        userId: auth.userId,
        collectionId: input.collection_id ?? null,
        materialRevisionId: revision.id,
        kind: SourceFileKind.URL,
        status: SourceFileStatus.UPLOADED,
        originalName: discovered.title,
        mimeType: "application/json",
        storageBucket: storage.storage.bucketName,
        storageKey,
        metadata: toJson({
          sourceUrl: discovered.sourceUrl,
          selectedUrls: discovered.selectedUrls,
        }),
      },
      select: { id: true },
    });
    const operation = await tx.agentSkillOperation.create({
      data: {
        userId: auth.userId,
        connectionId: auth.connectionId,
        kind: AgentOperationKind.MATERIAL_BATCH,
        toolName: MATERIAL_IMPORT_TOOL,
        status: AgentOperationStatus.QUEUED,
        idempotencyKey: input.idempotency_key,
        payloadHash,
        requestPayload: toJson({
          ...input,
          selected_urls: discovered.selectedUrls,
          source_url: discovered.sourceUrl,
        }),
        payloadExpiresAt: new Date(
          Date.now() + MATERIAL_OPERATION_PAYLOAD_RETENTION_MS,
        ),
        materialRevisionId: revision.id,
        sourceFileId: sourceFile.id,
      },
      select: MATERIAL_OPERATION_SELECT,
    });
    return { created: true as const, operation };
  });

  if (!claim.created) return serializeMaterialOperation(claim.operation);
  const queued = await sendMaterialIngestionEvent({
    auth,
    operation: claim.operation,
    eventSender: dependencies.eventSender,
  });
  return serializeMaterialOperation(queued);
}

export async function getAgentMaterialStatus(
  auth: AgentAuthContext,
  rawInput: unknown,
): Promise<AgentMaterialStatusResponse> {
  const input = agentGetMaterialStatusSchema.parse(rawInput);
  await authorizeAgentRead(auth, "materials:read");
  const material = await getPrisma().studyMaterial.findFirst({
    where: {
      id: input.material_id,
      userId: auth.userId,
      status: StudyMaterialStatus.ACTIVE,
    },
    select: {
      id: true,
      title: true,
      kind: true,
      status: true,
      activeRevisionId: true,
      revisions: {
        where: input.expected_revision_id
          ? { id: input.expected_revision_id }
          : undefined,
        orderBy: { revisionNumber: "desc" },
        take: 1,
        select: {
          id: true,
          revisionNumber: true,
          status: true,
          pageCount: true,
          fetchedPageCount: true,
          summary: true,
          errorCode: true,
          errorMessage: true,
          createdAt: true,
          updatedAt: true,
          finalizedAt: true,
          sourceFiles: {
            orderBy: { createdAt: "asc" },
            take: 1,
            select: { status: true },
          },
        },
      },
    },
  });
  if (!material) {
    throw new AgentMaterialIngestionError(
      "material_not_found",
      "The material was not found.",
    );
  }
  const revision = material.revisions[0];
  if (!revision) {
    throw new AgentMaterialIngestionError(
      "material_not_found",
      "The material revision was not found.",
    );
  }
  if (
    input.expected_revision_id &&
    material.activeRevisionId &&
    material.activeRevisionId !== input.expected_revision_id
  ) {
    throw new AgentMaterialIngestionError(
      "stale_material_revision",
      "The active material revision changed. Refresh the material and try again.",
    );
  }
  const processingStatuses: MaterialRevisionStatus[] = [
    MaterialRevisionStatus.PENDING_UPLOAD,
    MaterialRevisionStatus.QUEUED,
    MaterialRevisionStatus.PROCESSING,
  ];
  const processing = processingStatuses.includes(revision.status);
  return {
    material_id: material.id,
    title: material.title,
    kind: material.kind === StudyMaterialKind.PDF ? "pdf" : "web",
    status: material.status.toLocaleLowerCase("en-US"),
    material_revision_id: revision.id,
    revision_number: revision.revisionNumber,
    revision_status: revision.status.toLocaleLowerCase("en-US"),
    source_file_status:
      revision.sourceFiles[0]?.status.toLocaleLowerCase("en-US") ?? null,
    is_active_revision: material.activeRevisionId === revision.id,
    ready: revision.status === MaterialRevisionStatus.READY,
    processing,
    page_count: revision.pageCount,
    fetched_page_count: revision.fetchedPageCount,
    summary: truncate(revision.summary, 1_000),
    error_code: revision.errorCode,
    error_message: truncate(revision.errorMessage, 1_000),
    created_at: revision.createdAt.toISOString(),
    updated_at: revision.updatedAt.toISOString(),
    finalized_at: revision.finalizedAt?.toISOString() ?? null,
  } satisfies AgentMaterialStatusResponse;
}

/** Retry a failed/stalled material revision using the native repair service. */
export async function retryAgentMaterialIngestion(
  auth: AgentAuthContext,
  rawInput: unknown,
  dependencies: AgentMaterialIngestionDependencies = {},
): Promise<AgentMaterialOperationStatus> {
  requireSourceUploadScope(auth);
  const input = agentRetryMaterialIngestionSchema.parse(rawInput);
  const payloadHash = buildAgentPayloadHash(input);
  const claim = await runMaterialMutation(auth, async (tx) => {
    const existing = await tx.agentSkillOperation.findUnique({
      where: {
        connectionId_toolName_idempotencyKey: {
          connectionId: auth.connectionId,
          toolName: MATERIAL_RETRY_TOOL,
          idempotencyKey: input.idempotency_key,
        },
      },
      select: MATERIAL_OPERATION_SELECT,
    });
    if (existing) {
      assertPayloadHash(existing.payloadHash, payloadHash);
      return { created: false as const, operation: existing };
    }
    if (input.operation_id) {
      const referenced = await tx.agentSkillOperation.findFirst({
        where: {
          id: input.operation_id,
          userId: auth.userId,
          connectionId: auth.connectionId,
          materialRevisionId: input.material_revision_id,
          toolName: {
            in: [
              MATERIAL_PREPARE_TOOL,
              MATERIAL_COMPLETE_TOOL,
              MATERIAL_IMPORT_TOOL,
              MATERIAL_RETRY_TOOL,
            ],
          },
        },
        select: { id: true },
      });
      if (!referenced) {
        throw new AgentMaterialIngestionError(
          "operation_not_ready",
          "The material operation is not owned by this connection or revision.",
        );
      }
    }
    const material = input.material_id
      ? await tx.studyMaterial.findFirst({
          where: {
            id: input.material_id,
            userId: auth.userId,
            status: StudyMaterialStatus.ACTIVE,
          },
          select: { id: true },
        })
      : null;
    const revision = await tx.materialRevision.findFirst({
      where: {
        id: input.material_revision_id,
        userId: auth.userId,
        ...(input.material_id ? { materialId: input.material_id } : {}),
        material: { status: StudyMaterialStatus.ACTIVE },
      },
      select: {
        id: true,
        materialId: true,
        status: true,
        sourceFiles: {
          orderBy: { createdAt: "asc" },
          take: 1,
          select: { id: true },
        },
      },
    });
    if (!revision || (input.material_id && !material)) {
      throw new AgentMaterialIngestionError(
        "material_not_found",
        "The material revision was not found.",
      );
    }
    if (
      revision.status !== MaterialRevisionStatus.FAILED &&
      !(
        revision.status === MaterialRevisionStatus.QUEUED &&
        (await tx.materialRevision.findFirst({
          where: {
            id: revision.id,
            updatedAt: {
              lte: new Date(Date.now() - MATERIAL_QUEUE_STALE_AFTER_MS),
            },
          },
          select: { id: true },
        }))
      )
    ) {
      if (revision.status === MaterialRevisionStatus.READY) {
        const operation = await tx.agentSkillOperation.create({
          data: {
            userId: auth.userId,
            connectionId: auth.connectionId,
            kind: AgentOperationKind.MATERIAL_BATCH,
            toolName: MATERIAL_RETRY_TOOL,
            status: AgentOperationStatus.SUCCEEDED,
            idempotencyKey: input.idempotency_key,
            payloadHash,
            requestPayload: toJson(input),
            materialRevisionId: revision.id,
            sourceFileId: revision.sourceFiles[0]?.id,
            requestedCount: 0,
            activeCount: 1,
            completedAt: new Date(),
          },
          select: MATERIAL_OPERATION_SELECT,
        });
        return { created: true as const, operation, skipRetry: true as const };
      }
      throw new AgentMaterialIngestionError(
        "operation_not_retryable",
        "Material processing is still running and cannot be retried yet.",
      );
    }
    const operation = await tx.agentSkillOperation.create({
      data: {
        userId: auth.userId,
        connectionId: auth.connectionId,
        kind: AgentOperationKind.MATERIAL_BATCH,
        toolName: MATERIAL_RETRY_TOOL,
        status: AgentOperationStatus.QUEUED,
        idempotencyKey: input.idempotency_key,
        payloadHash,
        requestPayload: toJson(input),
        payloadExpiresAt: new Date(
          Date.now() + MATERIAL_OPERATION_PAYLOAD_RETENTION_MS,
        ),
        materialRevisionId: revision.id,
        sourceFileId: revision.sourceFiles[0]?.id,
      },
      select: MATERIAL_OPERATION_SELECT,
    });
    return { created: true as const, operation, skipRetry: false as const };
  });

  if (!claim.created || claim.skipRetry)
    return serializeMaterialOperation(claim.operation);
  const result = await retryMaterialIngestion({
    userId: auth.userId,
    materialRevisionId: input.material_revision_id,
    now: new Date(),
    eventSender: dependencies.eventSender,
  });
  if (result.status !== "queued") {
    const failed = await getPrisma().agentSkillOperation.update({
      where: { id: claim.operation.id },
      data: {
        status: AgentOperationStatus.FAILED,
        errorCode:
          result.status === "not-found" ? "NOT_RETRYABLE" : "EVENT_SEND_FAILED",
        errorMessage: result.message,
        completedAt: new Date(),
      },
      select: MATERIAL_OPERATION_SELECT,
    });
    return serializeMaterialOperation(failed);
  }
  const updated = await getPrisma().agentSkillOperation.findUniqueOrThrow({
    where: { id: claim.operation.id },
    select: MATERIAL_OPERATION_SELECT,
  });
  return serializeMaterialOperation(updated);
}

const MATERIAL_OPERATION_SELECT = {
  id: true,
  userId: true,
  connectionId: true,
  toolName: true,
  status: true,
  idempotencyKey: true,
  payloadHash: true,
  materialRevisionId: true,
  sourceFileId: true,
  errorCode: true,
  errorMessage: true,
  createdAt: true,
  updatedAt: true,
  completedAt: true,
  materialRevision: {
    select: {
      materialId: true,
      status: true,
      errorCode: true,
      errorMessage: true,
      finalizedAt: true,
      material: {
        select: { status: true },
      },
    },
  },
} satisfies Prisma.AgentSkillOperationSelect;

type MaterialOperation = Prisma.AgentSkillOperationGetPayload<{
  select: typeof MATERIAL_OPERATION_SELECT;
}>;

type MaterialOperationDatabase = Pick<
  Prisma.TransactionClient,
  "agentSkillOperation" | "materialRevision" | "sourceFile" | "studyMaterial"
>;

type PreparedMaterialUploadSnapshot = {
  revision: {
    id: string;
    materialId: string;
    status: MaterialRevisionStatus;
    processingMetadata?: Prisma.JsonValue | null;
  };
  sourceFile: {
    id: string;
    status: SourceFileStatus;
    storageBucket: string | null;
    storageKey: string | null;
    mimeType: string | null;
    byteSize: number | null;
  };
};

type PreparedUploadLeaseSnapshot = {
  revision: PreparedMaterialUploadSnapshot["revision"];
  sourceFile: PreparedMaterialUploadSnapshot["sourceFile"] & {
    presignedUploadExpiresAt: Date | null;
  };
};

function serializeMaterialOperation(
  operation: MaterialOperation,
): AgentMaterialOperationStatus {
  const terminalStatuses: AgentOperationStatus[] = [
    AgentOperationStatus.SUCCEEDED,
    AgentOperationStatus.PARTIAL,
    AgentOperationStatus.FAILED,
    AgentOperationStatus.CANCELED,
  ];
  const materialStatus = operation.materialRevision?.status;
  const materialDeleted =
    operation.materialRevision === null ||
    materialStatus === MaterialRevisionStatus.DELETING ||
    operation.materialRevision?.material.status ===
      StudyMaterialStatus.DELETING;
  const preparationCleanupFailure =
    operation.materialRevision === null &&
    operation.errorCode === "UPLOAD_PREPARATION_FAILED";
  const status =
    operation.toolName.startsWith("materials.") && !preparationCleanupFailure
      ? materialDeleted
        ? AgentOperationStatus.CANCELED
        : materialStatus === MaterialRevisionStatus.READY
          ? AgentOperationStatus.SUCCEEDED
          : materialStatus === MaterialRevisionStatus.FAILED
            ? AgentOperationStatus.FAILED
            : operation.status
      : operation.status;
  const deletedError = materialDeleted && !preparationCleanupFailure;
  const errorCode =
    status === AgentOperationStatus.SUCCEEDED
      ? null
      : deletedError
        ? "MATERIAL_DELETED"
        : (operation.errorCode ??
          operation.materialRevision?.errorCode ??
          null);
  const errorMessage =
    status === AgentOperationStatus.SUCCEEDED
      ? null
      : deletedError
        ? "The material was deleted before ingestion completed."
        : (operation.errorMessage ??
          operation.materialRevision?.errorMessage ??
          null);
  const terminal = terminalStatuses.includes(status);
  const retryable =
    status === AgentOperationStatus.FAILED &&
    ["EVENT_SEND_FAILED", "TRANSIENT_INGESTION_FAILURE"].includes(
      errorCode ?? "",
    );
  return {
    operation_id: operation.id,
    operation_uri: `learnrecur://material-operations/${operation.id}`,
    status: status.toLocaleLowerCase("en-US"),
    material_id: operation.materialRevision?.materialId ?? null,
    material_revision_id: operation.materialRevisionId,
    source_file_id: operation.sourceFileId,
    error_code: errorCode,
    error_message: truncate(errorMessage, 1_000),
    created_at: operation.createdAt.toISOString(),
    updated_at: operation.updatedAt.toISOString(),
    completed_at:
      status === AgentOperationStatus.SUCCEEDED
        ? (operation.materialRevision?.finalizedAt?.toISOString() ??
          operation.completedAt?.toISOString() ??
          null)
        : (operation.completedAt?.toISOString() ?? null),
    retryable: terminal ? retryable : false,
  };
}

function buildUploadResponse(
  operation: MaterialOperation,
  upload: { uploadUrl: string; expiresInSeconds: number },
): AgentMaterialUploadResponse {
  return {
    ...serializeMaterialOperation(operation),
    status: "prepared",
    upload_url: upload.uploadUrl,
    headers: { "Content-Type": "application/pdf" },
    expires_in_seconds: upload.expiresInSeconds,
  };
}

async function refreshOrSerializePreparedUpload(input: {
  auth: AgentAuthContext;
  operation: MaterialOperation;
  storage: SourceObjectStorage;
}): Promise<AgentMaterialUploadResponse> {
  if (
    input.operation.status !== AgentOperationStatus.AWAITING_UPLOAD ||
    !input.operation.materialRevisionId ||
    !input.operation.sourceFileId
  ) {
    return {
      ...serializeMaterialOperation(input.operation),
      upload_url: null,
      headers: null,
      expires_in_seconds: null,
    };
  }
  const preflight = await withAgentMutation(
    input.auth,
    "sources:upload",
    async (tx) => {
      const operation = await tx.agentSkillOperation.findFirst({
        where: {
          id: input.operation.id,
          userId: input.auth.userId,
          connectionId: input.auth.connectionId,
          toolName: MATERIAL_PREPARE_TOOL,
        },
        select: MATERIAL_OPERATION_SELECT,
      });
      if (!operation) {
        return { operation: input.operation, snapshot: null };
      }
      if (
        operation.status !== AgentOperationStatus.AWAITING_UPLOAD ||
        !operation.materialRevisionId ||
        !operation.sourceFileId
      ) {
        return { operation, snapshot: null };
      }
      return {
        operation,
        snapshot: await readPreparedUploadLeaseSnapshot(
          tx,
          input.auth,
          operation,
        ),
      };
    },
    undefined,
    // The idempotency claim in prepareAgentMaterialUpload already charged
    // this logical request. Recheck access and the owned snapshot without a
    // second rate charge before making the external presign call.
    { consumeMutationRate: false },
  );
  if (!preflight.snapshot) {
    return {
      ...serializeMaterialOperation(preflight.operation),
      upload_url: null,
      headers: null,
      expires_in_seconds: null,
    };
  }
  const sourceFile = preflight.snapshot.sourceFile;
  if (
    sourceFile.mimeType !== "application/pdf" ||
    sourceFile.byteSize === null ||
    !sourceFile.storageBucket ||
    !sourceFile.storageKey
  ) {
    throw new AgentMaterialIngestionError(
      "stale_material_revision",
      "The prepared material upload is no longer available. Prepare a new material upload.",
    );
  }
  const uploadUrl = await input.storage.createPresignedUploadUrl({
    key: sourceFile.storageKey,
    mimeType: sourceFile.mimeType,
    byteSize: sourceFile.byteSize,
    maxBytes: MAX_MATERIAL_PDF_BYTES,
    expiresInSeconds: MATERIAL_UPLOAD_URL_EXPIRES_IN_SECONDS,
  });
  const claimed = await withAgentMutation(
    input.auth,
    "sources:upload",
    async (tx) => {
      const operation = await tx.agentSkillOperation.findFirst({
        where: {
          id: input.operation.id,
          userId: input.auth.userId,
          connectionId: input.auth.connectionId,
          toolName: MATERIAL_PREPARE_TOOL,
        },
        select: MATERIAL_OPERATION_SELECT,
      });
      if (!operation) {
        return { operation: preflight.operation, claimed: false as const };
      }
      if (
        operation.status !== AgentOperationStatus.AWAITING_UPLOAD ||
        !operation.materialRevisionId ||
        !operation.sourceFileId
      ) {
        return { operation, claimed: false as const };
      }
      const current = await readPreparedUploadLeaseSnapshot(
        tx,
        input.auth,
        operation,
      );
      if (!samePreparedUploadLeaseSnapshot(preflight.snapshot, current)) {
        throw new AgentMaterialIngestionError(
          "stale_material_revision",
          "The prepared material upload changed while its upload URL was being refreshed. Prepare a new material upload.",
        );
      }
      const leaseExpiresAt = new Date(
        Date.now() +
          MATERIAL_UPLOAD_URL_EXPIRES_IN_SECONDS * 1_000 +
          MATERIAL_UPLOAD_LEASE_SAFETY_MS,
      );
      const leased = await tx.sourceFile.updateMany({
        where: {
          id: current.sourceFile.id,
          userId: input.auth.userId,
          materialRevisionId: current.revision.id,
          status: SourceFileStatus.DRAFT,
          storageBucket: current.sourceFile.storageBucket,
          storageKey: current.sourceFile.storageKey,
          mimeType: current.sourceFile.mimeType,
          byteSize: current.sourceFile.byteSize,
          presignedUploadExpiresAt: current.sourceFile.presignedUploadExpiresAt,
        },
        data: { presignedUploadExpiresAt: leaseExpiresAt },
      });
      if (leased.count !== 1) {
        throw new AgentMaterialIngestionError(
          "stale_material_revision",
          "The prepared material upload changed while its upload URL was being refreshed. Prepare a new material upload.",
        );
      }
      return { operation, claimed: true as const };
    },
    undefined,
    // The idempotency claim already charged this logical request. Recheck
    // every connection and lease invariant after presigning, but do not
    // charge the same idempotent upload retry again.
    { consumeMutationRate: false },
  );
  if (!claimed.claimed) {
    return {
      ...serializeMaterialOperation(claimed.operation),
      upload_url: null,
      headers: null,
      expires_in_seconds: null,
    };
  }
  return buildUploadResponse(claimed.operation, {
    uploadUrl,
    expiresInSeconds: MATERIAL_UPLOAD_URL_EXPIRES_IN_SECONDS,
  });
}

async function markMaterialPreparationFailed(input: {
  auth: AgentAuthContext;
  operationId: string;
  materialId: string;
  message: string;
}) {
  const prisma = getPrisma();
  await prisma.$transaction(async (tx) => {
    await tx.agentSkillOperation.updateMany({
      where: {
        id: input.operationId,
        userId: input.auth.userId,
        connectionId: input.auth.connectionId,
        status: AgentOperationStatus.AWAITING_UPLOAD,
      },
      data: {
        status: AgentOperationStatus.FAILED,
        errorCode: "UPLOAD_PREPARATION_FAILED",
        errorMessage: input.message.slice(0, 1_000),
        completedAt: new Date(),
      },
    });
    await tx.studyMaterial.deleteMany({
      where: { id: input.materialId, userId: input.auth.userId },
    });
  });
}

async function sendMaterialIngestionEvent(input: {
  auth: AgentAuthContext;
  operation: MaterialOperation;
  eventSender?: MaterialIngestionEventSender;
}): Promise<MaterialOperation> {
  if (!input.operation.materialRevisionId) return input.operation;
  // The transaction has already persisted the intent. A temporary or ambiguous
  // send stays queued; maintenance owns bounded recovery and terminal failure.
  await publishMaterialIngestion({
    userId: input.auth.userId,
    materialRevisionId: input.operation.materialRevisionId,
    now: new Date(),
    sender: input.eventSender,
  });
  return getPrisma().agentSkillOperation.findUniqueOrThrow({
    where: { id: input.operation.id },
    select: MATERIAL_OPERATION_SELECT,
  });
}

async function resolveWebsiteImport(
  input: AgentImportMaterialUrlInput,
  dependencies: AgentMaterialIngestionDependencies,
): Promise<{ title: string; sourceUrl: string; selectedUrls: string[] }> {
  if (input.selected_urls?.length) {
    try {
      const source = await validatePublicHttpsUrl(
        input.source_url,
        dependencies.resolveHostname,
      );
      const selectedUrls = await validateSameOriginWebsiteUrls(
        source,
        input.selected_urls,
        dependencies.resolveHostname,
      );
      return {
        title: input.title ?? truncateMaterialTitle(source.hostname),
        sourceUrl: source.toString(),
        selectedUrls,
      };
    } catch (error) {
      throw new AgentMaterialIngestionError(
        "invalid_source_url",
        error instanceof Error
          ? error.message
          : "The source URL is not supported.",
      );
    }
  }

  let discovery: WebsiteDiscovery;
  try {
    discovery = await (dependencies.discoverWebsite ?? discoverBookWebsite)({
      url: input.source_url,
      maximumPages: MAX_WEBSITE_REVISION_PAGES,
      resolveHostname: dependencies.resolveHostname,
    });
  } catch (error) {
    throw new AgentMaterialIngestionError(
      "invalid_source_url",
      error instanceof Error
        ? error.message
        : "The source URL could not be imported.",
    );
  }
  try {
    const source = await validatePublicHttpsUrl(
      discovery.sourceUrl,
      dependencies.resolveHostname,
    );
    if (discovery.pages.length > MAX_WEBSITE_REVISION_PAGES) {
      throw new Error(
        "The textbook exposed too many pages to import in one material.",
      );
    }
    const selectedUrls = await validateSameOriginWebsiteUrls(
      source,
      discovery.pages.map((page) => page.url),
      dependencies.resolveHostname,
    );
    if (selectedUrls.length === 0) {
      throw new Error(
        "The source URL did not expose readable textbook pages. Upload its PDF or choose a page-based textbook URL.",
      );
    }
    return {
      title: input.title ?? truncateMaterialTitle(discovery.title),
      sourceUrl: source.toString(),
      selectedUrls,
    };
  } catch (error) {
    throw new AgentMaterialIngestionError(
      "invalid_source_url",
      error instanceof Error
        ? error.message
        : "The source URL could not be imported.",
    );
  }
}

async function validateSameOriginWebsiteUrls(
  source: URL,
  values: string[],
  resolveHostname: ResolveHostname | undefined,
) {
  const selectedUrls: string[] = [];
  for (const value of values) {
    const candidate = await validatePublicHttpsUrl(value, resolveHostname);
    if (candidate.origin !== source.origin) {
      throw new Error(
        "Website pages must come from the same public textbook site.",
      );
    }
    selectedUrls.push(candidate.toString());
  }
  return [...new Set(selectedUrls)];
}

async function findPreparedMaterialOperation(
  tx: MaterialOperationDatabase,
  auth: AgentAuthContext,
  input: AgentCompleteMaterialUploadInput,
) {
  if (input.operation_id) {
    return tx.agentSkillOperation.findFirst({
      where: {
        id: input.operation_id,
        userId: auth.userId,
        connectionId: auth.connectionId,
        toolName: MATERIAL_PREPARE_TOOL,
      },
      select: MATERIAL_OPERATION_SELECT,
    });
  }
  return tx.agentSkillOperation.findFirst({
    where: {
      userId: auth.userId,
      connectionId: auth.connectionId,
      toolName: MATERIAL_PREPARE_TOOL,
      materialRevisionId: input.material_revision_id,
      ...(input.source_file_id ? { sourceFileId: input.source_file_id } : {}),
    },
    orderBy: { createdAt: "desc" },
    select: MATERIAL_OPERATION_SELECT,
  });
}

async function findMaterialCompletionReplay(
  auth: AgentAuthContext,
  input: AgentCompleteMaterialUploadInput,
  payloadHash: string,
) {
  const prisma = getPrisma();
  const replay = await prisma.agentOperationAction.findUnique({
    where: {
      connectionId_toolName_idempotencyKey: {
        connectionId: auth.connectionId,
        toolName: MATERIAL_COMPLETE_TOOL,
        idempotencyKey: input.idempotency_key,
      },
    },
    select: { operationId: true, userId: true, payloadHash: true },
  });
  if (!replay) return null;
  if (replay.userId !== auth.userId) {
    throw new AgentMaterialIngestionError(
      "idempotency_conflict",
      "That idempotency key was already used for a different material operation.",
    );
  }
  assertPayloadHash(replay.payloadHash, payloadHash);
  return refreshMaterialOperation(prisma, replay.operationId);
}

async function readPreparedMaterialUploadSnapshot(
  tx: MaterialOperationDatabase,
  auth: AgentAuthContext,
  input: AgentCompleteMaterialUploadInput,
  operation: MaterialOperation,
): Promise<PreparedMaterialUploadSnapshot> {
  const materialId = operation.materialRevision?.materialId;
  const sourceFileId = operation.sourceFileId;
  if (!materialId || !sourceFileId) {
    throw new AgentMaterialIngestionError(
      "stale_material_revision",
      "The prepared material upload is missing its ownership record.",
    );
  }
  const material = await tx.studyMaterial.findFirst({
    where: {
      id: materialId,
      userId: auth.userId,
      kind: StudyMaterialKind.PDF,
      status: StudyMaterialStatus.ACTIVE,
    },
    select: { id: true },
  });
  const revision = await tx.materialRevision.findFirst({
    where: {
      id: input.material_revision_id,
      userId: auth.userId,
      materialId,
    },
    select: {
      id: true,
      materialId: true,
      status: true,
      processingMetadata: true,
      sourceFiles: {
        where: {
          id: sourceFileId,
          userId: auth.userId,
          kind: SourceFileKind.PDF,
        },
        take: 1,
        select: {
          id: true,
          status: true,
          storageBucket: true,
          storageKey: true,
          mimeType: true,
          byteSize: true,
        },
      },
    },
  });
  const sourceFile = revision?.sourceFiles[0];
  if (!material || !revision || !sourceFile) {
    throw new AgentMaterialIngestionError(
      "stale_material_revision",
      "The prepared material upload is no longer available.",
    );
  }
  return { revision, sourceFile };
}

async function verifyPreparedMaterialUpload(
  storage: SourceObjectStorage,
  sourceFile: PreparedMaterialUploadSnapshot["sourceFile"],
) {
  if (
    sourceFile.status !== SourceFileStatus.DRAFT ||
    !sourceFile.storageBucket ||
    !sourceFile.storageKey ||
    sourceFile.mimeType !== "application/pdf" ||
    sourceFile.byteSize === null
  ) {
    throw new AgentMaterialIngestionError(
      "upload_not_available",
      "The prepared PDF upload is no longer available. Prepare a new material upload.",
    );
  }
  let head: Awaited<ReturnType<SourceObjectStorage["headObject"]>>;
  try {
    head = await storage.headObject({
      key: sourceFile.storageKey,
      bucket: sourceFile.storageBucket,
    });
  } catch {
    throw new AgentMaterialIngestionError(
      "upload_not_available",
      "The private PDF upload could not be verified. Try uploading it again.",
      true,
    );
  }
  if (
    head.byteSize !== sourceFile.byteSize ||
    head.mimeType?.split(";")[0]?.trim().toLowerCase() !== "application/pdf"
  ) {
    throw new AgentMaterialIngestionError(
      "upload_not_available",
      "The uploaded PDF did not match the prepared file. Upload it again or prepare a new material.",
    );
  }
}

function samePreparedMaterialUploadSnapshot(
  left: PreparedMaterialUploadSnapshot,
  right: PreparedMaterialUploadSnapshot,
) {
  return (
    left.revision.id === right.revision.id &&
    left.revision.materialId === right.revision.materialId &&
    left.revision.status === right.revision.status &&
    left.sourceFile.id === right.sourceFile.id &&
    left.sourceFile.status === right.sourceFile.status &&
    left.sourceFile.storageBucket === right.sourceFile.storageBucket &&
    left.sourceFile.storageKey === right.sourceFile.storageKey &&
    left.sourceFile.byteSize === right.sourceFile.byteSize &&
    left.sourceFile.mimeType === right.sourceFile.mimeType
  );
}

async function readPreparedUploadLeaseSnapshot(
  tx: Pick<Prisma.TransactionClient, "sourceFile">,
  auth: AgentAuthContext,
  operation: MaterialOperation,
): Promise<PreparedUploadLeaseSnapshot> {
  const materialRevisionId = operation.materialRevisionId;
  const sourceFileId = operation.sourceFileId;
  if (!materialRevisionId || !sourceFileId) {
    throw new AgentMaterialIngestionError(
      "stale_material_revision",
      "The prepared material upload is missing its ownership record.",
    );
  }
  const sourceFile = await tx.sourceFile.findFirst({
    where: {
      id: sourceFileId,
      userId: auth.userId,
      materialRevisionId,
      status: SourceFileStatus.DRAFT,
      materialRevision: {
        id: materialRevisionId,
        status: MaterialRevisionStatus.PENDING_UPLOAD,
        material: {
          status: StudyMaterialStatus.ACTIVE,
          kind: StudyMaterialKind.PDF,
        },
      },
    },
    select: {
      id: true,
      status: true,
      storageBucket: true,
      storageKey: true,
      mimeType: true,
      byteSize: true,
      presignedUploadExpiresAt: true,
      materialRevision: {
        select: { id: true, materialId: true, status: true },
      },
    },
  });
  if (!sourceFile?.materialRevision) {
    throw new AgentMaterialIngestionError(
      "stale_material_revision",
      "The prepared material upload is no longer available. Prepare a new material upload.",
    );
  }
  return {
    revision: sourceFile.materialRevision,
    sourceFile: {
      id: sourceFile.id,
      status: sourceFile.status,
      storageBucket: sourceFile.storageBucket,
      storageKey: sourceFile.storageKey,
      mimeType: sourceFile.mimeType,
      byteSize: sourceFile.byteSize,
      presignedUploadExpiresAt: sourceFile.presignedUploadExpiresAt,
    },
  };
}

function samePreparedUploadLeaseSnapshot(
  left: PreparedUploadLeaseSnapshot,
  right: PreparedUploadLeaseSnapshot,
) {
  return samePreparedMaterialUploadSnapshot(left, right);
}

function assertMaterialOperationIdentifiers(
  operation: MaterialOperation,
  input: AgentCompleteMaterialUploadInput,
) {
  if (
    operation.materialRevisionId !== input.material_revision_id ||
    (input.material_id &&
      operation.materialRevision?.materialId !== input.material_id) ||
    (input.source_file_id && operation.sourceFileId !== input.source_file_id)
  ) {
    throw new AgentMaterialIngestionError(
      "stale_material_revision",
      "The prepared upload identifiers do not match the saved material operation.",
    );
  }
}

async function recordMaterialAction(
  tx: Prisma.TransactionClient,
  auth: AgentAuthContext,
  operationId: string,
  toolName: string,
  idempotencyKey: string,
  payloadHash: string,
) {
  await tx.agentOperationAction.create({
    data: {
      userId: auth.userId,
      connectionId: auth.connectionId,
      operationId,
      toolName,
      idempotencyKey,
      payloadHash,
    },
  });
}

async function refreshMaterialOperation(
  tx: Pick<Prisma.TransactionClient, "agentSkillOperation">,
  operationId: string,
) {
  return tx.agentSkillOperation.findUniqueOrThrow({
    where: { id: operationId },
    select: MATERIAL_OPERATION_SELECT,
  });
}

async function assertOwnedCollection(
  tx: Prisma.TransactionClient,
  userId: string,
  collectionId: string | null | undefined,
) {
  if (!collectionId) return;
  const collection = await tx.collection.findFirst({
    where: { id: collectionId, userId },
    select: { id: true },
  });
  if (!collection) {
    throw new AgentMaterialIngestionError(
      "permission_denied",
      "The selected collection is not owned by this account.",
    );
  }
}

function requireSourceUploadScope(auth: AgentAuthContext) {
  if (!auth.scopes.includes("sources:upload")) {
    throw new AgentMaterialIngestionError(
      "permission_denied",
      "Agent permission sources:upload is required for material ingestion.",
    );
  }
}

async function authorizeMaterialOperationRead(auth: AgentAuthContext) {
  const eligibleScopes = (["materials:read", "sources:upload"] as const).filter(
    (scope) => auth.scopes.includes(scope),
  );
  if (eligibleScopes.length === 0) {
    throw new AgentMaterialIngestionError(
      "permission_denied",
      "Agent permission materials:read or sources:upload is required for material operation status.",
    );
  }

  let lastPermissionError: AgentOperationError | null = null;
  for (const scope of eligibleScopes) {
    try {
      await authorizeAgentRead(auth, scope);
      return;
    } catch (error) {
      if (
        !(error instanceof AgentOperationError) ||
        error.code !== "permission_denied"
      ) {
        throw error;
      }
      lastPermissionError = error;
    }
  }
  throw (
    lastPermissionError ??
    new AgentMaterialIngestionError(
      "permission_denied",
      "An active material permission is required for operation status.",
    )
  );
}

function assertPayloadHash(actual: string, expected: string) {
  if (actual !== expected) {
    throw new AgentMaterialIngestionError(
      "idempotency_conflict",
      "That idempotency key was already used with different input.",
    );
  }
}

function resolveMaterialStorage(storage?: SourceObjectStorage) {
  if (storage) return { status: "ready" as const, storage };
  return resolveS3SourceObjectStorage();
}

function toJson(value: unknown): Prisma.InputJsonValue {
  return JSON.parse(JSON.stringify(value)) as Prisma.InputJsonValue;
}

function truncate(value: string | null, maximum: number) {
  if (!value || value.length <= maximum) return value;
  return `${value.slice(0, maximum - 1)}…`;
}

async function runMaterialMutation<T>(
  auth: AgentAuthContext,
  work: (tx: Prisma.TransactionClient) => Promise<T>,
): Promise<T> {
  for (let attempt = 0; attempt < 8; attempt += 1) {
    try {
      return await withAgentMutation(auth, "sources:upload", work);
    } catch (error) {
      const retryableConflict =
        error instanceof Prisma.PrismaClientKnownRequestError &&
        (error.code === "P2002" || error.code === "P2034");
      if (!retryableConflict || attempt === 7) throw error;
      await new Promise((resolve) => setTimeout(resolve, (attempt + 1) * 10));
    }
  }
  throw new AgentMaterialIngestionError(
    "rate_limited",
    "The request could not obtain a database reservation.",
    true,
  );
}
