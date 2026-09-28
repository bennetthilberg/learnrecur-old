import "server-only";

import {
  AgentCandidateStatus,
  AgentConnectionStatus,
  AgentOperationItemStatus,
  AgentOperationKind,
  AgentOperationStatus,
  AgentRateLimitKind,
  MaterialRevisionStatus,
  Prisma,
  SourceFileKind,
  SourceFileStatus,
  SkillDraftBatchItemStatus,
  StudyMaterialStatus,
} from "@/generated/prisma/client";
import type { AgentAuthContext } from "@/lib/agent-access/auth";
import { runAgentSerializable as runAgentDatabaseTransaction } from "@/lib/agent-access/transactions";
import {
  AGENT_OPERATION_POLL_AFTER_MS,
  agentAddFromMaterialSchema,
  agentAddFromTextSchema,
  agentContinueOperationSchema,
  agentPrepareFilesSchema,
  agentRetryOperationSchema,
  agentStartFilesSchema,
  buildAgentPayloadHash,
  normalizeAgentCandidateExercise,
} from "@/lib/agent-access/contracts";
import { MAX_IMPORT_BATCH_ITEMS } from "@/lib/import-limits";
import { getPrisma } from "@/lib/prisma";
import { sendAgentSkillOperationRequested } from "@/lib/jobs/events";
import { getPendingImportUsage } from "@/lib/usage-limits";
import {
  MaterialSourceReferenceError,
  parseAgentSpecInputWithSourceRefs,
  resolveMaterialSourceReferences,
  type MaterialSourceReferenceCache,
} from "@/lib/materials/source-references";
import {
  prepareSourceUpload,
  refreshPreparedSourceUpload,
} from "@/lib/skills/uploads";

export { reduceAgentOperationStatus } from "./status";

const PUBLIC_OPERATION_SELECT = {
  id: true,
  status: true,
  requestedCount: true,
  activeCount: true,
  reusedCount: true,
  failedCount: true,
  errorCode: true,
  createdAt: true,
  updatedAt: true,
  completedAt: true,
  items: {
    orderBy: { ordinal: "asc" as const },
    select: {
      id: true,
      clientReference: true,
      status: true,
      proposedTitle: true,
      duplicateConfidence: true,
      sourceReferenceOutcome: true,
      resultSkillId: true,
      errorCode: true,
      retryCount: true,
    },
  },
} satisfies Prisma.AgentSkillOperationSelect;

export type PublicAgentOperation = {
  operation_id: string;
  operation_uri: string;
  status: string;
  requested_count: number;
  active_count: number;
  reused_count: number;
  failed_count: number;
  error_code: string | null;
  created_at: string;
  updated_at: string;
  completed_at: string | null;
  poll_after_ms?: number;
  items: Array<{
    item_id: string;
    client_reference: string;
    status: string;
    progress_state: AgentItemProgressState;
    proposed_title: string | null;
    duplicate_confidence: string | null;
    source_reference_outcome: Prisma.JsonValue | null;
    skill_id: string | null;
    skill_url: string | null;
    error_code: string | null;
    retry_count: number;
  }>;
};

export type AgentItemProgressState = "working" | "waiting_for_retry" | "needs_action" | "ready" | "stopped";

export function getAgentItemProgressState(item: {
  status: AgentOperationItemStatus;
  errorCode: string | null;
}): AgentItemProgressState {
  if (item.status === AgentOperationItemStatus.ACTIVE || item.status === AgentOperationItemStatus.REUSED) {
    return "ready";
  }
  if (item.status === AgentOperationItemStatus.CANCELED) return "stopped";
  if (
    item.status === AgentOperationItemStatus.FAILED ||
    item.status === AgentOperationItemStatus.NEEDS_INPUT ||
    item.status === AgentOperationItemStatus.NEEDS_REVIEW
  ) return "needs_action";
  if (
    item.status === AgentOperationItemStatus.QUEUED &&
    (item.errorCode === "TRANSIENT_WORKER_FAILURE" || item.errorCode === "STALE_WORKER_RECOVERY")
  ) return "waiting_for_retry";
  return "working";
}

export class AgentOperationError extends Error {
  constructor(
    readonly code:
      | "idempotency_conflict"
      | "permission_denied"
      | "settings_not_found"
      | "invalid_stored_settings"
      | "collection_too_large"
      | "rate_limited"
      | "too_many_pending_items"
      | "material_not_found"
      | "stale_material_revision"
      | "operation_not_found"
      | "operation_not_ready"
      | "operation_not_retryable"
      | "upload_preparation_failed"
      | "invalid_input"
      | "invalid_cursor"
      | "skill_not_found"
      | "skill_not_active"
      | "collection_not_found"
      | "stale_state"
      | "meaning_change_requires_reset"
      | "setup_not_found"
      | "setup_stale"
      | "setup_in_progress"
      | "setup_failed"
      | "exercise_not_found",
    message: string,
  ) {
    super(message);
    this.name = "AgentOperationError";
  }
}

export async function createAgentSpecOperation(
  auth: AgentAuthContext,
  rawInput: unknown,
): Promise<PublicAgentOperation> {
  const input = parseAgentSpecInputWithSourceRefs(rawInput);
  const payloadHash = buildAgentPayloadHash(input);
  const claim = await runAgentSerializable(
    async (tx) => {
      await assertActiveConnectionAndConsumeMutation(tx, auth);
      const replay = await findReplay(tx, auth, "skills.add_from_specs", input.idempotency_key, payloadHash);
      if (replay) return { operation: replay, created: false as const };
      await assertPendingItemLimit(tx, auth.userId, input.items.length);
      const sourceReferenceCache: MaterialSourceReferenceCache = new Map();
      for (const item of input.items) {
        if (item.source_refs?.length) {
          try {
            await resolveMaterialSourceReferences({
              userId: auth.userId,
              sourceRefs: item.source_refs,
              client: tx,
              cache: sourceReferenceCache,
            });
          } catch (error) {
            throw sourceReferenceOperationError(error);
          }
        }
      }
      const operation = await tx.agentSkillOperation.create({
        data: {
          userId: auth.userId,
          connectionId: auth.connectionId,
          kind: AgentOperationKind.SPEC_BATCH,
          toolName: "skills.add_from_specs",
          idempotencyKey: input.idempotency_key,
          payloadHash,
          requestPayload: toJson({
            items: input.items.map((item) => ({
              client_reference: item.client_reference,
              skill: item.skill,
              ...(item.source_refs ? { source_refs: item.source_refs } : {}),
            })),
          }),
          payloadExpiresAt: daysFromNow(30),
          requestedCount: input.items.length,
          items: {
            create: input.items.map((item, ordinal) => ({
              ordinal,
              clientReference: item.client_reference,
              proposedTitle: item.skill.title,
              proposedObjective: item.skill.objective,
              skillSnapshot: toJson({
                ...item.skill,
                ...(item.source_refs ? { source_refs: item.source_refs } : {}),
              }),
              candidateFingerprint: buildAgentPayloadHash(item.skill),
              candidates: item.candidate_exercises
                ? {
                    create: item.candidate_exercises.map((candidate, index) => {
                      const normalized = normalizeAgentCandidateExercise(candidate, index);
                      return {
                        ordinal: index,
                        clientReference: normalized.clientReference,
                        kind: normalized.answerKind,
                        normalizedPayload: toJson(normalized),
                        status: AgentCandidateStatus.VALIDATED,
                      };
                    }),
                  }
                : undefined,
            })),
          },
        },
        select: PUBLIC_OPERATION_SELECT,
      });
      return { operation, created: true as const };
    },
  );
  if (claim.created) await enqueueOperation(auth.userId, claim.operation.id);
  return serializeAgentOperation(claim.operation);
}

export async function createAgentTextOperation(
  auth: AgentAuthContext,
  rawInput: unknown,
): Promise<PublicAgentOperation> {
  const input = agentAddFromTextSchema.parse(rawInput);
  const payloadHash = buildAgentPayloadHash(input);
  const claim = await runAgentSerializable(
    async (tx) => {
      await assertActiveConnectionAndConsumeMutation(tx, auth);
      const replay = await findReplay(tx, auth, "skills.add_from_text", input.idempotency_key, payloadHash);
      if (replay) return { operation: replay, created: false as const };
      await assertPendingItemLimit(tx, auth.userId, 1);
      const source = await tx.sourceFile.create({
        data: {
          userId: auth.userId,
          kind: SourceFileKind.TEXT,
          status: SourceFileStatus.READY,
          originalName: input.source_label ?? "Agent pasted text",
          mimeType: "text/plain",
          byteSize: Buffer.byteLength(input.source_text, "utf8"),
          extractedText: input.source_text,
          metadata: toJson({ source: "agent", connectionId: auth.connectionId }),
        },
      });
      const operation = await tx.agentSkillOperation.create({
        data: {
          userId: auth.userId,
          connectionId: auth.connectionId,
          kind: AgentOperationKind.TEXT_SOURCE,
          toolName: "skills.add_from_text",
          idempotencyKey: input.idempotency_key,
          payloadHash,
          sourceFileId: source.id,
          requestPayload: toJson({
            intent: input.intent,
            collection: input.collection,
            tags: input.tags,
          }),
          payloadExpiresAt: daysFromNow(30),
          requestedCount: 1,
          items: {
            create: {
              ordinal: 0,
              clientReference: "text-1",
              candidates: input.candidate_exercises
                ? {
                    create: input.candidate_exercises.map((candidate, index) => {
                      const normalized = normalizeAgentCandidateExercise(candidate, index);
                      return {
                        ordinal: index,
                        clientReference: normalized.clientReference,
                        kind: normalized.answerKind,
                        normalizedPayload: toJson(normalized),
                        status: AgentCandidateStatus.VALIDATED,
                      };
                    }),
                  }
                : undefined,
            },
          },
        },
        select: PUBLIC_OPERATION_SELECT,
      });
      return { operation, created: true as const };
    },
  );
  if (claim.created) await enqueueOperation(auth.userId, claim.operation.id);
  return serializeAgentOperation(claim.operation);
}

export async function createAgentMaterialOperation(
  auth: AgentAuthContext,
  rawInput: unknown,
): Promise<PublicAgentOperation> {
  const input = agentAddFromMaterialSchema.parse(rawInput);
  const payloadHash = buildAgentPayloadHash(input);
  const claim = await runAgentSerializable(
    async (tx) => {
      await assertActiveConnectionAndConsumeMutation(tx, auth);
      const replay = await findReplay(tx, auth, "skills.add_from_material", input.idempotency_key, payloadHash);
      if (replay) return { operation: replay, created: false as const };
      await assertPendingItemLimit(tx, auth.userId, input.max_skills);
      const material = await tx.studyMaterial.findFirst({
        where: { id: input.material_id, userId: auth.userId, status: StudyMaterialStatus.ACTIVE },
        select: { activeRevisionId: true },
      });
      if (!material) throw new AgentOperationError("material_not_found", "The material was not found.");
      if (material.activeRevisionId !== input.expected_revision_id) {
        throw new AgentOperationError("stale_material_revision", "The material revision changed. Refresh its outline and retry.");
      }
      const revision = await tx.materialRevision.findFirst({
        where: { id: input.expected_revision_id, userId: auth.userId, status: MaterialRevisionStatus.READY },
        select: { id: true },
      });
      if (!revision) throw new AgentOperationError("stale_material_revision", "The requested revision is not ready.");
      if (input.section_ids?.length) {
        const sectionCount = await tx.materialSection.count({
          where: {
            id: { in: input.section_ids },
            userId: auth.userId,
            materialRevisionId: revision.id,
          },
        });
        if (sectionCount !== input.section_ids.length) {
          throw new AgentOperationError("material_not_found", "One or more requested material sections were not found.");
        }
      }
      const operation = await tx.agentSkillOperation.create({
        data: {
          userId: auth.userId,
          connectionId: auth.connectionId,
          kind: AgentOperationKind.MATERIAL_BATCH,
          toolName: "skills.add_from_material",
          status: AgentOperationStatus.PLANNING,
          idempotencyKey: input.idempotency_key,
          payloadHash,
          materialRevisionId: revision.id,
          requestPayload: toJson({
            instruction: input.instruction,
            sectionIds: input.section_ids,
            maxSkills: input.max_skills,
          }),
          payloadExpiresAt: daysFromNow(30),
          requestedCount: input.max_skills,
        },
        select: PUBLIC_OPERATION_SELECT,
      });
      return { operation, created: true as const };
    },
  );
  if (claim.created) await enqueueOperation(auth.userId, claim.operation.id);
  return serializeAgentOperation(claim.operation);
}

export type PreparedAgentFileOperation = PublicAgentOperation & {
  uploads: Array<{
    source_file_id: string;
    upload_url: string;
    headers: Record<string, string>;
    expires_in_seconds: number;
  }>;
};

export async function prepareAgentFileOperation(
  auth: AgentAuthContext,
  rawInput: unknown,
): Promise<PreparedAgentFileOperation> {
  const input = agentPrepareFilesSchema.parse(rawInput);
  const payloadHash = buildAgentPayloadHash(input);
  const prisma = getPrisma();
  const preparedClaim = await runAgentSerializable(
    async (tx) => {
      await assertActiveConnectionAndConsumeMutation(tx, auth);
      const replay = await findReplay(
        tx,
        auth,
        "skills.prepare_files",
        input.idempotency_key,
        payloadHash,
      );
      if (replay) return { operation: replay, created: false as const };
      await assertPendingItemLimit(tx, auth.userId, 1);
      const operation = await tx.agentSkillOperation.create({
        data: {
          userId: auth.userId,
          connectionId: auth.connectionId,
          kind: AgentOperationKind.QUICK_FILES,
          toolName: "skills.prepare_files",
          status: AgentOperationStatus.AWAITING_UPLOAD,
          idempotencyKey: input.idempotency_key,
          payloadHash,
          requestPayload: toJson({
            intent: input.intent,
            sourceLabel: input.source_label,
            collection: input.collection,
            tags: input.tags,
          }),
          payloadExpiresAt: daysFromNow(30),
          requestedCount: 1,
          items: {
            create: {
              ordinal: 0,
              clientReference: "files-1",
              candidates: input.candidate_exercises
                ? {
                    create: input.candidate_exercises.map((candidate, index) => {
                      const normalized = normalizeAgentCandidateExercise(candidate, index);
                      return {
                        ordinal: index,
                        clientReference: normalized.clientReference,
                        kind: normalized.answerKind,
                        normalizedPayload: toJson(normalized),
                        status: AgentCandidateStatus.VALIDATED,
                      };
                    }),
                  }
                : undefined,
            },
          },
        },
        select: PUBLIC_OPERATION_SELECT,
      });
      return { operation, created: true as const };
    },
  );
  const operation = preparedClaim.operation;
  if (!preparedClaim.created) return buildPreparedFileResponse(auth, operation);

  const preparedUploads: Array<
    Extract<Awaited<ReturnType<typeof prepareSourceUpload>>, { status: "prepared" }>
  > = [];
  for (const file of input.files) {
    const prepared = await prepareSourceUpload({
      userId: auth.userId,
      now: new Date(),
      input: {
        originalName: file.name,
        mimeType: file.media_type,
        byteSize: file.size_bytes,
        sourceLabel: input.source_label,
        focusNote: input.intent,
        collectionName: input.collection,
        tags: input.tags,
      },
    });
    if (prepared.status !== "prepared") {
      if (preparedUploads.length > 0) {
        await prisma.sourceFile.deleteMany({
          where: {
            userId: auth.userId,
            id: { in: preparedUploads.map((upload) => upload.sourceFileId) },
            status: SourceFileStatus.DRAFT,
          },
        });
      }
      const failedAt = new Date();
      await prisma.$transaction([
        prisma.agentSkillOperation.update({
          where: { id: operation.id },
          data: {
            status: AgentOperationStatus.FAILED,
            failedCount: 1,
            errorCode: "UPLOAD_PREPARATION_FAILED",
            errorMessage: prepared.message,
            completedAt: failedAt,
          },
        }),
        prisma.agentSkillOperationItem.updateMany({
          where: { operationId: operation.id, userId: auth.userId },
          data: {
            status: AgentOperationItemStatus.FAILED,
            errorCode: "UPLOAD_PREPARATION_FAILED",
            completedAt: failedAt,
          },
        }),
      ]);
      throw new AgentOperationError("upload_preparation_failed", "The private upload could not be prepared.");
    }
    preparedUploads.push(prepared);
  }

  await prisma.agentSkillOperation.update({
    where: { id: operation.id },
    data: {
      sources: {
        create: preparedUploads.map((prepared, ordinal) => ({
          sourceFileId: prepared.sourceFileId,
          ordinal,
        })),
      },
    },
  });
  return {
    ...serializeAgentOperation(operation),
    uploads: preparedUploads.map((prepared) => ({
      source_file_id: prepared.sourceFileId,
      upload_url: prepared.uploadUrl,
      headers: prepared.headers,
      expires_in_seconds: prepared.expiresInSeconds,
    })),
  };
}

export async function startAgentFileOperation(
  auth: AgentAuthContext,
  rawInput: unknown,
): Promise<PublicAgentOperation> {
  const input = agentStartFilesSchema.parse(rawInput);
  const payloadHash = buildAgentPayloadHash(input);
  const claim = await mutateOperationWithAction({
    auth,
    operationId: input.operation_id,
    toolName: "skills.start_files",
    idempotencyKey: input.idempotency_key,
    payloadHash,
    mutate: async (tx, current) => {
      if (current.kind !== AgentOperationKind.QUICK_FILES) {
        throw new AgentOperationError("operation_not_ready", "This is not a prepared file operation.");
      }
      if (current.status === AgentOperationStatus.AWAITING_UPLOAD) {
        await tx.agentSkillOperation.update({
          where: { id: current.id },
          data: { status: AgentOperationStatus.QUEUED, errorCode: null, errorMessage: null },
        });
      }
    },
  });
  if (claim.mutated) await enqueueOperation(auth.userId, claim.operation.operation_id);
  return claim.operation;
}

export async function continueAgentOperation(
  auth: AgentAuthContext,
  rawInput: unknown,
): Promise<PublicAgentOperation> {
  const input = agentContinueOperationSchema.parse(rawInput);
  const payloadHash = buildAgentPayloadHash(input);
  const claim = await mutateOperationWithAction({
    auth,
    operationId: input.operation_id,
    toolName: "operations.continue",
    idempotencyKey: input.idempotency_key,
    payloadHash,
    mutate: async (tx, current) => {
      if (
        current.kind !== AgentOperationKind.MATERIAL_BATCH ||
        current.status !== AgentOperationStatus.NEEDS_INPUT
      ) {
        throw new AgentOperationError("operation_not_ready", "This operation is not awaiting clarification.");
      }
      const payload = parseStoredRecord(current.requestPayload);
      await tx.agentSkillOperation.update({
        where: { id: current.id },
        data: {
          status: AgentOperationStatus.PLANNING,
          requestPayload: toJson({ ...payload, clarification: input.instruction }),
          errorCode: null,
          errorMessage: null,
        },
      });
    },
  });
  if (claim.mutated) await enqueueOperation(auth.userId, claim.operation.operation_id);
  return claim.operation;
}

export async function retryFailedAgentOperationItems(
  auth: AgentAuthContext,
  rawInput: unknown,
): Promise<PublicAgentOperation> {
  const input = agentRetryOperationSchema.parse(rawInput);
  const payloadHash = buildAgentPayloadHash(input);
  const claim = await mutateOperationWithAction({
    auth,
    operationId: input.operation_id,
    toolName: "operations.retry_failed",
    idempotencyKey: input.idempotency_key,
    payloadHash,
    mutate: async (tx, current) => {
      const failedItems = await tx.agentSkillOperationItem.findMany({
        where: {
          operationId: current.id,
          userId: auth.userId,
          status: AgentOperationItemStatus.FAILED,
          ...(input.item_ids ? { id: { in: input.item_ids } } : {}),
        },
        select: { id: true, ordinal: true, retryCount: true, errorCode: true },
        take: MAX_IMPORT_BATCH_ITEMS,
      });
      const failed = failedItems.filter((item) => isRetryableAgentItemError(item.errorCode));
      if (failed.length === 0) {
        throw new AgentOperationError("operation_not_retryable", "No requested failed items can be retried.");
      }
      await tx.agentSkillOperationItem.updateMany({
        where: { id: { in: failed.map((item) => item.id) }, userId: auth.userId },
        data: {
          status: AgentOperationItemStatus.QUEUED,
          retryCount: { increment: 1 },
          errorCode: null,
          errorMessage: null,
          activationReservedAt: null,
          completedAt: null,
        },
      });
      if (current.kind === AgentOperationKind.MATERIAL_BATCH) {
        const materialBatchId = parseStoredRecord(current.requestPayload).materialBatchId;
        if (typeof materialBatchId === "string") {
          await tx.skillDraftBatchItem.updateMany({
            where: {
              batchId: materialBatchId,
              userId: auth.userId,
              ordinal: { in: failed.map((item) => item.ordinal) },
              status: SkillDraftBatchItemStatus.FAILED,
            },
            data: {
              status: SkillDraftBatchItemStatus.PLANNED,
              errorCode: null,
              errorMessage: null,
              generationClaimId: null,
            },
          });
        }
      }
      const remainingFailed = await tx.agentSkillOperationItem.count({
        where: {
          operationId: current.id,
          userId: auth.userId,
          status: AgentOperationItemStatus.FAILED,
        },
      });
      await tx.agentSkillOperation.update({
        where: { id: current.id },
        data: {
          status: AgentOperationStatus.QUEUED,
          failedCount: remainingFailed,
          errorCode: null,
          errorMessage: null,
          completedAt: null,
        },
      });
    },
  });
  if (claim.mutated) await enqueueOperation(auth.userId, claim.operation.operation_id);
  return claim.operation;
}

export async function getAgentOperation(
  auth: AgentAuthContext,
  operationId: string,
): Promise<PublicAgentOperation> {
  if (!auth.scopes.includes("skills:create")) {
    throw new AgentOperationError(
      "permission_denied",
      "Agent permission skills:create is required for skill operation status.",
    );
  }
  const prisma = getPrisma();
  await consumeAgentReadRateLimit(auth);
  const operation = await prisma.agentSkillOperation.findFirst({
    where: { id: operationId, userId: auth.userId, connectionId: auth.connectionId },
    select: PUBLIC_OPERATION_SELECT,
  });
  if (!operation) throw new AgentOperationError("operation_not_found", "The operation was not found.");
  return serializeAgentOperation(operation);
}

export async function consumeAgentReadRateLimit(auth: AgentAuthContext) {
  await runAgentSerializable(async (tx) => {
    const connection = await tx.agentConnection.findFirst({
      where: {
        id: auth.connectionId,
        userId: auth.userId,
        status: AgentConnectionStatus.ACTIVE,
        workosSubject: auth.subject,
        workosSessionId: auth.sessionId,
        clientId: auth.clientId,
        resourceUrl: auth.resourceUrl,
        user: { agentAccessDisabledAt: null },
      },
      select: { id: true, permissionVersion: true },
    });
    const deleting = await tx.accountDeletionJob.findUnique({
      where: { userId: auth.userId },
      select: { id: true },
    });
    if (
      !connection ||
      deleting ||
      auth.expiresAt <= Date.now() / 1_000 ||
      (auth.permissionVersion !== undefined && connection.permissionVersion !== auth.permissionVersion)
    ) {
      throw new AgentOperationError("permission_denied", "The agent connection is not active.");
    }
    await consumeRateLimit(tx, auth, AgentRateLimitKind.READ, 60);
  });
}

export function serializeAgentOperation(
  operation: Prisma.AgentSkillOperationGetPayload<{ select: typeof PUBLIC_OPERATION_SELECT }>,
): PublicAgentOperation {
  const terminal =
    operation.status === AgentOperationStatus.SUCCEEDED ||
    operation.status === AgentOperationStatus.PARTIAL ||
    operation.status === AgentOperationStatus.FAILED ||
    operation.status === AgentOperationStatus.CANCELED;
  return {
    operation_id: operation.id,
    operation_uri: `learnrecur://operations/${operation.id}`,
    status: operation.status.toLocaleLowerCase("en-US"),
    requested_count: operation.requestedCount,
    active_count: operation.activeCount,
    reused_count: operation.reusedCount,
    failed_count: operation.failedCount,
    error_code: operation.errorCode,
    created_at: operation.createdAt.toISOString(),
    updated_at: operation.updatedAt.toISOString(),
    completed_at: operation.completedAt?.toISOString() ?? null,
    ...(terminal ? {} : { poll_after_ms: AGENT_OPERATION_POLL_AFTER_MS }),
    items: operation.items.map((item) => ({
      item_id: item.id,
      client_reference: item.clientReference,
      status: item.status.toLocaleLowerCase("en-US"),
      progress_state: getAgentItemProgressState(item),
      proposed_title: item.proposedTitle,
      duplicate_confidence: item.duplicateConfidence,
      source_reference_outcome: item.sourceReferenceOutcome,
      skill_id: item.resultSkillId,
      skill_url: item.resultSkillId ? `/skills/${item.resultSkillId}` : null,
      error_code: item.errorCode,
      retry_count: item.retryCount,
    })),
  };
}

function sourceReferenceOperationError(error: unknown) {
  if (error instanceof MaterialSourceReferenceError) {
    if (error.code === "material_not_found") {
      return new AgentOperationError("material_not_found", error.message);
    }
    if (error.code === "stale_material_revision") {
      return new AgentOperationError("stale_material_revision", error.message);
    }
    return new AgentOperationError("invalid_input", error.message);
  }
  return new AgentOperationError("invalid_input", "The supplied source reference could not be validated.");
}

async function buildPreparedFileResponse(
  auth: AgentAuthContext,
  operation: Prisma.AgentSkillOperationGetPayload<{ select: typeof PUBLIC_OPERATION_SELECT }>,
): Promise<PreparedAgentFileOperation> {
  const sources = await getPrisma().agentOperationSource.findMany({
    where: { operationId: operation.id, userId: auth.userId },
    orderBy: { ordinal: "asc" },
    select: { sourceFileId: true },
  });
  const uploads = [];
  for (const source of sources) {
    const prepared = await refreshPreparedSourceUpload({
      userId: auth.userId,
      sourceFileId: source.sourceFileId,
    });
    if (prepared.status !== "prepared") {
      throw new AgentOperationError("operation_not_ready", "The prepared upload is no longer available.");
    }
    uploads.push({
      source_file_id: prepared.sourceFileId,
      upload_url: prepared.uploadUrl,
      headers: prepared.headers,
      expires_in_seconds: prepared.expiresInSeconds,
    });
  }
  if (uploads.length === 0) {
    throw new AgentOperationError("operation_not_ready", "The prepared upload is not ready.");
  }
  return { ...serializeAgentOperation(operation), uploads };
}

async function mutateOperationWithAction(input: {
  auth: AgentAuthContext;
  operationId: string;
  toolName: string;
  idempotencyKey: string;
  payloadHash: string;
  mutate: (
    tx: Prisma.TransactionClient,
    operation: {
      id: string;
      kind: AgentOperationKind;
      status: AgentOperationStatus;
      requestPayload: Prisma.JsonValue | null;
    },
  ) => Promise<void>;
}): Promise<{ operation: PublicAgentOperation; mutated: boolean }> {
  return runAgentSerializable(
    async (tx) => {
      await assertActiveConnection(tx, input.auth);
      await consumeRateLimit(tx, input.auth, AgentRateLimitKind.MUTATION, 10);
      const operation = await tx.agentSkillOperation.findFirst({
        where: {
          id: input.operationId,
          userId: input.auth.userId,
          connectionId: input.auth.connectionId,
        },
        select: { id: true, kind: true, status: true, requestPayload: true },
      });
      if (!operation) {
        throw new AgentOperationError("operation_not_found", "The operation was not found.");
      }
      const replay = await tx.agentOperationAction.findUnique({
        where: {
          connectionId_toolName_idempotencyKey: {
            connectionId: input.auth.connectionId,
            toolName: input.toolName,
            idempotencyKey: input.idempotencyKey,
          },
        },
        select: { operationId: true, userId: true, payloadHash: true },
      });
      let mutated = false;
      if (replay) {
        if (
          replay.operationId !== operation.id ||
          replay.userId !== input.auth.userId ||
          replay.payloadHash !== input.payloadHash
        ) {
          throw new AgentOperationError("idempotency_conflict", "That idempotency key was already used with different input.");
        }
      } else {
        await input.mutate(tx, operation);
        mutated = true;
        await tx.agentOperationAction.create({
          data: {
            userId: input.auth.userId,
            connectionId: input.auth.connectionId,
            operationId: operation.id,
            toolName: input.toolName,
            idempotencyKey: input.idempotencyKey,
            payloadHash: input.payloadHash,
          },
        });
      }
      const updated = await tx.agentSkillOperation.findFirst({
        where: {
          id: operation.id,
          userId: input.auth.userId,
          connectionId: input.auth.connectionId,
        },
        select: PUBLIC_OPERATION_SELECT,
      });
      if (!updated) {
        throw new AgentOperationError("operation_not_found", "The operation was not found.");
      }
      return { operation: serializeAgentOperation(updated), mutated };
    },
  );
}

async function runAgentSerializable<T>(
  work: (tx: Prisma.TransactionClient) => Promise<T>,
): Promise<T> {
  return runAgentDatabaseTransaction(work, { retryUniqueConstraint: true });
}

async function findReplay(
  tx: Prisma.TransactionClient,
  auth: AgentAuthContext,
  toolName: string,
  idempotencyKey: string,
  payloadHash: string,
) {
  const existing = await tx.agentSkillOperation.findUnique({
    where: { connectionId_toolName_idempotencyKey: { connectionId: auth.connectionId, toolName, idempotencyKey } },
    select: { ...PUBLIC_OPERATION_SELECT, payloadHash: true, userId: true },
  });
  if (!existing) return null;
  if (existing.userId !== auth.userId) throw new AgentOperationError("permission_denied", "The operation is not owned by this account.");
  if (existing.payloadHash !== payloadHash) {
    throw new AgentOperationError("idempotency_conflict", "That idempotency key was already used with different input.");
  }
  return {
    id: existing.id,
    status: existing.status,
    requestedCount: existing.requestedCount,
    activeCount: existing.activeCount,
    reusedCount: existing.reusedCount,
    failedCount: existing.failedCount,
    errorCode: existing.errorCode,
    createdAt: existing.createdAt,
    updatedAt: existing.updatedAt,
    completedAt: existing.completedAt,
    items: existing.items,
  };
}

async function assertActiveConnectionAndConsumeMutation(
  tx: Prisma.TransactionClient,
  auth: AgentAuthContext,
) {
  // Keep pending-item and activation reservations serialized with native
  // imports that use the same account lock. Serializable isolation protects
  // the operation row itself, but the shared capacity counts span several
  // tables and must be read while this lock is held.
  await tx.$queryRaw`SELECT "id" FROM "users" WHERE "id" = ${auth.userId} FOR UPDATE`;
  await assertActiveConnection(tx, auth);
  await consumeRateLimit(tx, auth, AgentRateLimitKind.MUTATION, 10);
}

async function assertActiveConnection(
  tx: Prisma.TransactionClient,
  auth: AgentAuthContext,
) {
  const connection = await tx.agentConnection.findFirst({
    where: {
      id: auth.connectionId,
      userId: auth.userId,
      status: AgentConnectionStatus.ACTIVE,
      workosSubject: auth.subject,
      workosSessionId: auth.sessionId,
      clientId: auth.clientId,
      resourceUrl: auth.resourceUrl,
      user: { agentAccessDisabledAt: null },
    },
    select: { id: true, permissionVersion: true },
  });
  const deleting = await tx.accountDeletionJob.findUnique({ where: { userId: auth.userId }, select: { id: true } });
  if (
    !connection ||
    deleting ||
    auth.expiresAt <= Date.now() / 1_000 ||
    (auth.permissionVersion !== undefined && connection.permissionVersion !== auth.permissionVersion)
  ) throw new AgentOperationError("permission_denied", "The agent connection is not active.");
}

async function assertPendingItemLimit(
  tx: Prisma.TransactionClient,
  userId: string,
  requestedItems: number,
) {
  const usage = await getPendingImportUsage({ userId, prisma: tx });
  if (
    usage.pendingItemCount + requestedItems > usage.pendingItemLimit
  ) {
    throw new AgentOperationError("too_many_pending_items", "Finish or review pending agent skills before adding more.");
  }
}

export async function consumeRateLimit(
  tx: Prisma.TransactionClient,
  auth: AgentAuthContext,
  kind: AgentRateLimitKind,
  limit: number,
) {
  const windowStart = new Date(Math.floor(Date.now() / 60_000) * 60_000);
  const bucket = await tx.agentRateLimitBucket.upsert({
    where: { connectionId_kind_windowStart: { connectionId: auth.connectionId, kind, windowStart } },
    create: { userId: auth.userId, connectionId: auth.connectionId, kind, windowStart, count: 1 },
    update: { count: { increment: 1 } },
    select: { count: true, userId: true },
  });
  if (bucket.userId !== auth.userId || bucket.count > limit) {
    throw new AgentOperationError("rate_limited", "This agent connection has reached its per-minute request limit.");
  }
}

function toJson(value: unknown): Prisma.InputJsonValue {
  return JSON.parse(JSON.stringify(value)) as Prisma.InputJsonValue;
}

function parseStoredRecord(value: Prisma.JsonValue | null): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

export function isRetryableAgentItemError(errorCode: string | null) {
  if (!errorCode) return false;
  const normalizedErrorCode = errorCode.toUpperCase().replace(/[^A-Z0-9]+/g, "_");
  return [
    "DRAFT_CREATE_FAILED",
    "DRAFT_GENERATION_FAILED",
    "EXTRACTION_FAILED",
    "GENERATION_FAILED",
    "INVALID_GENERATION",
    "MATERIAL_DRAFT_FAILED",
    "MISSING_GEMINI_ENV",
    "MISSING_S3_ENV",
    "QUOTA_EXCEEDED",
    "SOURCE_NOT_READY",
    "SKILL_NOT_DRAFT",
    "STALE_WORKER_RECOVERY",
    "TRANSIENT_WORKER_FAILURE",
    "VERIFICATION_FAILED",
  ].includes(normalizedErrorCode);
}

function daysFromNow(days: number) {
  return new Date(Date.now() + days * 24 * 60 * 60 * 1_000);
}

export async function enqueueOperation(
  userId: string,
  operationId: string,
  continuation?: { eventId: string; requestedAt: string; signal?: AbortSignal },
) {
  await sendAgentSkillOperationRequested({
    userId,
    operationId,
    requestedAt: continuation?.requestedAt ?? new Date().toISOString(),
  }, continuation
    ? {
        eventId: continuation.eventId,
        ...(continuation.signal ? { signal: continuation.signal } : {}),
      }
    : undefined);
}
