import { randomUUID } from "node:crypto";

import { afterAll, beforeAll, describe, expect, it } from "vitest";

import {
  AgentConnectionStatus,
  AgentRateLimitKind,
  MaterialRevisionStatus,
  SourceFileKind,
  SourceFileStatus,
  StudyMaterialKind,
  StudyMaterialStatus,
} from "@/generated/prisma/client";
import type { AgentAuthContext as AgentAuthClaims } from "@/lib/agent-access/auth";
import {
  completeAgentMaterialUpload,
  getAgentMaterialOperationStatus,
  getAgentMaterialStatus,
  importAgentMaterialUrl,
  prepareAgentMaterialUpload,
  retryAgentMaterialIngestion,
} from "@/lib/agent-access/material-ingestion";
import type {
  MaterialIngestionEventPayload,
  MaterialIngestionEventSender,
} from "@/lib/jobs/events";
import { recoverMaterialIngestionPublications } from "@/lib/materials/ingestion-publication";
import { getPrisma } from "@/lib/prisma";
import type { SourceObjectStorage } from "@/lib/storage/s3";

const runDatabaseTests = process.env.RUN_DATABASE_TESTS === "1";
const describeDatabase = runDatabaseTests ? describe : describe.skip;
const runId = `agent_material_ingestion_${randomUUID()}`;

describeDatabase("agent material ingestion", () => {
  const prisma = getPrisma();
  const userIds: string[] = [];

  beforeAll(async () => {
    await prisma.$queryRaw`SELECT 1`;
  });

  afterAll(async () => {
    await prisma.user.deleteMany({ where: { id: { in: userIds } } });
    await prisma.$disconnect();
  });

  async function createAuth(label: string): Promise<AgentAuthClaims> {
    const userId = `${runId}_${label}`;
    userIds.push(userId);
    await prisma.user.create({
      data: { id: userId, email: `${userId}@example.test` },
    });
    const identity = await prisma.workosIdentity.create({
      data: {
        userId,
        workosUserId: `workos_${userId}`,
        externalId: userId,
      },
    });
    const connection = await prisma.agentConnection.create({
      data: {
        userId,
        workosIdentityId: identity.id,
        workosSubject: userId,
        workosSessionId: `session_${userId}`,
        workosApplicationId: `application_${userId}`,
        clientId: "https://agent.example/client.json",
        clientName: "Material test agent",
        clientDomain: "agent.example",
        resourceUrl: "https://learnrecur.com/mcp",
        scopes: ["materials:read", "sources:upload"],
      },
    });
    return {
      userId,
      connectionId: connection.id,
      subject: userId,
      sessionId: connection.workosSessionId,
      clientId: connection.clientId,
      clientName: connection.clientName,
      clientDomain: connection.clientDomain,
      resourceUrl: connection.resourceUrl,
      expiresAt: Math.floor(Date.now() / 1_000) + 3_600,
      permissionVersion: 1,
      scopes: ["materials:read", "sources:upload"],
    } satisfies AgentAuthClaims;
  }

  async function mutationRateCount(auth: AgentAuthClaims) {
    const buckets = await prisma.agentRateLimitBucket.findMany({
      where: {
        connectionId: auth.connectionId,
        kind: AgentRateLimitKind.MUTATION,
      },
      select: { count: true },
    });
    return buckets.reduce((total, bucket) => total + bucket.count, 0);
  }

  async function queueRecoverableMaterial(
    auth: AgentAuthClaims,
    kind: "pdf" | "web",
    eventSender: MaterialIngestionEventSender,
  ) {
    const storage = createMemoryStorage();
    if (kind === "web") {
      return importAgentMaterialUrl(
        auth,
        {
          idempotency_key: `reliable_web_${randomUUID()}`,
          source_url: "https://books.example/intro",
          selected_urls: ["https://books.example/intro"],
        },
        {
          storage,
          resolveHostname: async () => ["93.184.216.34"],
          eventSender,
        },
      );
    }
    const prepared = await prepareAgentMaterialUpload(
      auth,
      {
        idempotency_key: `reliable_pdf_${randomUUID()}`,
        title: "Reliable material",
        original_name: "reliable.pdf",
        mime_type: "application/pdf",
        byte_size: 2_048,
      },
      { storage },
    );
    seedUploadedObject(storage, prepared.upload_url, 2_048);
    return completeAgentMaterialUpload(
      auth,
      {
        idempotency_key: `reliable_complete_${randomUUID()}`,
        material_revision_id: prepared.material_revision_id!,
        operation_id: prepared.operation_id,
      },
      { storage, eventSender },
    );
  }

  it.each([
    { kind: "pdf" as const, ambiguous: false },
    { kind: "web" as const, ambiguous: false },
    { kind: "pdf" as const, ambiguous: true },
    { kind: "web" as const, ambiguous: true },
  ])(
    "recovers agent $kind publication (ambiguous: $ambiguous) with a stable identity",
    async ({ kind, ambiguous }) => {
      const auth = await createAuth(`reliable_${kind}_${ambiguous}`);
      const sends: Array<{
        payload: MaterialIngestionEventPayload;
        eventId?: string;
      }> = [];
      const operation = await queueRecoverableMaterial(auth, kind, {
        async sendMaterialIngestionRequested(payload, options) {
          sends.push({ payload, eventId: options?.eventId });
          throw new Error(
            ambiguous
              ? "synthetic acknowledgement lost"
              : "synthetic queue unavailable",
          );
        },
      });
      expect(operation).toMatchObject({
        status: "queued",
        error_code: null,
        completed_at: null,
      });
      const revisionId = operation.material_revision_id!;
      const revision = await prisma.materialRevision.findUniqueOrThrow({
        where: { id: revisionId },
      });
      expect(revision).toMatchObject({
        status: "QUEUED",
        processingMetadata: {
          ingestionDelivery: {
            status: "pending",
            attempt: 1,
            eventId: sends[0].eventId,
          },
        },
      });
      expect(sends[0].eventId).toBeTruthy();
      expect(
        await prisma.sourceFile.findFirstOrThrow({
          where: { materialRevisionId: revisionId },
        }),
      ).toMatchObject({ status: "UPLOADED" });
      await prisma.materialRevision.update({
        where: { id: revisionId },
        data: { updatedAt: new Date(0) },
      });
      await recoverMaterialIngestionPublications({
        now: new Date(Date.now() + 6 * 60_000),
        sender: {
          async sendMaterialIngestionRequested(payload, options) {
            if (payload.materialRevisionId === revisionId)
              sends.push({ payload, eventId: options?.eventId });
          },
        },
      });
      expect(sends).toHaveLength(2);
      expect(sends[1]).toEqual(sends[0]);
      expect(
        await getAgentMaterialOperationStatus(auth, operation.operation_id),
      ).toMatchObject({ status: "queued", error_code: null });
      const finalizedAt = new Date();
      await prisma.materialRevision.update({
        where: { id: revisionId },
        data: { status: "READY", finalizedAt },
      });
      expect(
        await getAgentMaterialOperationStatus(auth, operation.operation_id),
      ).toMatchObject({
        status: "succeeded",
        completed_at: finalizedAt.toISOString(),
      });
    },
  );

  it.each(["pdf", "web"] as const)(
    "reports bounded agent %s publication exhaustion through operation polling",
    async (kind) => {
      const auth = await createAuth(`exhausted_${kind}`);
      let sends = 0;
      const sender: MaterialIngestionEventSender = {
        async sendMaterialIngestionRequested() {
          sends += 1;
          throw new Error("synthetic persistent queue outage");
        },
      };
      const operation = await queueRecoverableMaterial(auth, kind, sender);
      expect(operation.status).toBe("queued");
      for (let attempt = 1; attempt <= 5; attempt += 1) {
        await prisma.materialRevision.update({
          where: { id: operation.material_revision_id! },
          data: { updatedAt: new Date(0) },
        });
        await recoverMaterialIngestionPublications({
          now: new Date(Date.now() + attempt * 6 * 60_000),
          sender,
        });
      }
      expect(sends).toBe(5);
      expect(
        await getAgentMaterialOperationStatus(auth, operation.operation_id),
      ).toMatchObject({
        status: "failed",
        error_code: "EVENT_SEND_FAILED",
        retryable: true,
      });
      expect(
        await prisma.sourceFile.findFirstOrThrow({
          where: { materialRevisionId: operation.material_revision_id! },
        }),
      ).toMatchObject({ status: "FAILED" });
    },
  );

  it("fences concurrent PDF preparation and enqueues one completion", async () => {
    const auth = await createAuth("pdf");
    const storage = createMemoryStorage();
    const events: MaterialIngestionEventPayload[] = [];
    const input = {
      idempotency_key: `prepare_${randomUUID()}`,
      title: "Practical Spanish Grammar",
      original_name: "spanish-grammar.pdf",
      mime_type: "application/pdf" as const,
      byte_size: 2_048,
    };
    const dependencies = {
      storage,
      eventSender: {
        async sendMaterialIngestionRequested(
          payload: MaterialIngestionEventPayload,
        ) {
          events.push(payload);
        },
      },
    };

    const [first, second] = await Promise.all([
      prepareAgentMaterialUpload(auth, input, dependencies),
      prepareAgentMaterialUpload(auth, input, dependencies),
    ]);
    expect(first.operation_id).toBe(second.operation_id);
    expect(first.material_revision_id).toBe(second.material_revision_id);
    expect(first.upload_url).toMatch(/^https:\/\/uploads\.example\//);
    expect(
      await prisma.agentSkillOperation.count({
        where: { userId: auth.userId, toolName: "materials.prepare_upload" },
      }),
    ).toBe(1);
    expect(
      await prisma.studyMaterial.count({ where: { userId: auth.userId } }),
    ).toBe(1);

    seedUploadedObject(storage, first.upload_url, input.byte_size);
    const completeInput = {
      idempotency_key: `complete_${randomUUID()}`,
      material_revision_id: first.material_revision_id!,
      operation_id: first.operation_id,
    };
    const [completed, concurrentReplay] = await Promise.all([
      completeAgentMaterialUpload(auth, completeInput, dependencies),
      completeAgentMaterialUpload(auth, completeInput, dependencies),
    ]);
    const headCallsAfterConcurrentCompletion = storage.headCalls;
    const replay = await completeAgentMaterialUpload(
      auth,
      completeInput,
      dependencies,
    );

    expect(completed).toMatchObject({
      operation_id: first.operation_id,
      status: "queued",
      retryable: false,
    });
    expect(concurrentReplay).toMatchObject({
      operation_id: first.operation_id,
      status: "queued",
    });
    expect(replay.operation_id).toBe(completed.operation_id);
    expect(storage.headCalls).toBe(headCallsAfterConcurrentCompletion);
    expect(events).toHaveLength(1);
    await expect(
      prisma.materialRevision.findUniqueOrThrow({
        where: { id: first.material_revision_id! },
        select: { status: true },
      }),
    ).resolves.toEqual({ status: MaterialRevisionStatus.QUEUED });
    await expect(
      prisma.sourceFile.findFirstOrThrow({
        where: { materialRevisionId: first.material_revision_id! },
        select: { status: true },
      }),
    ).resolves.toEqual({ status: SourceFileStatus.UPLOADED });

    const finalizedAt = new Date();
    await prisma.materialRevision.update({
      where: { id: first.material_revision_id! },
      data: { status: MaterialRevisionStatus.READY, finalizedAt },
    });
    await expect(
      completeAgentMaterialUpload(auth, completeInput, dependencies),
    ).resolves.toMatchObject({
      operation_id: first.operation_id,
      status: "succeeded",
      completed_at: finalizedAt.toISOString(),
    });
    await expect(
      getAgentMaterialOperationStatus(auth, first.operation_id),
    ).resolves.toMatchObject({
      operation_id: first.operation_id,
      operation_uri: `learnrecur://material-operations/${first.operation_id}`,
      status: "succeeded",
      completed_at: finalizedAt.toISOString(),
    });
    expect(
      await prisma.agentOperationAction.count({
        where: {
          connectionId: auth.connectionId,
          toolName: "materials.complete_upload",
        },
      }),
    ).toBe(1);
    await expect(
      completeAgentMaterialUpload(
        auth,
        { ...completeInput, material_revision_id: "a-different-revision" },
        dependencies,
      ),
    ).rejects.toMatchObject({ code: "idempotency_conflict" });
  });

  it("charges one mutation allowance for an upload URL refresh", async () => {
    const auth = await createAuth("replay_rate");
    const storage = createMemoryStorage();
    const input = {
      idempotency_key: `replay_rate_${randomUUID()}`,
      title: "Replay rate",
      original_name: "replay-rate.pdf",
      mime_type: "application/pdf" as const,
      byte_size: 1_024,
    };

    const first = await prepareAgentMaterialUpload(auth, input, { storage });
    const beforeReplay = await mutationRateCount(auth);
    expect(beforeReplay).toBe(1);

    const replay = await prepareAgentMaterialUpload(auth, input, { storage });
    const afterReplay = await mutationRateCount(auth);

    expect(replay.operation_id).toBe(first.operation_id);
    // The idempotency claim pays once; both refresh checks are trusted
    // continuations that must not charge again.
    expect(afterReplay - beforeReplay).toBe(1);
  });

  it("keeps concurrent distinct preparations mapped to their own objects", async () => {
    const auth = await createAuth("distinct_keys");
    const storage = createMemoryStorage();
    const firstInput = {
      idempotency_key: `distinct_first_${randomUUID()}`,
      title: "First distinct upload",
      original_name: "first-distinct.pdf",
      mime_type: "application/pdf" as const,
      byte_size: 1_024,
    };
    const secondInput = {
      idempotency_key: `distinct_second_${randomUUID()}`,
      title: "Second distinct upload",
      original_name: "second-distinct.pdf",
      mime_type: "application/pdf" as const,
      byte_size: 2_048,
    };

    const [first, second] = await Promise.all([
      prepareAgentMaterialUpload(auth, firstInput, { storage }),
      prepareAgentMaterialUpload(auth, secondInput, { storage }),
    ]);
    expect(first.operation_id).not.toBe(second.operation_id);
    expect(first.upload_url).toMatch(/^https:\/\/uploads\.example\//);
    expect(second.upload_url).toMatch(/^https:\/\/uploads\.example\//);
    const firstKey = storageKeyFromUploadUrl(first.upload_url!);
    const secondKey = storageKeyFromUploadUrl(second.upload_url!);
    expect(firstKey).not.toBe(secondKey);
    expect(storage.preparedKeys).toEqual(
      expect.arrayContaining([firstKey, secondKey]),
    );

    seedUploadedObject(storage, first.upload_url, firstInput.byte_size);
    seedUploadedObject(storage, second.upload_url, secondInput.byte_size);
    const [completedFirst, completedSecond] = await Promise.all([
      completeAgentMaterialUpload(
        auth,
        {
          idempotency_key: `distinct_complete_first_${randomUUID()}`,
          material_revision_id: first.material_revision_id!,
          operation_id: first.operation_id,
        },
        { storage, eventSender: { async sendMaterialIngestionRequested() {} } },
      ),
      completeAgentMaterialUpload(
        auth,
        {
          idempotency_key: `distinct_complete_second_${randomUUID()}`,
          material_revision_id: second.material_revision_id!,
          operation_id: second.operation_id,
        },
        { storage, eventSender: { async sendMaterialIngestionRequested() {} } },
      ),
    ]);
    expect(completedFirst.status).toBe("queued");
    expect(completedSecond.status).toBe("queued");
  });

  it("does not refresh a replayed upload after revocation during presigning", async () => {
    const auth = await createAuth("replay_revoked");
    const storage = createMemoryStorage();
    const input = {
      idempotency_key: `replay_revoked_${randomUUID()}`,
      title: "Revoked replay",
      original_name: "revoked-replay.pdf",
      mime_type: "application/pdf" as const,
      byte_size: 1_024,
    };
    const prepared = await prepareAgentMaterialUpload(auth, input, { storage });
    const before = await prisma.sourceFile.findUniqueOrThrow({
      where: { id: prepared.source_file_id! },
      select: { presignedUploadExpiresAt: true },
    });
    let presignCalls = 0;
    storage.createPresignedUploadUrl = async () => {
      presignCalls += 1;
      await prisma.agentConnection.update({
        where: { id: auth.connectionId },
        data: { status: AgentConnectionStatus.REVOKED },
      });
      return "https://uploads.example/revoked-replay";
    };

    await expect(
      prepareAgentMaterialUpload(auth, input, { storage }),
    ).rejects.toMatchObject({ code: "permission_denied" });
    expect(presignCalls).toBe(1);
    await expect(
      prisma.sourceFile.findUniqueOrThrow({
        where: { id: prepared.source_file_id! },
        select: { presignedUploadExpiresAt: true },
      }),
    ).resolves.toEqual(before);
  });

  it("denies a replay during account deletion before generating an upload URL", async () => {
    const auth = await createAuth("replay_deleting");
    const storage = createMemoryStorage();
    const input = {
      idempotency_key: `replay_deleting_${randomUUID()}`,
      title: "Deleting replay",
      original_name: "deleting-replay.pdf",
      mime_type: "application/pdf" as const,
      byte_size: 1_024,
    };
    const prepared = await prepareAgentMaterialUpload(auth, input, { storage });
    const before = await prisma.sourceFile.findUniqueOrThrow({
      where: { id: prepared.source_file_id! },
      select: { presignedUploadExpiresAt: true },
    });
    const deletionJob = await prisma.accountDeletionJob.create({
      data: {
        userId: auth.userId,
        manifest: { version: 1, storageObjects: [], agentConnections: [] },
      },
    });
    let presignCalls = 0;
    storage.createPresignedUploadUrl = async () => {
      presignCalls += 1;
      return "https://uploads.example/deleting-replay";
    };

    try {
      await expect(
        prepareAgentMaterialUpload(auth, input, { storage }),
      ).rejects.toMatchObject({ code: "permission_denied" });
      expect(presignCalls).toBe(0);
      await expect(
        prisma.sourceFile.findUniqueOrThrow({
          where: { id: prepared.source_file_id! },
          select: { presignedUploadExpiresAt: true },
        }),
      ).resolves.toEqual(before);
    } finally {
      await prisma.accountDeletionJob.delete({ where: { id: deletionJob.id } });
    }
  });

  it("reconciles native failure and deletion states for operation polling", async () => {
    const auth = await createAuth("operation_states");
    const storage = createMemoryStorage();
    const prepared = await prepareAgentMaterialUpload(
      auth,
      {
        idempotency_key: `operation_states_${randomUUID()}`,
        title: "Operation states",
        original_name: "operation-states.pdf",
        mime_type: "application/pdf",
        byte_size: 1_024,
      },
      { storage },
    );
    seedUploadedObject(storage, prepared.upload_url, 1_024);
    await completeAgentMaterialUpload(
      auth,
      {
        idempotency_key: `operation_states_complete_${randomUUID()}`,
        material_revision_id: prepared.material_revision_id!,
        operation_id: prepared.operation_id,
      },
      { storage, eventSender: { async sendMaterialIngestionRequested() {} } },
    );

    await prisma.materialRevision.update({
      where: { id: prepared.material_revision_id! },
      data: {
        status: MaterialRevisionStatus.FAILED,
        errorCode: "TRANSIENT_INGESTION_FAILURE",
        errorMessage: "The native parser timed out.",
      },
    });
    await expect(
      getAgentMaterialOperationStatus(auth, prepared.operation_id),
    ).resolves.toMatchObject({
      status: "failed",
      error_code: "TRANSIENT_INGESTION_FAILURE",
      retryable: true,
    });

    await prisma.agentConnection.update({
      where: { id: auth.connectionId },
      data: { scopes: [] },
    });
    await expect(
      getAgentMaterialOperationStatus(auth, prepared.operation_id),
    ).rejects.toMatchObject({ code: "permission_denied" });
    await prisma.agentConnection.update({
      where: { id: auth.connectionId },
      data: { scopes: ["materials:read"] },
    });

    await prisma.studyMaterial.update({
      where: { id: prepared.material_id! },
      data: { status: StudyMaterialStatus.DELETING },
    });
    await expect(
      getAgentMaterialOperationStatus(auth, prepared.operation_id),
    ).resolves.toMatchObject({
      status: "canceled",
      error_code: "MATERIAL_DELETED",
      retryable: false,
    });
  });

  it("does not overwrite native readiness when event acknowledgement is lost", async () => {
    const auth = await createAuth("event_race");
    const storage = createMemoryStorage();
    const prepared = await prepareAgentMaterialUpload(
      auth,
      {
        idempotency_key: `event_race_${randomUUID()}`,
        title: "Event race",
        original_name: "event-race.pdf",
        mime_type: "application/pdf",
        byte_size: 1_024,
      },
      { storage },
    );
    seedUploadedObject(storage, prepared.upload_url, 1_024);
    const finalizedAt = new Date();
    let senderReachedIntentionalFailure = false;

    const completed = await completeAgentMaterialUpload(
      auth,
      {
        idempotency_key: `event_race_complete_${randomUUID()}`,
        material_revision_id: prepared.material_revision_id!,
        operation_id: prepared.operation_id,
      },
      {
        storage,
        eventSender: {
          async sendMaterialIngestionRequested() {
            // Native ingestion makes the source immutable only after the
            // source reaches READY and the revision is finalized.
            await prisma.sourceFile.update({
              where: { id: prepared.source_file_id! },
              data: {
                status: SourceFileStatus.READY,
                extractedText: "native late-ack fixture",
              },
            });
            await prisma.materialRevision.update({
              where: { id: prepared.material_revision_id! },
              data: {
                status: MaterialRevisionStatus.READY,
                finalizedAt,
              },
            });
            senderReachedIntentionalFailure = true;
            throw new Error("queue acknowledgement was lost");
          },
        },
      },
    );

    expect(completed).toMatchObject({
      operation_id: prepared.operation_id,
      status: "succeeded",
      error_code: null,
    });
    expect(senderReachedIntentionalFailure).toBe(true);
    await expect(
      prisma.materialRevision.findUniqueOrThrow({
        where: { id: prepared.material_revision_id! },
        select: { status: true, finalizedAt: true },
      }),
    ).resolves.toEqual({
      status: MaterialRevisionStatus.READY,
      finalizedAt,
    });
    await expect(
      prisma.sourceFile.findUniqueOrThrow({
        where: { id: prepared.source_file_id! },
        select: { status: true },
      }),
    ).resolves.toEqual({ status: SourceFileStatus.READY });
    await expect(
      prisma.agentSkillOperation.findUniqueOrThrow({
        where: { id: prepared.operation_id },
        select: { status: true, errorCode: true },
      }),
    ).resolves.toEqual({
      status: "QUEUED",
      errorCode: null,
    });
  });

  it("imports a bounded same-origin URL set and keeps the operation replayable", async () => {
    const auth = await createAuth("web");
    const storage = createMemoryStorage();
    const events: MaterialIngestionEventPayload[] = [];
    const input = {
      idempotency_key: `import_${randomUUID()}`,
      source_url: "https://books.example/intro",
      selected_urls: [
        "https://books.example/intro",
        "https://books.example/chapter-1",
      ],
    };
    const dependencies = {
      storage,
      resolveHostname: async () => ["93.184.216.34"],
      eventSender: {
        async sendMaterialIngestionRequested(
          payload: MaterialIngestionEventPayload,
        ) {
          events.push(payload);
        },
      },
    };

    const first = await importAgentMaterialUrl(auth, input, dependencies);
    const replay = await importAgentMaterialUrl(auth, input, dependencies);

    expect(replay).toEqual(first);
    expect(first.status).toBe("queued");
    expect(events).toHaveLength(1);
    const source = await prisma.sourceFile.findFirstOrThrow({
      where: {
        userId: auth.userId,
        materialRevisionId: first.material_revision_id!,
      },
      select: { status: true, kind: true, metadata: true },
    });
    expect(source).toMatchObject({
      status: SourceFileStatus.UPLOADED,
      kind: "URL",
    });
    expect(source.metadata).toMatchObject({
      selectedUrls: input.selected_urls,
    });
  });

  it("returns an import replay before repeating website discovery", async () => {
    const auth = await createAuth("web_replay");
    const storage = createMemoryStorage();
    let discoveryCalls = 0;
    const input = {
      idempotency_key: `import_replay_${randomUUID()}`,
      source_url: "https://books.example/intro",
    };
    const dependencies = {
      storage,
      resolveHostname: async () => ["93.184.216.34"],
      discoverWebsite: async () => {
        discoveryCalls += 1;
        if (discoveryCalls > 1) {
          throw new Error("the source site is unavailable");
        }
        return {
          title: "Example textbook",
          sourceUrl: input.source_url,
          pages: [{ title: "Introduction", url: input.source_url, level: 1 }],
          preferredPdf: null,
        };
      },
      eventSender: { async sendMaterialIngestionRequested() {} },
    };

    const first = await importAgentMaterialUrl(auth, input, dependencies);
    const replay = await importAgentMaterialUrl(auth, input, dependencies);

    expect(replay).toEqual(first);
    expect(discoveryCalls).toBe(1);
  });

  it("does not expose a deleted material through status reads", async () => {
    const auth = await createAuth("deleted");
    const storage = createMemoryStorage();
    const prepared = await prepareAgentMaterialUpload(
      auth,
      {
        idempotency_key: `deleted_${randomUUID()}`,
        title: "Deleted material",
        original_name: "deleted.pdf",
        mime_type: "application/pdf",
        byte_size: 1_024,
      },
      { storage },
    );
    await prisma.studyMaterial.update({
      where: { id: prepared.material_id! },
      data: { status: StudyMaterialStatus.DELETING },
    });

    await expect(
      getAgentMaterialStatus(auth, { material_id: prepared.material_id! }),
    ).rejects.toMatchObject({
      code: "material_not_found",
    });
    await prisma.agentConnection.update({
      where: { id: auth.connectionId },
      data: { status: AgentConnectionStatus.REVOKED },
    });
    await expect(
      prepareAgentMaterialUpload(
        auth,
        {
          idempotency_key: `revoked_${randomUUID()}`,
          title: "Revoked material",
          original_name: "revoked.pdf",
          mime_type: "application/pdf",
          byte_size: 1_024,
        },
        { storage },
      ),
    ).rejects.toMatchObject({
      code: "permission_denied",
    });
  });

  it("repairs a stalled revision through the native retry pipeline", async () => {
    const auth = await createAuth("retry");
    const material = await prisma.studyMaterial.create({
      data: {
        userId: auth.userId,
        title: "Stalled textbook",
        kind: StudyMaterialKind.PDF,
        revisions: {
          create: {
            revisionNumber: 1,
            status: MaterialRevisionStatus.QUEUED,
            updatedAt: new Date(Date.now() - 10 * 60 * 1_000),
          },
        },
      },
      include: { revisions: true },
    });
    const revision = material.revisions[0];
    await prisma.sourceFile.create({
      data: {
        userId: auth.userId,
        materialRevisionId: revision.id,
        kind: SourceFileKind.PDF,
        status: SourceFileStatus.FAILED,
        originalName: "stalled.pdf",
        mimeType: "application/pdf",
      },
    });
    const events: MaterialIngestionEventPayload[] = [];

    const result = await retryAgentMaterialIngestion(
      auth,
      {
        idempotency_key: `retry_${randomUUID()}`,
        material_revision_id: revision.id,
        material_id: material.id,
      },
      {
        eventSender: {
          async sendMaterialIngestionRequested(
            payload: MaterialIngestionEventPayload,
          ) {
            events.push(payload);
          },
        },
      },
    );

    expect(result).toMatchObject({
      status: "queued",
      material_id: material.id,
      material_revision_id: revision.id,
    });
    expect(events).toHaveLength(1);
    await expect(
      prisma.materialRevision.findUniqueOrThrow({
        where: { id: revision.id },
        select: { status: true },
      }),
    ).resolves.toEqual({ status: MaterialRevisionStatus.QUEUED });
  });

  it("rechecks stored scopes and permission versions for each request", async () => {
    const auth = await createAuth("grant");
    const storage = createMemoryStorage();
    const prepared = await prepareAgentMaterialUpload(
      auth,
      {
        idempotency_key: `grant_prepare_${randomUUID()}`,
        title: "Grant checks",
        original_name: "grant-checks.pdf",
        mime_type: "application/pdf",
        byte_size: 1_024,
      },
      { storage },
    );

    await prisma.agentConnection.update({
      where: { id: auth.connectionId },
      data: { scopes: ["materials:read"] },
    });
    await expect(
      prepareAgentMaterialUpload(
        auth,
        {
          idempotency_key: `grant_denied_${randomUUID()}`,
          title: "Denied upload",
          original_name: "denied.pdf",
          mime_type: "application/pdf",
          byte_size: 1_024,
        },
        { storage },
      ),
    ).rejects.toMatchObject({ code: "permission_denied" });

    await prisma.agentConnection.update({
      where: { id: auth.connectionId },
      data: { permissionVersion: 2 },
    });
    await expect(
      getAgentMaterialStatus(auth, { material_id: prepared.material_id! }),
    ).rejects.toMatchObject({ code: "permission_denied" });
  });
});

function createMemoryStorage(): SourceObjectStorage & {
  objects: Map<string, Buffer>;
  preparedKeys: string[];
  headCalls: number;
} {
  const objects = new Map<string, Buffer>();
  const storage = {
    bucketName: "test-agent-materials",
    objects,
    preparedKeys: [] as string[],
    headCalls: 0,
    async createPresignedUploadUrl({ key }: { key: string }) {
      storage.preparedKeys.push(key);
      return `https://uploads.example/${encodeURIComponent(key)}`;
    },
    async headObject({ key }: { key: string }) {
      storage.headCalls += 1;
      const bytes = objects.get(key);
      return {
        byteSize: bytes?.byteLength ?? null,
        mimeType: bytes ? "application/pdf" : null,
      };
    },
    async getObjectBytes({ key }: { key: string }) {
      const bytes = objects.get(key);
      if (!bytes) throw new Error("missing test object");
      return bytes;
    },
    async listObjects() {
      return [...objects.keys()];
    },
    async deleteObject({ key }: { key: string }) {
      objects.delete(key);
    },
  } satisfies SourceObjectStorage & {
    objects: Map<string, Buffer>;
    preparedKeys: string[];
    headCalls: number;
  };
  return storage;
}

function storageKeyFromUploadUrl(uploadUrl: string) {
  return decodeURIComponent(new URL(uploadUrl).pathname.slice(1));
}

function seedUploadedObject(
  storage: { objects: Map<string, Buffer> },
  uploadUrl: string | null,
  byteSize: number,
) {
  expect(uploadUrl).toMatch(/^https:\/\/uploads\.example\//);
  storage.objects.set(
    storageKeyFromUploadUrl(uploadUrl!),
    Buffer.alloc(byteSize),
  );
}
