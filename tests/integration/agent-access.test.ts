import { randomUUID } from "node:crypto";

import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";

vi.mock("@/lib/jobs/events", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@/lib/jobs/events")>()),
  sendAgentConnectionRevocationRequested: vi.fn().mockResolvedValue(undefined),
  sendAgentSkillOperationRequested: vi.fn().mockResolvedValue(undefined),
}));

vi.mock("@/lib/skills/activation-timing", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@/lib/skills/activation-timing")>();
  return {
    ...actual,
    GENERATION_TIMEOUT_MS: 1_000,
    ACTIVATION_PROVIDER_CHAIN_TIMEOUT_MS: 2_500,
    ACTIVATION_SOURCE_EVIDENCE_TIMEOUT_MS: 250,
    ACTIVATION_RECENT_HISTORY_TIMEOUT_MS: 2_500,
    CHOICE_VERIFICATION_TIMEOUT_MS: 2_500,
    ACTIVATION_GENERATION_COMPLETION_SLACK_MS: 500,
    ACTIVATION_PUBLISH_TRANSACTION_MAX_WAIT_MS: 5_000,
    ACTIVATION_PUBLISH_TRANSACTION_TIMEOUT_MS: 5_000,
    ACTIVATION_RESERVATION_TRANSACTION_MAX_WAIT_MS: 5_000,
    ACTIVATION_RESERVATION_TRANSACTION_TIMEOUT_MS: 15_000,
    ACTIVATION_GENERATION_TIMEOUT_MS: 20_000,
  };
});

vi.mock("@/lib/agent-access/recovery-policy", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@/lib/agent-access/recovery-policy")>();
  return {
    ...actual,
    AGENT_OPERATION_CLEANUP_MARGIN_MS: 5,
    AGENT_OPERATION_ACTIVATION_RESERVE_MS: 20_005,
    AGENT_OPERATION_CANDIDATE_VERIFICATION_RESERVE_MS: 5,
  };
});

import {
  AgentCandidateStatus,
  AgentConnectionStatus,
  AgentOperationKind,
  AgentOperationItemStatus,
  AgentOperationStatus,
  AgentRateLimitKind,
  AgentRemoteRevocationStatus,
  AgentRevocationOutboxStatus,
  AnswerKind,
  ExerciseType,
  GenerationFailureCategory,
  GenerationJobKind,
  GenerationJobStage,
  GenerationJobStatus,
  SourceFileKind,
  SourceFileStatus,
  StudyMaterialKind,
  SkillStatus,
} from "@/generated/prisma/client";
import {
  disableAgentAccessForAccountDeletion,
  pauseSkillsFromAgent,
  resolveAgentDuplicateReview,
  revokeAgentConnection,
  runAgentAccessMaintenance,
} from "@/lib/agent-access/settings";
import {
  continueAgentOperation,
  createAgentSpecOperation,
} from "@/lib/agent-access/operations";
import { recoverStaleAgentOperationItems } from "@/lib/agent-access/recovery";
import * as agentAccessRecovery from "@/lib/agent-access/recovery";
import { listAgentMaterials } from "@/lib/agent-access/materials";
import { reserveAgentActivation, runAgentSkillOperationJob } from "@/lib/agent-access/worker";
import * as materialBatches from "@/lib/materials/batches";
import { createMaterialWithInitialRevision } from "@/lib/materials/lifecycle";
import { materialScopeResolutionSchema } from "@/lib/materials/contracts";
import { MaterialEvidencePendingError } from "@/lib/materials/evidence";
import {
  sendAgentConnectionRevocationRequested,
  sendAgentSkillOperationRequested,
} from "@/lib/jobs/events";
import { getPrisma } from "@/lib/prisma";
import { JobStageTimeoutError } from "@/lib/jobs/deadline";
import { JobContinuationLimitError } from "@/lib/jobs/publication-context";
import { getUserDataExport } from "@/lib/settings/data-export";
import * as refillJobs from "@/lib/skills/refill-jobs";
import { ALPHA_ACTIVE_SKILLS } from "@/lib/usage-limits";

const runDatabaseTests = process.env.RUN_DATABASE_TESTS === "1";
const describeDatabase = runDatabaseTests ? describe : describe.skip;
const runId = `agent_access_${randomUUID()}`;

describeDatabase("agent access persistence", () => {
  const prisma = getPrisma();
  const userIds: string[] = [];

  beforeAll(async () => {
    await prisma.$queryRaw`SELECT 1`;
  });

  afterAll(async () => {
    await prisma.agentRevocationOutbox.deleteMany({ where: { userId: { in: userIds } } });
    await prisma.user.deleteMany({ where: { id: { in: userIds } } });
    await prisma.$disconnect();
  });

  async function createConnection(label: string) {
    const userId = `${runId}_${label}`;
    userIds.push(userId);
    await prisma.user.create({ data: { id: userId, email: `${label}@example.test` } });
    const identity = await prisma.workosIdentity.create({
      data: {
        userId,
        workosUserId: `workos_${label}_${runId}`,
        externalId: userId,
      },
    });
    const connection = await prisma.agentConnection.create({
      data: {
        userId,
        workosIdentityId: identity.id,
        workosSubject: identity.externalId,
        workosSessionId: `session_${label}_${runId}`,
        workosApplicationId: `application_${label}_${runId}`,
        clientId: `https://${label}.example.test/client.json`,
        clientName: `${label} agent`,
        clientDomain: `${label}.example.test`,
        resourceUrl: "https://learnrecur.com/mcp",
        scopes: ["skills:create"],
      },
    });
    return { userId, identity, connection };
  }

  async function createQueuedDraftOperation(
    fixture: Awaited<ReturnType<typeof createConnection>>,
    label: string,
  ) {
    const skill = await prisma.skill.create({
      data: {
        userId: fixture.userId,
        title: `${label} worker reliability skill`,
        objective: `Practice ${label} with phrase ${randomUUID()}.`,
        status: SkillStatus.DRAFT,
      },
    });
    const operation = await prisma.agentSkillOperation.create({
      data: {
        userId: fixture.userId,
        connectionId: fixture.connection.id,
        kind: AgentOperationKind.SPEC_BATCH,
        toolName: "skills.add_from_specs",
        status: AgentOperationStatus.QUEUED,
        idempotencyKey: `worker-reliability-${label}-${runId}`,
        payloadHash: randomUUID().replaceAll("-", "").padEnd(64, "a"),
        requestedCount: 1,
        items: {
          create: {
            ordinal: 0,
            clientReference: `${label}-item`,
            status: AgentOperationItemStatus.QUEUED,
            createdSkillId: skill.id,
          },
        },
      },
      include: { items: true },
    });
    return { skill, operation, item: operation.items[0] };
  }

  function quickActivationOptions(verifyChoiceExercises?: (input: {
    signal?: AbortSignal;
    candidates: Array<{ candidateId: string }>;
  }) => Promise<unknown>) {
    return {
      generateChoiceExercises: async () => ({
        exercises: [1, 2, 3].map((index) => ({
          prompt: `Choose the verified answer for reliability example ${index}.`,
          choices: [
            { id: "correct", label: `Correct answer ${index}` },
            { id: "wrong", label: `Incorrect answer ${index}` },
          ],
          correctChoiceId: "correct",
          explanation: `The skill contract establishes answer ${index}.`,
          difficulty: 2,
          expectedSeconds: 20,
        })),
      }),
      verifyChoiceExercises: verifyChoiceExercises ?? (async ({ candidates }) => ({
        verifications: candidates.map((candidate) => ({
          candidateId: candidate.candidateId,
          verdict: "verified",
        })),
      })),
    };
  }

  it("revokes locally before queuing a durable remote revocation", async () => {
    const fixture = await createConnection("revoke");
    const now = new Date("2026-08-13T15:00:00.000Z");
    vi.mocked(sendAgentConnectionRevocationRequested).mockRejectedValueOnce(
      new Error("temporary event dispatch failure"),
    );

    await expect(
      revokeAgentConnection({ userId: fixture.userId, connectionId: fixture.connection.id, now }),
    ).resolves.toMatchObject({ status: "revoked", alreadyRevoked: false });
    expect(sendAgentConnectionRevocationRequested).toHaveBeenCalledWith({
      userId: fixture.userId,
      connectionId: fixture.connection.id,
      requestedAt: now.toISOString(),
    });

    const connection = await prisma.agentConnection.findUniqueOrThrow({
      where: { id: fixture.connection.id },
    });
    expect(connection).toMatchObject({
      status: AgentConnectionStatus.REVOKED,
      remoteRevocationStatus: AgentRemoteRevocationStatus.PENDING,
      revokedAt: now,
    });
    await expect(
      prisma.agentRevocationOutbox.findUniqueOrThrow({
        where: { connectionId: fixture.connection.id },
      }),
    ).resolves.toMatchObject({
      userId: fixture.userId,
      workosUserId: fixture.identity.workosUserId,
      applicationId: fixture.connection.workosApplicationId,
      status: AgentRevocationOutboxStatus.PENDING,
    });
  });

  it("revokes every local connection covered by one WorkOS grant", async () => {
    const fixture = await createConnection("grant-revoke");
    const sibling = await prisma.agentConnection.create({
      data: {
        userId: fixture.userId,
        workosIdentityId: fixture.identity.id,
        workosSubject: fixture.identity.externalId,
        workosSessionId: `session_grant_sibling_${runId}`,
        workosApplicationId: fixture.connection.workosApplicationId,
        clientId: fixture.connection.clientId,
        clientName: fixture.connection.clientName,
        clientDomain: fixture.connection.clientDomain,
        resourceUrl: fixture.connection.resourceUrl,
        scopes: ["skills:create"],
      },
    });
    const now = new Date("2026-08-13T15:30:00.000Z");

    await expect(
      revokeAgentConnection({ userId: fixture.userId, connectionId: fixture.connection.id, now }),
    ).resolves.toMatchObject({ status: "revoked", alreadyRevoked: false });
    await expect(
      prisma.agentConnection.findMany({
        where: { id: { in: [fixture.connection.id, sibling.id] } },
        select: { status: true, revokedAt: true, remoteRevocationStatus: true },
      }),
    ).resolves.toEqual([
      {
        status: AgentConnectionStatus.REVOKED,
        revokedAt: now,
        remoteRevocationStatus: AgentRemoteRevocationStatus.PENDING,
      },
      {
        status: AgentConnectionStatus.REVOKED,
        revokedAt: now,
        remoteRevocationStatus: AgentRemoteRevocationStatus.PENDING,
      },
    ]);
  });

  it("keeps remote revocation tombstones after account data cascades", async () => {
    const fixture = await createConnection("delete");
    await disableAgentAccessForAccountDeletion({
      userId: fixture.userId,
      now: new Date("2026-08-13T16:00:00.000Z"),
    });
    await prisma.user.delete({ where: { id: fixture.userId } });

    await expect(
      prisma.agentRevocationOutbox.findUniqueOrThrow({
        where: { connectionId: fixture.connection.id },
      }),
    ).resolves.toMatchObject({
      userId: fixture.userId,
      workosUserId: fixture.identity.workosUserId,
      applicationId: fixture.connection.workosApplicationId,
    });
  });

  it("enforces owned source links and exports safe agent provenance", async () => {
    const owner = await createConnection("owner");
    const other = await createConnection("other");
    const operation = await prisma.agentSkillOperation.create({
      data: {
        userId: owner.userId,
        connectionId: owner.connection.id,
        kind: AgentOperationKind.SPEC_BATCH,
        toolName: "skills.add_from_specs",
        status: AgentOperationStatus.SUCCEEDED,
        idempotencyKey: `idempotency-${runId}`,
        payloadHash: "a".repeat(64),
        requestedCount: 1,
        activeCount: 1,
        items: {
          create: {
            ordinal: 0,
            clientReference: "skill-1",
            proposedTitle: "A safe exported skill",
            proposedObjective: "Recall the bounded provenance contract.",
            status: "ACTIVE",
          },
        },
      },
    });
    const foreignSource = await prisma.sourceFile.create({
      data: { userId: other.userId, originalName: "private.pdf" },
    });
    await expect(
      prisma.agentOperationSource.create({
        data: {
          userId: owner.userId,
          operationId: operation.id,
          sourceFileId: foreignSource.id,
          ordinal: 0,
        },
      }),
    ).rejects.toBeDefined();

    const exported = await getUserDataExport({
      userId: owner.userId,
      generatedAt: new Date("2026-08-13T17:00:00.000Z"),
    });
    expect(exported.status).toBe("ready");
    if (exported.status !== "ready") return;
    expect(exported.export.agentConnections).toHaveLength(1);
    expect(exported.export.agentConnections[0]).not.toHaveProperty("workosSubject");
    expect(exported.export.agentOperations).toEqual([
      expect.objectContaining({ id: operation.id, payloadHash: "a".repeat(64) }),
    ]);
    expect(exported.export.agentOperations[0]).not.toHaveProperty("requestPayload");
    expect(exported.export.agentOperationItems).toEqual([
      expect.objectContaining({ proposedTitle: "A safe exported skill" }),
    ]);
  });

  it("replays identical operation keys and rejects changed bodies", async () => {
    const fixture = await createConnection("idempotency");
    const auth = {
      userId: fixture.userId,
      connectionId: fixture.connection.id,
      subject: fixture.identity.externalId,
      sessionId: fixture.connection.workosSessionId,
      clientId: fixture.connection.clientId,
      clientName: fixture.connection.clientName,
      clientDomain: fixture.connection.clientDomain,
      resourceUrl: fixture.connection.resourceUrl,
      expiresAt: Math.floor(Date.now() / 1_000) + 300,
      scopes: ["skills:create" as const],
    };
    const request = {
      idempotency_key: "stable-request-key",
      items: [
        {
          client_reference: "item-1",
          skill: {
            title: "Binary search boundaries",
            objective: "Choose the correct binary-search boundary update.",
            rules: [],
            examples: [],
            exerciseConstraints: "",
            tags: ["algorithms"],
          },
        },
      ],
    };

    vi.mocked(sendAgentSkillOperationRequested).mockClear();
    const concurrent = await Promise.all(
      Array.from({ length: 4 }, () => createAgentSpecOperation(auth, request)),
    );
    const [first] = concurrent;
    expect(new Set(concurrent.map((result) => result.operation_id))).toEqual(
      new Set([first.operation_id]),
    );
    await expect(
      createAgentSpecOperation(auth, {
        ...request,
        items: [{ ...request.items[0], client_reference: "changed-item" }],
      }),
    ).rejects.toMatchObject({ code: "idempotency_conflict" });
    await expect(
      prisma.agentSkillOperation.count({
        where: { connectionId: fixture.connection.id, toolName: "skills.add_from_specs" },
      }),
    ).resolves.toBe(1);
    expect(sendAgentSkillOperationRequested).toHaveBeenCalledTimes(1);
  });

  it("rate-limits material reads through the shared connection bucket", async () => {
    const fixture = await createConnection("material-read-limit");
    const auth = {
      userId: fixture.userId,
      connectionId: fixture.connection.id,
      subject: fixture.identity.externalId,
      sessionId: fixture.connection.workosSessionId,
      clientId: fixture.connection.clientId,
      clientName: fixture.connection.clientName,
      clientDomain: fixture.connection.clientDomain,
      resourceUrl: fixture.connection.resourceUrl,
      expiresAt: Math.floor(Date.now() / 1_000) + 300,
      scopes: ["materials:read" as const],
    };

    await prisma.agentRateLimitBucket.create({
      data: {
        userId: fixture.userId,
        connectionId: fixture.connection.id,
        kind: AgentRateLimitKind.READ,
        windowStart: new Date(Math.floor(Date.now() / 60_000) * 60_000),
        count: 60,
      },
    });
    await expect(listAgentMaterials(auth, {})).rejects.toMatchObject({
      code: "rate_limited",
    });
  });

  it("preserves a material request while recording its clarification", async () => {
    const fixture = await createConnection("material-clarification");
    const operation = await prisma.agentSkillOperation.create({
      data: {
        userId: fixture.userId,
        connectionId: fixture.connection.id,
        kind: AgentOperationKind.MATERIAL_BATCH,
        toolName: "skills.add_from_material",
        status: AgentOperationStatus.NEEDS_INPUT,
        idempotencyKey: `clarification-${runId}`,
        payloadHash: "e".repeat(64),
        requestPayload: {
          instruction: "Cover the proof techniques in this chapter.",
          maxSkills: 3,
        },
        requestedCount: 3,
      },
    });
    const auth = {
      userId: fixture.userId,
      connectionId: fixture.connection.id,
      subject: fixture.identity.externalId,
      sessionId: fixture.connection.workosSessionId,
      clientId: fixture.connection.clientId,
      clientName: fixture.connection.clientName,
      clientDomain: fixture.connection.clientDomain,
      resourceUrl: fixture.connection.resourceUrl,
      expiresAt: Math.floor(Date.now() / 1_000) + 300,
      scopes: ["skills:create" as const],
    };

    await continueAgentOperation(auth, {
      operation_id: operation.id,
      idempotency_key: "continue-material-request",
      instruction: "Focus on induction and contradiction.",
    });

    await expect(
      prisma.agentSkillOperation.findUniqueOrThrow({
        where: { id: operation.id },
        select: { requestPayload: true },
      }),
    ).resolves.toEqual({
      requestPayload: {
        instruction: "Cover the proof techniques in this chapter.",
        clarification: "Focus on induction and contradiction.",
        maxSkills: 3,
      },
    });
  });

  it("clears an unusable explicit section selection before requesting scope clarification", async () => {
    const fixture = await createConnection("material-scope-clarification");
    const { revision } = await createMaterialWithInitialRevision({
      userId: fixture.userId,
      title: "Scope clarification fixture",
      kind: StudyMaterialKind.PDF,
    });
    const section = await prisma.materialSection.create({
      data: {
        userId: fixture.userId,
        materialRevisionId: revision.id,
        ordinal: 0,
        level: 1,
        title: "Selected section",
        normalizedTitle: "selected section",
        headingPath: ["Selected section"],
      },
    });
    const operation = await prisma.agentSkillOperation.create({
      data: {
        userId: fixture.userId,
        connectionId: fixture.connection.id,
        kind: AgentOperationKind.MATERIAL_BATCH,
        toolName: "skills.add_from_material",
        status: AgentOperationStatus.PLANNING,
        idempotencyKey: `material-scope-clarification-${runId}`,
        payloadHash: "f".repeat(64),
        materialRevisionId: revision.id,
        requestPayload: {
          instruction: "Create one skill from this section.",
          sectionIds: [section.id],
          maxSkills: 1,
        },
        requestedCount: 1,
      },
    });
    const planningResult = {
      status: "needs-scope" as const,
      batchId: `${runId}_scope_clarification_batch`,
      plan: materialScopeResolutionSchema.parse({
        version: 1,
        materialRevisionId: revision.id,
        instruction: "Create one skill from this section.",
        resolutionStatus: "ambiguous",
        resolvedScopeLabel: "The selected section does not identify instructional content.",
        warnings: [],
        clarification: "Select an instructional section.",
        items: [],
      }),
    };
    const planSpy = vi.spyOn(materialBatches, "planMaterialSkills").mockResolvedValue(planningResult);

    try {
      await expect(
        runAgentSkillOperationJob({
          userId: fixture.userId,
          operationId: operation.id,
          now: new Date("2026-09-23T12:00:00.000Z"),
        }),
      ).resolves.toMatchObject({ status: "processed", operationId: operation.id });
      expect(planSpy).toHaveBeenCalledWith(
        expect.objectContaining({ input: expect.objectContaining({ sectionIds: [section.id] }) }),
      );
      await expect(
        prisma.agentSkillOperation.findUniqueOrThrow({
          where: { id: operation.id },
          select: { status: true, requestPayload: true },
        }),
      ).resolves.toEqual({
        status: AgentOperationStatus.NEEDS_INPUT,
        requestPayload: {
          instruction: "Create one skill from this section.",
          originalSectionIds: [section.id],
          maxSkills: 1,
          materialBatchId: planningResult.batchId,
        },
      });
    } finally {
      planSpy.mockRestore();
    }
  });

  it("cancels expired upload operations and removes their draft sources", async () => {
    const fixture = await createConnection("upload-expiry");
    const source = await prisma.sourceFile.create({
      data: {
        userId: fixture.userId,
        kind: SourceFileKind.PDF,
        status: SourceFileStatus.DRAFT,
        originalName: "abandoned.pdf",
        mimeType: "application/pdf",
        byteSize: 1_024,
        storageBucket: "staging-private",
        storageKey: `agent-test/${runId}/abandoned.pdf`,
      },
    });
    const operation = await prisma.agentSkillOperation.create({
      data: {
        userId: fixture.userId,
        connectionId: fixture.connection.id,
        kind: AgentOperationKind.QUICK_FILES,
        toolName: "skills.prepare_files",
        status: AgentOperationStatus.AWAITING_UPLOAD,
        idempotencyKey: `upload-expiry-${runId}`,
        payloadHash: "f".repeat(64),
        requestedCount: 1,
        createdAt: new Date("2026-08-13T10:00:00.000Z"),
        items: { create: { ordinal: 0, clientReference: "files-1" } },
        sources: { create: { sourceFileId: source.id, ordinal: 0 } },
      },
      include: { items: true },
    });

    await expect(
      runAgentAccessMaintenance(new Date("2026-08-13T10:11:00.000Z")),
    ).resolves.toMatchObject({ expiredUploadOperations: 1 });
    await expect(
      prisma.agentSkillOperation.findUniqueOrThrow({ where: { id: operation.id } }),
    ).resolves.toMatchObject({
      status: AgentOperationStatus.CANCELED,
      errorCode: "UPLOAD_WINDOW_EXPIRED",
    });
    await expect(
      prisma.agentSkillOperationItem.findUniqueOrThrow({ where: { id: operation.items[0].id } }),
    ).resolves.toMatchObject({
      status: AgentOperationItemStatus.CANCELED,
      errorCode: "UPLOAD_WINDOW_EXPIRED",
    });
    await expect(prisma.sourceFile.findUnique({ where: { id: source.id } })).resolves.toBeNull();
  });

  it("retries material planning timeouts through the bounded delivery attempts", async () => {
    const fixture = await createConnection("material-planning-timeout");
    const { revision } = await createMaterialWithInitialRevision({
      userId: fixture.userId,
      title: "Spanish material planning timeout",
      kind: StudyMaterialKind.PDF,
    });
    const operation = await prisma.agentSkillOperation.create({
      data: {
        userId: fixture.userId,
        connectionId: fixture.connection.id,
        kind: AgentOperationKind.MATERIAL_BATCH,
        toolName: "skills.add_from_material",
        status: AgentOperationStatus.QUEUED,
        idempotencyKey: `material-planning-timeout-${runId}`,
        payloadHash: "m".repeat(64),
        requestedCount: 0,
        materialRevisionId: revision.id,
        requestPayload: { instruction: "Create one skill from Spanish pronouns.", maxSkills: 1 },
      },
    });
    const plan = vi.spyOn(materialBatches, "planMaterialSkills").mockRejectedValue(
      new JobStageTimeoutError("Scope planning timed out.", "material scope planning"),
    );
    try {
      for (const attempt of [0, 1]) {
        await expect(runAgentSkillOperationJob({
          userId: fixture.userId,
          operationId: operation.id,
          deliveryAttempt: { attempt, maxAttempts: 3 },
        })).rejects.toMatchObject({ retryable: true });
        await expect(
          prisma.agentSkillOperation.findUniqueOrThrow({ where: { id: operation.id } }),
        ).resolves.toMatchObject({
          status: AgentOperationStatus.QUEUED,
          errorCode: null,
          completedAt: null,
        });
      }
      await expect(runAgentSkillOperationJob({
        userId: fixture.userId,
        operationId: operation.id,
        deliveryAttempt: { attempt: 2, maxAttempts: 3 },
      })).rejects.toMatchObject({ retryable: false });
      await expect(
        prisma.agentSkillOperation.findUniqueOrThrow({ where: { id: operation.id } }),
      ).resolves.toMatchObject({
        status: AgentOperationStatus.FAILED,
        errorCode: "MATERIAL_PLANNING_TIMEOUT_RETRIES_EXHAUSTED",
      });
      expect(plan).toHaveBeenCalledTimes(3);
      expect(plan).toHaveBeenCalledWith(expect.objectContaining({
        preservePlanningOnTimeout: true,
        deadlineAt: expect.any(Date),
      }));
    } finally {
      plan.mockRestore();
    }
  }, 60_000);

  it("fails maintenance when recovery continuation publication fails", async () => {
    const fixture = await createConnection("maintenance-continuation-publish-failure");
    const queued = await createQueuedDraftOperation(fixture, "publish-failure");
    vi.mocked(sendAgentSkillOperationRequested).mockRejectedValueOnce(
      new Error("JOB_PUBLISH_FAILED"),
    );

    await expect(runAgentAccessMaintenance(new Date())).rejects.toMatchObject({
      message: "Agent access maintenance could not recover activation items.",
      retryable: true,
    });
    await expect(
      prisma.agentSkillOperationItem.findUniqueOrThrow({ where: { id: queued.item.id } }),
    ).resolves.toMatchObject({ status: AgentOperationItemStatus.QUEUED });
  });

  it("sends continuation-limit failures from maintenance to the permanent worker path", async () => {
    const continuationLimitError = new JobContinuationLimitError("depth");
    const recovery = vi
      .spyOn(agentAccessRecovery, "recoverStaleAgentOperationItems")
      .mockRejectedValueOnce(continuationLimitError);

    try {
      await expect(runAgentAccessMaintenance(new Date())).rejects.toBe(
        continuationLimitError,
      );
    } finally {
      recovery.mockRestore();
    }
  });

  it("does not let awaiting-upload operations starve the bounded continuation scan", async () => {
    const fixture = await createConnection("continuation-scan-capacity");
    const staleCreatedAt = new Date("2026-08-13T10:00:00.000Z");
    for (let index = 0; index < 30; index += 1) {
      await prisma.agentSkillOperation.create({
        data: {
          userId: fixture.userId,
          connectionId: fixture.connection.id,
          kind: AgentOperationKind.QUICK_FILES,
          toolName: "skills.prepare_files",
          status: AgentOperationStatus.AWAITING_UPLOAD,
          idempotencyKey: `awaiting-upload-${index}-${runId}`,
          payloadHash: `${index}`.padStart(64, "a"),
          requestedCount: 1,
          createdAt: staleCreatedAt,
          updatedAt: staleCreatedAt,
          items: {
            create: {
              ordinal: 0,
              clientReference: `awaiting-upload-${index}`,
              status: AgentOperationItemStatus.QUEUED,
              createdAt: staleCreatedAt,
              updatedAt: staleCreatedAt,
            },
          },
        },
      });
    }
    const eligible = await createQueuedDraftOperation(fixture, "continuation-after-upload-backlog");
    vi.mocked(sendAgentSkillOperationRequested).mockClear();

    await expect(recoverStaleAgentOperationItems({
      userId: fixture.userId,
      now: new Date(),
    })).resolves.toMatchObject({ continuations: 1 });

    expect(sendAgentSkillOperationRequested).toHaveBeenCalledTimes(1);
    expect(sendAgentSkillOperationRequested).toHaveBeenCalledWith(
      expect.objectContaining({ operationId: eligible.operation.id, userId: fixture.userId }),
      expect.objectContaining({ eventId: expect.stringMatching(/^agent-op-[a-f0-9]{40}$/) }),
    );
  }, 60_000);

  it("finishes expiry and revocation before retrying failed refill recovery", async () => {
    const fixture = await createConnection("maintenance-recovery-failure");
    const now = new Date("2026-08-13T10:11:00.000Z");
    // Earlier cases intentionally leave durable outbox rows for their own
    // assertions. Remove only this suite's earlier rows so the bounded
    // maintenance batch tests this fixture deterministically.
    await prisma.agentRevocationOutbox.deleteMany({
      where: { userId: { in: userIds.filter((userId) => userId !== fixture.userId) } },
    });
    const source = await prisma.sourceFile.create({
      data: {
        userId: fixture.userId,
        kind: SourceFileKind.PDF,
        status: SourceFileStatus.DRAFT,
        originalName: "recovery-failure.pdf",
        mimeType: "application/pdf",
        byteSize: 1_024,
        storageBucket: "staging-private",
        storageKey: `agent-test/${runId}/recovery-failure.pdf`,
      },
    });
    const operation = await prisma.agentSkillOperation.create({
      data: {
        userId: fixture.userId,
        connectionId: fixture.connection.id,
        kind: AgentOperationKind.QUICK_FILES,
        toolName: "skills.prepare_files",
        status: AgentOperationStatus.AWAITING_UPLOAD,
        idempotencyKey: `recovery-failure-${runId}`,
        payloadHash: "r".repeat(64),
        requestedCount: 1,
        createdAt: new Date("2026-08-13T10:00:00.000Z"),
        items: { create: { ordinal: 0, clientReference: "files-1" } },
        sources: { create: { sourceFileId: source.id, ordinal: 0 } },
      },
      include: { items: true },
    });
    await prisma.agentRevocationOutbox.create({
      data: {
        userId: fixture.userId,
        connectionId: fixture.connection.id,
        workosUserId: fixture.identity.workosUserId,
        applicationId: fixture.connection.workosApplicationId,
        status: AgentRevocationOutboxStatus.PENDING,
        nextAttemptAt: now,
      },
    });

    const recovery = vi
      .spyOn(refillJobs, "recoverPendingRefillEvents")
      .mockRejectedValueOnce(new Error("private database failure"));
    const fetch = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValue(new Response(null, { status: 404 }));
    // The CI database gate intentionally does not provision WorkOS credentials.
    // Supply a synthetic key so maintenance reaches the mocked remote delete.
    vi.stubEnv("WORKOS_API_KEY", "sk_test_agent_maintenance");
    try {
      await expect(runAgentAccessMaintenance(now)).rejects.toMatchObject({
        message: "Agent access maintenance could not recover refill events.",
        retryable: true,
      });
      expect(recovery).toHaveBeenCalledWith({ now });
      expect(fetch).toHaveBeenCalledTimes(1);
      await expect(
        prisma.agentSkillOperation.findUniqueOrThrow({ where: { id: operation.id } }),
      ).resolves.toMatchObject({
        status: AgentOperationStatus.CANCELED,
        errorCode: "UPLOAD_WINDOW_EXPIRED",
      });
      await expect(prisma.sourceFile.findUnique({ where: { id: source.id } })).resolves.toBeNull();
      await expect(
        prisma.agentRevocationOutbox.findUniqueOrThrow({ where: { connectionId: fixture.connection.id } }),
      ).resolves.toMatchObject({ status: AgentRevocationOutboxStatus.SUCCEEDED });
    } finally {
      vi.unstubAllEnvs();
      recovery.mockRestore();
      fetch.mockRestore();
    }
  });

  it("times out a stuck verifier, persists a retry, and lets a same-user sibling finish", async () => {
    const fixture = await createConnection("verifier-deadline");
    const stalled = await createQueuedDraftOperation(fixture, "stalled-verifier");
    const siblings = await Promise.all(
      ["healthy-sibling-a", "healthy-sibling-b", "healthy-sibling-c"]
        .map((label) => createQueuedDraftOperation(fixture, label)),
    );
    let verifierAborted = false;
    const deadlineAt = new Date(Date.now() + 120_000);
    const startedAt = Date.now();

    await expect(runAgentSkillOperationJob({
      userId: fixture.userId,
      operationId: stalled.operation.id,
      deadlineAt,
    }, {
      activationOptions: quickActivationOptions(({ signal }) => new Promise((_, reject) => {
        signal?.addEventListener("abort", () => {
          verifierAborted = signal.aborted;
          reject(signal.reason);
        }, { once: true });
      })),
    })).resolves.toMatchObject({ status: "processed" });

    expect(verifierAborted).toBe(true);
    await expect(
      prisma.agentSkillOperationItem.findUniqueOrThrow({ where: { id: stalled.item.id } }),
    ).resolves.toMatchObject({
      status: AgentOperationItemStatus.QUEUED,
      errorCode: "TRANSIENT_WORKER_FAILURE",
      retryCount: 1,
      activationReservedAt: null,
      workerClaimToken: null,
    });
    await expect(
      prisma.generationJob.findFirstOrThrow({ where: { skillId: stalled.skill.id } }),
    ).resolves.toMatchObject({
      status: GenerationJobStatus.FAILED,
      stage: GenerationJobStage.FAILED,
      failureCategory: GenerationFailureCategory.TIMEOUT,
    });
    await expect(prisma.exercise.count({ where: { skillId: stalled.skill.id } })).resolves.toBe(0);

    for (const sibling of siblings) {
      await expect(runAgentSkillOperationJob({
        userId: fixture.userId,
        operationId: sibling.operation.id,
        deadlineAt: new Date(Date.now() + 120_000),
      }, { activationOptions: quickActivationOptions() })).resolves.toMatchObject({ status: "processed" });
      await expect(prisma.skill.findUniqueOrThrow({ where: { id: sibling.skill.id } }))
        .resolves.toMatchObject({ status: SkillStatus.ACTIVE, firstIntroducedAt: null });
      await expect(
        prisma.agentSkillOperationItem.findUniqueOrThrow({ where: { id: sibling.item.id } }),
      ).resolves.toMatchObject({ status: AgentOperationItemStatus.ACTIVE, resultSkillId: sibling.skill.id });
      await expect(prisma.exercise.count({ where: { skillId: sibling.skill.id } })).resolves.toBe(3);
      await expect(prisma.exerciseAttempt.count({ where: { skillId: sibling.skill.id } })).resolves.toBe(0);
    }
    expect(Date.now() - startedAt).toBeLessThan(180_000);
  }, 180_000);

  it.each(["noncooperative", "cooperative"] as const)(
    "bounds a %s source read, then records truthful retry completion times",
    async (abortBehavior) => {
      const fixture = await createConnection(`source-read-deadline-${abortBehavior}`);
      const stalled = await createQueuedDraftOperation(fixture, `stalled-source-read-${abortBehavior}`);
      const source = await prisma.sourceFile.create({
        data: {
          userId: fixture.userId,
          kind: SourceFileKind.IMAGE,
          status: SourceFileStatus.READY,
          originalName: "source-read-test.png",
          mimeType: "image/png",
          byteSize: 4,
          storageBucket: "staging-private",
          storageKey: `agent-test/${runId}/source-read-test.png`,
        },
      });
      await prisma.skillSourceRef.create({
        data: { userId: fixture.userId, skillId: stalled.skill.id, sourceFileId: source.id },
      });

      let sourceAborted = false;
      const started = Date.now();
      await expect(runAgentSkillOperationJob({
        userId: fixture.userId,
        operationId: stalled.operation.id,
        deadlineAt: new Date(Date.now() + 120_000),
      }, {
        activationOptions: {
          ...quickActivationOptions(),
          sourceMediaLoader: ({ signal }) => new Promise((_, reject) => {
            signal?.addEventListener("abort", () => {
              sourceAborted = true;
              if (abortBehavior === "cooperative") reject(signal.reason);
            }, { once: true });
          }),
        },
      })).resolves.toMatchObject({ status: "processed" });
      expect(Date.now() - started).toBeLessThan(10_000);
      expect(sourceAborted).toBe(true);
      const firstJob = await prisma.generationJob.findFirstOrThrow({
        where: { skillId: stalled.skill.id },
      });
      expect(firstJob).toMatchObject({
        status: GenerationJobStatus.FAILED,
        failureCategory: GenerationFailureCategory.TIMEOUT,
      });
      expect(firstJob.errorMessage).toContain("source-evidence timed out");
      await expect(prisma.agentSkillOperationItem.findUniqueOrThrow({ where: { id: stalled.item.id } }))
        .resolves.toMatchObject({
          status: AgentOperationItemStatus.QUEUED,
          errorCode: "TRANSIENT_WORKER_FAILURE",
          retryCount: 1,
          createdSkillId: stalled.skill.id,
        });
      const retryItem = await prisma.agentSkillOperationItem.findUniqueOrThrow({ where: { id: stalled.item.id } });
      expect(retryItem.updatedAt.getTime()).toBeGreaterThanOrEqual(started + 200);
      await expect(prisma.exercise.count({ where: { skillId: stalled.skill.id } })).resolves.toBe(0);

      await prisma.agentSkillOperationItem.update({
        where: { id: stalled.item.id },
        data: { updatedAt: new Date(Date.now() - 31_000) },
      });
      await expect(runAgentSkillOperationJob({
        userId: fixture.userId,
        operationId: stalled.operation.id,
        deadlineAt: new Date(Date.now() + 120_000),
      }, {
        activationOptions: {
          ...quickActivationOptions(),
          sourceMediaLoader: async () => [],
        },
      })).resolves.toMatchObject({ status: "processed" });

      const jobs = await prisma.generationJob.findMany({
        where: { skillId: stalled.skill.id },
        orderBy: { createdAt: "asc" },
      });
      expect(jobs).toHaveLength(2);
      expect(jobs[1].status).toBe(GenerationJobStatus.SUCCEEDED);
      expect(jobs[1].completedAt!.getTime()).toBeGreaterThanOrEqual(jobs[1].createdAt.getTime());
      const completedOperation = await prisma.agentSkillOperation.findUniqueOrThrow({
        where: { id: stalled.operation.id },
      });
      expect(completedOperation.status).toBe(AgentOperationStatus.SUCCEEDED);
      expect(completedOperation.completedAt!.getTime()).toBeGreaterThanOrEqual(jobs[1].createdAt.getTime());
      await expect(prisma.exercise.count({ where: { skillId: stalled.skill.id } })).resolves.toBe(3);
      await expect(prisma.skill.findUniqueOrThrow({ where: { id: stalled.skill.id } }))
        .resolves.toMatchObject({ status: SkillStatus.ACTIVE, firstIntroducedAt: null });
      await expect(prisma.exerciseAttempt.count({ where: { skillId: stalled.skill.id } })).resolves.toBe(0);
    }, 30_000);

  it("fences a claimed generation job when activation throws before recording failure", async () => {
    const fixture = await createConnection("claimed-job-exception");
    const stalled = await createQueuedDraftOperation(fixture, "claimed-job-exception");
    let claimedJobId: string | undefined;

    await expect(runAgentSkillOperationJob({
      userId: fixture.userId,
      operationId: stalled.operation.id,
      deadlineAt: new Date(Date.now() + 120_000),
    }, {
      activateDraft: async (input) => {
        const job = await prisma.generationJob.create({
          data: {
            userId: input.userId,
            skillId: input.skillId,
            kind: GenerationJobKind.SKILL_ACTIVATION,
            status: GenerationJobStatus.RUNNING,
            stage: GenerationJobStage.GENERATING,
            checkpoint: "claimed",
            provider: "test",
            model: "test",
            promptVersion: "test",
            requestedCount: 3,
          },
        });
        claimedJobId = job.id;
        input.onGenerationJobClaimed?.(job.id);
        throw new Error("injected failure after generation job claim");
      },
    })).rejects.toMatchObject({ retryable: true });

    const failedJob = await prisma.generationJob.findUniqueOrThrow({ where: { id: claimedJobId } });
    expect(failedJob).toMatchObject({
      status: GenerationJobStatus.FAILED,
      stage: GenerationJobStage.FAILED,
      checkpoint: "worker-exception",
    });
    expect(failedJob.completedAt!.getTime()).toBeGreaterThanOrEqual(failedJob.createdAt.getTime());
    await expect(prisma.agentSkillOperationItem.findUniqueOrThrow({ where: { id: stalled.item.id } }))
      .resolves.toMatchObject({
        status: AgentOperationItemStatus.QUEUED,
        errorCode: "TRANSIENT_WORKER_FAILURE",
        retryCount: 1,
        createdSkillId: stalled.skill.id,
      });

    await prisma.agentSkillOperationItem.update({
      where: { id: stalled.item.id },
      data: { updatedAt: new Date(Date.now() - 31_000) },
    });
    await expect(runAgentSkillOperationJob({
      userId: fixture.userId,
      operationId: stalled.operation.id,
      deadlineAt: new Date(Date.now() + 120_000),
    }, { activationOptions: quickActivationOptions() })).resolves.toMatchObject({ status: "processed" });
    await expect(prisma.generationJob.count({ where: { skillId: stalled.skill.id } })).resolves.toBe(2);
    await expect(prisma.exercise.count({ where: { skillId: stalled.skill.id } })).resolves.toBe(3);
    await expect(prisma.skill.findUniqueOrThrow({ where: { id: stalled.skill.id } }))
      .resolves.toMatchObject({ status: SkillStatus.ACTIVE, firstIntroducedAt: null });
  }, 30_000);

  it("retries when source OCR is still owned by another worker", async () => {
    const fixture = await createConnection("pending-source-ocr");
    const stalled = await createQueuedDraftOperation(fixture, "pending-source-ocr");
    const source = await prisma.sourceFile.create({
      data: {
        userId: fixture.userId,
        kind: SourceFileKind.IMAGE,
        status: SourceFileStatus.READY,
        originalName: "pending-source.png",
        mimeType: "image/png",
        byteSize: 4,
        storageBucket: "staging-private",
        storageKey: `agent-test/${runId}/pending-source.png`,
      },
    });
    await prisma.skillSourceRef.create({
      data: { userId: fixture.userId, skillId: stalled.skill.id, sourceFileId: source.id },
    });

    await expect(runAgentSkillOperationJob({
      userId: fixture.userId,
      operationId: stalled.operation.id,
      deadlineAt: new Date(Date.now() + 120_000),
    }, {
      activationOptions: {
        ...quickActivationOptions(),
        sourceMediaLoader: async () => { throw new MaterialEvidencePendingError(); },
      },
    })).resolves.toMatchObject({ status: "processed" });
    await expect(prisma.agentSkillOperationItem.findUniqueOrThrow({ where: { id: stalled.item.id } }))
      .resolves.toMatchObject({
        status: AgentOperationItemStatus.QUEUED,
        errorCode: "TRANSIENT_WORKER_FAILURE",
        retryCount: 1,
        createdSkillId: stalled.skill.id,
      });
    await expect(prisma.generationJob.findFirstOrThrow({ where: { skillId: stalled.skill.id } }))
      .resolves.toMatchObject({ status: GenerationJobStatus.FAILED });
  }, 30_000);

  it("recovers a candidate activation whose publication transaction times out", async () => {
    const fixture = await createConnection("publishing-deadline");
    const stalled = await createQueuedDraftOperation(fixture, "candidate-publishing");
    const sibling = await createQueuedDraftOperation(fixture, "publishing-sibling");
    await prisma.agentExerciseCandidate.create({
      data: {
        userId: fixture.userId,
        operationItemId: stalled.item.id,
        ordinal: 0,
        kind: AnswerKind.CHOICE,
        status: AgentCandidateStatus.VERIFIED,
        normalizedPayload: {
          candidateId: "candidate-publishing-1",
          clientReference: "candidate-publishing-1",
          type: ExerciseType.MULTIPLE_CHOICE,
          answerKind: AnswerKind.CHOICE,
          prompt: "Choose the correct example that was verified before publication.",
          choices: [
            { id: "correct", label: "The verified answer" },
            { id: "wrong", label: "An incorrect answer" },
          ],
          answerSpec: { kind: "choice", correctChoiceId: "correct" },
          correctAnswerDisplay: "The verified answer",
          explanation: "The verified candidate must publish atomically with the skill.",
          difficulty: 2,
          expectedSeconds: 20,
        },
      },
    });

    const originalTransaction = prisma.$transaction.bind(prisma) as unknown as (
      operation: unknown,
      options?: { maxWait?: number; timeout?: number },
    ) => Promise<unknown>;
    const transactionSpy = vi.spyOn(prisma, "$transaction");
    let publicationTimedOut = false;
    let sawPublishingStage = false;
    transactionSpy.mockImplementation((async (...args: unknown[]) => {
      const operation = args[0];
      const options = args[1] as { maxWait?: number; timeout?: number } | undefined;
      if (!publicationTimedOut && typeof operation === "function" && options?.timeout === 5_000) {
        const generationJob = await prisma.generationJob.findFirst({
          where: { skillId: stalled.skill.id },
          orderBy: { createdAt: "desc" },
          select: { stage: true },
        });
        if (generationJob?.stage === GenerationJobStage.PUBLISHING) {
          publicationTimedOut = true;
          sawPublishingStage = true;
          return (async () => {
            await new Promise((resolve) => setTimeout(resolve, 25));
            throw Object.assign(new Error("The interactive transaction expired."), { code: "P2028" });
          })();
        }
      }
      return originalTransaction(operation, options);
    }) as typeof prisma.$transaction);

    try {
      await expect(runAgentSkillOperationJob({
        userId: fixture.userId,
        operationId: stalled.operation.id,
        deadlineAt: new Date(Date.now() + 120_000),
      }, { activationOptions: quickActivationOptions() })).resolves.toMatchObject({ status: "processed" });
    } finally {
      transactionSpy.mockRestore();
    }

    expect(publicationTimedOut).toBe(true);
    expect(sawPublishingStage).toBe(true);
    await expect(
      prisma.agentSkillOperationItem.findUniqueOrThrow({ where: { id: stalled.item.id } }),
    ).resolves.toMatchObject({
      status: AgentOperationItemStatus.QUEUED,
      errorCode: "TRANSIENT_WORKER_FAILURE",
      retryCount: 1,
      activationReservedAt: null,
    });
    await expect(
      prisma.agentExerciseCandidate.findFirstOrThrow({ where: { operationItemId: stalled.item.id } }),
    ).resolves.toMatchObject({ status: AgentCandidateStatus.VERIFIED, exerciseId: null });
    await expect(
      prisma.generationJob.findFirstOrThrow({ where: { skillId: stalled.skill.id } }),
    ).resolves.toMatchObject({
      status: GenerationJobStatus.FAILED,
      stage: GenerationJobStage.FAILED,
      failureCategory: GenerationFailureCategory.TIMEOUT,
    });
    await expect(prisma.exercise.count({ where: { skillId: stalled.skill.id } })).resolves.toBe(0);

    await expect(runAgentSkillOperationJob({
      userId: fixture.userId,
      operationId: sibling.operation.id,
      deadlineAt: new Date(Date.now() + 120_000),
    }, { activationOptions: quickActivationOptions() })).resolves.toMatchObject({ status: "processed" });
    await expect(prisma.skill.findUniqueOrThrow({ where: { id: sibling.skill.id } }))
      .resolves.toMatchObject({ status: SkillStatus.ACTIVE, firstIntroducedAt: null });
    await expect(prisma.exercise.count({ where: { skillId: sibling.skill.id } })).resolves.toBe(3);
    await expect(prisma.exerciseAttempt.count({ where: { skillId: sibling.skill.id } })).resolves.toBe(0);
  }, 60_000);

  it("retries a delivery when its normal continuation publish fails", async () => {
    const fixture = await createConnection("worker-continuation-publish-retry");
    const first = await createQueuedDraftOperation(fixture, "continuation-publish-first");
    const siblingSkill = await prisma.skill.create({
      data: {
        userId: fixture.userId,
        title: "Continuation publish sibling",
        objective: "Practice resuming a queued sibling after a continuation publish failure.",
        status: SkillStatus.DRAFT,
      },
    });
    const sibling = await prisma.agentSkillOperationItem.create({
      data: {
        userId: fixture.userId,
        operationId: first.operation.id,
        ordinal: 1,
        clientReference: "continuation-publish-sibling",
        status: AgentOperationItemStatus.QUEUED,
        createdSkillId: siblingSkill.id,
      },
    });
    await prisma.agentSkillOperation.update({
      where: { id: first.operation.id },
      data: { requestedCount: 2 },
    });
    vi.mocked(sendAgentSkillOperationRequested).mockClear();
    vi.mocked(sendAgentSkillOperationRequested).mockRejectedValueOnce(
      new Error("JOB_PUBLISH_FAILED"),
    );

    await expect(runAgentSkillOperationJob({
      userId: fixture.userId,
      operationId: first.operation.id,
      deadlineAt: new Date(Date.now() + 120_000),
    }, { activationOptions: quickActivationOptions() })).rejects.toMatchObject({
      name: "AgentSkillWorkerError",
      message: "AGENT_CONTINUATION_PUBLISH_FAILED",
      retryable: true,
    });
    await expect(
      prisma.agentSkillOperationItem.findUniqueOrThrow({ where: { id: first.item.id } }),
    ).resolves.toMatchObject({ status: AgentOperationItemStatus.ACTIVE });
    await expect(
      prisma.agentSkillOperationItem.findUniqueOrThrow({ where: { id: sibling.id } }),
    ).resolves.toMatchObject({ status: AgentOperationItemStatus.QUEUED, retryCount: 0 });
    expect(sendAgentSkillOperationRequested).toHaveBeenCalledTimes(1);

    await expect(runAgentSkillOperationJob({
      userId: fixture.userId,
      operationId: first.operation.id,
      deadlineAt: new Date(Date.now() + 120_000),
    }, { activationOptions: quickActivationOptions() })).resolves.toMatchObject({ status: "processed" });
    await expect(
      prisma.agentSkillOperationItem.findUniqueOrThrow({ where: { id: sibling.id } }),
    ).resolves.toMatchObject({ status: AgentOperationItemStatus.ACTIVE, resultSkillId: siblingSkill.id });
    await expect(prisma.exercise.count({ where: { skillId: siblingSkill.id } })).resolves.toBe(3);
    await expect(prisma.exerciseAttempt.count({ where: { skillId: siblingSkill.id } })).resolves.toBe(0);
  }, 60_000);

  it("keeps continuation-limit failures permanent after operation reconciliation", async () => {
    const fixture = await createConnection("worker-continuation-limit");
    const first = await createQueuedDraftOperation(fixture, "continuation-limit-first");
    const siblingSkill = await prisma.skill.create({
      data: {
        userId: fixture.userId,
        title: "Continuation limit sibling",
        objective: "Practice resuming a sibling after the continuation limit is reached.",
        status: SkillStatus.DRAFT,
      },
    });
    const sibling = await prisma.agentSkillOperationItem.create({
      data: {
        userId: fixture.userId,
        operationId: first.operation.id,
        ordinal: 1,
        clientReference: "continuation-limit-sibling",
        status: AgentOperationItemStatus.QUEUED,
        createdSkillId: siblingSkill.id,
      },
    });
    await prisma.agentSkillOperation.update({
      where: { id: first.operation.id },
      data: { requestedCount: 2 },
    });
    const limitError = new JobContinuationLimitError("depth");
    vi.mocked(sendAgentSkillOperationRequested).mockClear().mockRejectedValueOnce(limitError);

    await expect(runAgentSkillOperationJob({
      userId: fixture.userId,
      operationId: first.operation.id,
      deadlineAt: new Date(Date.now() + 120_000),
    }, { activationOptions: quickActivationOptions() })).rejects.toBe(limitError);

    await expect(prisma.agentSkillOperation.findUniqueOrThrow({
      where: { id: first.operation.id },
    })).resolves.toMatchObject({ status: AgentOperationStatus.QUEUED, activeCount: 1 });
    await expect(prisma.agentSkillOperationItem.findUniqueOrThrow({
      where: { id: first.item.id },
    })).resolves.toMatchObject({ status: AgentOperationItemStatus.ACTIVE });
    await expect(prisma.agentSkillOperationItem.findUniqueOrThrow({
      where: { id: sibling.id },
    })).resolves.toMatchObject({ status: AgentOperationItemStatus.QUEUED, retryCount: 0 });
    expect(sendAgentSkillOperationRequested).toHaveBeenCalledOnce();
  }, 60_000);

  it("sweeps queued operations after an ambiguous continuation publish with one stable event ID", async () => {
    const fixture = await createConnection("continuation-recovery");
    const queued = await createQueuedDraftOperation(fixture, "ambiguous-continuation");
    const now = new Date();
    vi.mocked(sendAgentSkillOperationRequested).mockClear();

    await expect(recoverStaleAgentOperationItems({
      userId: fixture.userId,
      operationId: queued.operation.id,
      now,
    })).resolves.toMatchObject({ continuations: 1 });
    await expect(recoverStaleAgentOperationItems({
      userId: fixture.userId,
      operationId: queued.operation.id,
      now,
    })).resolves.toMatchObject({ continuations: 1 });

    expect(sendAgentSkillOperationRequested).toHaveBeenCalledTimes(2);
    const first = vi.mocked(sendAgentSkillOperationRequested).mock.calls[0];
    const second = vi.mocked(sendAgentSkillOperationRequested).mock.calls[1];
    expect(first[0]).toMatchObject({ userId: fixture.userId, operationId: queued.operation.id });
    expect(first[1]).toEqual({
      eventId: expect.stringMatching(/^agent-op-[a-f0-9]{40}$/),
      signal: expect.any(AbortSignal),
    });
    expect(second[1]).toMatchObject({ eventId: first[1]?.eventId });
    expect(second[1]?.signal).toBeInstanceOf(AbortSignal);

    await prisma.agentSkillOperation.update({
      where: { id: queued.operation.id },
      data: { updatedAt: new Date(Date.now() + 1_000) },
    });
    await recoverStaleAgentOperationItems({
      userId: fixture.userId,
      operationId: queued.operation.id,
      now: new Date(Date.now() + 1_000),
    });
    const third = vi.mocked(sendAgentSkillOperationRequested).mock.calls[2];
    expect(third[1]?.eventId).not.toBe(first[1]?.eventId);
  }, 60_000);

  it("reclaims interrupted text items and resumes their stored draft", async () => {
    const fixture = await createConnection("worker-recovery");
    const [source, staleDraft] = await Promise.all([
      prisma.sourceFile.create({
        data: {
          userId: fixture.userId,
          kind: SourceFileKind.TEXT,
          status: SourceFileStatus.READY,
          originalName: "agent text",
          extractedText: "A sufficiently long source passage for an interrupted text operation.",
        },
      }),
      prisma.skill.create({
        data: {
          userId: fixture.userId,
          title: "Interrupted draft",
          objective: "Practice recovering a previously created agent skill draft.",
          status: SkillStatus.ARCHIVED,
        },
      }),
    ]);
    const operation = await prisma.agentSkillOperation.create({
      data: {
        userId: fixture.userId,
        connectionId: fixture.connection.id,
        kind: AgentOperationKind.TEXT_SOURCE,
        toolName: "skills.add_from_text",
        status: AgentOperationStatus.ACTIVATING,
        idempotencyKey: `worker-recovery-${runId}`,
        payloadHash: "1".repeat(64),
        sourceFileId: source.id,
        requestedCount: 1,
        items: {
          create: {
            ordinal: 0,
            clientReference: "text-1",
            status: AgentOperationItemStatus.ACTIVATING,
            createdSkillId: staleDraft.id,
            activationReservedAt: new Date("2026-08-13T11:00:00.000Z"),
            workerClaimToken: "interrupted-token",
            workerClaimedAt: new Date("2026-08-13T11:00:00.000Z"),
            updatedAt: new Date("2026-08-13T11:00:00.000Z"),
          },
        },
      },
      include: { items: true },
    });

    const recoveryNow = new Date("2026-08-13T11:12:00.000Z");
    await expect(
      recoverStaleAgentOperationItems({
        userId: fixture.userId,
        operationId: operation.id,
        now: recoveryNow,
      }),
    ).resolves.toMatchObject({ requeued: 1, continuations: 1 });
    await expect(
      runAgentSkillOperationJob({
        userId: fixture.userId,
        operationId: operation.id,
        now: recoveryNow,
      }),
    ).resolves.toMatchObject({ status: "processed" });
    await expect(
      prisma.agentSkillOperationItem.findUniqueOrThrow({ where: { id: operation.items[0].id } }),
    ).resolves.toMatchObject({
      status: AgentOperationItemStatus.FAILED,
      createdSkillId: staleDraft.id,
      errorCode: "DRAFT_NOT_FOUND",
      activationReservedAt: null,
    });
  });

  it("fences stale activation recovery and preserves fresh generation work", async () => {
    const staleFixture = await createConnection("stale-activation");
    const staleNow = new Date("2026-08-13T12:00:00.000Z");
    const staleClaimedAt = new Date("2026-08-13T11:45:00.000Z");
    const staleSkill = await prisma.skill.create({
      data: {
        userId: staleFixture.userId,
        title: "Stale recovery draft",
        objective: "Practice a draft whose activation worker stopped.",
        status: SkillStatus.DRAFT,
      },
    });
    const staleJob = await prisma.generationJob.create({
      data: {
        userId: staleFixture.userId,
        skillId: staleSkill.id,
        kind: GenerationJobKind.SKILL_ACTIVATION,
        status: GenerationJobStatus.RUNNING,
        stage: GenerationJobStage.GENERATING,
        provider: "google",
        model: "recovery-test-gemini",
        promptVersion: "recovery-test-v1",
        requestedCount: 3,
        startedAt: staleClaimedAt,
        updatedAt: staleClaimedAt,
      },
    });
    const staleOperation = await prisma.agentSkillOperation.create({
      data: {
        userId: staleFixture.userId,
        connectionId: staleFixture.connection.id,
        kind: AgentOperationKind.SPEC_BATCH,
        toolName: "skills.add_from_specs",
        status: AgentOperationStatus.ACTIVATING,
        idempotencyKey: `stale-activation-${runId}`,
        payloadHash: "2".repeat(64),
        requestedCount: 1,
        items: {
          create: {
            ordinal: 0,
            clientReference: "stale-activation-item",
            status: AgentOperationItemStatus.ACTIVATING,
            createdSkillId: staleSkill.id,
            activationReservedAt: staleClaimedAt,
            workerClaimToken: "stale-worker-token",
            workerClaimedAt: staleClaimedAt,
            updatedAt: staleClaimedAt,
          },
        },
      },
      include: { items: true },
    });

    vi.mocked(sendAgentSkillOperationRequested).mockClear();
    await expect(
      recoverStaleAgentOperationItems({
        userId: staleFixture.userId,
        operationId: staleOperation.id,
        now: staleNow,
      }),
    ).resolves.toMatchObject({ scanned: 1, requeued: 1, continuations: 1 });
    await expect(
      prisma.generationJob.findUniqueOrThrow({ where: { id: staleJob.id } }),
    ).resolves.toMatchObject({
      status: GenerationJobStatus.FAILED,
      stage: GenerationJobStage.FAILED,
      failureCategory: GenerationFailureCategory.TIMEOUT,
      completedAt: staleNow,
    });
    const staleItem = await prisma.agentSkillOperationItem.findUniqueOrThrow({
      where: { id: staleOperation.items[0].id },
    });
    expect(staleItem).toMatchObject({
      status: AgentOperationItemStatus.QUEUED,
      createdSkillId: staleSkill.id,
      activationReservedAt: null,
      workerClaimToken: null,
      errorCode: "STALE_WORKER_RECOVERY",
      retryCount: 1,
    });
    expect(sendAgentSkillOperationRequested).toHaveBeenCalledWith(
      expect.objectContaining({
        userId: staleFixture.userId,
        operationId: staleOperation.id,
        requestedAt: expect.any(String),
      }),
      expect.objectContaining({
        eventId: expect.stringMatching(/^agent-op-[a-f0-9]{40}$/),
        signal: expect.any(AbortSignal),
      }),
    );
    await expect(
      prisma.agentSkillOperation.findUniqueOrThrow({ where: { id: staleOperation.id } }),
    ).resolves.toMatchObject({ status: AgentOperationStatus.QUEUED });

    await expect(runAgentSkillOperationJob({
      userId: staleFixture.userId,
      operationId: staleOperation.id,
      now: staleNow,
    }, { activationOptions: quickActivationOptions() })).resolves.toMatchObject({
      status: "processed",
      operationId: staleOperation.id,
    });
    await expect(
      prisma.agentSkillOperationItem.findUniqueOrThrow({ where: { id: staleItem.id } }),
    ).resolves.toMatchObject({
      status: AgentOperationItemStatus.ACTIVE,
      resultSkillId: staleSkill.id,
      errorCode: null,
      retryCount: 1,
    });
    await expect(
      prisma.skill.findUniqueOrThrow({ where: { id: staleSkill.id } }),
    ).resolves.toMatchObject({ status: SkillStatus.ACTIVE, firstIntroducedAt: null });
    await expect(prisma.exercise.count({ where: { skillId: staleSkill.id } })).resolves.toBe(3);
    await expect(prisma.exerciseAttempt.count({ where: { skillId: staleSkill.id } })).resolves.toBe(0);

    await expect(
      prisma.agentSkillOperationItem.updateMany({
        where: {
          id: staleItem.id,
          userId: staleFixture.userId,
          workerClaimToken: "stale-worker-token",
        },
        data: { status: AgentOperationItemStatus.ACTIVE },
      }),
    ).resolves.toMatchObject({ count: 0 });

    const freshFixture = await createConnection("fresh-activation");
    const freshClaimedAt = new Date("2026-08-13T11:45:00.000Z");
    const freshJobUpdatedAt = new Date("2026-08-13T11:59:00.000Z");
    const freshSkill = await prisma.skill.create({
      data: {
        userId: freshFixture.userId,
        title: "Fresh recovery draft",
        objective: "Keep a live activation owned by its current worker.",
        status: SkillStatus.DRAFT,
      },
    });
    const freshJob = await prisma.generationJob.create({
      data: {
        userId: freshFixture.userId,
        skillId: freshSkill.id,
        kind: GenerationJobKind.SKILL_ACTIVATION,
        status: GenerationJobStatus.RUNNING,
        stage: GenerationJobStage.VERIFYING,
        provider: "google",
        model: "recovery-test-gemini",
        promptVersion: "recovery-test-v1",
        requestedCount: 3,
        startedAt: freshClaimedAt,
        updatedAt: freshJobUpdatedAt,
      },
    });
    const freshOperation = await prisma.agentSkillOperation.create({
      data: {
        userId: freshFixture.userId,
        connectionId: freshFixture.connection.id,
        kind: AgentOperationKind.SPEC_BATCH,
        toolName: "skills.add_from_specs",
        status: AgentOperationStatus.ACTIVATING,
        idempotencyKey: `fresh-activation-${runId}`,
        payloadHash: "3".repeat(64),
        requestedCount: 1,
        items: {
          create: {
            ordinal: 0,
            clientReference: "fresh-activation-item",
            status: AgentOperationItemStatus.ACTIVATING,
            createdSkillId: freshSkill.id,
            activationReservedAt: freshClaimedAt,
            workerClaimToken: "fresh-worker-token",
            workerClaimedAt: freshClaimedAt,
            updatedAt: freshClaimedAt,
          },
        },
      },
    });
    await expect(
      recoverStaleAgentOperationItems({
        userId: freshFixture.userId,
        operationId: freshOperation.id,
        now: staleNow,
      }),
    ).resolves.toMatchObject({ scanned: 1, waiting: 1, requeued: 0 });
    await expect(
      prisma.generationJob.findUniqueOrThrow({ where: { id: freshJob.id } }),
    ).resolves.toMatchObject({ status: GenerationJobStatus.RUNNING });
    await expect(
      prisma.agentSkillOperationItem.findFirstOrThrow({
        where: { operationId: freshOperation.id },
      }),
    ).resolves.toMatchObject({
      status: AgentOperationItemStatus.ACTIVATING,
      workerClaimToken: "fresh-worker-token",
      activationReservedAt: freshClaimedAt,
    });
  });

  it("finalizes an activation that published before recovery inspected it", async () => {
    const fixture = await createConnection("published-before-recovery");
    const now = new Date("2026-08-13T12:00:00.000Z");
    const claimedAt = new Date("2026-08-13T11:45:00.000Z");
    const skill = await prisma.skill.create({
      data: {
        userId: fixture.userId,
        title: "Published recovery skill",
        objective: "Reconcile an activation after publication committed.",
        status: SkillStatus.ACTIVE,
      },
    });
    const operation = await prisma.agentSkillOperation.create({
      data: {
        userId: fixture.userId,
        connectionId: fixture.connection.id,
        kind: AgentOperationKind.SPEC_BATCH,
        toolName: "skills.add_from_specs",
        status: AgentOperationStatus.ACTIVATING,
        idempotencyKey: `published-recovery-${runId}`,
        payloadHash: "4".repeat(64),
        requestedCount: 1,
        items: {
          create: {
            ordinal: 0,
            clientReference: "published-recovery-item",
            status: AgentOperationItemStatus.ACTIVATING,
            createdSkillId: skill.id,
            activationReservedAt: claimedAt,
            workerClaimToken: "published-worker-token",
            workerClaimedAt: claimedAt,
            updatedAt: claimedAt,
          },
        },
      },
    });

    await expect(
      recoverStaleAgentOperationItems({
        userId: fixture.userId,
        operationId: operation.id,
        now,
      }),
    ).resolves.toMatchObject({ scanned: 1, finalized: 1, continuations: 0 });
    await expect(
      prisma.agentSkillOperationItem.findFirstOrThrow({ where: { operationId: operation.id } }),
    ).resolves.toMatchObject({
      status: AgentOperationItemStatus.ACTIVE,
      resultSkillId: skill.id,
      activationReservedAt: null,
      workerClaimToken: null,
      completedAt: now,
    });
    await expect(
      prisma.agentSkillOperation.findUniqueOrThrow({ where: { id: operation.id } }),
    ).resolves.toMatchObject({ status: AgentOperationStatus.SUCCEEDED, activeCount: 1 });
  });

  it("keeps concurrent deliveries waiting on one running activation without duplicating records", async () => {
    const fixture = await createConnection("concurrent-worker-wait");
    const now = new Date();
    const skill = await prisma.skill.create({
      data: {
        userId: fixture.userId,
        title: "Concurrent activation draft",
        objective: "Wait for a single existing activation before publishing.",
        status: SkillStatus.DRAFT,
      },
    });
    const job = await prisma.generationJob.create({
      data: {
        userId: fixture.userId,
        skillId: skill.id,
        kind: GenerationJobKind.SKILL_ACTIVATION,
        status: GenerationJobStatus.RUNNING,
        stage: GenerationJobStage.PUBLISHING,
        provider: "google",
        model: "recovery-test-gemini",
        promptVersion: "recovery-test-v1",
        requestedCount: 5,
        startedAt: now,
        updatedAt: now,
      },
    });
    const operation = await prisma.agentSkillOperation.create({
      data: {
        userId: fixture.userId,
        connectionId: fixture.connection.id,
        kind: AgentOperationKind.SPEC_BATCH,
        toolName: "skills.add_from_specs",
        status: AgentOperationStatus.QUEUED,
        idempotencyKey: "concurrent-worker-wait-" + runId,
        payloadHash: "5".repeat(64),
        requestedCount: 1,
        items: {
          create: {
            ordinal: 0,
            clientReference: "concurrent-wait-item",
            status: AgentOperationItemStatus.QUEUED,
            createdSkillId: skill.id,
          },
        },
      },
      include: { items: true },
    });

    const updateMany = prisma.agentSkillOperationItem.updateMany.bind(
      prisma.agentSkillOperationItem,
    );
    let claimAttempts = 0;
    let releaseClaims!: () => void;
    const bothClaimsReached = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(
        () => reject(new Error("Both deliveries did not reach the item claim.")),
        10_000,
      );
      releaseClaims = () => {
        clearTimeout(timeout);
        resolve();
      };
    });
    const claimSpy = vi
      .spyOn(prisma.agentSkillOperationItem, "updateMany")
      .mockImplementation((args) => {
        if (
          args.where?.id === operation.items[0].id &&
          args.where?.status === AgentOperationItemStatus.QUEUED
        ) {
          claimAttempts += 1;
          if (claimAttempts === 2) releaseClaims();
          return (async () => {
            await bothClaimsReached;
            return updateMany(args);
          })() as unknown as ReturnType<typeof updateMany>;
        }
        return updateMany(args);
      });

    try {
      await expect(
        Promise.all([
          runAgentSkillOperationJob({ userId: fixture.userId, operationId: operation.id, now }),
          runAgentSkillOperationJob({ userId: fixture.userId, operationId: operation.id, now }),
        ]),
      ).resolves.toEqual([
        { status: "processed", operationId: operation.id },
        { status: "processed", operationId: operation.id },
      ]);
      expect(claimAttempts).toBe(2);
    } finally {
      releaseClaims();
      claimSpy.mockRestore();
    }

    await expect(
      prisma.agentSkillOperationItem.findUniqueOrThrow({ where: { id: operation.items[0].id } }),
    ).resolves.toMatchObject({
      status: AgentOperationItemStatus.ACTIVATING,
      errorCode: "ACTIVATION_WAITING",
      workerClaimToken: null,
      workerClaimedAt: null,
      activationReservedAt: now,
      retryCount: 0,
    });
    await expect(
      prisma.agentSkillOperation.findUniqueOrThrow({ where: { id: operation.id } }),
    ).resolves.toMatchObject({
      status: AgentOperationStatus.ACTIVATING,
      activeCount: 0,
      reusedCount: 0,
      failedCount: 0,
      completedAt: null,
    });
    await expect(
      prisma.generationJob.count({
        where: { userId: fixture.userId, skillId: skill.id, kind: GenerationJobKind.SKILL_ACTIVATION },
      }),
    ).resolves.toBe(1);
    await expect(
      prisma.generationJob.findUniqueOrThrow({ where: { id: job.id } }),
    ).resolves.toMatchObject({ status: GenerationJobStatus.RUNNING });
    await expect(prisma.skill.count({ where: { userId: fixture.userId } })).resolves.toBe(1);
    await expect(prisma.exercise.count({ where: { userId: fixture.userId } })).resolves.toBe(0);
  });

  it("recovers stale legacy ACTIVATION_IN_PROGRESS failures and reconciles reservations", async () => {
    const fixture = await createConnection("legacy-activation-wait");
    const now = new Date("2026-08-13T14:00:00.000Z");
    const staleAt = new Date(now.getTime() - 12 * 60_000);
    const skill = await prisma.skill.create({
      data: {
        userId: fixture.userId,
        title: "Legacy activation draft",
        objective: "Resume a draft whose older worker marked activation as failed.",
        status: SkillStatus.DRAFT,
      },
    });
    const job = await prisma.generationJob.create({
      data: {
        userId: fixture.userId,
        skillId: skill.id,
        kind: GenerationJobKind.SKILL_ACTIVATION,
        status: GenerationJobStatus.RUNNING,
        stage: GenerationJobStage.PUBLISHING,
        provider: "google",
        model: "recovery-test-gemini",
        promptVersion: "recovery-test-v1",
        requestedCount: 5,
        startedAt: staleAt,
        updatedAt: staleAt,
      },
    });
    const operation = await prisma.agentSkillOperation.create({
      data: {
        userId: fixture.userId,
        connectionId: fixture.connection.id,
        kind: AgentOperationKind.SPEC_BATCH,
        toolName: "skills.add_from_specs",
        status: AgentOperationStatus.QUEUED,
        idempotencyKey: "legacy-activation-wait-" + runId,
        payloadHash: "6".repeat(64),
        requestedCount: 1,
        items: {
          create: {
            ordinal: 0,
            clientReference: "legacy-wait-item",
            status: AgentOperationItemStatus.FAILED,
            errorCode: "ACTIVATION_IN_PROGRESS",
            errorMessage: "The previous worker observed another activation in progress.",
            createdSkillId: skill.id,
            activationReservedAt: staleAt,
            updatedAt: staleAt,
          },
        },
      },
      include: { items: true },
    });

    await expect(
      recoverStaleAgentOperationItems({
        userId: fixture.userId,
        operationId: operation.id,
        now,
      }),
    ).resolves.toMatchObject({ scanned: 1, requeued: 1, continuations: 1 });

    await expect(
      prisma.generationJob.findUniqueOrThrow({ where: { id: job.id } }),
    ).resolves.toMatchObject({
      status: GenerationJobStatus.FAILED,
      stage: GenerationJobStage.FAILED,
      failureCategory: GenerationFailureCategory.TIMEOUT,
      completedAt: now,
    });
    await expect(
      prisma.agentSkillOperationItem.findUniqueOrThrow({ where: { id: operation.items[0].id } }),
    ).resolves.toMatchObject({
      status: AgentOperationItemStatus.QUEUED,
      errorCode: "STALE_WORKER_RECOVERY",
      activationReservedAt: null,
      workerClaimToken: null,
      workerClaimedAt: null,
      retryCount: 1,
      completedAt: null,
    });
    await expect(
      prisma.agentSkillOperation.findUniqueOrThrow({ where: { id: operation.id } }),
    ).resolves.toMatchObject({
      status: AgentOperationStatus.QUEUED,
      activeCount: 0,
      failedCount: 0,
      completedAt: null,
    });
    expect(sendAgentSkillOperationRequested).toHaveBeenCalledWith(
      expect.objectContaining({
        userId: fixture.userId,
        operationId: operation.id,
        requestedAt: expect.any(String),
      }),
      expect.objectContaining({
        eventId: expect.stringMatching(/^agent-op-[a-f0-9]{40}$/),
        signal: expect.any(AbortSignal),
      }),
    );
    await expect(prisma.skill.count({ where: { userId: fixture.userId } })).resolves.toBe(1);
    await expect(prisma.exercise.count({ where: { userId: fixture.userId } })).resolves.toBe(0);
  });

  it("does not reopen a stale child of a terminal operation", async () => {
    const fixture = await createConnection("terminal-recovery");
    const claimedAt = new Date("2026-08-13T13:45:00.000Z");
    const skill = await prisma.skill.create({
      data: {
        userId: fixture.userId,
        title: "Terminal recovery draft",
        objective: "Keep terminal operations closed during stale recovery.",
        status: SkillStatus.DRAFT,
      },
    });
    const operation = await prisma.agentSkillOperation.create({
      data: {
        userId: fixture.userId,
        connectionId: fixture.connection.id,
        kind: AgentOperationKind.SPEC_BATCH,
        toolName: "skills.add_from_specs",
        status: AgentOperationStatus.FAILED,
        idempotencyKey: `terminal-recovery-${runId}`,
        payloadHash: "terminal".repeat(8),
        requestedCount: 1,
        items: {
          create: {
            ordinal: 0,
            clientReference: "terminal-recovery-item",
            status: AgentOperationItemStatus.ACTIVATING,
            createdSkillId: skill.id,
            workerClaimToken: "terminal-worker-token",
            workerClaimedAt: claimedAt,
            updatedAt: claimedAt,
          },
        },
      },
      include: { items: true },
    });

    vi.mocked(sendAgentSkillOperationRequested).mockClear();
    await expect(
      recoverStaleAgentOperationItems({
        userId: fixture.userId,
        operationId: operation.id,
        now: new Date("2026-08-13T14:00:00.000Z"),
      }),
    ).resolves.toMatchObject({ scanned: 0, continuations: 0 });
    await expect(
      prisma.agentSkillOperationItem.findUniqueOrThrow({ where: { id: operation.items[0].id } }),
    ).resolves.toMatchObject({
      status: AgentOperationItemStatus.ACTIVATING,
      workerClaimToken: "terminal-worker-token",
    });
    expect(sendAgentSkillOperationRequested).not.toHaveBeenCalled();
  });

  it("serializes active-skill reservations at the shared account limit", async () => {
    const fixture = await createConnection("quota");
    await prisma.skill.createMany({
      data: Array.from({ length: ALPHA_ACTIVE_SKILLS - 1 }, (_, index) => ({
        userId: fixture.userId,
        title: `Active skill ${index + 1}`,
        objective: `Practice active skill number ${index + 1}.`,
        status: SkillStatus.ACTIVE,
      })),
    });
    const operation = await prisma.agentSkillOperation.create({
      data: {
        userId: fixture.userId,
        connectionId: fixture.connection.id,
        kind: AgentOperationKind.SPEC_BATCH,
        toolName: "skills.add_from_specs",
        idempotencyKey: `quota-${runId}`,
        payloadHash: "b".repeat(64),
        requestedCount: 2,
        items: {
          create: [
            { ordinal: 0, clientReference: "first" },
            { ordinal: 1, clientReference: "second" },
          ],
        },
      },
      include: { items: { orderBy: { ordinal: "asc" } } },
    });

    const reservations = await Promise.all(
      operation.items.map((item) =>
        reserveAgentActivation(fixture.userId, item.id, new Date("2026-08-13T18:00:00.000Z")),
      ),
    );
    expect(reservations.filter(Boolean)).toHaveLength(1);
  });

  it("pauses only active skills created through the selected connection", async () => {
    const fixture = await createConnection("pause");
    const [agentSkill, browserSkill] = await Promise.all([
      prisma.skill.create({
        data: {
          userId: fixture.userId,
          title: "Agent-created limits",
          objective: "Recall the limits attached to an agent-created skill.",
          status: SkillStatus.ACTIVE,
        },
      }),
      prisma.skill.create({
        data: {
          userId: fixture.userId,
          title: "Browser-created limits",
          objective: "Recall the limits attached to a browser-created skill.",
          status: SkillStatus.ACTIVE,
        },
      }),
    ]);
    await prisma.agentSkillOperation.create({
      data: {
        userId: fixture.userId,
        connectionId: fixture.connection.id,
        kind: AgentOperationKind.SPEC_BATCH,
        toolName: "skills.add_from_specs",
        status: AgentOperationStatus.SUCCEEDED,
        idempotencyKey: `pause-${runId}`,
        payloadHash: "c".repeat(64),
        requestedCount: 1,
        activeCount: 1,
        items: {
          create: {
            ordinal: 0,
            clientReference: "pause-skill",
            status: AgentOperationItemStatus.ACTIVE,
            createdSkillId: agentSkill.id,
            resultSkillId: agentSkill.id,
            completedAt: new Date("2026-08-13T18:30:00.000Z"),
          },
        },
      },
    });

    await expect(
      pauseSkillsFromAgent({ userId: fixture.userId, connectionId: fixture.connection.id }),
    ).resolves.toEqual({ status: "paused", count: 1 });
    await expect(
      prisma.skill.findMany({
        where: { id: { in: [agentSkill.id, browserSkill.id] } },
        orderBy: { title: "asc" },
        select: { title: true, status: true },
      }),
    ).resolves.toEqual([
      { title: "Agent-created limits", status: SkillStatus.PAUSED },
      { title: "Browser-created limits", status: SkillStatus.ACTIVE },
    ]);
  });

  it("resolves duplicate review without letting the agent choose the outcome", async () => {
    const fixture = await createConnection("duplicate-review");
    const existingSkill = await prisma.skill.create({
      data: {
        userId: fixture.userId,
        title: "Existing boundary checks",
        objective: "Practice the boundary checks already stored in the library.",
        status: SkillStatus.ACTIVE,
      },
    });
    const proposedDraft = await prisma.skill.create({
      data: {
        userId: fixture.userId,
        title: "Proposed boundary checks",
        objective: "Practice a proposed set of overlapping boundary checks.",
        status: SkillStatus.DRAFT,
      },
    });
    const operation = await prisma.agentSkillOperation.create({
      data: {
        userId: fixture.userId,
        connectionId: fixture.connection.id,
        kind: AgentOperationKind.SPEC_BATCH,
        toolName: "skills.add_from_specs",
        status: AgentOperationStatus.NEEDS_REVIEW,
        idempotencyKey: `review-${runId}`,
        payloadHash: "d".repeat(64),
        requestedCount: 1,
        items: {
          create: {
            ordinal: 0,
            clientReference: "duplicate-skill",
            status: AgentOperationItemStatus.NEEDS_REVIEW,
            createdSkillId: proposedDraft.id,
            resultSkillId: existingSkill.id,
          },
        },
      },
      include: { items: true },
    });
    const item = operation.items[0];

    await expect(
      resolveAgentDuplicateReview({
        userId: fixture.userId,
        itemId: item.id,
        decision: "use-existing",
        now: new Date("2026-08-13T19:00:00.000Z"),
      }),
    ).resolves.toEqual({ status: "saved" });
    await expect(prisma.skill.findUnique({ where: { id: proposedDraft.id } })).resolves.toBeNull();
    await expect(
      prisma.agentSkillOperationItem.findUniqueOrThrow({ where: { id: item.id } }),
    ).resolves.toMatchObject({
      status: AgentOperationItemStatus.REUSED,
      resultSkillId: existingSkill.id,
    });
  });
});
