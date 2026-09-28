import "server-only";

import {
  AgentConnectionStatus,
  AgentOperationKind,
  AgentOperationItemStatus,
  AgentOperationStatus,
  AgentRemoteRevocationStatus,
  AgentRevocationOutboxStatus,
  Prisma,
  SkillStatus,
} from "@/generated/prisma/client";
import { getAgentAccessConfig } from "@/lib/agent-access/auth";
import { sendAgentConnectionRevocationRequested, sendAgentSkillOperationRequested } from "@/lib/jobs/events";
import { getPrisma } from "@/lib/prisma";
import { cleanupPreparedSourceUploads } from "@/lib/skills/uploads";
import { recoverPendingRefillEvents } from "@/lib/skills/refill-jobs";
import { recoverStaleAgentOperationItems } from "@/lib/agent-access/recovery";
import { recoverMaterialIngestionPublications } from "@/lib/materials/ingestion-publication";
import { getSafeJobErrorDetails } from "@/lib/jobs/errors";
import { JobContinuationLimitError } from "@/lib/jobs/publication-context";

const AGENT_UPLOAD_WINDOW_MS = 10 * 60 * 1_000;
const WORKOS_AUTHORIZED_APPLICATION_PAGE_LIMIT = 100;
const WORKOS_AUTHORIZED_APPLICATION_MAX_PAGES = 20;

export async function revokeAllWorkosAuthorizedApplicationsForUser(input: {
  userId: string;
  fetchImpl?: typeof fetch;
}): Promise<{ revoked: number }> {
  const config = getAgentAccessConfig();
  const apiKey = config.enabled ? config.workosApiKey : process.env.WORKOS_API_KEY?.trim();
  const resourceUrl = config.enabled ? config.resourceUrl : process.env.MCP_RESOURCE_URL?.trim();
  if (!apiKey) throw new Error("WorkOS revocation credentials are unavailable.");
  if (!resourceUrl) throw new Error("WorkOS resource configuration is unavailable.");
  const identity = await getPrisma().workosIdentity.findUnique({
    where: { userId: input.userId },
    select: { workosUserId: true },
  });
  const fetchImpl = input.fetchImpl ?? fetch;
  const workosUserId = identity?.workosUserId ?? await resolveWorkosUserIdByExternalId({
    externalId: input.userId,
    apiKey,
    fetchImpl,
  });
  if (!workosUserId) return { revoked: 0 };
  return revokeWorkosAuthorizedApplications({
    workosUserId,
    apiKey,
    resourceUrl,
    fetchImpl,
  });
}

async function resolveWorkosUserIdByExternalId(input: {
  externalId: string;
  apiKey: string;
  fetchImpl: typeof fetch;
}): Promise<string | null> {
  const response = await input.fetchImpl(
    `https://api.workos.com/user_management/users/external_id/${encodeURIComponent(input.externalId)}`,
    {
      headers: { authorization: `Bearer ${input.apiKey}` },
      redirect: "error",
      signal: AbortSignal.timeout(10_000),
    },
  );
  if (response.status === 404) return null;
  if (!response.ok) {
    throw new Error(`WorkOS identity lookup failed with HTTP ${response.status}.`);
  }
  const body = await response.json() as { id?: unknown; external_id?: unknown };
  if (
    typeof body.id !== "string" ||
    !body.id ||
    body.external_id !== input.externalId
  ) {
    throw new Error("WorkOS identity lookup returned an invalid response.");
  }
  return body.id;
}

export async function revokeWorkosAuthorizedApplications(input: {
  workosUserId: string;
  apiKey: string;
  resourceUrl: string;
  fetchImpl?: typeof fetch;
}): Promise<{ revoked: number }> {
  const fetchImpl = input.fetchImpl ?? fetch;
  const applicationIds = new Set<string>();
  let after: string | null = null;

  for (let page = 0; page < WORKOS_AUTHORIZED_APPLICATION_MAX_PAGES; page += 1) {
    const url = new URL(
      `https://api.workos.com/user_management/users/${encodeURIComponent(input.workosUserId)}/authorized_applications`,
    );
    url.searchParams.set("limit", String(WORKOS_AUTHORIZED_APPLICATION_PAGE_LIMIT));
    if (after) url.searchParams.set("after", after);
    const response = await fetchImpl(url, {
      headers: { authorization: `Bearer ${input.apiKey}` },
      redirect: "error",
      signal: AbortSignal.timeout(10_000),
    });
    if (response.status === 404 && page === 0) return { revoked: 0 };
    if (!response.ok) {
      throw new Error(`WorkOS authorized application listing failed with HTTP ${response.status}.`);
    }
    const body = await response.json() as {
      data?: Array<{
        application?: { id?: unknown };
        oauth_resource?: unknown;
      }>;
      list_metadata?: { after?: unknown };
    };
    if (!Array.isArray(body.data)) {
      throw new Error("WorkOS authorized application listing returned an invalid response.");
    }
    for (const entry of body.data) {
      if (typeof entry.application?.id !== "string" || !entry.application.id) {
        throw new Error("WorkOS authorized application listing returned an invalid application.");
      }
      if (
        entry.oauth_resource == null ||
        entry.oauth_resource === input.resourceUrl
      ) {
        applicationIds.add(entry.application.id);
      }
    }
    const nextAfter = body.list_metadata?.after;
    if (nextAfter == null) break;
    if (typeof nextAfter !== "string" || !nextAfter || nextAfter === after) {
      throw new Error("WorkOS authorized application listing returned an invalid cursor.");
    }
    after = nextAfter;
    if (page === WORKOS_AUTHORIZED_APPLICATION_MAX_PAGES - 1) {
      throw new Error("WorkOS authorized application listing exceeded the page limit.");
    }
  }

  for (const applicationId of applicationIds) {
    const response = await fetchImpl(
      `https://api.workos.com/user_management/users/${encodeURIComponent(input.workosUserId)}/authorized_applications/${encodeURIComponent(applicationId)}`,
      {
        method: "DELETE",
        headers: { authorization: `Bearer ${input.apiKey}` },
        redirect: "error",
        signal: AbortSignal.timeout(10_000),
      },
    );
    if (!response.ok && response.status !== 404) {
      throw new Error(`WorkOS authorized application deletion failed with HTTP ${response.status}.`);
    }
  }

  return { revoked: applicationIds.size };
}

export async function getAgentAccessOverview(userId: string) {
  const config = getAgentAccessConfig();
  if (!config.enabled) return { status: "disabled" as const };
  const prisma = getPrisma();
  const [connections, operations, preparing, needsReview] = await Promise.all([
    prisma.agentConnection.findMany({
      where: { userId },
      orderBy: [{ status: "asc" }, { connectedAt: "desc" }],
      select: {
        id: true,
        clientName: true,
        clientDomain: true,
        scopes: true,
        status: true,
        connectedAt: true,
        lastUsedAt: true,
        revokedAt: true,
        remoteRevocationStatus: true,
        _count: {
          select: {
            operations: {
              where: { status: AgentOperationStatus.SUCCEEDED },
            },
          },
        },
      },
    }),
    prisma.agentSkillOperation.findMany({
      where: { userId },
      orderBy: { updatedAt: "desc" },
      take: 8,
      select: {
        id: true,
        status: true,
        requestedCount: true,
        activeCount: true,
        reusedCount: true,
        failedCount: true,
        updatedAt: true,
        connection: { select: { clientName: true } },
      },
    }),
    prisma.agentSkillOperationItem.findMany({
      where: {
        userId,
        status: {
          in: [
            AgentOperationItemStatus.QUEUED,
            AgentOperationItemStatus.PLANNING,
            AgentOperationItemStatus.GENERATING,
            AgentOperationItemStatus.VERIFYING,
            AgentOperationItemStatus.ACTIVATING,
          ],
        },
      },
      orderBy: { updatedAt: "desc" },
      select: {
        id: true,
        status: true,
        proposedTitle: true,
        updatedAt: true,
        operation: { select: { connection: { select: { clientName: true } } } },
      },
    }),
    prisma.agentSkillOperationItem.findMany({
      where: { userId, status: AgentOperationItemStatus.NEEDS_REVIEW },
      orderBy: { updatedAt: "desc" },
      select: {
        id: true,
        proposedTitle: true,
        proposedObjective: true,
        duplicateConfidence: true,
        createdSkillId: true,
        resultSkillId: true,
        updatedAt: true,
        operation: { select: { connection: { select: { clientName: true } } } },
      },
    }),
  ]);
  return {
    status: "ready" as const,
    resourceUrl: config.resourceUrl,
    connections,
    operations,
    preparing,
    needsReview,
  };
}

export async function revokeAgentConnection(input: { userId: string; connectionId: string; now: Date }) {
  const prisma = getPrisma();
  const result = await prisma.$transaction(async (tx) => {
    await tx.$queryRaw`
      SELECT "id" FROM "agent_connections"
      WHERE "id" = ${input.connectionId} AND "userId" = ${input.userId}
      FOR UPDATE
    `;
    const connection = await tx.agentConnection.findFirst({
      where: { id: input.connectionId, userId: input.userId },
      select: {
        id: true,
        status: true,
        workosApplicationId: true,
        workosIdentity: { select: { workosUserId: true } },
      },
    });
    if (!connection) return { status: "not-found" as const };
    const grantConnections = await tx.agentConnection.findMany({
      where: {
        userId: input.userId,
        workosApplicationId: connection.workosApplicationId,
      },
      select: { id: true, status: true },
    });
    const hasActiveConnection = grantConnections.some(
      (candidate) => candidate.status === AgentConnectionStatus.ACTIVE,
    );
    if (!hasActiveConnection) {
      return { status: "revoked" as const, alreadyRevoked: true };
    }
    await tx.agentConnection.updateMany({
      where: {
        userId: input.userId,
        workosApplicationId: connection.workosApplicationId,
        status: AgentConnectionStatus.ACTIVE,
      },
      data: {
        status: AgentConnectionStatus.REVOKED,
        revokedAt: input.now,
        remoteRevocationStatus: AgentRemoteRevocationStatus.PENDING,
      },
    });
    await tx.agentRevocationOutbox.upsert({
      where: { connectionId: connection.id },
      create: {
        userId: input.userId,
        connectionId: connection.id,
        workosUserId: connection.workosIdentity.workosUserId,
        applicationId: connection.workosApplicationId,
      },
      update: {
        status: AgentRevocationOutboxStatus.PENDING,
        nextAttemptAt: input.now,
        errorCode: null,
      },
    });
    return { status: "revoked" as const, alreadyRevoked: false };
  });
  if (result.status === "revoked") {
    try {
      await sendAgentConnectionRevocationRequested({
        userId: input.userId,
        connectionId: input.connectionId,
        requestedAt: input.now.toISOString(),
      });
    } catch (error) {
      console.error("[agent-access] WorkOS revocation dispatch deferred", {
        userId: input.userId,
        connectionId: input.connectionId,
        ...getSafeJobErrorDetails(error),
      });
    }
  }
  return result;
}

export async function disableAgentAccessForAccountDeletion(input: {
  userId: string;
  now: Date;
}) {
  const prisma = getPrisma();
  const connectionIds = await prisma.$transaction(async (tx) => {
    await tx.$queryRaw`SELECT "id" FROM "users" WHERE "id" = ${input.userId} FOR UPDATE`;
    const connections = await tx.agentConnection.findMany({
      where: { userId: input.userId },
      select: {
        id: true,
        workosApplicationId: true,
        workosIdentity: { select: { workosUserId: true } },
      },
    });
    await tx.user.updateMany({
      where: { id: input.userId },
      data: { agentAccessDisabledAt: input.now },
    });
    await tx.agentConnection.updateMany({
      where: { userId: input.userId, status: AgentConnectionStatus.ACTIVE },
      data: {
        status: AgentConnectionStatus.REVOKED,
        revokedAt: input.now,
        remoteRevocationStatus: AgentRemoteRevocationStatus.PENDING,
      },
    });
    for (const connection of connections) {
      await tx.agentRevocationOutbox.upsert({
        where: { connectionId: connection.id },
        create: {
          userId: input.userId,
          connectionId: connection.id,
          workosUserId: connection.workosIdentity.workosUserId,
          applicationId: connection.workosApplicationId,
          nextAttemptAt: input.now,
        },
        update: {
          status: AgentRevocationOutboxStatus.PENDING,
          nextAttemptAt: input.now,
          errorCode: null,
        },
      });
    }
    return connections.map((connection) => connection.id);
  });
  await Promise.all(
    connectionIds.map((connectionId) =>
      sendAgentConnectionRevocationRequested({
        userId: input.userId,
        connectionId,
        requestedAt: input.now.toISOString(),
      }),
    ),
  );
  return { status: "disabled" as const, connectionCount: connectionIds.length };
}

export async function pauseSkillsFromAgent(input: { userId: string; connectionId: string }) {
  const prisma = getPrisma();
  const connection = await prisma.agentConnection.findFirst({
    where: { id: input.connectionId, userId: input.userId },
    select: { id: true },
  });
  if (!connection) return { status: "not-found" as const };
  const skills = await prisma.skill.findMany({
    where: {
      userId: input.userId,
      status: SkillStatus.ACTIVE,
      agentCreatedItems: { some: { userId: input.userId, operation: { connectionId: input.connectionId } } },
    },
    select: { id: true },
  });
  const updated = await prisma.skill.updateMany({
    where: { id: { in: skills.map((skill) => skill.id) }, userId: input.userId, status: SkillStatus.ACTIVE },
    data: { status: SkillStatus.PAUSED },
  });
  return { status: "paused" as const, count: updated.count };
}

export async function resolveAgentDuplicateReview(input: {
  userId: string;
  itemId: string;
  decision: "use-existing" | "create-separately";
  now: Date;
}) {
  const prisma = getPrisma();
  const item = await prisma.agentSkillOperationItem.findFirst({
    where: { id: input.itemId, userId: input.userId, status: AgentOperationItemStatus.NEEDS_REVIEW },
    select: { id: true, operationId: true, createdSkillId: true, resultSkillId: true },
  });
  if (!item?.resultSkillId) return { status: "not-found" as const };
  if (input.decision === "use-existing") {
    await prisma.$transaction(async (tx) => {
      if (item.createdSkillId) {
        await tx.skill.deleteMany({ where: { id: item.createdSkillId, userId: input.userId, status: SkillStatus.DRAFT } });
      }
      await tx.agentSkillOperationItem.update({
        where: { id: item.id },
        data: {
          status: AgentOperationItemStatus.REUSED,
          activationReservedAt: null,
          completedAt: input.now,
        },
      });
    });
  } else {
    await prisma.agentSkillOperationItem.update({
      where: { id: item.id },
      data: {
        status: AgentOperationItemStatus.QUEUED,
        duplicateOverrideApprovedAt: input.now,
        activationReservedAt: null,
      },
    });
  }
  await sendAgentSkillOperationRequested({
    userId: input.userId,
    operationId: item.operationId,
    requestedAt: input.now.toISOString(),
  });
  return { status: "saved" as const };
}

export async function getAgentDuplicateReview(input: { userId: string; itemId: string }) {
  const prisma = getPrisma();
  return prisma.agentSkillOperationItem.findFirst({
    where: { id: input.itemId, userId: input.userId, status: AgentOperationItemStatus.NEEDS_REVIEW },
    select: {
      id: true,
      proposedTitle: true,
      proposedObjective: true,
      duplicateConfidence: true,
      createdSkillId: true,
      resultSkill: {
        select: { id: true, title: true, objective: true, tags: true, collection: { select: { name: true } } },
      },
      operation: { select: { connection: { select: { clientName: true } } } },
    },
  });
}

export async function runAgentConnectionRevocationJob(input: {
  userId: string;
  connectionId: string;
}) {
  const config = getAgentAccessConfig();
  const workosApiKey = config.enabled ? config.workosApiKey : process.env.WORKOS_API_KEY?.trim();
  if (!workosApiKey) throw new Error("WorkOS revocation credentials are unavailable.");
  const prisma = getPrisma();
  const revocationJob = await prisma.agentRevocationOutbox.findFirst({
    where: { connectionId: input.connectionId, userId: input.userId },
    select: {
      id: true,
      attemptCount: true,
      status: true,
      workosUserId: true,
      applicationId: true,
    },
  });
  if (!revocationJob) return { status: "not-found" as const };
  if (revocationJob.status === AgentRevocationOutboxStatus.SUCCEEDED) {
    return { status: "revoked" as const };
  }
  await prisma.agentRevocationOutbox.update({
    where: { id: revocationJob.id },
    data: { status: AgentRevocationOutboxStatus.RUNNING, attemptCount: { increment: 1 } },
  });
  const response = await fetch(
    `https://api.workos.com/user_management/users/${encodeURIComponent(revocationJob.workosUserId)}/authorized_applications/${encodeURIComponent(revocationJob.applicationId)}`,
    {
      method: "DELETE",
      headers: { authorization: `Bearer ${workosApiKey}` },
      redirect: "error",
      signal: AbortSignal.timeout(10_000),
    },
  );
  if (!response.ok && response.status !== 404) {
    await prisma.$transaction([
      prisma.agentRevocationOutbox.update({
        where: { id: revocationJob.id },
        data: { status: AgentRevocationOutboxStatus.FAILED, errorCode: `WORKOS_${response.status}`, nextAttemptAt: new Date(Date.now() + 60_000) },
      }),
      prisma.agentConnection.updateMany({
        where: {
          userId: input.userId,
          workosApplicationId: revocationJob.applicationId,
        },
        data: { remoteRevocationStatus: AgentRemoteRevocationStatus.FAILED },
      }),
    ]);
    throw new Error(`WorkOS authorized application deletion failed with HTTP ${response.status}.`);
  }
  await prisma.$transaction([
    prisma.agentRevocationOutbox.update({
      where: { id: revocationJob.id },
      data: { status: AgentRevocationOutboxStatus.SUCCEEDED, errorCode: null, completedAt: new Date(), nextAttemptAt: null },
    }),
    prisma.agentConnection.updateMany({
      where: {
        userId: input.userId,
        workosApplicationId: revocationJob.applicationId,
      },
      data: { remoteRevocationStatus: AgentRemoteRevocationStatus.SUCCEEDED },
    }),
  ]);
  return { status: "revoked" as const };
}

export async function runAgentAccessMaintenance(now: Date, options?: { deadlineAt?: Date }) {
  const prisma = getPrisma();
  const uploadCutoff = new Date(now.getTime() - AGENT_UPLOAD_WINDOW_MS);
  let refillRecoveryFailed = false;
  let materialRecoveryFailed = false;
  let recoveryCause: unknown;
  let activationRecoveryFailed = false;
  const [purged, rateBuckets, pending, expiredUploads, refillEvents, activationRecovery, materialPublications] = await Promise.all([
    prisma.agentSkillOperation.updateMany({
      where: { payloadExpiresAt: { lte: now }, requestPayload: { not: Prisma.DbNull } },
      data: { requestPayload: Prisma.DbNull, payloadExpiresAt: null },
    }),
    prisma.agentRateLimitBucket.deleteMany({
      where: { windowStart: { lt: new Date(now.getTime() - 24 * 60 * 60 * 1_000) } },
    }),
    prisma.agentRevocationOutbox.findMany({
      where: {
        status: { in: [AgentRevocationOutboxStatus.PENDING, AgentRevocationOutboxStatus.FAILED] },
        OR: [{ nextAttemptAt: null }, { nextAttemptAt: { lte: now } }],
      },
      orderBy: { createdAt: "asc" },
      take: 25,
      select: { userId: true, connectionId: true },
    }),
    prisma.agentSkillOperation.findMany({
      where: {
        kind: AgentOperationKind.QUICK_FILES,
        OR: [
          { status: AgentOperationStatus.AWAITING_UPLOAD, createdAt: { lte: uploadCutoff } },
          { status: AgentOperationStatus.CANCELED, errorCode: "UPLOAD_WINDOW_EXPIRED" },
        ],
        sources: { some: {} },
      },
      orderBy: { createdAt: "asc" },
      take: 25,
      select: {
        id: true,
        userId: true,
        status: true,
        sources: { select: { sourceFileId: true } },
      },
    }),
    recoverPendingRefillEvents({ now, deadlineAt: options?.deadlineAt }).catch((error: unknown) => {
      if (error instanceof JobContinuationLimitError) throw error;
      refillRecoveryFailed = true;
      recoveryCause ??= error;
      console.error("[agent-access] refill event recovery failed during maintenance", {
        ...getSafeJobErrorDetails(error),
      });
      return { attempted: 0, delivered: 0, failed: 1 };
    }),
    recoverStaleAgentOperationItems({ now }).catch((error: unknown) => {
      if (error instanceof JobContinuationLimitError) throw error;
      activationRecoveryFailed = true;
      recoveryCause ??= error;
      console.error("[agent-access] activation item recovery failed during maintenance", {
        ...getSafeJobErrorDetails(error),
      });
      return {
        scanned: 0,
        requeued: 0,
        finalized: 0,
        waiting: 0,
        unchanged: 0,
        legacyPromoted: 0,
        continuations: 0,
        continuationPublishFailures: 0,
      };
    }),
    recoverMaterialIngestionPublications({ now, deadlineAt: options?.deadlineAt }).catch((error: unknown) => {
      if (error instanceof JobContinuationLimitError) throw error;
      materialRecoveryFailed = true;
      recoveryCause ??= error;
      console.error("[agent-access] material publication recovery failed during maintenance", getSafeJobErrorDetails(error));
      return { attempted: 0, delivered: 0, failed: 1 };
    }),
  ]);
  if (activationRecovery.continuationPublishFailures > 0) {
    activationRecoveryFailed = true;
  }
  let expiredUploadOperations = 0;
  for (const operation of expiredUploads) {
    if (operation.status === AgentOperationStatus.AWAITING_UPLOAD) {
      const claimed = await prisma.agentSkillOperation.updateMany({
        where: {
          id: operation.id,
          userId: operation.userId,
          status: AgentOperationStatus.AWAITING_UPLOAD,
          createdAt: { lte: uploadCutoff },
        },
        data: {
          status: AgentOperationStatus.CANCELED,
          errorCode: "UPLOAD_WINDOW_EXPIRED",
          errorMessage: "The private upload window expired before the files were started.",
          completedAt: now,
          requestPayload: Prisma.DbNull,
          payloadExpiresAt: null,
        },
      });
      if (claimed.count === 0) continue;
      await prisma.agentSkillOperationItem.updateMany({
        where: {
          operationId: operation.id,
          userId: operation.userId,
          status: AgentOperationItemStatus.QUEUED,
        },
        data: {
          status: AgentOperationItemStatus.CANCELED,
          errorCode: "UPLOAD_WINDOW_EXPIRED",
          completedAt: now,
        },
      });
      expiredUploadOperations += 1;
    }
    await cleanupPreparedSourceUploads({
      userId: operation.userId,
      sourceFileIds: operation.sources.map((source) => source.sourceFileId),
    });
  }
  const revocations = await Promise.allSettled(
    pending.map((job) => runAgentConnectionRevocationJob(job)),
  );
  const result = {
    purgedPayloads: purged.count,
    purgedRateBuckets: rateBuckets.count,
    expiredUploadOperations,
    revocationsAttempted: pending.length,
    revocationsFailed: revocations.filter((result) => result.status === "rejected").length,
    refillEventsAttempted: refillEvents.attempted,
    refillEventsDelivered: refillEvents.delivered,
    refillEventsFailed: refillEvents.failed,
    materialPublicationsAttempted: materialPublications.attempted,
    materialPublicationsDelivered: materialPublications.delivered,
    materialPublicationsFailed: materialPublications.failed,
    activationItemsScanned: activationRecovery.scanned,
    activationItemsRequeued: activationRecovery.requeued,
    activationItemsFinalized: activationRecovery.finalized,
    activationItemsWaiting: activationRecovery.waiting,
    activationLegacyItemsPromoted: activationRecovery.legacyPromoted,
    activationContinuations: activationRecovery.continuations,
    activationContinuationPublishFailures: activationRecovery.continuationPublishFailures,
  };
  if (refillRecoveryFailed || activationRecoveryFailed || materialRecoveryFailed) {
    const failures = [
      ...(refillRecoveryFailed ? ["refill events"] : []),
      ...(activationRecoveryFailed ? ["activation items"] : []),
      ...(materialRecoveryFailed ? ["material publications"] : []),
    ];
    const retryableError = new Error(
      `Agent access maintenance could not recover ${failures.join(" and ")}.`,
      { cause: recoveryCause },
    );
    Object.assign(retryableError, { retryable: true, code: "MAINTENANCE_RECOVERY_FAILED" });
    throw retryableError;
  }
  return result;
}
