import "server-only";
import { randomUUID } from "node:crypto";
import { z } from "zod";
import { Prisma } from "@/generated/prisma/client";
import { getPrisma } from "@/lib/prisma";
import {
  awsMaterialIngestionEventSender,
  type MaterialIngestionEventSender,
} from "@/lib/jobs/events";
import { withAbortableTimeout } from "@/lib/jobs/deadline";
import { getSafeJobErrorDetails } from "@/lib/jobs/errors";
import { JobContinuationLimitError } from "@/lib/jobs/publication-context";
import { getJobsEnvStatus } from "@/lib/jobs/config";
import { withBoundedActivationRead } from "@/lib/prisma-bounded-read";

const RETRY_DELAY_MS = 5 * 60_000;
const MAX_ATTEMPTS = 5;
const deliverySchema = z.object({
  version: z.literal(1),
  eventId: z.string().uuid(),
  requestedAt: z.string().datetime(),
  attempt: z.number().int().min(0).max(MAX_ATTEMPTS),
  nextAttemptAt: z.string().datetime(),
  status: z.enum(["pending", "delivering", "delivered", "failed"]),
  leaseId: z.string().uuid().nullable(),
});
type Delivery = z.infer<typeof deliverySchema>;
type Candidate = {
  id: string;
  userId: string;
  updatedAt: Date;
  processingMetadata: Prisma.JsonValue | null;
};
type PublicationResult = "pending" | "delivered" | "terminal" | "superseded";

export function createMaterialIngestionDelivery(now: Date): Delivery {
  return {
    version: 1,
    eventId: randomUUID(),
    requestedAt: now.toISOString(),
    attempt: 0,
    nextAttemptAt: now.toISOString(),
    status: "pending",
    leaseId: null,
  };
}

function readDelivery(candidate: Candidate): Delivery | null {
  const parsed = deliverySchema.safeParse(
    metadata(candidate).ingestionDelivery,
  );
  return parsed.success ? parsed.data : null;
}
function metadata(candidate: Candidate): Prisma.JsonObject {
  return candidate.processingMetadata &&
    typeof candidate.processingMetadata === "object" &&
    !Array.isArray(candidate.processingMetadata)
    ? candidate.processingMetadata
    : {};
}
function activeWhere(userId: string): Prisma.MaterialRevisionWhereInput {
  return {
    userId,
    status: "QUEUED",
    material: { status: "ACTIVE" },
    user: { agentAccessDisabledAt: null },
  };
}

export async function publishMaterialIngestion(input: {
  userId: string;
  materialRevisionId: string;
  now: Date;
  sender?: MaterialIngestionEventSender;
}): Promise<PublicationResult> {
  const candidate = await withBoundedActivationRead((tx) =>
    tx.materialRevision.findFirst({
      where: { id: input.materialRevisionId, ...activeWhere(input.userId) },
      select: {
        id: true,
        userId: true,
        updatedAt: true,
        processingMetadata: true,
      },
    }),
  );
  if (!candidate) return "superseded";
  return deliver(
    candidate,
    input.now,
    input.sender ?? awsMaterialIngestionEventSender,
  );
}

/** Publication intent is part of the domain commit; this sweep closes both send crash windows. */
export async function recoverMaterialIngestionPublications(input: {
  now: Date;
  sender?: MaterialIngestionEventSender;
  deadlineAt?: Date;
}): Promise<{ attempted: number; delivered: number; failed: number }> {
  if (!input.sender && getJobsEnvStatus().status === "missing-env")
    return { attempted: 0, delivered: 0, failed: 0 };
  const candidates = await withBoundedActivationRead(
    (tx) => tx.$queryRaw<Candidate[]>`
    SELECT r."id", r."userId", r."updatedAt", r."processingMetadata"
    FROM "material_revisions" r JOIN "study_materials" m ON m."id" = r."materialId" AND m."userId" = r."userId"
    JOIN "users" u ON u."id" = r."userId"
    WHERE r."status" = 'QUEUED' AND m."status" = 'ACTIVE' AND u."agentAccessDisabledAt" IS NULL
      AND NOT EXISTS (SELECT 1 FROM "account_deletion_jobs" d WHERE d."userId" = r."userId")
      AND r."processingMetadata" ? 'ingestionDelivery'
      AND coalesce(r."processingMetadata"->'ingestionDelivery'->>'status', '') NOT IN ('delivered', 'failed')
      AND r."updatedAt" <= ${new Date(input.now.getTime() - RETRY_DELAY_MS)}
    ORDER BY r."updatedAt", r."id" LIMIT 25
  `,
  );
  const counts = { attempted: 0, delivered: 0, failed: 0 };
  for (const candidate of candidates) {
    if (input.deadlineAt && Date.now() >= input.deadlineAt.getTime() - 45_000)
      break;
    const result = await deliver(
      candidate,
      input.now,
      input.sender ?? awsMaterialIngestionEventSender,
    );
    if (result !== "superseded") counts.attempted += 1;
    if (result === "delivered") counts.delivered += 1;
    if (result === "pending" || result === "terminal") counts.failed += 1;
  }
  return counts;
}

async function deliver(
  candidate: Candidate,
  now: Date,
  sender: MaterialIngestionEventSender,
): Promise<PublicationResult> {
  const prisma = getPrisma();
  const delivery = readDelivery(candidate);
  if (delivery?.status === "delivered") return "delivered";
  if (delivery?.status === "failed") return "superseded";
  if (!delivery || delivery.attempt >= MAX_ATTEMPTS) {
    const failed = await finishFailure(candidate, now);
    return failed ? "terminal" : "superseded";
  }
  if (
    !["pending", "delivering"].includes(delivery.status) ||
    Date.parse(delivery.nextAttemptAt) > now.getTime()
  )
    return "superseded";
  const leaseId = randomUUID();
  const claimedDelivery: Delivery = {
    ...delivery,
    attempt: delivery.attempt + 1,
    status: "delivering",
    leaseId,
    nextAttemptAt: new Date(now.getTime() + RETRY_DELAY_MS).toISOString(),
  };
  const claimed = await prisma.$transaction(
    async (tx) => {
      await tx.$queryRaw`SELECT set_config('statement_timeout', '5000', true)`;
      const users = await tx.$queryRaw<
        Array<{ id: string }>
      >`SELECT "id" FROM "users" WHERE "id" = ${candidate.userId} FOR UPDATE`;
      if (
        !users.length ||
        (await tx.accountDeletionJob.findUnique({
          where: { userId: candidate.userId },
          select: { id: true },
        }))
      )
        return false;
      const update = await tx.materialRevision.updateMany({
        where: {
          id: candidate.id,
          ...activeWhere(candidate.userId),
          updatedAt: candidate.updatedAt,
        },
        data: {
          processingMetadata: {
            ...metadata(candidate),
            ingestionDelivery: claimedDelivery,
          },
        },
      });
      return update.count === 1;
    },
    { maxWait: 2_000, timeout: 10_000 },
  );
  if (!claimed) return "superseded";
  const where: Prisma.MaterialRevisionWhereInput = {
    id: candidate.id,
    ...activeWhere(candidate.userId),
    processingMetadata: {
      path: ["ingestionDelivery", "leaseId"],
      equals: leaseId,
    },
  };
  // A deletion request can arrive after claiming. Check again immediately before send.
  if (
    !(await withBoundedActivationRead(async (tx) => {
      if (
        await tx.accountDeletionJob.findUnique({
          where: { userId: candidate.userId },
          select: { id: true },
        })
      )
        return null;
      return tx.materialRevision.findFirst({ where, select: { id: true } });
    }))
  )
    return "superseded";
  try {
    await withAbortableTimeout({
      timeoutMs: 5_000,
      stage: "material-event-publication",
      message: "Material publication timed out.",
      run: (signal) =>
        sender.sendMaterialIngestionRequested(
          {
            userId: candidate.userId,
            materialRevisionId: candidate.id,
            requestedAt: delivery.requestedAt,
          },
          { eventId: delivery.eventId, signal },
        ),
    });
    await withBoundedActivationRead((tx) =>
      tx.materialRevision.updateMany({
        where,
        data: {
          processingMetadata: {
            ...metadata(candidate),
            ingestionDelivery: {
              ...claimedDelivery,
              status: "delivered",
              leaseId: null,
            },
          },
        },
      }),
    );
    return "delivered";
  } catch (error) {
    if (error instanceof JobContinuationLimitError) throw error;
    console.warn(
      JSON.stringify({
        component: "material-publication",
        revisionId: candidate.id,
        attempt: claimedDelivery.attempt,
        outcome: "send-failed",
        ...getSafeJobErrorDetails(error),
      }),
    );
    if (claimedDelivery.attempt >= MAX_ATTEMPTS) {
      const failed = await finishFailure(
        candidate,
        now,
        leaseId,
        claimedDelivery,
      );
      return failed ? "terminal" : "superseded";
    }
    const update = await withBoundedActivationRead((tx) =>
      tx.materialRevision.updateMany({
        where,
        data: {
          processingMetadata: {
            ...metadata(candidate),
            ingestionDelivery: {
              ...claimedDelivery,
              status: "pending",
              leaseId: null,
            },
          },
        },
      }),
    );
    return update.count === 1 ? "pending" : "superseded";
  }
}

async function finishFailure(
  candidate: Candidate,
  now: Date,
  leaseId?: string,
  claimedDelivery?: Delivery,
): Promise<boolean> {
  const delivery = claimedDelivery ?? readDelivery(candidate);
  const failed = await withBoundedActivationRead(async (tx) => {
    const users = await tx.$queryRaw<
      Array<{ id: string }>
    >`SELECT "id" FROM "users" WHERE "id" = ${candidate.userId} FOR UPDATE`;
    if (
      !users.length ||
      (await tx.accountDeletionJob.findUnique({
        where: { userId: candidate.userId },
        select: { id: true },
      }))
    )
      return false;
    const update = await tx.materialRevision.updateMany({
      where: {
        id: candidate.id,
        ...activeWhere(candidate.userId),
        ...(leaseId
          ? {
              processingMetadata: {
                path: ["ingestionDelivery", "leaseId"],
                equals: leaseId,
              },
            }
          : { updatedAt: candidate.updatedAt }),
      },
      data: {
        status: "FAILED",
        errorCode: "EVENT_SEND_FAILED",
        errorMessage:
          "Background processing could not be delivered after bounded retries. Retry the import.",
        processingMetadata: {
          ...metadata(candidate),
          ...(delivery
            ? {
                ingestionDelivery: {
                  ...delivery,
                  status: "failed",
                  leaseId: null,
                },
              }
            : {}),
        },
        updatedAt: now,
      },
    });
    if (update.count !== 1) return false;
    await tx.sourceFile.updateMany({
      where: {
        userId: candidate.userId,
        materialRevisionId: candidate.id,
        status: "UPLOADED",
      },
      data: { status: "FAILED" },
    });
    return true;
  });
  if (failed)
    console.warn(
      JSON.stringify({
        component: "material-publication",
        revisionId: candidate.id,
        attempt: delivery?.attempt,
        outcome: "publication-exhausted",
      }),
    );
  return failed;
}
