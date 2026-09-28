import { randomUUID } from "node:crypto";
import { afterAll, afterEach, describe, expect, it, vi } from "vitest";
import { getPrisma } from "@/lib/prisma";
import {
  createMaterialIngestionDelivery,
  publishMaterialIngestion,
  recoverMaterialIngestionPublications,
} from "@/lib/materials/ingestion-publication";
import type { MaterialIngestionEventSender } from "@/lib/jobs/events";
import { JobContinuationLimitError } from "@/lib/jobs/publication-context";

const describeDatabase =
  process.env.RUN_DATABASE_TESTS === "1" ? describe : describe.skip;
const prisma = getPrisma();
const userIds: string[] = [];
const now = new Date();

describeDatabase("durable material ingestion publication", () => {
  afterEach(async () => {
    await prisma.accountDeletionJob.deleteMany({
      where: { userId: { in: userIds } },
    });
    await prisma.user.deleteMany({ where: { id: { in: userIds } } });
    userIds.length = 0;
  });
  afterAll(async () => prisma.$disconnect());
  async function fixture() {
    const userId = `material_publication_${randomUUID()}`;
    userIds.push(userId);
    await prisma.user.create({ data: { id: userId } });
    const material = await prisma.studyMaterial.create({
      data: { userId, title: "Fixture", kind: "WEB" },
    });
    const revision = await prisma.materialRevision.create({
      data: {
        userId,
        materialId: material.id,
        revisionNumber: 1,
        status: "QUEUED",
        processingMetadata: {
          retainedContext: true,
          ingestionDelivery: createMaterialIngestionDelivery(now),
        },
        updatedAt: new Date(now.getTime() - 600_000),
      },
    });
    const source = await prisma.sourceFile.create({
      data: {
        userId,
        materialRevisionId: revision.id,
        kind: "URL",
        status: "UPLOADED",
        originalName: "Fixture",
      },
    });
    return {
      userId,
      materialRevisionId: revision.id,
      materialId: material.id,
      sourceId: source.id,
      now,
    };
  }
  const sender = (
    run: MaterialIngestionEventSender["sendMaterialIngestionRequested"],
  ): MaterialIngestionEventSender => ({ sendMaterialIngestionRequested: run });

  it.each([
    { stage: "candidate", failures: [1], sendFails: false },
    { stage: "claim", failures: [2], sendFails: false },
    { stage: "pre-send", failures: [3], sendFails: false },
    { stage: "acknowledgement", failures: [4, 5], sendFails: false },
    { stage: "retry persistence", failures: [4], sendFails: true },
  ])(
    "keeps committed intent recoverable after a $stage database outage",
    async ({ failures, sendFails }) => {
      const input = await fixture();
      const sends: Array<{ eventId?: string; requestedAt: string }> = [];
      const original = prisma.$transaction.bind(prisma) as unknown as (
        operation: unknown,
        options?: unknown,
      ) => Promise<unknown>;
      const spy = vi.spyOn(prisma, "$transaction");
      let calls = 0;
      spy.mockImplementation((async (...args: unknown[]) => {
        calls += 1;
        if (failures.includes(calls))
          throw Object.assign(new Error("synthetic private database outage"), {
            code: "P2028",
          });
        return original(args[0], args[1]);
      }) as typeof prisma.$transaction);
      try {
        await expect(
          publishMaterialIngestion({
            ...input,
            sender: sender(async (payload, options) => {
              sends.push({
                eventId: options?.eventId,
                requestedAt: payload.requestedAt,
              });
              if (sendFails) throw new Error("synthetic lost response");
            }),
          }),
        ).resolves.toBe("pending");
      } finally {
        spy.mockRestore();
      }
      expect(
        await prisma.materialRevision.findUniqueOrThrow({
          where: { id: input.materialRevisionId },
        }),
      ).toMatchObject({ status: "QUEUED", errorCode: null });
      await prisma.materialRevision.update({
        where: { id: input.materialRevisionId },
        data: { updatedAt: new Date(0) },
      });
      await recoverMaterialIngestionPublications({
        now: new Date(now.getTime() + 600_000),
        sender: sender(async (payload, options) => {
          sends.push({
            eventId: options?.eventId,
            requestedAt: payload.requestedAt,
          });
        }),
      });
      expect(sends.length).toBeGreaterThanOrEqual(1);
      for (const send of sends) expect(send).toEqual(sends[0]);
      expect(
        (
          await prisma.materialRevision.findUniqueOrThrow({
            where: { id: input.materialRevisionId },
          })
        ).processingMetadata,
      ).toMatchObject({ ingestionDelivery: { status: "delivered" } });
    },
  );

  it("preserves the continuation limit instead of disguising it as pending publication", async () => {
    const input = await fixture();
    const error = new JobContinuationLimitError("fanout");
    await expect(
      publishMaterialIngestion({
        ...input,
        sender: sender(async () => {
          throw error;
        }),
      }),
    ).rejects.toBe(error);
  });

  it("recovers commit-before-send crashes with the stored identity and preserves context", async () => {
    const input = await fixture();
    const send = vi.fn().mockResolvedValue(undefined);
    expect(
      await recoverMaterialIngestionPublications({ now, sender: sender(send) }),
    ).toEqual({ attempted: 1, delivered: 1, failed: 0 });
    const revision = await prisma.materialRevision.findUniqueOrThrow({
      where: { id: input.materialRevisionId },
    });
    expect(revision.processingMetadata).toMatchObject({
      retainedContext: true,
      ingestionDelivery: { status: "delivered", attempt: 1 },
    });
    expect(send).toHaveBeenCalledWith(
      {
        userId: input.userId,
        materialRevisionId: input.materialRevisionId,
        requestedAt: now.toISOString(),
      },
      expect.objectContaining({
        eventId: expect.any(String),
        signal: expect.any(AbortSignal),
      }),
    );
  });

  it("keeps failed sends queued and repeats an ambiguous send with the same identity", async () => {
    const input = await fixture();
    const ids: Array<string | undefined> = [];
    expect(
      await publishMaterialIngestion({
        ...input,
        sender: sender(async (_payload, options) => {
          ids.push(options?.eventId);
          throw new Error("response lost after acceptance");
        }),
      }),
    ).toBe("pending");
    expect(
      await prisma.materialRevision.findUniqueOrThrow({
        where: { id: input.materialRevisionId },
      }),
    ).toMatchObject({ status: "QUEUED" });
    expect(
      await recoverMaterialIngestionPublications({
        now: new Date(now.getTime() + 360_000),
        sender: sender(async (_payload, options) => {
          ids.push(options?.eventId);
        }),
      }),
    ).toEqual({ attempted: 1, delivered: 1, failed: 0 });
    expect(ids).toHaveLength(2);
    expect(ids[0]).toBe(ids[1]);
  });

  it("does not overwrite a worker that started before the send response failed", async () => {
    const input = await fixture();
    await publishMaterialIngestion({
      ...input,
      sender: sender(async () => {
        await prisma.materialRevision.update({
          where: { id: input.materialRevisionId },
          data: { status: "PROCESSING" },
        });
        throw new Error("lost response");
      }),
    });
    expect(
      await prisma.materialRevision.findUniqueOrThrow({
        where: { id: input.materialRevisionId },
      }),
    ).toMatchObject({ status: "PROCESSING", errorCode: null });
  });

  it("claims publication once when maintainers overlap", async () => {
    await fixture();
    let started!: () => void;
    let finish!: () => void;
    const hasStarted = new Promise<void>((resolve) => {
      started = resolve;
    });
    const canFinish = new Promise<void>((resolve) => {
      finish = resolve;
    });
    const send = vi.fn(async () => {
      started();
      await canFinish;
    });
    const first = recoverMaterialIngestionPublications({
      now,
      sender: sender(send),
    });
    await hasStarted;
    expect(
      await recoverMaterialIngestionPublications({ now, sender: sender(send) }),
    ).toEqual({ attempted: 0, delivered: 0, failed: 0 });
    finish();
    await first;
    expect(send).toHaveBeenCalledTimes(1);
  });

  it("ignores deleted materials and account deletion tombstones", async () => {
    const input = await fixture();
    await prisma.studyMaterial.update({
      where: { id: input.materialId },
      data: { status: "DELETING" },
    });
    const send = vi.fn();
    expect(
      await publishMaterialIngestion({ ...input, sender: sender(send) }),
    ).toBe("superseded");
    expect(send).not.toHaveBeenCalled();
  });

  it("does not claim another owner's revision", async () => {
    const input = await fixture();
    const other = await fixture();
    const send = vi.fn();
    expect(
      await publishMaterialIngestion({
        ...input,
        userId: other.userId,
        sender: sender(send),
      }),
    ).toBe("superseded");
    expect(send).not.toHaveBeenCalled();
  });

  it("respects a deletion tombstone before access has been disabled", async () => {
    const input = await fixture();
    await prisma.accountDeletionJob.create({
      data: { userId: input.userId, manifest: {} },
    });
    const send = vi.fn();
    expect(
      await publishMaterialIngestion({ ...input, sender: sender(send) }),
    ).toBe("superseded");
    expect(
      await recoverMaterialIngestionPublications({ now, sender: sender(send) }),
    ).toEqual({ attempted: 0, delivered: 0, failed: 0 });
    expect(send).not.toHaveBeenCalled();
  });

  it("recovers an expired publisher lease after acceptance without changing identity", async () => {
    const input = await fixture();
    const delivery = createMaterialIngestionDelivery(
      new Date(now.getTime() - 600_000),
    );
    await prisma.materialRevision.update({
      where: { id: input.materialRevisionId },
      data: {
        processingMetadata: {
          ingestionDelivery: {
            ...delivery,
            status: "delivering",
            attempt: 1,
            leaseId: randomUUID(),
          },
        },
        updatedAt: new Date(now.getTime() - 600_000),
      },
    });
    const send = vi.fn().mockResolvedValue(undefined);
    expect(
      await recoverMaterialIngestionPublications({ now, sender: sender(send) }),
    ).toEqual({ attempted: 1, delivered: 1, failed: 0 });
    expect(send.mock.calls[0][1].eventId).toBe(delivery.eventId);
    expect(
      (
        await prisma.materialRevision.findUniqueOrThrow({
          where: { id: input.materialRevisionId },
        })
      ).processingMetadata,
    ).toMatchObject({ ingestionDelivery: { attempt: 2, status: "delivered" } });
  });

  it.each([
    { status: "unknown" },
    { status: "pending", nextAttemptAt: "broken" },
  ])(
    "makes malformed publication intent visible instead of stranding it: %j",
    async (malformed) => {
      const input = await fixture();
      await prisma.materialRevision.update({
        where: { id: input.materialRevisionId },
        data: {
          processingMetadata: { ingestionDelivery: malformed },
          updatedAt: new Date(now.getTime() - 600_000),
        },
      });
      const send = vi.fn();
      expect(
        await recoverMaterialIngestionPublications({
          now,
          sender: sender(send),
        }),
      ).toEqual({ attempted: 1, delivered: 0, failed: 1 });
      expect(
        await prisma.materialRevision.findUniqueOrThrow({
          where: { id: input.materialRevisionId },
        }),
      ).toMatchObject({ status: "FAILED", errorCode: "EVENT_SEND_FAILED" });
      expect(send).not.toHaveBeenCalled();
    },
  );

  it("leaves durable work pending when maintenance has no cleanup time", async () => {
    const input = await fixture();
    const send = vi.fn();
    expect(
      await recoverMaterialIngestionPublications({
        now,
        sender: sender(send),
        deadlineAt: new Date(Date.now() + 1_000),
      }),
    ).toEqual({ attempted: 0, delivered: 0, failed: 0 });
    expect(
      await prisma.materialRevision.findUniqueOrThrow({
        where: { id: input.materialRevisionId },
      }),
    ).toMatchObject({ status: "QUEUED" });
    expect(send).not.toHaveBeenCalled();
  });

  it("exhausts publication attempts visibly without an infinite retry loop", async () => {
    const input = await fixture();
    const send = vi.fn(async () => {
      throw new Error("queue unavailable");
    });
    for (let attempt = 0; attempt < 5; attempt += 1)
      await publishMaterialIngestion({
        ...input,
        now: new Date(now.getTime() + attempt * 360_000),
        sender: sender(send),
      });
    expect(send).toHaveBeenCalledTimes(5);
    expect(
      await prisma.materialRevision.findUniqueOrThrow({
        where: { id: input.materialRevisionId },
      }),
    ).toMatchObject({ status: "FAILED", errorCode: "EVENT_SEND_FAILED" });
    expect(
      await prisma.sourceFile.findUniqueOrThrow({
        where: { id: input.sourceId },
      }),
    ).toMatchObject({ status: "FAILED" });
    await publishMaterialIngestion({
      ...input,
      now: new Date(now.getTime() + 3_600_000),
      sender: sender(send),
    });
    expect(send).toHaveBeenCalledTimes(5);
  });

  it("prevents a stale publisher from overwriting a newer publication intent", async () => {
    const input = await fixture();
    await publishMaterialIngestion({
      ...input,
      sender: sender(async () => {
        await prisma.materialRevision.update({
          where: { id: input.materialRevisionId },
          data: {
            processingMetadata: {
              ingestionDelivery: createMaterialIngestionDelivery(
                new Date(now.getTime() + 1_000),
              ),
            },
          },
        });
        throw new Error("old response lost");
      }),
    });
    const revision = await prisma.materialRevision.findUniqueOrThrow({
      where: { id: input.materialRevisionId },
    });
    expect(revision.processingMetadata).toMatchObject({
      ingestionDelivery: {
        status: "pending",
        attempt: 0,
        requestedAt: new Date(now.getTime() + 1_000).toISOString(),
      },
    });
  });
});
