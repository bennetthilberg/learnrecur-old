import { randomUUID } from "node:crypto";

import { afterAll, beforeAll, describe, expect, it } from "vitest";

import {
  GenerationJobKind,
  GenerationJobStage,
  GenerationJobStatus,
  Prisma,
} from "@/generated/prisma/client";
import type {
  ExerciseRefillEventPayload,
  ExerciseRefillEventSender,
} from "@/lib/jobs/events";
import { getPrisma } from "@/lib/prisma";
import { recoverPendingRefillEvents } from "@/lib/skills/refill-jobs";

import { createSkillFixture } from "./test-helpers";

const describeDatabase = process.env.RUN_DATABASE_TESTS === "1" ? describe : describe.skip;
const runId = `refill_delivery_${randomUUID()}`;
const now = new Date("2026-09-08T18:00:00.000Z");
const staleUpdatedAt = new Date(now.getTime() - 10 * 60 * 1_000);

describeDatabase("refill delivery recovery", () => {
  const prisma = getPrisma();
  const userIds: string[] = [];

  beforeAll(async () => {
    await prisma.$queryRaw`SELECT 1`;
  });

  afterAll(async () => {
    await prisma.accountDeletionJob.deleteMany({
      where: { userId: { in: userIds } },
    });
    await prisma.user.deleteMany({ where: { id: { in: userIds } } });
    await prisma.$disconnect();
  });

  async function createPendingJob(
    label: string,
    options: {
      checkpoint?: string;
      attempt?: number;
      status?: GenerationJobStatus;
      updatedAt?: Date;
      stageMetrics?: Prisma.InputJsonValue | typeof Prisma.DbNull;
    } = {},
  ) {
    const userId = `${runId}_${label}`;
    userIds.push(userId);
    await prisma.user.create({ data: { id: userId } });
    const skill = await createSkillFixture(prisma, {
      userId,
      title: `Recovery ${label}`,
    });
    const stageMetrics = {
      refillDelivery: {
        kind: "choice",
        targetReadyCount: 5,
        requestedAt: staleUpdatedAt.toISOString(),
        attempt: options.attempt ?? 0,
        nextAttemptAt: staleUpdatedAt.toISOString(),
      },
    } satisfies Prisma.InputJsonValue;
    const generationJob = await prisma.generationJob.create({
      data: {
        userId,
        skillId: skill.id,
        kind: GenerationJobKind.CHOICE_EXERCISE_GENERATION,
        status: options.status ?? GenerationJobStatus.PENDING,
        stage: GenerationJobStage.QUEUED,
        checkpoint: options.checkpoint ?? "event-pending",
        provider: "test",
        model: "fixture",
        promptVersion: "fixture-v1",
        requestedCount: 1,
        createdAt: staleUpdatedAt,
        updatedAt: options.updatedAt ?? staleUpdatedAt,
        stageMetrics: "stageMetrics" in options ? options.stageMetrics : stageMetrics,
      },
    });
    return { userId, skill, generationJob };
  }

  function senderFor(
    onChoice: (payload: ExerciseRefillEventPayload) => Promise<void> | void,
  ): ExerciseRefillEventSender {
    return {
      sendChoiceRefillRequested: async (payload) => onChoice(payload),
      async sendExactInputRefillRequested() {
        throw new Error("unexpected exact-input refill");
      },
      async sendMathRefillRequested() {
        throw new Error("unexpected math refill");
      },
    };
  }

  it("recovers a committed event after the publishing process exits", async () => {
    const fixture = await createPendingJob("crash-before-publish");
    const payloads: ExerciseRefillEventPayload[] = [];

    await expect(
      recoverPendingRefillEvents({
        now,
        sender: senderFor((payload) => {
          payloads.push(payload);
        }),
      }),
    ).resolves.toEqual({ attempted: 1, delivered: 1, failed: 0 });

    expect(payloads).toEqual([{
      userId: fixture.userId,
      skillId: fixture.skill.id,
      generationJobId: fixture.generationJob.id,
      targetReadyCount: 5,
      requestedAt: staleUpdatedAt.toISOString(),
    }]);
    await expect(
      prisma.generationJob.findUniqueOrThrow({
        where: { id: fixture.generationJob.id },
        select: { status: true, checkpoint: true },
      }),
    ).resolves.toEqual({
      status: GenerationJobStatus.PENDING,
      checkpoint: "event-dispatched",
    });
  });

  it("claims a stale delivery lease once when maintenance overlaps", async () => {
    const fixture = await createPendingJob("overlap", {
      checkpoint: "event-delivery",
      attempt: 1,
    });
    const payloads: ExerciseRefillEventPayload[] = [];
    const sender = senderFor(async (payload) => {
      payloads.push(payload);
      await new Promise((resolve) => setTimeout(resolve, 10));
    });

    const results = await Promise.all([
      recoverPendingRefillEvents({ now, sender }),
      recoverPendingRefillEvents({ now, sender }),
    ]);

    expect(results).toHaveLength(2);
    expect(results).toEqual(expect.arrayContaining([
      { attempted: 1, delivered: 1, failed: 0 },
      { attempted: 0, delivered: 0, failed: 0 },
    ]));
    expect(payloads).toHaveLength(1);
    await expect(
      prisma.generationJob.findUniqueOrThrow({
        where: { id: fixture.generationJob.id },
        select: { status: true, checkpoint: true },
      }),
    ).resolves.toMatchObject({
      status: GenerationJobStatus.PENDING,
      checkpoint: "event-dispatched",
    });
  });

  it("does not rewrite a job that advances while delivery is in flight", async () => {
    const fixture = await createPendingJob("advanced-during-send");
    const sender = senderFor(async () => {
      await prisma.generationJob.update({
        where: { id: fixture.generationJob.id },
        data: {
          status: GenerationJobStatus.RUNNING,
          stage: GenerationJobStage.GENERATING,
          checkpoint: "claimed",
        },
      });
    });

    await expect(recoverPendingRefillEvents({ now, sender })).resolves.toEqual({
      attempted: 1,
      delivered: 0,
      failed: 0,
    });
    await expect(
      prisma.generationJob.findUniqueOrThrow({
        where: { id: fixture.generationJob.id },
        select: { status: true, stage: true, checkpoint: true },
      }),
    ).resolves.toEqual({
      status: GenerationJobStatus.RUNNING,
      stage: GenerationJobStage.GENERATING,
      checkpoint: "claimed",
    });
  });

  it("does not let a stale publisher finish after another maintainer reclaims its lease", async () => {
    const fixture = await createPendingJob("stale-publisher");
    let senderStarted!: () => void;
    const senderHasStarted = new Promise<void>((resolve) => {
      senderStarted = resolve;
    });
    let releaseSender!: () => void;
    const senderCanFinish = new Promise<void>((resolve) => {
      releaseSender = resolve;
    });
    const firstSender = senderFor(async () => {
      senderStarted();
      await senderCanFinish;
    });
    const firstRecovery = recoverPendingRefillEvents({ now, sender: firstSender });
    await senderHasStarted;

    await prisma.generationJob.update({
      where: { id: fixture.generationJob.id },
      data: {
        updatedAt: staleUpdatedAt,
        stageMetrics: {
          refillDelivery: {
            kind: "choice",
            targetReadyCount: 5,
            requestedAt: staleUpdatedAt.toISOString(),
            attempt: 1,
            nextAttemptAt: staleUpdatedAt.toISOString(),
            leaseId: "reclaimed-lease",
          },
        },
      },
    });
    const secondRecovery = await recoverPendingRefillEvents({
      now,
      sender: senderFor(() => undefined),
    });
    releaseSender();
    const firstResult = await firstRecovery;

    expect(secondRecovery).toEqual({ attempted: 1, delivered: 1, failed: 0 });
    expect(firstResult).toEqual({ attempted: 1, delivered: 0, failed: 0 });
    await expect(
      prisma.generationJob.findUniqueOrThrow({
        where: { id: fixture.generationJob.id },
        select: { status: true, checkpoint: true },
      }),
    ).resolves.toMatchObject({
      status: GenerationJobStatus.PENDING,
      checkpoint: "event-dispatched",
    });
  });

  it("makes exhausted delivery attempts visible as a repairable failure", async () => {
    const fixture = await createPendingJob("exhausted", { attempt: 5 });

    await expect(
      recoverPendingRefillEvents({
        now,
        sender: senderFor(() => {
          throw new Error("must not send after the delivery budget");
        }),
      }),
    ).resolves.toEqual({ attempted: 0, delivered: 0, failed: 1 });
    await expect(
      prisma.generationJob.findUniqueOrThrow({
        where: { id: fixture.generationJob.id },
        select: { status: true, stage: true, checkpoint: true, errorMessage: true },
      }),
    ).resolves.toMatchObject({
      status: GenerationJobStatus.FAILED,
      stage: GenerationJobStage.FAILED,
      checkpoint: "event-send-failed",
      errorMessage: "Exercise preparation delivery could not be recovered. Retry the repair.",
    });
  });

  it("leaves legacy delivery rows unchanged without starving recoverable rows", async () => {
    const legacyFixtures = [];
    for (let index = 0; index < 25; index += 1) {
      legacyFixtures.push(
        await createPendingJob(`legacy-${index}`, {
          stageMetrics:
            index % 2 === 0
              ? Prisma.DbNull
              : ({} satisfies Prisma.InputJsonValue),
        }),
      );
    }
    const recoverable = await createPendingJob("legacy-batch-recoverable");
    const legacyIds = legacyFixtures.map(({ generationJob }) => generationJob.id);
    const legacyBefore = await prisma.generationJob.findMany({
      where: { id: { in: legacyIds } },
      orderBy: { id: "asc" },
      select: {
        id: true,
        status: true,
        stage: true,
        checkpoint: true,
        stageMetrics: true,
        errorMessage: true,
        updatedAt: true,
      },
    });
    const payloads: ExerciseRefillEventPayload[] = [];

    await expect(
      recoverPendingRefillEvents({
        now,
        sender: senderFor((payload) => {
          payloads.push(payload);
        }),
      }),
    ).resolves.toEqual({ attempted: 1, delivered: 1, failed: 0 });

    expect(payloads).toEqual([{
      userId: recoverable.userId,
      skillId: recoverable.skill.id,
      generationJobId: recoverable.generationJob.id,
      targetReadyCount: 5,
      requestedAt: staleUpdatedAt.toISOString(),
    }]);
    await expect(
      prisma.generationJob.findMany({
        where: { id: { in: legacyIds } },
        orderBy: { id: "asc" },
        select: {
          id: true,
          status: true,
          stage: true,
          checkpoint: true,
          stageMetrics: true,
          errorMessage: true,
          updatedAt: true,
        },
      }),
    ).resolves.toEqual(legacyBefore);
  });

  it("fails malformed present delivery metadata without treating it as legacy", async () => {
    const fixture = await createPendingJob("malformed-delivery-metadata", {
      stageMetrics: {
        refillDelivery: {
          kind: "choice",
          targetReadyCount: 0,
        },
      },
    });

    await expect(
      recoverPendingRefillEvents({
        now,
        sender: senderFor(() => {
          throw new Error("must not send malformed delivery metadata");
        }),
      }),
    ).resolves.toEqual({ attempted: 0, delivered: 0, failed: 1 });
    await expect(
      prisma.generationJob.findUniqueOrThrow({
        where: { id: fixture.generationJob.id },
        select: { status: true, stage: true, checkpoint: true },
      }),
    ).resolves.toEqual({
      status: GenerationJobStatus.FAILED,
      stage: GenerationJobStage.FAILED,
      checkpoint: "event-send-failed",
    });
  });

  it("does not deliver a refill after a deletion tombstone is created", async () => {
    const fixture = await createPendingJob("deletion-tombstone");
    await prisma.accountDeletionJob.create({
      data: {
        userId: fixture.userId,
        manifest: { version: 1 },
        nextAttemptAt: now,
      },
    });
    let sendCount = 0;

    await expect(
      recoverPendingRefillEvents({
        now,
        sender: senderFor(() => {
          sendCount += 1;
        }),
      }),
    ).resolves.toEqual({ attempted: 0, delivered: 0, failed: 0 });

    expect(sendCount).toBe(0);
    await expect(
      prisma.generationJob.findUniqueOrThrow({
        where: { id: fixture.generationJob.id },
        select: { status: true, checkpoint: true },
      }),
    ).resolves.toEqual({
      status: GenerationJobStatus.PENDING,
      checkpoint: "event-pending",
    });
  });

  it("preserves other diagnostics while retrying a publication", async () => {
    const fixture = await createPendingJob("retained-diagnostics");
    await prisma.generationJob.update({ where: { id: fixture.generationJob.id }, data: { stageMetrics: { ...(fixture.generationJob.stageMetrics as Prisma.JsonObject), retainedDiagnostic: true }, updatedAt: staleUpdatedAt } });
    await recoverPendingRefillEvents({ now, sender: senderFor(() => { throw new Error("temporary transport failure"); }) });
    expect((await prisma.generationJob.findUniqueOrThrow({ where: { id: fixture.generationJob.id } })).stageMetrics).toMatchObject({ retainedDiagnostic: true, refillDelivery: { attempt: 1 } });
  });

  it("rejects a stored event whose family does not match its generation job", async () => {
    const fixture = await createPendingJob("mismatched-family", { stageMetrics: { refillDelivery: { kind: "math", targetReadyCount: 1, requestedAt: staleUpdatedAt.toISOString(), attempt: 0, nextAttemptAt: staleUpdatedAt.toISOString() } } });
    await recoverPendingRefillEvents({ now, sender: senderFor(() => { throw new Error("must not send"); }) });
    expect(await prisma.generationJob.findUniqueOrThrow({ where: { id: fixture.generationJob.id } })).toMatchObject({ status: "FAILED", checkpoint: "event-send-failed" });
  });
});
