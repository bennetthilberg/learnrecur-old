import { randomUUID } from "node:crypto";
import { afterAll, describe, expect, it } from "vitest";
import { getPrisma } from "@/lib/prisma";
import {
  queueChoiceExerciseRefillForSkill,
  queueExactInputExerciseRefillForSkill,
  queueMathExerciseRefillForSkill,
  runChoiceExerciseRefillJob,
  runExactInputExerciseRefillJob,
  runMathExerciseRefillJob,
} from "@/lib/skills/refill-jobs";
import type { ExerciseRefillEventSender } from "@/lib/jobs/events";
import { createSkillFixture } from "./test-helpers";
import {
  recoverPendingRefillEvents,
  markRefillJobRetryableFailure,
} from "@/lib/skills/refill-jobs";
import { buildJobEnvelope } from "@/lib/jobs/contracts";
import { recoverInterruptedJob } from "@/lib/jobs/recovery";

const describeDatabase =
  process.env.RUN_DATABASE_TESTS === "1" ? describe : describe.skip;
const prisma = getPrisma();
const userIds: string[] = [];
const sender: ExerciseRefillEventSender = {
  async sendChoiceRefillRequested() {},
  async sendExactInputRefillRequested() {},
  async sendMathRefillRequested() {},
};
const families = [
  {
    kind: "choice",
    queue: queueChoiceExerciseRefillForSkill,
    run: runChoiceExerciseRefillJob,
    generator: "generateChoiceExercises",
    verifier: "verifyChoiceExercises",
    exercise: {
      prompt: "Which word names the color of grass?",
      choices: [
        { id: "a", label: "green" },
        { id: "b", label: "red" },
        { id: "c", label: "blue" },
      ],
      correctChoiceId: "a",
      explanation: "Grass is green.",
      expectedSeconds: 20,
    },
  },
  {
    kind: "exact-input",
    queue: queueExactInputExerciseRefillForSkill,
    run: runExactInputExerciseRefillJob,
    generator: "generateExactInputExercises",
    verifier: "verifyExactInputExercises",
    exercise: {
      prompt: "Name the color of grass.",
      answerKind: "TEXT",
      answerSpec: {
        kind: "text",
        policyVersion: 2,
        accepted: ["green"],
        normalizeCase: true,
        normalizeWhitespace: true,
        normalizeDiacritics: false,
      },
      correctAnswerDisplay: "green",
      explanation: "Grass is green.",
      expectedSeconds: 20,
    },
  },
  {
    kind: "math",
    queue: queueMathExerciseRefillForSkill,
    run: runMathExerciseRefillJob,
    generator: "generateMathExercises",
    verifier: "verifyMathExercises",
    exercise: {
      prompt: "Simplify x + x.",
      answerKind: "MATH",
      answerSpec: {
        kind: "math",
        acceptedExpressions: ["2x"],
        equivalence: "basic-symbolic",
      },
      correctAnswerDisplay: "2x",
      explanation: "Combine like terms.",
      expectedSeconds: 20,
    },
  },
] as const;

describeDatabase("refill reliability", () => {
  afterAll(async () => {
    await prisma.accountDeletionJob.deleteMany({
      where: { userId: { in: userIds } },
    });
    await prisma.user.deleteMany({ where: { id: { in: userIds } } });
    await prisma.$disconnect();
  });

  async function fixture(label: string) {
    const userId = `refill_reliable_${randomUUID()}_${label}`;
    userIds.push(userId);
    await prisma.user.create({ data: { id: userId } });
    const skill = await createSkillFixture(prisma, {
      userId,
      title: "Reliable skill",
      repetitions: 5,
    });
    await prisma.skill.update({
      where: { id: skill.id },
      data: { alreadyStudied: true },
    });
    return {
      userId,
      skillId: skill.id,
      now: new Date(),
      targetReadyCount: 1,
      sender,
    };
  }

  it.each(families)(
    "retries a temporary $kind provider outage on the same job without learner writes",
    async (family) => {
      const input = await fixture(family.kind);
      const beforeSkill = await prisma.skill.findUniqueOrThrow({
        where: { id: input.skillId },
      });
      const queued = await family.queue(input);
      if (queued.status !== "queued") throw new Error(queued.message);
      const payload = {
        ...input,
        generationJobId: queued.generationJobId,
        requestedAt: input.now.toISOString(),
      };
      const temporary = Object.assign(new Error("synthetic temporary outage"), {
        code: 503,
      });
      const verification = async ({
        candidates,
      }: {
        candidates: Array<{ candidateId: string }>;
      }) => ({
        verifications: candidates.map((candidate) => ({
          candidateId: candidate.candidateId,
          verdict: "verified",
        })),
      });
      await expect(
        family.run({
          ...payload,
          [family.generator]: async () => {
            throw temporary;
          },
          [family.verifier]: verification,
        }),
      ).rejects.toMatchObject({ retryable: true });
      expect(
        await prisma.generationJob.findUniqueOrThrow({
          where: { id: queued.generationJobId },
        }),
      ).toMatchObject({ status: "FAILED", checkpoint: "retryable-exception" });
      expect(await family.queue({ ...input, now: new Date() })).toMatchObject({
        status: "not-queued",
        reason: "job-in-progress",
        generationJobId: queued.generationJobId,
      });
      const result = await family.run({
        ...payload,
        now: new Date(),
        [family.generator]: async () => ({ exercises: [family.exercise] }),
        [family.verifier]: verification,
      });
      expect(result).toMatchObject({
        status: "refilled",
        generationJobId: queued.generationJobId,
        exerciseCount: 1,
      });
      if (family.kind === "choice") {
        expect(
          await prisma.generationAuditRecord.findFirstOrThrow({
            where: {
              jobId: queued.generationJobId,
              eventKey: "publication-complete",
            },
          }),
        ).toMatchObject({ attempt: 2, retryCount: 1 });
      }
      expect(
        await prisma.generationJob.count({ where: { skillId: input.skillId } }),
      ).toBe(1);
      expect(
        await prisma.exercise.count({ where: { skillId: input.skillId } }),
      ).toBe(1);
      const afterSkill = await prisma.skill.findUniqueOrThrow({
        where: { id: input.skillId },
      });
      for (const field of [
        "firstIntroducedAt",
        "repetitions",
        "dueAt",
        "stability",
        "difficulty",
        "lapses",
        "fsrsState",
      ] as const)
        expect(afterSkill[field]).toEqual(beforeSkill[field]);
      expect(
        await prisma.exerciseAttempt.count({ where: { userId: input.userId } }),
      ).toBe(0);
      expect(
        await prisma.reviewLog.count({ where: { userId: input.userId } }),
      ).toBe(0);
    },
  );

  it("keeps a failed initial publication pending for scheduled recovery", async () => {
    const input = await fixture("send-failure");
    const result = await queueChoiceExerciseRefillForSkill({
      ...input,
      sender: {
        ...sender,
        async sendChoiceRefillRequested() {
          throw new Error("temporary queue outage");
        },
      },
    });
    expect(result.status).toBe("queued");
    if (result.status !== "queued") throw new Error(result.message);
    expect(
      await prisma.generationJob.findUniqueOrThrow({
        where: { id: result.generationJobId },
      }),
    ).toMatchObject({ status: "PENDING", checkpoint: "event-pending" });
  });
  it("accepts a late ambiguous delivery after publication retries exhaust", async () => {
    const input = await fixture("late-after-publication-exhaustion");
    const failingSender = {
      ...sender,
      async sendChoiceRefillRequested() {
        throw new Error("ambiguous send");
      },
    };
    const queued = await queueChoiceExerciseRefillForSkill({
      ...input,
      sender: failingSender,
    });
    if (queued.status !== "queued") throw new Error(queued.message);
    for (let attempt = 1; attempt < 5; attempt += 1)
      await recoverPendingRefillEvents({
        now: new Date(input.now.getTime() + attempt * 360_000),
        generationJobId: queued.generationJobId,
        userId: input.userId,
        sender: failingSender,
      });
    expect(
      await prisma.generationJob.findUniqueOrThrow({
        where: { id: queued.generationJobId },
      }),
    ).toMatchObject({
      status: "FAILED",
      checkpoint: "event-send-failed",
      attemptCount: 0,
    });
    const family = families[0];
    const result = await family.run({
      ...input,
      generationJobId: queued.generationJobId,
      requestedAt: input.now.toISOString(),
      generateChoiceExercises: async () => ({ exercises: [family.exercise] }),
      verifyChoiceExercises: async ({ candidates }) => ({
        verifications: candidates.map((candidate) => ({
          candidateId: candidate.candidateId,
          verdict: "verified",
        })),
      }),
    });
    expect(result.status).toBe("refilled");
    expect(
      await prisma.exercise.count({ where: { skillId: input.skillId } }),
    ).toBe(1);
  });

  it.each(families)(
    "rejects late $kind publication from a superseded attempt",
    async (family) => {
      const input = await fixture(`late-${family.kind}`);
      const queued = await family.queue(input);
      if (queued.status !== "queued") throw new Error(queued.message);
      const payload = {
        ...input,
        generationJobId: queued.generationJobId,
        requestedAt: input.now.toISOString(),
      };
      let started!: () => void;
      let finish!: () => void;
      const hasStarted = new Promise<void>((resolve) => {
        started = resolve;
      });
      const canFinish = new Promise<void>((resolve) => {
        finish = resolve;
      });
      const verification = async ({
        candidates,
      }: {
        candidates: Array<{ candidateId: string }>;
      }) => ({
        verifications: candidates.map((candidate) => ({
          candidateId: candidate.candidateId,
          verdict: "verified",
        })),
      });
      const old = family.run({
        ...payload,
        [family.generator]: async () => {
          started();
          await canFinish;
          return { exercises: [family.exercise] };
        },
        [family.verifier]: verification,
      });
      await hasStarted;
      await prisma.generationJob.update({
        where: { id: queued.generationJobId },
        data: {
          status: "FAILED",
          stage: "FAILED",
          checkpoint: "retryable-exception",
        },
      });
      const current = await family.run({
        ...payload,
        [family.generator]: async () => ({ exercises: [family.exercise] }),
        [family.verifier]: verification,
      });
      expect(current.status).toBe("refilled");
      const beforeLate = await prisma.generationJob.findUniqueOrThrow({
        where: { id: queued.generationJobId },
      });
      finish();
      expect((await old).status).toBe("not-refilled");
      expect(
        await prisma.generationJob.findUniqueOrThrow({
          where: { id: queued.generationJobId },
        }),
      ).toEqual(beforeLate);
      expect(
        await prisma.exercise.count({ where: { skillId: input.skillId } }),
      ).toBe(1);
    },
  );

  it.each(families)(
    "reclaims a killed $kind attempt through delivery recovery and keeps retries bounded",
    async (family) => {
      const input = await fixture(`killed-${family.kind}`);
      const queued = await family.queue(input);
      if (queued.status !== "queued") throw new Error(queued.message);
      await prisma.generationJob.update({
        where: { id: queued.generationJobId },
        data: { status: "RUNNING", attemptCount: 1, startedAt: new Date() },
      });
      let calls = 0;
      const payload = {
        ...input,
        generationJobId: queued.generationJobId,
        requestedAt: input.now.toISOString(),
        [family.generator]: async () => {
          calls += 1;
          throw Object.assign(new Error("outage"), { code: 503 });
        },
      };
      expect((await family.run(payload)).status).toBe("not-refilled");
      expect(calls).toBe(0);
      await prisma.generationJob.update({
        where: { id: queued.generationJobId },
        data: { startedAt: new Date(Date.now() - 720_000) },
      });
      // Dispatch invokes recovery only after the delivery ledger grants a new
      // lease. The ledger's lease-expiry contract is tested in jobs-delivery.
      await recoverInterruptedJob(
        buildJobEnvelope(
          `learnrecur/${family.kind}-refill.requested`,
          {
            userId: input.userId,
            skillId: input.skillId,
            generationJobId: queued.generationJobId,
            targetReadyCount: 1,
            requestedAt: input.now.toISOString(),
          },
          "staging",
        ),
      );
      await expect(family.run(payload)).rejects.toMatchObject({
        retryable: true,
      });
      expect(
        await prisma.generationJob.findUniqueOrThrow({
          where: { id: queued.generationJobId },
        }),
      ).toMatchObject({ attemptCount: 2, retryCount: 1 });
      let finalError: unknown;
      try {
        await family.run(payload);
      } catch (error) {
        finalError = error;
      }
      expect(finalError).toMatchObject({ retryable: true });
      await markRefillJobRetryableFailure({
        ...input,
        generationJobId: queued.generationJobId,
        error: finalError,
        now: new Date(),
        terminal: true,
      });
      expect(
        await prisma.generationJob.findUniqueOrThrow({
          where: { id: queued.generationJobId },
        }),
      ).toMatchObject({
        status: "FAILED",
        checkpoint: "retry-exhausted",
        attemptCount: 3,
        retryCount: 2,
      });
      expect((await family.run(payload)).status).toBe("not-refilled");
      expect(calls).toBe(2);
      expect(
        await prisma.exercise.count({ where: { skillId: input.skillId } }),
      ).toBe(0);
    },
  );

  it.each(families)(
    "keeps terminal $kind provider and quality failures out of automatic retries",
    async (family) => {
      for (const mode of ["auth", "quality"] as const) {
        const input = await fixture(`${mode}-${family.kind}`);
        const queued = await family.queue(input);
        if (queued.status !== "queued") throw new Error(queued.message);
        const payload = {
          ...input,
          generationJobId: queued.generationJobId,
          requestedAt: input.now.toISOString(),
        };
        const result = await family.run({
          ...payload,
          [family.generator]: async () => {
            if (mode === "auth")
              throw Object.assign(new Error("unauthorized"), { code: 401 });
            return { exercises: [family.exercise] };
          },
          [family.verifier]: async ({
            candidates,
          }: {
            candidates: Array<{ candidateId: string }>;
          }) => ({
            verifications: candidates.map((candidate) => ({
              candidateId: candidate.candidateId,
              verdict: "rejected",
              reason: "synthetic rejection",
            })),
          }),
        });
        expect(result.status).toBe("not-refilled");
        expect(
          await prisma.generationJob.findUniqueOrThrow({
            where: { id: queued.generationJobId },
          }),
        ).toMatchObject({ status: "FAILED", checkpoint: "failed" });
        expect(
          await prisma.exercise.count({ where: { skillId: input.skillId } }),
        ).toBe(0);
      }
    },
  );

  it.each(families)(
    "does not publish $kind results after account deletion is requested",
    async (family) => {
      const input = await fixture(`deleted-during-generation-${family.kind}`);
      const queued = await family.queue(input);
      if (queued.status !== "queued") throw new Error(queued.message);
      const result = await family.run({
        ...input,
        generationJobId: queued.generationJobId,
        requestedAt: input.now.toISOString(),
        [family.generator]: async () => {
          await prisma.accountDeletionJob.create({
            data: { userId: input.userId, manifest: {} },
          });
          return { exercises: [family.exercise] };
        },
        [family.verifier]: async ({
          candidates,
        }: {
          candidates: Array<{ candidateId: string }>;
        }) => ({
          verifications: candidates.map((candidate) => ({
            candidateId: candidate.candidateId,
            verdict: "verified",
          })),
        }),
      });
      expect(result.status).toBe("not-refilled");
      expect(
        await prisma.exercise.count({ where: { skillId: input.skillId } }),
      ).toBe(0);
    },
  );
});
