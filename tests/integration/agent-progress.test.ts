import { randomUUID } from "node:crypto";

import { afterAll, beforeAll, describe, expect, it } from "vitest";

import {
  ExerciseAttemptResult,
  ExerciseFlagReason,
  FsrsRating,
  GenerationJobKind,
  GenerationJobStage,
  GenerationJobStatus,
} from "@/generated/prisma/client";
import {
  type AgentAccessScope,
  type AgentAuthContext,
} from "@/lib/agent-access/auth";
import {
  getAgentProgressSummary,
  getAgentReadiness,
  repairAgentReadiness,
} from "@/lib/agent-access/progress";
import type { ExerciseRefillEventSender } from "@/lib/jobs/events";
import {
  publishDeferredExerciseRefillEvent,
  queueChoiceExerciseRefillForSkill,
} from "@/lib/skills/refill-jobs";
import {
  EXACT_TEXT_POLICY,
  NATURAL_TEXT_POLICY,
  textAnswerContract,
} from "@/lib/practice/policies";
import { getPrisma } from "@/lib/prisma";

import {
  createChoiceExercise,
  createSkillFixture,
  createTextExercise,
} from "./test-helpers";

const suite = process.env.RUN_DATABASE_TESTS === "1" ? describe : describe.skip;

suite("agent progress read model", () => {
  const prisma = getPrisma();
  const userIds: string[] = [];
  const now = new Date("2026-09-08T18:00:00.000Z");

  async function createAuth(): Promise<AgentAuthContext> {
    const userId = `agent_progress_${randomUUID()}`;
    userIds.push(userId);
    await prisma.user.create({ data: { id: userId } });
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
        workosApplicationId: `app_${userId}`,
        clientId: "https://agent.example/client.json",
        clientName: "Progress test agent",
        clientDomain: "agent.example",
        resourceUrl: "https://learnrecur.com/mcp",
        scopes: ["progress:read", "skills:write"] satisfies AgentAccessScope[],
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
      scopes: ["progress:read", "skills:write"],
    };
  }

  beforeAll(async () => {
    await prisma.$queryRaw`SELECT 1`;
  });

  afterAll(async () => {
    await prisma.user.deleteMany({ where: { id: { in: userIds } } });
    await prisma.$disconnect();
  });

  it("uses alreadyStudied and effective text policy in readiness and due totals", async () => {
    const auth = await createAuth();
    await prisma.user.update({
      where: { id: auth.userId },
      data: {
        dailyNewSkillLimit: 3,
        practiceTimezone: "America/Chicago",
        practiceDayStartMinutes: 270,
      },
    });
    const locked = await createSkillFixture(prisma, {
      userId: auth.userId,
      title: "Locked exact input",
      dueAt: new Date("2026-09-07T09:00:00.000Z"),
      repetitions: 0,
    });
    await createTextExercise(prisma, auth.userId, locked.id, {
      answerSpec: textAnswerContract(["right"], NATURAL_TEXT_POLICY),
    });

    const introduced = await createSkillFixture(prisma, {
      userId: auth.userId,
      title: "Introduced exact input",
      dueAt: new Date("2026-09-07T09:00:00.000Z"),
      repetitions: 0,
    });
    await prisma.skill.update({
      where: { id: introduced.id },
      data: { alreadyStudied: true },
    });
    await createTextExercise(prisma, auth.userId, introduced.id, {
      answerSpec: textAnswerContract(["right"], NATURAL_TEXT_POLICY),
    });

    const exactCollection = await prisma.collection.create({
      data: {
        userId: auth.userId,
        name: "Exact policy",
        textPolicy: EXACT_TEXT_POLICY,
      },
    });
    const incompatible = await createSkillFixture(prisma, {
      userId: auth.userId,
      title: "Incompatible text inventory",
      collectionId: exactCollection.id,
      dueAt: new Date("2026-09-07T09:00:00.000Z"),
      repetitions: 3,
    });
    await createTextExercise(prisma, auth.userId, incompatible.id, {
      answerSpec: textAnswerContract(["right"], NATURAL_TEXT_POLICY),
    });

    await expect(
      getAgentReadiness(auth, { skill_id: locked.id }),
    ).resolves.toMatchObject({
      skills: [
        {
          skill_id: locked.id,
          ready_exercise_count: 0,
          status_reason: "needs_preparation",
        },
      ],
    });
    await expect(
      getAgentReadiness(auth, { skill_id: introduced.id }),
    ).resolves.toMatchObject({
      skills: [
        {
          skill_id: introduced.id,
          ready_exercise_count: 1,
          status_reason: "ready",
        },
      ],
    });
    await expect(
      getAgentReadiness(auth, { skill_id: incompatible.id }),
    ).resolves.toMatchObject({
      skills: [
        {
          skill_id: incompatible.id,
          ready_exercise_count: 0,
          status_reason: "needs_preparation",
        },
      ],
    });

    await expect(
      getAgentProgressSummary(auth, { collection_id: exactCollection.id }),
    ).resolves.toMatchObject({
      new_skill_allowance: {
        limit: 3,
        timezone: "America/Chicago",
        day_start_minutes: 270,
      },
      due: {
        skill_count: 1,
        ready_skill_count: 0,
        waiting_for_preparation_count: 1,
      },
      totals: { active_skill_count: 1 },
    });
  });

  it("derives trouble spots from independent scheduled evidence and scopes quality work to a collection", async () => {
    const auth = await createAuth();
    const includedCollection = await prisma.collection.create({
      data: { userId: auth.userId, name: "Included progress collection" },
    });
    const excludedCollection = await prisma.collection.create({
      data: { userId: auth.userId, name: "Excluded progress collection" },
    });
    const practiceOnlyNoise = await createSkillFixture(prisma, {
      userId: auth.userId,
      title: "Practice only noise",
      collectionId: includedCollection.id,
      dueAt: new Date("2026-09-07T09:00:00.000Z"),
      repetitions: 5,
    });
    const invalidatedNoise = await createSkillFixture(prisma, {
      userId: auth.userId,
      title: "Invalidated noise",
      collectionId: includedCollection.id,
      dueAt: new Date("2026-09-07T09:00:00.000Z"),
      repetitions: 5,
    });
    const excluded = await createSkillFixture(prisma, {
      userId: auth.userId,
      title: "Excluded quality work",
      collectionId: excludedCollection.id,
      dueAt: new Date("2026-09-07T09:00:00.000Z"),
      repetitions: 5,
    });

    await createReviewHistory(prisma, auth.userId, practiceOnlyNoise.id, now, [
      {},
      {},
      { practiceOnly: true },
    ]);
    const invalidatedReviews = await createReviewHistory(
      prisma,
      auth.userId,
      invalidatedNoise.id,
      now,
      [{}, {}, {}],
    );
    await prisma.exerciseFlag.create({
      data: {
        userId: auth.userId,
        exerciseId: invalidatedReviews.at(-1)!.exerciseId,
        reason: ExerciseFlagReason.UNCLEAR_PROMPT,
        practiceEvidenceNeedsCorrection: true,
        evidenceCorrectionStatus: "PENDING",
      },
    });
    const excludedExercise = await createChoiceExercise({
      prisma,
      userId: auth.userId,
      skillId: excluded.id,
    });
    await prisma.exerciseFlag.create({
      data: {
        userId: auth.userId,
        exerciseId: excludedExercise.id,
        reason: ExerciseFlagReason.UNCLEAR_PROMPT,
      },
    });
    await prisma.generationJob.create({
      data: {
        userId: auth.userId,
        skillId: excluded.id,
        kind: GenerationJobKind.CHOICE_EXERCISE_GENERATION,
        status: GenerationJobStatus.FAILED,
        stage: GenerationJobStage.FAILED,
        provider: "test",
        model: "fixture",
        promptVersion: "fixture-v1",
        requestedCount: 1,
      },
    });
    const includedExercise = await createChoiceExercise({
      prisma,
      userId: auth.userId,
      skillId: practiceOnlyNoise.id,
    });
    await prisma.exerciseFlag.create({
      data: {
        userId: auth.userId,
        exerciseId: includedExercise.id,
        reason: ExerciseFlagReason.UNCLEAR_PROMPT,
      },
    });
    await prisma.generationJob.create({
      data: {
        userId: auth.userId,
        skillId: practiceOnlyNoise.id,
        kind: GenerationJobKind.CHOICE_EXERCISE_GENERATION,
        status: GenerationJobStatus.FAILED,
        stage: GenerationJobStage.FAILED,
        provider: "test",
        model: "fixture",
        promptVersion: "fixture-v1",
        requestedCount: 1,
      },
    });

    const summary = await getAgentProgressSummary(auth, {
      collection_id: includedCollection.id,
      recent_limit: 20,
    });
    expect(summary.quality).toEqual({ open_flag_count: 2 });
    expect(summary.preparation).toEqual({
      pending_job_count: 0,
      failed_job_count: 1,
    });
    expect(summary.trouble_spots).toEqual([]);
  });

  it("merges omitted guidance fields from the locked current skill", async () => {
    const auth = await createAuth();
    const skill = await createSkillFixture(prisma, {
      userId: auth.userId,
      title: "Guidance repair",
    });
    await prisma.skill.update({
      where: { id: skill.id },
      data: {
        rules: { items: ["Keep the original rule."] },
        examples: { items: ["Keep the original example."] },
        exerciseConstraints: {
          notes: "Keep the original constraint.",
          answerKind: "choice",
          requestedCount: 5,
        },
      },
    });

    await expect(
      repairAgentReadiness(auth, {
        skill_id: skill.id,
        action: "update_guidance",
        rules: ["Use the repaired rule."],
      }),
    ).resolves.toMatchObject({
      status: "updated",
      action: "update_guidance",
      skill_id: skill.id,
    });

    await expect(
      prisma.skill.findUniqueOrThrow({
        where: { id: skill.id },
        select: { rules: true, examples: true, exerciseConstraints: true },
      }),
    ).resolves.toMatchObject({
      rules: { items: ["Use the repaired rule."] },
      examples: { items: ["Keep the original example."] },
      exerciseConstraints: {
        notes: "Keep the original constraint.",
        answerKind: "choice",
        requestedCount: 5,
      },
    });
  });

  it("does not flag an exercise from a different owned skill", async () => {
    const auth = await createAuth();
    const requestedSkill = await createSkillFixture(prisma, {
      userId: auth.userId,
      title: "Requested skill",
    });
    const otherSkill = await createSkillFixture(prisma, {
      userId: auth.userId,
      title: "Other skill",
    });
    const exercise = await createChoiceExercise({
      prisma,
      userId: auth.userId,
      skillId: otherSkill.id,
    });

    await expect(
      repairAgentReadiness(auth, {
        skill_id: requestedSkill.id,
        action: "flag_exercise",
        exercise_id: exercise.id,
        reasons: ["UNCLEAR_PROMPT"],
      }),
    ).resolves.toMatchObject({
      status: "not-found",
      action: "flag_exercise",
    });
    await expect(
      prisma.exercise.findUniqueOrThrow({
        where: { id: exercise.id },
        select: { retiredAt: true },
      }),
    ).resolves.toEqual({ retiredAt: null });
    await expect(
      prisma.exerciseFlag.count({
        where: { userId: auth.userId, exerciseId: exercise.id },
      }),
    ).resolves.toBe(0);
  });

  it("rechecks revocation before committing a readiness repair", async () => {
    const auth = await createAuth();
    const skill = await createSkillFixture(prisma, {
      userId: auth.userId,
      title: "Revocation repair",
    });
    await prisma.skill.update({
      where: { id: skill.id },
      data: { rules: { items: ["Original rule."] } },
    });

    let locked!: () => void;
    let release!: () => void;
    let revocationPid = 0;
    const didLock = new Promise<void>((resolve) => {
      locked = resolve;
    });
    const canCommit = new Promise<void>((resolve) => {
      release = resolve;
    });
    const revoke = prisma.$transaction(
      async (tx) => {
        const [backend] = await tx.$queryRaw<Array<{ pid: number }>>`
          SELECT pg_backend_pid() AS pid
        `;
        revocationPid = backend.pid;
        await tx.agentConnection.update({
          where: { id: auth.connectionId },
          data: { status: "REVOKED" },
        });
        locked();
        await canCommit;
      },
      { timeout: 10_000 },
    );
    await didLock;

    const pending = repairAgentReadiness(auth, {
      skill_id: skill.id,
      action: "update_guidance",
      examples: ["This write must not commit."],
    });
    const assertion = expect(pending).rejects.toMatchObject({
      code: "permission_denied",
    });
    try {
      await expect
        .poll(
          async () => {
            const [state] = await prisma.$queryRaw<Array<{ waiting: boolean }>>`
              SELECT EXISTS (
                SELECT 1 FROM pg_stat_activity
                WHERE datname = current_database()
                  AND ${revocationPid} = ANY(pg_blocking_pids(pid))
              ) AS waiting
            `;
            return state.waiting;
          },
          { timeout: 3_000, interval: 50 },
        )
        .toBe(true);
    } finally {
      release();
    }
    await revoke;
    await assertion;
    await expect(
      prisma.skill.findUniqueOrThrow({
        where: { id: skill.id },
        select: { examples: true },
      }),
    ).resolves.toEqual({ examples: null });
  });

  it("keeps the worker state when a committed refill event acknowledgement is lost", async () => {
    const auth = await createAuth();
    const skill = await createSkillFixture(prisma, {
      userId: auth.userId,
      title: "Lost refill acknowledgement",
    });
    const generationJob = await prisma.generationJob.create({
      data: {
        userId: auth.userId,
        skillId: skill.id,
        kind: GenerationJobKind.CHOICE_EXERCISE_GENERATION,
        status: GenerationJobStatus.PENDING,
        stage: GenerationJobStage.QUEUED,
        checkpoint: "event-pending",
        stageMetrics: { refillDelivery: { kind: "choice", targetReadyCount: 5, requestedAt: now.toISOString(), attempt: 0, nextAttemptAt: now.toISOString() } },
        provider: "test",
        model: "fixture",
        promptVersion: "fixture-v1",
        requestedCount: 1,
      },
    });
    const sender: ExerciseRefillEventSender = {
      async sendChoiceRefillRequested() {
        await prisma.generationJob.update({
          where: { id: generationJob.id },
          data: {
            status: GenerationJobStatus.SUCCEEDED,
            stage: GenerationJobStage.COMPLETE,
          },
        });
        throw new Error("ack lost after worker started");
      },
      async sendExactInputRefillRequested() {
        throw new Error("unexpected exact-input event");
      },
      async sendMathRefillRequested() {
        throw new Error("unexpected math event");
      },
    };

    const result = await publishDeferredExerciseRefillEvent({
      result: {
        status: "queued",
        skillId: skill.id,
        generationJobId: generationJob.id,
        requestedCount: 1,
        readyExerciseCount: 0,
        targetReadyCount: 5,
        message: "Exercise preparation started.",
        deferredEvent: {
          kind: "choice",
          payload: {
            userId: auth.userId,
            skillId: skill.id,
            generationJobId: generationJob.id,
            targetReadyCount: 5,
            requestedAt: now.toISOString(),
          },
        },
      },
      now,
      sender,
    });

    expect(result).toMatchObject({
      status: "queued",
      message: "Exercise preparation completed.",
    });
    expect(JSON.stringify(result)).not.toContain("deferredEvent");
    await expect(
      prisma.generationJob.findUniqueOrThrow({
        where: { id: generationJob.id },
        select: { status: true, stage: true },
      }),
    ).resolves.toEqual({
      status: GenerationJobStatus.SUCCEEDED,
      stage: GenerationJobStage.COMPLETE,
    });
  });
  it("publishes one committed refill event and strips deferred details from the result", async () => {
    const auth = await createAuth();
    const skill = await createSkillFixture(prisma, {
      userId: auth.userId,
      title: "Committed refill event",
    });
    const deferredQueueSender: ExerciseRefillEventSender = {
      async sendChoiceRefillRequested() {},
      async sendExactInputRefillRequested() {},
      async sendMathRefillRequested() {},
    };
    let queued!: Awaited<ReturnType<typeof queueChoiceExerciseRefillForSkill>>;
    await prisma.$transaction(async (tx) => {
      queued = await queueChoiceExerciseRefillForSkill({
        userId: auth.userId,
        skillId: skill.id,
        now,
        transaction: tx,
        sender: deferredQueueSender,
        deferEvent: true,
      });
    });

    expect(queued).toMatchObject({ status: "queued" });
    if (queued.status !== "queued" || !queued.deferredEvent) {
      throw new Error("Expected a committed refill event to be deferred.");
    }

    let sendCount = 0;
    let observedStatus: GenerationJobStatus | null = null;
    const sender: ExerciseRefillEventSender = {
      async sendChoiceRefillRequested(payload) {
        sendCount += 1;
        const job = await prisma.generationJob.findUniqueOrThrow({
          where: { id: payload.generationJobId },
          select: { status: true },
        });
        observedStatus = job.status;
      },
      async sendExactInputRefillRequested() {
        throw new Error("unexpected exact-input event");
      },
      async sendMathRefillRequested() {
        throw new Error("unexpected math event");
      },
    };

    const result = await publishDeferredExerciseRefillEvent({
      result: queued,
      now,
      sender,
    });

    expect(result).toMatchObject({
      status: "queued",
      generationJobId: queued.generationJobId,
    });
    expect(result).not.toHaveProperty("deferredEvent");
    expect(sendCount).toBe(1);
    expect(observedStatus).toBe(GenerationJobStatus.PENDING);
  });
});

type ReviewHistoryInput = { practiceOnly?: boolean };

async function createReviewHistory(
  prisma: ReturnType<typeof getPrisma>,
  userId: string,
  skillId: string,
  now: Date,
  reviews: readonly ReviewHistoryInput[],
) {
  const created: Array<{ exerciseId: string }> = [];
  for (const [index, input] of reviews.entries()) {
    const exercise = await createChoiceExercise({ prisma, userId, skillId });
    const reviewedAt = new Date(
      now.getTime() - (reviews.length - index) * 86_400_000,
    );
    const attemptId = randomUUID();
    await prisma.exerciseAttempt.create({
      data: {
        id: attemptId,
        userId,
        skillId,
        exerciseId: exercise.id,
        answer: { choiceId: "wrong" },
        isCorrect: false,
        result: ExerciseAttemptResult.INCORRECT,
        finalRating: FsrsRating.AGAIN,
        proposedRating: FsrsRating.AGAIN,
        practiceContext: {
          version: 1,
          answerMode: "CHOICE",
          mixedReview: false,
          reducedRuleCues: false,
          assistance: "none",
          exposure: input.practiceOnly ? "PRACTICE_ONLY" : "SCHEDULED",
          sessionMode: input.practiceOnly ? "PRACTICE_ONLY" : "SCHEDULED",
        },
        answerPolicySnapshot: { kind: "choice", correctChoiceId: "right" },
        feedbackShownAt: reviewedAt,
        createdAt: reviewedAt,
      },
    });
    await prisma.reviewLog.create({
      data: {
        id: randomUUID(),
        userId,
        skillId,
        exerciseAttemptId: attemptId,
        finalRating: FsrsRating.AGAIN,
        reviewedAt,
        previousDueAt: new Date(reviewedAt.getTime() - 3_600_000),
        nextDueAt: new Date(reviewedAt.getTime() + 86_400_000),
        schedulerName: "test",
        schedulerVersion: "test",
        desiredRetention: 0.9,
        schedulerParameters: {},
        createdAt: reviewedAt,
      },
    });
    created.push({ exerciseId: exercise.id });
  }
  return created;
}
