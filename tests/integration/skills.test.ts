import { randomUUID } from "node:crypto";

import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";

import {
  AnswerKind,
  ExerciseRetirementReason,
  ExerciseType,
  ExerciseVerificationStatus,
  GenerationJobKind,
  GenerationAuditDecision,
  GenerationJobStatus,
  Prisma,
  SkillGenerationSpecStatus,
  SkillFsrsState,
  SkillStatus,
  SourceFileKind,
  SourceFileStatus,
} from "@/generated/prisma/client";
import {
  getNextChoicePracticeItemForUser,
  getNextPracticeItemForUser,
} from "@/app/practice/queries";
import { getDashboardHome } from "@/lib/dashboard";
import { DEFAULT_GEMINI_MODEL } from "@/lib/gemini";
import { getPrisma } from "@/lib/prisma";
import {
  ACTIVATION_GENERATION_STALE_AFTER_MS,
  ACTIVATION_STALE_JOB_MESSAGE,
  activateSkillDraft,
  createGeneratedSkillDraftsForSourceFile,
  createSkillDraft,
  createSkillDraftFromSource,
  DEFAULT_READY_EXACT_INPUT_TARGET,
  DEFAULT_READY_EXERCISE_TARGET,
  DEFAULT_READY_MATH_TARGET,
  EXACT_INPUT_UNLOCK_REPETITIONS,
  refillMathExercisesForSkill,
  refillExactInputExercisesForSkill,
  refillChoiceExercisesForSkill,
  updateSkillDraft,
  updateSkillPracticeGuidance,
  type ChoiceExerciseGenerator,
  type ChoiceExerciseVerifier,
  type ExactInputExerciseGenerator,
  type ExactInputExerciseVerifier,
  type MathExerciseVerifier,
  type SourceMediaContext,
  type SourceMediaContextLoader,
  type SkillDraftGenerator,
} from "@/lib/skills";
import {
  MAX_SOURCE_UPLOAD_BYTES,
  MAX_SOURCE_UPLOAD_REQUEUE_ATTEMPTS,
  SOURCE_PROCESSING_STALE_AFTER_MS,
  cleanupPreparedSourceUploads,
  completeSourceUploadDrafts,
  dismissFailedSourceUpload,
  prepareSourceUpload,
  queueSourceUploadDrafts,
  requeueSourceUploadDraft,
  runQueuedSourceUploadDraftJob,
  type SourceTextExtractor,
  type SourceUploadStorage,
} from "@/lib/skills/uploads";
import { SourceObjectSizeLimitError } from "@/lib/storage/s3";
import {
  queueExactInputExerciseRefillForSkill,
  queueChoiceExerciseRefillForSkill,
  queueMathExerciseRefillForSkill,
  markRefillJobRetryableFailure,
  runChoiceExerciseRefillJob,
  runExactInputExerciseRefillJob,
  runMathExerciseRefillJob,
  type RefillQueueResult,
} from "@/lib/skills/refill-jobs";
import { REFILL_JOB_RETRY_LIMIT } from "@/lib/skills/refill-policy";
import type {
  ExerciseRefillEventPayload,
  ExerciseRefillEventSender,
  SourceUploadDraftEventPayload,
  SourceUploadDraftEventSender,
} from "@/lib/jobs/events";
import { getSkillsLibrary } from "@/lib/skills/library";
import { getSkillCreationSourceRecoveryItems } from "@/lib/skills/source-recovery";
import { removeSkillSource } from "@/lib/skills/sources";
import {
  buildSkillDuplicateCandidateFingerprint,
  buildSkillDuplicateLibraryFingerprint,
  buildSkillDuplicateReviewFingerprint,
} from "@/lib/skills/similarity";
import { ALPHA_ACTIVE_SKILLS } from "@/lib/usage-limits";
import { createInitialSkillSchedule } from "@/lib/scheduling";

const runDatabaseTests = process.env.RUN_DATABASE_TESTS === "1";
const describeDatabase = runDatabaseTests ? describe : describe.skip;
const runId = `skills_${randomUUID()}`;
const now = new Date("2026-06-04T16:00:00.000Z");

const generatedExercise = (id: number) => ({
  prompt: `What is the best translation for item ${id}?`,
  choices: [
    { id: "correct", label: `Correct answer ${id}` },
    { id: "close", label: `Close distractor ${id}` },
    { id: "wrong", label: `Wrong answer ${id}` },
  ],
  correctChoiceId: "correct",
  explanation: `Item ${id} checks the defined skill.`,
  difficulty: 2,
  expectedSeconds: 30,
});

const generatedExactInputExercise = (id: number) => ({
  prompt: `Type the answer for exact item ${id}.`,
  answerKind: AnswerKind.TEXT,
  answerSpec: {
    kind: "text",
    policyVersion: 2,
    accepted: [`exact answer ${id}`],
    normalizeCase: true,
    normalizeWhitespace: true,
    normalizeDiacritics: false,
  },
  correctAnswerDisplay: `exact answer ${id}`,
  explanation: `Exact item ${id} checks direct recall.`,
  difficulty: 2,
  expectedSeconds: 35,
});

const generatedMathExercise = (id: number) => ({
  prompt: `Simplify math item ${id}: x + ${id}x.`,
  answerKind: AnswerKind.MATH,
  answerSpec: {
    kind: "math",
    acceptedExpressions: [`${id + 1}x`],
    equivalence: "basic-symbolic",
  },
  correctAnswerDisplay: `${id + 1}x`,
  explanation: `Combine like terms to get ${id + 1}x.`,
  difficulty: 2,
  expectedSeconds: 35,
});

const successfulGenerator: ChoiceExerciseGenerator = async () => ({
  exercises: [generatedExercise(1), generatedExercise(2), generatedExercise(3)],
});

const acceptAllVerifier: ChoiceExerciseVerifier = async (input) => ({
  verifications: input.candidates.map((candidate) => ({
    candidateId: candidate.candidateId,
    verdict: "verified",
  })),
});

const acceptAllExactInputVerifier: ExactInputExerciseVerifier = async (input) => ({
  verifications: input.candidates.map((candidate) => ({
    candidateId: candidate.candidateId,
    verdict: "verified",
  })),
});

const acceptAllMathVerifier: MathExerciseVerifier = async (input) => ({
  verifications: input.candidates.map((candidate) => ({
    candidateId: candidate.candidateId,
    verdict: "verified",
  })),
});

function createFakeRefillSender() {
  const choiceEvents: ExerciseRefillEventPayload[] = [];
  const exactInputEvents: ExerciseRefillEventPayload[] = [];
  const mathEvents: ExerciseRefillEventPayload[] = [];
  const sender: ExerciseRefillEventSender = {
    async sendChoiceRefillRequested(payload) {
      choiceEvents.push(payload);
    },
    async sendExactInputRefillRequested(payload) {
      exactInputEvents.push(payload);
    },
    async sendMathRefillRequested(payload) {
      mathEvents.push(payload);
    },
  };

  return {
    sender,
    choiceEvents,
    exactInputEvents,
    mathEvents,
  };
}

function createFakeSourceUploadSender() {
  const events: SourceUploadDraftEventPayload[] = [];
  const sender: SourceUploadDraftEventSender = {
    async sendSourceUploadDraftRequested(payload) {
      events.push(payload);
    },
  };

  return {
    sender,
    events,
  };
}

function expectQueued(result: RefillQueueResult): Extract<RefillQueueResult, { status: "queued" }> {
  expect(result.status).toBe("queued");

  if (result.status !== "queued") {
    throw new Error(`Expected refill queueing to succeed: ${result.message}`);
  }

  return result;
}

const generatedSkillDraft = {
  title: "Ser vs. estar in classroom sentences",
  objective: "Choose ser or estar in beginner Spanish sentences about identity and location.",
  rules: ["Use ser for identity.", "Use estar for location and temporary states."],
  examples: ["Soy estudiante.", "Estoy en casa."],
  exerciseConstraints: "Use short multiple-choice prompts with one clear verb choice.",
  tags: ["Spanish", "grammar"],
};

const successfulSkillDraftGenerator: SkillDraftGenerator = async () => ({
  drafts: [generatedSkillDraft],
});

function createFakeUploadStorage({
  bytes = Buffer.from(
    "Use ser for identity. Use estar for location. Practice each idea with short classroom examples.",
  ),
  mimeType = "image/png",
  byteSize = bytes.byteLength,
  headError = null,
  headErrorForKey,
  getError = null,
  onDeleteObject,
}: {
  bytes?: Buffer;
  mimeType?: string;
  byteSize?: number;
  headError?: Error | null;
  headErrorForKey?: (input: Parameters<SourceUploadStorage["headObject"]>[0]) => Error | null;
  getError?: Error | null;
  onDeleteObject?: (
    input: Parameters<SourceUploadStorage["deleteObject"]>[0],
  ) => Promise<void> | void;
} = {}) {
  const deletedKeys: string[] = [];
  const presignedUploadInputs: Parameters<SourceUploadStorage["createPresignedUploadUrl"]>[0][] = [];
  const getObjectByteInputs: Parameters<SourceUploadStorage["getObjectBytes"]>[0][] = [];
  const storage: SourceUploadStorage = {
    bucketName: "learnrecur-dev",
    async createPresignedUploadUrl(input) {
      presignedUploadInputs.push(input);

      if (input.byteSize > input.maxBytes) {
        throw new SourceObjectSizeLimitError(
          `Upload exceeds maximum size of ${input.maxBytes} bytes.`,
        );
      }

      return "https://s3.example.test/presigned-upload";
    },
    async headObject(input) {
      const keyError = headErrorForKey?.(input);

      if (keyError) {
        throw keyError;
      }

      if (headError) {
        throw headError;
      }

      return {
        byteSize,
        mimeType,
      };
    },
    async getObjectBytes(input) {
      getObjectByteInputs.push(input);

      if (getError) {
        throw getError;
      }

      if (input.maxBytes !== undefined && bytes.byteLength > input.maxBytes) {
        throw new SourceObjectSizeLimitError(
          `S3 object exceeded maximum read size of ${input.maxBytes} bytes.`,
        );
      }

      return bytes;
    },
    async listObjects() {
      return [];
    },
    async deleteObject(input) {
      if (onDeleteObject) {
        await onDeleteObject(input);
      }

      deletedKeys.push(input.key);
    },
  };

  return {
    storage,
    deletedKeys,
    presignedUploadInputs,
    getObjectByteInputs,
  };
}

const successfulSourceExtractor: SourceTextExtractor = async () => ({
  extractedText:
    "Use ser for identity and long-term traits. Use estar for location and temporary states. Classroom practice should use short sentences with one obvious choice.",
});

describeDatabase("skill drafts and Gemini activation", () => {
  const prisma = getPrisma();
  const ownedUserIds: string[] = [];

  function makeUserId(label: string) {
    const userId = `${runId}_${label}`;
    ownedUserIds.push(userId);
    return userId;
  }

  async function cleanupUser(userId: string) {
    await prisma.user.deleteMany({ where: { id: userId } });
  }

  async function createUser(label: string) {
    const userId = makeUserId(label);
    await cleanupUser(userId);
    await prisma.user.create({
      data: {
        id: userId,
        email: `${label}@example.com`,
      },
    });
    return userId;
  }

  async function createActiveSkillFixture({
    userId,
    title = "Active refill skill",
    status = SkillStatus.ACTIVE,
    repetitions = 0,
  }: {
    userId: string;
    title?: string;
    status?: SkillStatus;
    repetitions?: number;
  }) {
    return prisma.skill.create({
      data: {
        userId,
        title,
        objective: "Choose the correct answer for short generated practice items.",
        status,
        tags: ["refill"],
        ...createInitialSkillSchedule(now),
        repetitions,
      },
    });
  }

  async function createChoiceExerciseFixture({
    userId,
    skillId,
    id = 1,
    verificationStatus = ExerciseVerificationStatus.VERIFIED,
    choices = [
      { id: "correct", label: `Correct answer ${id}` },
      { id: "wrong", label: `Wrong answer ${id}` },
      { id: "close", label: `Close distractor ${id}` },
    ],
    retiredAt = null,
  }: {
    userId: string;
    skillId: string;
    id?: number;
    verificationStatus?: ExerciseVerificationStatus;
    choices?: Prisma.InputJsonValue;
    retiredAt?: Date | null;
  }) {
    return prisma.exercise.create({
      data: {
        userId,
        skillId,
        type: ExerciseType.MULTIPLE_CHOICE,
        answerKind: AnswerKind.CHOICE,
        prompt: `Existing refill prompt ${id}?`,
        choices,
        answerSpec: {
          kind: "choice",
          correctChoiceId: "correct",
        },
        correctAnswerDisplay: `Correct answer ${id}`,
        explanation: `Existing explanation ${id}.`,
        verificationStatus,
        retiredAt,
        retirementReason: retiredAt ? ExerciseRetirementReason.MANUAL : null,
      },
    });
  }

  async function createTextExerciseFixture({
    userId,
    skillId,
    id = 1,
    verificationStatus = ExerciseVerificationStatus.VERIFIED,
    retiredAt = null,
  }: {
    userId: string;
    skillId: string;
    id?: number;
    verificationStatus?: ExerciseVerificationStatus;
    retiredAt?: Date | null;
  }) {
    return prisma.exercise.create({
      data: {
        userId,
        skillId,
        type: ExerciseType.EXACT_INPUT,
        answerKind: AnswerKind.TEXT,
        prompt: `Type the exact answer ${id}.`,
        answerSpec: {
          kind: "text",
          accepted: [`answer ${id}`],
        },
        correctAnswerDisplay: `answer ${id}`,
        verificationStatus,
        retiredAt,
        retirementReason: retiredAt ? ExerciseRetirementReason.MANUAL : null,
      },
    });
  }

  async function createNumericExerciseFixture({
    userId,
    skillId,
    id = 1,
  }: {
    userId: string;
    skillId: string;
    id?: number;
  }) {
    return prisma.exercise.create({
      data: {
        userId,
        skillId,
        type: ExerciseType.EXACT_INPUT,
        answerKind: AnswerKind.NUMERIC,
        prompt: `Enter the numeric answer ${id}.`,
        answerSpec: {
          kind: "numeric",
          accepted: [id],
          tolerance: 0,
        },
        correctAnswerDisplay: String(id),
        verificationStatus: ExerciseVerificationStatus.VERIFIED,
      },
    });
  }

  async function createMathExerciseFixture({
    userId,
    skillId,
    id = 1,
    verificationStatus = ExerciseVerificationStatus.VERIFIED,
    retiredAt = null,
  }: {
    userId: string;
    skillId: string;
    id?: number;
    verificationStatus?: ExerciseVerificationStatus;
    retiredAt?: Date | null;
  }) {
    return prisma.exercise.create({
      data: {
        userId,
        skillId,
        type: ExerciseType.EXACT_INPUT,
        answerKind: AnswerKind.MATH,
        prompt: `Simplify the math answer ${id}.`,
        answerSpec: {
          kind: "math",
          acceptedExpressions: [`${id + 1}x`],
          equivalence: "basic-symbolic",
        },
        correctAnswerDisplay: `${id + 1}x`,
        verificationStatus,
        retiredAt,
        retirementReason: retiredAt ? ExerciseRetirementReason.MANUAL : null,
      },
    });
  }

  async function createUploadedSourceRefFixture({
    userId,
    skillId,
    originalName = "Uploaded source worksheet",
    extractedText = "Use the attached worksheet image as the source of truth for exercise generation.",
  }: {
    userId: string;
    skillId: string;
    originalName?: string;
    extractedText?: string;
  }) {
    const sourceFile = await prisma.sourceFile.create({
      data: {
        userId,
        kind: SourceFileKind.IMAGE,
        status: SourceFileStatus.READY,
        originalName,
        mimeType: "image/png",
        byteSize: 128,
        storageBucket: "learnrecur-dev",
        storageKey: `source-uploads/${userId}/${randomUUID()}/worksheet.png`,
        extractedText,
      },
    });

    await prisma.skillSourceRef.create({
      data: {
        userId,
        skillId,
        sourceFileId: sourceFile.id,
      },
    });

    return sourceFile;
  }

  function expectSingleSourceMedia(
    media: SourceMediaContext[] | undefined,
    expected: {
      sourceFileId: string;
      label: string;
      mimeType: "image/png";
      bytes: Buffer;
    },
  ) {
    expect(media).toHaveLength(1);
    expect(media?.[0]).toMatchObject({
      sourceFileId: expected.sourceFileId,
      label: expected.label,
      mimeType: expected.mimeType,
    });
    expect(media?.[0].bytes.equals(expected.bytes)).toBe(true);
  }

  function createSingleSourceMediaLoader({
    bytes,
    sourceFile,
  }: {
    bytes: Buffer;
    sourceFile: {
      id: string;
      originalName: string;
    };
  }): SourceMediaContextLoader {
    return async ({ sourceFiles }) => {
      expect(sourceFiles).toHaveLength(1);
      expect(sourceFiles[0]).toMatchObject({
        id: sourceFile.id,
        status: SourceFileStatus.READY,
        originalName: sourceFile.originalName,
        mimeType: "image/png",
        storageBucket: "learnrecur-dev",
      });

      return [
        {
          sourceFileId: sourceFile.id,
          label: sourceFile.originalName,
          mimeType: "image/png",
          bytes,
        },
      ];
    };
  }

  async function expectRefillPassesSingleSourceMedia({
    bytes,
    createExistingExercises,
    extractedText,
    refill,
    repetitions = 0,
    sourceName,
    skillTitle,
    userIdSuffix,
  }: {
    bytes: Buffer;
    createExistingExercises?: (input: { userId: string; skillId: string }) => Promise<void>;
    extractedText: string;
    refill: (input: {
      captureGeneratorSourceMedia: (media: SourceMediaContext[] | undefined) => void;
      captureVerifierSourceMedia: (media: SourceMediaContext[] | undefined) => void;
      sourceMediaLoader: SourceMediaContextLoader;
      skillId: string;
      userId: string;
    }) => Promise<{ status: string }>;
    repetitions?: number;
    sourceName: string;
    skillTitle: string;
    userIdSuffix: string;
  }) {
    const userId = await createUser(userIdSuffix);
    const skill = await createActiveSkillFixture({
      userId,
      title: skillTitle,
      repetitions,
    });
    await createExistingExercises?.({ userId, skillId: skill.id });
    const sourceFile = await createUploadedSourceRefFixture({
      userId,
      skillId: skill.id,
      originalName: sourceName,
      extractedText,
    });
    let generatorSourceMedia: SourceMediaContext[] | undefined;
    let verifierSourceMedia: SourceMediaContext[] | undefined;

    const result = await refill({
      userId,
      skillId: skill.id,
      sourceMediaLoader: createSingleSourceMediaLoader({
        sourceFile,
        bytes,
      }),
      captureGeneratorSourceMedia: (media) => {
        generatorSourceMedia = media;
      },
      captureVerifierSourceMedia: (media) => {
        verifierSourceMedia = media;
      },
    });

    expect(result.status).toBe("refilled");
    expectSingleSourceMedia(generatorSourceMedia, {
      sourceFileId: sourceFile.id,
      label: sourceName,
      mimeType: "image/png",
      bytes,
    });
    expectSingleSourceMedia(verifierSourceMedia, {
      sourceFileId: sourceFile.id,
      label: sourceName,
      mimeType: "image/png",
      bytes,
    });
  }

  beforeAll(async () => {
    await prisma.$queryRaw`SELECT 1`;
  });

  afterAll(async () => {
    for (const userId of ownedUserIds.reverse()) {
      await cleanupUser(userId);
    }

    await prisma.$disconnect();
  });

  it("creates and updates user-owned skill drafts with collection resolution", async () => {
    const userId = await createUser("drafts");
    const otherUserId = await createUser("drafts_other");

    const created = await createSkillDraft({
      userId,
      input: {
        title: " Ser vs estar ",
        objective: "Choose between ser and estar in simple identity and location cases.",
        collectionName: " Spanish grammar ",
        rules: "Use ser for identity.",
        examples: "Soy estudiante.",
        exerciseConstraints: "Avoid trick options.",
        tags: "spanish, grammar, spanish",
      },
    });

    expect(created.status).toBe("created");

    if (created.status !== "created") {
      throw new Error("Expected draft creation to succeed.");
    }

    const skill = await prisma.skill.findUniqueOrThrow({
      where: { id: created.skill.id },
      include: { collection: true },
    });

    expect(skill).toMatchObject({
      userId,
      title: "Ser vs estar",
      objective: "Choose between ser and estar in simple identity and location cases.",
      status: SkillStatus.DRAFT,
      tags: ["spanish", "grammar"],
    });
    expect(skill.collection?.name).toBe("Spanish grammar");

    const denied = await updateSkillDraft({
      userId: otherUserId,
      skillId: skill.id,
      input: {
        title: "Cross-user edit",
        objective: "This should not be allowed to alter another user draft.",
      },
    });

    expect(denied).toMatchObject({
      status: "not-found",
      reason: "skill-not-found",
    });

    const updated = await updateSkillDraft({
      userId,
      skillId: skill.id,
      input: {
        title: "Ser and estar in context",
        objective: "Choose ser or estar for identity, location, and temporary state.",
        collectionName: "Spanish grammar",
        rules: "Use estar for temporary state.",
        examples: "Estoy cansado.",
        exerciseConstraints: "Prefer concrete classroom examples.",
        tags: ["spanish", "verbs", "grammar"],
      },
    });

    expect(updated.status).toBe("updated");
    await expect(prisma.collection.count({ where: { userId, name: "Spanish grammar" } })).resolves.toBe(1);

    const afterUpdate = await prisma.skill.findUniqueOrThrow({ where: { id: skill.id } });
    expect(afterUpdate).toMatchObject({
      title: "Ser and estar in context",
      status: SkillStatus.DRAFT,
      tags: ["spanish", "verbs", "grammar"],
    });
  });

  it("updates practice guidance for active skills owned by the user", async () => {
    const userId = await createUser("guidance");
    const otherUserId = await createUser("guidance_other");
    const skill = await prisma.skill.create({
      data: {
        userId,
        title: "Spanish articles",
        objective: "Choose the correct Spanish definite article for short noun phrases.",
        status: SkillStatus.ACTIVE,
        tags: ["spanish"],
        rules: { items: ["Use el before masculine singular nouns."] },
        examples: { items: ["el libro"] },
        exerciseConstraints: {
          notes: "Use short noun phrases.",
          answerKind: "choice",
          requestedCount: 5,
        },
        ...createInitialSkillSchedule(now),
      },
    });

    const denied = await updateSkillPracticeGuidance({
      userId: otherUserId,
      skillId: skill.id,
      input: {
        rules: "Use la for every noun.",
        examples: "la libro",
        exerciseConstraints: "This should not save.",
      },
    });

    expect(denied).toMatchObject({
      status: "not-found",
      reason: "skill-not-found",
    });

    const unchanged = await prisma.skill.findUniqueOrThrow({ where: { id: skill.id } });
    expect(unchanged.rules).toEqual({ items: ["Use el before masculine singular nouns."] });

    const updated = await updateSkillPracticeGuidance({
      userId,
      skillId: skill.id,
      input: {
        rules: "Prefer singular examples.\nKeep articles explicit.",
        examples: "el mapa\nla clase",
        exerciseConstraints: "Avoid ambiguous nouns with multiple accepted articles.",
      },
    });

    expect(updated).toEqual({
      status: "updated",
      skillId: skill.id,
    });

    const afterUpdate = await prisma.skill.findUniqueOrThrow({ where: { id: skill.id } });
    expect(afterUpdate.rules).toEqual({
      items: ["Prefer singular examples.", "Keep articles explicit."],
    });
    expect(afterUpdate.examples).toEqual({ items: ["el mapa", "la clase"] });
    expect(afterUpdate.exerciseConstraints).toEqual({
      notes: "Avoid ambiguous nouns with multiple accepted articles.",
      answerKind: "choice",
      requestedCount: 5,
    });
    expect(afterUpdate.generationSpecStatus).toBe(SkillGenerationSpecStatus.SUPERSEDED);
  });

  it("activates a draft with verified choice exercises, FSRS fields, and audit metadata", async () => {
    const userId = await createUser("activate");
    const draft = await createSkillDraft({
      userId,
      input: {
        title: "Spanish classroom phrases",
        objective: "Choose the correct meaning for common Spanish classroom phrases.",
        collectionName: "Spanish",
        tags: "spanish, classroom",
      },
    });

    if (draft.status !== "created") {
      throw new Error("Expected draft creation to succeed.");
    }

    const activated = await activateSkillDraft({
      userId,
      skillId: draft.skill.id,
      now,
      generateChoiceExercises: successfulGenerator,
      verifyChoiceExercises: acceptAllVerifier,
      model: "test-gemini",
    });

    expect(activated).toMatchObject({
      status: "activated",
      exerciseCount: 3,
    });

    const skill = await prisma.skill.findUniqueOrThrow({
      where: { id: draft.skill.id },
      include: {
        exercises: true,
        generationJobs: true,
      },
    });

    expect(skill).toMatchObject({
      status: SkillStatus.ACTIVE,
      dueAt: now,
      stability: 0,
      difficulty: 0,
      fsrsState: SkillFsrsState.NEW,
      repetitions: 0,
      lapses: 0,
    });
    expect(skill.exercises).toHaveLength(3);
    expect(skill.exercises.every((exercise) => exercise.userId === userId)).toBe(true);
    expect(skill.exercises.every((exercise) => exercise.type === ExerciseType.MULTIPLE_CHOICE)).toBe(true);
    expect(skill.exercises.every((exercise) => exercise.answerKind === AnswerKind.CHOICE)).toBe(true);
    expect(
      skill.exercises.every(
        (exercise) => exercise.verificationStatus === ExerciseVerificationStatus.VERIFIED,
      ),
    ).toBe(true);

    expect(skill.generationJobs).toHaveLength(1);
    expect(skill.generationJobs[0]).toMatchObject({
      userId,
      kind: GenerationJobKind.SKILL_ACTIVATION,
      status: GenerationJobStatus.SUCCEEDED,
      provider: "google",
      model: "test-gemini",
      promptVersion: "skill-mcq-v2",
      requestedCount: 5,
      acceptedCount: 3,
      rejectedCount: 0,
      errorMessage: null,
    });

    const dashboard = await getDashboardHome({ userId, now });
    expect(dashboard.readyNowCount).toBe(1);
    expect(dashboard.activeSkillCount).toBe(1);

    const nextPracticeItem = await getNextChoicePracticeItemForUser(userId, now);
    expect(nextPracticeItem.status).toBe("ready");

    if (nextPracticeItem.status === "ready") {
      expect(nextPracticeItem.skill.id).toBe(skill.id);
      expect(skill.exercises.map((exercise) => exercise.id)).toContain(nextPracticeItem.exercise.id);
    }
  });

  it("does not start activation when the draft changed after duplicate review", async () => {
    const userId = await createUser("activate_reviewed_snapshot");
    const draft = await createSkillDraft({
      userId,
      input: {
        title: "Spanish classroom directions",
        objective: "Choose the correct meaning for common classroom directions.",
        collectionName: "Spanish",
        tags: "spanish, classroom",
      },
    });

    if (draft.status !== "created") {
      throw new Error("Expected draft creation to succeed.");
    }

    const expectedDraftFingerprint =
      buildSkillDuplicateCandidateFingerprint(draft.skill);
    const updated = await updateSkillDraft({
      userId,
      skillId: draft.skill.id,
      input: {
        title: "Updated Spanish classroom directions",
        objective: "Choose the correct meaning after reviewing the updated scope.",
        collectionName: "Spanish",
        tags: "spanish, classroom",
      },
    });
    expect(updated.status).toBe("updated");
    const generator = vi.fn(successfulGenerator);

    const result = await activateSkillDraft({
      userId,
      skillId: draft.skill.id,
      expectedDraftFingerprint,
      now,
      generateChoiceExercises: generator,
      verifyChoiceExercises: acceptAllVerifier,
      model: "test-gemini",
    });

    expect(result).toMatchObject({
      status: "not-activated",
      reason: "draft-changed",
    });
    expect(generator).not.toHaveBeenCalled();
    await expect(
      prisma.generationJob.count({
        where: {
          userId,
          skillId: draft.skill.id,
          kind: GenerationJobKind.SKILL_ACTIVATION,
        },
      }),
    ).resolves.toBe(0);
    await expect(
      prisma.skill.findUniqueOrThrow({ where: { id: draft.skill.id } }),
    ).resolves.toMatchObject({
      title: "Updated Spanish classroom directions",
      status: SkillStatus.DRAFT,
    });
  });

  it("does not start activation when a matching skill appears after an initial no-match snapshot", async () => {
    const userId = await createUser("activate_new_duplicate_before_reservation");
    const draft = await createSkillDraft({
      userId,
      input: {
        title: "Spanish direct object pronouns",
        objective:
          "Choose the correct direct object pronoun in short Spanish sentences.",
      },
    });
    if (draft.status !== "created") {
      throw new Error("Expected draft creation to succeed.");
    }
    const expectedDuplicateLibraryFingerprint =
      buildSkillDuplicateLibraryFingerprint([draft.skill]);

    const matchingSkill = await createSkillDraft({
      userId,
      input: {
        title: draft.skill.title,
        objective: draft.skill.objective,
      },
    });
    if (matchingSkill.status !== "created") {
      throw new Error("Expected matching draft creation to succeed.");
    }
    const generator = vi.fn(successfulGenerator);

    const result = await activateSkillDraft({
      userId,
      skillId: draft.skill.id,
      expectedDraftFingerprint:
        buildSkillDuplicateCandidateFingerprint(draft.skill),
      expectedDuplicateLibraryFingerprint,
      now,
      generateChoiceExercises: generator,
      verifyChoiceExercises: acceptAllVerifier,
      model: "test-gemini",
    });

    expect(result).toMatchObject({
      status: "not-activated",
      reason: "duplicate-review-changed",
      message:
        "Your skill library changed after the duplicate check. Check again before adding this draft.",
    });
    expect(generator).not.toHaveBeenCalled();
    await expect(
      prisma.generationJob.count({
        where: {
          userId,
          skillId: draft.skill.id,
          kind: GenerationJobKind.SKILL_ACTIVATION,
        },
      }),
    ).resolves.toBe(0);
    await expect(
      prisma.skill.findUniqueOrThrow({ where: { id: draft.skill.id } }),
    ).resolves.toMatchObject({ status: SkillStatus.DRAFT });
  });

  it("does not finish activation when an existing skill becomes a match after the no-match snapshot", async () => {
    const userId = await createUser("activate_edited_duplicate_during_generation");
    const [draft, existing] = await Promise.all([
      createSkillDraft({
        userId,
        input: {
          title: "Spanish indirect object pronouns",
          objective:
            "Choose the correct indirect object pronoun in short Spanish sentences.",
        },
      }),
      createSkillDraft({
        userId,
        input: {
          title: "Spanish classroom greetings",
          objective: "Choose an appropriate greeting for a classroom exchange.",
        },
      }),
    ]);
    if (draft.status !== "created" || existing.status !== "created") {
      throw new Error("Expected both draft creations to succeed.");
    }
    const expectedDuplicateLibraryFingerprint =
      buildSkillDuplicateLibraryFingerprint([
        draft.skill,
        existing.skill,
      ]);
    const generator: ChoiceExerciseGenerator = async (generatorInput) => {
      const updated = await updateSkillDraft({
        userId,
        skillId: existing.skill.id,
        input: {
          title: draft.skill.title,
          objective: draft.skill.objective,
        },
      });
      expect(updated.status).toBe("updated");
      return successfulGenerator(generatorInput);
    };

    const result = await activateSkillDraft({
      userId,
      skillId: draft.skill.id,
      expectedDraftFingerprint:
        buildSkillDuplicateCandidateFingerprint(draft.skill),
      expectedDuplicateLibraryFingerprint,
      now,
      generateChoiceExercises: generator,
      verifyChoiceExercises: acceptAllVerifier,
      model: "test-gemini",
    });

    expect(result).toMatchObject({
      status: "not-activated",
      reason: "duplicate-review-changed",
      message:
        "Your skill library changed while exercises were being prepared. Check again before adding this draft.",
    });
    const [candidate, exerciseCount, generationJob] = await Promise.all([
      prisma.skill.findUniqueOrThrow({ where: { id: draft.skill.id } }),
      prisma.exercise.count({ where: { userId, skillId: draft.skill.id } }),
      prisma.generationJob.findFirstOrThrow({
        where: {
          userId,
          skillId: draft.skill.id,
          kind: GenerationJobKind.SKILL_ACTIVATION,
        },
      }),
    ]);
    expect(candidate.status).toBe(SkillStatus.DRAFT);
    expect(exerciseCount).toBe(0);
    expect(generationJob).toMatchObject({
      status: GenerationJobStatus.FAILED,
      errorMessage:
        "Your skill library changed while exercises were being prepared. Check again before adding this draft.",
    });
  });

  it("does not invalidate a no-match library snapshot for lifecycle-only changes", async () => {
    const userId = await createUser("activate_library_lifecycle_change");
    const [draft, existing] = await Promise.all([
      createSkillDraft({
        userId,
        input: {
          title: "Spanish present-tense endings",
          objective: "Choose the correct present-tense ending for a regular verb.",
        },
      }),
      createSkillDraft({
        userId,
        input: {
          title: "Spanish classroom greetings",
          objective: "Choose an appropriate greeting for a classroom exchange.",
        },
      }),
    ]);
    if (draft.status !== "created" || existing.status !== "created") {
      throw new Error("Expected both draft creations to succeed.");
    }
    const expectedDuplicateLibraryFingerprint =
      buildSkillDuplicateLibraryFingerprint([
        draft.skill,
        existing.skill,
      ]);
    const generator: ChoiceExerciseGenerator = async (generatorInput) => {
      await prisma.skill.update({
        where: { id: existing.skill.id },
        data: { status: SkillStatus.ARCHIVED },
      });
      return successfulGenerator(generatorInput);
    };

    const result = await activateSkillDraft({
      userId,
      skillId: draft.skill.id,
      expectedDraftFingerprint:
        buildSkillDuplicateCandidateFingerprint(draft.skill),
      expectedDuplicateLibraryFingerprint,
      now,
      generateChoiceExercises: generator,
      verifyChoiceExercises: acceptAllVerifier,
      model: "test-gemini",
    });

    expect(result).toMatchObject({
      status: "activated",
      skillId: draft.skill.id,
      exerciseCount: 3,
    });
  });

  it("does not start activation when the reviewed duplicate changes before reservation", async () => {
    const userId = await createUser("activate_changed_duplicate");
    const [draft, existing] = await Promise.all([
      createSkillDraft({
        userId,
        input: {
          title: "Ser or estar practice",
          objective: "Choose the correct Spanish copula in short sentences.",
        },
      }),
      createSkillDraft({
        userId,
        input: {
          title: "Ser and estar in context",
          objective:
            "Choose ser or estar for identity, location, and temporary state.",
        },
      }),
    ]);

    if (draft.status !== "created" || existing.status !== "created") {
      throw new Error("Expected both draft creations to succeed.");
    }

    const reviewedDuplicateFingerprint = buildSkillDuplicateReviewFingerprint(
      existing.skill,
    );
    const reviewedLibraryFingerprint =
      buildSkillDuplicateLibraryFingerprint([
        draft.skill,
        existing.skill,
      ]);
    const updated = await updateSkillDraft({
      userId,
      skillId: existing.skill.id,
      input: {
        title: "Updated ser and estar contexts",
        objective:
          "Choose the copula for identity, condition, place, and event prompts.",
      },
    });
    expect(updated.status).toBe("updated");
    const generator = vi.fn(successfulGenerator);

    const result = await activateSkillDraft({
      userId,
      skillId: draft.skill.id,
      expectedDraftFingerprint:
        buildSkillDuplicateCandidateFingerprint(draft.skill),
      expectedDuplicateLibraryFingerprint:
        reviewedLibraryFingerprint,
      expectedDuplicateMatch: {
        skillId: existing.skill.id,
        fingerprint: reviewedDuplicateFingerprint,
      },
      now,
      generateChoiceExercises: generator,
      verifyChoiceExercises: acceptAllVerifier,
      model: "test-gemini",
    });

    expect(result).toMatchObject({
      status: "not-activated",
      reason: "duplicate-review-changed",
    });
    expect(generator).not.toHaveBeenCalled();
    await expect(
      prisma.generationJob.count({
        where: {
          userId,
          skillId: draft.skill.id,
          kind: GenerationJobKind.SKILL_ACTIVATION,
        },
      }),
    ).resolves.toBe(0);
    await expect(
      prisma.skill.findUniqueOrThrow({ where: { id: draft.skill.id } }),
    ).resolves.toMatchObject({
      status: SkillStatus.DRAFT,
    });
  });

  it("does not finish activation when the reviewed duplicate changes during generation", async () => {
    const userId = await createUser(
      "activate_duplicate_changed_during_generation",
    );
    const [draft, existing] = await Promise.all([
      createSkillDraft({
        userId,
        input: {
          title: "Ser or estar drills",
          objective: "Choose the correct Spanish copula in short prompts.",
        },
      }),
      createSkillDraft({
        userId,
        input: {
          title: "Ser and estar sentence practice",
          objective:
            "Choose ser or estar for identity, location, and temporary state.",
        },
      }),
    ]);

    if (draft.status !== "created" || existing.status !== "created") {
      throw new Error("Expected both draft creations to succeed.");
    }

    const reviewedDuplicateFingerprint =
      buildSkillDuplicateReviewFingerprint(existing.skill);
    const reviewedLibraryFingerprint =
      buildSkillDuplicateLibraryFingerprint([
        draft.skill,
        existing.skill,
      ]);
    const generator: ChoiceExerciseGenerator = async (generatorInput) => {
      const updated = await updateSkillDraft({
        userId,
        skillId: existing.skill.id,
        input: {
          title: "Updated ser and estar sentence practice",
          objective:
            "Choose the copula for identity, condition, place, and event prompts.",
        },
      });
      expect(updated.status).toBe("updated");
      return successfulGenerator(generatorInput);
    };

    const result = await activateSkillDraft({
      userId,
      skillId: draft.skill.id,
      expectedDraftFingerprint:
        buildSkillDuplicateCandidateFingerprint(draft.skill),
      expectedDuplicateLibraryFingerprint:
        reviewedLibraryFingerprint,
      expectedDuplicateMatch: {
        skillId: existing.skill.id,
        fingerprint: reviewedDuplicateFingerprint,
      },
      now,
      generateChoiceExercises: generator,
      verifyChoiceExercises: acceptAllVerifier,
      model: "test-gemini",
    });

    expect(result).toMatchObject({
      status: "not-activated",
      reason: "duplicate-review-changed",
    });
    const [candidate, exerciseCount, generationJob] = await Promise.all([
      prisma.skill.findUniqueOrThrow({ where: { id: draft.skill.id } }),
      prisma.exercise.count({ where: { userId, skillId: draft.skill.id } }),
      prisma.generationJob.findFirstOrThrow({
        where: {
          userId,
          skillId: draft.skill.id,
          kind: GenerationJobKind.SKILL_ACTIVATION,
        },
      }),
    ]);
    expect(candidate.status).toBe(SkillStatus.DRAFT);
    expect(exerciseCount).toBe(0);
    expect(generationJob).toMatchObject({
      status: GenerationJobStatus.FAILED,
      errorMessage:
        "The existing skill changed while exercises were being prepared. Compare it again before adding this draft separately.",
    });
  });

  it("does not activate generated exercises when the draft changes during generation", async () => {
    const userId = await createUser("activate_changed_during_generation");
    const draft = await createSkillDraft({
      userId,
      input: {
        title: "Spanish weather phrases",
        objective: "Choose the correct meaning for common weather phrases.",
        collectionName: "Spanish",
        tags: "spanish, weather",
      },
    });

    if (draft.status !== "created") {
      throw new Error("Expected draft creation to succeed.");
    }

    const expectedDraftFingerprint =
      buildSkillDuplicateCandidateFingerprint(draft.skill);
    const generator: ChoiceExerciseGenerator = async () => {
      const updated = await updateSkillDraft({
        userId,
        skillId: draft.skill.id,
        input: {
          title: "Spanish weather questions",
          objective: "Answer short questions about current weather conditions.",
          collectionName: "Spanish",
          tags: "spanish, weather",
        },
      });
      expect(updated.status).toBe("updated");
      return successfulGenerator({
        skill: draft.skill,
        sourceContext: null,
        requestedCount: 3,
      });
    };

    const result = await activateSkillDraft({
      userId,
      skillId: draft.skill.id,
      expectedDraftFingerprint,
      now,
      generateChoiceExercises: generator,
      verifyChoiceExercises: acceptAllVerifier,
      model: "test-gemini",
    });

    expect(result).toMatchObject({
      status: "not-activated",
      reason: "draft-changed",
    });
    const [skill, exerciseCount, generationJob] = await Promise.all([
      prisma.skill.findUniqueOrThrow({ where: { id: draft.skill.id } }),
      prisma.exercise.count({ where: { userId, skillId: draft.skill.id } }),
      prisma.generationJob.findFirstOrThrow({
        where: {
          userId,
          skillId: draft.skill.id,
          kind: GenerationJobKind.SKILL_ACTIVATION,
        },
      }),
    ]);
    expect(skill).toMatchObject({
      title: "Spanish weather questions",
      status: SkillStatus.DRAFT,
    });
    expect(exerciseCount).toBe(0);
    expect(generationJob).toMatchObject({
      status: GenerationJobStatus.FAILED,
      errorMessage:
        "This draft changed while exercises were being prepared. Review it again before adding it.",
    });
  });

  it("does not let a waiting draft edit overwrite a completed activation", async () => {
    const userId = await createUser("activate_waiting_edit");
    const draft = await createSkillDraft({
      userId,
      input: {
        title: "Spanish calendar words",
        objective: "Choose the correct calendar word for a short date prompt.",
      },
    });

    if (draft.status !== "created") {
      throw new Error("Expected draft creation to succeed.");
    }

    let markSkillLocked: (() => void) | undefined;
    const skillLocked = new Promise<void>((resolve) => {
      markSkillLocked = resolve;
    });
    let allowActivationCommit: (() => void) | undefined;
    const activationCanCommit = new Promise<void>((resolve) => {
      allowActivationCommit = resolve;
    });
    const activation = prisma.$transaction(async (tx) => {
      await tx.$queryRaw`
        SELECT "id"
        FROM "skills"
        WHERE "id" = ${draft.skill.id} AND "userId" = ${userId}
        FOR UPDATE
      `;
      markSkillLocked?.();
      await activationCanCommit;
      await tx.skill.update({
        where: { id: draft.skill.id },
        data: {
          ...createInitialSkillSchedule(now),
          status: SkillStatus.ACTIVE,
        },
      });
    });

    await skillLocked;
    const waitingUpdate = updateSkillDraft({
      userId,
      skillId: draft.skill.id,
      input: {
        title: "Stale calendar edit",
        objective: "This stale edit must not overwrite the active skill.",
      },
    });
    await new Promise((resolve) => setTimeout(resolve, 100));
    allowActivationCommit?.();
    await activation;

    await expect(waitingUpdate).resolves.toMatchObject({
      status: "not-found",
      reason: "skill-not-found",
    });
    await expect(
      prisma.skill.findUniqueOrThrow({ where: { id: draft.skill.id } }),
    ).resolves.toMatchObject({
      title: "Spanish calendar words",
      status: SkillStatus.ACTIVE,
    });
  });

  it("lets only the newest worker finish after reclaiming a stale activation", async () => {
    const userId = await createUser("activate_stale_worker_fence");
    const firstNow = new Date();
    const draft = await createSkillDraft({
      userId,
      input: {
        title: "Spanish time expressions",
        objective: "Choose the correct time expression for a short schedule prompt.",
      },
    });

    if (draft.status !== "created") {
      throw new Error("Expected draft creation to succeed.");
    }

    let signalFirstStarted: (() => void) | undefined;
    const firstStarted = new Promise<void>((resolve) => {
      signalFirstStarted = resolve;
    });
    let releaseFirst: (() => void) | undefined;
    const firstCanFinish = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });
    const firstActivation = activateSkillDraft({
      userId,
      skillId: draft.skill.id,
      now: firstNow,
      generateChoiceExercises: async () => {
        signalFirstStarted?.();
        await firstCanFinish;
        return successfulGenerator({
          skill: draft.skill,
          sourceContext: null,
          requestedCount: 3,
        });
      },
      verifyChoiceExercises: acceptAllVerifier,
      model: "first-test-gemini",
    });
    await firstStarted;

    let signalSecondStarted: (() => void) | undefined;
    const secondStarted = new Promise<void>((resolve) => {
      signalSecondStarted = resolve;
    });
    let releaseSecond: (() => void) | undefined;
    const secondCanFinish = new Promise<void>((resolve) => {
      releaseSecond = resolve;
    });
    const secondNow = new Date(firstNow.getTime() + ACTIVATION_GENERATION_STALE_AFTER_MS + 1_000);
    const secondActivation = activateSkillDraft({
      userId,
      skillId: draft.skill.id,
      now: secondNow,
      eventNow: () => secondNow,
      generateChoiceExercises: async () => {
        signalSecondStarted?.();
        await secondCanFinish;
        return successfulGenerator({
          skill: draft.skill,
          sourceContext: null,
          requestedCount: 3,
        });
      },
      verifyChoiceExercises: acceptAllVerifier,
      model: "second-test-gemini",
    });
    await secondStarted;

    releaseFirst?.();
    const firstResult = await firstActivation;
    releaseSecond?.();
    const secondResult = await secondActivation;

    expect(firstResult).toMatchObject({
      status: "not-activated",
      reason: "activation-superseded",
    });
    expect(secondResult).toMatchObject({
      status: "activated",
      skillId: draft.skill.id,
      exerciseCount: 3,
    });
    if (
      firstResult.status !== "not-activated" ||
      secondResult.status !== "activated"
    ) {
      throw new Error("Expected the replacement worker alone to activate the skill.");
    }
    const [skill, generationJobs, exerciseCount] = await Promise.all([
      prisma.skill.findUniqueOrThrow({ where: { id: draft.skill.id } }),
      prisma.generationJob.findMany({
        where: {
          userId,
          skillId: draft.skill.id,
          kind: GenerationJobKind.SKILL_ACTIVATION,
        },
        orderBy: { createdAt: "asc" },
      }),
      prisma.exercise.count({ where: { userId, skillId: draft.skill.id } }),
    ]);
    expect(skill.status).toBe(SkillStatus.ACTIVE);
    expect(generationJobs).toHaveLength(2);
    expect(
      generationJobs.find(
        (job) => job.id === firstResult.generationJobId,
      ),
    ).toMatchObject({
      status: GenerationJobStatus.FAILED,
      errorMessage: ACTIVATION_STALE_JOB_MESSAGE,
    });
    expect(
      generationJobs.find(
        (job) => job.id === secondResult.generationJobId,
      ),
    ).toMatchObject({
      status: GenerationJobStatus.SUCCEEDED,
    });
    expect(exerciseCount).toBe(3);
  });

  it("enforces the active-skill cap when two activations finish together", async () => {
    const userId = await createUser("activate_concurrent_cap");
    await prisma.skill.createMany({
      data: Array.from({ length: ALPHA_ACTIVE_SKILLS - 1 }, (_, index) => ({
        userId,
        title: `Active cap fixture ${index + 1}`,
        tags: [],
        status: SkillStatus.ACTIVE,
      })),
    });
    const drafts = await Promise.all([
      createSkillDraft({
        userId,
        input: {
          title: "First cap boundary draft",
          objective: "Choose the first correct answer at the active skill boundary.",
        },
      }),
      createSkillDraft({
        userId,
        input: {
          title: "Second cap boundary draft",
          objective: "Choose the second correct answer at the active skill boundary.",
        },
      }),
    ]);
    if (drafts.some((draft) => draft.status !== "created")) {
      throw new Error("Expected both cap-boundary drafts to be created.");
    }
    const skillIds = drafts.map((draft) =>
      draft.status === "created" ? draft.skill.id : "missing-skill",
    );

    let startedCount = 0;
    let releaseGenerators: (() => void) | undefined;
    const generatorsCanFinish = new Promise<void>((resolve) => {
      releaseGenerators = resolve;
    });
    const generator: ChoiceExerciseGenerator = async () => {
      startedCount += 1;
      await generatorsCanFinish;
      return successfulGenerator({
        skill: {
          id: "cap-boundary-skill",
          title: "Cap boundary",
          objective: "Choose the correct cap-boundary answer.",
          rules: null,
          examples: null,
          exerciseConstraints: null,
          tags: [],
        },
        sourceContext: null,
        requestedCount: 3,
      });
    };
    const activationResultsPromise = Promise.all(
      skillIds.map((skillId) =>
        activateSkillDraft({
          userId,
          skillId,
          now,
          generateChoiceExercises: generator,
          verifyChoiceExercises: acceptAllVerifier,
          model: "test-gemini",
        }),
      ),
    );
    await vi.waitFor(() => expect(startedCount).toBe(1));
    releaseGenerators?.();
    const activationResults = await activationResultsPromise;

    expect(
      activationResults.map((result) =>
        result.status === "activated"
          ? "activated"
          : `${result.status}:${result.reason}`,
      ).sort(),
    ).toEqual(["activated", "not-activated:quota-exceeded"]);
    await expect(
      prisma.skill.count({
        where: {
          userId,
          status: { in: [SkillStatus.ACTIVE, SkillStatus.PAUSED] },
        },
      }),
    ).resolves.toBe(ALPHA_ACTIVE_SKILLS);
  });

  it("retries draft activation with a new job after superseding a stale worker", async () => {
    const userId = await createUser("activate_stale_job");
    const draft = await createSkillDraft({
      userId,
      input: {
        title: "Spanish cardinal numbers",
        objective: "Choose the correct Spanish cardinal number for simple quantities.",
      },
    });

    if (draft.status !== "created") {
      throw new Error("Expected draft creation to succeed.");
    }

    const staleJob = await prisma.generationJob.create({
      data: {
        userId,
        skillId: draft.skill.id,
        kind: GenerationJobKind.SKILL_ACTIVATION,
        status: GenerationJobStatus.RUNNING,
        provider: "google",
        model: "stale-test-gemini",
        promptVersion: "skill-mcq-v0",
        requestedCount: 5,
        startedAt: new Date(now.getTime() - ACTIVATION_GENERATION_STALE_AFTER_MS - 1_000),
        updatedAt: new Date(now.getTime() - ACTIVATION_GENERATION_STALE_AFTER_MS - 1_000),
      },
    });

    const activated = await activateSkillDraft({
      userId,
      skillId: draft.skill.id,
      now,
      generateChoiceExercises: successfulGenerator,
      verifyChoiceExercises: acceptAllVerifier,
      model: "test-gemini",
    });

    expect(activated).toMatchObject({
      status: "activated",
      exerciseCount: 3,
    });
    if (activated.status !== "activated") {
      throw new Error("Expected the replacement activation to succeed.");
    }
    expect(activated.generationJobId).not.toBe(staleJob.id);

    const [skill, generationJobs] = await Promise.all([
      prisma.skill.findUniqueOrThrow({ where: { id: draft.skill.id } }),
      prisma.generationJob.findMany({ where: { userId, skillId: draft.skill.id } }),
    ]);

    expect(skill.status).toBe(SkillStatus.ACTIVE);
    expect(generationJobs).toHaveLength(2);
    expect(
      generationJobs.find((generationJob) => generationJob.id === staleJob.id),
    ).toMatchObject({
      id: staleJob.id,
      status: GenerationJobStatus.FAILED,
      model: "stale-test-gemini",
      errorMessage: ACTIVATION_STALE_JOB_MESSAGE,
      completedAt: expect.any(Date),
    });
    expect(
      generationJobs.find(
        (generationJob) =>
          generationJob.id === activated.generationJobId,
      ),
    ).toMatchObject({
      id: activated.generationJobId,
      status: GenerationJobStatus.SUCCEEDED,
      model: "test-gemini",
      acceptedCount: 3,
      rejectedCount: 0,
      errorMessage: null,
      startedAt: expect.any(Date),
      completedAt: expect.any(Date),
    });
  });

  it("does not reclaim an activation job during the completion slack window", async () => {
    const userId = await createUser("activate_verifying_job");
    const draft = await createSkillDraft({
      userId,
      input: {
        title: "Spanish fractions",
        objective: "Choose the correct Spanish expression for simple fractions.",
      },
    });

    if (draft.status !== "created") {
      throw new Error("Expected draft creation to succeed.");
    }

    const startedAt = new Date(now.getTime() - 90_001);
    const runningJob = await prisma.generationJob.create({
      data: {
        userId,
        skillId: draft.skill.id,
        kind: GenerationJobKind.SKILL_ACTIVATION,
        status: GenerationJobStatus.RUNNING,
        provider: "google",
        model: "test-gemini",
        promptVersion: "skill-mcq-v0",
        requestedCount: 5,
        startedAt,
      },
    });
    const generator = vi.fn(successfulGenerator);

    const result = await activateSkillDraft({
      userId,
      skillId: draft.skill.id,
      now,
      generateChoiceExercises: generator,
      verifyChoiceExercises: acceptAllVerifier,
      model: "test-gemini",
    });

    expect(result).toMatchObject({
      status: "not-activated",
      reason: "activation-in-progress",
      generationJobId: runningJob.id,
    });
    expect(generator).not.toHaveBeenCalled();

    const generationJob = await prisma.generationJob.findUniqueOrThrow({
      where: { id: runningJob.id },
    });
    expect(generationJob.status).toBe(GenerationJobStatus.RUNNING);
    expect(generationJob.startedAt).toEqual(startedAt);
  });

  it("does not start a second activation while one is already running", async () => {
    const userId = await createUser("activate_running_job");
    const draft = await createSkillDraft({
      userId,
      input: {
        title: "Spanish ordinal numbers",
        objective: "Choose the correct Spanish ordinal number for simple rankings.",
      },
    });

    if (draft.status !== "created") {
      throw new Error("Expected draft creation to succeed.");
    }

    const runningJob = await prisma.generationJob.create({
      data: {
        userId,
        skillId: draft.skill.id,
        kind: GenerationJobKind.SKILL_ACTIVATION,
        status: GenerationJobStatus.RUNNING,
        provider: "google",
        model: "test-gemini",
        promptVersion: "skill-mcq-v0",
        requestedCount: 5,
        startedAt: now,
      },
    });
    const generator = vi.fn(successfulGenerator);

    const result = await activateSkillDraft({
      userId,
      skillId: draft.skill.id,
      now,
      generateChoiceExercises: generator,
      verifyChoiceExercises: acceptAllVerifier,
      model: "test-gemini",
    });

    expect(result).toMatchObject({
      status: "not-activated",
      reason: "activation-in-progress",
      generationJobId: runningJob.id,
    });
    expect(generator).not.toHaveBeenCalled();

    const [skill, exerciseCount, generationJob] = await Promise.all([
      prisma.skill.findUniqueOrThrow({ where: { id: draft.skill.id } }),
      prisma.exercise.count({ where: { userId, skillId: draft.skill.id } }),
      prisma.generationJob.findUniqueOrThrow({ where: { id: runningJob.id } }),
    ]);

    expect(skill.status).toBe(SkillStatus.DRAFT);
    expect(exerciseCount).toBe(0);
    expect(generationJob.status).toBe(GenerationJobStatus.RUNNING);
  });

  it("persists only verifier-approved exercises and counts rejected candidates", async () => {
    const userId = await createUser("verified_subset");
    const draft = await createSkillDraft({
      userId,
      input: {
        title: "Spanish prepositions",
        objective: "Choose the correct meaning for common Spanish prepositions.",
      },
    });

    if (draft.status !== "created") {
      throw new Error("Expected draft creation to succeed.");
    }

    const selectiveGenerator: ChoiceExerciseGenerator = async () => ({
      exercises: Array.from({ length: 5 }, (_, index) => generatedExercise(index + 1)),
    });
    const selectiveVerifier: ChoiceExerciseVerifier = async (input) => ({
      verifications: input.candidates.map((candidate) =>
        candidate.candidateId === "candidate-2" || candidate.candidateId === "candidate-4"
          ? {
              candidateId: candidate.candidateId,
              verdict: "rejected",
              reason: "ambiguous",
              note: "The distractors are not clearly wrong.",
            }
          : {
              candidateId: candidate.candidateId,
              verdict: "verified",
            },
      ),
    });

    const activated = await activateSkillDraft({
      userId,
      skillId: draft.skill.id,
      now,
      generateChoiceExercises: selectiveGenerator,
      verifyChoiceExercises: selectiveVerifier,
      model: "test-gemini",
    });

    expect(activated).toMatchObject({
      status: "activated",
      exerciseCount: 3,
    });

    const [skill, exercises, generationJob] = await Promise.all([
      prisma.skill.findUniqueOrThrow({ where: { id: draft.skill.id } }),
      prisma.exercise.findMany({
        where: { userId, skillId: draft.skill.id },
        orderBy: { prompt: "asc" },
        select: { prompt: true, verificationStatus: true },
      }),
      prisma.generationJob.findFirstOrThrow({ where: { userId, skillId: draft.skill.id } }),
    ]);

    expect(skill.status).toBe(SkillStatus.ACTIVE);
    expect(exercises.map((exercise) => exercise.prompt)).toEqual([
      "What is the best translation for item 1?",
      "What is the best translation for item 3?",
      "What is the best translation for item 5?",
    ]);
    expect(
      exercises.every(
        (exercise) => exercise.verificationStatus === ExerciseVerificationStatus.VERIFIED,
      ),
    ).toBe(true);
    expect(generationJob).toMatchObject({
      status: GenerationJobStatus.SUCCEEDED,
      acceptedCount: 3,
      rejectedCount: 2,
    });
  });

  it("keeps a skill draft inactive when verification approves too few exercises", async () => {
    const userId = await createUser("failed_verification");
    const draft = await createSkillDraft({
      userId,
      input: {
        title: "French classroom verbs",
        objective: "Choose the correct meaning for common French classroom verbs.",
      },
    });

    if (draft.status !== "created") {
      throw new Error("Expected draft creation to succeed.");
    }

    const sparseVerifier: ChoiceExerciseVerifier = async (input) => ({
      verifications: input.candidates.map((candidate) =>
        candidate.candidateId === "candidate-1"
          ? {
              candidateId: candidate.candidateId,
              verdict: "verified",
            }
          : {
              candidateId: candidate.candidateId,
              verdict: "rejected",
              reason: "source_mismatch",
            },
      ),
    });

    const result = await activateSkillDraft({
      userId,
      skillId: draft.skill.id,
      now,
      generateChoiceExercises: successfulGenerator,
      verifyChoiceExercises: sparseVerifier,
      model: "test-gemini",
    });

    expect(result).toMatchObject({
      status: "not-activated",
      reason: "invalid-verification",
    });

    const [skill, exerciseCount, generationJob, dashboard, nextPracticeItem] = await Promise.all([
      prisma.skill.findUniqueOrThrow({ where: { id: draft.skill.id } }),
      prisma.exercise.count({ where: { userId, skillId: draft.skill.id } }),
      prisma.generationJob.findFirstOrThrow({ where: { userId, skillId: draft.skill.id } }),
      getDashboardHome({ userId, now }),
      getNextChoicePracticeItemForUser(userId, now),
    ]);

    expect(skill.status).toBe(SkillStatus.DRAFT);
    expect(skill.dueAt).toBeNull();
    expect(exerciseCount).toBe(0);
    expect(generationJob).toMatchObject({
      status: GenerationJobStatus.FAILED,
      acceptedCount: 1,
      rejectedCount: 2,
    });
    expect(dashboard.readyNowCount).toBe(0);
    expect(nextPracticeItem.status).toBe("none-due");
  });

  it("keeps a skill draft inactive when verification fails after structural validation", async () => {
    const userId = await createUser("verification_error");
    const draft = await createSkillDraft({
      userId,
      input: {
        title: "German classroom nouns",
        objective: "Choose the correct meaning for common German classroom nouns.",
      },
    });

    if (draft.status !== "created") {
      throw new Error("Expected draft creation to succeed.");
    }

    const partiallyValidGenerator: ChoiceExerciseGenerator = async () => ({
      exercises: [
        generatedExercise(1),
        generatedExercise(2),
        generatedExercise(3),
        {
          ...generatedExercise(4),
          correctChoiceId: "missing",
        },
      ],
    });
    const failingVerifier: ChoiceExerciseVerifier = async () => {
      throw new Error("verifier unavailable");
    };

    const result = await activateSkillDraft({
      userId,
      skillId: draft.skill.id,
      now,
      generateChoiceExercises: partiallyValidGenerator,
      verifyChoiceExercises: failingVerifier,
      model: "test-gemini",
    });

    expect(result).toMatchObject({
      status: "not-activated",
      reason: "verification-failed",
    });

    const [skill, exerciseCount, generationJob] = await Promise.all([
      prisma.skill.findUniqueOrThrow({ where: { id: draft.skill.id } }),
      prisma.exercise.count({ where: { userId, skillId: draft.skill.id } }),
      prisma.generationJob.findFirstOrThrow({ where: { userId, skillId: draft.skill.id } }),
    ]);

    expect(skill.status).toBe(SkillStatus.DRAFT);
    expect(exerciseCount).toBe(0);
    expect(generationJob).toMatchObject({
      status: GenerationJobStatus.FAILED,
      acceptedCount: 0,
      rejectedCount: 4,
    });
  });

  it("keeps a skill draft inactive when generation validation fails", async () => {
    const userId = await createUser("failed_generation");
    const draft = await createSkillDraft({
      userId,
      input: {
        title: "French greetings",
        objective: "Choose the correct meaning for short French greeting phrases.",
      },
    });

    if (draft.status !== "created") {
      throw new Error("Expected draft creation to succeed.");
    }

    const result = await activateSkillDraft({
      userId,
      skillId: draft.skill.id,
      now,
      generateChoiceExercises: async () => ({
        exercises: [generatedExercise(1), generatedExercise(2)],
      }),
      model: "test-gemini",
    });

    expect(result).toMatchObject({
      status: "not-activated",
      reason: "invalid-generation",
    });

    const skill = await prisma.skill.findUniqueOrThrow({
      where: { id: draft.skill.id },
      include: {
        exercises: true,
        generationJobs: true,
      },
    });

    expect(skill.status).toBe(SkillStatus.DRAFT);
    expect(skill.dueAt).toBeNull();
    expect(skill.exercises).toHaveLength(0);
    expect(skill.generationJobs).toHaveLength(1);
    expect(skill.generationJobs[0]).toMatchObject({
      status: GenerationJobStatus.FAILED,
      acceptedCount: 2,
      rejectedCount: 0,
    });
    await expect(prisma.generationAuditRecord.findFirstOrThrow({
      where: { jobId: skill.generationJobs[0].id, checkpoint: "failed" },
      select: { decision: true, failureCategory: true, stageMetrics: true },
    })).resolves.toMatchObject({
      decision: GenerationAuditDecision.FAILED,
      failureCategory: skill.generationJobs[0].failureCategory,
      stageMetrics: expect.objectContaining({ acceptedCount: 2, rejectedCount: 0 }),
    });
  });

  it("creates a source-backed draft with a ready text source and ownership link", async () => {
    const userId = await createUser("source_draft");
    const result = await createSkillDraftFromSource({
      userId,
      now,
      model: "test-gemini",
      generateSkillDraft: successfulSkillDraftGenerator,
      input: {
        sourceText:
          "Use ser for identity and long-term traits. Use estar for location and temporary states. Classroom practice should use short sentences with one obvious choice.",
        sourceLabel: "Spanish chapter notes",
        focusNote: "Keep this at a beginner grammar level.",
        collectionName: "Spanish grammar",
        tags: "Spanish, verbs",
      },
    });

    expect(result.status).toBe("created");

    if (result.status !== "created") {
      throw new Error("Expected source draft creation to succeed.");
    }

    expect(result.skills).toHaveLength(1);
    expect(result.skillSourceRefIds).toHaveLength(1);

    const skill = await prisma.skill.findUniqueOrThrow({
      where: { id: result.skills[0].id },
      include: {
        collection: true,
        sourceRefs: {
          include: {
            sourceFile: true,
          },
        },
      },
    });

    expect(skill).toMatchObject({
      userId,
      title: generatedSkillDraft.title,
      objective: generatedSkillDraft.objective,
      status: SkillStatus.DRAFT,
      tags: ["spanish", "verbs", "grammar"],
    });
    expect(skill.collection?.name).toBe("Spanish grammar");
    expect(skill.sourceRefs).toHaveLength(1);
    expect(skill.sourceRefs[0]).toMatchObject({
      userId,
      note: "Keep this at a beginner grammar level.",
    });
    expect(skill.sourceRefs[0].sourceFile).toMatchObject({
      userId,
      collectionId: skill.collectionId,
      kind: SourceFileKind.TEXT,
      status: SourceFileStatus.READY,
      originalName: "Spanish chapter notes",
      mimeType: "text/plain",
    });
    expect(skill.sourceRefs[0].sourceFile.extractedText).toContain("Use ser for identity");
  });

  it("saves pasted source material when skill preparation fails", async () => {
    const userId = await createUser("source_draft_failed_saved");
    const sourceText =
      "Use ser for identity and long-term traits. Use estar for location and temporary states. Classroom practice should use short sentences with one obvious choice.";
    const result = await createSkillDraftFromSource({
      userId,
      now,
      model: "test-gemini",
      generateSkillDraft: async () => ({
        drafts: [
          {
            title: "Bad draft",
            objective: "too short",
            rules: [],
            examples: [],
            exerciseConstraints: "",
            tags: [],
          },
        ],
      }),
      input: {
        sourceText,
        sourceLabel: "Pasted ser estar notes",
      },
      persistFailedSource: true,
      skipUsageLimitCheck: true,
    });

    expect(result).toMatchObject({
      status: "not-created",
      reason: "invalid-generation",
    });

    const sourceFile = await prisma.sourceFile.findFirstOrThrow({
      where: {
        userId,
        kind: SourceFileKind.TEXT,
      },
    });
    expect(sourceFile).toMatchObject({
      status: SourceFileStatus.FAILED,
      originalName: "Pasted ser estar notes",
      mimeType: "text/plain",
      extractedText: sourceText,
    });
    expect(sourceFile.metadata).toMatchObject({
      failureReason: "invalid-generation",
    });
  });

  it("hides in-flight and linked pasted sources from skill creation recovery", async () => {
    const userId = await createUser("source_recovery_processing_text_hidden");
    const failedSource = await prisma.sourceFile.create({
      data: {
        userId,
        kind: SourceFileKind.TEXT,
        status: SourceFileStatus.FAILED,
        originalName: "failed pasted source",
        mimeType: "text/plain",
        byteSize: 120,
        extractedText:
          "Use ser for identity and long-term traits. Use estar for location and temporary states.",
      },
    });
    const processingSource = await prisma.sourceFile.create({
      data: {
        userId,
        kind: SourceFileKind.TEXT,
        status: SourceFileStatus.PROCESSING,
        originalName: "processing pasted source",
        mimeType: "text/plain",
        byteSize: 120,
        extractedText:
          "Use preterite for completed past actions and imperfect for background descriptions.",
      },
    });
    const linkedSource = await prisma.sourceFile.create({
      data: {
        userId,
        kind: SourceFileKind.TEXT,
        status: SourceFileStatus.FAILED,
        originalName: "linked pasted source",
        mimeType: "text/plain",
        byteSize: 120,
        extractedText:
          "Linked material should stay attached to its existing draft instead of appearing as reusable saved text.",
      },
    });
    const linkedSkill = await prisma.skill.create({
      data: {
        userId,
        title: "Linked saved text",
        objective: "Keep linked failed text out of source recovery.",
        rules: [],
        examples: [],
        exerciseConstraints: Prisma.DbNull,
        tags: [],
        status: SkillStatus.DRAFT,
      },
    });
    await prisma.skillSourceRef.create({
      data: {
        userId,
        skillId: linkedSkill.id,
        sourceFileId: linkedSource.id,
      },
    });

    const recoveryItems = await getSkillCreationSourceRecoveryItems({ userId, now });

    expect(recoveryItems.map((item) => item.id)).toContain(failedSource.id);
    expect(recoveryItems.map((item) => item.id)).not.toContain(processingSource.id);
    expect(recoveryItems.map((item) => item.id)).not.toContain(linkedSource.id);
  });

  it("consumes a recovered pasted source and creates a fresh counted retry source", async () => {
    const userId = await createUser("source_recovery_consumed");
    const sourceText =
      "Use ser for identity and long-term traits. Use estar for location and temporary states. Classroom practice should use short sentences with one obvious choice.";
    const failedSource = await prisma.sourceFile.create({
      data: {
        userId,
        kind: SourceFileKind.TEXT,
        status: SourceFileStatus.FAILED,
        originalName: "failed pasted source",
        mimeType: "text/plain",
        byteSize: Buffer.byteLength(sourceText, "utf8"),
        extractedText: sourceText,
      },
    });

    const result = await createSkillDraftFromSource({
      userId,
      now,
      model: "test-gemini",
      generateSkillDraft: successfulSkillDraftGenerator,
      input: {
        sourceText,
        sourceLabel: "Recovered pasted source",
      },
      recoveredSourceFileId: failedSource.id,
      skipUsageLimitCheck: true,
    });

    expect(result).toMatchObject({
      status: "created",
    });

    if (result.status !== "created") {
      throw new Error("Expected source recovery draft creation to succeed.");
    }

    expect(result.sourceFileId).not.toBe(failedSource.id);

    const consumedSource = await prisma.sourceFile.findUniqueOrThrow({
      where: { id: failedSource.id },
    });
    expect(consumedSource.status).toBe(SourceFileStatus.READY);
    expect(consumedSource.metadata).toMatchObject({
      recoveredIntoSourceFileId: result.sourceFileId,
    });

    const retrySource = await prisma.sourceFile.findUniqueOrThrow({
      where: { id: result.sourceFileId },
      include: {
        skillRefs: true,
      },
    });
    expect(retrySource.status).toBe(SourceFileStatus.READY);
    expect(retrySource.originalName).toBe("Recovered pasted source");
    expect(retrySource.metadata).toMatchObject({
      recoveredFromSourceFileId: failedSource.id,
    });
    expect(retrySource.skillRefs).toHaveLength(1);

    const pastedSourceCount = await prisma.sourceFile.count({
      where: {
        userId,
        kind: SourceFileKind.TEXT,
      },
    });
    expect(pastedSourceCount).toBe(2);

    const recoveryItems = await getSkillCreationSourceRecoveryItems({ userId, now });
    expect(recoveryItems.map((item) => item.id)).not.toContain(failedSource.id);
    expect(recoveryItems.map((item) => item.id)).not.toContain(result.sourceFileId);
  });

  it("creates a fresh recoverable retry source when recovered pasted text fails again", async () => {
    const userId = await createUser("source_recovery_failed_retry");
    const sourceText =
      "Use ser for identity and long-term traits. Use estar for location and temporary states. Classroom practice should use short sentences with one obvious choice.";
    const failedSource = await prisma.sourceFile.create({
      data: {
        userId,
        kind: SourceFileKind.TEXT,
        status: SourceFileStatus.FAILED,
        originalName: "failed pasted source",
        mimeType: "text/plain",
        byteSize: Buffer.byteLength(sourceText, "utf8"),
        extractedText: sourceText,
      },
    });

    const result = await createSkillDraftFromSource({
      userId,
      now,
      model: "test-gemini",
      generateSkillDraft: async () => {
        throw new Error("Model unavailable.");
      },
      input: {
        sourceText,
        sourceLabel: "Recovered pasted source",
      },
      recoveredSourceFileId: failedSource.id,
      skipUsageLimitCheck: true,
    });

    expect(result).toMatchObject({
      status: "not-created",
      reason: "generation-failed",
    });

    const sourceFiles = await prisma.sourceFile.findMany({
      where: {
        userId,
        kind: SourceFileKind.TEXT,
      },
      orderBy: {
        createdAt: "asc",
      },
    });
    expect(sourceFiles).toHaveLength(2);
    expect(sourceFiles[0]).toMatchObject({
      id: failedSource.id,
      status: SourceFileStatus.READY,
    });
    expect(sourceFiles[1]).toMatchObject({
      status: SourceFileStatus.FAILED,
      originalName: "Recovered pasted source",
    });
    expect(sourceFiles[1].metadata).toMatchObject({
      recoveredFromSourceFileId: failedSource.id,
      failureReason: "generation-failed",
    });

    const recoveryItems = await getSkillCreationSourceRecoveryItems({ userId, now });
    expect(recoveryItems.map((item) => item.id)).not.toContain(failedSource.id);
    expect(recoveryItems.map((item) => item.id)).toContain(sourceFiles[1].id);
  });

  it("links generated draft skills to one source", async () => {
    const userId = await createUser("source_split");
    const result = await createSkillDraftFromSource({
      userId,
      now,
      model: "test-gemini",
      generateSkillDraft: async () => ({
        drafts: [generatedSkillDraft],
      }),
      input: {
        sourceText:
          "This broad Spanish review page covers ser and estar, regular preterite endings, and imperfect background actions with classroom examples for each topic.",
        sourceLabel: "Spanish unit review",
        focusNote: "Create one focused grammar skill.",
        collectionName: "Spanish grammar",
        tags: "Spanish, review",
      },
    });

    expect(result.status).toBe("created");

    if (result.status !== "created") {
      throw new Error("Expected source draft creation to succeed.");
    }

    expect(result.skills).toHaveLength(1);
    expect(result.skillSourceRefIds).toHaveLength(1);

    const sourceFile = await prisma.sourceFile.findUniqueOrThrow({
      where: { id: result.sourceFileId },
      include: {
        skillRefs: {
          orderBy: { createdAt: "asc" },
          include: {
            skill: {
              include: {
                collection: true,
              },
            },
          },
        },
      },
    });

    expect(sourceFile).toMatchObject({
      userId,
      kind: SourceFileKind.TEXT,
      status: SourceFileStatus.READY,
      originalName: "Spanish unit review",
      mimeType: "text/plain",
    });
    expect(sourceFile.metadata).toMatchObject({
      focusNote: "Create one focused grammar skill.",
      model: "test-gemini",
    });
    expect(sourceFile.skillRefs).toHaveLength(1);
    expect(sourceFile.skillRefs[0].skill.title).toBe(generatedSkillDraft.title);
    expect(
      sourceFile.skillRefs.every(
        (ref) =>
          ref.userId === userId &&
          ref.sourceFileId === result.sourceFileId &&
          ref.note === "Create one focused grammar skill." &&
          ref.skill.collection?.name === "Spanish grammar",
      ),
    ).toBe(true);
    expect(
      sourceFile.skillRefs.every((ref) => ref.skill.tags.includes("spanish")),
    ).toBe(true);
    expect(
      sourceFile.skillRefs.every((ref) => ref.skill.tags.includes("review")),
    ).toBe(true);

    const library = await getSkillsLibrary({ userId, now });
    expect(library.draftSkills.map((skill) => skill.title).toSorted()).toEqual(
      [generatedSkillDraft.title],
    );
    expect(library.draftSkills.every((skill) => skill.sourceRefCount === 1)).toBe(true);

    const firstRef = sourceFile.skillRefs[0];
    const removed = await removeSkillSource({
      userId,
      skillId: firstRef.skillId,
      sourceRefId: firstRef.id,
    });

    expect(removed).toMatchObject({
      status: "removed",
      sourceFileDeleted: true,
    });
    await expect(
      prisma.sourceFile.count({ where: { id: result.sourceFileId, userId } }),
    ).resolves.toBe(0);
    await expect(
      prisma.skillSourceRef.count({ where: { sourceFileId: result.sourceFileId, userId } }),
    ).resolves.toBe(0);
  });

  it("creates source-backed draft skills from an uploaded source object", async () => {
    const userId = await createUser("upload_success");
    const uploadedBytes = Buffer.from("uploaded worksheet original image bytes");
    const { storage, presignedUploadInputs, getObjectByteInputs } = createFakeUploadStorage({
      bytes: uploadedBytes,
      byteSize: 4096,
    });
    const { sender, events } = createFakeSourceUploadSender();
    const prepared = await prepareSourceUpload({
      userId,
      now,
      storage,
      input: {
        originalName: "worksheet.png",
        mimeType: "image/png",
        byteSize: "4096",
        sourceLabel: "Uploaded worksheet",
        focusNote: "Split this into small grammar skills.",
        collectionName: "Spanish uploads",
        tags: "Spanish, upload",
      },
    });

    expect(prepared.status).toBe("prepared");

    if (prepared.status !== "prepared") {
      throw new Error("Expected upload preparation to succeed.");
    }

    expect(prepared.uploadUrl).toBe("https://s3.example.test/presigned-upload");
    expect(prepared.headers).toEqual({ "Content-Type": "image/png" });
    expect(prepared.objectKey).toContain(`source-uploads/${userId}/`);
    expect(presignedUploadInputs).toEqual([
      {
        key: prepared.objectKey,
        mimeType: "image/png",
        byteSize: 4096,
        maxBytes: MAX_SOURCE_UPLOAD_BYTES,
        expiresInSeconds: 600,
      },
    ]);

    const queued = await queueSourceUploadDrafts({
      userId,
      sourceFileId: prepared.sourceFileId,
      now,
      storage,
      eventSender: sender,
    });

    expect(queued).toMatchObject({
      status: "queued",
      sourceFileId: prepared.sourceFileId,
    });
    expect(events).toEqual([
      {
        userId,
        sourceFileId: prepared.sourceFileId,
        requestedAt: now.toISOString(),
      },
    ]);
    await expect(prisma.skill.count({ where: { userId } })).resolves.toBe(0);

    const queuedLibrary = await getSkillsLibrary({ userId, now });
    expect(queuedLibrary.sourceProcessing).toHaveLength(1);
    expect(queuedLibrary.sourceProcessing[0]).toMatchObject({
      id: prepared.sourceFileId,
      originalName: "Uploaded worksheet",
      status: SourceFileStatus.UPLOADED,
    });

    let capturedSkillDraftSourceMedia: SourceMediaContext[] | undefined;
    const completed = await runQueuedSourceUploadDraftJob({
      userId,
      sourceFileId: prepared.sourceFileId,
      now,
      storage,
      extractSourceText: successfulSourceExtractor,
      generateSkillDraft: async (input) => {
        capturedSkillDraftSourceMedia = input.sourceMedia;
        expect(input.sourceContext).toContain("Use ser for identity");
        return {
          drafts: [generatedSkillDraft],
        };
      },
      model: "test-gemini",
    });

    expect(completed.status).toBe("created");
    expectSingleSourceMedia(capturedSkillDraftSourceMedia, {
      sourceFileId: prepared.sourceFileId,
      label: "Uploaded worksheet",
      mimeType: "image/png",
      bytes: uploadedBytes,
    });
    expect(getObjectByteInputs).toEqual([
      {
        key: prepared.objectKey,
        bucket: "learnrecur-dev",
        maxBytes: MAX_SOURCE_UPLOAD_BYTES,
      },
    ]);

    if (completed.status !== "created") {
      throw new Error("Expected uploaded source completion to succeed.");
    }

    expect(completed.skills).toHaveLength(1);
    expect(completed.skillSourceRefIds).toHaveLength(1);

    const sourceFile = await prisma.sourceFile.findUniqueOrThrow({
      where: { id: completed.sourceFileId },
      include: {
        skillRefs: {
          include: {
            skill: {
              include: {
                collection: true,
              },
            },
          },
        },
      },
    });

    expect(sourceFile).toMatchObject({
      userId,
      kind: SourceFileKind.IMAGE,
      status: SourceFileStatus.READY,
      originalName: "Uploaded worksheet",
      mimeType: "image/png",
      byteSize: 4096,
      storageBucket: "learnrecur-dev",
    });
    expect(sourceFile.storageKey).toContain(`source-uploads/${userId}/`);
    expect(sourceFile.extractedText).toContain("Use ser for identity");
    expect(sourceFile.metadata).toMatchObject({
      createdBy: "source-upload-drafts-v0",
      originalFileName: "worksheet.png",
      focusNote: "Split this into small grammar skills.",
      model: "test-gemini",
    });
    expect(sourceFile.skillRefs).toHaveLength(1);
    expect(sourceFile.skillRefs.every((ref) => ref.skill.collection?.name === "Spanish uploads")).toBe(
      true,
    );

    const library = await getSkillsLibrary({ userId, now });
    expect(library.draftSkills.map((skill) => skill.title).toSorted()).toEqual(
      [generatedSkillDraft.title],
    );
    expect(library.draftSkills.every((skill) => skill.sourceRefCount === 1)).toBe(true);
    expect(library.sourceProcessing).toHaveLength(0);
  });

  it("creates source-backed draft skills from multiple uploaded source objects", async () => {
    const userId = await createUser("upload_multi_success");
    const uploadedBytes = Buffer.from("uploaded worksheet original image bytes for a multi-source skill");
    const { storage, getObjectByteInputs } = createFakeUploadStorage({
      bytes: uploadedBytes,
      byteSize: 4096,
    });
    const preparedSourceFileIds: string[] = [];

    for (const [index, originalName] of ["worksheet-page-1.png", "worksheet-page-2.png"].entries()) {
      const prepared = await prepareSourceUpload({
        userId,
        now,
        storage,
        input: {
          originalName,
          mimeType: "image/png",
          byteSize: "4096",
          sourceLabel: `Uploaded worksheet ${index + 1}`,
          focusNote: "Create one focused grammar skill from the full worksheet.",
          collectionName: "Spanish uploads",
          tags: "Spanish, upload",
        },
      });

      expect(prepared.status).toBe("prepared");

      if (prepared.status !== "prepared") {
        throw new Error("Expected upload preparation to succeed.");
      }

      preparedSourceFileIds.push(prepared.sourceFileId);
    }

    const extractionInputs: Parameters<SourceTextExtractor>[0][] = [];
    let capturedSkillDraftSourceMedia: SourceMediaContext[] | undefined;
    const completed = await completeSourceUploadDrafts({
      userId,
      sourceFileId: preparedSourceFileIds[0],
      sourceFileIds: preparedSourceFileIds,
      now,
      storage,
      extractSourceText: async (input) => {
        extractionInputs.push(input);
        return {
          extractedText: `Extracted details for ${input.sourceLabel}. Use ser for identity and estar for location. Keep examples short and classroom-focused.`,
        };
      },
      generateSkillDraft: async (input) => {
        capturedSkillDraftSourceMedia = input.sourceMedia;
        expect(input.sourceContext).toContain("Source 1: Uploaded worksheet 1");
        expect(input.sourceContext).toContain("Source 2: Uploaded worksheet 2");
        return {
          drafts: [generatedSkillDraft],
        };
      },
      model: "test-gemini",
    });

    expect(completed.status).toBe("created");

    if (completed.status !== "created") {
      throw new Error("Expected uploaded source completion to succeed.");
    }

    expect(completed.sourceFileIds).toEqual(preparedSourceFileIds);
    expect(completed.skillSourceRefIds).toHaveLength(2);
    expect(extractionInputs.map((input) => input.sourceLabel)).toEqual([
      "Uploaded worksheet 1",
      "Uploaded worksheet 2",
    ]);
    expect(capturedSkillDraftSourceMedia).toHaveLength(2);
    expect(capturedSkillDraftSourceMedia?.map((source) => source.sourceFileId)).toEqual(
      preparedSourceFileIds,
    );
    expect(capturedSkillDraftSourceMedia?.map((source) => source.label)).toEqual([
      "Uploaded worksheet 1",
      "Uploaded worksheet 2",
    ]);
    expect(capturedSkillDraftSourceMedia?.every((source) => source.bytes.equals(uploadedBytes))).toBe(
      true,
    );
    expect(getObjectByteInputs).toHaveLength(2);

    const skill = completed.skills[0];

    if (!skill) {
      throw new Error("Expected a generated draft skill.");
    }

    const sourceRefs = await prisma.skillSourceRef.findMany({
      where: {
        skillId: skill.id,
        userId,
      },
      orderBy: {
        createdAt: "asc",
      },
    });

    expect(sourceRefs.map((ref) => ref.sourceFileId).toSorted()).toEqual(
      preparedSourceFileIds.toSorted(),
    );

    const sourceFiles = await prisma.sourceFile.findMany({
      where: {
        id: {
          in: preparedSourceFileIds,
        },
        userId,
      },
      orderBy: {
        originalName: "asc",
      },
    });

    expect(sourceFiles).toHaveLength(2);
    expect(sourceFiles.every((sourceFile) => sourceFile.status === SourceFileStatus.READY)).toBe(
      true,
    );
    expect(sourceFiles.map((sourceFile) => sourceFile.extractedText)).toEqual([
      expect.stringContaining("Uploaded worksheet 1"),
      expect.stringContaining("Uploaded worksheet 2"),
    ]);

    const library = await getSkillsLibrary({ userId, now });
    expect(library.draftSkills).toHaveLength(1);
    expect(library.draftSkills[0]?.sourceRefCount).toBe(2);
    expect(library.sourceProcessing).toHaveLength(0);
  });

  it("starts multi-source extraction calls concurrently", async () => {
    const userId = await createUser("upload_multi_parallel_extraction");
    const { storage } = createFakeUploadStorage({
      bytes: Buffer.from("uploaded worksheet original image bytes for parallel extraction"),
      byteSize: 4096,
    });
    const preparedSourceFileIds: string[] = [];

    for (const [index, originalName] of ["worksheet-page-1.png", "worksheet-page-2.png"].entries()) {
      const prepared = await prepareSourceUpload({
        userId,
        now,
        storage,
        input: {
          originalName,
          mimeType: "image/png",
          byteSize: "4096",
          sourceLabel: `Parallel worksheet ${index + 1}`,
        },
      });

      if (prepared.status !== "prepared") {
        throw new Error("Expected upload preparation to succeed.");
      }

      preparedSourceFileIds.push(prepared.sourceFileId);
    }

    const extractionInputs: string[] = [];
    let resolveSecondExtractionStarted: (() => void) | undefined;
    const secondExtractionStarted = new Promise<void>((resolve) => {
      resolveSecondExtractionStarted = resolve;
    });
    const waitForSecondExtractionStarted = () => {
      return new Promise<void>((resolve, reject) => {
        const timeoutId = setTimeout(() => {
          reject(new Error("Second extraction did not start before the first extraction resolved."));
        }, 250);

        secondExtractionStarted.then(
          () => {
            clearTimeout(timeoutId);
            resolve();
          },
          reject,
        );
      });
    };

    const completed = await completeSourceUploadDrafts({
      userId,
      sourceFileId: preparedSourceFileIds[0],
      sourceFileIds: preparedSourceFileIds,
      now,
      storage,
      extractSourceText: async (input) => {
        if (!input.sourceLabel) {
          throw new Error("expected a source label");
        }
        extractionInputs.push(input.sourceLabel);

        if (extractionInputs.length === 2) {
          resolveSecondExtractionStarted?.();
        }

        if (extractionInputs.length === 1) {
          await waitForSecondExtractionStarted();
        }

        return {
          extractedText: `Extracted parallel details for ${input.sourceLabel}. Use ser for identity and estar for location. Keep examples short and classroom-focused.`,
        };
      },
      generateSkillDraft: async (input) => {
        expect(input.sourceContext).toContain("Source 1: Parallel worksheet 1");
        expect(input.sourceContext).toContain("Source 2: Parallel worksheet 2");
        return {
          drafts: [generatedSkillDraft],
        };
      },
      model: "test-gemini",
    });

    expect(completed.status).toBe("created");
    expect(extractionInputs).toEqual(["Parallel worksheet 1", "Parallel worksheet 2"]);
  });

  it("uses a single normalized sourceFileIds value when completing an upload", async () => {
    const userId = await createUser("upload_single_normalized_id");
    const { storage } = createFakeUploadStorage({
      byteSize: 4096,
    });
    const prepared = await prepareSourceUpload({
      userId,
      now,
      storage,
      input: {
        originalName: "worksheet.png",
        mimeType: "image/png",
        byteSize: "4096",
      },
    });

    if (prepared.status !== "prepared") {
      throw new Error("Expected upload preparation to succeed.");
    }

    const completed = await completeSourceUploadDrafts({
      userId,
      sourceFileId: "wrong-source-id",
      sourceFileIds: [prepared.sourceFileId],
      now,
      storage,
      extractSourceText: successfulSourceExtractor,
      generateSkillDraft: successfulSkillDraftGenerator,
      model: "test-gemini",
    });

    expect(completed.status).toBe("created");
    expect(completed).toMatchObject({
      status: "created",
      sourceFileId: prepared.sourceFileId,
    });
  });

  it("cleans up prepared upload rows before a batch is completed", async () => {
    const userId = await createUser("upload_cleanup_prepared_batch");
    const { storage, deletedKeys } = createFakeUploadStorage();
    const preparedSourceFileIds: string[] = [];
    const preparedObjectKeys: string[] = [];

    for (const originalName of ["worksheet-page-1.png", "worksheet-page-2.png"]) {
      const prepared = await prepareSourceUpload({
        userId,
        now,
        storage,
        input: {
          originalName,
          mimeType: "image/png",
          byteSize: "4096",
        },
      });

      if (prepared.status !== "prepared") {
        throw new Error("Expected upload preparation to succeed.");
      }

      preparedSourceFileIds.push(prepared.sourceFileId);
      preparedObjectKeys.push(prepared.objectKey);
    }

    await cleanupPreparedSourceUploads({
      userId,
      sourceFileIds: preparedSourceFileIds,
      storage,
    });

    await expect(
      prisma.sourceFile.count({
        where: {
          id: {
            in: preparedSourceFileIds,
          },
          userId,
        },
      }),
    ).resolves.toBe(0);
    expect(deletedKeys.toSorted()).toEqual(preparedObjectKeys.toSorted());
  });

  it("does not queue a partial batch when later uploaded source validation fails", async () => {
    const userId = await createUser("upload_batch_queue_atomic");
    let missingObjectKey = "";
    const { storage, deletedKeys } = createFakeUploadStorage({
      headErrorForKey: (input) => {
        return input.key === missingObjectKey ? Object.assign(new Error("missing"), { name: "NoSuchKey" }) : null;
      },
    });
    const preparedSourceFileIds: string[] = [];
    const preparedObjectKeys: string[] = [];

    for (const originalName of ["worksheet-page-1.png", "worksheet-page-2.png"]) {
      const prepared = await prepareSourceUpload({
        userId,
        now,
        storage,
        input: {
          originalName,
          mimeType: "image/png",
          byteSize: "4096",
        },
      });

      if (prepared.status !== "prepared") {
        throw new Error("Expected upload preparation to succeed.");
      }

      preparedSourceFileIds.push(prepared.sourceFileId);
      preparedObjectKeys.push(prepared.objectKey);
    }

    missingObjectKey = preparedObjectKeys[1];

    const completed = await completeSourceUploadDrafts({
      userId,
      sourceFileId: preparedSourceFileIds[0],
      sourceFileIds: preparedSourceFileIds,
      now,
      storage,
      extractSourceText: successfulSourceExtractor,
      generateSkillDraft: successfulSkillDraftGenerator,
      model: "test-gemini",
    });

    expect(completed).toMatchObject({
      status: "not-created",
      reason: "invalid-upload",
    });
    await expect(
      prisma.sourceFile.count({
        where: {
          id: {
            in: preparedSourceFileIds,
          },
          userId,
        },
      }),
    ).resolves.toBe(0);
    expect(deletedKeys.toSorted()).toEqual(preparedObjectKeys.toSorted());
  });

  it("keeps draft batch uploads when queue validation fails transiently", async () => {
    const userId = await createUser("upload_batch_queue_transient_head");
    const transientHeadError = new Error("temporary head failure");
    const { storage, deletedKeys } = createFakeUploadStorage({
      headError: transientHeadError,
    });
    const preparedSourceFileIds: string[] = [];
    const preparedObjectKeys: string[] = [];

    for (const originalName of ["worksheet-page-1.png", "worksheet-page-2.png"]) {
      const prepared = await prepareSourceUpload({
        userId,
        now,
        storage,
        input: {
          originalName,
          mimeType: "image/png",
          byteSize: "4096",
        },
      });

      if (prepared.status !== "prepared") {
        throw new Error("Expected upload preparation to succeed.");
      }

      preparedSourceFileIds.push(prepared.sourceFileId);
      preparedObjectKeys.push(prepared.objectKey);
    }

    const completed = await completeSourceUploadDrafts({
      userId,
      sourceFileId: preparedSourceFileIds[0],
      sourceFileIds: preparedSourceFileIds,
      now,
      storage,
      extractSourceText: successfulSourceExtractor,
      generateSkillDraft: successfulSkillDraftGenerator,
      model: "test-gemini",
    });

    expect(completed).toMatchObject({
      status: "not-created",
      reason: "invalid-upload",
    });
    if (completed.status !== "not-created") {
      throw new Error("expected invalid upload to return a public failure");
    }
    expect(completed.message).toContain("Could not verify S3 upload:");

    const draftSources = await prisma.sourceFile.findMany({
      where: {
        id: {
          in: preparedSourceFileIds,
        },
        userId,
      },
      orderBy: {
        originalName: "asc",
      },
    });

    expect(draftSources).toHaveLength(2);
    expect(draftSources.every((sourceFile) => sourceFile.status === SourceFileStatus.DRAFT)).toBe(
      true,
    );
    expect(draftSources.map((sourceFile) => sourceFile.storageKey).toSorted()).toEqual(
      preparedObjectKeys.toSorted(),
    );
    expect(deletedKeys).toEqual([]);
  });

  it("does not offer per-file retry for failed multi-source upload batches", async () => {
    const userId = await createUser("upload_multi_failed_no_single_retry");
    const { storage } = createFakeUploadStorage({
      byteSize: 4096,
    });
    const preparedSourceFileIds: string[] = [];

    for (const originalName of ["worksheet-page-1.png", "worksheet-page-2.png"]) {
      const prepared = await prepareSourceUpload({
        userId,
        now,
        storage,
        input: {
          originalName,
          mimeType: "image/png",
          byteSize: "4096",
        },
      });

      if (prepared.status !== "prepared") {
        throw new Error("Expected upload preparation to succeed.");
      }

      preparedSourceFileIds.push(prepared.sourceFileId);
    }

    const completed = await completeSourceUploadDrafts({
      userId,
      sourceFileId: preparedSourceFileIds[0],
      sourceFileIds: preparedSourceFileIds,
      now,
      storage,
      extractSourceText: async () => {
        throw new Error("transient extraction failure");
      },
      generateSkillDraft: successfulSkillDraftGenerator,
      model: "test-gemini",
    });

    expect(completed).toMatchObject({
      status: "not-created",
      reason: "extraction-failed",
    });

    const library = await getSkillsLibrary({ userId, now });
    expect(library.sourceProcessing).toHaveLength(2);
    expect(library.sourceProcessing.every((source) => source.status === SourceFileStatus.FAILED)).toBe(
      true,
    );
    expect(library.sourceProcessing.every((source) => source.canRequeue === false)).toBe(true);

    const failedSources = await prisma.sourceFile.findMany({
      where: {
        id: {
          in: preparedSourceFileIds,
        },
        userId,
      },
      orderBy: {
        originalName: "asc",
      },
    });

    expect(failedSources).toHaveLength(2);
    expect(
      failedSources.every((sourceFile) => {
        const metadata = sourceFile.metadata as { batchSourceFileIds?: unknown };
        return (
          Array.isArray(metadata.batchSourceFileIds) &&
          metadata.batchSourceFileIds.length === preparedSourceFileIds.length
        );
      }),
    ).toBe(true);
  });

  it("rolls back generated skill draft source updates when a later source guard fails", async () => {
    const userId = await createUser("upload_multi_guard_rollback");
    const firstSource = await prisma.sourceFile.create({
      data: {
        userId,
        kind: SourceFileKind.IMAGE,
        status: SourceFileStatus.PROCESSING,
        originalName: "worksheet-page-1.png",
        mimeType: "image/png",
        byteSize: 4096,
        storageBucket: "learnrecur-dev",
        storageKey: "source-uploads/user/source_1/worksheet.png",
      },
    });
    const secondSource = await prisma.sourceFile.create({
      data: {
        userId,
        kind: SourceFileKind.IMAGE,
        status: SourceFileStatus.FAILED,
        originalName: "worksheet-page-2.png",
        mimeType: "image/png",
        byteSize: 4096,
        storageBucket: "learnrecur-dev",
        storageKey: "source-uploads/user/source_2/worksheet.png",
      },
    });

    const created = await createGeneratedSkillDraftsForSourceFile({
      userId,
      sourceFileId: firstSource.id,
      sourceFileGuard: {
        status: SourceFileStatus.PROCESSING,
      },
      sourceFileUpdate: {
        status: SourceFileStatus.READY,
        extractedText: "First page extraction.",
      },
      additionalSourceFiles: [
        {
          sourceFileId: secondSource.id,
          sourceFileGuard: {
            status: SourceFileStatus.PROCESSING,
          },
          sourceFileUpdate: {
            status: SourceFileStatus.READY,
            extractedText: "Second page extraction.",
          },
        },
      ],
      collectionName: null,
      focusNote: null,
      tags: [],
      drafts: [generatedSkillDraft],
    });

    expect(created).toMatchObject({
      status: "not-found",
      reason: "source-not-found",
    });
    await expect(prisma.skill.count({ where: { userId } })).resolves.toBe(0);
    await expect(
      prisma.sourceFile.findUniqueOrThrow({
        where: {
          id: firstSource.id,
        },
        select: {
          status: true,
          extractedText: true,
        },
      }),
    ).resolves.toEqual({
      status: SourceFileStatus.PROCESSING,
      extractedText: null,
    });
  });

  it("cleans up uploaded source state when extraction validation fails", async () => {
    const userId = await createUser("upload_invalid_extraction");
    const { storage, deletedKeys } = createFakeUploadStorage({
      byteSize: 2048,
      mimeType: "application/pdf",
    });
    const prepared = await prepareSourceUpload({
      userId,
      now,
      storage,
      input: {
        originalName: "bad.pdf",
        mimeType: "application/pdf",
        byteSize: "2048",
      },
    });

    if (prepared.status !== "prepared") {
      throw new Error("Expected upload preparation to succeed.");
    }

    const { sender } = createFakeSourceUploadSender();
    const queued = await queueSourceUploadDrafts({
      userId,
      sourceFileId: prepared.sourceFileId,
      now,
      storage,
      eventSender: sender,
    });

    expect(queued.status).toBe("queued");

    const completed = await runQueuedSourceUploadDraftJob({
      userId,
      sourceFileId: prepared.sourceFileId,
      now,
      storage,
      extractSourceText: async () => ({
        extractedText: "   ",
      }),
      generateSkillDraft: successfulSkillDraftGenerator,
      model: "test-gemini",
    });

    expect(completed).toMatchObject({
      status: "not-created",
      reason: "invalid-extraction",
    });
    await expect(prisma.skill.count({ where: { userId } })).resolves.toBe(0);
    const failedSource = await prisma.sourceFile.findUniqueOrThrow({
      where: { id: prepared.sourceFileId },
    });
    expect(failedSource.status).toBe(SourceFileStatus.FAILED);
    expect(failedSource.storageKey).toBe(prepared.objectKey);
    expect(failedSource.metadata).toMatchObject({
      failureReason: "invalid-extraction",
    });
    const library = await getSkillsLibrary({ userId, now });
    expect(library.sourceProcessing).toHaveLength(1);
    expect(library.sourceProcessing[0]).toMatchObject({
      status: SourceFileStatus.FAILED,
      errorMessage: "The AI could not extract enough study text from this file.",
      canRequeue: true,
    });
    await expect(prisma.skillSourceRef.count({ where: { userId } })).resolves.toBe(0);
    expect(deletedKeys).toEqual([]);
  });

  it("stores a public error when uploaded source extraction is temporarily unavailable", async () => {
    const consoleError = vi.spyOn(console, "error").mockImplementation(() => {});
    const userId = await createUser("upload_extraction_unavailable");
    const { storage, deletedKeys } = createFakeUploadStorage({
      byteSize: 2048,
      mimeType: "image/png",
    });
    const prepared = await prepareSourceUpload({
      userId,
      now,
      storage,
      input: {
        originalName: "busy-model.png",
        mimeType: "image/png",
        byteSize: "2048",
      },
    });

    if (prepared.status !== "prepared") {
      throw new Error("Expected upload preparation to succeed.");
    }

    const { sender } = createFakeSourceUploadSender();
    const queued = await queueSourceUploadDrafts({
      userId,
      sourceFileId: prepared.sourceFileId,
      now,
      storage,
      eventSender: sender,
    });

    expect(queued.status).toBe("queued");

    try {
      const completed = await runQueuedSourceUploadDraftJob({
        userId,
        sourceFileId: prepared.sourceFileId,
        now,
        storage,
        extractSourceText: async () => {
          throw new Error(
            JSON.stringify({
              error: {
                code: 503,
                message: "This model is currently experiencing high demand.",
                status: "UNAVAILABLE",
              },
            }),
          );
        },
        generateSkillDraft: successfulSkillDraftGenerator,
        model: "test-gemini",
      });

      expect(completed).toMatchObject({
        status: "not-created",
        reason: "extraction-failed",
        message:
          "The AI service is busy right now, so LearnRecur could not finish creating this skill. Try again in a minute.",
      });
      if (completed.status !== "not-created") {
        throw new Error("expected extraction failure to return a public failure");
      }
      expect(completed.message).not.toContain("{");

      const failedSource = await prisma.sourceFile.findUniqueOrThrow({
        where: { id: prepared.sourceFileId },
      });
      expect(failedSource.storageKey).toBe(prepared.objectKey);
      expect(failedSource.metadata).toMatchObject({
        failureReason: "extraction-failed",
        errorMessage:
          "The AI service is busy right now, so LearnRecur could not finish creating this skill. Try again in a minute.",
      });

      const library = await getSkillsLibrary({ userId, now });
      expect(library.sourceProcessing[0]).toMatchObject({
        status: SourceFileStatus.FAILED,
        errorMessage:
          "The AI service is busy right now, so LearnRecur could not finish creating this skill. Try again in a minute.",
        canRequeue: true,
      });
      expect(library.sourceProcessing[0].errorMessage).not.toContain("{");
      expect(deletedKeys).toEqual([]);
      expect(consoleError).toHaveBeenCalledWith(
        "[ai] source extraction failed",
        expect.objectContaining({
          code: 503,
          status: "UNAVAILABLE",
        }),
      );
    } finally {
      consoleError.mockRestore();
    }
  });

  it("revalidates queued upload size before downloading source bytes", async () => {
    const userId = await createUser("upload_oversized_revalidated");
    let byteSize = 2048;
    let getObjectBytesCalls = 0;
    const { storage, deletedKeys } = createFakeUploadStorage({
      byteSize,
      bytes: Buffer.from("valid queued upload"),
    });
    const guardedStorage: SourceUploadStorage = {
      ...storage,
      async headObject(input) {
        const head = await storage.headObject(input);
        return {
          ...head,
          byteSize,
        };
      },
      async getObjectBytes(input) {
        getObjectBytesCalls += 1;
        return storage.getObjectBytes(input);
      },
    };
    const prepared = await prepareSourceUpload({
      userId,
      now,
      storage: guardedStorage,
      input: {
        originalName: "oversized-overwrite.png",
        mimeType: "image/png",
        byteSize: String(byteSize),
      },
    });

    if (prepared.status !== "prepared") {
      throw new Error("Expected upload preparation to succeed.");
    }

    const { sender } = createFakeSourceUploadSender();
    const queued = await queueSourceUploadDrafts({
      userId,
      sourceFileId: prepared.sourceFileId,
      now,
      storage: guardedStorage,
      eventSender: sender,
    });

    expect(queued.status).toBe("queued");

    byteSize = MAX_SOURCE_UPLOAD_BYTES + 1;

    const completed = await runQueuedSourceUploadDraftJob({
      userId,
      sourceFileId: prepared.sourceFileId,
      now,
      storage: guardedStorage,
      extractSourceText: successfulSourceExtractor,
      generateSkillDraft: successfulSkillDraftGenerator,
      model: "test-gemini",
    });

    expect(completed).toMatchObject({
      status: "not-created",
      reason: "invalid-upload",
      message: "Uploaded file is missing or larger than 10 MB.",
    });
    expect(getObjectBytesCalls).toBe(0);

    const failedSource = await prisma.sourceFile.findUniqueOrThrow({
      where: { id: prepared.sourceFileId },
    });
    expect(failedSource.status).toBe(SourceFileStatus.FAILED);
    expect(failedSource.storageKey).toBeNull();
    expect(failedSource.storageBucket).toBeNull();
    expect(failedSource.byteSize).toBeNull();
    expect(failedSource.metadata).toMatchObject({
      failureReason: "invalid-upload",
      errorMessage: "Uploaded file is missing or larger than 10 MB.",
    });
    expect(deletedKeys).toEqual([prepared.objectKey]);
  });

  it("rejects a batch when actual object bytes exceed the total source upload limit", async () => {
    const userId = await createUser("upload_actual_total_limit");
    const metadataByteSize = 4096;
    const actualBytes = Buffer.alloc(Math.floor(MAX_SOURCE_UPLOAD_BYTES / 2), 1);
    let actualReadCount = 0;
    const { storage, deletedKeys } = createFakeUploadStorage({
      byteSize: metadataByteSize,
      bytes: Buffer.from("valid queued upload"),
    });
    const guardedStorage: SourceUploadStorage = {
      ...storage,
      async getObjectBytes() {
        actualReadCount += 1;
        return actualBytes;
      },
    };
    const preparedSourceFileIds: string[] = [];
    const preparedObjectKeys: string[] = [];

    for (const originalName of [
      "worksheet-page-1.png",
      "worksheet-page-2.png",
      "worksheet-page-3.png",
      "worksheet-page-4.png",
      "worksheet-page-5.png",
    ]) {
      const prepared = await prepareSourceUpload({
        userId,
        now,
        storage: guardedStorage,
        input: {
          originalName,
          mimeType: "image/png",
          byteSize: String(metadataByteSize),
        },
      });

      if (prepared.status !== "prepared") {
        throw new Error("Expected upload preparation to succeed.");
      }

      preparedSourceFileIds.push(prepared.sourceFileId);
      preparedObjectKeys.push(prepared.objectKey);
    }

    const completed = await completeSourceUploadDrafts({
      userId,
      sourceFileId: preparedSourceFileIds[0],
      sourceFileIds: preparedSourceFileIds,
      now,
      storage: guardedStorage,
      extractSourceText: successfulSourceExtractor,
      generateSkillDraft: successfulSkillDraftGenerator,
      model: "test-gemini",
    });

    expect(completed).toMatchObject({
      status: "not-created",
      reason: "invalid-upload",
      message: "Upload files totaling 20 MB or less.",
    });
    expect(actualReadCount).toBe(5);

    const failedSources = await prisma.sourceFile.findMany({
      where: {
        id: {
          in: preparedSourceFileIds,
        },
        userId,
      },
      orderBy: {
        originalName: "asc",
      },
    });

    expect(failedSources).toHaveLength(5);
    expect(failedSources.every((sourceFile) => sourceFile.status === SourceFileStatus.FAILED)).toBe(
      true,
    );
    expect(failedSources.every((sourceFile) => sourceFile.storageKey === null)).toBe(true);
    expect(deletedKeys.toSorted()).toEqual(preparedObjectKeys.toSorted());
  });

  it("cleans up queued upload state when head revalidation cannot find the object", async () => {
    const userId = await createUser("upload_missing_head_object");
    const missingObjectError = Object.assign(new Error("object missing"), {
      name: "NoSuchKey",
      $metadata: {
        httpStatusCode: 404,
      },
    });
    let headError: Error | null = null;
    const { storage, deletedKeys } = createFakeUploadStorage({
      byteSize: 2048,
      bytes: Buffer.from("valid queued upload"),
    });
    const guardedStorage: SourceUploadStorage = {
      ...storage,
      async headObject(input) {
        if (headError) {
          throw headError;
        }

        return storage.headObject(input);
      },
    };
    const prepared = await prepareSourceUpload({
      userId,
      now,
      storage: guardedStorage,
      input: {
        originalName: "missing-after-queue.png",
        mimeType: "image/png",
        byteSize: "2048",
      },
    });

    if (prepared.status !== "prepared") {
      throw new Error("Expected upload preparation to succeed.");
    }

    const queued = await queueSourceUploadDrafts({
      userId,
      sourceFileId: prepared.sourceFileId,
      now,
      storage: guardedStorage,
      eventSender: createFakeSourceUploadSender().sender,
    });

    expect(queued.status).toBe("queued");

    headError = missingObjectError;

    await expect(
      runQueuedSourceUploadDraftJob({
        userId,
        sourceFileId: prepared.sourceFileId,
        now,
        storage: guardedStorage,
        extractSourceText: successfulSourceExtractor,
        generateSkillDraft: successfulSkillDraftGenerator,
        model: "test-gemini",
      }),
    ).resolves.toMatchObject({
      status: "not-created",
      reason: "invalid-upload",
      message: "Uploaded file is missing or larger than 10 MB.",
    });

    const failedSource = await prisma.sourceFile.findUniqueOrThrow({
      where: { id: prepared.sourceFileId },
    });
    expect(failedSource.status).toBe(SourceFileStatus.FAILED);
    expect(failedSource.storageKey).toBeNull();
    expect(failedSource.storageBucket).toBeNull();
    expect(failedSource.byteSize).toBeNull();
    expect(deletedKeys).toEqual([prepared.objectKey]);
  });

  it("stores the revalidated upload size when later processing fails", async () => {
    const consoleError = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      const userId = await createUser("upload_revalidated_size_failure");
      const queuedByteSize = 2048;
      const actualByteSize = 4096;
      let byteSize = queuedByteSize;
      const { storage, deletedKeys } = createFakeUploadStorage({
        byteSize: queuedByteSize,
        bytes: Buffer.from("valid queued upload"),
      });
      const guardedStorage: SourceUploadStorage = {
        ...storage,
        async headObject(input) {
          const head = await storage.headObject(input);
          return {
            ...head,
            byteSize,
          };
        },
      };
      const prepared = await prepareSourceUpload({
        userId,
        now,
        storage: guardedStorage,
        input: {
          originalName: "changed-size-before-failure.png",
          mimeType: "image/png",
          byteSize: String(queuedByteSize),
        },
      });

      if (prepared.status !== "prepared") {
        throw new Error("Expected upload preparation to succeed.");
      }

      await queueSourceUploadDrafts({
        userId,
        sourceFileId: prepared.sourceFileId,
        now,
        storage: guardedStorage,
        eventSender: createFakeSourceUploadSender().sender,
      });

      byteSize = actualByteSize;

      await expect(
        runQueuedSourceUploadDraftJob({
          userId,
          sourceFileId: prepared.sourceFileId,
          now,
          storage: guardedStorage,
          extractSourceText: async () => {
            throw new Error("temporary extractor failure");
          },
          generateSkillDraft: successfulSkillDraftGenerator,
          model: "test-gemini",
        }),
      ).resolves.toMatchObject({
        status: "not-created",
        reason: "extraction-failed",
      });

      const failedSource = await prisma.sourceFile.findUniqueOrThrow({
        where: { id: prepared.sourceFileId },
      });
      expect(failedSource.status).toBe(SourceFileStatus.FAILED);
      expect(failedSource.storageKey).toBe(prepared.objectKey);
      expect(failedSource.storageBucket).toBe("learnrecur-dev");
      expect(failedSource.byteSize).toBe(actualByteSize);
      expect(deletedKeys).toEqual([]);
    } finally {
      consoleError.mockRestore();
    }
  });

  it("does not overwrite an upload that changes while processing failure is recorded", async () => {
    const consoleError = vi.spyOn(console, "error").mockImplementation(() => {});
    const userId = await createUser("upload_failure_state_changed");
    const { storage, deletedKeys } = createFakeUploadStorage({
      byteSize: 2048,
      mimeType: "image/png",
    });
    const prepared = await prepareSourceUpload({
      userId,
      now,
      storage,
      input: {
        originalName: "changed-while-processing.png",
        mimeType: "image/png",
        byteSize: "2048",
      },
    });

    if (prepared.status !== "prepared") {
      throw new Error("Expected upload preparation to succeed.");
    }

    const queued = await queueSourceUploadDrafts({
      userId,
      sourceFileId: prepared.sourceFileId,
      now,
      storage,
      eventSender: createFakeSourceUploadSender().sender,
    });

    expect(queued.status).toBe("queued");

    try {
      await expect(
        runQueuedSourceUploadDraftJob({
          userId,
          sourceFileId: prepared.sourceFileId,
          now,
          storage,
          extractSourceText: async () => {
            await prisma.sourceFile.updateMany({
              where: {
                id: prepared.sourceFileId,
                userId,
                status: SourceFileStatus.PROCESSING,
              },
              data: {
                status: SourceFileStatus.UPLOADED,
                metadata: {
                  changedByConcurrentRetry: true,
                },
              },
            });
            throw new Error("temporary extractor failure");
          },
          generateSkillDraft: successfulSkillDraftGenerator,
          model: "test-gemini",
        }),
      ).resolves.toMatchObject({
        status: "not-created",
        reason: "extraction-failed",
      });

      const changedSource = await prisma.sourceFile.findUniqueOrThrow({
        where: {
          id: prepared.sourceFileId,
        },
      });
      expect(changedSource.status).toBe(SourceFileStatus.UPLOADED);
      expect(changedSource.metadata).toMatchObject({
        changedByConcurrentRetry: true,
      });
      expect(changedSource.metadata).not.toMatchObject({
        failureReason: "extraction-failed",
      });
      expect(deletedKeys).toEqual([]);
    } finally {
      consoleError.mockRestore();
    }
  });

  it("cleans up uploaded source state when downloaded bytes exceed the upload limit", async () => {
    const userId = await createUser("upload_oversized_download");
    const { storage, deletedKeys, getObjectByteInputs } = createFakeUploadStorage({
      byteSize: 2048,
      bytes: Buffer.alloc(MAX_SOURCE_UPLOAD_BYTES + 1, "a"),
    });
    const prepared = await prepareSourceUpload({
      userId,
      now,
      storage,
      input: {
        originalName: "oversized-after-head.png",
        mimeType: "image/png",
        byteSize: "2048",
      },
    });

    if (prepared.status !== "prepared") {
      throw new Error("Expected upload preparation to succeed.");
    }

    const { sender } = createFakeSourceUploadSender();
    const queued = await queueSourceUploadDrafts({
      userId,
      sourceFileId: prepared.sourceFileId,
      now,
      storage,
      eventSender: sender,
    });

    expect(queued.status).toBe("queued");

    const completed = await runQueuedSourceUploadDraftJob({
      userId,
      sourceFileId: prepared.sourceFileId,
      now,
      storage,
      extractSourceText: successfulSourceExtractor,
      generateSkillDraft: successfulSkillDraftGenerator,
      model: "test-gemini",
    });

    expect(completed).toMatchObject({
      status: "not-created",
      reason: "invalid-upload",
    });
    await expect(prisma.skill.count({ where: { userId } })).resolves.toBe(0);
    const failedSource = await prisma.sourceFile.findUniqueOrThrow({
      where: { id: prepared.sourceFileId },
    });
    expect(failedSource.status).toBe(SourceFileStatus.FAILED);
    expect(failedSource.storageKey).toBeNull();
    expect(failedSource.storageBucket).toBeNull();
    expect(failedSource.byteSize).toBeNull();
    expect(failedSource.metadata).toMatchObject({
      failureReason: "invalid-upload",
      errorMessage: "Uploaded file is missing or larger than 10 MB.",
    });
    const library = await getSkillsLibrary({ userId, now });
    expect(library.sourceProcessing).toHaveLength(1);
    const sourceProcessing = library.sourceProcessing[0];
    expect(sourceProcessing.id).toBe(prepared.sourceFileId);
    expect(sourceProcessing).toMatchObject({
      status: SourceFileStatus.FAILED,
      errorMessage: "Uploaded file is missing or larger than 10 MB.",
      canRequeue: false,
      canDismiss: true,
    });
    expect(getObjectByteInputs).toEqual([
      expect.objectContaining({
        maxBytes: MAX_SOURCE_UPLOAD_BYTES,
      }),
    ]);
    expect(deletedKeys).toEqual([prepared.objectKey]);
  });

  it("preserves uploaded source storage metadata when invalid upload cleanup fails", async () => {
    const userId = await createUser("upload_oversized_delete_failure");
    const storageSetup = createFakeUploadStorage({
      byteSize: 2048,
      bytes: Buffer.alloc(MAX_SOURCE_UPLOAD_BYTES + 1, "a"),
      onDeleteObject: () => {
        throw new Error("temporary S3 delete failure");
      },
    });
    const prepared = await prepareSourceUpload({
      userId,
      now,
      storage: storageSetup.storage,
      input: {
        originalName: "oversized-delete-failure.png",
        mimeType: "image/png",
        byteSize: "2048",
      },
    });

    if (prepared.status !== "prepared") {
      throw new Error("Expected upload preparation to succeed.");
    }

    await queueSourceUploadDrafts({
      userId,
      sourceFileId: prepared.sourceFileId,
      now,
      storage: storageSetup.storage,
      eventSender: createFakeSourceUploadSender().sender,
    });

    await expect(
      runQueuedSourceUploadDraftJob({
        userId,
        sourceFileId: prepared.sourceFileId,
        now,
        storage: storageSetup.storage,
        extractSourceText: successfulSourceExtractor,
        generateSkillDraft: successfulSkillDraftGenerator,
        model: "test-gemini",
      }),
    ).resolves.toMatchObject({
      status: "not-created",
      reason: "invalid-upload",
      message: "Uploaded file is missing or larger than 10 MB.",
    });

    const failedSource = await prisma.sourceFile.findUniqueOrThrow({
      where: { id: prepared.sourceFileId },
    });
    expect(failedSource.status).toBe(SourceFileStatus.FAILED);
    expect(failedSource.storageKey).toBe(prepared.objectKey);
    expect(failedSource.storageBucket).toBe("learnrecur-dev");
    expect(failedSource.byteSize).toBe(2048);
    expect(storageSetup.deletedKeys).toEqual([]);

    await expect(getSkillsLibrary({ userId, now })).resolves.toMatchObject({
      sourceProcessing: [
        expect.objectContaining({
          id: prepared.sourceFileId,
          canDismiss: true,
        }),
      ],
    });
  });

  it("keeps retryable upload state only for transient source read failures", async () => {
    const userId = await createUser("upload_read_failure_split");
    const missingObjectError = Object.assign(new Error("object missing"), {
      name: "NoSuchKey",
      $metadata: {
        httpStatusCode: 404,
      },
    });
    const missingStorageSetup = createFakeUploadStorage({
      byteSize: 2048,
      getError: missingObjectError,
    });
    const missingPrepared = await prepareSourceUpload({
      userId,
      now,
      storage: missingStorageSetup.storage,
      input: {
        originalName: "missing-object.png",
        mimeType: "image/png",
        byteSize: "2048",
      },
    });

    if (missingPrepared.status !== "prepared") {
      throw new Error("Expected upload preparation to succeed.");
    }

    await queueSourceUploadDrafts({
      userId,
      sourceFileId: missingPrepared.sourceFileId,
      now,
      storage: missingStorageSetup.storage,
      eventSender: createFakeSourceUploadSender().sender,
    });
    await expect(
      runQueuedSourceUploadDraftJob({
        userId,
        sourceFileId: missingPrepared.sourceFileId,
        now,
        storage: missingStorageSetup.storage,
        extractSourceText: successfulSourceExtractor,
        generateSkillDraft: successfulSkillDraftGenerator,
        model: "test-gemini",
      }),
    ).resolves.toMatchObject({
      status: "not-created",
      reason: "invalid-upload",
      message: "Uploaded file is missing or larger than 10 MB.",
    });

    const missingSource = await prisma.sourceFile.findUniqueOrThrow({
      where: { id: missingPrepared.sourceFileId },
    });
    expect(missingSource.status).toBe(SourceFileStatus.FAILED);
    expect(missingSource.storageKey).toBeNull();
    expect(missingSource.byteSize).toBeNull();
    expect(missingStorageSetup.deletedKeys).toEqual([missingPrepared.objectKey]);

    const transientStorageSetup = createFakeUploadStorage({
      byteSize: 2048,
      getError: new Error("temporary S3 timeout"),
    });
    const transientPrepared = await prepareSourceUpload({
      userId,
      now,
      storage: transientStorageSetup.storage,
      input: {
        originalName: "transient-read-failure.png",
        mimeType: "image/png",
        byteSize: "2048",
      },
    });

    if (transientPrepared.status !== "prepared") {
      throw new Error("Expected upload preparation to succeed.");
    }

    await queueSourceUploadDrafts({
      userId,
      sourceFileId: transientPrepared.sourceFileId,
      now,
      storage: transientStorageSetup.storage,
      eventSender: createFakeSourceUploadSender().sender,
    });
    await expect(
      runQueuedSourceUploadDraftJob({
        userId,
        sourceFileId: transientPrepared.sourceFileId,
        now,
        storage: transientStorageSetup.storage,
        extractSourceText: successfulSourceExtractor,
        generateSkillDraft: successfulSkillDraftGenerator,
        model: "test-gemini",
      }),
    ).resolves.toMatchObject({
      status: "not-created",
      reason: "invalid-upload",
      message: "Could not read S3 upload: temporary S3 timeout",
    });

    const transientSource = await prisma.sourceFile.findUniqueOrThrow({
      where: { id: transientPrepared.sourceFileId },
    });
    expect(transientSource.status).toBe(SourceFileStatus.FAILED);
    expect(transientSource.storageKey).toBe(transientPrepared.objectKey);
    expect(transientSource.byteSize).toBe(2048);
    expect(transientStorageSetup.deletedKeys).toEqual([]);

    const library = await getSkillsLibrary({ userId, now });
    expect(library.sourceProcessing).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          id: missingPrepared.sourceFileId,
          canRequeue: false,
        }),
        expect.objectContaining({
          id: transientPrepared.sourceFileId,
          canRequeue: true,
        }),
      ]),
    );
  });

  it("does not process the same uploaded source twice", async () => {
    const userId = await createUser("upload_duplicate_completion");
    const { storage } = createFakeUploadStorage({
      byteSize: 4096,
    });
    const prepared = await prepareSourceUpload({
      userId,
      now,
      storage,
      input: {
        originalName: "worksheet.png",
        mimeType: "image/png",
        byteSize: "4096",
      },
    });

    if (prepared.status !== "prepared") {
      throw new Error("Expected upload preparation to succeed.");
    }

    const { sender } = createFakeSourceUploadSender();
    const queued = await queueSourceUploadDrafts({
      userId,
      sourceFileId: prepared.sourceFileId,
      now,
      storage,
      eventSender: sender,
    });

    expect(queued.status).toBe("queued");

    const firstCompletion = await runQueuedSourceUploadDraftJob({
      userId,
      sourceFileId: prepared.sourceFileId,
      now,
      storage,
      extractSourceText: successfulSourceExtractor,
      generateSkillDraft: successfulSkillDraftGenerator,
      model: "test-gemini",
    });

    expect(firstCompletion.status).toBe("created");

    const secondCompletion = await runQueuedSourceUploadDraftJob({
      userId,
      sourceFileId: prepared.sourceFileId,
      now,
      storage,
      extractSourceText: successfulSourceExtractor,
      generateSkillDraft: successfulSkillDraftGenerator,
      model: "test-gemini",
    });

    expect(secondCompletion).toMatchObject({
      status: "not-created",
      reason: "invalid-upload",
    });
    await expect(prisma.skill.count({ where: { userId } })).resolves.toBe(1);
    await expect(prisma.skillSourceRef.count({ where: { userId } })).resolves.toBe(1);
  });

  it("does not create skills after a stale processing upload is dismissed", async () => {
    const userId = await createUser("upload_dismissed_during_completion");
    const { storage, deletedKeys } = createFakeUploadStorage({
      byteSize: 4096,
    });
    const prepared = await prepareSourceUpload({
      userId,
      now,
      storage,
      input: {
        originalName: "dismissed-during-completion.png",
        mimeType: "image/png",
        byteSize: "4096",
        collectionName: "Canceled upload collection",
      },
    });

    if (prepared.status !== "prepared") {
      throw new Error("Expected upload preparation to succeed.");
    }

    await queueSourceUploadDrafts({
      userId,
      sourceFileId: prepared.sourceFileId,
      now,
      storage,
      eventSender: createFakeSourceUploadSender().sender,
    });
    const queuedSource = await prisma.sourceFile.findUniqueOrThrow({
      where: { id: prepared.sourceFileId },
      select: { metadata: true },
    });

    if (
      !queuedSource.metadata ||
      typeof queuedSource.metadata !== "object" ||
      Array.isArray(queuedSource.metadata)
    ) {
      throw new Error("Expected queued upload metadata to be an object.");
    }

    await prisma.sourceFile.update({
      where: { id: prepared.sourceFileId },
      data: {
        metadata: {
          ...queuedSource.metadata,
          retryCount: MAX_SOURCE_UPLOAD_REQUEUE_ATTEMPTS,
        },
      },
    });

    const dismissNow = new Date(now.getTime() + SOURCE_PROCESSING_STALE_AFTER_MS);
    const completed = await runQueuedSourceUploadDraftJob({
      userId,
      sourceFileId: prepared.sourceFileId,
      now,
      storage,
      extractSourceText: async (input) => {
        await expect(
          dismissFailedSourceUpload({
            userId,
            sourceFileId: prepared.sourceFileId,
            now: dismissNow,
            storage,
          }),
        ).resolves.toMatchObject({
          status: "dismissed",
          sourceFileId: prepared.sourceFileId,
        });

        return successfulSourceExtractor(input);
      },
      generateSkillDraft: successfulSkillDraftGenerator,
      model: "test-gemini",
    });

    expect(completed).toMatchObject({
      status: "not-found",
      reason: "source-not-found",
    });
    await expect(
      prisma.collection.count({
        where: {
          userId,
          name: "Canceled upload collection",
        },
      }),
    ).resolves.toBe(0);
    await expect(prisma.skill.count({ where: { userId } })).resolves.toBe(0);
    await expect(prisma.skillSourceRef.count({ where: { userId } })).resolves.toBe(0);
    await expect(getSkillsLibrary({ userId, now })).resolves.toMatchObject({
      sourceProcessing: [],
    });
    await expect(getSkillCreationSourceRecoveryItems({ userId, now })).resolves.toEqual([]);

    const dismissedSource = await prisma.sourceFile.findUniqueOrThrow({
      where: { id: prepared.sourceFileId },
    });
    expect(dismissedSource).toMatchObject({
      status: SourceFileStatus.FAILED,
      byteSize: 4096,
      storageBucket: null,
      storageKey: null,
      metadata: expect.objectContaining({
        dismissedAt: dismissNow.toISOString(),
      }),
    });
    expect(deletedKeys).toEqual([prepared.objectKey]);
  });

  it("rejects cross-user uploaded source completion without cleanup", async () => {
    const userId = await createUser("upload_cross_owner");
    const otherUserId = await createUser("upload_cross_other");
    const { storage, deletedKeys } = createFakeUploadStorage();
    const prepared = await prepareSourceUpload({
      userId,
      now,
      storage,
      input: {
        originalName: "worksheet.png",
        mimeType: "image/png",
        byteSize: "4096",
      },
    });

    if (prepared.status !== "prepared") {
      throw new Error("Expected upload preparation to succeed.");
    }

    await expect(
      queueSourceUploadDrafts({
        userId: otherUserId,
        sourceFileId: prepared.sourceFileId,
        now,
        storage,
        eventSender: createFakeSourceUploadSender().sender,
      }),
    ).resolves.toMatchObject({
      status: "not-found",
      reason: "source-not-found",
    });
    await expect(prisma.sourceFile.count({ where: { id: prepared.sourceFileId } })).resolves.toBe(1);
    expect(deletedKeys).toEqual([]);
  });

  it("cleans up uploaded source state when queueing fails before the job starts", async () => {
    const userId = await createUser("upload_queue_failure");
    const { storage, deletedKeys } = createFakeUploadStorage({
      byteSize: 4096,
    });
    const prepared = await prepareSourceUpload({
      userId,
      now,
      storage,
      input: {
        originalName: "queue-failure.png",
        mimeType: "image/png",
        byteSize: "4096",
      },
    });

    if (prepared.status !== "prepared") {
      throw new Error("Expected upload preparation to succeed.");
    }

    const queued = await queueSourceUploadDrafts({
      userId,
      sourceFileId: prepared.sourceFileId,
      now,
      storage,
      eventSender: {
        async sendSourceUploadDraftRequested() {
          throw new Error("event transport down");
        },
      },
    });

    expect(queued).toMatchObject({
      status: "not-queued",
      reason: "event-send-failed",
    });
    await expect(prisma.sourceFile.count({ where: { id: prepared.sourceFileId } })).resolves.toBe(
      0,
    );
    expect(deletedKeys).toEqual([prepared.objectKey]);
  });

  it("resends an uploaded source while counting retry attempts", async () => {
    const userId = await createUser("upload_requeue_uploaded");
    const { storage } = createFakeUploadStorage({
      byteSize: 4096,
    });
    const prepared = await prepareSourceUpload({
      userId,
      now,
      storage,
      input: {
        originalName: "queued-again.png",
        mimeType: "image/png",
        byteSize: "4096",
      },
    });

    if (prepared.status !== "prepared") {
      throw new Error("Expected upload preparation to succeed.");
    }

    const firstSender = createFakeSourceUploadSender();
    await expect(
      queueSourceUploadDrafts({
        userId,
        sourceFileId: prepared.sourceFileId,
        now,
        storage,
        eventSender: firstSender.sender,
      }),
    ).resolves.toMatchObject({ status: "queued" });

    const { sender, events } = createFakeSourceUploadSender();
    const tooSoonAt = new Date(
      now.getTime() + SOURCE_PROCESSING_STALE_AFTER_MS - 1,
    );
    await expect(
      requeueSourceUploadDraft({
        userId,
        sourceFileId: prepared.sourceFileId,
        now: tooSoonAt,
        storage,
        eventSender: sender,
      }),
    ).resolves.toMatchObject({
      status: "not-queued",
      reason: "not-stale",
    });

    const requeueAt = new Date(now.getTime() + SOURCE_PROCESSING_STALE_AFTER_MS);
    await expect(
      requeueSourceUploadDraft({
        userId,
        sourceFileId: prepared.sourceFileId,
        now: requeueAt,
        storage,
        eventSender: sender,
      }),
    ).resolves.toMatchObject({
      status: "queued",
      sourceFileId: prepared.sourceFileId,
    });
    const secondRequeueAt = new Date(
      requeueAt.getTime() + SOURCE_PROCESSING_STALE_AFTER_MS,
    );
    await expect(
      requeueSourceUploadDraft({
        userId,
        sourceFileId: prepared.sourceFileId,
        now: secondRequeueAt,
        storage,
        eventSender: sender,
      }),
    ).resolves.toMatchObject({
      status: "queued",
      sourceFileId: prepared.sourceFileId,
    });
    expect(events).toEqual([
      {
        userId,
        sourceFileId: prepared.sourceFileId,
        requestedAt: requeueAt.toISOString(),
      },
      {
        userId,
        sourceFileId: prepared.sourceFileId,
        requestedAt: secondRequeueAt.toISOString(),
      },
    ]);
    await expect(prisma.skill.count({ where: { userId } })).resolves.toBe(0);

    const sourceFile = await prisma.sourceFile.findUniqueOrThrow({
      where: { id: prepared.sourceFileId },
    });
    expect(sourceFile.status).toBe(SourceFileStatus.UPLOADED);
    expect(sourceFile.metadata).toMatchObject({
      queuedAt: secondRequeueAt.toISOString(),
      requeuedAt: secondRequeueAt.toISOString(),
      retryCount: 2,
    });
  });

  it("rejects requeue for uploaded and stale processing sources at the retry limit", async () => {
    const userId = await createUser("upload_requeue_retry_limit");
    const { storage } = createFakeUploadStorage({
      byteSize: 4096,
    });

    const uploaded = await prisma.sourceFile.create({
      data: {
        userId,
        kind: SourceFileKind.IMAGE,
        status: SourceFileStatus.UPLOADED,
        originalName: "waiting upload",
        mimeType: "image/png",
        byteSize: 4096,
        storageBucket: "learnrecur-dev",
        storageKey: `source-uploads/${userId}/waiting.png`,
        metadata: {
          queuedAt: new Date(
            now.getTime() - SOURCE_PROCESSING_STALE_AFTER_MS,
          ).toISOString(),
          retryCount: MAX_SOURCE_UPLOAD_REQUEUE_ATTEMPTS,
        },
      },
    });
    const processingStartedAt = new Date(
      now.getTime() - SOURCE_PROCESSING_STALE_AFTER_MS,
    ).toISOString();
    const staleProcessing = await prisma.sourceFile.create({
      data: {
        userId,
        kind: SourceFileKind.IMAGE,
        status: SourceFileStatus.PROCESSING,
        originalName: "stale upload",
        mimeType: "image/png",
        byteSize: 4096,
        storageBucket: "learnrecur-dev",
        storageKey: `source-uploads/${userId}/stale.png`,
        metadata: {
          processingStartedAt,
          retryCount: MAX_SOURCE_UPLOAD_REQUEUE_ATTEMPTS,
        },
      },
    });
    const failed = await prisma.sourceFile.create({
      data: {
        userId,
        kind: SourceFileKind.IMAGE,
        status: SourceFileStatus.FAILED,
        originalName: "failed upload",
        mimeType: "image/png",
        byteSize: 4096,
        storageBucket: "learnrecur-dev",
        storageKey: `source-uploads/${userId}/failed.png`,
        metadata: {
          retryCount: MAX_SOURCE_UPLOAD_REQUEUE_ATTEMPTS,
        },
      },
    });

    const sender = createFakeSourceUploadSender();

    await expect(
      requeueSourceUploadDraft({
        userId,
        sourceFileId: uploaded.id,
        now,
        storage,
        eventSender: sender.sender,
      }),
    ).resolves.toMatchObject({
      status: "not-queued",
      reason: "not-requeueable",
    });
    await expect(
      requeueSourceUploadDraft({
        userId,
        sourceFileId: staleProcessing.id,
        now,
        storage,
        eventSender: sender.sender,
      }),
    ).resolves.toMatchObject({
      status: "not-queued",
      reason: "not-requeueable",
    });
    await expect(
      requeueSourceUploadDraft({
        userId,
        sourceFileId: failed.id,
        now,
        storage,
        eventSender: sender.sender,
      }),
    ).resolves.toMatchObject({
      status: "not-queued",
      reason: "not-requeueable",
    });
    expect(sender.events).toHaveLength(0);

    const library = await getSkillsLibrary({ userId, now });
    expect(library.sourceProcessing).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          id: uploaded.id,
          canDismiss: true,
          canRequeue: false,
        }),
        expect.objectContaining({
          id: staleProcessing.id,
          canDismiss: true,
          isStaleProcessing: true,
          canRequeue: false,
        }),
        expect.objectContaining({
          id: failed.id,
          canDismiss: true,
          canRequeue: false,
        }),
      ]),
    );
    await expect(getSkillCreationSourceRecoveryItems({ userId, now })).resolves.toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          id: uploaded.id,
          canDismiss: true,
          canRequeue: false,
        }),
        expect.objectContaining({
          id: staleProcessing.id,
          canDismiss: true,
          canRequeue: false,
        }),
        expect.objectContaining({
          id: failed.id,
          canDismiss: true,
          canRequeue: false,
        }),
      ]),
    );
  });

  it("rolls back requeue state when event sending fails", async () => {
    const userId = await createUser("upload_requeue_send_failure");
    const { storage, deletedKeys } = createFakeUploadStorage({
      byteSize: 4096,
    });
    const prepared = await prepareSourceUpload({
      userId,
      now,
      storage,
      input: {
        originalName: "requeue-send-failure.png",
        mimeType: "image/png",
        byteSize: "4096",
      },
    });

    if (prepared.status !== "prepared") {
      throw new Error("Expected upload preparation to succeed.");
    }

    await queueSourceUploadDrafts({
      userId,
      sourceFileId: prepared.sourceFileId,
      now,
      storage,
      eventSender: createFakeSourceUploadSender().sender,
    });
    const originalMetadata = {
      processingStartedAt: new Date(
        now.getTime() - SOURCE_PROCESSING_STALE_AFTER_MS,
      ).toISOString(),
      retryCount: 2,
    };
    await prisma.sourceFile.update({
      where: { id: prepared.sourceFileId },
      data: {
        status: SourceFileStatus.PROCESSING,
        byteSize: 2048,
        metadata: originalMetadata,
      },
    });

    const requeued = await requeueSourceUploadDraft({
      userId,
      sourceFileId: prepared.sourceFileId,
      now,
      storage,
      eventSender: {
        async sendSourceUploadDraftRequested() {
          throw new Error("event transport down");
        },
      },
    });

    expect(requeued).toMatchObject({
      status: "not-queued",
      reason: "event-send-failed",
    });
    const sourceFile = await prisma.sourceFile.findUniqueOrThrow({
      where: { id: prepared.sourceFileId },
    });
    expect(sourceFile.status).toBe(SourceFileStatus.PROCESSING);
    expect(sourceFile.byteSize).toBe(2048);
    expect(sourceFile.metadata).toEqual(originalMetadata);
    expect(sourceFile.storageKey).toBe(prepared.objectKey);
    expect(deletedKeys).toEqual([]);
  });

  it("does not roll back a later requeue when event sending fails", async () => {
    const userId = await createUser("upload_requeue_send_failure_race");
    const { storage } = createFakeUploadStorage({
      byteSize: 4096,
    });
    const prepared = await prepareSourceUpload({
      userId,
      now,
      storage,
      input: {
        originalName: "requeue-send-failure-race.png",
        mimeType: "image/png",
        byteSize: "4096",
      },
    });

    if (prepared.status !== "prepared") {
      throw new Error("Expected upload preparation to succeed.");
    }

    await queueSourceUploadDrafts({
      userId,
      sourceFileId: prepared.sourceFileId,
      now,
      storage,
      eventSender: createFakeSourceUploadSender().sender,
    });
    const originalMetadata = {
      processingStartedAt: new Date(
        now.getTime() - SOURCE_PROCESSING_STALE_AFTER_MS,
      ).toISOString(),
      retryCount: 2,
    };
    await prisma.sourceFile.update({
      where: { id: prepared.sourceFileId },
      data: {
        status: SourceFileStatus.PROCESSING,
        byteSize: 2048,
        metadata: originalMetadata,
      },
    });

    const requeued = await requeueSourceUploadDraft({
      userId,
      sourceFileId: prepared.sourceFileId,
      now,
      storage,
      eventSender: {
        async sendSourceUploadDraftRequested() {
          await prisma.sourceFile.update({
            where: { id: prepared.sourceFileId },
            data: {
              status: SourceFileStatus.UPLOADED,
              byteSize: 4096,
              metadata: {
                laterRequeueAt: now.toISOString(),
              },
            },
          });
          throw new Error("event transport down");
        },
      },
    });

    expect(requeued).toMatchObject({
      status: "not-queued",
      reason: "event-send-failed",
    });
    const sourceFile = await prisma.sourceFile.findUniqueOrThrow({
      where: { id: prepared.sourceFileId },
    });
    expect(sourceFile.status).toBe(SourceFileStatus.UPLOADED);
    expect(sourceFile.byteSize).toBe(4096);
    expect(sourceFile.metadata).toEqual({
      laterRequeueAt: now.toISOString(),
    });
    expect(sourceFile.storageKey).toBe(prepared.objectKey);
  });

  it("requeues stale processing sources and rejects fresh processing sources", async () => {
    const userId = await createUser("upload_requeue_processing");
    const { storage } = createFakeUploadStorage({
      byteSize: 4096,
    });
    const prepared = await prepareSourceUpload({
      userId,
      now,
      storage,
      input: {
        originalName: "stale-processing.png",
        mimeType: "image/png",
        byteSize: "4096",
      },
    });

    if (prepared.status !== "prepared") {
      throw new Error("Expected upload preparation to succeed.");
    }

    await queueSourceUploadDrafts({
      userId,
      sourceFileId: prepared.sourceFileId,
      now,
      storage,
      eventSender: createFakeSourceUploadSender().sender,
    });

    const staleStartedAt = new Date(
      now.getTime() - SOURCE_PROCESSING_STALE_AFTER_MS,
    ).toISOString();
    await prisma.sourceFile.update({
      where: { id: prepared.sourceFileId },
      data: {
        status: SourceFileStatus.PROCESSING,
        metadata: {
          processingStartedAt: staleStartedAt,
        },
      },
    });

    const { sender, events } = createFakeSourceUploadSender();
    const requeued = await requeueSourceUploadDraft({
      userId,
      sourceFileId: prepared.sourceFileId,
      now,
      storage,
      eventSender: sender,
    });

    expect(requeued).toMatchObject({
      status: "queued",
      sourceFileId: prepared.sourceFileId,
    });
    expect(events).toHaveLength(1);
    const sourceFile = await prisma.sourceFile.findUniqueOrThrow({
      where: { id: prepared.sourceFileId },
    });
    expect(sourceFile.status).toBe(SourceFileStatus.UPLOADED);
    expect(sourceFile.metadata).toMatchObject({
      processingStartedAt: staleStartedAt,
      requeuedAt: now.toISOString(),
      retryCount: 1,
    });

    const freshPrepared = await prepareSourceUpload({
      userId,
      now,
      storage,
      input: {
        originalName: "fresh-processing.png",
        mimeType: "image/png",
        byteSize: "4096",
      },
    });

    if (freshPrepared.status !== "prepared") {
      throw new Error("Expected upload preparation to succeed.");
    }

    await queueSourceUploadDrafts({
      userId,
      sourceFileId: freshPrepared.sourceFileId,
      now,
      storage,
      eventSender: createFakeSourceUploadSender().sender,
    });
    await prisma.sourceFile.update({
      where: { id: freshPrepared.sourceFileId },
      data: {
        status: SourceFileStatus.PROCESSING,
        metadata: {
          processingStartedAt: now.toISOString(),
        },
      },
    });

    const freshSender = createFakeSourceUploadSender();
    await expect(
      requeueSourceUploadDraft({
        userId,
        sourceFileId: freshPrepared.sourceFileId,
        now,
        storage,
        eventSender: freshSender.sender,
      }),
    ).resolves.toMatchObject({
      status: "not-queued",
      reason: "not-stale",
    });
    expect(freshSender.events).toHaveLength(0);
  });

  it("rejects requeue for non-recoverable, missing, and cross-user sources", async () => {
    const userId = await createUser("upload_requeue_reject");
    const otherUserId = await createUser("upload_requeue_reject_other");
    const { storage } = createFakeUploadStorage();
    const readySource = await prisma.sourceFile.create({
      data: {
        userId,
        kind: SourceFileKind.IMAGE,
        status: SourceFileStatus.READY,
        originalName: "ready upload",
        mimeType: "image/png",
        byteSize: 1024,
        storageBucket: "learnrecur-dev",
        storageKey: `source-uploads/${userId}/ready.png`,
      },
    });
    const failedSource = await prisma.sourceFile.create({
      data: {
        userId,
        kind: SourceFileKind.IMAGE,
        status: SourceFileStatus.FAILED,
        originalName: "failed upload",
        mimeType: "image/png",
      },
    });
    const savedFailedSource = await prisma.sourceFile.create({
      data: {
        userId,
        kind: SourceFileKind.IMAGE,
        status: SourceFileStatus.FAILED,
        originalName: "saved failed upload",
        mimeType: "image/png",
        byteSize: 1024,
        storageBucket: "learnrecur-dev",
        storageKey: `source-uploads/${userId}/failed.png`,
      },
    });
    const textFailedSource = await prisma.sourceFile.create({
      data: {
        userId,
        kind: SourceFileKind.TEXT,
        status: SourceFileStatus.FAILED,
        originalName: "failed pasted source",
        mimeType: "text/plain",
        byteSize: 1024,
        extractedText:
          "Use ser for identity and long-term traits. Use estar for location and temporary states.",
      },
    });
    const linkedSkill = await createSkillDraft({
      userId,
      input: {
        title: "Linked failed upload",
        objective: "Review an upload already attached to another skill.",
      },
    });

    if (linkedSkill.status !== "created") {
      throw new Error("Expected linked draft creation to succeed.");
    }

    const linkedFailedSource = await prisma.sourceFile.create({
      data: {
        userId,
        kind: SourceFileKind.IMAGE,
        status: SourceFileStatus.FAILED,
        originalName: "linked failed upload",
        mimeType: "image/png",
        byteSize: 1024,
        storageBucket: "learnrecur-dev",
        storageKey: `source-uploads/${userId}/linked-failed.png`,
      },
    });
    await prisma.skillSourceRef.create({
      data: {
        userId,
        skillId: linkedSkill.skill.id,
        sourceFileId: linkedFailedSource.id,
      },
    });

    await expect(
      requeueSourceUploadDraft({
        userId,
        sourceFileId: readySource.id,
        now,
        storage,
        eventSender: createFakeSourceUploadSender().sender,
      }),
    ).resolves.toMatchObject({
      status: "not-queued",
      reason: "not-requeueable",
    });
    await expect(
      requeueSourceUploadDraft({
        userId,
        sourceFileId: failedSource.id,
        now,
        storage,
        eventSender: createFakeSourceUploadSender().sender,
      }),
    ).resolves.toMatchObject({
      status: "not-queued",
      reason: "not-requeueable",
    });
    await expect(
      requeueSourceUploadDraft({
        userId,
        sourceFileId: textFailedSource.id,
        now,
        storage,
        eventSender: createFakeSourceUploadSender().sender,
      }),
    ).resolves.toMatchObject({
      status: "not-queued",
      reason: "not-requeueable",
    });
    await expect(
      requeueSourceUploadDraft({
        userId,
        sourceFileId: linkedFailedSource.id,
        now,
        storage,
        eventSender: createFakeSourceUploadSender().sender,
      }),
    ).resolves.toMatchObject({
      status: "not-queued",
      reason: "not-requeueable",
    });

    const retrySender = createFakeSourceUploadSender();
    await expect(
      requeueSourceUploadDraft({
        userId,
        sourceFileId: savedFailedSource.id,
        now,
        storage,
        eventSender: retrySender.sender,
      }),
    ).resolves.toMatchObject({
      status: "queued",
      sourceFileId: savedFailedSource.id,
    });
    expect(retrySender.events).toHaveLength(1);
    await expect(
      requeueSourceUploadDraft({
        userId,
        sourceFileId: "missing-source-file",
        now,
        storage,
        eventSender: createFakeSourceUploadSender().sender,
      }),
    ).resolves.toMatchObject({
      status: "not-found",
      reason: "source-not-found",
    });
    await expect(
      requeueSourceUploadDraft({
        userId: otherUserId,
        sourceFileId: readySource.id,
        now,
        storage,
        eventSender: createFakeSourceUploadSender().sender,
      }),
    ).resolves.toMatchObject({
      status: "not-found",
      reason: "source-not-found",
    });
  });

  it("rejects requeue when storage metadata or the S3 object is missing", async () => {
    const userId = await createUser("upload_requeue_missing_storage");
    const incompleteSource = await prisma.sourceFile.create({
      data: {
        userId,
        kind: SourceFileKind.IMAGE,
        status: SourceFileStatus.UPLOADED,
        originalName: "incomplete upload",
        mimeType: "image/png",
        byteSize: 2048,
        storageBucket: "learnrecur-dev",
        metadata: {
          queuedAt: new Date(
            now.getTime() - SOURCE_PROCESSING_STALE_AFTER_MS,
          ).toISOString(),
        },
      },
    });
    const missingObjectSource = await prisma.sourceFile.create({
      data: {
        userId,
        kind: SourceFileKind.IMAGE,
        status: SourceFileStatus.UPLOADED,
        originalName: "missing object",
        mimeType: "image/png",
        byteSize: 2048,
        storageBucket: "learnrecur-dev",
        storageKey: `source-uploads/${userId}/missing-object.png`,
        metadata: {
          queuedAt: new Date(
            now.getTime() - SOURCE_PROCESSING_STALE_AFTER_MS,
          ).toISOString(),
        },
      },
    });
    const sender = createFakeSourceUploadSender();

    await expect(
      requeueSourceUploadDraft({
        userId,
        sourceFileId: incompleteSource.id,
        now,
        storage: createFakeUploadStorage().storage,
        eventSender: sender.sender,
      }),
    ).resolves.toMatchObject({
      status: "not-queued",
      reason: "invalid-upload",
    });
    await expect(
      requeueSourceUploadDraft({
        userId,
        sourceFileId: missingObjectSource.id,
        now,
        storage: createFakeUploadStorage({
          headError: new Error("object missing"),
        }).storage,
        eventSender: sender.sender,
      }),
    ).resolves.toMatchObject({
      status: "not-queued",
      reason: "invalid-upload",
    });
    expect(sender.events).toHaveLength(0);
  });

  it("does not revive a failed upload dismissed while requeue is validating storage", async () => {
    const userId = await createUser("upload_requeue_dismissed_race");
    const storageKey = `source-uploads/${userId}/dismissed-during-requeue.pdf`;
    const failedSource = await prisma.sourceFile.create({
      data: {
        userId,
        kind: SourceFileKind.PDF,
        status: SourceFileStatus.FAILED,
        originalName: "dismissed during requeue",
        mimeType: "application/pdf",
        byteSize: 2048,
        storageBucket: "learnrecur-dev",
        storageKey,
        metadata: {
          errorMessage: "Temporary processing failure.",
          retryCount: 1,
        },
      },
    });
    const storageSetup = createFakeUploadStorage({
      byteSize: 2048,
    });
    let dismissed = false;
    const guardedStorage: SourceUploadStorage = {
      ...storageSetup.storage,
      async headObject(input) {
        if (!dismissed) {
          dismissed = true;
          await expect(
            dismissFailedSourceUpload({
              userId,
              sourceFileId: failedSource.id,
              now,
              storage: storageSetup.storage,
            }),
          ).resolves.toMatchObject({
            status: "dismissed",
            sourceFileId: failedSource.id,
          });
        }

        return storageSetup.storage.headObject(input);
      },
    };
    const sender = createFakeSourceUploadSender();

    await expect(
      requeueSourceUploadDraft({
        userId,
        sourceFileId: failedSource.id,
        now,
        storage: guardedStorage,
        eventSender: sender.sender,
      }),
    ).resolves.toMatchObject({
      status: "not-queued",
      reason: "invalid-upload",
    });

    const dismissedSource = await prisma.sourceFile.findUniqueOrThrow({
      where: { id: failedSource.id },
    });
    expect(dismissedSource).toMatchObject({
      status: SourceFileStatus.FAILED,
      storageBucket: null,
      storageKey: null,
      metadata: expect.objectContaining({
        dismissedAt: now.toISOString(),
      }),
    });
    expect(sender.events).toEqual([]);
    expect(storageSetup.deletedKeys).toEqual([storageKey]);
  });

  it("allows saved upload rows to be dismissed", async () => {
    const userId = await createUser("upload_failed_saved_row");
    const uploadedStorageKey = `source-uploads/${userId}/waiting.pdf`;
    const staleStorageKey = `source-uploads/${userId}/stale.pdf`;
    const failedStorageKey = `source-uploads/${userId}/failed.pdf`;
    const retryableFailedStorageKey = `source-uploads/${userId}/retryable-failed.pdf`;
    const dismissedSourceIds: string[] = [];
    const processingStartedAt = new Date(
      now.getTime() - SOURCE_PROCESSING_STALE_AFTER_MS,
    ).toISOString();
    const uploadedSource = await prisma.sourceFile.create({
      data: {
        userId,
        kind: SourceFileKind.PDF,
        status: SourceFileStatus.UPLOADED,
        originalName: "waiting worksheet",
        mimeType: "application/pdf",
        byteSize: 1024,
        storageBucket: "learnrecur-dev",
        storageKey: uploadedStorageKey,
        metadata: {
          retryCount: MAX_SOURCE_UPLOAD_REQUEUE_ATTEMPTS,
        },
      },
    });
    const staleSource = await prisma.sourceFile.create({
      data: {
        userId,
        kind: SourceFileKind.PDF,
        status: SourceFileStatus.PROCESSING,
        originalName: "stale worksheet",
        mimeType: "application/pdf",
        byteSize: 1024,
        storageBucket: "learnrecur-dev",
        storageKey: staleStorageKey,
        metadata: {
          processingStartedAt,
          retryCount: MAX_SOURCE_UPLOAD_REQUEUE_ATTEMPTS,
        },
      },
    });
    const failedSource = await prisma.sourceFile.create({
      data: {
        userId,
        kind: SourceFileKind.PDF,
        status: SourceFileStatus.FAILED,
        originalName: "failed worksheet",
        mimeType: "application/pdf",
        byteSize: 1024,
        storageBucket: "learnrecur-dev",
        storageKey: failedStorageKey,
        metadata: {
          errorMessage: "Gemini could not extract enough study text from this file.",
          retryCount: MAX_SOURCE_UPLOAD_REQUEUE_ATTEMPTS,
        },
      },
    });
    const retryableFailedSource = await prisma.sourceFile.create({
      data: {
        userId,
        kind: SourceFileKind.PDF,
        status: SourceFileStatus.FAILED,
        originalName: "retryable failed worksheet",
        mimeType: "application/pdf",
        byteSize: 1024,
        storageBucket: "learnrecur-dev",
        storageKey: retryableFailedStorageKey,
        metadata: {
          errorMessage: "Gemini could not extract enough study text from this file.",
          retryCount: 1,
        },
      },
    });
    const cleanedFailedSource = await prisma.sourceFile.create({
      data: {
        userId,
        kind: SourceFileKind.PDF,
        status: SourceFileStatus.FAILED,
        originalName: "cleaned failed worksheet",
        mimeType: "application/pdf",
        metadata: {
          errorMessage: "Uploaded file is missing or larger than 10 MB.",
        },
      },
    });
    const storageSetup = createFakeUploadStorage();
    dismissedSourceIds.push(
      uploadedSource.id,
      staleSource.id,
      failedSource.id,
      retryableFailedSource.id,
      cleanedFailedSource.id,
    );

    await expect(getSkillsLibrary({ userId, now })).resolves.toMatchObject({
      sourceProcessing: expect.arrayContaining([
        expect.objectContaining({
          id: uploadedSource.id,
          status: SourceFileStatus.UPLOADED,
          canDismiss: true,
          canRequeue: false,
        }),
        expect.objectContaining({
          id: staleSource.id,
          status: SourceFileStatus.PROCESSING,
          canDismiss: true,
          canRequeue: false,
        }),
        expect.objectContaining({
          id: failedSource.id,
          status: SourceFileStatus.FAILED,
          canDismiss: true,
          canRequeue: false,
        }),
        expect.objectContaining({
          id: retryableFailedSource.id,
          status: SourceFileStatus.FAILED,
          canDismiss: true,
          canRequeue: true,
        }),
        expect.objectContaining({
          id: cleanedFailedSource.id,
          status: SourceFileStatus.FAILED,
          canDismiss: true,
          canRequeue: false,
        }),
      ]),
    });
    await expect(getSkillCreationSourceRecoveryItems({ userId, now })).resolves.toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          id: uploadedSource.id,
          canDismiss: true,
          canRequeue: false,
        }),
        expect.objectContaining({
          id: staleSource.id,
          canDismiss: true,
          canRequeue: false,
        }),
        expect.objectContaining({
          id: failedSource.id,
          canDismiss: true,
          canRequeue: false,
        }),
        expect.objectContaining({
          id: retryableFailedSource.id,
          canDismiss: true,
          canRequeue: true,
        }),
        expect.objectContaining({
          id: cleanedFailedSource.id,
          canDismiss: true,
          canRequeue: false,
        }),
      ]),
    );

    await expect(
      dismissFailedSourceUpload({
        userId,
        sourceFileId: uploadedSource.id,
        now,
        storage: storageSetup.storage,
      }),
    ).resolves.toMatchObject({
      status: "dismissed",
      sourceFileId: uploadedSource.id,
    });
    await expect(
      dismissFailedSourceUpload({
        userId,
        sourceFileId: staleSource.id,
        now,
        storage: storageSetup.storage,
      }),
    ).resolves.toMatchObject({
      status: "dismissed",
      sourceFileId: staleSource.id,
    });
    await expect(
      dismissFailedSourceUpload({
        userId,
        sourceFileId: failedSource.id,
        now,
        storage: storageSetup.storage,
      }),
    ).resolves.toMatchObject({
      status: "dismissed",
      sourceFileId: failedSource.id,
    });
    await expect(
      dismissFailedSourceUpload({
        userId,
        sourceFileId: retryableFailedSource.id,
        now,
        storage: storageSetup.storage,
      }),
    ).resolves.toMatchObject({
      status: "dismissed",
      sourceFileId: retryableFailedSource.id,
    });
    await expect(
      dismissFailedSourceUpload({
        userId,
        sourceFileId: cleanedFailedSource.id,
        now,
        storage: storageSetup.storage,
      }),
    ).resolves.toMatchObject({
      status: "dismissed",
      sourceFileId: cleanedFailedSource.id,
    });

    await expect(getSkillsLibrary({ userId, now })).resolves.toMatchObject({
      sourceProcessing: [],
    });
    await expect(getSkillCreationSourceRecoveryItems({ userId, now })).resolves.toEqual([]);

    const dismissedSources = await prisma.sourceFile.findMany({
      where: {
        id: {
          in: dismissedSourceIds,
        },
      },
      orderBy: {
        id: "asc",
      },
      select: {
        id: true,
        status: true,
        byteSize: true,
        storageBucket: true,
        storageKey: true,
        metadata: true,
      },
    });
    expect(dismissedSources).toHaveLength(5);
    expect(dismissedSources).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          id: uploadedSource.id,
          status: SourceFileStatus.FAILED,
          byteSize: 1024,
          storageBucket: null,
          storageKey: null,
          metadata: expect.objectContaining({
            dismissedAt: now.toISOString(),
          }),
        }),
        expect.objectContaining({
          id: staleSource.id,
          status: SourceFileStatus.FAILED,
          byteSize: 1024,
          storageBucket: null,
          storageKey: null,
          metadata: expect.objectContaining({
            dismissedAt: now.toISOString(),
          }),
        }),
        expect.objectContaining({
          id: failedSource.id,
          status: SourceFileStatus.FAILED,
          byteSize: 1024,
          storageBucket: null,
          storageKey: null,
          metadata: expect.objectContaining({
            dismissedAt: now.toISOString(),
          }),
        }),
        expect.objectContaining({
          id: retryableFailedSource.id,
          status: SourceFileStatus.FAILED,
          byteSize: 1024,
          storageBucket: null,
          storageKey: null,
          metadata: expect.objectContaining({
            dismissedAt: now.toISOString(),
          }),
        }),
        expect.objectContaining({
          id: cleanedFailedSource.id,
          status: SourceFileStatus.FAILED,
          byteSize: null,
          storageBucket: null,
          storageKey: null,
          metadata: expect.objectContaining({
            dismissedAt: now.toISOString(),
          }),
        }),
      ]),
    );
    expect(storageSetup.deletedKeys).toEqual([
      uploadedStorageKey,
      staleStorageKey,
      failedStorageKey,
      retryableFailedStorageKey,
    ]);
  });

  it("preserves saved upload rows when dismissed storage cleanup fails", async () => {
    const userId = await createUser("upload_dismiss_storage_failure");
    const storageKey = `source-uploads/${userId}/failed.pdf`;
    const source = await prisma.sourceFile.create({
      data: {
        userId,
        kind: SourceFileKind.PDF,
        status: SourceFileStatus.FAILED,
        originalName: "failed worksheet",
        mimeType: "application/pdf",
        byteSize: 1024,
        storageBucket: "learnrecur-dev",
        storageKey,
        metadata: {
          errorMessage: "Gemini could not extract enough study text from this file.",
        },
      },
    });
    const storageSetup = createFakeUploadStorage({
      onDeleteObject: () => {
        throw new Error("temporary S3 delete failure");
      },
    });

    await expect(
      dismissFailedSourceUpload({
        userId,
        sourceFileId: source.id,
        now,
        storage: storageSetup.storage,
      }),
    ).resolves.toMatchObject({
      status: "not-dismissed",
      reason: "storage-delete-failed",
      message: "temporary S3 delete failure",
    });

    await expect(
      prisma.sourceFile.findUnique({
        where: {
          id: source.id,
        },
        select: {
          status: true,
          storageBucket: true,
          storageKey: true,
          byteSize: true,
          metadata: true,
        },
      }),
    ).resolves.toMatchObject({
      status: SourceFileStatus.FAILED,
      storageBucket: "learnrecur-dev",
      storageKey,
      byteSize: 1024,
      metadata: {
        errorMessage: "Gemini could not extract enough study text from this file.",
      },
    });
    expect(storageSetup.deletedKeys).toEqual([]);
  });

  it("keeps dismissed upload storage pointers until storage objects are deleted", async () => {
    const userId = await createUser("upload_dismiss_keeps_until_delete");
    const storageKey = `source-uploads/${userId}/failed.pdf`;
    const source = await prisma.sourceFile.create({
      data: {
        userId,
        kind: SourceFileKind.PDF,
        status: SourceFileStatus.FAILED,
        originalName: "failed worksheet",
        mimeType: "application/pdf",
        byteSize: 1024,
        storageBucket: "learnrecur-dev",
        storageKey,
        metadata: {
          errorMessage: "Gemini could not extract enough study text from this file.",
        },
      },
    });
    let sourceDuringDelete:
      | {
          storageBucket: string | null;
          storageKey: string | null;
          metadata: Prisma.JsonValue | null;
        }
      | null = null;
    const storageSetup = createFakeUploadStorage({
      onDeleteObject: async () => {
        sourceDuringDelete = await prisma.sourceFile.findUnique({
          where: {
            id: source.id,
          },
          select: {
            status: true,
            storageBucket: true,
            storageKey: true,
            metadata: true,
          },
        });
      },
    });

    await expect(
      dismissFailedSourceUpload({
        userId,
        sourceFileId: source.id,
        now,
        storage: storageSetup.storage,
      }),
    ).resolves.toMatchObject({
      status: "dismissed",
      sourceFileId: source.id,
    });

    expect(sourceDuringDelete).toMatchObject({
      status: SourceFileStatus.FAILED,
      storageBucket: "learnrecur-dev",
      storageKey,
      metadata: expect.objectContaining({
        dismissalPendingAt: now.toISOString(),
      }),
    });
    const observedSourceDuringDelete = sourceDuringDelete as {
      metadata: Prisma.JsonValue | null;
    } | null;
    if (!observedSourceDuringDelete) {
      throw new Error("expected to observe the source before storage deletion");
    }
    expect(observedSourceDuringDelete.metadata).not.toEqual(
      expect.objectContaining({
        dismissedAt: expect.any(String),
      }),
    );
    await expect(
      prisma.sourceFile.findUnique({
        where: {
          id: source.id,
        },
        select: {
          status: true,
          storageBucket: true,
          storageKey: true,
          metadata: true,
        },
      }),
    ).resolves.toMatchObject({
      status: SourceFileStatus.FAILED,
      storageBucket: null,
      storageKey: null,
      metadata: expect.objectContaining({
        dismissedAt: now.toISOString(),
      }),
    });
    expect(storageSetup.deletedKeys).toEqual([storageKey]);
  });

  it("logs dismissal rollback races while preserving the stored object pointer", async () => {
    const userId = await createUser("upload_dismiss_rollback_race");
    const storageKey = `source-uploads/${userId}/failed.pdf`;
    const source = await prisma.sourceFile.create({
      data: {
        userId,
        kind: SourceFileKind.PDF,
        status: SourceFileStatus.FAILED,
        originalName: "failed worksheet",
        mimeType: "application/pdf",
        byteSize: 1024,
        storageBucket: "learnrecur-dev",
        storageKey,
        metadata: {
          errorMessage: "Gemini could not extract enough study text from this file.",
        },
      },
    });
    const storageSetup = createFakeUploadStorage({
      onDeleteObject: async () => {
        await prisma.sourceFile.update({
          where: {
            id: source.id,
          },
          data: {
            metadata: {
              errorMessage: "Concurrent cleanup update.",
            },
          },
        });
        throw new Error("temporary S3 delete failure");
      },
    });
    const consoleError = vi.spyOn(console, "error").mockImplementation(() => {});

    try {
      await expect(
        dismissFailedSourceUpload({
          userId,
          sourceFileId: source.id,
          now,
          storage: storageSetup.storage,
        }),
      ).resolves.toMatchObject({
        status: "not-dismissed",
        reason: "storage-delete-failed",
        message: "temporary S3 delete failure",
      });
      expect(consoleError).toHaveBeenCalledWith(
        "[skills] failed to restore source upload dismissal claim",
        expect.objectContaining({
          sourceFileId: source.id,
          userId,
        }),
      );
    } finally {
      consoleError.mockRestore();
    }

    await expect(
      prisma.sourceFile.findUnique({
        where: {
          id: source.id,
        },
        select: {
          status: true,
          storageBucket: true,
          storageKey: true,
          byteSize: true,
          metadata: true,
        },
      }),
    ).resolves.toMatchObject({
      status: SourceFileStatus.FAILED,
      storageBucket: "learnrecur-dev",
      storageKey,
      byteSize: 1024,
      metadata: {
        errorMessage: "Concurrent cleanup update.",
      },
    });
    expect(storageSetup.deletedKeys).toEqual([]);
  });

  it("rejects dismissal for cross-user, linked, and non-failed uploaded sources", async () => {
    const userId = await createUser("upload_dismiss_reject");
    const otherUserId = await createUser("upload_dismiss_reject_other");
    const linkedSkill = await createSkillDraft({
      userId,
      input: {
        title: "Linked failed source skill",
        objective: "Review a linked failed source without deleting its history.",
      },
    });

    if (linkedSkill.status !== "created") {
      throw new Error("Expected draft creation to succeed.");
    }

    const linkedFailedSource = await prisma.sourceFile.create({
      data: {
        userId,
        kind: SourceFileKind.IMAGE,
        status: SourceFileStatus.FAILED,
        originalName: "linked failed source",
        mimeType: "image/png",
      },
    });
    await prisma.skillSourceRef.create({
      data: {
        userId,
        skillId: linkedSkill.skill.id,
        sourceFileId: linkedFailedSource.id,
      },
    });
    const readySource = await prisma.sourceFile.create({
      data: {
        userId,
        kind: SourceFileKind.IMAGE,
        status: SourceFileStatus.READY,
        originalName: "ready source",
        mimeType: "image/png",
      },
    });

    const library = await getSkillsLibrary({ userId, now });
    expect(library.sourceProcessing).toEqual([
      expect.objectContaining({
        id: linkedFailedSource.id,
        status: SourceFileStatus.FAILED,
        canDismiss: false,
      }),
    ]);

    await expect(
      dismissFailedSourceUpload({
        userId,
        sourceFileId: linkedFailedSource.id,
      }),
    ).resolves.toMatchObject({
      status: "not-dismissed",
      reason: "linked-source",
    });
    await expect(
      dismissFailedSourceUpload({
        userId,
        sourceFileId: readySource.id,
      }),
    ).resolves.toMatchObject({
      status: "not-dismissed",
      reason: "not-failed",
    });
    await expect(
      dismissFailedSourceUpload({
        userId: otherUserId,
        sourceFileId: linkedFailedSource.id,
      }),
    ).resolves.toMatchObject({
      status: "not-found",
      reason: "source-not-found",
    });
    await expect(
      prisma.sourceFile.count({
        where: {
          id: {
            in: [linkedFailedSource.id, readySource.id],
          },
        },
      }),
    ).resolves.toBe(2);
  });

  it("does not persist a source, skill, or link when generated draft validation fails", async () => {
    const userId = await createUser("source_invalid_generation");
    const result = await createSkillDraftFromSource({
      userId,
      now,
      model: "test-gemini",
      generateSkillDraft: async () => ({
        drafts: [
          {
            title: "Incomplete draft",
            objective: "too short",
            rules: [],
            examples: [],
            exerciseConstraints: "",
            tags: [],
          },
        ],
      }),
      input: {
        sourceText:
          "Use ser for identity and long-term traits. Use estar for location and temporary states. Classroom practice should use short sentences with one obvious choice.",
      },
    });

    expect(result).toMatchObject({
      status: "not-created",
      reason: "invalid-generation",
    });

    await expect(prisma.skill.count({ where: { userId } })).resolves.toBe(0);
    await expect(prisma.sourceFile.count({ where: { userId } })).resolves.toBe(0);
    await expect(prisma.skillSourceRef.count({ where: { userId } })).resolves.toBe(0);
  });

  it("keeps source-backed drafts isolated by user ownership", async () => {
    const userId = await createUser("source_owner");
    const otherUserId = await createUser("source_other");
    const result = await createSkillDraftFromSource({
      userId,
      now,
      model: "test-gemini",
      generateSkillDraft: successfulSkillDraftGenerator,
      input: {
        sourceText:
          "Use ser for identity and long-term traits. Use estar for location and temporary states. Classroom practice should use short sentences with one obvious choice.",
      },
    });

    if (result.status !== "created") {
      throw new Error("Expected source draft creation to succeed.");
    }

    const denied = await activateSkillDraft({
      userId: otherUserId,
      skillId: result.skills[0].id,
      now,
      generateChoiceExercises: successfulGenerator,
      verifyChoiceExercises: acceptAllVerifier,
      model: "test-gemini",
    });

    expect(denied).toMatchObject({
      status: "not-found",
      reason: "skill-not-found",
    });

    await expect(prisma.generationJob.count({ where: { userId: otherUserId } })).resolves.toBe(0);
  });

  it("passes linked source context into activation generation and verification", async () => {
    const userId = await createUser("source_activation");
    const result = await createSkillDraftFromSource({
      userId,
      now,
      model: "test-gemini",
      generateSkillDraft: successfulSkillDraftGenerator,
      input: {
        sourceText:
          "Use ser for identity and long-term traits. Use estar for location and temporary states. Classroom practice should use short sentences with one obvious choice.",
        sourceLabel: "Activation source",
      },
    });

    if (result.status !== "created") {
      throw new Error("Expected source draft creation to succeed.");
    }

    let capturedSourceContext: string | null | undefined;
    let capturedVerifierSourceContext: string | null | undefined;
    const activationGenerator: ChoiceExerciseGenerator = async (input) => {
      capturedSourceContext = input.sourceContext;
      return {
        exercises: [generatedExercise(1), generatedExercise(2), generatedExercise(3)],
      };
    };
    const activationVerifier: ChoiceExerciseVerifier = async (input) => {
      capturedVerifierSourceContext = input.sourceContext;
      return acceptAllVerifier(input);
    };

    const activated = await activateSkillDraft({
      userId,
      skillId: result.skills[0].id,
      now,
      generateChoiceExercises: activationGenerator,
      verifyChoiceExercises: activationVerifier,
      model: "test-gemini",
    });

    expect(activated.status).toBe("activated");
    expect(capturedSourceContext).toContain("Use ser for identity");
    expect(capturedSourceContext).toContain("Use estar for location");
    expect(capturedVerifierSourceContext).toContain("Use ser for identity");
    expect(capturedVerifierSourceContext).toContain("Use estar for location");
  });

  it("passes linked uploaded media into activation generation and verification", async () => {
    const userId = await createUser("source_activation_media");
    const draft = await createSkillDraft({
      userId,
      input: {
        title: "Uploaded worksheet activation",
        objective: "Choose the correct answer from the uploaded worksheet source.",
        tags: "upload, worksheet",
      },
    });

    if (draft.status !== "created") {
      throw new Error("Expected draft creation to succeed.");
    }

    const sourceFile = await createUploadedSourceRefFixture({
      userId,
      skillId: draft.skill.id,
      originalName: "Activation worksheet",
      extractedText: "The worksheet says to use ser for identity and estar for location.",
    });
    const textSourceFile = await prisma.sourceFile.create({
      data: {
        userId,
        kind: SourceFileKind.TEXT,
        status: SourceFileStatus.READY,
        originalName: "Activation pasted notes",
        mimeType: "text/plain",
        byteSize: 64,
        extractedText: "Additional pasted notes should stay text-only.",
      },
    });
    await prisma.skillSourceRef.create({
      data: {
        userId,
        skillId: draft.skill.id,
        sourceFileId: textSourceFile.id,
      },
    });
    const uploadedBytes = Buffer.from("activation worksheet image bytes");
    let capturedGeneratorSourceMedia: SourceMediaContext[] | undefined;
    let capturedVerifierSourceMedia: SourceMediaContext[] | undefined;

    const activated = await activateSkillDraft({
      userId,
      skillId: draft.skill.id,
      now,
      sourceMediaLoader: createSingleSourceMediaLoader({
        sourceFile,
        bytes: uploadedBytes,
      }),
      generateChoiceExercises: async (input) => {
        capturedGeneratorSourceMedia = input.sourceMedia;
        expect(input.sourceContext).toContain("worksheet says to use ser");
        expect(input.sourceContext).toContain("Additional pasted notes");
        return {
          exercises: [generatedExercise(1), generatedExercise(2), generatedExercise(3)],
        };
      },
      verifyChoiceExercises: async (input) => {
        capturedVerifierSourceMedia = input.sourceMedia;
        expect(input.sourceContext).toContain("worksheet says to use ser");
        expect(input.sourceContext).toContain("Additional pasted notes");
        return acceptAllVerifier(input);
      },
      model: "test-gemini",
    });

    expect(activated.status).toBe("activated");
    expectSingleSourceMedia(capturedGeneratorSourceMedia, {
      sourceFileId: sourceFile.id,
      label: "Activation worksheet",
      mimeType: "image/png",
      bytes: uploadedBytes,
    });
    expectSingleSourceMedia(capturedVerifierSourceMedia, {
      sourceFileId: sourceFile.id,
      label: "Activation worksheet",
      mimeType: "image/png",
      bytes: uploadedBytes,
    });
  });

  it("fails activation generation when linked uploaded media cannot be loaded", async () => {
    const userId = await createUser("source_activation_media_failure");
    const draft = await createSkillDraft({
      userId,
      input: {
        title: "Uploaded worksheet activation failure",
        objective: "Choose the correct answer from an uploaded worksheet source.",
      },
    });

    if (draft.status !== "created") {
      throw new Error("Expected draft creation to succeed.");
    }

    await createUploadedSourceRefFixture({
      userId,
      skillId: draft.skill.id,
      originalName: "Activation unavailable worksheet",
    });

    const result = await activateSkillDraft({
      userId,
      skillId: draft.skill.id,
      now,
      sourceMediaLoader: async () => {
        throw new Error("media store unavailable");
      },
      generateChoiceExercises: async () => {
        throw new Error("Generator should not run when media loading fails.");
      },
      verifyChoiceExercises: acceptAllVerifier,
      model: "test-gemini",
    });

    expect(result).toMatchObject({
      status: "not-activated",
      reason: "generation-failed",
      message: expect.stringContaining("media store unavailable"),
    });

    if (result.status !== "not-activated") {
      throw new Error("Expected activation to fail.");
    }

    expect(result.generationJobId).toBeDefined();

    if (!result.generationJobId) {
      throw new Error("Expected activation failure to include a generation job id.");
    }

    await expect(
      prisma.generationJob.findUniqueOrThrow({
        where: { id: result.generationJobId },
      }),
    ).resolves.toMatchObject({
      userId,
      skillId: draft.skill.id,
      status: GenerationJobStatus.FAILED,
      acceptedCount: 0,
      rejectedCount: 0,
      errorMessage: expect.stringContaining("media store unavailable"),
    });
  });

  it("does not refill an active skill that already has target ready choice exercises", async () => {
    const userId = await createUser("refill_noop");
    const skill = await createActiveSkillFixture({
      userId,
      title: "No-op refill skill",
    });

    for (let index = 1; index <= DEFAULT_READY_EXERCISE_TARGET; index += 1) {
      await createChoiceExerciseFixture({ userId, skillId: skill.id, id: index });
    }

    const result = await refillChoiceExercisesForSkill({
      userId,
      skillId: skill.id,
      now,
      generateChoiceExercises: async () => {
        throw new Error("Generator should not be called for a full queue.");
      },
      verifyChoiceExercises: acceptAllVerifier,
      model: "test-gemini",
    });

    expect(result).toMatchObject({
      status: "not-refilled",
      reason: "already-at-target",
      readyExerciseCount: DEFAULT_READY_EXERCISE_TARGET,
      targetReadyCount: DEFAULT_READY_EXERCISE_TARGET,
    });

    await expect(
      Promise.all([
        prisma.exercise.count({ where: { userId, skillId: skill.id } }),
        prisma.generationJob.count({ where: { userId, skillId: skill.id } }),
      ]),
    ).resolves.toEqual([DEFAULT_READY_EXERCISE_TARGET, 0]);
  });

  it("queues choice refill work without creating exercises synchronously", async () => {
    const userId = await createUser("choice_refill_queue");
    const skill = await createActiveSkillFixture({
      userId,
      title: "Queued refill skill",
    });
    await createChoiceExerciseFixture({ userId, skillId: skill.id, id: 1 });
    await createChoiceExerciseFixture({ userId, skillId: skill.id, id: 2 });
    const fake = createFakeRefillSender();

    const queued = expectQueued(
      await queueChoiceExerciseRefillForSkill({
        userId,
        skillId: skill.id,
        now,
        sender: fake.sender,
        model: "test-gemini",
      }),
    );

    expect(queued.requestedCount).toBe(3);
    expect(fake.choiceEvents).toEqual([
      {
        userId,
        skillId: skill.id,
        generationJobId: queued.generationJobId,
        targetReadyCount: DEFAULT_READY_EXERCISE_TARGET,
        requestedAt: now.toISOString(),
      },
    ]);
    await expect(
      prisma.generationJob.findUniqueOrThrow({
        where: { id: queued.generationJobId },
        select: { status: true, startedAt: true, completedAt: true },
      }),
    ).resolves.toEqual({
      status: GenerationJobStatus.PENDING,
      startedAt: null,
      completedAt: null,
    });
    await expect(prisma.exercise.count({ where: { userId, skillId: skill.id } })).resolves.toBe(2);
  });

  it("runs queued choice refill jobs idempotently", async () => {
    const userId = await createUser("choice_refill_runner");
    const skill = await createActiveSkillFixture({
      userId,
      title: "Queued runner skill",
    });
    await createChoiceExerciseFixture({ userId, skillId: skill.id, id: 1 });
    await createChoiceExerciseFixture({ userId, skillId: skill.id, id: 2 });
    const fake = createFakeRefillSender();
    const queued = expectQueued(
      await queueChoiceExerciseRefillForSkill({
        userId,
        skillId: skill.id,
        now,
        sender: fake.sender,
        model: "test-gemini",
      }),
    );

    const firstRun = await runChoiceExerciseRefillJob({
      userId,
      skillId: skill.id,
      generationJobId: queued.generationJobId,
      targetReadyCount: queued.targetReadyCount,
      requestedAt: now.toISOString(),
      now,
      generateChoiceExercises: async () => ({
        exercises: [generatedExercise(401), generatedExercise(402), generatedExercise(403)],
      }),
      verifyChoiceExercises: acceptAllVerifier,
      model: "test-gemini",
    });

    expect(firstRun).toMatchObject({
      status: "refilled",
      exerciseCount: 3,
      readyExerciseCount: DEFAULT_READY_EXERCISE_TARGET,
    });

    const secondRun = await runChoiceExerciseRefillJob({
      userId,
      skillId: skill.id,
      generationJobId: queued.generationJobId,
      targetReadyCount: queued.targetReadyCount,
      requestedAt: now.toISOString(),
      now,
      generateChoiceExercises: async () => {
        throw new Error("Duplicate execution should not generate again.");
      },
      verifyChoiceExercises: acceptAllVerifier,
      model: "test-gemini",
    });

    expect(secondRun).toMatchObject({
      status: "not-refilled",
      reason: "job-not-pending",
    });
    await expect(
      Promise.all([
        prisma.exercise.count({ where: { userId, skillId: skill.id } }),
        prisma.generationJob.findUniqueOrThrow({
          where: { id: queued.generationJobId },
          select: { status: true, acceptedCount: true, rejectedCount: true },
        }),
      ]),
    ).resolves.toEqual([
      DEFAULT_READY_EXERCISE_TARGET,
      {
        status: GenerationJobStatus.SUCCEEDED,
        acceptedCount: 3,
        rejectedCount: 0,
      },
    ]);
  });

  it("reclaims a refill job after an unexpected retriable worker failure", async () => {
    const userId = await createUser("choice_refill_retry");
    const skill = await createActiveSkillFixture({ userId, title: "Retry refill skill" });
    await createChoiceExerciseFixture({ userId, skillId: skill.id, id: 1 });
    await createChoiceExerciseFixture({ userId, skillId: skill.id, id: 2 });
    const fake = createFakeRefillSender();
    const queued = expectQueued(await queueChoiceExerciseRefillForSkill({
      userId,
      skillId: skill.id,
      now,
      sender: fake.sender,
      model: "test-gemini",
    }));
    await prisma.generationJob.update({
      where: { id: queued.generationJobId },
      data: {
        status: GenerationJobStatus.RUNNING,
        stage: "GENERATING",
        checkpoint: "claimed",
        attemptCount: 1,
        startedAt: now,
      },
    });
    await markRefillJobRetryableFailure({
      userId,
      skillId: skill.id,
      generationJobId: queued.generationJobId,
      now,
      error: new Error("database connection reset"),
    });

    const result = await runChoiceExerciseRefillJob({
      userId,
      skillId: skill.id,
      generationJobId: queued.generationJobId,
      targetReadyCount: queued.targetReadyCount,
      requestedAt: now.toISOString(),
      now,
      generateChoiceExercises: async () => ({
        exercises: [generatedExercise(501), generatedExercise(502), generatedExercise(503)],
      }),
      verifyChoiceExercises: acceptAllVerifier,
      model: "test-gemini",
    });

    expect(result).toMatchObject({ status: "refilled", exerciseCount: 3 });
    await expect(prisma.generationJob.findUniqueOrThrow({
      where: { id: queued.generationJobId },
      select: { status: true, attemptCount: true, retryCount: true },
    })).resolves.toEqual({
      status: GenerationJobStatus.SUCCEEDED,
      attemptCount: 2,
      retryCount: 1,
    });
    await expect(prisma.generationAuditRecord.findFirstOrThrow({
      where: { jobId: queued.generationJobId, checkpoint: "retryable-exception" },
      select: { decision: true, idempotencyKey: true },
    })).resolves.toEqual({
      decision: GenerationAuditDecision.FAILED,
      idempotencyKey: `${GenerationJobKind.CHOICE_EXERCISE_GENERATION}:${skill.id}:${now.toISOString()}`,
    });
  });

  it("leaves refill jobs terminally failed after the worker retry limit", async () => {
    const userId = await createUser("choice_refill_retry_limit");
    const skill = await createActiveSkillFixture({ userId, title: "Retry-limited refill skill" });
    const fake = createFakeRefillSender();
    const queued = expectQueued(await queueChoiceExerciseRefillForSkill({
      userId,
      skillId: skill.id,
      now,
      sender: fake.sender,
      model: "test-gemini",
    }));
    await prisma.generationJob.update({
      where: { id: queued.generationJobId },
      data: {
        status: GenerationJobStatus.FAILED,
        checkpoint: "retryable-exception",
        attemptCount: REFILL_JOB_RETRY_LIMIT + 1,
        retryCount: REFILL_JOB_RETRY_LIMIT,
      },
    });

    const result = await runChoiceExerciseRefillJob({
      userId,
      skillId: skill.id,
      generationJobId: queued.generationJobId,
      targetReadyCount: queued.targetReadyCount,
      requestedAt: now.toISOString(),
      now,
      generateChoiceExercises: async () => {
        throw new Error("Retry-limited jobs must not call the generator.");
      },
      verifyChoiceExercises: acceptAllVerifier,
      model: "test-gemini",
    });

    expect(result).toMatchObject({ status: "not-refilled", reason: "job-not-pending" });
    await expect(prisma.generationJob.findUniqueOrThrow({
      where: { id: queued.generationJobId },
      select: { status: true, attemptCount: true, retryCount: true },
    })).resolves.toEqual({
      status: GenerationJobStatus.FAILED,
      attemptCount: REFILL_JOB_RETRY_LIMIT + 1,
      retryCount: REFILL_JOB_RETRY_LIMIT,
    });
  });

  it("reclaims the final refill attempt allowed by the worker retry limit", async () => {
    const userId = await createUser("choice_refill_last_retry");
    const skill = await createActiveSkillFixture({ userId, title: "Last retry refill skill" });
    const fake = createFakeRefillSender();
    const queued = expectQueued(await queueChoiceExerciseRefillForSkill({
      userId,
      skillId: skill.id,
      now,
      sender: fake.sender,
      model: "test-gemini",
    }));
    await prisma.generationJob.update({
      where: { id: queued.generationJobId },
      data: {
        status: GenerationJobStatus.FAILED,
        checkpoint: "retryable-exception",
        attemptCount: REFILL_JOB_RETRY_LIMIT,
        retryCount: REFILL_JOB_RETRY_LIMIT - 1,
      },
    });

    const result = await runChoiceExerciseRefillJob({
      userId,
      skillId: skill.id,
      generationJobId: queued.generationJobId,
      targetReadyCount: queued.targetReadyCount,
      requestedAt: now.toISOString(),
      now,
      generateChoiceExercises: async () => ({
        exercises: Array.from(
          { length: DEFAULT_READY_EXERCISE_TARGET },
          (_, index) => generatedExercise(601 + index),
        ),
      }),
      verifyChoiceExercises: acceptAllVerifier,
      model: "test-gemini",
    });

    expect(result).toMatchObject({ status: "refilled" });
    await expect(prisma.generationJob.findUniqueOrThrow({
      where: { id: queued.generationJobId },
      select: { status: true, attemptCount: true, retryCount: true },
    })).resolves.toEqual({
      status: GenerationJobStatus.SUCCEEDED,
      attemptCount: REFILL_JOB_RETRY_LIMIT + 1,
      retryCount: REFILL_JOB_RETRY_LIMIT,
    });
  });

  it("does not queue duplicate or locked refill jobs", async () => {
    const userId = await createUser("refill_queue_guards");
    const skill = await createActiveSkillFixture({
      userId,
      title: "Duplicate queued refill skill",
    });
    await createChoiceExerciseFixture({ userId, skillId: skill.id, id: 1 });
    const fake = createFakeRefillSender();
    const queued = expectQueued(
      await queueChoiceExerciseRefillForSkill({
        userId,
        skillId: skill.id,
        now,
        sender: fake.sender,
        model: "test-gemini",
      }),
    );

    const duplicate = await queueChoiceExerciseRefillForSkill({
      userId,
      skillId: skill.id,
      now,
      sender: fake.sender,
      model: "test-gemini",
    });

    expect(duplicate).toMatchObject({
      status: "not-queued",
      reason: "job-in-progress",
      generationJobId: queued.generationJobId,
    });
    expect(fake.choiceEvents).toHaveLength(1);

    const lockedExactInput = await queueExactInputExerciseRefillForSkill({
      userId,
      skillId: skill.id,
      now,
      sender: fake.sender,
      model: "test-gemini",
    });

    expect(lockedExactInput).toMatchObject({
      status: "not-queued",
      reason: "exact-input-locked",
    });
    expect(fake.exactInputEvents).toHaveLength(0);
  });

  it("records refill quota deferral durably and retries it after the UTC reset", async () => {
    const userId = await createUser("refill_quota_deferred");
    const skill = await createActiveSkillFixture({
      userId,
      title: "Deferred refill skill",
    });
    const fake = createFakeRefillSender();
    await prisma.generationJob.createMany({
      data: Array.from({ length: 50 }, (_, index) => ({
        userId,
        skillId: skill.id,
        kind: GenerationJobKind.CHOICE_EXERCISE_GENERATION,
        status: GenerationJobStatus.SUCCEEDED,
        stage: "COMPLETE" as const,
        provider: "test",
        model: "test-gemini",
        promptVersion: "test-refill",
        idempotencyKey: `refill-quota-${index}`,
        requestedCount: 1,
        acceptedCount: 1,
        createdAt: now,
        completedAt: now,
      })),
    });

    const first = await queueChoiceExerciseRefillForSkill({
      userId,
      skillId: skill.id,
      now,
      sender: fake.sender,
      model: "test-gemini",
    });
    expect(first).toMatchObject({
      status: "deferred",
      reason: "quota-exceeded",
      readyExerciseCount: 0,
      targetReadyCount: DEFAULT_READY_EXERCISE_TARGET,
      retryAt: "2026-06-05T00:00:00.000Z",
    });
    if (first.status !== "deferred") throw new Error("expected durable deferral");
    await expect(
      prisma.generationJob.findUniqueOrThrow({
        where: { id: first.generationJobId },
        select: { status: true, stage: true, checkpoint: true, failureCategory: true },
      }),
    ).resolves.toEqual({
      status: GenerationJobStatus.FAILED,
      stage: "FAILED",
      checkpoint: "deferred-quota",
      failureCategory: "COST_LIMIT",
    });

    const duplicate = await queueChoiceExerciseRefillForSkill({
      userId,
      skillId: skill.id,
      now,
      sender: fake.sender,
      model: "test-gemini",
    });
    expect(duplicate).toMatchObject({
      status: "deferred",
      generationJobId: first.generationJobId,
    });
    await expect(
      prisma.generationJob.count({
        where: { userId, skillId: skill.id, checkpoint: "deferred-quota" },
      }),
    ).resolves.toBe(1);
    expect(fake.choiceEvents).toHaveLength(0);

    const retried = expectQueued(
      await queueChoiceExerciseRefillForSkill({
        userId,
        skillId: skill.id,
        now: new Date("2026-06-05T00:01:00.000Z"),
        sender: fake.sender,
        model: "test-gemini",
      }),
    );
    expect(retried.generationJobId).not.toBe(first.generationJobId);
    expect(fake.choiceEvents).toHaveLength(1);
  });

  it("enforces one pending or running generation job per user skill and kind", async () => {
    const userId = await createUser("refill_queue_unique_index");
    const skill = await createActiveSkillFixture({
      userId,
      title: "Unique queued refill skill",
    });
    const jobDefaults = {
      userId,
      skillId: skill.id,
      provider: "google",
      model: "test-gemini",
      promptVersion: "skill-mcq-v2",
      requestedCount: 3,
    };

    await prisma.generationJob.create({
      data: {
        ...jobDefaults,
        kind: GenerationJobKind.CHOICE_EXERCISE_GENERATION,
        status: GenerationJobStatus.PENDING,
      },
    });

    await expect(
      prisma.generationJob.create({
        data: {
          ...jobDefaults,
          kind: GenerationJobKind.CHOICE_EXERCISE_GENERATION,
          status: GenerationJobStatus.RUNNING,
        },
      }),
    ).rejects.toMatchObject({ code: "P2002" });

    await expect(
      prisma.generationJob.create({
        data: {
          ...jobDefaults,
          kind: GenerationJobKind.CHOICE_EXERCISE_GENERATION,
          status: GenerationJobStatus.FAILED,
          errorMessage: "older failed job",
        },
      }),
    ).resolves.toBeTruthy();

    await expect(
      prisma.generationJob.create({
        data: {
          ...jobDefaults,
          kind: GenerationJobKind.EXACT_INPUT_EXERCISE_GENERATION,
          status: GenerationJobStatus.PENDING,
          promptVersion: "skill-exact-input-v3",
        },
      }),
    ).resolves.toBeTruthy();
  });

  it("refills an active skill below target while preserving schedule state", async () => {
    const userId = await createUser("refill_success");
    const skill = await createActiveSkillFixture({
      userId,
      title: "Source-backed refill skill",
    });
    const sourceFile = await prisma.sourceFile.create({
      data: {
        userId,
        kind: SourceFileKind.TEXT,
        status: SourceFileStatus.READY,
        originalName: "Refill source",
        mimeType: "text/plain",
        extractedText:
          "Use source-backed classroom examples with short prompts and clear choices.",
      },
    });
    await prisma.skillSourceRef.create({
      data: {
        userId,
        skillId: skill.id,
        sourceFileId: sourceFile.id,
      },
    });
    await createChoiceExerciseFixture({ userId, skillId: skill.id, id: 1 });
    await createChoiceExerciseFixture({ userId, skillId: skill.id, id: 2 });

    const originalSchedule = await prisma.skill.findUniqueOrThrow({
      where: { id: skill.id },
      select: {
        status: true,
        dueAt: true,
        stability: true,
        difficulty: true,
        elapsedDays: true,
        scheduledDays: true,
        learningSteps: true,
        repetitions: true,
        lapses: true,
        fsrsState: true,
        lastReviewedAt: true,
      },
    });
    let capturedRequestedCount: number | undefined;
    let capturedSourceContext: string | null | undefined;
    let capturedExistingExerciseContext: string | null | undefined;
    let capturedVerifierExistingExerciseContext: string | null | undefined;

    const refillGenerator: ChoiceExerciseGenerator = async (input) => {
      capturedRequestedCount = input.requestedCount;
      capturedSourceContext = input.sourceContext;
      capturedExistingExerciseContext = input.existingExerciseContext;
      return {
        exercises: [generatedExercise(101), generatedExercise(102), generatedExercise(103)],
      };
    };
    const refillVerifier: ChoiceExerciseVerifier = async (input) => {
      capturedVerifierExistingExerciseContext = input.existingExerciseContext;
      return acceptAllVerifier(input);
    };

    const result = await refillChoiceExercisesForSkill({
      userId,
      skillId: skill.id,
      now,
      generateChoiceExercises: refillGenerator,
      verifyChoiceExercises: refillVerifier,
      model: "test-gemini",
    });

    expect(result).toMatchObject({
      status: "refilled",
      exerciseCount: 3,
      readyExerciseCount: DEFAULT_READY_EXERCISE_TARGET,
      targetReadyCount: DEFAULT_READY_EXERCISE_TARGET,
    });
    expect(capturedRequestedCount).toBe(3);
    expect(capturedSourceContext).toContain("Use source-backed classroom examples");
    expect(capturedExistingExerciseContext).toContain("Existing refill prompt 1?");
    expect(capturedExistingExerciseContext).toContain("Correct answer 1");
    expect(capturedVerifierExistingExerciseContext).toContain("Existing refill prompt 2?");

    const [afterSchedule, exercises, generationJob, nextPracticeItem] = await Promise.all([
      prisma.skill.findUniqueOrThrow({
        where: { id: skill.id },
        select: {
          status: true,
          dueAt: true,
          stability: true,
          difficulty: true,
          elapsedDays: true,
          scheduledDays: true,
          learningSteps: true,
          repetitions: true,
          lapses: true,
          fsrsState: true,
          lastReviewedAt: true,
        },
      }),
      prisma.exercise.findMany({
        where: { userId, skillId: skill.id },
        orderBy: { createdAt: "asc" },
      }),
      prisma.generationJob.findFirstOrThrow({
        where: { userId, skillId: skill.id },
      }),
      getNextChoicePracticeItemForUser(userId, now),
    ]);

    expect(afterSchedule).toEqual(originalSchedule);
    expect(exercises).toHaveLength(DEFAULT_READY_EXERCISE_TARGET);
    expect(
      exercises.filter((exercise) =>
        /^What is the best translation for item 10[1-3]\?$/.test(exercise.prompt),
      ),
    ).toHaveLength(3);
    expect(generationJob).toMatchObject({
      status: GenerationJobStatus.SUCCEEDED,
      kind: GenerationJobKind.CHOICE_EXERCISE_GENERATION,
      provider: "google",
      model: "test-gemini",
      promptVersion: "skill-mcq-v2",
      requestedCount: 3,
      acceptedCount: 3,
      rejectedCount: 0,
      errorMessage: null,
    });
    expect(nextPracticeItem.status).toBe("ready");
  });

  it("passes linked uploaded media into choice refill generation and verification", async () => {
    const bytes = Buffer.from("choice refill worksheet image bytes");

    await expectRefillPassesSingleSourceMedia({
      userIdSuffix: "choice_refill_source_media",
      skillTitle: "Choice media refill skill",
      sourceName: "Choice refill worksheet",
      extractedText: "Choice refill extracted text should stay available.",
      bytes,
      createExistingExercises: async ({ userId, skillId }) => {
        await createChoiceExerciseFixture({ userId, skillId, id: 1 });
        await createChoiceExerciseFixture({ userId, skillId, id: 2 });
      },
      refill: async ({
        captureGeneratorSourceMedia,
        captureVerifierSourceMedia,
        skillId,
        sourceMediaLoader,
        userId,
      }) =>
        refillChoiceExercisesForSkill({
          userId,
          skillId,
          now,
          sourceMediaLoader,
          generateChoiceExercises: async (input) => {
            captureGeneratorSourceMedia(input.sourceMedia);
            expect(input.sourceContext).toContain("Choice refill extracted text");
            return {
              exercises: [generatedExercise(501), generatedExercise(502), generatedExercise(503)],
            };
          },
          verifyChoiceExercises: async (input) => {
            captureVerifierSourceMedia(input.sourceMedia);
            expect(input.sourceContext).toContain("Choice refill extracted text");
            return acceptAllVerifier(input);
          },
          model: "test-gemini",
        }),
    });
  });

  it("skips source media loader during refill when no source media is attachable", async () => {
    const userId = await createUser("choice_refill_no_attachable_source_media");
    const skill = await createActiveSkillFixture({ userId });
    await createChoiceExerciseFixture({ userId, skillId: skill.id, id: 1 });
    await createChoiceExerciseFixture({ userId, skillId: skill.id, id: 2 });
    const sourceMediaLoader = vi.fn(async (): Promise<SourceMediaContext[]> => {
      throw new Error("source media loader should not run");
    });

    const result = await refillChoiceExercisesForSkill({
      userId,
      skillId: skill.id,
      now,
      sourceMediaLoader,
      generateChoiceExercises: async (input) => {
        expect(input.sourceMedia).toEqual([]);
        return {
          exercises: [generatedExercise(501), generatedExercise(502), generatedExercise(503)],
        };
      },
      verifyChoiceExercises: async (input) => {
        expect(input.sourceMedia).toEqual([]);
        return acceptAllVerifier(input);
      },
      model: "test-gemini",
    });

    expect(result.status).toBe("refilled");
    expect(sourceMediaLoader).not.toHaveBeenCalled();
  });

  it("fails choice refill generation when linked uploaded media cannot be loaded", async () => {
    const userId = await createUser("choice_refill_source_media_failure");
    const skill = await createActiveSkillFixture({
      userId,
      title: "Choice media failure refill skill",
    });
    await createChoiceExerciseFixture({ userId, skillId: skill.id, id: 1 });
    await createChoiceExerciseFixture({ userId, skillId: skill.id, id: 2 });
    await createUploadedSourceRefFixture({
      userId,
      skillId: skill.id,
      originalName: "Choice unavailable worksheet",
    });

    const result = await refillChoiceExercisesForSkill({
      userId,
      skillId: skill.id,
      now,
      sourceMediaLoader: async () => {
        throw new Error("media store unavailable");
      },
      generateChoiceExercises: async () => {
        throw new Error("Generator should not run when media loading fails.");
      },
      verifyChoiceExercises: acceptAllVerifier,
      model: "test-gemini",
    });

    expect(result).toMatchObject({
      status: "not-refilled",
      reason: "generation-failed",
      readyExerciseCount: 2,
      targetReadyCount: DEFAULT_READY_EXERCISE_TARGET,
      message: expect.stringContaining("media store unavailable"),
    });

    if (result.status !== "not-refilled") {
      throw new Error("Expected refill to fail.");
    }

    expect(result.generationJobId).toBeDefined();

    if (!result.generationJobId) {
      throw new Error("Expected refill failure to include a generation job id.");
    }

    await expect(
      prisma.generationJob.findUniqueOrThrow({
        where: { id: result.generationJobId },
      }),
    ).resolves.toMatchObject({
      userId,
      skillId: skill.id,
      status: GenerationJobStatus.FAILED,
      acceptedCount: 0,
      rejectedCount: 0,
      errorMessage: expect.stringContaining("media store unavailable"),
    });
    await expect(
      prisma.exercise.count({ where: { userId, skillId: skill.id } }),
    ).resolves.toBe(2);
  });

  it("passes linked uploaded media into exact-input refill generation and verification", async () => {
    const bytes = Buffer.from("exact refill worksheet image bytes");

    await expectRefillPassesSingleSourceMedia({
      userIdSuffix: "exact_refill_source_media",
      skillTitle: "Exact media refill skill",
      repetitions: EXACT_INPUT_UNLOCK_REPETITIONS,
      sourceName: "Exact refill worksheet",
      extractedText: "Exact refill extracted text should stay available.",
      bytes,
      refill: async ({
        captureGeneratorSourceMedia,
        captureVerifierSourceMedia,
        skillId,
        sourceMediaLoader,
        userId,
      }) =>
        refillExactInputExercisesForSkill({
          userId,
          skillId,
          now,
          sourceMediaLoader,
          generateExactInputExercises: async (input) => {
            captureGeneratorSourceMedia(input.sourceMedia);
            expect(input.sourceContext).toContain("Exact refill extracted text");
            return {
              exercises: [generatedExactInputExercise(501), generatedExactInputExercise(502)],
            };
          },
          verifyExactInputExercises: async (input) => {
            captureVerifierSourceMedia(input.sourceMedia);
            expect(input.sourceContext).toContain("Exact refill extracted text");
            return acceptAllExactInputVerifier(input);
          },
          model: "test-gemini",
        }),
    });
  });

  it("passes linked uploaded media into math refill generation and verification", async () => {
    const bytes = Buffer.from("math refill worksheet image bytes");

    await expectRefillPassesSingleSourceMedia({
      userIdSuffix: "math_refill_source_media",
      skillTitle: "Math media refill skill",
      repetitions: EXACT_INPUT_UNLOCK_REPETITIONS,
      sourceName: "Math refill worksheet",
      extractedText: "Math refill extracted text should stay available.",
      bytes,
      refill: async ({
        captureGeneratorSourceMedia,
        captureVerifierSourceMedia,
        skillId,
        sourceMediaLoader,
        userId,
      }) =>
        refillMathExercisesForSkill({
          userId,
          skillId,
          now,
          sourceMediaLoader,
          generateMathExercises: async (input) => {
            captureGeneratorSourceMedia(input.sourceMedia);
            expect(input.sourceContext).toContain("Math refill extracted text");
            return {
              exercises: [generatedMathExercise(501), generatedMathExercise(502)],
            };
          },
          verifyMathExercises: async (input) => {
            captureVerifierSourceMedia(input.sourceMedia);
            expect(input.sourceContext).toContain("Math refill extracted text");
            return acceptAllMathVerifier(input);
          },
          model: "test-gemini",
        }),
    });
  });

  it("filters duplicate generated exercises during refill", async () => {
    const userId = await createUser("refill_duplicates");
    const skill = await createActiveSkillFixture({ userId });
    await createChoiceExerciseFixture({ userId, skillId: skill.id, id: 1 });

    const result = await refillChoiceExercisesForSkill({
      userId,
      skillId: skill.id,
      now,
      targetReadyCount: 3,
      generateChoiceExercises: async () => ({
        exercises: [
          {
            ...generatedExercise(100),
            prompt: "Existing refill prompt 1?",
            choices: [
              { id: "correct", label: "Correct answer 1" },
              { id: "wrong", label: "Wrong answer 1" },
              { id: "close", label: "Close distractor 1" },
            ],
          },
          generatedExercise(201),
        ],
      }),
      verifyChoiceExercises: acceptAllVerifier,
      model: "test-gemini",
    });

    expect(result).toMatchObject({
      status: "refilled",
      exerciseCount: 1,
      readyExerciseCount: 2,
      targetReadyCount: 3,
    });

    const [prompts, job] = await Promise.all([
      prisma.exercise.findMany({
        where: { userId, skillId: skill.id },
        orderBy: { createdAt: "asc" },
        select: { prompt: true },
      }),
      prisma.generationJob.findFirstOrThrow({ where: { userId, skillId: skill.id } }),
    ]);

    expect(prompts.map((exercise) => exercise.prompt)).toEqual([
      "Existing refill prompt 1?",
      "What is the best translation for item 201?",
    ]);
    expect(job).toMatchObject({
      status: GenerationJobStatus.SUCCEEDED,
      requestedCount: 2,
      acceptedCount: 1,
      rejectedCount: 1,
    });
  });

  it("creates failed generation jobs without writing exercises when refill generation fails", async () => {
    const userId = await createUser("refill_generation_fail");
    const skill = await createActiveSkillFixture({ userId });

    const result = await refillChoiceExercisesForSkill({
      userId,
      skillId: skill.id,
      now,
      targetReadyCount: 2,
      generateChoiceExercises: async () => {
        throw new Error("generator unavailable");
      },
      verifyChoiceExercises: acceptAllVerifier,
      model: "test-gemini",
    });

    expect(result).toMatchObject({
      status: "not-refilled",
      reason: "generation-failed",
    });

    await expect(
      Promise.all([
        prisma.exercise.count({ where: { userId, skillId: skill.id } }),
        prisma.generationJob.findFirstOrThrow({ where: { userId, skillId: skill.id } }),
      ]),
    ).resolves.toEqual([
      0,
      expect.objectContaining({
        status: GenerationJobStatus.FAILED,
        requestedCount: 2,
        acceptedCount: 0,
        rejectedCount: 0,
      }),
    ]);
  });

  it("creates failed generation jobs without writing exercises when refill verification fails", async () => {
    const userId = await createUser("refill_verification_fail");
    const skill = await createActiveSkillFixture({ userId });

    const result = await refillChoiceExercisesForSkill({
      userId,
      skillId: skill.id,
      now,
      targetReadyCount: 2,
      generateChoiceExercises: async () => ({
        exercises: [generatedExercise(1), generatedExercise(2)],
      }),
      verifyChoiceExercises: async () => {
        throw new Error("verifier unavailable");
      },
      model: "test-gemini",
    });

    expect(result).toMatchObject({
      status: "not-refilled",
      reason: "verification-failed",
    });

    await expect(
      Promise.all([
        prisma.exercise.count({ where: { userId, skillId: skill.id } }),
        prisma.generationJob.findFirstOrThrow({ where: { userId, skillId: skill.id } }),
      ]),
    ).resolves.toEqual([
      0,
      expect.objectContaining({
        status: GenerationJobStatus.FAILED,
        requestedCount: 2,
        acceptedCount: 0,
        rejectedCount: 2,
      }),
    ]);
  });

  it("rejects draft, archived, missing, and cross-user refill requests without jobs", async () => {
    const userId = await createUser("refill_reject");
    const otherUserId = await createUser("refill_reject_other");
    const draftSkill = await createActiveSkillFixture({
      userId,
      title: "Draft refill skill",
      status: SkillStatus.DRAFT,
    });
    const archivedSkill = await createActiveSkillFixture({
      userId,
      title: "Archived refill skill",
      status: SkillStatus.ARCHIVED,
    });
    const otherSkill = await createActiveSkillFixture({
      userId: otherUserId,
      title: "Other refill skill",
    });

    await expect(
      refillChoiceExercisesForSkill({
        userId,
        skillId: draftSkill.id,
        now,
        generateChoiceExercises: successfulGenerator,
        model: "test-gemini",
      }),
    ).resolves.toMatchObject({
      status: "not-refilled",
      reason: "skill-not-active",
    });
    await expect(
      refillChoiceExercisesForSkill({
        userId,
        skillId: archivedSkill.id,
        now,
        generateChoiceExercises: successfulGenerator,
        model: "test-gemini",
      }),
    ).resolves.toMatchObject({
      status: "not-refilled",
      reason: "skill-not-active",
    });
    await expect(
      refillChoiceExercisesForSkill({
        userId,
        skillId: otherSkill.id,
        now,
        generateChoiceExercises: successfulGenerator,
        model: "test-gemini",
      }),
    ).resolves.toMatchObject({
      status: "not-found",
      reason: "skill-not-found",
    });
    await expect(
      refillChoiceExercisesForSkill({
        userId,
        skillId: "missing-skill",
        now,
        generateChoiceExercises: successfulGenerator,
        model: "test-gemini",
      }),
    ).resolves.toMatchObject({
      status: "not-found",
      reason: "skill-not-found",
    });

    await expect(prisma.generationJob.count({ where: { userId } })).resolves.toBe(0);
  });

  it("counts only ready choice exercises when deciding refill size", async () => {
    const userId = await createUser("refill_ready_count");
    const skill = await createActiveSkillFixture({ userId });
    await createChoiceExerciseFixture({ userId, skillId: skill.id, id: 1 });
    await createChoiceExerciseFixture({
      userId,
      skillId: skill.id,
      id: 2,
      retiredAt: new Date("2026-06-04T16:05:00.000Z"),
    });
    await createChoiceExerciseFixture({
      userId,
      skillId: skill.id,
      id: 3,
      verificationStatus: ExerciseVerificationStatus.UNVERIFIED,
    });
    await createChoiceExerciseFixture({
      userId,
      skillId: skill.id,
      id: 4,
      choices: [{ id: "correct" }],
    });
    await createTextExerciseFixture({ userId, skillId: skill.id });
    let capturedRequestedCount: number | undefined;

    const result = await refillChoiceExercisesForSkill({
      userId,
      skillId: skill.id,
      now,
      targetReadyCount: 2,
      generateChoiceExercises: async (input) => {
        capturedRequestedCount = input.requestedCount;
        return {
          exercises: [generatedExercise(301)],
        };
      },
      verifyChoiceExercises: acceptAllVerifier,
      model: "test-gemini",
    });

    expect(result).toMatchObject({
      status: "refilled",
      exerciseCount: 1,
      readyExerciseCount: 2,
      targetReadyCount: 2,
    });
    expect(capturedRequestedCount).toBe(1);
  });

  it("refills exact-input exercises for an unlocked active skill while preserving schedule state", async () => {
    const userId = await createUser("exact_refill_success");
    const skill = await createActiveSkillFixture({
      userId,
      title: "Unlocked exact refill skill",
      repetitions: EXACT_INPUT_UNLOCK_REPETITIONS,
    });
    const originalSchedule = await prisma.skill.findUniqueOrThrow({
      where: { id: skill.id },
      select: {
        status: true,
        dueAt: true,
        stability: true,
        difficulty: true,
        elapsedDays: true,
        scheduledDays: true,
        learningSteps: true,
        repetitions: true,
        lapses: true,
        fsrsState: true,
        lastReviewedAt: true,
      },
    });
    let capturedRequestedCount: number | undefined;
    let capturedExistingExerciseContext: string | null | undefined;

    const generator: ExactInputExerciseGenerator = async (input) => {
      capturedRequestedCount = input.requestedCount;
      capturedExistingExerciseContext = input.existingExerciseContext;
      return {
        exercises: [
          generatedExactInputExercise(1),
          {
            prompt: "Enter the decimal equivalent of one half.",
            answerKind: AnswerKind.NUMERIC,
            answerSpec: {
              kind: "numeric",
              accepted: ["1/2", 0.5],
              tolerance: 0,
            },
            correctAnswerDisplay: "0.5",
            explanation: "One half is 0.5.",
            difficulty: 2,
            expectedSeconds: 30,
          },
        ],
      };
    };

    const result = await refillExactInputExercisesForSkill({
      userId,
      skillId: skill.id,
      now,
      generateExactInputExercises: generator,
      verifyExactInputExercises: acceptAllExactInputVerifier,
      model: "test-gemini",
    });

    expect(result).toMatchObject({
      status: "refilled",
      exerciseCount: DEFAULT_READY_EXACT_INPUT_TARGET,
      readyExerciseCount: DEFAULT_READY_EXACT_INPUT_TARGET,
      targetReadyCount: DEFAULT_READY_EXACT_INPUT_TARGET,
    });
    expect(capturedRequestedCount).toBe(DEFAULT_READY_EXACT_INPUT_TARGET);
    expect(capturedExistingExerciseContext).toBeNull();

    const [afterSchedule, exercises, generationJob, choicePracticeItem] = await Promise.all([
      prisma.skill.findUniqueOrThrow({
        where: { id: skill.id },
        select: {
          status: true,
          dueAt: true,
          stability: true,
          difficulty: true,
          elapsedDays: true,
          scheduledDays: true,
          learningSteps: true,
          repetitions: true,
          lapses: true,
          fsrsState: true,
          lastReviewedAt: true,
        },
      }),
      prisma.exercise.findMany({
        where: { userId, skillId: skill.id },
        orderBy: { createdAt: "asc" },
      }),
      prisma.generationJob.findFirstOrThrow({ where: { userId, skillId: skill.id } }),
      getNextChoicePracticeItemForUser(userId, now),
    ]);

    expect(afterSchedule).toEqual(originalSchedule);
    expect(exercises).toHaveLength(DEFAULT_READY_EXACT_INPUT_TARGET);
    expect(exercises.map((exercise) => exercise.answerKind).toSorted()).toEqual([
      AnswerKind.NUMERIC,
      AnswerKind.TEXT,
    ]);
    expect(generationJob).toMatchObject({
      status: GenerationJobStatus.SUCCEEDED,
      kind: GenerationJobKind.EXACT_INPUT_EXERCISE_GENERATION,
      acceptedCount: DEFAULT_READY_EXACT_INPUT_TARGET,
      rejectedCount: 0,
    });
    expect(choicePracticeItem.status).toBe("none-due");
  });

  it("queues and runs exact-input refill jobs for unlocked skills", async () => {
    const userId = await createUser("exact_refill_queue");
    const skill = await createActiveSkillFixture({
      userId,
      repetitions: EXACT_INPUT_UNLOCK_REPETITIONS,
    });
    const fake = createFakeRefillSender();

    const queued = expectQueued(
      await queueExactInputExerciseRefillForSkill({
        userId,
        skillId: skill.id,
        now,
        sender: fake.sender,
        model: "test-gemini",
      }),
    );

    expect(fake.exactInputEvents).toEqual([
      {
        userId,
        skillId: skill.id,
        generationJobId: queued.generationJobId,
        targetReadyCount: DEFAULT_READY_EXACT_INPUT_TARGET,
        requestedAt: now.toISOString(),
      },
    ]);
    await expect(prisma.exercise.count({ where: { userId, skillId: skill.id } })).resolves.toBe(0);

    const result = await runExactInputExerciseRefillJob({
      userId,
      skillId: skill.id,
      generationJobId: queued.generationJobId,
      targetReadyCount: queued.targetReadyCount,
      requestedAt: now.toISOString(),
      now,
      generateExactInputExercises: async () => ({
        exercises: [generatedExactInputExercise(701), generatedExactInputExercise(702)],
      }),
      verifyExactInputExercises: acceptAllExactInputVerifier,
      model: "test-gemini",
    });

    expect(result).toMatchObject({
      status: "refilled",
      exerciseCount: DEFAULT_READY_EXACT_INPUT_TARGET,
    });
    await expect(
      prisma.generationJob.findUniqueOrThrow({
        where: { id: queued.generationJobId },
        select: { status: true, acceptedCount: true, rejectedCount: true },
      }),
    ).resolves.toEqual({
      status: GenerationJobStatus.SUCCEEDED,
      acceptedCount: DEFAULT_READY_EXACT_INPUT_TARGET,
      rejectedCount: 0,
    });
  });

  it("queues and runs math refill jobs for unlocked skills", async () => {
    const userId = await createUser("math_refill_queue");
    const skill = await createActiveSkillFixture({
      userId,
      title: "Solving linear equations",
      repetitions: EXACT_INPUT_UNLOCK_REPETITIONS,
    });
    const fake = createFakeRefillSender();

    const queued = expectQueued(
      await queueMathExerciseRefillForSkill({
        userId,
        skillId: skill.id,
        now,
        sender: fake.sender,
        model: "test-gemini",
      }),
    );

    expect(fake.mathEvents).toEqual([
      {
        userId,
        skillId: skill.id,
        generationJobId: queued.generationJobId,
        targetReadyCount: DEFAULT_READY_MATH_TARGET,
        requestedAt: now.toISOString(),
      },
    ]);
    await expect(prisma.exercise.count({ where: { userId, skillId: skill.id } })).resolves.toBe(0);

    const result = await runMathExerciseRefillJob({
      userId,
      skillId: skill.id,
      generationJobId: queued.generationJobId,
      targetReadyCount: queued.targetReadyCount,
      requestedAt: now.toISOString(),
      now,
      generateMathExercises: async () => ({
        exercises: [generatedMathExercise(701), generatedMathExercise(702)],
      }),
      verifyMathExercises: acceptAllMathVerifier,
      model: "test-gemini",
    });

    expect(result).toMatchObject({
      status: "refilled",
      exerciseCount: DEFAULT_READY_MATH_TARGET,
      readyExerciseCount: DEFAULT_READY_MATH_TARGET,
    });
    await expect(
      Promise.all([
        prisma.generationJob.findUniqueOrThrow({
          where: { id: queued.generationJobId },
          select: { status: true, acceptedCount: true, rejectedCount: true, kind: true },
        }),
        getNextPracticeItemForUser(userId, now),
      ]),
    ).resolves.toEqual([
      {
        status: GenerationJobStatus.SUCCEEDED,
        acceptedCount: DEFAULT_READY_MATH_TARGET,
        rejectedCount: 0,
        kind: GenerationJobKind.MATH_EXERCISE_GENERATION,
      },
      expect.objectContaining({
        status: "ready",
        exercise: expect.objectContaining({
          answerKind: AnswerKind.MATH,
        }),
      }),
    ]);
  });

  it("does not refill exact-input exercises before the review threshold", async () => {
    const userId = await createUser("exact_refill_locked");
    const skill = await createActiveSkillFixture({
      userId,
      repetitions: EXACT_INPUT_UNLOCK_REPETITIONS - 1,
    });

    const result = await refillExactInputExercisesForSkill({
      userId,
      skillId: skill.id,
      now,
      generateExactInputExercises: async () => {
        throw new Error("Generator should not run before exact input unlocks.");
      },
      verifyExactInputExercises: acceptAllExactInputVerifier,
      model: "test-gemini",
    });

    expect(result).toMatchObject({
      status: "not-refilled",
      reason: "exact-input-locked",
    });

    await expect(
      Promise.all([
        prisma.exercise.count({ where: { userId, skillId: skill.id } }),
        prisma.generationJob.count({ where: { userId, skillId: skill.id } }),
      ]),
    ).resolves.toEqual([0, 0]);
  });

  it("does not refill exact-input exercises that are already at target", async () => {
    const userId = await createUser("exact_refill_noop");
    const skill = await createActiveSkillFixture({
      userId,
      repetitions: EXACT_INPUT_UNLOCK_REPETITIONS,
    });
    await createTextExerciseFixture({ userId, skillId: skill.id, id: 1 });
    await createNumericExerciseFixture({ userId, skillId: skill.id, id: 2 });

    const result = await refillExactInputExercisesForSkill({
      userId,
      skillId: skill.id,
      now,
      generateExactInputExercises: async () => {
        throw new Error("Generator should not run for a full exact-input queue.");
      },
      verifyExactInputExercises: acceptAllExactInputVerifier,
      model: "test-gemini",
    });

    expect(result).toMatchObject({
      status: "not-refilled",
      reason: "already-at-target",
      readyExerciseCount: DEFAULT_READY_EXACT_INPUT_TARGET,
    });
    await expect(
      prisma.generationJob.count({ where: { userId, skillId: skill.id } }),
    ).resolves.toBe(0);
  });

  it("creates a failed exact-input generation job without writing exercises when verification fails", async () => {
    const userId = await createUser("exact_refill_verification_fail");
    const skill = await createActiveSkillFixture({
      userId,
      repetitions: EXACT_INPUT_UNLOCK_REPETITIONS,
    });

    const result = await refillExactInputExercisesForSkill({
      userId,
      skillId: skill.id,
      now,
      generateExactInputExercises: async () => ({
        exercises: [generatedExactInputExercise(1)],
      }),
      verifyExactInputExercises: async (input) => ({
        verifications: input.candidates.map((candidate) => ({
          candidateId: candidate.candidateId,
          verdict: "rejected",
          reason: "answer_mismatch",
        })),
      }),
      model: "test-gemini",
    });

    expect(result).toMatchObject({
      status: "not-refilled",
      reason: "invalid-verification",
    });

    const [exerciseCount, generationJob] = await Promise.all([
      prisma.exercise.count({ where: { userId, skillId: skill.id } }),
      prisma.generationJob.findFirstOrThrow({ where: { userId, skillId: skill.id } }),
    ]);

    expect(exerciseCount).toBe(0);
    expect(generationJob).toMatchObject({
      status: GenerationJobStatus.FAILED,
      kind: GenerationJobKind.EXACT_INPUT_EXERCISE_GENERATION,
      acceptedCount: 0,
      rejectedCount: 1,
    });
  });

  it("reports an exact-input refill as superseded when its job finishes concurrently", async () => {
    const userId = await createUser("exact_refill_concurrent_finish");
    const skill = await createActiveSkillFixture({
      userId,
      repetitions: EXACT_INPUT_UNLOCK_REPETITIONS,
    });

    const result = await refillExactInputExercisesForSkill({
      userId,
      skillId: skill.id,
      now,
      generateExactInputExercises: async () => {
        await prisma.generationJob.updateMany({
          where: {
            userId,
            skillId: skill.id,
            kind: GenerationJobKind.EXACT_INPUT_EXERCISE_GENERATION,
            status: GenerationJobStatus.RUNNING,
          },
          data: { status: GenerationJobStatus.SUCCEEDED },
        });
        throw new Error("late provider failure");
      },
      verifyExactInputExercises: acceptAllExactInputVerifier,
      model: "test-gemini",
    });

    expect(result).toMatchObject({ status: "not-refilled", reason: "job-not-pending" });
    await expect(prisma.generationJob.findFirstOrThrow({
      where: { userId, skillId: skill.id },
      select: { status: true },
    })).resolves.toEqual({ status: GenerationJobStatus.SUCCEEDED });
  });

  it("rejects non-active, missing, and cross-user exact-input refills without jobs", async () => {
    const userId = await createUser("exact_refill_rejects");
    const otherUserId = await createUser("exact_refill_rejects_other");
    const draftSkill = await createActiveSkillFixture({
      userId,
      status: SkillStatus.DRAFT,
      repetitions: EXACT_INPUT_UNLOCK_REPETITIONS,
    });
    const archivedSkill = await createActiveSkillFixture({
      userId,
      status: SkillStatus.ARCHIVED,
      repetitions: EXACT_INPUT_UNLOCK_REPETITIONS,
    });
    const otherSkill = await createActiveSkillFixture({
      userId: otherUserId,
      repetitions: EXACT_INPUT_UNLOCK_REPETITIONS,
    });

    for (const skillId of [draftSkill.id, archivedSkill.id]) {
      await expect(
        refillExactInputExercisesForSkill({
          userId,
          skillId,
          now,
          generateExactInputExercises: async () => ({
            exercises: [generatedExactInputExercise(1)],
          }),
          verifyExactInputExercises: acceptAllExactInputVerifier,
          model: "test-gemini",
        }),
      ).resolves.toMatchObject({
        status: "not-refilled",
        reason: "skill-not-active",
      });
    }

    await expect(
      refillExactInputExercisesForSkill({
        userId,
        skillId: otherSkill.id,
        now,
        generateExactInputExercises: async () => ({
          exercises: [generatedExactInputExercise(1)],
        }),
        verifyExactInputExercises: acceptAllExactInputVerifier,
        model: "test-gemini",
      }),
    ).resolves.toMatchObject({
      status: "not-found",
      reason: "skill-not-found",
    });

    await expect(
      refillExactInputExercisesForSkill({
        userId,
        skillId: "missing-skill",
        now,
        generateExactInputExercises: async () => ({
          exercises: [generatedExactInputExercise(1)],
        }),
        verifyExactInputExercises: acceptAllExactInputVerifier,
        model: "test-gemini",
      }),
    ).resolves.toMatchObject({
      status: "not-found",
      reason: "skill-not-found",
    });

    await expect(
      prisma.generationJob.count({
        where: { userId, kind: GenerationJobKind.EXACT_INPUT_EXERCISE_GENERATION },
      }),
    ).resolves.toBe(0);
  });

  it("does not refill math exercises before the review threshold", async () => {
    const userId = await createUser("math_refill_locked");
    const skill = await createActiveSkillFixture({
      userId,
      repetitions: EXACT_INPUT_UNLOCK_REPETITIONS - 1,
    });

    const result = await refillMathExercisesForSkill({
      userId,
      skillId: skill.id,
      now,
      generateMathExercises: async () => {
        throw new Error("Generator should not run before math input unlocks.");
      },
      verifyMathExercises: acceptAllMathVerifier,
      model: "test-gemini",
    });

    expect(result).toMatchObject({
      status: "not-refilled",
      reason: "exact-input-locked",
    });

    await expect(
      Promise.all([
        prisma.exercise.count({ where: { userId, skillId: skill.id } }),
        prisma.generationJob.count({ where: { userId, skillId: skill.id } }),
      ]),
    ).resolves.toEqual([0, 0]);
  });

  it("does not refill math exercises that are already at target", async () => {
    const userId = await createUser("math_refill_noop");
    const skill = await createActiveSkillFixture({
      userId,
      repetitions: EXACT_INPUT_UNLOCK_REPETITIONS,
    });
    await createMathExerciseFixture({ userId, skillId: skill.id, id: 1 });
    await createMathExerciseFixture({ userId, skillId: skill.id, id: 2 });

    const result = await refillMathExercisesForSkill({
      userId,
      skillId: skill.id,
      now,
      generateMathExercises: async () => {
        throw new Error("Generator should not run for a full math queue.");
      },
      verifyMathExercises: acceptAllMathVerifier,
      model: "test-gemini",
    });

    expect(result).toMatchObject({
      status: "not-refilled",
      reason: "already-at-target",
      readyExerciseCount: DEFAULT_READY_MATH_TARGET,
    });
    await expect(
      prisma.generationJob.count({ where: { userId, skillId: skill.id } }),
    ).resolves.toBe(0);
  });

  it("creates a failed math generation job without writing exercises when verification fails", async () => {
    const userId = await createUser("math_refill_verification_fail");
    const skill = await createActiveSkillFixture({
      userId,
      title: "Algebra input practice",
      repetitions: EXACT_INPUT_UNLOCK_REPETITIONS,
    });

    const result = await refillMathExercisesForSkill({
      userId,
      skillId: skill.id,
      now,
      generateMathExercises: async () => ({
        exercises: [generatedMathExercise(1)],
      }),
      verifyMathExercises: async (input) => ({
        verifications: input.candidates.map((candidate) => ({
          candidateId: candidate.candidateId,
          verdict: "rejected",
          reason: "answer_mismatch",
        })),
      }),
      model: "test-gemini",
    });

    expect(result).toMatchObject({
      status: "not-refilled",
      reason: "invalid-verification",
    });

    const [exerciseCount, generationJob] = await Promise.all([
      prisma.exercise.count({ where: { userId, skillId: skill.id } }),
      prisma.generationJob.findFirstOrThrow({ where: { userId, skillId: skill.id } }),
    ]);

    expect(exerciseCount).toBe(0);
    expect(generationJob).toMatchObject({
      status: GenerationJobStatus.FAILED,
      kind: GenerationJobKind.MATH_EXERCISE_GENERATION,
      acceptedCount: 0,
      rejectedCount: 1,
    });
  });

  it("reports a math refill as superseded when its job finishes concurrently", async () => {
    const userId = await createUser("math_refill_concurrent_finish");
    const skill = await createActiveSkillFixture({
      userId,
      title: "Algebra input practice",
      repetitions: EXACT_INPUT_UNLOCK_REPETITIONS,
    });

    const result = await refillMathExercisesForSkill({
      userId,
      skillId: skill.id,
      now,
      generateMathExercises: async () => {
        await prisma.generationJob.updateMany({
          where: {
            userId,
            skillId: skill.id,
            kind: GenerationJobKind.MATH_EXERCISE_GENERATION,
            status: GenerationJobStatus.RUNNING,
          },
          data: { status: GenerationJobStatus.SUCCEEDED },
        });
        throw new Error("late provider failure");
      },
      verifyMathExercises: acceptAllMathVerifier,
      model: "test-gemini",
    });

    expect(result).toMatchObject({ status: "not-refilled", reason: "job-not-pending" });
    await expect(prisma.generationJob.findFirstOrThrow({
      where: { userId, skillId: skill.id },
      select: { status: true },
    })).resolves.toEqual({ status: GenerationJobStatus.SUCCEEDED });
  });

  it("rejects non-active, missing, and cross-user math refills without jobs", async () => {
    const userId = await createUser("math_refill_rejects");
    const otherUserId = await createUser("math_refill_rejects_other");
    const draftSkill = await createActiveSkillFixture({
      userId,
      status: SkillStatus.DRAFT,
      repetitions: EXACT_INPUT_UNLOCK_REPETITIONS,
    });
    const archivedSkill = await createActiveSkillFixture({
      userId,
      status: SkillStatus.ARCHIVED,
      repetitions: EXACT_INPUT_UNLOCK_REPETITIONS,
    });
    const otherSkill = await createActiveSkillFixture({
      userId: otherUserId,
      repetitions: EXACT_INPUT_UNLOCK_REPETITIONS,
    });

    for (const skillId of [draftSkill.id, archivedSkill.id]) {
      await expect(
        refillMathExercisesForSkill({
          userId,
          skillId,
          now,
          generateMathExercises: async () => ({
            exercises: [generatedMathExercise(1)],
          }),
          verifyMathExercises: acceptAllMathVerifier,
          model: "test-gemini",
        }),
      ).resolves.toMatchObject({
        status: "not-refilled",
        reason: "skill-not-active",
      });
    }

    await expect(
      refillMathExercisesForSkill({
        userId,
        skillId: otherSkill.id,
        now,
        generateMathExercises: async () => ({
          exercises: [generatedMathExercise(1)],
        }),
        verifyMathExercises: acceptAllMathVerifier,
        model: "test-gemini",
      }),
    ).resolves.toMatchObject({
      status: "not-found",
      reason: "skill-not-found",
    });

    await expect(
      refillMathExercisesForSkill({
        userId,
        skillId: "missing-skill",
        now,
        generateMathExercises: async () => ({
          exercises: [generatedMathExercise(1)],
        }),
        verifyMathExercises: acceptAllMathVerifier,
        model: "test-gemini",
      }),
    ).resolves.toMatchObject({
      status: "not-found",
      reason: "skill-not-found",
    });

    await expect(
      prisma.generationJob.count({
        where: { userId, kind: GenerationJobKind.MATH_EXERCISE_GENERATION },
      }),
    ).resolves.toBe(0);
  });

  it("returns a typed exact-input setup error when Gemini env is missing", async () => {
    const originalGeminiApiKey = process.env.GEMINI_API_KEY;
    const originalGeminiEnterpriseKey = process.env.GEMINI_ENTERPRISE_AGENT_KEY_PLATFORM_KEY;
    const originalGeminiModel = process.env.GEMINI_MODEL;

    try {
      delete process.env.GEMINI_API_KEY;
      delete process.env.GEMINI_ENTERPRISE_AGENT_KEY_PLATFORM_KEY;
      delete process.env.GEMINI_MODEL;

      const userId = await createUser("exact_refill_missing_env");
      const skill = await createActiveSkillFixture({
        userId,
        repetitions: EXACT_INPUT_UNLOCK_REPETITIONS,
      });

      const result = await refillExactInputExercisesForSkill({
        userId,
        skillId: skill.id,
        now,
      });

      expect(result).toMatchObject({
        status: "not-refilled",
        reason: "missing-gemini-env",
      });

      await expect(
        prisma.generationJob.findFirstOrThrow({
          where: { userId, skillId: skill.id },
        }),
      ).resolves.toMatchObject({
        status: GenerationJobStatus.FAILED,
        kind: GenerationJobKind.EXACT_INPUT_EXERCISE_GENERATION,
        acceptedCount: 0,
        rejectedCount: 0,
      });
    } finally {
      if (originalGeminiApiKey === undefined) {
        delete process.env.GEMINI_API_KEY;
      } else {
        process.env.GEMINI_API_KEY = originalGeminiApiKey;
      }

      if (originalGeminiEnterpriseKey === undefined) {
        delete process.env.GEMINI_ENTERPRISE_AGENT_KEY_PLATFORM_KEY;
      } else {
        process.env.GEMINI_ENTERPRISE_AGENT_KEY_PLATFORM_KEY = originalGeminiEnterpriseKey;
      }

      if (originalGeminiModel === undefined) {
        delete process.env.GEMINI_MODEL;
      } else {
        process.env.GEMINI_MODEL = originalGeminiModel;
      }
    }
  });

  it("returns a typed setup error without creating practiceable exercises when Gemini env is missing", async () => {
    const originalGeminiApiKey = process.env.GEMINI_API_KEY;
    const originalGeminiEnterpriseKey = process.env.GEMINI_ENTERPRISE_AGENT_KEY_PLATFORM_KEY;
    const originalGeminiModel = process.env.GEMINI_MODEL;

    try {
      delete process.env.GEMINI_API_KEY;
      delete process.env.GEMINI_ENTERPRISE_AGENT_KEY_PLATFORM_KEY;
      delete process.env.GEMINI_MODEL;

      const userId = await createUser("missing_env");
      const draft = await createSkillDraft({
        userId,
        input: {
          title: "German articles",
          objective: "Choose the correct German article for common beginner nouns.",
        },
      });

      if (draft.status !== "created") {
        throw new Error("Expected draft creation to succeed.");
      }

      const result = await activateSkillDraft({
        userId,
        skillId: draft.skill.id,
        now,
      });

      expect(result).toMatchObject({
        status: "not-activated",
        reason: "missing-gemini-env",
      });

      const [skill, exerciseCount, job] = await Promise.all([
        prisma.skill.findUniqueOrThrow({ where: { id: draft.skill.id } }),
        prisma.exercise.count({ where: { userId, skillId: draft.skill.id } }),
        prisma.generationJob.findFirstOrThrow({ where: { userId, skillId: draft.skill.id } }),
      ]);

      expect(skill.status).toBe(SkillStatus.DRAFT);
      expect(exerciseCount).toBe(0);
      expect(job).toMatchObject({
        status: GenerationJobStatus.FAILED,
        provider: "google",
        model: DEFAULT_GEMINI_MODEL,
        acceptedCount: 0,
        rejectedCount: 0,
      });
    } finally {
      if (originalGeminiApiKey === undefined) {
        delete process.env.GEMINI_API_KEY;
      } else {
        process.env.GEMINI_API_KEY = originalGeminiApiKey;
      }

      if (originalGeminiEnterpriseKey === undefined) {
        delete process.env.GEMINI_ENTERPRISE_AGENT_KEY_PLATFORM_KEY;
      } else {
        process.env.GEMINI_ENTERPRISE_AGENT_KEY_PLATFORM_KEY = originalGeminiEnterpriseKey;
      }

      if (originalGeminiModel === undefined) {
        delete process.env.GEMINI_MODEL;
      } else {
        process.env.GEMINI_MODEL = originalGeminiModel;
      }
    }
  });
});
