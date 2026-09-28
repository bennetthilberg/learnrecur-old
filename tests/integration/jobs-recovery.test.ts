import { randomUUID } from "node:crypto";
import { afterAll, describe, expect, it } from "vitest";
import { buildJobEnvelope } from "@/lib/jobs/contracts";
import { finalizeFailedJob, recoverInterruptedJob } from "@/lib/jobs/recovery";
import { getPrisma } from "@/lib/prisma";
import { createSkillFixture } from "./test-helpers";

const describeDatabase = process.env.RUN_DATABASE_TESTS === "1" ? describe : describe.skip;

describeDatabase("interrupted AWS domain work", () => {
  const prisma = getPrisma();
  const ids: string[] = [];
  async function user() {
    const id = `aws-recovery-${randomUUID()}`;
    ids.push(id);
    return prisma.user.create({ data: { id, email: `${id}@example.com` } });
  }
  afterAll(async () => {
    await prisma.user.deleteMany({ where: { id: { in: ids } } });
    await prisma.$disconnect();
  });

  it("releases interrupted refills but preserves completed and semantic failures", async () => {
    const owner = await user();
    const skill = await createSkillFixture(prisma, { userId: owner.id, title: "Recovery fixture" });
    const generation = await prisma.generationJob.create({ data: {
      userId: owner.id, skillId: skill.id, kind: "CHOICE_EXERCISE_GENERATION", status: "RUNNING",
      stage: "GENERATING", provider: "google", model: "gemini-3.8-flash", promptVersion: "test", requestedCount: 1, attemptCount: 1,
    } });
    const job = buildJobEnvelope("learnrecur/choice-refill.requested", {
      userId: owner.id, skillId: skill.id, generationJobId: generation.id, targetReadyCount: 1, requestedAt: new Date().toISOString(),
    }, "staging");
    await recoverInterruptedJob(job);
    expect(await prisma.generationJob.findUniqueOrThrow({ where: { id: generation.id } })).toMatchObject({ status: "FAILED", checkpoint: "retryable-exception", attemptCount: 1 });
    expect(await prisma.generationAuditRecord.count({ where: { jobId: generation.id } })).toBe(1);
    for (const status of ["SUCCEEDED", "FAILED"] as const) {
      await prisma.generationJob.update({ where: { id: generation.id }, data: { status, checkpoint: "domain-result" } });
      await recoverInterruptedJob(job);
      expect(await prisma.generationJob.findUniqueOrThrow({ where: { id: generation.id } })).toMatchObject({ status, checkpoint: "domain-result" });
    }
    await prisma.generationJob.update({ where: { id: generation.id }, data: { status: "RUNNING" } });
    await recoverInterruptedJob({ ...job, data: { ...job.data, userId: "different-owner" } });
    expect(await prisma.generationJob.findUniqueOrThrow({ where: { id: generation.id } })).toMatchObject({ status: "RUNNING" });
  });

  it("releases only the matching owner's unfinished quick upload", async () => {
    const owner = await user();
    const source = await prisma.sourceFile.create({ data: { userId: owner.id, originalName: "fixture.pdf", status: "PROCESSING", storageKey: "fixture-key", metadata: { retained: true } } });
    const job = buildJobEnvelope("learnrecur/source-upload-draft.requested", { userId: owner.id, sourceFileId: source.id, requestedAt: new Date().toISOString() }, "staging");
    await recoverInterruptedJob({ ...job, data: { ...job.data, userId: "different-owner" } });
    expect(await prisma.sourceFile.findUniqueOrThrow({ where: { id: source.id } })).toMatchObject({ status: "PROCESSING" });
    await recoverInterruptedJob(job);
    expect(await prisma.sourceFile.findUniqueOrThrow({ where: { id: source.id } })).toMatchObject({ status: "UPLOADED", storageKey: "fixture-key", metadata: { retained: true } });
    for (const status of ["READY", "FAILED"] as const) {
      await prisma.sourceFile.update({ where: { id: source.id }, data: { status } });
      await recoverInterruptedJob(job);
      expect(await prisma.sourceFile.findUniqueOrThrow({ where: { id: source.id } })).toMatchObject({ status });
    }
    const skill = await createSkillFixture(prisma, { userId: owner.id, title: "Already committed draft" });
    await prisma.skillSourceRef.create({ data: { userId: owner.id, skillId: skill.id, sourceFileId: source.id } });
    await prisma.sourceFile.update({ where: { id: source.id }, data: { status: "PROCESSING" } });
    await expect(recoverInterruptedJob(job)).rejects.toMatchObject({ message: "JOB_SOURCE_RECOVERY_CONFLICT", retryable: false });
    expect(await prisma.sourceFile.findUniqueOrThrow({ where: { id: source.id } })).toMatchObject({ status: "PROCESSING" });
  });

  it("finalizes refills after the last killed delivery and preserves semantic outcomes", async () => {
    const owner = await user();
    const skill = await createSkillFixture(prisma, { userId: owner.id, title: "Terminal fixture" });
    const generation = await prisma.generationJob.create({ data: { userId: owner.id, skillId: skill.id, kind: "CHOICE_EXERCISE_GENERATION", status: "RUNNING", stage: "GENERATING", provider: "test", model: "fixture", promptVersion: "test", requestedCount: 1, attemptCount: 3 } });
    const job = buildJobEnvelope("learnrecur/choice-refill.requested", { userId: owner.id, skillId: skill.id, generationJobId: generation.id, targetReadyCount: 1, requestedAt: new Date().toISOString() }, "staging");
    await finalizeFailedJob(job, "JOB_ID_CONFLICT");
    expect((await prisma.generationJob.findUniqueOrThrow({ where: { id: generation.id } })).status).toBe("RUNNING");
    await finalizeFailedJob(job, "JOB_RETRIES_EXHAUSTED");
    expect(await prisma.generationJob.findUniqueOrThrow({ where: { id: generation.id } })).toMatchObject({ status: "FAILED", checkpoint: "retry-exhausted" });
    for (const status of ["SUCCEEDED", "FAILED"] as const) {
      await prisma.generationJob.update({ where: { id: generation.id }, data: { status, checkpoint: "semantic-outcome", failureCategory: "SEMANTIC", errorMessage: "Retained diagnostic" } });
      await finalizeFailedJob(job, "JOB_RETRIES_EXHAUSTED");
      expect(await prisma.generationJob.findUniqueOrThrow({ where: { id: generation.id } })).toMatchObject({ status, checkpoint: "semantic-outcome", errorMessage: "Retained diagnostic" });
    }
  });
});
