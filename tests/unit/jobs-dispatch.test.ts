import { afterEach, describe, expect, it, vi } from "vitest";

const recover = vi.hoisted(() => vi.fn());
vi.mock("@/lib/jobs/recovery", () => ({ recoverInterruptedJob: recover }));
const handlers = vi.hoisted(() => ({
  choice: vi.fn(), exact: vi.fn(), math: vi.fn(), refillFailure: vi.fn(), source: vi.fn(),
  ingestion: vi.fn(), cleanup: vi.fn(), draft: vi.fn(), activation: vi.fn(),
  agent: vi.fn(), revocation: vi.fn(), deletion: vi.fn(), recovery: vi.fn(), maintenance: vi.fn(), reminders: vi.fn(), email: vi.fn(),
}));
vi.mock("@/lib/skills/refill-jobs", () => ({ runChoiceExerciseRefillJob: handlers.choice, runExactInputExerciseRefillJob: handlers.exact, runMathExerciseRefillJob: handlers.math, markRefillJobRetryableFailure: handlers.refillFailure }));
vi.mock("@/lib/skills/uploads", () => ({ runQueuedSourceUploadDraftJob: handlers.source }));
vi.mock("@/lib/materials/ingestion", () => ({ runMaterialIngestionJob: handlers.ingestion }));
vi.mock("@/lib/materials/cleanup", () => ({ runMaterialCleanupJob: handlers.cleanup }));
vi.mock("@/lib/materials/batches", () => ({ runMaterialDraftItemJob: handlers.draft, runMaterialBatchActivationJob: handlers.activation }));
vi.mock("@/lib/agent-access/worker", () => ({ runAgentSkillOperationJob: handlers.agent }));
vi.mock("@/lib/agent-access/settings", () => ({ runAgentConnectionRevocationJob: handlers.revocation, runAgentAccessMaintenance: handlers.maintenance }));
vi.mock("@/lib/account-deletion", () => ({ runAccountDeletionJob: handlers.deletion, recoverRetryableAccountDeletionJobs: handlers.recovery }));
vi.mock("@/lib/reminders", () => ({ processDueReminderBatch: handlers.reminders, resolveClerkReminderAccountEmail: handlers.email }));

import { executeJob } from "@/lib/jobs/dispatch";
import { buildJobEnvelope } from "@/lib/jobs/contracts";
import { reserveWorkerJobFollowUp } from "@/lib/jobs/publication-context";

const requestedAt = "2026-09-05T01:00:00.000Z";
const user = { userId: "user-a", requestedAt };
const refill = { ...user, skillId: "skill-a", generationJobId: "generation-a", targetReadyCount: 5 };
const item = { ...user, batchId: "batch-a", itemId: "item-a" };
const now = new Date("2026-09-05T02:00:00.000Z");
const context = { attempt: 0, maxAttempts: 4 };

describe("all migrated job families", () => {
  afterEach(() => { vi.useRealTimers(); vi.resetAllMocks(); });

  it.each([
    ["choice-refill.requested", refill, "choice", { ...refill, now }],
    ["exact-input-refill.requested", refill, "exact", { ...refill, now }],
    ["math-refill.requested", refill, "math", { ...refill, now }],
    ["source-upload-draft.requested", { ...user, sourceFileId: "source-a" }, "source", { ...user, sourceFileId: "source-a", now }],
    ["material-ingestion.requested", { ...user, materialRevisionId: "revision-a" }, "ingestion", { userId: user.userId, materialRevisionId: "revision-a" }],
    ["material-cleanup.requested", { ...user, materialId: "material-a", cleanupJobId: "cleanup-a" }, "cleanup", { ...user, materialId: "material-a", cleanupJobId: "cleanup-a" }],
    ["material-draft-item.requested", item, "draft", { ...item, ...context }],
    ["material-batch-activation.requested", { ...item, generationJobId: "generation-a" }, "activation", { ...item, generationJobId: "generation-a", ...context }],
    ["agent-skill-operation.requested", { ...user, operationId: "operation-a" }, "agent", { ...user, operationId: "operation-a", deliveryAttempt: context }],
    ["agent-connection-revocation.requested", { ...user, connectionId: "connection-a" }, "revocation", { ...user, connectionId: "connection-a" }],
    ["account-deletion.requested", { ...user, deletionJobId: "deletion-a" }, "deletion", { ...user, deletionJobId: "deletion-a" }],
    ["account-deletion.recovery", { requestedAt }, "recovery", { now }],
    ["agent-access.maintenance", { requestedAt }, "maintenance", now],
    ["practice-reminders.due", { requestedAt }, "reminders", { now, accountEmailResolver: handlers.email }],
  ] as const)("dispatches %s with its original domain contract", async (name, data, handler, expected) => {
    vi.useFakeTimers();
    vi.setSystemTime(now);
    await executeJob(buildJobEnvelope(`learnrecur/${name}`, data, "staging"), context);
    expect(handlers[handler]).toHaveBeenCalledExactlyOnceWith(expected);
    expect(Object.values(handlers).reduce((sum, mock) => sum + mock.mock.calls.length, 0)).toBe(1);
    expect(recover).not.toHaveBeenCalled();
  });

  it("recovers an interrupted domain claim before executing a reclaimed delivery", async () => {
    const job = buildJobEnvelope("learnrecur/choice-refill.requested", refill, "staging");
    await executeJob(job, { attempt: 1, maxAttempts: 3 });
    expect(recover).toHaveBeenCalledExactlyOnceWith(job);
    expect(recover.mock.invocationCallOrder[0]).toBeLessThan(handlers.choice.mock.invocationCallOrder[0]);
  });

  it("keeps retries and domain work inside the worker continuation publication context", async () => {
    handlers.agent.mockImplementationOnce(async () => {
      expect(reserveWorkerJobFollowUp()).toBe(1);
    });
    await executeJob(buildJobEnvelope("learnrecur/agent-skill-operation.requested", {
      ...user,
      operationId: "operation-a",
    }, "staging"), context);
    expect(reserveWorkerJobFollowUp()).toBeUndefined();
  });

  it("does not execute when interrupted-state recovery fails", async () => {
    recover.mockRejectedValue(new Error("database unavailable"));
    await expect(executeJob(buildJobEnvelope("learnrecur/choice-refill.requested", refill, "staging"), { attempt: 1, maxAttempts: 3 })).rejects.toThrow("database unavailable");
    expect(handlers.choice).not.toHaveBeenCalled();
  });

  it.each(["choice", "exact-input", "math"])("marks %s refill failures retryable without replacing the original error", async (kind) => {
    const handler = kind === "exact-input" ? handlers.exact : handlers[kind as "choice" | "math"];
    const error = new Error("provider failed");
    handler.mockRejectedValue(error);
    handlers.refillFailure.mockRejectedValue(new Error("database unavailable"));
    const log = vi.spyOn(console, "error").mockImplementation(() => {});
    await expect(executeJob(buildJobEnvelope(`learnrecur/${kind}-refill.requested`, refill, "staging"), context)).rejects.toBe(error);
    expect(handlers.refillFailure).toHaveBeenCalledWith(expect.objectContaining({ ...refill, error }));
    expect(JSON.stringify(log.mock.calls)).not.toMatch(/provider failed|database unavailable/);
    log.mockRestore();
  });

  it.each(["choice", "exact-input", "math"])("retries a returned transient %s refill failure", async (kind) => {
    const handler = kind === "exact-input" ? handlers.exact : handlers[kind as "choice" | "math"];
    handler.mockResolvedValue({ status: "not-refilled", reason: "generation-failed", retryable: true, failureCode: "REFILL_PROVIDER_UNAVAILABLE" });
    await expect(executeJob(buildJobEnvelope(`learnrecur/${kind}-refill.requested`, refill, "staging"), context))
      .rejects.toMatchObject({ retryable: true, code: "REFILL_PROVIDER_UNAVAILABLE" });
  });

  it.each(["choice", "exact-input", "math"])("acknowledges terminal %s quality rejection", async (kind) => {
    const handler = kind === "exact-input" ? handlers.exact : handlers[kind as "choice" | "math"];
    const result = { status: "not-refilled", reason: "invalid-verification", retryable: false };
    handler.mockResolvedValue(result);
    await expect(executeJob(buildJobEnvelope(`learnrecur/${kind}-refill.requested`, refill, "staging"), context)).resolves.toEqual(result);
    expect(handlers.refillFailure).not.toHaveBeenCalled();
  });

  it.each(["choice", "exact-input", "math"])("passes the delivery deadline to %s refill work", async (kind) => {
    const handler = kind === "exact-input" ? handlers.exact : handlers[kind as "choice" | "math"];
    const deadlineAt = new Date("2026-09-05T02:06:00Z");
    await executeJob(buildJobEnvelope(`learnrecur/${kind}-refill.requested`, refill, "staging"), { ...context, deadlineAt });
    expect(handler).toHaveBeenCalledWith(expect.objectContaining({ deadlineAt }));
  });
});
