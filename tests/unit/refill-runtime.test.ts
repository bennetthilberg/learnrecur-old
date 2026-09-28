import { afterEach, describe, expect, it, vi } from "vitest";
import { JobStageTimeoutError } from "@/lib/jobs/deadline";
import {
  refillRetryMetadata,
  refillStageBudget,
  runRefillStage,
} from "@/lib/skills/refill-runtime";

const input = { userId: "owner", skillId: "skill" };
describe("refill stage deadlines", () => {
  afterEach(() => vi.useRealTimers());
  it("reserves cleanup time and caps each provider stage", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-09-28T00:00:00Z"));
    expect(refillStageBudget(input, "refill-generation")).toBe(105_000);
    expect(refillStageBudget(input, "refill-verification")).toBe(195_000);
    expect(
      refillStageBudget(
        { ...input, deadlineAt: new Date(Date.now() + 35_000) },
        "refill-generation",
      ),
    ).toBe(5_000);
  });
  it("does not start a stage after the cleanup margin", async () => {
    const run = vi.fn();
    await expect(
      runRefillStage(
        { ...input, deadlineAt: new Date(Date.now() + 100) },
        "refill-load",
        run,
      ),
    ).rejects.toMatchObject({ retryable: true });
    expect(run).not.toHaveBeenCalled();
  });
  it("aborts provider work at its stage deadline and ignores a late result", async () => {
    vi.useFakeTimers();
    let signal!: AbortSignal;
    let finish!: (value: string) => void;
    const work = runRefillStage(
      input,
      "refill-generation",
      async (stageSignal) => {
        signal = stageSignal;
        return new Promise<string>((resolve) => {
          finish = resolve;
        });
      },
    );
    const rejected = expect(work).rejects.toMatchObject({
      stage: "refill-generation",
      retryable: true,
    });
    await vi.advanceTimersByTimeAsync(105_000);
    await rejected;
    expect(signal.aborted).toBe(true);
    finish("late provider answer");
    await vi.advanceTimersByTimeAsync(0);
    expect(vi.getTimerCount()).toBe(0);
  });
  it("does not start a provider call when its parent has already canceled", async () => {
    const parent = new AbortController();
    parent.abort(
      new JobStageTimeoutError("delivery expired", "refill-generation"),
    );
    const run = vi.fn();
    await expect(
      runRefillStage(
        { ...input, signal: parent.signal },
        "refill-generation",
        run,
      ),
    ).rejects.toBe(parent.signal.reason);
    expect(run).not.toHaveBeenCalled();
  });
  it("retains source-pending, provider and database retry identities", () => {
    expect(refillRetryMetadata({ retryable: true }, "source", 2)).toEqual({
      retryable: true,
      failureCode: "REFILL_SOURCE_UNAVAILABLE",
      generationAttempt: 2,
    });
    expect(refillRetryMetadata({ code: 503 }, "verification", 2)).toEqual({
      retryable: true,
      failureCode: "REFILL_PROVIDER_UNAVAILABLE",
      generationAttempt: 2,
    });
    expect(refillRetryMetadata({ code: "P2024" }, "source", 2)).toEqual({
      retryable: true,
      failureCode: "REFILL_DATABASE_UNAVAILABLE",
      generationAttempt: 2,
    });
    expect(refillRetryMetadata({ code: 401 }, "generation", 2)).toEqual({});
  });
});
