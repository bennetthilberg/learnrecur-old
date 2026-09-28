import { beforeEach, describe, expect, it, vi } from "vitest";
const publish = vi.hoisted(() => vi.fn());
vi.mock("@/lib/jobs/publisher", () => ({ publishJob: publish }));
import {
  awsExerciseRefillEventSender,
  awsMaterialIngestionEventSender,
} from "@/lib/jobs/events";

describe("durable event publication options", () => {
  beforeEach(() => publish.mockReset().mockResolvedValue(undefined));
  it.each(["choice", "exact-input", "math", "material"] as const)(
    "passes the persisted %s identity to the actual publisher",
    async (kind) => {
      const signal = new AbortController().signal;
      const options = { eventId: "stable-envelope-id", signal };
      if (kind === "material")
        await awsMaterialIngestionEventSender.sendMaterialIngestionRequested(
          {
            userId: "owner",
            materialRevisionId: "revision",
            requestedAt: "2026-09-28T00:00:00Z",
          },
          options,
        );
      else {
        const run =
          kind === "choice"
            ? awsExerciseRefillEventSender.sendChoiceRefillRequested
            : kind === "math"
              ? awsExerciseRefillEventSender.sendMathRefillRequested
              : awsExerciseRefillEventSender.sendExactInputRefillRequested;
        await run(
          {
            userId: "owner",
            skillId: "skill",
            generationJobId: "generation",
            requestedAt: "2026-09-28T00:00:00Z",
            targetReadyCount: 5,
          },
          options,
        );
      }
      expect(publish).toHaveBeenCalledWith(
        expect.any(String),
        expect.any(Object),
        { id: options.eventId, signal },
      );
    },
  );
});
