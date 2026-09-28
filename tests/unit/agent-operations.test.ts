import { describe, expect, it } from "vitest";

import { AgentOperationItemStatus, AgentOperationStatus } from "@/generated/prisma/client";
import {
  getAgentItemProgressState,
  isRetryableAgentItemError,
  reduceAgentOperationStatus,
} from "@/lib/agent-access/operations";

describe("getAgentItemProgressState", () => {
  it.each([
    [AgentOperationItemStatus.ACTIVATING, "ACTIVATION_WAITING", "working"],
    [AgentOperationItemStatus.QUEUED, null, "working"],
    [AgentOperationItemStatus.QUEUED, "TRANSIENT_WORKER_FAILURE", "waiting_for_retry"],
    [AgentOperationItemStatus.QUEUED, "STALE_WORKER_RECOVERY", "waiting_for_retry"],
    [AgentOperationItemStatus.NEEDS_REVIEW, null, "needs_action"],
    [AgentOperationItemStatus.FAILED, "TIMEOUT", "needs_action"],
    [AgentOperationItemStatus.ACTIVE, null, "ready"],
    [AgentOperationItemStatus.CANCELED, null, "stopped"],
  ] as const)("maps %s with %s to %s", (status, errorCode, expected) => {
    expect(getAgentItemProgressState({ status, errorCode })).toBe(expected);
  });
});

describe("reduceAgentOperationStatus", () => {
  it.each([
    [[AgentOperationItemStatus.ACTIVE], AgentOperationStatus.SUCCEEDED],
    [[AgentOperationItemStatus.REUSED], AgentOperationStatus.SUCCEEDED],
    [[AgentOperationItemStatus.FAILED], AgentOperationStatus.FAILED],
    [[AgentOperationItemStatus.CANCELED], AgentOperationStatus.CANCELED],
    [[AgentOperationItemStatus.ACTIVE, AgentOperationItemStatus.FAILED], AgentOperationStatus.PARTIAL],
    [[AgentOperationItemStatus.QUEUED], AgentOperationStatus.QUEUED],
    [[AgentOperationItemStatus.PLANNING], AgentOperationStatus.PLANNING],
    [[AgentOperationItemStatus.GENERATING], AgentOperationStatus.GENERATING],
    [[AgentOperationItemStatus.VERIFYING], AgentOperationStatus.VERIFYING],
    [[AgentOperationItemStatus.ACTIVATING], AgentOperationStatus.ACTIVATING],
    [[AgentOperationItemStatus.NEEDS_INPUT], AgentOperationStatus.NEEDS_INPUT],
    [[AgentOperationItemStatus.NEEDS_REVIEW], AgentOperationStatus.NEEDS_REVIEW],
  ])("reduces %j to %s", (items, expected) => {
    expect(reduceAgentOperationStatus(items)).toBe(expected);
  });

  it("prioritizes user review over background work", () => {
    expect(
      reduceAgentOperationStatus([
        AgentOperationItemStatus.NEEDS_REVIEW,
        AgentOperationItemStatus.GENERATING,
      ]),
    ).toBe(AgentOperationStatus.NEEDS_REVIEW);
  });
});

describe("isRetryableAgentItemError", () => {
  it("accepts canonical and legacy punctuation for recoverable activation failures", () => {
    expect(isRetryableAgentItemError("SKILL_NOT_DRAFT")).toBe(true);
    expect(isRetryableAgentItemError("SKILL-NOT-DRAFT")).toBe(true);
    expect(isRetryableAgentItemError("STALE_WORKER_RECOVERY")).toBe(true);
    expect(isRetryableAgentItemError("TRANSIENT_WORKER_FAILURE")).toBe(true);
  });

  it("does not retry unknown failures", () => {
    expect(isRetryableAgentItemError("UNSAFE_UNKNOWN_FAILURE")).toBe(false);
  });
});
