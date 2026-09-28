/**
 * Keep transport timing in one place. Lambda has a ten-minute hard limit;
 * ordinary jobs stop at six minutes and agent skill operations stop at eight.
 * The delivery lease outlives Lambda. SQS starts with a longer visibility
 * window for infrastructure retries; a claimed message gets a shorter window
 * so a killed invocation releases its lane shortly after its lease expires.
 */
export const JOB_TIMEOUT_SECONDS = 600;
export const JOB_LEASE_SECONDS = JOB_TIMEOUT_SECONDS + 60;
export const JOB_SOFT_DEADLINE_SECONDS = 360;
export const JOB_SOFT_DEADLINE_MS = JOB_SOFT_DEADLINE_SECONDS * 1_000;
export const AGENT_SKILL_OPERATION_SOFT_DEADLINE_SECONDS = 480;
export const AGENT_SKILL_OPERATION_SOFT_DEADLINE_MS = AGENT_SKILL_OPERATION_SOFT_DEADLINE_SECONDS * 1_000;
export const SQS_VISIBILITY_TIMEOUT_SECONDS = 3_600;
// After a durable claim, a killed invocation should release its FIFO lane
// shortly after the lease expires. Keep a margin for clocks and transport.
export const CLAIMED_JOB_VISIBILITY_SECONDS = JOB_LEASE_SECONDS + 60;

export function getJobSoftDeadlineMs(jobName: string): number {
  return jobName === "learnrecur/agent-skill-operation.requested"
    ? AGENT_SKILL_OPERATION_SOFT_DEADLINE_MS
    : JOB_SOFT_DEADLINE_MS;
}

export function hasValidJobTiming(): boolean {
  return (
    JOB_LEASE_SECONDS > JOB_TIMEOUT_SECONDS &&
    JOB_SOFT_DEADLINE_SECONDS < JOB_TIMEOUT_SECONDS &&
    AGENT_SKILL_OPERATION_SOFT_DEADLINE_SECONDS < JOB_TIMEOUT_SECONDS &&
    SQS_VISIBILITY_TIMEOUT_SECONDS > JOB_TIMEOUT_SECONDS &&
    CLAIMED_JOB_VISIBILITY_SECONDS > JOB_LEASE_SECONDS
  );
}
