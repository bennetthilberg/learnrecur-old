import "server-only";

import type { z } from "zod";
import { JOB_PAYLOAD_SCHEMAS } from "./contracts";

import { publishJob } from "./publisher";

export const CHOICE_REFILL_REQUESTED_EVENT = "learnrecur/choice-refill.requested";
export const EXACT_INPUT_REFILL_REQUESTED_EVENT = "learnrecur/exact-input-refill.requested";
export const MATH_REFILL_REQUESTED_EVENT = "learnrecur/math-refill.requested";
export const SOURCE_UPLOAD_DRAFT_REQUESTED_EVENT =
  "learnrecur/source-upload-draft.requested";
export const MATERIAL_INGESTION_REQUESTED_EVENT = "learnrecur/material-ingestion.requested";
export const MATERIAL_CLEANUP_REQUESTED_EVENT = "learnrecur/material-cleanup.requested";
export const MATERIAL_DRAFT_ITEM_REQUESTED_EVENT = "learnrecur/material-draft-item.requested";
export const MATERIAL_BATCH_ACTIVATION_REQUESTED_EVENT =
  "learnrecur/material-batch-activation.requested";
export const AGENT_SKILL_OPERATION_REQUESTED_EVENT =
  "learnrecur/agent-skill-operation.requested";
export const AGENT_CONNECTION_REVOCATION_REQUESTED_EVENT =
  "learnrecur/agent-connection-revocation.requested";
export const ACCOUNT_DELETION_REQUESTED_EVENT = "learnrecur/account-deletion.requested";

const refillEventPayloadSchema = JOB_PAYLOAD_SCHEMAS[CHOICE_REFILL_REQUESTED_EVENT];

const sourceUploadDraftEventPayloadSchema = JOB_PAYLOAD_SCHEMAS[SOURCE_UPLOAD_DRAFT_REQUESTED_EVENT];

const materialIngestionEventPayloadSchema = JOB_PAYLOAD_SCHEMAS[MATERIAL_INGESTION_REQUESTED_EVENT];

const materialCleanupEventPayloadSchema = JOB_PAYLOAD_SCHEMAS[MATERIAL_CLEANUP_REQUESTED_EVENT];

const materialDraftItemEventPayloadSchema = JOB_PAYLOAD_SCHEMAS[MATERIAL_DRAFT_ITEM_REQUESTED_EVENT];

const materialBatchActivationEventPayloadSchema = JOB_PAYLOAD_SCHEMAS[MATERIAL_BATCH_ACTIVATION_REQUESTED_EVENT];

const agentSkillOperationEventPayloadSchema = JOB_PAYLOAD_SCHEMAS[AGENT_SKILL_OPERATION_REQUESTED_EVENT];

const agentConnectionRevocationEventPayloadSchema = JOB_PAYLOAD_SCHEMAS[AGENT_CONNECTION_REVOCATION_REQUESTED_EVENT];

const accountDeletionEventPayloadSchema = JOB_PAYLOAD_SCHEMAS[ACCOUNT_DELETION_REQUESTED_EVENT];

export type ExerciseRefillEventPayload = z.infer<typeof refillEventPayloadSchema>;
export type SourceUploadDraftEventPayload = z.infer<
  typeof sourceUploadDraftEventPayloadSchema
>;
export type MaterialIngestionEventPayload = z.infer<
  typeof materialIngestionEventPayloadSchema
>;
export type MaterialCleanupEventPayload = z.infer<typeof materialCleanupEventPayloadSchema>;
export type MaterialDraftItemEventPayload = z.infer<typeof materialDraftItemEventPayloadSchema>;
export type MaterialBatchActivationEventPayload = z.infer<
  typeof materialBatchActivationEventPayloadSchema
>;
export type AgentSkillOperationEventPayload = z.infer<
  typeof agentSkillOperationEventPayloadSchema
>;
export type AgentConnectionRevocationEventPayload = z.infer<
  typeof agentConnectionRevocationEventPayloadSchema
>;
export type AccountDeletionEventPayload = z.infer<typeof accountDeletionEventPayloadSchema>;

export type JobPublicationOptions = { eventId?: string; signal?: AbortSignal };
export type ExerciseRefillEventSender = {
  sendChoiceRefillRequested(payload: ExerciseRefillEventPayload, options?: JobPublicationOptions): Promise<void>;
  sendExactInputRefillRequested(payload: ExerciseRefillEventPayload, options?: JobPublicationOptions): Promise<void>;
  sendMathRefillRequested(payload: ExerciseRefillEventPayload, options?: JobPublicationOptions): Promise<void>;
};

export type SourceUploadDraftEventSender = {
  sendSourceUploadDraftRequested(payload: SourceUploadDraftEventPayload): Promise<void>;
};

export type MaterialIngestionEventSender = {
  sendMaterialIngestionRequested(payload: MaterialIngestionEventPayload, options?: JobPublicationOptions): Promise<void>;
};

export type MaterialCleanupEventSender = {
  sendMaterialCleanupRequested(payload: MaterialCleanupEventPayload): Promise<void>;
};

export type MaterialDraftItemEventSender = {
  sendMaterialDraftItemRequested(payload: MaterialDraftItemEventPayload): Promise<void>;
};

export type MaterialBatchActivationEventSender = {
  sendMaterialBatchActivationRequested(
    payload: MaterialBatchActivationEventPayload,
  ): Promise<void>;
};

export type AccountDeletionEventSender = {
  sendAccountDeletionRequested(payload: AccountDeletionEventPayload): Promise<void>;
};

export function parseExerciseRefillEventPayload(input: unknown): ExerciseRefillEventPayload {
  return refillEventPayloadSchema.parse(input);
}

export function parseSourceUploadDraftEventPayload(
  input: unknown,
): SourceUploadDraftEventPayload {
  return sourceUploadDraftEventPayloadSchema.parse(input);
}

export function parseMaterialIngestionEventPayload(
  input: unknown,
): MaterialIngestionEventPayload {
  return materialIngestionEventPayloadSchema.parse(input);
}

export function parseMaterialCleanupEventPayload(input: unknown): MaterialCleanupEventPayload {
  return materialCleanupEventPayloadSchema.parse(input);
}

export function parseMaterialDraftItemEventPayload(input: unknown): MaterialDraftItemEventPayload {
  return materialDraftItemEventPayloadSchema.parse(input);
}

export function parseMaterialBatchActivationEventPayload(
  input: unknown,
): MaterialBatchActivationEventPayload {
  return materialBatchActivationEventPayloadSchema.parse(input);
}

export function parseAgentSkillOperationEventPayload(
  input: unknown,
): AgentSkillOperationEventPayload {
  return agentSkillOperationEventPayloadSchema.parse(input);
}

export function parseAgentConnectionRevocationEventPayload(
  input: unknown,
): AgentConnectionRevocationEventPayload {
  return agentConnectionRevocationEventPayloadSchema.parse(input);
}

export function parseAccountDeletionEventPayload(input: unknown): AccountDeletionEventPayload {
  return accountDeletionEventPayloadSchema.parse(input);
}

export const awsExerciseRefillEventSender: ExerciseRefillEventSender = {
  async sendChoiceRefillRequested(payload, options) {
    await publishJob(CHOICE_REFILL_REQUESTED_EVENT, payload, { id: options?.eventId, signal: options?.signal });
  },
  async sendExactInputRefillRequested(payload, options) {
    await publishJob(EXACT_INPUT_REFILL_REQUESTED_EVENT, payload, { id: options?.eventId, signal: options?.signal });
  },
  async sendMathRefillRequested(payload, options) {
    await publishJob(MATH_REFILL_REQUESTED_EVENT, payload, { id: options?.eventId, signal: options?.signal });
  },
};

export const awsSourceUploadDraftEventSender: SourceUploadDraftEventSender = {
  async sendSourceUploadDraftRequested(payload) {
    await publishJob(SOURCE_UPLOAD_DRAFT_REQUESTED_EVENT, payload);
  },
};

export const awsMaterialIngestionEventSender: MaterialIngestionEventSender = {
  async sendMaterialIngestionRequested(payload, options) {
    await publishJob(MATERIAL_INGESTION_REQUESTED_EVENT, payload, { id: options?.eventId, signal: options?.signal });
  },
};

export const awsMaterialCleanupEventSender: MaterialCleanupEventSender = {
  async sendMaterialCleanupRequested(payload) {
    await publishJob(MATERIAL_CLEANUP_REQUESTED_EVENT, payload);
  },
};

export const awsMaterialDraftItemEventSender: MaterialDraftItemEventSender = {
  async sendMaterialDraftItemRequested(payload) {
    await publishJob(MATERIAL_DRAFT_ITEM_REQUESTED_EVENT, payload);
  },
};

export const awsMaterialBatchActivationEventSender: MaterialBatchActivationEventSender = {
  async sendMaterialBatchActivationRequested(payload) {
    await publishJob(MATERIAL_BATCH_ACTIVATION_REQUESTED_EVENT, payload);
  },
};

export async function sendAgentSkillOperationRequested(
  payload: AgentSkillOperationEventPayload,
  options?: { eventId?: string; signal?: AbortSignal },
) {
  await publishJob(AGENT_SKILL_OPERATION_REQUESTED_EVENT, payload, {
    ...(options?.eventId ? { id: options.eventId } : {}),
    ...(options?.signal ? { signal: options.signal } : {}),
  });
}

export async function sendAgentConnectionRevocationRequested(
  payload: AgentConnectionRevocationEventPayload,
) {
  await publishJob(AGENT_CONNECTION_REVOCATION_REQUESTED_EVENT, payload);
}

export const awsAccountDeletionEventSender: AccountDeletionEventSender = {
  async sendAccountDeletionRequested(payload) {
    await publishJob(ACCOUNT_DELETION_REQUESTED_EVENT, payload);
  },
};
