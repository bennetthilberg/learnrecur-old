import {
  GoogleGenAI,
  type GenerateContentResponse,
  type ThinkingLevel,
} from "@google/genai";
import { withAbortableTimeout } from "@/lib/jobs/deadline";
import { GENERATION_TIMEOUT_MS } from "@/lib/skills/activation-timing";

export const DEFAULT_GEMINI_MODEL = "gemini-3.8-flash";
export const DEFAULT_GEMINI_EMBEDDING_MODEL = "gemini-embedding-2";
export const DEFAULT_GEMINI_FALLBACK_MODELS = [] as const;
export const GEMINI_LOW_THINKING_CONFIG = Object.freeze({
  thinkingLevel: "LOW" as ThinkingLevel,
});

export type GeminiApiMode = "developer-api" | "enterprise-agent-platform";

export type GeminiRuntimeConfig = {
  apiMode: GeminiApiMode;
  endpoint: string;
  model: string;
  clientOptions: ConstructorParameters<typeof GoogleGenAI>[0];
};

export type GeminiRuntimeEnv = {
  GEMINI_API_KEY?: string;
  GEMINI_ENTERPRISE_AGENT_KEY_PLATFORM_KEY?: string;
  GEMINI_MODEL?: string;
};

type GeminiMediaLogInput = {
  count: number;
  totalBytes: number;
  mimeTypes: string[];
  sourceFileIds?: string[];
};

type GeminiOperationMetadata = {
  candidateCount?: number;
  media?: GeminiMediaLogInput;
  promptChars?: number;
  requestedCount?: number;
  schemaName?: string;
};

type GeminiOperationInput<T> = {
  config: GeminiRuntimeConfig;
  metadata?: GeminiOperationMetadata;
  operation: string;
  signal?: AbortSignal;
  timeoutMs?: number;
  run: (ai: GoogleGenAI, signal: AbortSignal) => Promise<{
    response: GenerateContentResponse;
    value: T;
  }>;
};

type GeminiErrorDetails = {
  code: number | null;
  status: string | null;
  message: string | null;
};

export function resolveGeminiRuntimeConfig(env: GeminiRuntimeEnv): GeminiRuntimeConfig {
  const model = env.GEMINI_MODEL?.trim() || DEFAULT_GEMINI_MODEL;
  const enterpriseApiKey = env.GEMINI_ENTERPRISE_AGENT_KEY_PLATFORM_KEY?.trim();

  if (enterpriseApiKey) {
    return {
      apiMode: "enterprise-agent-platform",
      endpoint: "https://aiplatform.googleapis.com/",
      model,
      clientOptions: {
        vertexai: true,
        apiKey: enterpriseApiKey,
        httpOptions: {
          apiVersion: "v1",
        },
      },
    };
  }

  const developerApiKey = env.GEMINI_API_KEY?.trim();

  if (!developerApiKey) {
    throw new Error("GEMINI_API_KEY or GEMINI_ENTERPRISE_AGENT_KEY_PLATFORM_KEY is required");
  }

  return {
    apiMode: "developer-api",
    endpoint: "https://generativelanguage.googleapis.com/",
    model,
    clientOptions: {
      apiKey: developerApiKey,
    },
  };
}

export function getGeminiRuntimeLogContext(config: GeminiRuntimeConfig) {
  return {
    provider: "google",
    apiMode: config.apiMode,
    endpoint: config.endpoint,
    model: config.model,
  };
}

export async function runLoggedGeminiOperation<T>({
  config,
  metadata,
  operation,
  run,
  signal,
  timeoutMs,
}: GeminiOperationInput<T>): Promise<T> {
  const requestId = buildGeminiRequestId();
  const startedAt = Date.now();
  const context = {
    requestId,
    operation,
    ...getGeminiRuntimeLogContext(config),
    ...metadata,
  };

  console.info("[ai] gemini request started", context);

  try {
    const result = await withAbortableTimeout({
      run: (requestSignal) => run(new GoogleGenAI(config.clientOptions), requestSignal),
      timeoutMs: timeoutMs ?? GENERATION_TIMEOUT_MS,
      message: `${operation} timed out with Gemini.`,
      stage: `gemini ${operation}`,
      parentSignal: signal,
    });
    const text = result.response.text ?? "";

    console.info("[ai] gemini request succeeded", {
      ...context,
      elapsedMs: Date.now() - startedAt,
      finishReason: result.response.candidates?.[0]?.finishReason ?? null,
      outputTextLength: text.length,
      usageMetadata: summarizeGeminiUsageMetadata(result.response.usageMetadata),
    });

    return result.value;
  } catch (error) {
    console.error("[ai] gemini request failed", {
      ...context,
      elapsedMs: Date.now() - startedAt,
      error: getGeminiErrorLogDetails(error),
    });
    throw error;
  }
}

function buildGeminiRequestId(): string {
  return `gemini_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`;
}

function summarizeGeminiUsageMetadata(
  usageMetadata: GenerateContentResponse["usageMetadata"] | undefined,
) {
  if (!usageMetadata) {
    return null;
  }

  return {
    promptTokenCount: usageMetadata.promptTokenCount ?? null,
    candidatesTokenCount: usageMetadata.candidatesTokenCount ?? null,
    thoughtsTokenCount: usageMetadata.thoughtsTokenCount ?? null,
    totalTokenCount: usageMetadata.totalTokenCount ?? null,
    trafficType: usageMetadata.trafficType ?? null,
  };
}

type GeminiProviderFallbackInput<T> = {
  signal?: AbortSignal;
  fallback?: {
    model: string;
    provider: string;
    run: () => Promise<T>;
  } | null;
  operation: string;
  primary?: ReturnType<typeof getGeminiRuntimeLogContext>;
  primaryModel: string;
  runPrimary: () => Promise<T>;
};

export function parseGeminiFallbackModels(value: unknown): string[] {
  if (value === undefined || value === null) {
    return [...DEFAULT_GEMINI_FALLBACK_MODELS];
  }

  if (typeof value === "string") {
    const models = value
      .split(",")
      .map((model) => model.trim())
      .filter(Boolean);

    return models.length ? models : [...DEFAULT_GEMINI_FALLBACK_MODELS];
  }

  if (Array.isArray(value)) {
    const models = value
      .filter((model): model is string => typeof model === "string")
      .map((model) => model.trim())
      .filter(Boolean);

    return models.length ? models : [...DEFAULT_GEMINI_FALLBACK_MODELS];
  }

  return [...DEFAULT_GEMINI_FALLBACK_MODELS];
}

export async function runWithGeminiProviderFallback<T>({
  fallback,
  operation,
  primary,
  primaryModel,
  runPrimary,
  signal,
}: GeminiProviderFallbackInput<T>): Promise<T> {
  try {
    return await runPrimary();
  } catch (error) {
    if (signal?.aborted) {
      throw signal.reason instanceof Error ? signal.reason : error;
    }
    if (!fallback) {
      throw error;
    }

    console.warn("[ai] retrying with fallback provider", {
      operation,
      failedProvider: "gemini",
      failedModel: primaryModel,
      primary,
      fallbackProvider: fallback.provider,
      fallbackModel: fallback.model,
      error: getGeminiErrorLogDetails(error),
    });

    if (signal?.aborted) {
      throw signal.reason instanceof Error ? signal.reason : error;
    }
    return fallback.run();
  }
}

export function isRetryableGeminiModelError(error: unknown): boolean {
  const details = getGeminiErrorDetails(error);

  return isRetryableGeminiErrorDetails(details);
}

export function getPublicGeminiFailureMessage(error: unknown): string {
  const details = getGeminiErrorDetails(error);
  const message = details.message?.toLowerCase() ?? "";

  if (
    details.code === 504 ||
    details.status === "DEADLINE_EXCEEDED" ||
    message.includes("timed out")
  ) {
    return "Creating the skill took too long. Try again with a smaller file or a shorter excerpt.";
  }

  if (
    isRetryableGeminiErrorDetails(details) ||
    message.includes("high demand") ||
    message.includes("temporarily overloaded") ||
    message.includes("temporarily running out of capacity")
  ) {
    return "The AI service is busy right now, so LearnRecur could not finish creating this skill. Try again in a minute.";
  }

  return "LearnRecur could not create a skill from that source. Try again, or use a clearer excerpt.";
}

export function getPublicGeminiScopePlanningFailureMessage(error: unknown): string {
  const details = getGeminiErrorDetails(error);
  const message = details.message?.toLowerCase() ?? "";

  if (
    details.code === 504 ||
    details.status === "DEADLINE_EXCEEDED" ||
    message.includes("timed out")
  ) {
    return "Reviewing the scope took too long. Try a narrower request.";
  }

  if (
    isRetryableGeminiErrorDetails(details) ||
    message.includes("high demand") ||
    message.includes("temporarily overloaded") ||
    message.includes("temporarily running out of capacity")
  ) {
    return "The AI service is busy right now, so LearnRecur could not review the scope. Try again in a minute.";
  }

  return "LearnRecur could not review that scope. Check the request and try again.";
}

export function getGeminiErrorLogDetails(error: unknown) {
  const { code, status } = getGeminiErrorDetails(error);
  // Provider exception text may echo source material. Policy decisions use
  // the original details; logs expose only HTTP codes and known statuses.
  return {
    code: code !== null && Number.isInteger(code) && code >= 100 && code <= 599 ? code : null,
    status: status && [
      "CANCELLED", "INVALID_ARGUMENT", "DEADLINE_EXCEEDED", "NOT_FOUND",
      "ALREADY_EXISTS", "PERMISSION_DENIED", "RESOURCE_EXHAUSTED",
      "FAILED_PRECONDITION", "ABORTED", "OUT_OF_RANGE", "UNIMPLEMENTED",
      "INTERNAL", "UNAVAILABLE", "DATA_LOSS", "UNAUTHENTICATED", "UNKNOWN",
      "BAD_GATEWAY", "GATEWAY_TIMEOUT",
    ].includes(status) ? status : null,
  };
}

function getGeminiErrorDetails(error: unknown): GeminiErrorDetails {
  const records = collectErrorRecords(error);

  for (const record of records) {
    const nested = toRecord(record.error);
    const source = nested ?? record;
    const code = readNumber(source.code) ?? readNumber(source.statusCode) ?? readNumber(record.status);
    const status = readString(source.status) ?? readString(source.code);
    const message = readString(source.message) ?? readString(record.message);

    if (code || status || message) {
      return {
        code,
        status,
        message,
      };
    }
  }

  if (error instanceof Error && error.message.trim()) {
    return {
      code: null,
      status: null,
      message: error.message,
    };
  }

  if (typeof error === "string" && error.trim()) {
    return {
      code: null,
      status: null,
      message: error,
    };
  }

  return {
    code: null,
    status: null,
    message: null,
  };
}

function isRetryableGeminiErrorDetails(details: GeminiErrorDetails): boolean {
  const status = details.status?.toUpperCase() ?? null;
  const message = details.message?.toLowerCase() ?? "";

  return (
    details.code === 429 ||
    details.code === 500 ||
    details.code === 502 ||
    details.code === 503 ||
    details.code === 504 ||
    status === "DEADLINE_EXCEEDED" ||
    status === "INTERNAL" ||
    status === "RESOURCE_EXHAUSTED" ||
    status === "UNAVAILABLE" ||
    message.includes("timed out")
  );
}

function collectErrorRecords(error: unknown): Array<Record<string, unknown>> {
  const records: Array<Record<string, unknown>> = [];
  const message = error instanceof Error ? error.message : typeof error === "string" ? error : null;
  const parsedMessage = parseJsonRecord(message);

  if (parsedMessage) {
    records.push(parsedMessage);
  }

  const direct = toRecord(error);

  if (direct) {
    records.push(direct);
  }

  return records;
}

function parseJsonRecord(value: string | null): Record<string, unknown> | null {
  if (!value) {
    return null;
  }

  try {
    return toRecord(JSON.parse(value));
  } catch {
    return null;
  }
}

function toRecord(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }

  return value as Record<string, unknown>;
}

function readNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }

  if (typeof value === "string" && /^\d+$/.test(value)) {
    return Number(value);
  }

  return null;
}

function readString(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value.trim() : null;
}
