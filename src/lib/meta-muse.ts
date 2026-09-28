import { JobStageTimeoutError } from "@/lib/jobs/deadline";

export const DEFAULT_META_MUSE_BASE_URL = "https://api.meta.ai/v1";
export const DEFAULT_META_MUSE_MODEL = "muse-spark-1.3";
export const DEFAULT_META_MUSE_REQUEST_TIMEOUT_MS = 60_000;
export const MAX_META_MUSE_INLINE_FILE_BYTES = 50_000_000;

export type MetaMuseFallbackConfig = {
  apiKey: string;
  baseUrl: string;
  model: string;
};

export type MetaMuseInputTextPart = {
  type: "input_text";
  text: string;
};

export type MetaMuseInputImagePart = {
  type: "input_image";
  image_url: string;
  detail: "high";
};

export type MetaMuseInputFilePart = {
  type: "input_file";
  filename: string;
  file_data: string;
  detail: "high";
};

export type MetaMuseInputContentPart =
  | MetaMuseInputTextPart
  | MetaMuseInputImagePart
  | MetaMuseInputFilePart;

export type MetaMuseContentPart = MetaMuseInputContentPart;

export type MetaMuseChatMessage = {
  role: "system" | "user" | "assistant";
  content: string | MetaMuseInputContentPart[];
};

type MetaMuseJsonResponseInput = {
  apiKey: string;
  baseUrl: string;
  instructions?: string;
  messages?: MetaMuseChatMessage[];
  metadata?: Record<string, unknown>;
  model: string;
  operation: string;
  responseJsonSchema?: Record<string, unknown>;
  responseJsonSchemaName?: string;
  signal?: AbortSignal;
  timeoutMs?: number;
  userContent?: string | MetaMuseInputContentPart[];
};

export async function runMetaMuseJsonResponse({
  apiKey,
  baseUrl,
  instructions,
  messages,
  metadata,
  model,
  operation,
  responseJsonSchema,
  responseJsonSchemaName,
  signal,
  timeoutMs,
  userContent,
}: MetaMuseJsonResponseInput): Promise<unknown> {
  const normalizedInput = normalizeMetaMuseInput({ instructions, messages, userContent });
  const endpoint = `${baseUrl.replace(/\/+$/, "")}/responses`;
  const requestId = buildMetaMuseRequestId();
  const startedAt = Date.now();
  const requestTimeoutMs = normalizeRequestTimeoutMs(timeoutMs);
  const abortController = new AbortController();
  const abortFromCaller = () => {
    const reason = signal?.reason;
    abortController.abort(reason instanceof Error ? reason : new Error(`${operation} was canceled.`));
  };
  if (signal?.aborted) abortFromCaller();
  else signal?.addEventListener("abort", abortFromCaller, { once: true });
  const timeoutId = setTimeout(() => {
    abortController.abort(
      new JobStageTimeoutError(`${operation} timed out after ${requestTimeoutMs}ms with Meta Muse.`, "meta request"),
    );
  }, requestTimeoutMs);
  const context = {
    requestId,
    operation,
    provider: "meta",
    apiMode: "responses",
    endpoint,
    model,
    ...metadata,
  };

  console.info("[ai] meta muse request started", context);

  try {
    const requestBody = JSON.stringify({
      model,
      store: false,
      instructions: normalizedInput.instructions,
      input: [
        {
          role: "user",
          content:
            typeof normalizedInput.userContent === "string"
              ? [{ type: "input_text", text: normalizedInput.userContent }]
              : normalizedInput.userContent,
        },
      ],
      reasoning: { effort: "minimal" },
      max_output_tokens: 8_192,
      text: responseJsonSchema
        ? {
            format: {
              type: "json_schema",
              name: sanitizeSchemaName(responseJsonSchemaName),
              strict: true,
              schema: responseJsonSchema,
            },
          }
        : { format: { type: "json_object" } },
    });
    let body: unknown = null;
    let response: Response | null = null;
    let attempt = 0;
    while (attempt < 2) {
      attempt += 1;
      try {
        response = await fetch(endpoint, {
          method: "POST",
          headers: {
            Authorization: `Bearer ${apiKey}`,
            "Content-Type": "application/json",
          },
          body: requestBody,
          signal: abortController.signal,
        });
      } catch (error) {
        if (attempt < 2 && !abortController.signal.aborted) {
          console.warn("[ai] retrying transient meta muse transport failure", {
            ...context,
            attempt,
            error: getMetaMuseErrorLogDetails(error),
          });
          await waitForMetaMuseTransportRetry(attempt, abortController.signal);
          continue;
        }
        throw error;
      }
      body = parseMaybeJson(await response.text());
      if (response.ok) {
        break;
      }
      if (attempt < 2 && isRetryableMetaMuseStatus(response.status)) {
        console.warn("[ai] retrying transient meta muse request", {
          ...context,
          attempt,
          statusCode: response.status,
        });
        await waitForMetaMuseRetry(response, attempt, abortController.signal);
        continue;
      }
      throw buildMetaMuseHttpError(response, body, operation);
    }
    if (!response?.ok) {
      throw new Error(`${operation} failed with Meta Muse.`);
    }
    const bodyRecord = toRecord(body);
    const responseStatus = readString(bodyRecord?.status);
    if (responseStatus && responseStatus !== "completed") {
      throw buildMetaMuseResponseError(bodyRecord ?? {}, operation);
    }
    const outputText = readMetaMuseOutputText(body);
    if (!outputText) {
      throw new Error("Meta Muse returned no text.");
    }

    let value: unknown;
    try {
      value = JSON.parse(outputText) as unknown;
    } catch {
      throw new Error("Meta Muse returned invalid JSON.");
    }

    console.info("[ai] meta muse request succeeded", {
      ...context,
      elapsedMs: Date.now() - startedAt,
      outputTextLength: outputText.length,
      responseModel: readString(bodyRecord?.model),
      responseStatus: responseStatus ?? null,
      usageMetadata: summarizeUsage(bodyRecord?.usage),
    });
    return value;
  } catch (error) {
    const loggedError =
      abortController.signal.aborted && abortController.signal.reason instanceof Error
        ? abortController.signal.reason
        : error;
    console.error("[ai] meta muse request failed", {
      ...context,
      elapsedMs: Date.now() - startedAt,
      error: getMetaMuseErrorLogDetails(loggedError),
    });
    throw loggedError;
  } finally {
    clearTimeout(timeoutId);
    signal?.removeEventListener("abort", abortFromCaller);
  }
}

async function waitForMetaMuseTransportRetry(attempt: number, signal: AbortSignal): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const timeoutId = setTimeout(resolve, 100 * attempt);
    signal.addEventListener(
      "abort",
      () => {
        clearTimeout(timeoutId);
        reject(signal.reason);
      },
      { once: true },
    );
  });
}

function isRetryableMetaMuseStatus(status: number): boolean {
  return status === 429 || status === 500 || status === 503;
}

async function waitForMetaMuseRetry(
  response: Response,
  attempt: number,
  signal: AbortSignal,
): Promise<void> {
  const retryAfterSeconds = Number(response.headers.get("retry-after"));
  const delayMs = Number.isFinite(retryAfterSeconds) && retryAfterSeconds > 0
    ? Math.min(2_000, retryAfterSeconds * 1_000)
    : 100 * attempt;
  await new Promise<void>((resolve, reject) => {
    const timeoutId = setTimeout(resolve, delayMs);
    signal.addEventListener(
      "abort",
      () => {
        clearTimeout(timeoutId);
        reject(signal.reason);
      },
      { once: true },
    );
  });
}

function normalizeMetaMuseInput(input: {
  instructions?: string;
  messages?: MetaMuseChatMessage[];
  userContent?: string | MetaMuseInputContentPart[];
}) {
  if (input.instructions && input.userContent) {
    return { instructions: input.instructions, userContent: input.userContent };
  }
  const messages = input.messages ?? [];
  const instructions = messages
    .filter((message) => message.role === "system")
    .flatMap((message) => (typeof message.content === "string" ? [message.content] : []))
    .join("\n")
    .trim();
  const userMessage = [...messages].reverse().find((message) => message.role === "user");
  if (!instructions || !userMessage) {
    throw new Error("Meta Muse requests require instructions and user content.");
  }
  return { instructions, userContent: userMessage.content };
}

export function buildMetaMuseDataUrl(bytes: Buffer, mimeType: string): string {
  return `data:${mimeType};base64,${bytes.toString("base64")}`;
}

function buildMetaMuseRequestId(): string {
  return `meta_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`;
}

function normalizeRequestTimeoutMs(value: number | undefined): number {
  return typeof value === "number" && Number.isFinite(value) && value > 0
    ? value
    : DEFAULT_META_MUSE_REQUEST_TIMEOUT_MS;
}

function sanitizeSchemaName(value: string | undefined): string {
  const sanitized = (value ?? "learnrecur_json_response")
    .replace(/[^a-zA-Z0-9_-]/g, "_")
    .slice(0, 64);
  return sanitized || "learnrecur_json_response";
}

function buildMetaMuseHttpError(response: Response, body: unknown, operation: string): Error {
  const errorRecord = toRecord(toRecord(body)?.error);
  return normalizedMetaMuseError({
    code: response.status,
    status:
      readString(errorRecord?.code) ??
      readString(errorRecord?.type) ??
      response.statusText,
    message: readString(errorRecord?.message) ?? `${operation} failed with Meta Muse.`,
  });
}

function buildMetaMuseResponseError(
  body: Record<string, unknown>,
  operation: string,
): Error {
  const errorRecord = toRecord(body.error);
  const incomplete = toRecord(body.incomplete_details);
  return normalizedMetaMuseError({
    code: null,
    status: readString(body.status) ?? readString(errorRecord?.type) ?? "FAILED",
    message:
      readString(errorRecord?.message) ??
      readString(incomplete?.reason) ??
      `${operation} did not complete with Meta Muse.`,
  });
}

function normalizedMetaMuseError(input: {
  code: number | null;
  status: string;
  message: string;
}) {
  return new Error(JSON.stringify({ error: input }));
}

function readMetaMuseOutputText(value: unknown): string | null {
  const outputValue = toRecord(value)?.output;
  const output: unknown[] = Array.isArray(outputValue) ? outputValue : [];
  const text = output
    .flatMap((item) => {
      const content = toRecord(item)?.content;
      return Array.isArray(content) ? content : [];
    })
    .flatMap((part) => {
      const record = toRecord(part);
      return record?.type === "output_text" && typeof record.text === "string"
        ? [record.text]
        : [];
    })
    .join("\n")
    .trim();
  return text || null;
}

function summarizeUsage(value: unknown) {
  const usage = toRecord(value);
  if (!usage) {
    return null;
  }
  return {
    inputTokens: readNumber(usage.input_tokens),
    outputTokens: readNumber(usage.output_tokens),
    totalTokens: readNumber(usage.total_tokens),
  };
}

function getMetaMuseErrorLogDetails(error: unknown) {
  const record = parseJsonRecord(error instanceof Error ? error.message : null);
  const nested = toRecord(record?.error) ?? record;
  return {
    code: readNumber(nested?.code),
    status: readString(nested?.status),
    message:
      readString(nested?.message) ??
      (error instanceof Error && error.message.trim() ? error.message : null),
  };
}

function parseMaybeJson(value: string): unknown {
  if (!value.trim()) {
    return null;
  }
  try {
    return JSON.parse(value) as unknown;
  } catch {
    return value;
  }
}

function parseJsonRecord(value: string | null): Record<string, unknown> | null {
  if (!value) {
    return null;
  }
  try {
    return toRecord(JSON.parse(value) as unknown);
  } catch {
    return null;
  }
}

function toRecord(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;
}

function readString(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

function readNumber(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}
