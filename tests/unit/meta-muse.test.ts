import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import {
  DEFAULT_META_MUSE_BASE_URL,
  DEFAULT_META_MUSE_MODEL,
  buildMetaMuseDataUrl,
  runMetaMuseJsonResponse,
} from "@/lib/meta-muse";
import {
  resolveMetaMuseFallbackConfig,
  resolveOptionalMetaMuseFallbackConfig,
} from "@/lib/meta-muse-fallback";

const originalEnv = process.env;

describe("Meta Muse Responses client", () => {
  beforeEach(() => {
    vi.spyOn(console, "info").mockImplementation(() => {});
    vi.spyOn(console, "error").mockImplementation(() => {});
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
    process.env = originalEnv;
  });

  it("sends private PDF evidence with strict structured output", async () => {
    const fetchMock = vi.fn<typeof fetch>(async () =>
      new Response(
        JSON.stringify({
          id: "resp_test",
          model: DEFAULT_META_MUSE_MODEL,
          status: "completed",
          output: [
            {
              type: "message",
              role: "assistant",
              content: [
                {
                  type: "output_text",
                  text: JSON.stringify({ drafts: [{ title: "Ser vs. estar" }] }),
                },
              ],
            },
          ],
          usage: { input_tokens: 20, output_tokens: 10, total_tokens: 30 },
        }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      ),
    );
    vi.stubGlobal("fetch", fetchMock);
    const schema = {
      type: "object",
      additionalProperties: false,
      required: ["drafts"],
      properties: { drafts: { type: "array", items: { type: "object" } } },
    };

    await expect(
      runMetaMuseJsonResponse({
        apiKey: "LLM|123|secret",
        baseUrl: DEFAULT_META_MUSE_BASE_URL,
        model: DEFAULT_META_MUSE_MODEL,
        operation: "skill draft generation",
        instructions: "Create a grounded draft.",
        responseJsonSchema: schema,
        responseJsonSchemaName: "skillDraft",
        userContent: [
          { type: "input_text", text: "Use the attached pages." },
          {
            type: "input_file",
            filename: "chapter.pdf",
            file_data: buildMetaMuseDataUrl(Buffer.from("%PDF"), "application/pdf"),
            detail: "high",
          },
        ],
      }),
    ).resolves.toEqual({ drafts: [{ title: "Ser vs. estar" }] });

    expect(fetchMock).toHaveBeenCalledWith(
      `${DEFAULT_META_MUSE_BASE_URL}/responses`,
      expect.objectContaining({
        method: "POST",
        headers: expect.objectContaining({ Authorization: "Bearer LLM|123|secret" }),
        signal: expect.any(AbortSignal),
      }),
    );
    const body = JSON.parse(String(fetchMock.mock.calls[0][1]?.body));
    expect(body).toMatchObject({
      model: DEFAULT_META_MUSE_MODEL,
      store: false,
      instructions: "Create a grounded draft.",
      reasoning: { effort: "minimal" },
      input: [
        {
          role: "user",
          content: expect.arrayContaining([
            expect.objectContaining({
              type: "input_file",
              filename: "chapter.pdf",
              detail: "high",
            }),
          ]),
        },
      ],
      text: {
        format: {
          type: "json_schema",
          name: "skillDraft",
          strict: true,
          schema,
        },
      },
    });
  });

  it("normalizes API errors for the shared provider fallback handler", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        new Response(
          JSON.stringify({
            error: { type: "rate_limit_error", code: "rate_limited", message: "Try later." },
          }),
          { status: 429, statusText: "Too Many Requests" },
        ),
      ),
    );

    await expect(
      runMetaMuseJsonResponse({
        apiKey: "LLM|123|secret",
        baseUrl: DEFAULT_META_MUSE_BASE_URL,
        model: DEFAULT_META_MUSE_MODEL,
        operation: "skill draft generation",
        instructions: "Return JSON.",
        userContent: "Return JSON.",
      }),
    ).rejects.toThrow(/Try later/);
  });

  it("retries one transient Meta failure before returning the structured result", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            error: { type: "server_error", code: "unavailable", message: "Try again." },
          }),
          { status: 503, statusText: "Service Unavailable" },
        ),
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            status: "completed",
            output: [
              {
                type: "message",
                role: "assistant",
                content: [{ type: "output_text", text: JSON.stringify({ ok: true }) }],
              },
            ],
          }),
          { status: 200 },
        ),
      );
    vi.stubGlobal("fetch", fetchMock);

    await expect(
      runMetaMuseJsonResponse({
        apiKey: "LLM|123|secret",
        baseUrl: DEFAULT_META_MUSE_BASE_URL,
        model: DEFAULT_META_MUSE_MODEL,
        operation: "exercise generation",
        instructions: "Return JSON.",
        userContent: "Return JSON.",
      }),
    ).resolves.toEqual({ ok: true });
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("retries one network-level transport failure", async () => {
    const fetchMock = vi
      .fn()
      .mockRejectedValueOnce(new TypeError("fetch failed"))
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            status: "completed",
            output: [
              {
                type: "message",
                role: "assistant",
                content: [{ type: "output_text", text: JSON.stringify({ ok: true }) }],
              },
            ],
          }),
          { status: 200 },
        ),
      );
    vi.stubGlobal("fetch", fetchMock);

    await expect(
      runMetaMuseJsonResponse({
        apiKey: "LLM|123|secret",
        baseUrl: DEFAULT_META_MUSE_BASE_URL,
        model: DEFAULT_META_MUSE_MODEL,
        operation: "exercise verification",
        instructions: "Return JSON.",
        userContent: "Return JSON.",
      }),
    ).resolves.toEqual({ ok: true });
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("aborts stalled requests after the configured timeout", async () => {
    vi.useFakeTimers();
    vi.stubGlobal(
      "fetch",
      vi.fn((_endpoint: string, init?: RequestInit) =>
        new Promise<Response>((_resolve, reject) => {
          const signal = init?.signal;
          if (!(signal instanceof AbortSignal)) {
            reject(new Error("Expected an AbortSignal."));
            return;
          }
          signal.addEventListener("abort", () => reject(signal.reason));
        }),
      ),
    );

    const result = runMetaMuseJsonResponse({
      apiKey: "LLM|123|secret",
      baseUrl: DEFAULT_META_MUSE_BASE_URL,
      model: DEFAULT_META_MUSE_MODEL,
      operation: "skill draft generation",
      instructions: "Return JSON.",
      userContent: "Return JSON.",
      timeoutMs: 25,
    });
    const expectation = expect(result).rejects.toThrow(
      "skill draft generation timed out after 25ms with Meta Muse.",
    );
    await vi.advanceTimersByTimeAsync(25);
    await expectation;
    await expect(result).rejects.toMatchObject({ retryable: true });
  });

  it("builds data URLs for image and PDF inputs", () => {
    expect(buildMetaMuseDataUrl(Buffer.from("image"), "image/png")).toBe(
      "data:image/png;base64,aW1hZ2U=",
    );
    expect(buildMetaMuseDataUrl(Buffer.from("%PDF"), "application/pdf")).toBe(
      "data:application/pdf;base64,JVBERg==",
    );
  });

  it("disables optional fallback when META_API_KEY is absent", () => {
    process.env = {
      ...originalEnv,
      META_API_KEY: "",
      META_MUSE_BASE_URL: "",
      META_MUSE_MODEL: "",
    };

    expect(resolveMetaMuseFallbackConfig()).toBeNull();
    expect(resolveOptionalMetaMuseFallbackConfig()).toEqual({
      status: "ready",
      config: null,
    });
  });

  it("reports invalid optional fallback configuration without throwing", () => {
    process.env = {
      ...originalEnv,
      META_API_KEY: "LLM_opaque_meta_key",
      META_MUSE_BASE_URL: "not-a-url",
      META_MUSE_MODEL: DEFAULT_META_MUSE_MODEL,
    };

    expect(resolveOptionalMetaMuseFallbackConfig()).toMatchObject({ status: "invalid" });
  });
});
