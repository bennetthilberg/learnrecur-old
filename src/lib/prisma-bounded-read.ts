import "server-only";

import type { Prisma } from "@/generated/prisma/client";
import { getPrisma } from "@/lib/prisma";

export class BoundedActivationReadError extends Error {
  readonly retryable = true;
  constructor(cause: unknown) {
    super("Activation evidence could not be read from the database.", { cause });
    this.name = "BoundedActivationReadError";
  }
}

/** Server-side cancellation keeps a timed-out read from holding the worker's small pool. */
export async function withBoundedActivationRead<T>(
  read: (tx: Prisma.TransactionClient) => Promise<T>,
  options: { statementTimeoutMs?: number } = {},
): Promise<T> {
  const statementTimeoutMs = options.statementTimeoutMs ?? 5_000;
  if (!Number.isInteger(statementTimeoutMs) || statementTimeoutMs < 1 || statementTimeoutMs > 5_000) {
    throw new RangeError("Activation read statement timeout must be between 1 and 5000 milliseconds.");
  }
  try {
    return await getPrisma().$transaction(async (tx) => {
      await tx.$queryRaw`SELECT set_config('statement_timeout', ${String(statementTimeoutMs)}, true)`;
      return read(tx);
    }, { maxWait: 2_000, timeout: statementTimeoutMs * 2 + 3_000 });
  } catch (error) {
    throw new BoundedActivationReadError(error);
  }
}
