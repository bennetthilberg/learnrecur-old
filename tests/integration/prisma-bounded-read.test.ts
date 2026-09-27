import { afterAll, describe, expect, it } from "vitest";

import { getPrisma } from "@/lib/prisma";
import {
  BoundedActivationReadError,
  withBoundedActivationRead,
} from "@/lib/prisma-bounded-read";

const describeDatabase = process.env.RUN_DATABASE_TESTS === "1" ? describe : describe.skip;

describeDatabase("bounded activation database reads", () => {
  const prisma = getPrisma();

  afterAll(async () => { await prisma.$disconnect(); });

  it("returns a short read", async () => {
    await expect(withBoundedActivationRead(async (tx) => {
      const rows = await tx.$queryRaw<Array<{ value: number }>>`SELECT 1 AS value`;
      return rows[0].value;
    })).resolves.toBe(1);
  });

  it("cancels a slow database statement and frees its connection", async () => {
    const started = Date.now();
    await expect(withBoundedActivationRead(async (tx) => {
      await tx.$queryRaw`SELECT pg_sleep(0.5)`;
    }, { statementTimeoutMs: 100 })).rejects.toBeInstanceOf(BoundedActivationReadError);
    expect(Date.now() - started).toBeLessThan(2_000);
    await expect(prisma.$queryRaw`SELECT 1`).resolves.toBeDefined();
  });
});
