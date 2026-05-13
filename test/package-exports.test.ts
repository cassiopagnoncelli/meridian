import { createRequire } from "node:module";
import { resolve } from "node:path";

import { describe, expect, it } from "vitest";

describe("package exports", () => {
  it("supports ESM import from dist", async () => {
    const mod = await import(resolve("dist/index.js"));
    expect(mod.Meridian).toBeTypeOf("function");
  });

  it("supports CommonJS require from dist", () => {
    const require = createRequire(import.meta.url);
    const mod = require(resolve("dist/index.cjs")) as Record<string, unknown>;
    expect(mod.Meridian).toBeTypeOf("function");
  });
});
