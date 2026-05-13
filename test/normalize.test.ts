import { describe, expect, it } from "vitest";

import { cityCountryKey, cityStateKey, normalizeKey } from "../src/normalize";

describe("normalizeKey", () => {
  it("normalizes accents, punctuation, whitespace, and casing", () => {
    expect(normalizeKey("  São   Paulo!!! ")).toBe("sao paulo");
    expect(normalizeKey("Derry/Londonderry")).toBe("derry londonderry");
    expect(normalizeKey("CURITIBA")).toBe("curitiba");
  });

  it("builds deterministic composite keys", () => {
    expect(cityStateKey("São Paulo", "sp")).toBe("sao paulo|sp");
    expect(cityCountryKey("São Paulo", "Brazil")).toBe("sao paulo|brazil");
  });
});
