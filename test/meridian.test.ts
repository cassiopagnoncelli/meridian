import { existsSync } from "node:fs";
import { mkdir } from "node:fs/promises";
import { resolve } from "node:path";

import { describe, expect, it } from "vitest";

import { Meridian, MeridianDataError, MeridianInputError } from "../src";

const fixtureDataDir = resolve("test/fixtures/meridian");
const emptyDataDir = resolve("test/fixtures/empty");
const maxmindDataDir = resolve("datasets");

describe("Meridian city data", () => {
  it("loads selected CSV sources and reports source status", async () => {
    const meridian = await Meridian.open({
      dataDir: fixtureDataDir,
      sources: ["ibge", "ghsl"]
    });

    expect(meridian.sources()).toEqual({ maxmind: false, ibge: true, ghsl: true });

    meridian.close();
    expect(meridian.sources()).toEqual({ maxmind: false, ibge: false, ghsl: false });
  });

  it("looks up IBGE income by city and state", async () => {
    const meridian = await Meridian.open({
      dataDir: fixtureDataDir,
      sources: ["ibge"]
    });

    expect(meridian.ibge("São Paulo", "SP")).toEqual({
      source: "ibge",
      city: "São Paulo",
      state: "SP",
      country: "Brazil",
      municipalityCode: "3550308",
      income: {
        meanMonthlyHouseholdPerCapitaBrl2022: 2713.36,
        medianMonthlyHouseholdPerCapitaBrl2022: 1340
      }
    });
  });

  it("matches IBGE keys accent- and case-insensitively", async () => {
    const meridian = await Meridian.open({
      dataDir: fixtureDataDir,
      sources: ["ibge"]
    });

    expect(meridian.ibge("Sao Paulo", "sp")?.municipalityCode).toBe("3550308");
  });

  it("matches IBGE Brazilian state names as aliases", async () => {
    const meridian = await Meridian.open({
      dataDir: fixtureDataDir,
      sources: ["ibge"]
    });

    expect(meridian.ibge("Sao Paulo", "São Paulo")?.municipalityCode).toBe("3550308");
  });

  it("returns null for unknown IBGE city/state pairs", async () => {
    const meridian = await Meridian.open({
      dataDir: fixtureDataDir,
      sources: ["ibge"]
    });

    expect(meridian.ibge("Nowhere", "SP")).toBeNull();
  });

  it("looks up GHSL metrics by city and country", async () => {
    const meridian = await Meridian.open({
      dataDir: fixtureDataDir,
      sources: ["ghsl"]
    });

    expect(meridian.ghsl("São Paulo", "Brazil")).toEqual({
      source: "ghsl",
      city: "São Paulo",
      country: "Brazil",
      urbanCentreId: "6351",
      worldRegion: "Latin America and the Caribbean",
      worldBankIncomeGroup: "Upper Middle",
      urbanAreaKm2_2025: 1962,
      population_2025: 19485158.24,
      hdi_2020: 0.76
    });
  });

  it("matches GHSL country aliases", async () => {
    const meridian = await Meridian.open({
      dataDir: fixtureDataDir,
      sources: ["ghsl"]
    });

    expect(meridian.ghsl("São Paulo", "Brasil")?.urbanCentreId).toBe("6351");
  });

  it("uses the largest-population GHSL row for duplicate city-country keys", async () => {
    const meridian = await Meridian.open({
      dataDir: fixtureDataDir,
      sources: ["ghsl"]
    });

    expect(meridian.ghsl("Springfield", "United States")?.urbanCentreId).toBe("large");
  });
});

describe("Meridian data loading", () => {
  it("returns metadata for selected source files", async () => {
    const meridian = await Meridian.open({
      dataDir: fixtureDataDir,
      sources: ["ibge", "ghsl"]
    });

    const metadata = meridian.metadata();
    expect(metadata.dataDir).toBe(fixtureDataDir);
    expect(metadata.sources).toEqual({ maxmind: false, ibge: true, ghsl: true });
    expect(metadata.files).toHaveLength(2);
    expect(metadata.files.every((file) => file.exists)).toBe(true);
    expect(metadata.files.every((file) => (file.sizeBytes ?? 0) > 0)).toBe(true);
  });

  it("fails fast in strict mode when selected source files are missing", async () => {
    await mkdir(emptyDataDir, { recursive: true });

    await expect(
      Meridian.open({
        dataDir: emptyDataDir,
        sources: ["ibge"]
      })
    ).rejects.toThrow(MeridianDataError);
  });

  it("skips missing sources when strict is false", async () => {
    await mkdir(emptyDataDir, { recursive: true });

    const meridian = await Meridian.open({
      dataDir: emptyDataDir,
      sources: ["ibge"],
      strict: false
    });

    expect(meridian.sources()).toEqual({ maxmind: false, ibge: false, ghsl: false });
    expect(meridian.ibge("São Paulo", "SP")).toBeNull();
  });
});

describe("Meridian MaxMind", () => {
  const hasMaxMindFixtures =
    existsSync(resolve(maxmindDataDir, "maxmind/GeoLite2-City.mmdb")) &&
    existsSync(resolve(maxmindDataDir, "maxmind/GeoLite2-Country.mmdb")) &&
    existsSync(resolve(maxmindDataDir, "maxmind/GeoLite2-ASN.mmdb"));

  it.skipIf(!hasMaxMindFixtures)("returns polished MaxMind data by default", async () => {
    const meridian = await Meridian.open({
      dataDir: maxmindDataDir,
      sources: ["maxmind"]
    });

    const result = meridian.ip("8.8.8.8");

    expect(result?.source).toBe("maxmind");
    expect(result?.ip).toBe("8.8.8.8");
    expect(result?.city).not.toHaveProperty("raw");
    expect(result?.country).not.toHaveProperty("raw");
    expect(result?.asn).not.toHaveProperty("raw");
  });

  it.skipIf(!hasMaxMindFixtures)("returns raw MaxMind JSON when requested", async () => {
    const meridian = await Meridian.open({
      dataDir: maxmindDataDir,
      sources: ["maxmind"]
    });

    const result = meridian.ip("8.8.8.8", true);

    expect(result).not.toHaveProperty("source");
    expect(result).not.toHaveProperty("ip");
    expect(result?.country).not.toBeNull();
    expect(result?.asn).not.toBeNull();
    expect(JSON.parse(JSON.stringify(result))).toEqual(result);
  });

  it("throws MeridianInputError for invalid IP addresses", async () => {
    await mkdir(emptyDataDir, { recursive: true });

    const meridian = await Meridian.open({
      dataDir: emptyDataDir,
      sources: ["maxmind"],
      strict: false
    });

    expect(() => meridian.ip("not-an-ip")).toThrow(MeridianInputError);
  });

  it("fails strict MaxMind open when mmdb files are missing", async () => {
    await expect(
      Meridian.open({
        dataDir: emptyDataDir,
        sources: ["maxmind"]
      })
    ).rejects.toThrow(MeridianDataError);
  });
});
