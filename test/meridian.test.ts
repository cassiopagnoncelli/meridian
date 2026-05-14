import { execFile } from "node:child_process";
import { existsSync } from "node:fs";
import { mkdir } from "node:fs/promises";
import { resolve } from "node:path";
import { promisify } from "node:util";

import { describe, expect, it } from "vitest";

import { Meridian, MeridianDataError, MeridianInputError } from "../src";
import { loadGhsl, lookupGhslByGeonameId } from "../src/loaders/ghsl";

const fixtureDataDir = resolve("test/fixtures/meridian");
const noAliasDataDir = resolve("test/fixtures/meridian-no-alias");
const emptyDataDir = resolve("test/fixtures/empty");
const maxmindDataDir = resolve("datasets");
const hostDataDir = resolve("lib/meridian");
const execFileAsync = promisify(execFile);

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

  it("matches IBGE generated city aliases", async () => {
    const meridian = await Meridian.open({
      dataDir: fixtureDataDir,
      sources: ["ibge"]
    });

    expect(meridian.ibge("Sampa", "SP")?.municipalityCode).toBe("3550308");
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

  it("matches GHSL generated city aliases", async () => {
    const meridian = await Meridian.open({
      dataDir: fixtureDataDir,
      sources: ["ghsl"]
    });

    expect(meridian.ghsl("Sampa", "BR")?.urbanCentreId).toBe("6351");
  });

  it("uses the largest-population GHSL row for duplicate city-country keys", async () => {
    const meridian = await Meridian.open({
      dataDir: fixtureDataDir,
      sources: ["ghsl"]
    });

    expect(meridian.ghsl("Springfield", "United States")?.urbanCentreId).toBe("large");
  });

  it("loads optional GHSL MaxMind geoname-id maps", async () => {
    const index = await loadGhsl(
      resolve(fixtureDataDir, "ghsl/ghsl_city_metrics.csv"),
      resolve(fixtureDataDir, "ghsl/ghsl_city_aliases.csv"),
      resolve(fixtureDataDir, "ghsl/ghsl_geoname_map.csv")
    );

    expect(lookupGhslByGeonameId(index, 3448439)?.urbanCentreId).toBe("6351");
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

  it("does not require optional city alias files in strict mode", async () => {
    const meridian = await Meridian.open({
      dataDir: noAliasDataDir,
      sources: ["ibge", "ghsl"]
    });

    expect(meridian.ibge("Sao Paulo", "SP")?.municipalityCode).toBe("3550308");
    expect(meridian.ghsl("Sao Paulo", "Brazil")?.urbanCentreId).toBe("6351");
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
    expect(result?.subdivision).toEqual({
      isoCode: null,
      name: null,
      geonameId: null
    });
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

  it.skipIf(!hasHostFixtures())("enriches Brazilian IP city data with IBGE and GHSL", async () => {
    const meridian = await Meridian.open({
      dataDir: hostDataDir
    });

    const result = meridian.ip("200.160.2.3", false, true);

    expect(result?.city.name).toBe("São Paulo");
    expect(result?.subdivision.isoCode).toBe("SP");
    expect(result?.ibge?.municipalityCode).toBe("3550308");
    expect(result?.ghsl?.country).toBe("Brazil");
  });

  it.skipIf(!hasHostFixtures())("enriches non-Brazil IP city data with GHSL only", async () => {
    const meridian = await Meridian.open({
      dataDir: hostDataDir
    });

    const result = meridian.ip("62.210.16.2", false, true);

    expect(result?.country.isoCode).toBe("FR");
    expect(result?.ibge).toBeNull();
    expect(result?.ghsl?.city).toBe("Paris");
    expect(result?.ghsl?.country).toBe("France");
  });

  it.skipIf(!hasHostFixtures())("keeps raw mode raw-only even when city enrichment is requested", async () => {
    const meridian = await Meridian.open({
      dataDir: hostDataDir
    });

    const result = meridian.ip("200.160.2.3", true, true);

    expect(result).not.toHaveProperty("source");
    expect(result).not.toHaveProperty("ibge");
    expect(result).not.toHaveProperty("ghsl");
    expect(result?.city).not.toBeNull();
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

describe("compatibility alias tooling", () => {
  it("prints stable generated alias CSV headers", async () => {
    const { stdout } = await execFileAsync("node", [
      "scripts/build_compatibility_aliases.mjs",
      "--print-headers"
    ]);

    expect(JSON.parse(stdout)).toEqual({
      ibge: ["alias_city", "state", "ibge_municipality_code"],
      ghsl: ["alias_city", "country", "ghsl_urban_centre_id"]
    });
  });

  it("prints stable GHSL geoname map CSV headers", async () => {
    const { stdout } = await execFileAsync("node", [
      "scripts/build_ghsl_geoname_map.mjs",
      "--print-headers"
    ]);

    expect(JSON.parse(stdout)).toEqual({
      ghsl_geoname_map: [
        "maxmind_geoname_id",
        "country_iso",
        "subdivision_iso",
        "ghsl_urban_centre_id",
        "method",
        "confidence",
        "distance_km",
        "maxmind_city_names",
        "maxmind_subdivision_names",
        "ghsl_city",
        "ghsl_country",
        "notes"
      ]
    });
  });
});

function hasHostFixtures(): boolean {
  return (
    existsSync(resolve(hostDataDir, "maxmind/GeoLite2-City.mmdb")) &&
    existsSync(resolve(hostDataDir, "maxmind/GeoLite2-Country.mmdb")) &&
    existsSync(resolve(hostDataDir, "maxmind/GeoLite2-ASN.mmdb")) &&
    existsSync(resolve(hostDataDir, "ibge/ibge_municipality_income.csv")) &&
    existsSync(resolve(hostDataDir, "ghsl/ghsl_city_metrics.csv"))
  );
}
