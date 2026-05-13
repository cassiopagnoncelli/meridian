#!/usr/bin/env node
import { readFile } from "node:fs/promises";
import { join, resolve } from "node:path";

import { parse } from "csv-parse/sync";

import { Meridian } from "../dist/index.js";

const REQUIRED_IBGE_COLUMNS = [
  "city",
  "state",
  "country",
  "ibge_municipality_code",
  "mean_monthly_household_income_per_capita_brl_2022",
  "median_monthly_household_income_per_capita_brl_2022"
];

const REQUIRED_GHSL_COLUMNS = [
  "city",
  "country",
  "ghsl_urban_centre_id",
  "world_region",
  "world_bank_income_group",
  "urban_area_km2_2025",
  "population_2025",
  "hdi_2020"
];

const dataDir = resolve(optionValue("--data-dir") ?? join(process.cwd(), "lib", "meridian"));

await assertColumns(join(dataDir, "ibge", "ibge_municipality_income.csv"), REQUIRED_IBGE_COLUMNS);
await assertColumns(join(dataDir, "ghsl", "ghsl_city_metrics.csv"), REQUIRED_GHSL_COLUMNS);

const meridian = await Meridian.open({ dataDir });
const metadata = meridian.metadata();
assert(metadata.sources.maxmind && metadata.sources.ibge && metadata.sources.ghsl, "all sources load");
assert(metadata.files.every((file) => file.exists), "all metadata files exist");
assert(metadata.files.every((file) => (file.sizeBytes ?? 0) > 0), "all metadata files are non-empty");

const ibge = meridian.ibge("Sao Paulo", "São Paulo");
assert(ibge?.municipalityCode === "3550308", "IBGE São Paulo alias lookup works");

const ghsl = meridian.ghsl("São Paulo", "Brasil");
assert(ghsl !== null && typeof ghsl.population_2025 === "number", "GHSL São Paulo alias lookup works");

const ip = meridian.ip("8.8.8.8");
assert(ip?.asn.autonomousSystemNumber !== null, "MaxMind ASN lookup works");

const enrichedIp = meridian.ip("200.160.2.3", false, true);
assert(enrichedIp?.subdivision.isoCode === "SP", "MaxMind subdivision lookup works");
assert(enrichedIp?.ibge?.municipalityCode === "3550308", "IP-to-IBGE city enrichment works");
assert(enrichedIp?.ghsl?.country === "Brazil", "IP-to-GHSL city enrichment works");

console.log(`ok  validated Meridian data at ${dataDir}`);

async function assertColumns(path, requiredColumns) {
  const content = await readFile(path, "utf8");
  const rows = parse(content, {
    bom: true,
    columns: true,
    skip_empty_lines: true,
    to_line: 2
  });
  const columns = new Set(Object.keys(rows[0] ?? {}));
  const missing = requiredColumns.filter((column) => !columns.has(column));
  if (missing.length > 0) {
    throw new Error(`Validation failed: ${path} missing columns: ${missing.join(", ")}`);
  }
  console.log(`ok  ${path} required columns present`);
}

function assert(condition, message) {
  if (!condition) {
    throw new Error(`Validation failed: ${message}`);
  }
  console.log(`ok  ${message}`);
}

function optionValue(name) {
  const index = process.argv.indexOf(name);
  if (index === -1) {
    return null;
  }
  const value = process.argv[index + 1];
  return value && !value.startsWith("--") ? value : null;
}
