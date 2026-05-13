import { access } from "node:fs/promises";

import { parseCsvRows, parseNullableNumber, parseNullableString } from "../csv";
import { cityCountryKey, normalizeKey } from "../normalize";
import { canonicalCountry } from "../aliases";
import type { GhslCityMetrics } from "../types";

export type GhslIndex = {
  canonical: Map<string, GhslCityMetrics[]>;
  aliases: Map<string, GhslCityMetrics[]>;
};

export async function loadGhsl(path: string, aliasesPath?: string): Promise<GhslIndex> {
  const rows = await parseCsvRows(path);
  const canonical = new Map<string, GhslCityMetrics[]>();
  const aliases = new Map<string, GhslCityMetrics[]>();
  const recordsById = new Map<string, GhslCityMetrics>();

  for (const row of rows) {
    const city = row.city?.trim();
    const country = row.country?.trim();
    const urbanCentreId = row.ghsl_urban_centre_id?.trim();

    if (!city || !country || !urbanCentreId) {
      continue;
    }

    const record: GhslCityMetrics = {
      source: "ghsl",
      city,
      country,
      urbanCentreId,
      worldRegion: parseNullableString(row.world_region),
      worldBankIncomeGroup: parseNullableString(row.world_bank_income_group),
      urbanAreaKm2_2025: parseNullableNumber(row.urban_area_km2_2025),
      population_2025: parseNullableNumber(row.population_2025),
      hdi_2020: parseNullableNumber(row.hdi_2020)
    };

    addRecord(canonical, cityCountryKey(city, country), record);
    recordsById.set(urbanCentreId, record);
  }

  if (aliasesPath && (await exists(aliasesPath))) {
    for (const row of await parseCsvRows(aliasesPath)) {
      const city = row.alias_city?.trim();
      const country = row.country?.trim();
      const urbanCentreId = row.ghsl_urban_centre_id?.trim();
      const record = urbanCentreId ? recordsById.get(urbanCentreId) : null;

      if (city && country && record && normalizeKey(city) && normalizeKey(canonicalCountry(country))) {
        addRecord(aliases, cityCountryKey(city, country), record);
      }
    }
  }

  sortBuckets(canonical);
  sortBuckets(aliases);

  return { canonical, aliases };
}

export function lookupGhsl(index: GhslIndex | null | undefined, city: string, country: string): GhslCityMetrics | null {
  if (!normalizeKey(city) || !normalizeKey(canonicalCountry(country))) {
    return null;
  }

  const key = cityCountryKey(city, country);
  return index?.canonical.get(key)?.[0] ?? index?.aliases.get(key)?.[0] ?? null;
}

function addRecord(index: Map<string, GhslCityMetrics[]>, key: string, record: GhslCityMetrics): void {
  const bucket = index.get(key);
  if (bucket) {
    if (!bucket.some((existing) => existing.urbanCentreId === record.urbanCentreId)) {
      bucket.push(record);
    }
  } else {
    index.set(key, [record]);
  }
}

function sortBuckets(index: Map<string, GhslCityMetrics[]>): void {
  for (const records of index.values()) {
    records.sort((left, right) => (right.population_2025 ?? -1) - (left.population_2025 ?? -1));
  }
}

function exists(path: string): Promise<boolean> {
  return access(path)
    .then(() => true)
    .catch(() => false);
}
