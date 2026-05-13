import { parseCsvRows, parseNullableNumber, parseNullableString } from "../csv";
import { cityCountryKey } from "../normalize";
import type { GhslCityMetrics } from "../types";

export type GhslIndex = Map<string, GhslCityMetrics[]>;

export async function loadGhsl(path: string): Promise<GhslIndex> {
  const rows = await parseCsvRows(path);
  const index: GhslIndex = new Map();

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

    const key = cityCountryKey(city, country);
    const bucket = index.get(key);
    if (bucket) {
      bucket.push(record);
    } else {
      index.set(key, [record]);
    }
  }

  for (const records of index.values()) {
    records.sort((left, right) => (right.population_2025 ?? -1) - (left.population_2025 ?? -1));
  }

  return index;
}
