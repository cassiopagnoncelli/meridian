import { access } from "node:fs/promises";

import { cityStateKey, normalizeKey } from "../normalize";
import { parseCsvRows, parseNullableNumber } from "../csv";
import type { IbgeMunicipalityIncome } from "../types";

export type IbgeIndex = {
  canonical: Map<string, IbgeMunicipalityIncome>;
  aliases: Map<string, IbgeMunicipalityIncome>;
};

export async function loadIbge(path: string, aliasesPath?: string): Promise<IbgeIndex> {
  const rows = await parseCsvRows(path);
  const canonical = new Map<string, IbgeMunicipalityIncome>();
  const aliases = new Map<string, IbgeMunicipalityIncome>();
  const recordsByCode = new Map<string, IbgeMunicipalityIncome>();

  for (const row of rows) {
    const city = row.city?.trim();
    const state = row.state?.trim();
    const municipalityCode = row.ibge_municipality_code?.trim();

    if (!city || !state || !municipalityCode) {
      continue;
    }

    const record: IbgeMunicipalityIncome = {
      source: "ibge",
      city,
      state,
      country: "Brazil",
      municipalityCode,
      income: {
        meanMonthlyHouseholdPerCapitaBrl2022: parseNullableNumber(
          row.mean_monthly_household_income_per_capita_brl_2022
        ),
        medianMonthlyHouseholdPerCapitaBrl2022: parseNullableNumber(
          row.median_monthly_household_income_per_capita_brl_2022
        )
      }
    };

    canonical.set(cityStateKey(city, state), record);
    recordsByCode.set(municipalityCode, record);
  }

  if (aliasesPath && (await exists(aliasesPath))) {
    for (const row of await parseCsvRows(aliasesPath)) {
      const city = row.alias_city?.trim();
      const state = row.state?.trim();
      const municipalityCode = row.ibge_municipality_code?.trim();
      const record = municipalityCode ? recordsByCode.get(municipalityCode) : null;

      if (city && state && record && normalizeKey(city) && normalizeKey(state)) {
        aliases.set(cityStateKey(city, state), record);
      }
    }
  }

  return { canonical, aliases };
}

export function lookupIbge(index: IbgeIndex | null | undefined, city: string, state: string): IbgeMunicipalityIncome | null {
  if (!normalizeKey(city) || !normalizeKey(state)) {
    return null;
  }

  const key = cityStateKey(city, state);
  return index?.canonical.get(key) ?? index?.aliases.get(key) ?? null;
}

function exists(path: string): Promise<boolean> {
  return access(path)
    .then(() => true)
    .catch(() => false);
}
