import { cityStateKey } from "../normalize";
import { parseCsvRows, parseNullableNumber } from "../csv";
import type { IbgeMunicipalityIncome } from "../types";

export type IbgeIndex = Map<string, IbgeMunicipalityIncome>;

export async function loadIbge(path: string): Promise<IbgeIndex> {
  const rows = await parseCsvRows(path);
  const index: IbgeIndex = new Map();

  for (const row of rows) {
    const city = row.city?.trim();
    const state = row.state?.trim();
    const municipalityCode = row.ibge_municipality_code?.trim();

    if (!city || !state || !municipalityCode) {
      continue;
    }

    index.set(cityStateKey(city, state), {
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
    });
  }

  return index;
}
