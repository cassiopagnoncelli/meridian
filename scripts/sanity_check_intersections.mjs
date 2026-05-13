#!/usr/bin/env node
import { access, mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";

import { parse } from "csv-parse/sync";

const dataDir = resolve(optionValue("--data-dir") ?? join(process.cwd(), "lib", "meridian"));
const reportsDir = resolve(optionValue("--reports-dir") ?? join(process.cwd(), "reports", "compatibility"));

const COUNTRY_ALIASES = new Map([
  ["brasil", "brazil"],
  ["br", "brazil"],
  ["uk", "united kingdom"],
  ["u k", "united kingdom"],
  ["great britain", "united kingdom"],
  ["britain", "united kingdom"],
  ["england", "united kingdom"],
  ["us", "united states"],
  ["u s", "united states"],
  ["usa", "united states"],
  ["u s a", "united states"],
  ["united states of america", "united states"]
]);

const BRAZIL_STATE_ALIASES = new Map([
  ["acre", "ac"], ["alagoas", "al"], ["amapa", "ap"], ["amazonas", "am"],
  ["bahia", "ba"], ["ceara", "ce"], ["distrito federal", "df"],
  ["espirito santo", "es"], ["goias", "go"], ["maranhao", "ma"],
  ["mato grosso", "mt"], ["mato grosso do sul", "ms"], ["minas gerais", "mg"],
  ["para", "pa"], ["paraiba", "pb"], ["parana", "pr"], ["pernambuco", "pe"],
  ["piaui", "pi"], ["rio de janeiro", "rj"], ["rio grande do norte", "rn"],
  ["rio grande do sul", "rs"], ["rondonia", "ro"], ["roraima", "rr"],
  ["santa catarina", "sc"], ["sao paulo", "sp"], ["sergipe", "se"],
  ["tocantins", "to"]
]);

for (const uf of [
  "ac", "al", "ap", "am", "ba", "ce", "df", "es", "go", "ma", "mt", "ms", "mg",
  "pa", "pb", "pr", "pe", "pi", "rj", "rn", "rs", "ro", "rr", "sc", "sp", "se", "to"
]) {
  BRAZIL_STATE_ALIASES.set(uf, uf);
}

const ibgeRows = await csvRows(join(dataDir, "ibge", "ibge_municipality_income.csv"));
const ghslRows = await csvRows(join(dataDir, "ghsl", "ghsl_city_metrics.csv"));
const ibgeAliases = await optionalCsvRows(join(dataDir, "ibge", "ibge_city_aliases.csv"));
const ghslAliases = await optionalCsvRows(join(dataDir, "ghsl", "ghsl_city_aliases.csv"));

const ibgeReport = analyzeIbge(ibgeRows, ibgeAliases);
const ghslReport = analyzeGhsl(ghslRows, ghslAliases);

await writeJson(join(reportsDir, "ibge_intersection_sanity.json"), ibgeReport.summary);
await writeCsv(join(reportsDir, "ibge_intersection_mismatches.csv"), ibgeReport.mismatches);
await writeCsv(join(reportsDir, "ibge_intersection_missing.csv"), ibgeReport.missing);
await writeJson(join(reportsDir, "ghsl_intersection_sanity.json"), ghslReport.summary);
await writeCsv(join(reportsDir, "ghsl_intersection_mismatches.csv"), ghslReport.mismatches);
await writeCsv(join(reportsDir, "ghsl_intersection_missing.csv"), ghslReport.missing);

printSummary("IBGE", ibgeReport.summary);
printSummary("GHSL", ghslReport.summary);
console.log(`Wrote intersection sanity reports to ${reportsDir}`);

function analyzeIbge(rows, aliases) {
  const aliasesByCode = groupBy(aliases, "ibge_municipality_code");
  const summary = baseSummary(rows.length, aliases.length);
  const mismatches = [];
  const missing = [];

  for (const row of rows) {
    const city = row.city?.trim();
    const state = row.state?.trim();
    const code = row.ibge_municipality_code?.trim();

    if (!city || !state || !code) {
      continue;
    }

    const bucket = aliasesByCode.get(code) ?? [];
    if (bucket.length === 0) {
      summary.missingRows += 1;
      missing.push({
        city,
        state,
        country: "Brazil",
        id: code,
        reason: "not_in_maxmind_alias_intersection"
      });
      continue;
    }

    summary.intersectionRows += 1;
    const exact = bucket.some((alias) => alias.alias_city === city && alias.state === state);
    const normalized = bucket.some(
      (alias) => normalize(alias.alias_city) === normalize(city) && canonicalBrazilState(alias.state) === canonicalBrazilState(state)
    );
    const cityVariants = cityVariantAliases(bucket, city);

    if (exact) {
      summary.exactRows += 1;
    } else if (normalized) {
      summary.normalizedOnlyRows += 1;
      mismatches.push(mismatchRow(row, "normalized_only", bucket, cityVariants));
    } else {
      summary.variantOnlyRows += 1;
      mismatches.push(mismatchRow(row, "variant_only", bucket, cityVariants));
    }

    if (cityVariants.length > 0) {
      summary.rowsWithAdditionalCityVariants += 1;
    }
  }

  finalizeSummary(summary);
  return { summary, mismatches: sortRows(mismatches), missing: sortRows(missing) };
}

function analyzeGhsl(rows, aliases) {
  const aliasesById = groupBy(aliases, "ghsl_urban_centre_id");
  const summary = baseSummary(rows.length, aliases.length);
  const mismatches = [];
  const missing = [];

  for (const row of rows) {
    const city = row.city?.trim();
    const country = row.country?.trim();
    const id = row.ghsl_urban_centre_id?.trim();

    if (!city || !country || !id) {
      continue;
    }

    const bucket = aliasesById.get(id) ?? [];
    if (bucket.length === 0) {
      summary.missingRows += 1;
      missing.push({
        city,
        country,
        id,
        reason: "not_in_maxmind_alias_intersection"
      });
      continue;
    }

    summary.intersectionRows += 1;
    const exact = bucket.some((alias) => alias.alias_city === city && alias.country === country);
    const normalized = bucket.some(
      (alias) => normalize(alias.alias_city) === normalize(city) && canonicalCountry(alias.country) === canonicalCountry(country)
    );
    const cityVariants = cityVariantAliases(bucket, city);

    if (exact) {
      summary.exactRows += 1;
    } else if (normalized) {
      summary.normalizedOnlyRows += 1;
      mismatches.push(mismatchRow(row, "normalized_only", bucket, cityVariants));
    } else {
      summary.variantOnlyRows += 1;
      mismatches.push(mismatchRow(row, "variant_only", bucket, cityVariants));
    }

    if (cityVariants.length > 0) {
      summary.rowsWithAdditionalCityVariants += 1;
    }
  }

  finalizeSummary(summary);
  return { summary, mismatches: sortRows(mismatches), missing: sortRows(missing) };
}

function baseSummary(totalRows, aliasRows) {
  return {
    totalRows,
    aliasRows,
    intersectionRows: 0,
    missingRows: 0,
    exactRows: 0,
    normalizedOnlyRows: 0,
    variantOnlyRows: 0,
    rowsWithAdditionalCityVariants: 0,
    coverageRate: 0,
    exactRateWithinIntersection: 0,
    mismatchRateWithinIntersection: 0
  };
}

function finalizeSummary(summary) {
  const mismatchRows = summary.normalizedOnlyRows + summary.variantOnlyRows;
  summary.coverageRate = ratio(summary.intersectionRows, summary.totalRows);
  summary.exactRateWithinIntersection = ratio(summary.exactRows, summary.intersectionRows);
  summary.mismatchRateWithinIntersection = ratio(mismatchRows, summary.intersectionRows);
}

function mismatchRow(row, reason, aliases, cityVariants) {
  return {
    city: row.city ?? "",
    state: row.state ?? "",
    country: row.country ?? "",
    id: row.ibge_municipality_code ?? row.ghsl_urban_centre_id ?? "",
    reason,
    aliases: compactAliasList(aliases),
    city_variant_aliases: cityVariants.join(" | ")
  };
}

function cityVariantAliases(aliases, canonicalCity) {
  const canonical = normalize(canonicalCity);
  return unique(
    aliases
      .map((alias) => alias.alias_city)
      .filter((city) => city && normalize(city) !== canonical)
  ).sort((left, right) => left.localeCompare(right));
}

function compactAliasList(aliases) {
  return unique(aliases.map((alias) => alias.alias_city).filter(Boolean))
    .sort((left, right) => left.localeCompare(right))
    .slice(0, 12)
    .join(" | ");
}

function groupBy(rows, field) {
  const grouped = new Map();
  for (const row of rows) {
    const key = row[field]?.trim();
    if (!key) {
      continue;
    }
    const bucket = grouped.get(key) ?? [];
    bucket.push(row);
    grouped.set(key, bucket);
  }
  return grouped;
}

function canonicalCountry(value) {
  const normalized = normalize(value);
  return COUNTRY_ALIASES.get(normalized) ?? normalized;
}

function canonicalBrazilState(value) {
  const normalized = normalize(value);
  return BRAZIL_STATE_ALIASES.get(normalized) ?? normalized;
}

function normalize(value) {
  return String(value ?? "")
    .normalize("NFKD")
    .replace(/\p{Diacritic}/gu, "")
    .replace(/[^0-9A-Za-z]+/g, " ")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function ratio(numerator, denominator) {
  return denominator === 0 ? 0 : Number((numerator / denominator).toFixed(6));
}

function sortRows(rows) {
  return rows.sort(
    (left, right) =>
      String(left.country ?? "").localeCompare(String(right.country ?? "")) ||
      String(left.state ?? "").localeCompare(String(right.state ?? "")) ||
      String(left.city ?? "").localeCompare(String(right.city ?? "")) ||
      String(left.id ?? "").localeCompare(String(right.id ?? ""))
  );
}

async function optionalCsvRows(path) {
  try {
    await access(path);
  } catch {
    return [];
  }
  return csvRows(path);
}

async function csvRows(path) {
  const content = await readFile(path, "utf8");
  return parse(content, { bom: true, columns: true, skip_empty_lines: true });
}

async function writeJson(path, value) {
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, `${JSON.stringify(value, null, 2)}\n`, "utf8");
}

async function writeCsv(path, rows) {
  await mkdir(dirname(path), { recursive: true });
  const headers = unique(rows.flatMap((row) => Object.keys(row)));
  const lines = [headers.join(",")];
  for (const row of rows) {
    lines.push(headers.map((header) => csvCell(row[header])).join(","));
  }
  await writeFile(path, `${lines.join("\n")}\n`, "utf8");
}

function csvCell(value) {
  const text = String(value ?? "");
  return /[",\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
}

function unique(values) {
  return [...new Set(values.filter(Boolean))];
}

function printSummary(label, summary) {
  console.log(`${label}: intersection ${summary.intersectionRows}/${summary.totalRows} (${(summary.coverageRate * 100).toFixed(2)}%)`);
  console.log(
    `  exact=${summary.exactRows} normalized_only=${summary.normalizedOnlyRows} variant_only=${summary.variantOnlyRows} missing=${summary.missingRows}`
  );
  console.log(`  rows_with_additional_city_variants=${summary.rowsWithAdditionalCityVariants}`);
}

function optionValue(name) {
  const index = process.argv.indexOf(name);
  if (index === -1) return null;
  const value = process.argv[index + 1];
  return value && !value.startsWith("--") ? value : null;
}
