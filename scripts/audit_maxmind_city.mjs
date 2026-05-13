#!/usr/bin/env node
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";

import maxmind from "maxmind";
import { parse } from "csv-parse/sync";

const dataDir = resolve(optionValue("--data-dir") ?? join(process.cwd(), "lib", "meridian"));
const outputDir = resolve(optionValue("--output-dir") ?? join(process.cwd(), "reports", "audit"));

const countryAliases = new Map([
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

const brazilStateAliases = new Map([
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
  brazilStateAliases.set(uf, uf);
}

const ibgeRows = await csvRows(join(dataDir, "ibge", "ibge_municipality_income.csv"));
const ghslRows = await csvRows(join(dataDir, "ghsl", "ghsl_city_metrics.csv"));
const cityReader = await maxmind.open(join(dataDir, "maxmind", "GeoLite2-City.mmdb"));
const maxmindRecords = enumerateMaxMindCityRecords(cityReader);

const ibgeIndexes = buildIbgeIndexes(ibgeRows);
const ghslIndexes = buildGhslIndexes(ghslRows);
const ibgeAudit = auditIbge(maxmindRecords, ibgeIndexes);
const ghslAudit = auditGhsl(maxmindRecords, ghslIndexes);
const pipelineMetrics = calculatePipelineMetrics(maxmindRecords, ibgeIndexes, ghslIndexes);

await writeJson(join(outputDir, "ip_to_ibge_summary.json"), ibgeAudit.summary);
await writeCsv(join(outputDir, "ip_to_ibge_misses.csv"), ibgeAudit.misses);
await writeJson(join(outputDir, "ip_to_ghsl_summary.json"), ghslAudit.summary);
await writeCsv(join(outputDir, "ip_to_ghsl_misses.csv"), ghslAudit.misses);
await writeJson(join(outputDir, "maxmind_pipeline_metrics.json"), pipelineMetrics);

console.log(`MaxMind unique city records: ${maxmindRecords.length}`);
printSummary("IBGE", ibgeAudit.summary);
printSummary("GHSL", ghslAudit.summary);
printPipelineMetrics(pipelineMetrics);
console.log(`Wrote audit reports to ${outputDir}`);

function enumerateMaxMindCityRecords(reader) {
  const pointers = enumerateDataPointers(reader);
  const records = new Map();

  for (const pointer of pointers) {
    const raw = reader.resolveDataPointer(pointer);
    const cityNames = objectValues(raw?.city?.names);
    const countryNames = objectValues(raw?.country?.names);
    const subdivisionNames = objectValues(raw?.subdivisions?.[0]?.names);
    const countryIso = raw?.country?.iso_code ?? "";
    const subdivisionIso = raw?.subdivisions?.[0]?.iso_code ?? "";
    const cityGeonameId = raw?.city?.geoname_id ?? "";

    if (cityNames.length === 0) {
      continue;
    }

    const fallbackCity = cityNames[0] ?? "";
    const identity = [
      cityGeonameId || normalize(fallbackCity),
      normalize(countryIso || countryNames[0] || ""),
      normalize(subdivisionIso),
      normalize(cityNames.join("|"))
    ].join("|");

    if (!records.has(identity)) {
      records.set(identity, {
        cityGeonameId,
        cityNames,
        countryIso,
        countryNames,
        subdivisionIso,
        subdivisionNames,
        latitude: raw?.location?.latitude ?? "",
        longitude: raw?.location?.longitude ?? ""
      });
    }
  }

  return [...records.values()];
}

function enumerateDataPointers(reader) {
  const nodeCount = reader.metadata.nodeCount;
  const nodeByteSize = reader.metadata.nodeByteSize;
  const stack = [0];
  const seenNodes = new Set();
  const pointers = new Set();

  while (stack.length > 0) {
    const node = stack.pop();
    if (node == null || node >= nodeCount || seenNodes.has(node)) {
      continue;
    }
    seenNodes.add(node);
    const offset = node * nodeByteSize;
    for (const child of [reader.walker.left(offset), reader.walker.right(offset)]) {
      if (child < nodeCount) {
        stack.push(child);
      } else if (child > nodeCount) {
        pointers.add(child);
      }
    }
  }

  return pointers;
}

function buildIbgeIndexes(rows) {
  const raw = new Set();
  const normalized = new Set();
  const alias = new Set();
  const cityToStates = new Map();

  for (const row of rows) {
    if (!normalize(row.city) || !normalize(row.state)) {
      continue;
    }
    raw.add(`${row.city}|${row.state}`);
    normalized.add(`${normalize(row.city)}|${normalize(row.state)}`);
    alias.add(`${normalize(row.city)}|${canonicalBrazilState(row.state)}`);
    addMapSet(cityToStates, normalize(row.city), canonicalBrazilState(row.state).toUpperCase());
  }

  return { raw, normalized, alias, cityToStates };
}

function buildGhslIndexes(rows) {
  const raw = new Set();
  const normalized = new Set();
  const alias = new Set();
  const duplicateCounts = new Map();
  const cityToCountries = new Map();

  for (const row of rows) {
    if (!normalize(row.city) || !normalize(row.country)) {
      continue;
    }
    raw.add(`${row.city}|${row.country}`);
    normalized.add(`${normalize(row.city)}|${normalize(row.country)}`);
    const aliasKey = `${normalize(row.city)}|${canonicalCountry(row.country)}`;
    alias.add(aliasKey);
    duplicateCounts.set(aliasKey, (duplicateCounts.get(aliasKey) ?? 0) + 1);
    addMapSet(cityToCountries, normalize(row.city), row.country);
  }

  return { raw, normalized, alias, duplicateCounts, cityToCountries };
}

function auditIbge(records, indexes) {
  const summary = baseSummary(records.length);
  const misses = [];
  let unmatchedCityInOtherState = 0;
  let unmatchedCityAbsent = 0;

  for (const record of records) {
    if (!isBrazil(record)) {
      summary.notApplicable += 1;
      continue;
    }
    if (!record.subdivisionIso) {
      summary.noSubdivision += 1;
      misses.push(missRow(record, "no_subdivision"));
      continue;
    }

    const stage = matchIbgeStage(record, indexes);
    incrementStage(summary, stage);
    if (stage === "unmatched") {
      const candidateStates = ibgeCandidateStates(record, indexes);
      if (candidateStates.length > 0) {
        unmatchedCityInOtherState += 1;
      } else {
        unmatchedCityAbsent += 1;
      }
      misses.push(missRow(record, candidateStates.length > 0 ? "city_exists_in_ibge_other_state" : "city_not_in_ibge", {
        candidate_states: candidateStates.join(" | ")
      }));
    }
  }

  finalizeSummary(summary);
  summary.scope = "MaxMind city records where country is Brazil";
  summary.unmatchedCityInOtherState = unmatchedCityInOtherState;
  summary.unmatchedCityAbsent = unmatchedCityAbsent;
  return { summary, misses: topRows(misses) };
}

function auditGhsl(records, indexes) {
  const summary = baseSummary(records.length);
  const misses = [];
  let duplicateKeyMatches = 0;
  let unmatchedCityInOtherCountry = 0;
  let unmatchedCityAbsent = 0;

  for (const record of records) {
    if (record.countryNames.length === 0 && !record.countryIso) {
      summary.noCountry += 1;
      misses.push(missRow(record, "no_country"));
      continue;
    }

    const stage = matchGhslStage(record, indexes);
    incrementStage(summary, stage);
    if (stage === "alias" && ghslHasDuplicateAlias(record, indexes)) {
      duplicateKeyMatches += 1;
    }
    if (stage === "unmatched") {
      const candidateCountries = ghslCandidateCountries(record, indexes);
      if (candidateCountries.length > 0) {
        unmatchedCityInOtherCountry += 1;
      } else {
        unmatchedCityAbsent += 1;
      }
      misses.push(missRow(record, candidateCountries.length > 0 ? "city_exists_in_ghsl_other_country" : "city_not_in_ghsl", {
        candidate_countries: candidateCountries.join(" | ")
      }));
    }
  }

  finalizeSummary(summary);
  summary.scope = "All unique MaxMind city records";
  summary.duplicateAliasMatches = duplicateKeyMatches;
  summary.unmatchedCityInOtherCountry = unmatchedCityInOtherCountry;
  summary.unmatchedCityAbsent = unmatchedCityAbsent;
  return { summary, misses: topRows(misses) };
}

function calculatePipelineMetrics(records, ibgeIndexes, ghslIndexes) {
  const brazilRecords = records.filter(isBrazil);
  const ibgeHits = brazilRecords.filter((record) => record.subdivisionIso && matchIbgeStage(record, ibgeIndexes) !== "unmatched").length;
  const ghslBrazilHits = brazilRecords.filter((record) => matchGhslStage(record, ghslIndexes) !== "unmatched").length;
  const ghslGlobalHits = records.filter((record) => matchGhslStage(record, ghslIndexes) !== "unmatched").length;

  return {
    direction: "MaxMind -> datasets",
    normalization: "ASCII/no-mark, punctuation-insensitive, lowercase, whitespace-collapsed keys; non-ASCII-only names are ignored as match keys",
    metrics: {
      Ibge_metric: metric("IBGE intersection with MaxMind Brazilian cities / MaxMind Brazilian cities", ibgeHits, brazilRecords.length),
      Ghsl_BR_metric: metric("GHSL intersection with MaxMind Brazilian cities / MaxMind Brazilian cities", ghslBrazilHits, brazilRecords.length),
      Ghsl_metric: metric("GHSL intersection with MaxMind global city records / MaxMind global city records", ghslGlobalHits, records.length)
    }
  };
}

function metric(definition, hits, total) {
  return {
    definition,
    hits,
    total,
    misses: total - hits,
    rate: total === 0 ? 0 : Number((hits / total).toFixed(6))
  };
}

function matchIbgeStage(record, indexes) {
  const state = record.subdivisionIso;

  for (const city of record.cityNames) {
    if (!normalize(city)) continue;
    if (indexes.raw.has(`${city}|${state}`)) return "raw";
  }
  for (const city of record.cityNames) {
    if (!normalize(city)) continue;
    if (indexes.normalized.has(`${normalize(city)}|${normalize(state)}`)) return "normalized";
  }
  for (const city of record.cityNames) {
    if (!normalize(city)) continue;
    if (indexes.alias.has(`${normalize(city)}|${canonicalBrazilState(state)}`)) return "alias";
  }
  return "unmatched";
}

function matchGhslStage(record, indexes) {
  const countryVariants = unique([...record.countryNames, record.countryIso]);

  for (const city of record.cityNames) {
    if (!normalize(city)) continue;
    for (const country of countryVariants) {
      if (!canonicalCountry(country)) continue;
      if (indexes.raw.has(`${city}|${country}`)) return "raw";
    }
  }
  for (const city of record.cityNames) {
    if (!normalize(city)) continue;
    for (const country of countryVariants) {
      if (!canonicalCountry(country)) continue;
      if (indexes.normalized.has(`${normalize(city)}|${normalize(country)}`)) return "normalized";
    }
  }
  for (const city of record.cityNames) {
    if (!normalize(city)) continue;
    for (const country of countryVariants) {
      if (!canonicalCountry(country)) continue;
      if (indexes.alias.has(`${normalize(city)}|${canonicalCountry(country)}`)) return "alias";
    }
  }
  return "unmatched";
}

function ghslHasDuplicateAlias(record, indexes) {
  for (const city of record.cityNames) {
    if (!normalize(city)) continue;
    for (const country of unique([...record.countryNames, record.countryIso])) {
      if (!canonicalCountry(country)) continue;
      if ((indexes.duplicateCounts.get(`${normalize(city)}|${canonicalCountry(country)}`) ?? 0) > 1) {
        return true;
      }
    }
  }
  return false;
}

function ibgeCandidateStates(record, indexes) {
  const states = new Set();
  for (const city of record.cityNames) {
    if (!normalize(city)) continue;
    for (const state of indexes.cityToStates.get(normalize(city)) ?? []) {
      states.add(state);
    }
  }
  return [...states].sort();
}

function ghslCandidateCountries(record, indexes) {
  const countries = new Set();
  for (const city of record.cityNames) {
    if (!normalize(city)) continue;
    for (const country of indexes.cityToCountries.get(normalize(city)) ?? []) {
      countries.add(country);
    }
  }
  return [...countries].sort();
}

function addMapSet(map, key, value) {
  if (!key || !value) {
    return;
  }
  const set = map.get(key) ?? new Set();
  set.add(value);
  map.set(key, set);
}

function baseSummary(totalMaxMindCityRecords) {
  return {
    totalMaxMindCityRecords,
    notApplicable: 0,
    noCountry: 0,
    noSubdivision: 0,
    matchedRaw: 0,
    matchedNormalized: 0,
    matchedAlias: 0,
    unmatched: 0,
    matchableTotal: 0,
    matchedTotal: 0,
    matchRate: 0
  };
}

function incrementStage(summary, stage) {
  if (stage === "raw") summary.matchedRaw += 1;
  else if (stage === "normalized") summary.matchedNormalized += 1;
  else if (stage === "alias") summary.matchedAlias += 1;
  else summary.unmatched += 1;
}

function finalizeSummary(summary) {
  summary.matchedTotal = summary.matchedRaw + summary.matchedNormalized + summary.matchedAlias;
  summary.matchableTotal = summary.matchedTotal + summary.unmatched;
  summary.matchRate = summary.matchableTotal === 0 ? 0 : Number((summary.matchedTotal / summary.matchableTotal).toFixed(6));
}

function missRow(record, reason, extras = {}) {
  return {
    reason,
    city_names: record.cityNames.join(" | "),
    country_iso: record.countryIso,
    country_names: record.countryNames.join(" | "),
    subdivision_iso: record.subdivisionIso,
    subdivision_names: record.subdivisionNames.join(" | "),
    city_geoname_id: String(record.cityGeonameId ?? ""),
    latitude: String(record.latitude ?? ""),
    longitude: String(record.longitude ?? ""),
    candidate_states: "",
    candidate_countries: "",
    ...extras
  };
}

function isBrazil(record) {
  return canonicalCountry(record.countryIso) === "brazil" || record.countryNames.some((name) => canonicalCountry(name) === "brazil");
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

function canonicalCountry(value) {
  const normalized = normalize(value);
  return countryAliases.get(normalized) ?? normalized;
}

function canonicalBrazilState(value) {
  const normalized = normalize(value);
  return brazilStateAliases.get(normalized) ?? normalized;
}

function objectValues(value) {
  return unique(Object.values(value ?? {}).filter((item) => typeof item === "string" && item.trim()));
}

function unique(values) {
  return [...new Set(values.filter(Boolean))];
}

function topRows(rows) {
  return rows.sort((left, right) => left.country_iso.localeCompare(right.country_iso) || left.city_names.localeCompare(right.city_names));
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
  const headers = [
    "reason",
    "city_names",
    "country_iso",
    "country_names",
    "subdivision_iso",
    "subdivision_names",
    "city_geoname_id",
    "latitude",
    "longitude",
    "candidate_states",
    "candidate_countries"
  ];
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

function printSummary(label, summary) {
  console.log(`${label}: matched ${summary.matchedTotal}/${summary.matchableTotal} (${(summary.matchRate * 100).toFixed(2)}%)`);
  console.log(`  raw=${summary.matchedRaw} normalized=${summary.matchedNormalized} alias=${summary.matchedAlias} unmatched=${summary.unmatched}`);
}

function printPipelineMetrics(summary) {
  console.log("Pipeline metrics (MaxMind -> datasets, normalized):");
  for (const [name, metric] of Object.entries(summary.metrics)) {
    console.log(`  ${name}: ${metric.hits}/${metric.total} (${(metric.rate * 100).toFixed(2)}%)`);
  }
}

function optionValue(name) {
  const index = process.argv.indexOf(name);
  if (index === -1) return null;
  const value = process.argv[index + 1];
  return value && !value.startsWith("--") ? value : null;
}
