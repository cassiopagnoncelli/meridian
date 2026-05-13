#!/usr/bin/env node
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";

import maxmind from "maxmind";
import { parse } from "csv-parse/sync";

const IBGE_ALIAS_HEADERS = ["alias_city", "state", "ibge_municipality_code"];
const GHSL_ALIAS_HEADERS = ["alias_city", "country", "ghsl_urban_centre_id"];

if (hasFlag("--print-headers")) {
  console.log(JSON.stringify({ ibge: IBGE_ALIAS_HEADERS, ghsl: GHSL_ALIAS_HEADERS }));
  process.exit(0);
}

const dataDir = resolve(optionValue("--data-dir") ?? join(process.cwd(), "lib", "meridian"));
const outputDir = resolve(optionValue("--output-dir") ?? dataDir);
const reportsDir = resolve(optionValue("--reports-dir") ?? join(process.cwd(), "reports", "compatibility"));

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

const ibge = buildIbgeIndex(ibgeRows);
const ghsl = buildGhslIndex(ghslRows);
const ibgeCompatibility = buildIbgeAliases(maxmindRecords, ibge);
const ghslCompatibility = buildGhslAliases(maxmindRecords, ghsl);

await writeCsv(join(outputDir, "ibge", "ibge_city_aliases.csv"), IBGE_ALIAS_HEADERS, ibgeCompatibility.aliases);
await writeCsv(join(outputDir, "ghsl", "ghsl_city_aliases.csv"), GHSL_ALIAS_HEADERS, ghslCompatibility.aliases);
await writeJson(join(reportsDir, "ibge_compatibility_summary.json"), ibgeCompatibility.summary);
await writeJson(join(reportsDir, "ghsl_compatibility_summary.json"), ghslCompatibility.summary);

console.log(`MaxMind unique city records: ${maxmindRecords.length}`);
console.log(`IBGE aliases: ${ibgeCompatibility.aliases.length}`);
console.log(`GHSL aliases: ${ghslCompatibility.aliases.length}`);
console.log(`Wrote alias files to ${outputDir}`);
console.log(`Wrote compatibility reports to ${reportsDir}`);

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
        subdivisionNames
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

function buildIbgeIndex(rows) {
  const byKey = new Map();

  for (const row of rows) {
    const city = row.city?.trim();
    const state = row.state?.trim();
    const code = row.ibge_municipality_code?.trim();
    if (city && state && code) {
      byKey.set(ibgeKey(city, state), { city, state, code });
    }
  }

  return { byKey };
}

function buildGhslIndex(rows) {
  const byKey = new Map();

  for (const row of rows) {
    const city = row.city?.trim();
    const country = row.country?.trim();
    const id = row.ghsl_urban_centre_id?.trim();
    const population = nullableNumber(row.population_2025);
    if (city && country && id) {
      const key = ghslKey(city, country);
      const bucket = byKey.get(key) ?? [];
      bucket.push({ city, country, id, population });
      bucket.sort((left, right) => (right.population ?? -1) - (left.population ?? -1));
      byKey.set(key, bucket);
    }
  }

  return { byKey };
}

function buildIbgeAliases(records, index) {
  const aliases = new Map();
  const summary = {
    scope: "MaxMind Brazil city records matched to IBGE municipalities",
    maxmindRecords: records.length,
    matchableBrazilRecords: 0,
    matchedRecords: 0,
    unmatchedRecords: 0,
    aliases: 0
  };

  for (const record of records) {
    if (!isBrazil(record) || !record.subdivisionIso) {
      continue;
    }

    summary.matchableBrazilRecords += 1;
    const match = findIbgeMatch(record, index);
    if (!match) {
      summary.unmatchedRecords += 1;
      continue;
    }

    summary.matchedRecords += 1;
    for (const city of record.cityNames) {
      if (!normalize(city)) {
        continue;
      }
      const row = {
        alias_city: city,
        state: match.state,
        ibge_municipality_code: match.code
      };
      aliases.set(`${normalize(row.alias_city)}|${normalize(row.state)}|${row.ibge_municipality_code}`, row);
    }
  }

  const rows = sortedRows([...aliases.values()], ["state", "alias_city", "ibge_municipality_code"]);
  summary.aliases = rows.length;
  return { aliases: rows, summary };
}

function buildGhslAliases(records, index) {
  const aliases = new Map();
  const summary = {
    scope: "MaxMind city records matched to GHSL urban centres",
    maxmindRecords: records.length,
    matchedRecords: 0,
    unmatchedRecords: 0,
    aliases: 0
  };

  for (const record of records) {
    const match = findGhslMatch(record, index);
    if (!match) {
      summary.unmatchedRecords += 1;
      continue;
    }

    summary.matchedRecords += 1;
    const countries = unique([match.country, record.countryIso, ...record.countryNames]);
    for (const city of record.cityNames) {
      if (!normalize(city)) {
        continue;
      }
      for (const country of countries) {
        if (!canonicalCountry(country)) {
          continue;
        }
        const row = {
          alias_city: city,
          country,
          ghsl_urban_centre_id: match.id
        };
        aliases.set(`${normalize(row.alias_city)}|${canonicalCountry(row.country)}|${row.ghsl_urban_centre_id}`, row);
      }
    }
  }

  const rows = sortedRows([...aliases.values()], ["country", "alias_city", "ghsl_urban_centre_id"]);
  summary.aliases = rows.length;
  return { aliases: rows, summary };
}

function findIbgeMatch(record, index) {
  for (const city of record.cityNames) {
    if (!normalize(city)) {
      continue;
    }
    const match = index.byKey.get(ibgeKey(city, record.subdivisionIso));
    if (match) {
      return match;
    }
  }
  return null;
}

function findGhslMatch(record, index) {
  const countries = unique([...record.countryNames, record.countryIso]);
  for (const city of record.cityNames) {
    if (!normalize(city)) {
      continue;
    }
    for (const country of countries) {
      if (!canonicalCountry(country)) {
        continue;
      }
      const match = index.byKey.get(ghslKey(city, country))?.[0];
      if (match) {
        return match;
      }
    }
  }
  return null;
}

function ibgeKey(city, state) {
  return `${normalize(city)}|${canonicalBrazilState(state)}`;
}

function ghslKey(city, country) {
  return `${normalize(city)}|${canonicalCountry(country)}`;
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

function nullableNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function objectValues(value) {
  return unique(Object.values(value ?? {}).filter((item) => typeof item === "string" && item.trim()));
}

function unique(values) {
  return [...new Set(values.filter(Boolean))];
}

function sortedRows(rows, fields) {
  return rows.sort((left, right) => {
    for (const field of fields) {
      const comparison = String(left[field] ?? "").localeCompare(String(right[field] ?? ""));
      if (comparison !== 0) {
        return comparison;
      }
    }
    return 0;
  });
}

async function csvRows(path) {
  const content = await readFile(path, "utf8");
  return parse(content, { bom: true, columns: true, skip_empty_lines: true });
}

async function writeJson(path, value) {
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, `${JSON.stringify(value, null, 2)}\n`, "utf8");
}

async function writeCsv(path, headers, rows) {
  await mkdir(dirname(path), { recursive: true });
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

function optionValue(name) {
  const index = process.argv.indexOf(name);
  if (index === -1) return null;
  const value = process.argv[index + 1];
  return value && !value.startsWith("--") ? value : null;
}

function hasFlag(name) {
  return process.argv.includes(name);
}
