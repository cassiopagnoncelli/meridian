#!/usr/bin/env node
import { execFile } from "node:child_process";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { promisify } from "node:util";

import maxmind from "maxmind";
import { parse } from "csv-parse/sync";

const execFileAsync = promisify(execFile);
const MAP_HEADERS = [
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
];

const countryAliases = new Map([
  ["brasil", "brazil"],
  ["br", "brazil"],
  ["gb", "united kingdom"],
  ["uk", "united kingdom"],
  ["u k", "united kingdom"],
  ["great britain", "united kingdom"],
  ["britain", "united kingdom"],
  ["england", "united kingdom"],
  ["tr", "turkey"],
  ["turkiye", "turkey"],
  ["turquia", "turkey"],
  ["turquie", "turkey"],
  ["turkei", "turkey"],
  ["türkiye", "turkey"],
  ["türkei", "turkey"],
  ["us", "united states"],
  ["u s", "united states"],
  ["usa", "united states"],
  ["u s a", "united states"],
  ["united states of america", "united states"],
  ["cd", "democratic republic of the congo"],
  ["congo kinshasa", "democratic republic of the congo"],
  ["dr congo", "democratic republic of the congo"],
  ["drc", "democratic republic of the congo"],
  ["cg", "republic of the congo"],
  ["congo brazzaville", "republic of the congo"],
  ["hk", "china"],
  ["hong kong sar china", "china"],
  ["ru", "russia"],
  ["russian federation", "russia"],
  ["kr", "south korea"],
  ["korea republic of", "south korea"],
  ["kp", "north korea"],
  ["ir", "iran"],
  ["iran islamic republic of", "iran"],
  ["vn", "vietnam"],
  ["viet nam", "vietnam"],
  ["la", "laos"],
  ["lao people s democratic republic", "laos"],
  ["sy", "syria"],
  ["syrian arab republic", "syria"],
  ["bo", "bolivia"],
  ["bolivia plurinational state of", "bolivia"],
  ["tz", "tanzania"],
  ["tanzania united republic of", "tanzania"],
  ["md", "moldova"],
  ["moldova republic of", "moldova"],
  ["bn", "brunei"],
  ["brunei darussalam", "brunei"]
]);

if (hasFlag("--print-headers")) {
  console.log(JSON.stringify({ ghsl_geoname_map: MAP_HEADERS }));
  process.exit(0);
}

const dataDir = resolve(optionValue("--data-dir") ?? join(process.cwd(), "lib", "meridian"));
const outputDir = resolve(optionValue("--output-dir") ?? dataDir);
const reportsDir = resolve(optionValue("--reports-dir") ?? join(process.cwd(), "reports", "compatibility"));
const ghslGpkg = resolve(
  optionValue("--ghsl-gpkg") ??
    join(process.cwd(), "datasets", "raw", "ghs_socio", "GHS_UCDB_THEME_SOCIOECONOMIC_GLOBE_R2024A.gpkg")
);
const nearestKm = Number(optionValue("--nearest-km") ?? "25");
const nameMaxDistanceKm = Number(optionValue("--name-max-km") ?? "100");

const workDir = await mkdtemp(join(tmpdir(), "meridian-ghsl-map-"));
try {
  const polygonsPath = join(workDir, "ghsl_polygons.geojson");
  const centroidsPath = join(workDir, "ghsl_centroids.geojson");

  await exportGhslGeoJson(ghslGpkg, polygonsPath, centroidsPath);

  const ghslRows = await csvRows(join(dataDir, "ghsl", "ghsl_city_metrics.csv"));
  const ghsl = buildGhslIndex(ghslRows);
  const polygons = buildPolygonIndex(await jsonFile(polygonsPath), ghsl.byId);
  const centroids = buildCentroidIndex(await jsonFile(centroidsPath), ghsl.byId);
  const cityReader = await maxmind.open(join(dataDir, "maxmind", "GeoLite2-City.mmdb"));
  const maxmindRecords = enumerateMaxMindCityRecords(cityReader);
  const result = buildGeonameMap(maxmindRecords, ghsl, polygons, centroids);

  await writeCsv(join(outputDir, "ghsl", "ghsl_geoname_map.csv"), MAP_HEADERS, result.rows);
  await writeCsv(join(reportsDir, "ghsl_geoname_map_review.csv"), MAP_HEADERS, result.reviewRows);
  await writeJson(join(reportsDir, "ghsl_geoname_map_summary.json"), result.summary);

  console.log(`MaxMind unique city records: ${maxmindRecords.length}`);
  console.log(`GHSL geoname map rows: ${result.rows.length}`);
  console.log(`GHSL geoname review rows: ${result.reviewRows.length}`);
  console.log(`Coverage: ${result.summary.hits}/${result.summary.total} (${(result.summary.hitRate * 100).toFixed(2)}%)`);
  console.log(`Wrote GHSL geoname map to ${join(outputDir, "ghsl", "ghsl_geoname_map.csv")}`);
  console.log(`Wrote reports to ${reportsDir}`);
} finally {
  await rm(workDir, { recursive: true, force: true });
}

async function exportGhslGeoJson(gpkgPath, polygonsPath, centroidsPath) {
  await execFileAsync("ogr2ogr", [
    "-f",
    "GeoJSON",
    "-t_srs",
    "EPSG:4326",
    polygonsPath,
    gpkgPath,
    "GHS_UCDB_THEME_SOCIOECONOMIC_GLOBE_R2024A",
    "-select",
    "ID_UC_G0,GC_UCN_MAI_2025,GC_CNT_GAD_2025"
  ]);
  await execFileAsync("ogr2ogr", [
    "-f",
    "GeoJSON",
    "-t_srs",
    "EPSG:4326",
    centroidsPath,
    gpkgPath,
    "UC_centroids",
    "-select",
    "ID_UC_G0"
  ]);
}

function buildGhslIndex(rows) {
  const byName = new Map();
  const byId = new Map();

  for (const row of rows) {
    const id = row.ghsl_urban_centre_id?.trim();
    const city = row.city?.trim();
    const country = row.country?.trim();
    const population = nullableNumber(row.population_2025);
    if (!id || !city || !country) {
      continue;
    }

    const record = { id, city, country, population };
    byId.set(id, record);
    addMapList(byName, ghslKey(city, country), record);
  }

  for (const records of byName.values()) {
    records.sort((left, right) => (right.population ?? -1) - (left.population ?? -1));
  }

  return { byName, byId };
}

function buildPolygonIndex(geojson, recordsById) {
  const features = [];
  const grid = new Map();

  for (const feature of geojson.features ?? []) {
    const id = String(feature.properties?.ID_UC_G0 ?? "");
    const record = recordsById.get(id);
    if (!id || !record || !feature.geometry) {
      continue;
    }

    const bbox = geometryBbox(feature.geometry);
    const indexed = {
      id,
      city: record.city,
      country: record.country,
      population: record.population,
      geometry: feature.geometry,
      bbox
    };
    features.push(indexed);
    for (const cell of bboxCells(bbox)) {
      addMapList(grid, cell, indexed);
    }
  }

  return { features, grid };
}

function buildCentroidIndex(geojson, recordsById) {
  const byCountry = new Map();
  const byId = new Map();
  for (const feature of geojson.features ?? []) {
    const id = String(feature.properties?.ID_UC_G0 ?? "");
    const record = recordsById.get(id);
    const coordinates = feature.geometry?.coordinates;
    if (!id || !record || !Array.isArray(coordinates) || coordinates.length < 2) {
      continue;
    }

    const centroid = {
      id,
      city: record.city,
      country: record.country,
      population: record.population,
      longitude: Number(coordinates[0]),
      latitude: Number(coordinates[1])
    };
    byId.set(id, centroid);
    addMapList(byCountry, canonicalCountry(record.country), centroid);
  }
  return { byCountry, byId };
}

function buildGeonameMap(records, ghsl, polygons, centroids) {
  const rows = [];
  const reviewRows = [];
  const counts = new Map();

  for (const record of records) {
    const match =
      findPolygonMatch(record, polygons) ??
      findNameMatch(record, ghsl, centroids) ??
      findNearestCentroidMatch(record, centroids) ??
      findAdminNameMatch(record, ghsl);

    if (!match) {
      reviewRows.push(mapRow(record, null, "unmatched", "review", "", "No GHSL candidate found"));
      increment(counts, "unmatched");
      continue;
    }

    const row = mapRow(record, match, match.method, match.confidence, match.distanceKm ?? "", match.notes ?? "");
    if (match.confidence === "review") {
      reviewRows.push(row);
      increment(counts, `${match.method}_review`);
      continue;
    }

    rows.push(row);
    increment(counts, match.method);
  }

  rows.sort((left, right) => left.maxmind_geoname_id.localeCompare(right.maxmind_geoname_id));
  reviewRows.sort((left, right) => left.country_iso.localeCompare(right.country_iso) || left.maxmind_city_names.localeCompare(right.maxmind_city_names));

  const summary = {
    direction: "MaxMind -> GHSL",
    total: records.length,
    hits: rows.length,
    misses: reviewRows.length,
    hitRate: ratio(rows.length, records.length),
    nearestKm,
    nameMaxDistanceKm,
    methods: Object.fromEntries([...counts.entries()].sort((left, right) => left[0].localeCompare(right[0])))
  };

  return { rows, reviewRows, summary };
}

function findNameMatch(record, ghsl, centroids) {
  const match = findGhslByNames(record.cityNames, countryVariants(record), ghsl, "name", "high", "MaxMind city name matched GHSL city");
  if (!match) {
    return null;
  }

  const point = recordPoint(record);
  const centroid = centroids.byId.get(match.id);
  if (!point || !centroid) {
    return match;
  }

  const distanceKm = haversineKm(point.latitude, point.longitude, centroid.latitude, centroid.longitude);
  if (distanceKm > nameMaxDistanceKm) {
    return {
      ...match,
      confidence: "review",
      distanceKm,
      notes: `City-name match is ${distanceKm.toFixed(1)}km from GHSL centroid; review duplicate-name risk`
    };
  }

  return { ...match, distanceKm };
}

function findAdminNameMatch(record, ghsl) {
  return findGhslByNames(
    record.subdivisionNames,
    countryVariants(record),
    ghsl,
    "admin_name",
    "review",
    "MaxMind subdivision/admin name matched GHSL city; review before production use"
  );
}

function findGhslByNames(cities, countries, ghsl, method, confidence, notes) {
  for (const city of cities) {
    if (!normalize(city)) {
      continue;
    }
    for (const country of countries) {
      const record = ghsl.byName.get(ghslKey(city, country))?.[0];
      if (record) {
        return { ...record, method, confidence, notes };
      }
    }
  }
  return null;
}

function findPolygonMatch(record, polygons) {
  const point = recordPoint(record);
  if (!point) {
    return null;
  }

  const candidates = polygons.grid.get(gridCell(point.longitude, point.latitude)) ?? [];
  const matches = candidates.filter(
    (candidate) => bboxContains(candidate.bbox, point.longitude, point.latitude) && geometryContainsPoint(candidate.geometry, point.longitude, point.latitude)
  );
  if (matches.length === 0) {
    return null;
  }

  matches.sort((left, right) => (right.population ?? -1) - (left.population ?? -1));
  return {
    ...matches[0],
    method: "polygon_contains",
    confidence: "high",
    distanceKm: 0,
    notes: "MaxMind point is inside GHSL urban-centre polygon"
  };
}

function findNearestCentroidMatch(record, centroids) {
  const point = recordPoint(record);
  if (!point) {
    return null;
  }

  const countries = countryVariants(record).map(canonicalCountry);
  const candidates = unique(countries).flatMap((country) => centroids.byCountry.get(country) ?? []);
  let best = null;
  for (const candidate of candidates) {
    const distanceKm = haversineKm(point.latitude, point.longitude, candidate.latitude, candidate.longitude);
    if (distanceKm <= nearestKm && (!best || distanceKm < best.distanceKm)) {
      best = { ...candidate, distanceKm };
    }
  }

  return best
    ? {
        ...best,
        method: "nearest_centroid",
        confidence: best.distanceKm <= 10 ? "medium" : "review",
        notes: `Nearest same-country GHSL centroid within ${nearestKm}km`
      }
    : null;
}

function mapRow(record, match, method, confidence, distanceKm, notes) {
  return {
    maxmind_geoname_id: String(record.cityGeonameId ?? ""),
    country_iso: record.countryIso,
    subdivision_iso: record.subdivisionIso,
    ghsl_urban_centre_id: match?.id ?? "",
    method,
    confidence,
    distance_km: typeof distanceKm === "number" ? distanceKm.toFixed(3) : String(distanceKm ?? ""),
    maxmind_city_names: record.cityNames.join(" | "),
    maxmind_subdivision_names: record.subdivisionNames.join(" | "),
    ghsl_city: match?.city ?? "",
    ghsl_country: match?.country ?? "",
    notes
  };
}

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

    if (cityNames.length === 0 || !cityGeonameId) {
      continue;
    }

    const identity = [
      cityGeonameId,
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
        latitude: raw?.location?.latitude ?? null,
        longitude: raw?.location?.longitude ?? null
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

function geometryContainsPoint(geometry, longitude, latitude) {
  if (geometry.type === "Polygon") {
    return polygonContainsPoint(geometry.coordinates, longitude, latitude);
  }
  if (geometry.type === "MultiPolygon") {
    return geometry.coordinates.some((polygon) => polygonContainsPoint(polygon, longitude, latitude));
  }
  return false;
}

function polygonContainsPoint(rings, longitude, latitude) {
  if (!rings?.[0] || !ringContainsPoint(rings[0], longitude, latitude)) {
    return false;
  }
  return !rings.slice(1).some((ring) => ringContainsPoint(ring, longitude, latitude));
}

function ringContainsPoint(ring, longitude, latitude) {
  let inside = false;
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i, i += 1) {
    const xi = ring[i][0];
    const yi = ring[i][1];
    const xj = ring[j][0];
    const yj = ring[j][1];
    const intersects = yi > latitude !== yj > latitude && longitude < ((xj - xi) * (latitude - yi)) / (yj - yi) + xi;
    if (intersects) inside = !inside;
  }
  return inside;
}

function geometryBbox(geometry) {
  const bbox = [Infinity, Infinity, -Infinity, -Infinity];
  visitCoordinates(geometry.coordinates, (coordinate) => {
    bbox[0] = Math.min(bbox[0], coordinate[0]);
    bbox[1] = Math.min(bbox[1], coordinate[1]);
    bbox[2] = Math.max(bbox[2], coordinate[0]);
    bbox[3] = Math.max(bbox[3], coordinate[1]);
  });
  return bbox;
}

function visitCoordinates(value, visitor) {
  if (typeof value?.[0] === "number" && typeof value?.[1] === "number") {
    visitor(value);
    return;
  }
  for (const item of value ?? []) {
    visitCoordinates(item, visitor);
  }
}

function bboxCells(bbox) {
  const cells = [];
  for (let lon = Math.floor(bbox[0]); lon <= Math.floor(bbox[2]); lon += 1) {
    for (let lat = Math.floor(bbox[1]); lat <= Math.floor(bbox[3]); lat += 1) {
      cells.push(`${lon}|${lat}`);
    }
  }
  return cells;
}

function bboxContains(bbox, longitude, latitude) {
  return longitude >= bbox[0] && longitude <= bbox[2] && latitude >= bbox[1] && latitude <= bbox[3];
}

function gridCell(longitude, latitude) {
  return `${Math.floor(longitude)}|${Math.floor(latitude)}`;
}

function recordPoint(record) {
  const latitude = Number(record.latitude);
  const longitude = Number(record.longitude);
  return Number.isFinite(latitude) && Number.isFinite(longitude) ? { latitude, longitude } : null;
}

function haversineKm(lat1, lon1, lat2, lon2) {
  const radiusKm = 6371;
  const dLat = toRadians(lat2 - lat1);
  const dLon = toRadians(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRadians(lat1)) * Math.cos(toRadians(lat2)) * Math.sin(dLon / 2) ** 2;
  return 2 * radiusKm * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function toRadians(value) {
  return (value * Math.PI) / 180;
}

function ghslKey(city, country) {
  return `${normalize(city)}|${canonicalCountry(country)}`;
}

function countryVariants(record) {
  return unique([
    record.countryIso,
    isoCountryName(record.countryIso),
    ...record.countryNames
  ]).map(canonicalCountry).filter(Boolean);
}

function isoCountryName(value) {
  const code = String(value ?? "").trim().toUpperCase();
  if (!/^[A-Z]{2}$/.test(code)) {
    return "";
  }
  try {
    return new Intl.DisplayNames(["en"], { type: "region" }).of(code) ?? "";
  } catch {
    return "";
  }
}

function canonicalCountry(value) {
  const normalized = normalize(value);
  return countryAliases.get(normalized) ?? normalized;
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

function objectValues(value) {
  return unique(Object.values(value ?? {}).filter((item) => typeof item === "string" && item.trim()));
}

function addMapList(map, key, value) {
  if (!key) return;
  const list = map.get(key) ?? [];
  list.push(value);
  map.set(key, list);
}

function increment(map, key) {
  map.set(key, (map.get(key) ?? 0) + 1);
}

function nullableNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function ratio(numerator, denominator) {
  return denominator === 0 ? 0 : Number((numerator / denominator).toFixed(6));
}

function unique(values) {
  return [...new Set(values.filter(Boolean))];
}

async function csvRows(path) {
  const content = await readFile(path, "utf8");
  return parse(content, { bom: true, columns: true, skip_empty_lines: true });
}

async function jsonFile(path) {
  return JSON.parse(await readFile(path, "utf8"));
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
