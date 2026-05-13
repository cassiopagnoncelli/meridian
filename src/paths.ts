import { join } from "node:path";

import type { MeridianSource } from "./types";

export const ALL_SOURCES: MeridianSource[] = ["maxmind", "ibge", "ghsl"];

export type RequiredFile = {
  source: MeridianSource;
  path: string;
};

export function defaultDataDir(): string {
  return join(process.cwd(), "lib", "meridian");
}

export function requiredFiles(dataDir: string, sources: MeridianSource[]): RequiredFile[] {
  const files: RequiredFile[] = [];

  if (sources.includes("maxmind")) {
    files.push(
      { source: "maxmind", path: join(dataDir, "maxmind", "GeoLite2-City.mmdb") },
      { source: "maxmind", path: join(dataDir, "maxmind", "GeoLite2-Country.mmdb") },
      { source: "maxmind", path: join(dataDir, "maxmind", "GeoLite2-ASN.mmdb") }
    );
  }
  if (sources.includes("ibge")) {
    files.push({
      source: "ibge",
      path: join(dataDir, "ibge", "ibge_municipality_income.csv")
    });
  }
  if (sources.includes("ghsl")) {
    files.push({
      source: "ghsl",
      path: join(dataDir, "ghsl", "ghsl_city_metrics.csv")
    });
  }

  return files;
}
