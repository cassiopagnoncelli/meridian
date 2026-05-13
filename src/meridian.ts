import { access } from "node:fs/promises";
import { isIP } from "node:net";
import { join } from "node:path";

import { MeridianDataError, MeridianInputError } from "./errors";
import { loadGhsl, type GhslIndex } from "./loaders/ghsl";
import { loadIbge, type IbgeIndex } from "./loaders/ibge";
import { loadMaxMind, lookupIp, type MaxMindReaders } from "./loaders/maxmind";
import { cityCountryKey, cityStateKey } from "./normalize";
import { ALL_SOURCES, defaultDataDir, requiredFiles, type RequiredFile } from "./paths";
import type {
  GhslCityMetrics,
  IbgeMunicipalityIncome,
  MeridianIpResult,
  MeridianOpenOptions,
  MeridianSource,
  MeridianSourcesStatus
} from "./types";

type LoadedSources = {
  maxmind: MaxMindReaders | null;
  ibge: IbgeIndex | null;
  ghsl: GhslIndex | null;
};

export class Meridian {
  readonly dataDir: string;
  #maxmind: MaxMindReaders | null;
  #ibge: IbgeIndex | null;
  #ghsl: GhslIndex | null;

  private constructor(dataDir: string, loaded: LoadedSources) {
    this.dataDir = dataDir;
    this.#maxmind = loaded.maxmind;
    this.#ibge = loaded.ibge;
    this.#ghsl = loaded.ghsl;
  }

  static async open(options: MeridianOpenOptions = {}): Promise<Meridian> {
    const dataDir = options.dataDir ?? defaultDataDir();
    const strict = options.strict ?? true;
    const selectedSources = normalizeSources(options.sources ?? ALL_SOURCES);
    const availableSources = await resolveAvailableSources(dataDir, selectedSources, strict);

    const [maxmind, ibge, ghsl] = await Promise.all([
      availableSources.includes("maxmind") ? loadMaxMind(dataDir) : Promise.resolve(null),
      availableSources.includes("ibge")
        ? loadIbge(join(dataDir, "ibge", "ibge_municipality_income.csv"))
        : Promise.resolve(null),
      availableSources.includes("ghsl")
        ? loadGhsl(join(dataDir, "ghsl", "ghsl_city_metrics.csv"))
        : Promise.resolve(null)
    ]);

    return new Meridian(dataDir, { maxmind, ibge, ghsl });
  }

  ip(ipAddress: string): MeridianIpResult | null {
    if (!isIP(ipAddress)) {
      throw new MeridianInputError(`Invalid IP address: ${ipAddress}`);
    }
    if (!this.#maxmind) {
      return null;
    }
    return lookupIp(this.#maxmind, ipAddress);
  }

  ibge(city: string, state: string): IbgeMunicipalityIncome | null {
    const record = this.#ibge?.get(cityStateKey(city, state));
    return record ? cloneIbge(record) : null;
  }

  ghsl(city: string, country: string): GhslCityMetrics | null {
    const records = this.#ghsl?.get(cityCountryKey(city, country));
    const record = records?.[0];
    return record ? { ...record } : null;
  }

  sources(): MeridianSourcesStatus {
    return {
      maxmind: this.#maxmind !== null,
      ibge: this.#ibge !== null,
      ghsl: this.#ghsl !== null
    };
  }

  close(): void {
    this.#ibge?.clear();
    this.#ghsl?.clear();
    this.#maxmind = null;
    this.#ibge = null;
    this.#ghsl = null;
  }
}

function cloneIbge(record: IbgeMunicipalityIncome): IbgeMunicipalityIncome {
  return {
    ...record,
    income: { ...record.income }
  };
}

function normalizeSources(sources: MeridianSource[]): MeridianSource[] {
  const invalid = sources.filter((source) => !ALL_SOURCES.includes(source));
  if (invalid.length > 0) {
    throw new MeridianInputError(`Unknown Meridian source(s): ${invalid.join(", ")}`);
  }
  return [...new Set(sources)];
}

async function resolveAvailableSources(
  dataDir: string,
  selectedSources: MeridianSource[],
  strict: boolean
): Promise<MeridianSource[]> {
  const files = requiredFiles(dataDir, selectedSources);
  const missing = await missingFiles(files);

  if (missing.length > 0 && strict) {
    throw new MeridianDataError(
      `Missing Meridian data file(s): ${missing.map((file) => file.path).join(", ")}`
    );
  }

  if (missing.length === 0) {
    return selectedSources;
  }

  const missingSources = new Set(missing.map((file) => file.source));
  return selectedSources.filter((source) => !missingSources.has(source));
}

async function missingFiles(files: RequiredFile[]): Promise<RequiredFile[]> {
  const checks = await Promise.all(
    files.map(async (file) => {
      try {
        await access(file.path);
        return null;
      } catch {
        return file;
      }
    })
  );
  return checks.filter((file): file is RequiredFile => file !== null);
}
