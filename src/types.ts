export type MeridianSource = "maxmind" | "ibge" | "ghsl";

export type MeridianOpenOptions = {
  dataDir?: string;
  sources?: MeridianSource[];
  strict?: boolean;
};

export type MeridianSourcesStatus = Record<MeridianSource, boolean>;

export type MeridianFileMetadata = {
  source: MeridianSource;
  path: string;
  exists: boolean;
  sizeBytes: number | null;
  mtimeMs: number | null;
};

export type MeridianMetadata = {
  dataDir: string;
  sources: MeridianSourcesStatus;
  files: MeridianFileMetadata[];
};

export type MeridianJsonPrimitive = string | number | boolean | null;
export type MeridianJsonValue =
  | MeridianJsonPrimitive
  | MeridianJsonValue[]
  | { [key: string]: MeridianJsonValue };

export type MeridianIpResult = {
  source: "maxmind";
  ip: string;
  city: {
    name: string | null;
    geonameId: number | null;
    latitude: number | null;
    longitude: number | null;
    timeZone: string | null;
  };
  subdivision: {
    isoCode: string | null;
    name: string | null;
    geonameId: number | null;
  };
  country: {
    isoCode: string | null;
    name: string | null;
    geonameId: number | null;
  };
  asn: {
    autonomousSystemNumber: number | null;
    autonomousSystemOrganization: string | null;
    network: string | null;
  };
};

export type MeridianIpRawResult = {
  city: MeridianJsonValue | null;
  country: MeridianJsonValue | null;
  asn: MeridianJsonValue | null;
};

export type MeridianIpCityResult = MeridianIpResult & {
  ibge: IbgeMunicipalityIncome | null;
  ghsl: GhslCityMetrics | null;
};

export type IbgeMunicipalityIncome = {
  source: "ibge";
  city: string;
  state: string;
  country: "Brazil";
  municipalityCode: string;
  income: {
    meanMonthlyHouseholdPerCapitaBrl2022: number | null;
    medianMonthlyHouseholdPerCapitaBrl2022: number | null;
  };
};

export type GhslCityMetrics = {
  source: "ghsl";
  city: string;
  country: string;
  urbanCentreId: string;
  worldRegion: string | null;
  worldBankIncomeGroup: string | null;
  urbanAreaKm2_2025: number | null;
  population_2025: number | null;
  hdi_2020: number | null;
};
