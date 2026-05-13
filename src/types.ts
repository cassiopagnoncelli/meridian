export type MeridianSource = "maxmind" | "ibge" | "ghsl";

export type MeridianOpenOptions = {
  dataDir?: string;
  sources?: MeridianSource[];
  strict?: boolean;
};

export type MeridianSourcesStatus = Record<MeridianSource, boolean>;

export type MeridianIpResult = {
  source: "maxmind";
  ip: string;
  city: {
    name: string | null;
    geonameId: number | null;
    latitude: number | null;
    longitude: number | null;
    timeZone: string | null;
    raw: unknown | null;
  };
  country: {
    isoCode: string | null;
    name: string | null;
    geonameId: number | null;
    raw: unknown | null;
  };
  asn: {
    autonomousSystemNumber: number | null;
    autonomousSystemOrganization: string | null;
    network: string | null;
    raw: unknown | null;
  };
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
