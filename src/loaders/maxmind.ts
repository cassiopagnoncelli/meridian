import { isIP } from "node:net";
import { join } from "node:path";

import {
  AddressNotFoundError,
  Reader,
  ValueError,
  type Asn,
  type City,
  type Country,
  type ReaderModel
} from "@maxmind/geoip2-node";

import { MeridianInputError } from "../errors";
import type { MeridianIpRawResult, MeridianIpResult, MeridianJsonValue } from "../types";

export type MaxMindReaders = {
  city: ReaderModel;
  country: ReaderModel;
  asn: ReaderModel;
};

export type MaxMindLookup = {
  city: City | null;
  country: Country | null;
  asn: Asn | null;
};

export async function loadMaxMind(dataDir: string): Promise<MaxMindReaders> {
  const maxmindDir = join(dataDir, "maxmind");
  const [city, country, asn] = await Promise.all([
    Reader.open(join(maxmindDir, "GeoLite2-City.mmdb")),
    Reader.open(join(maxmindDir, "GeoLite2-Country.mmdb")),
    Reader.open(join(maxmindDir, "GeoLite2-ASN.mmdb"))
  ]);

  return { city, country, asn };
}

export function lookupIp(readers: MaxMindReaders, ipAddress: string, raw: true): MeridianIpRawResult;
export function lookupIp(readers: MaxMindReaders, ipAddress: string, raw?: false): MeridianIpResult;
export function lookupIp(
  readers: MaxMindReaders,
  ipAddress: string,
  raw: boolean
): MeridianIpRawResult | MeridianIpResult;
export function lookupIp(
  readers: MaxMindReaders,
  ipAddress: string,
  raw = false
): MeridianIpRawResult | MeridianIpResult {
  const lookup = lookupMaxMind(readers, ipAddress);
  return raw ? rawMaxMindLookup(lookup) : polishMaxMindLookup(ipAddress, lookup);
}

export function lookupMaxMind(readers: MaxMindReaders, ipAddress: string): MaxMindLookup {
  if (!isIP(ipAddress)) {
    throw new MeridianInputError(`Invalid IP address: ${ipAddress}`);
  }

  return {
    city: safeLookup(() => readers.city.city(ipAddress)),
    country: safeLookup(() => readers.country.country(ipAddress)),
    asn: safeLookup(() => readers.asn.asn(ipAddress))
  };
}

export function rawMaxMindLookup(lookup: MaxMindLookup): MeridianIpRawResult {
  return {
    city: toJson(lookup.city),
    country: toJson(lookup.country),
    asn: toJson(lookup.asn)
  };
}

export function polishMaxMindLookup(ipAddress: string, lookup: MaxMindLookup): MeridianIpResult {
  const subdivision = lookup.city?.subdivisions?.[0];

  return {
    source: "maxmind",
    ip: ipAddress,
    city: {
      name: lookup.city?.city?.names.en ?? null,
      geonameId: lookup.city?.city?.geonameId ?? null,
      latitude: lookup.city?.location?.latitude ?? null,
      longitude: lookup.city?.location?.longitude ?? null,
      timeZone: lookup.city?.location?.timeZone ?? null
    },
    subdivision: {
      isoCode: subdivision?.isoCode ?? null,
      name: subdivision?.names.en ?? null,
      geonameId: subdivision?.geonameId ?? null
    },
    country: {
      isoCode: lookup.country?.country?.isoCode ?? lookup.city?.country?.isoCode ?? null,
      name: lookup.country?.country?.names.en ?? lookup.city?.country?.names.en ?? null,
      geonameId: lookup.country?.country?.geonameId ?? lookup.city?.country?.geonameId ?? null
    },
    asn: {
      autonomousSystemNumber: lookup.asn?.autonomousSystemNumber ?? null,
      autonomousSystemOrganization: lookup.asn?.autonomousSystemOrganization ?? null,
      network: lookup.asn?.network ?? null
    }
  };
}

export function maxMindCityNameVariants(lookup: MaxMindLookup): string[] {
  return unique([
    lookup.city?.city?.names.en,
    ...objectValues(lookup.city?.city?.names)
  ]).filter((city) => asciiKey(city).length > 0);
}

export function maxMindCountryVariants(lookup: MaxMindLookup): string[] {
  return unique([
    lookup.country?.country?.isoCode,
    lookup.city?.country?.isoCode,
    lookup.country?.country?.names.en,
    lookup.city?.country?.names.en,
    ...objectValues(lookup.country?.country?.names),
    ...objectValues(lookup.city?.country?.names)
  ]);
}

function toJson(value: City | Country | Asn | null): MeridianJsonValue | null {
  return value === null ? null : JSON.parse(JSON.stringify(value));
}

function objectValues(value: object | undefined): string[] {
  return Object.values(value ?? {}).filter((item): item is string => typeof item === "string" && item.trim().length > 0);
}

function unique(values: Array<string | null | undefined>): string[] {
  return [...new Set(values.filter((value): value is string => typeof value === "string" && value.trim().length > 0))];
}

function asciiKey(value: string): string {
  return value
    .normalize("NFKD")
    .replace(/\p{Diacritic}/gu, "")
    .replace(/[^0-9A-Za-z]+/g, " ")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function safeLookup<T extends City | Country | Asn>(lookup: () => T): T | null {
  try {
    return lookup();
  } catch (error) {
    if (error instanceof AddressNotFoundError) {
      return null;
    }
    if (error instanceof ValueError) {
      throw new MeridianInputError(error.message);
    }
    throw error;
  }
}
