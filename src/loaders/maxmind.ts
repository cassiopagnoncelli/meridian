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
  if (!isIP(ipAddress)) {
    throw new MeridianInputError(`Invalid IP address: ${ipAddress}`);
  }

  const city = safeLookup(() => readers.city.city(ipAddress));
  const country = safeLookup(() => readers.country.country(ipAddress));
  const asn = safeLookup(() => readers.asn.asn(ipAddress));

  if (raw) {
    return {
      city: toJson(city),
      country: toJson(country),
      asn: toJson(asn)
    };
  }

  return {
    source: "maxmind",
    ip: ipAddress,
    city: {
      name: city?.city?.names.en ?? null,
      geonameId: city?.city?.geonameId ?? null,
      latitude: city?.location?.latitude ?? null,
      longitude: city?.location?.longitude ?? null,
      timeZone: city?.location?.timeZone ?? null
    },
    country: {
      isoCode: country?.country?.isoCode ?? city?.country?.isoCode ?? null,
      name: country?.country?.names.en ?? city?.country?.names.en ?? null,
      geonameId: country?.country?.geonameId ?? city?.country?.geonameId ?? null
    },
    asn: {
      autonomousSystemNumber: asn?.autonomousSystemNumber ?? null,
      autonomousSystemOrganization: asn?.autonomousSystemOrganization ?? null,
      network: asn?.network ?? null
    }
  };
}

function toJson(value: City | Country | Asn | null): MeridianJsonValue | null {
  return value === null ? null : JSON.parse(JSON.stringify(value));
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
