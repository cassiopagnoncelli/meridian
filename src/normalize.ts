import { canonicalBrazilState, canonicalCountry } from "./aliases";

export function normalizeKey(value: unknown): string {
  return String(value ?? "")
    .normalize("NFKD")
    .replace(/\p{Diacritic}/gu, "")
    .replace(/[^0-9A-Za-z]+/g, " ")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

export function cityCountryKey(city: string, country: string): string {
  return `${normalizeKey(city)}|${normalizeKey(canonicalCountry(country))}`;
}

export function cityStateKey(city: string, state: string): string {
  return `${normalizeKey(city)}|${normalizeKey(canonicalBrazilState(state))}`;
}
