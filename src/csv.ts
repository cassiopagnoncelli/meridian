import { readFile } from "node:fs/promises";

import { parse } from "csv-parse/sync";

export async function parseCsvRows(path: string): Promise<Record<string, string>[]> {
  const content = await readFile(path, "utf8");
  return parse(content, {
    bom: true,
    columns: true,
    skip_empty_lines: true,
    trim: false
  }) as Record<string, string>[];
}

export function parseNullableNumber(value: unknown): number | null {
  const text = String(value ?? "").trim();
  if (!text || text === "-" || text === ".." || text === "..." || text.toLowerCase() === "nan") {
    return null;
  }

  const normalized = text.includes(",") && !text.includes(".") ? text.replace(",", ".") : text;
  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? parsed : null;
}

export function parseNullableString(value: unknown): string | null {
  const text = String(value ?? "").trim();
  return text ? text : null;
}
