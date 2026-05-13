#!/usr/bin/env python3
"""Extract city-first metric CSVs from Meridian raw datasets.

The script uses structured parsers (`csv`) rather than ad hoc line splitting.
Outputs are intentionally wide and source-specific: `city` is the first column,
followed by the metrics derived from that source.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]

GHSL_RAW = (
    REPO_ROOT
    / "datasets/raw/ghs_socio/GHS_UCDB_THEME_SOCIOECONOMIC_GLOBE_R2024A.csv"
)
NUMBEO_RAW = REPO_ROOT / "datasets/raw/numbeo/cost-of-living_v2.csv"
IBGE_RAW = REPO_ROOT / "datasets/raw/ibge/tabela10295_income.json"
PROCESSED_DIR = REPO_ROOT / "datasets/processed"

GHSL_OUTPUT = "ghsl_city_metrics.csv"
NUMBEO_OUTPUT = "numbeo_city_metrics.csv"
IBGE_OUTPUT = "ibge_municipality_income.csv"

GHSL_FIELDS = [
    "city",
    "country",
    "ghsl_urban_centre_id",
    "world_region",
    "world_bank_income_group",
    "urban_area_km2_2025",
    "population_2025",
    "hdi_2020",
]

NUMBEO_FIELDS = [
    "city",
    "country",
    "monthly_net_salary_after_tax_nominal_usd",
    "annual_net_salary_after_tax_nominal_usd",
    "data_quality",
]


@dataclass(frozen=True)
class BuildSummary:
    source: str
    output_path: Path | None
    rows_written: int
    rows_skipped: int
    message: str


def clean_text(value: object) -> str:
    return str(value or "").strip()


def parse_number(value: object) -> float | None:
    text = clean_text(value)
    if not text or text in {"-", "..", "..."} or text.lower() == "nan":
        return None

    # SIDRA CSVs can use Brazilian decimal commas in metric columns.
    if "," in text and "." not in text:
        text = text.replace(",", ".")

    try:
        return float(text)
    except ValueError:
        return None


def format_number(value: float | None) -> str:
    if value is None:
        return ""

    rounded = round(value, 2)
    if rounded == int(rounded):
        return str(int(rounded))
    return f"{rounded:.2f}"


def ensure_fields(fieldnames: Sequence[str] | None, required: Iterable[str], source: str) -> None:
    fields = set(fieldnames or [])
    missing = [field for field in required if field not in fields]
    if missing:
        raise ValueError(f"{source} is missing required columns: {', '.join(missing)}")


def write_rows(path: Path, fieldnames: Sequence[str], rows: Iterable[dict[str, str]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
            count += 1
    return count


def build_ghsl(raw_path: Path, output_dir: Path) -> BuildSummary:
    """Extract GHSL city metrics that are safe for city enrichment.

    GHSL GDP fields are intentionally omitted from processed output. The
    gridded GDP totals do not reconcile cleanly with known city/regional GDP
    benchmarks and are too easy to mistake for city income.
    """

    required = [
        "ID_UC_G0",
        "GC_UCN_MAI_2025",
        "GC_CNT_GAD_2025",
        "GC_UCA_KM2_2025",
        "GC_POP_TOT_2025",
        "GC_DEV_WIG_2025",
        "GC_DEV_USR_2025",
        "SC_SEC_HDI_2020",
    ]
    rows_skipped = 0

    def rows() -> Iterable[dict[str, str]]:
        nonlocal rows_skipped

        with raw_path.open(newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            ensure_fields(reader.fieldnames, required, "GHSL")

            for raw in reader:
                city = clean_text(raw.get("GC_UCN_MAI_2025"))
                country = clean_text(raw.get("GC_CNT_GAD_2025"))
                population = parse_number(raw.get("GC_POP_TOT_2025"))

                if not city or city == "-" or not country:
                    rows_skipped += 1
                    continue

                yield {
                    "city": city,
                    "country": country,
                    "ghsl_urban_centre_id": clean_text(raw.get("ID_UC_G0")),
                    "world_region": clean_text(raw.get("GC_DEV_USR_2025")),
                    "world_bank_income_group": clean_text(raw.get("GC_DEV_WIG_2025")),
                    "urban_area_km2_2025": format_number(parse_number(raw.get("GC_UCA_KM2_2025"))),
                    "population_2025": format_number(population),
                    "hdi_2020": format_number(parse_number(raw.get("SC_SEC_HDI_2020"))),
                }

    output_path = output_dir / GHSL_OUTPUT
    rows_written = write_rows(output_path, GHSL_FIELDS, rows())
    return BuildSummary(
        source="GHSL",
        output_path=output_path,
        rows_written=rows_written,
        rows_skipped=rows_skipped,
        message="city metrics only; GHSL GDP omitted from processed output",
    )


def build_numbeo(raw_path: Path, output_dir: Path) -> BuildSummary:
    """Extract Numbeo city salary metric and annualize it."""

    required = ["city", "country", "x54", "data_quality"]
    rows_skipped = 0

    def rows() -> Iterable[dict[str, str]]:
        nonlocal rows_skipped

        with raw_path.open(newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            ensure_fields(reader.fieldnames, required, "Numbeo")

            for raw in reader:
                city = clean_text(raw.get("city"))
                country = clean_text(raw.get("country"))
                monthly_salary = parse_number(raw.get("x54"))

                if not city or not country or not monthly_salary:
                    rows_skipped += 1
                    continue

                yield {
                    "city": city,
                    "country": country,
                    "monthly_net_salary_after_tax_nominal_usd": format_number(monthly_salary),
                    "annual_net_salary_after_tax_nominal_usd": format_number(monthly_salary * 12),
                    "data_quality": clean_text(raw.get("data_quality")),
                }

    output_path = output_dir / NUMBEO_OUTPUT
    rows_written = write_rows(output_path, NUMBEO_FIELDS, rows())
    return BuildSummary(
        source="Numbeo",
        output_path=output_path,
        rows_written=rows_written,
        rows_skipped=rows_skipped,
        message="city salary metrics",
    )


def split_brazilian_municipality(value: str) -> tuple[str, str]:
    match = re.match(r"^(?P<city>.+?)\s+-\s+(?P<state>[A-Z]{2})$", value)
    if not match:
        match = re.match(r"^(?P<city>.+?)\s+\((?P<state>[A-Z]{2})\)$", value)
    if not match:
        return value, ""
    return match.group("city").strip(), match.group("state")


def build_ibge(raw_path: Path, output_dir: Path) -> BuildSummary:
    """Extract IBGE municipality income into a city-first CSV."""

    output_path = output_dir / IBGE_OUTPUT
    if not raw_path.exists():
        if output_path.exists():
            output_path.unlink()
        return BuildSummary(
            source="IBGE",
            output_path=None,
            rows_written=0,
            rows_skipped=0,
            message=f"skipped: missing income extract {raw_path}; run scripts/fetch_ibge_income.py",
        )

    with raw_path.open(encoding="utf-8") as handle:
        data = json.load(handle)

    rows_skipped = 0
    municipalities: dict[str, dict[str, str]] = {}

    for raw in data[1:]:
        municipality_code = clean_text(raw.get("D1C"))
        municipality_name = clean_text(raw.get("D1N"))
        variable_code = clean_text(raw.get("D3C"))
        value = parse_number(raw.get("V"))

        if not municipality_code or not municipality_name or value is None:
            rows_skipped += 1
            continue

        city, state = split_brazilian_municipality(municipality_name)
        row = municipalities.setdefault(
            municipality_code,
            {
                "city": city,
                "state": state,
                "country": "Brazil",
                "ibge_municipality_code": municipality_code,
                "mean_monthly_household_income_per_capita_brl_2022": "",
                "median_monthly_household_income_per_capita_brl_2022": "",
            },
        )

        if variable_code == "13431":
            row["mean_monthly_household_income_per_capita_brl_2022"] = format_number(value)
        elif variable_code == "13534":
            row["median_monthly_household_income_per_capita_brl_2022"] = format_number(value)
        else:
            rows_skipped += 1

    fields = [
        "city",
        "state",
        "country",
        "ibge_municipality_code",
        "mean_monthly_household_income_per_capita_brl_2022",
        "median_monthly_household_income_per_capita_brl_2022",
    ]
    rows_written = write_rows(
        output_path,
        fields,
        (municipalities[key] for key in sorted(municipalities)),
    )

    return BuildSummary(
        source="IBGE",
        output_path=output_path,
        rows_written=rows_written,
        rows_skipped=rows_skipped,
        message="municipality mean and median monthly per-capita income extracted",
    )


def print_summary(summaries: Sequence[BuildSummary]) -> None:
    for summary in summaries:
        output = summary.output_path if summary.output_path else "no output"
        print(
            f"{summary.source}: {summary.message}; "
            f"rows_written={summary.rows_written}; "
            f"rows_skipped={summary.rows_skipped}; "
            f"output={output}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build city-first metric CSVs from Meridian raw datasets."
    )
    parser.add_argument("--processed-dir", type=Path, default=PROCESSED_DIR)
    parser.add_argument("--ghsl-raw", type=Path, default=GHSL_RAW)
    parser.add_argument("--numbeo-raw", type=Path, default=NUMBEO_RAW)
    parser.add_argument("--ibge-raw", type=Path, default=IBGE_RAW)
    parser.add_argument(
        "--source",
        action="append",
        choices=("ghsl", "numbeo", "ibge"),
        help="Source to build. Repeat for multiple sources. Defaults to all.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    selected = set(args.source or ("ghsl", "numbeo", "ibge"))
    summaries: list[BuildSummary] = []

    if "ghsl" in selected:
        summaries.append(build_ghsl(args.ghsl_raw, args.processed_dir))
    if "numbeo" in selected:
        summaries.append(build_numbeo(args.numbeo_raw, args.processed_dir))
    if "ibge" in selected:
        summaries.append(build_ibge(args.ibge_raw, args.processed_dir))

    print_summary(summaries)


if __name__ == "__main__":
    main()
