#!/usr/bin/env python3
"""Fetch the correct IBGE SIDRA table 10295 income variables.

The checked-in CSV export selected the residents/count variable. This script
fetches the mean and median monthly household income per-capita variables for
all Brazilian municipalities, restricted to total sex, race, and age groups.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = REPO_ROOT / "datasets/raw/ibge/tabela10295_income.json"

IBGE_TABLE = "10295"
IBGE_PERIOD = "2022"
MEAN_INCOME_VARIABLE = "13431"
MEDIAN_INCOME_VARIABLE = "13534"


def fetch_ibge_income() -> list[dict[str, str]]:
    params = urlencode(
        {
            "localidades": "N6[all]",
            "classificacao": "2[6794]|86[95251]|58[95253]",
            "view": "flat",
        }
    )
    url = (
        "https://servicodados.ibge.gov.br/api/v3/agregados/"
        f"{IBGE_TABLE}/periodos/{IBGE_PERIOD}/variaveis/"
        f"{MEAN_INCOME_VARIABLE}%7C{MEDIAN_INCOME_VARIABLE}?{params}"
    )

    with urlopen(url, timeout=60) as response:
        return json.load(response)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch IBGE table 10295 mean/median municipality income data."
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    data = fetch_ibge_income()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, ensure_ascii=False)
    print(f"wrote {len(data) - 1} data rows to {args.output}")


if __name__ == "__main__":
    main()
