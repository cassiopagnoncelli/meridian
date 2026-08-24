# Meridian

> ## Retired — this code now lives in Polaris
>
> **Status: archived, 2026-08-24.** Meridian has no consumer. Everything it
> was used for is in [Polaris](https://github.com/cassiopagnoncelli/polaris),
> and this repository is kept readable rather than deleted because it is the
> provenance of that code: Polaris's comments cite it by path and by commit
> `63768d7`, and those citations should keep resolving.
>
> Nothing here is maintained. Do not add to it, and do not import from it.
>
> ### Where each piece went
>
> | Meridian | Polaris | Card |
> | --- | --- | --- |
> | GHSL loaders, CSV reading, normalisation, alias resolution | `sync/enrichment/ghsl/v1` | LXC6O |
> | GeoLite2 reading | `sync/enrichment/geoip/v1` (already existed, on `mmdb-lib`) | — |
> | Keyless GeoLite2 fetch | `infra/geoip/refresh-geoip.sh` | 5NN52 |
> | Dataset update and verify scripts | `polaris datasets update` / `verify` / `status` | 8OOYE |
> | `scripts/build_processed_datasets.py` (GHSL reduction) | `apps/polaris-cli/src/commands/datasets/ghsl.ts` | 8OOYE |
>
> ### What deliberately did NOT go, and why
>
> **`src/loaders/maxmind.ts` — meridian's MaxMind reader.** It wraps
> `@maxmind/geoip2-node`; Polaris reads GeoLite2 through `mmdb-lib` in
> `sync/enrichment/geoip/v1`. Two readers for one lookup is precisely the
> outcome the absorption existed to avoid, so Polaris kept its own.
>
> **IBGE — `ibge_municipality_income.csv`, `ibge_city_aliases.csv`,
> `src/loaders/ibge.ts`, `scripts/fetch_ibge_income.py`.** Ruled out
> deliberately, not overlooked, by Polaris ADR-0016 Ruling 3. In short: a
> Brazilian visitor would carry an income figure and an identical visitor
> anywhere else would carry nothing, so every consumer writes
> country-conditional code and every audience built on it silently means
> "Brazilians only"; it keys on city + state where GHSL keys on city +
> country, so none of the keying work is reused; and municipality-granularity
> household income invites an inference about a person's means from where they
> are, on every event. GHSL's `world_bank_income_group` is the tier that
> attaches everywhere instead.
>
> That refusal is cheap to reverse and was left that way on purpose. What
> survives archiving is the CODE, not the data: `datasets/processed` and
> `lib/meridian` are gitignored here, so `ibge_municipality_income.csv` and
> `ibge_city_aliases.csv` exist only on machines that built them.
> `scripts/fetch_ibge_income.py` and `scripts/build_processed_datasets.py`
> are tracked and stay readable, and IBGE publishes the source tables — which
> is what makes the refusal reversible. If Polaris ever wants municipality
> income it comes back as its own card with its own record: keyed, fielded
> and country-scoped on purpose.
>
> **`scripts/build_compatibility_aliases.mjs` and
> `scripts/build_ghsl_geoname_map.mjs`.** Not ported, and not needed. Polaris
> builds the alias table from GHSL's own `GC_UCN_LIS_2025` name lists, and
> resolves an address-derived city by position against the centroids in the
> release's own GeoPackage — so it needs neither the MaxMind enumeration the
> first script did through private `mmdb-lib` internals, nor the GDAL
> point-in-polygon join the second one needed (Polaris card ODQRT).
>
> Not a like-for-like replacement, and worth saying so: meridian's alias table
> was 25,630 rows keyed on ISO-2 country codes, derived against GeoLite2;
> Polaris's is 5,436 rows keyed on full country names, derived from JRC's own
> statement of which settlements each urban centre comprises. Different
> source, different provenance, narrower. The 25,630-row file was gitignored
> here and does not survive the archive either.


Local data enrichment library for Node.js.

Meridian is code-only. Host applications provide data files under:

```text
lib/meridian/
  maxmind/
    GeoLite2-City.mmdb
    GeoLite2-Country.mmdb
    GeoLite2-ASN.mmdb
  ibge/
    ibge_municipality_income.csv
    ibge_city_aliases.csv
  ghsl/
    ghsl_city_metrics.csv
    ghsl_city_aliases.csv
    ghsl_geoname_map.csv
```

## Usage

```ts
import { Meridian } from "meridian";

const meridian = await Meridian.open();

const ip = meridian.ip("8.8.8.8");
const rawIp = meridian.ip("8.8.8.8", true);
const enrichedIp = meridian.ip("200.160.2.3", false, true);
const ibge = meridian.ibge("São Paulo", "SP");
const ghsl = meridian.ghsl("São Paulo", "Brazil");
const metadata = meridian.metadata();
```

Use a custom data directory:

```ts
const meridian = await Meridian.open({
  dataDir: "/app/lib/meridian",
  sources: ["maxmind", "ibge", "ghsl"]
});
```

`Meridian.open()` is strict by default and fails fast when selected files are missing.

## Data Semantics

- `ip()` returns polished city, subdivision, country, and ASN fields.
- `ip(address, true)` returns raw MaxMind JSON payloads only.
- `ip(address, false, true)` returns polished IP data enriched with canonical IBGE/GHSL city matches when those sources are loaded.
- `ibge()` returns 2022 mean and median monthly household income per capita in BRL.
- `ghsl()` returns city profile metrics only: urban-centre id, region, income group, area, population, and HDI. GHSL GDP is intentionally omitted from processed output.

Lookup keys are accent-insensitive, punctuation-insensitive, and case-insensitive.
Common country aliases (`US`, `USA`, `UK`, `Brasil`) and Brazilian state names
(`São Paulo`, `Paraná`, etc.) are normalized at lookup time.
Optional `ibge_city_aliases.csv`, `ghsl_city_aliases.csv`, and
`ghsl_geoname_map.csv` files add MaxMind-derived lookup compatibility while
preserving canonical returned city names. When `ghsl_geoname_map.csv` exists,
IP enrichment trusts MaxMind city geoname-id mappings before looser city-name
fallbacks.

## Operations

```sh
make data-host       # prepare local lib/meridian symlinks
make data-validate   # validate host data files and sample lookups
make data-compatibility  # generate MaxMind-to-IBGE/GHSL alias files
make ghsl-geoname-map  # generate MaxMind geoname-id to GHSL urban-centre map
make sanity-intersections  # compare canonical datasets against MaxMind intersections
make audit-maxmind-city  # audit MaxMind city coverage against IBGE and GHSL
make console         # open a REPL with ip(), ibge(), ghsl(), and meridian loaded
make benchmark       # benchmark open(), ip(), ibge(), and ghsl()
make ci              # typecheck, tests, and package dry-run
```

See [docs/host-app.md](docs/host-app.md) for host application setup.
