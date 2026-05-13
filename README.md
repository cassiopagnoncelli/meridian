# Meridian

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
  ghsl/
    ghsl_city_metrics.csv
```

## Usage

```ts
import { Meridian } from "meridian";

const meridian = await Meridian.open();

const ip = meridian.ip("8.8.8.8");
const rawIp = meridian.ip("8.8.8.8", true);
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

- `ip()` returns polished city, country, and ASN fields. Use `ip(address, true)` for raw MaxMind JSON payloads only.
- `ibge()` returns 2022 mean and median monthly household income per capita in BRL.
- `ghsl()` returns city profile metrics only: urban-centre id, region, income group, area, population, and HDI. GHSL GDP is intentionally omitted from processed output.

Lookup keys are accent-insensitive, punctuation-insensitive, and case-insensitive.
Common country aliases (`US`, `USA`, `UK`, `Brasil`) and Brazilian state names
(`São Paulo`, `Paraná`, etc.) are normalized at lookup time.

## Operations

```sh
make data-host       # prepare local lib/meridian symlinks
make data-validate   # validate host data files and sample lookups
make audit-maxmind-city  # audit MaxMind city coverage against IBGE and GHSL
make console         # open a REPL with ip(), ibge(), ghsl(), and meridian loaded
make benchmark       # benchmark open(), ip(), ibge(), and ghsl()
make ci              # typecheck, tests, and package dry-run
```

See [docs/host-app.md](docs/host-app.md) for host application setup.
