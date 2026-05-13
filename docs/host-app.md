# Host App Setup

Meridian does not bundle data files. Each host app should provide this layout:

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

The default `Meridian.open()` call resolves this directory from the host app's
current working directory:

```ts
const meridian = await Meridian.open();
```

Use an explicit path when the files live elsewhere:

```ts
const meridian = await Meridian.open({
  dataDir: "/app/lib/meridian"
});
```

## Data Files

- MaxMind files are `.mmdb` databases and are read with `@maxmind/geoip2-node`.
- `ibge_municipality_income.csv` contains 2022 mean and median monthly household income per capita in BRL.
- `ghsl_city_metrics.csv` contains city profile attributes only. GHSL GDP is intentionally omitted.

## Validation

From this repository, prepare and validate local data with:

```sh
make data-host
make data-validate
```

`data-validate` checks required CSV columns, opens all selected sources, and runs
sample lookups for MaxMind, IBGE, and GHSL.

To measure city-name coverage between MaxMind and the city datasets:

```sh
make audit-maxmind-city
```

The audit enumerates unique MaxMind City records, then writes summary JSON and
miss CSV files under `reports/audit`.

For manual lookups during development:

```sh
make console
```

The console opens Meridian once and exposes `ip(...)`, `ibge(...)`, `ghsl(...)`,
`sources()`, `metadata()`, and the full `meridian` instance. Use
`ip("8.8.8.8", true)` when you want raw MaxMind JSON payloads.
