# Data Logger to ML

CLI for requesting AD Controls data logger data and transforming those to the desired ML output format and destination.

## Installation

In your environment, run `pip install --extra-index-url https://www-bd.fnal.gov/pip3 datalogger-to-ml`. This will install this package as a command line tool. You should now be able to run `datalogger-to-ml --help` to see what commands are available.

For development, or to run it without managing an environment, use [pixi](https://pixi.sh): `pixi install` creates the environment from `pyproject.toml` and `pixi.lock`, and `pixi run datalogger-to-ml --help` runs the tool in it.

## Usage

There are three sub-commands this CLI provides.

### Nanny

The `nanny` sub-command generates data files, HDF5 or Parquet, from the AD data loggers, one file per time window, in a `YYYYMM/DD/` directory structure under the output path. The output path may be write-once storage such as pnfs, where a file cannot be edited in place, so each file is built and closed in a local workbench directory (`output.workbench` in the config, default `./workbench`) and only then delivered: copied in under a `.partial` name, written once front to back, and renamed. A crash never leaves a half-written file under a real name, and a stale `.partial` from an interrupted copy is removed before the next try. The generated data files are named using the [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) standard for timestamps with a period. The name of the file indicates the start time and duration. The name of the file also includes a version number at the end as a marker of requests list modification. e.g., `20200101T000000PT1H-1_0_0.h5`

Each run lists every whole window from the configured start to now and pulls each one that has no file yet, in any request-list version, newest first, so a skipped or failed window is retried on the next run and, during a long backfill, the latest windows land first. Run it from cron under `flock` and every gap fills itself. Without a configured start, `nanny` fills back to its earliest existing file, or pulls only the newest whole window.

Arguments can be passed to `nanny` in two ways. Command line flags are provided for convenience and easy troubleshooting, but it is recommended to create a config file named `config.yaml` for persistent processes.

#### Config file

`nanny` looks for `config.yaml` in the directory it is run, `.`. The config file is in the [YAML format](https://en.wikipedia.org/wiki/YAML).

Here's an example config file that uses GitHub to host the requests list:

```yaml
---
  github:
    owner: fermi-ad
    repo: linac-logger-device-cleaner
    file: linac_logger_drf_requests.txt
  start: 20200101T000000
  duration: T1H
  output:
    path: .
  logging:
    level: DEBUG

```

Here's an example config file that uses a local requests list:

```yaml
---
  local:
    file: custom_requests.txt
    version: 1.0.0
  start: 20200101T000000
  duration: T1H
  output:
    path: .
  logging:
    level: DEBUG

```

When using the config file, a start datetime and duration can be specified. The default duration is one hour; without a start, see above.

The output format is `hdf5` (one table per request, keyed by the request) unless `output.format` in the config or `--output-format` says `parquet`: one long table per window with columns `device` (the request), `Timestamps` (microseconds, Unix epoch), and `Data`. Long rather than one column per device, because each device carries its own timestamps; an array device becomes one `device[i]` series per element.

#### CLI arguments

##### Requests list

`nanny` requires a request list argument via the `-r` or `--requests-list` flags. This path points to a line separated file of [DRF](https://www-bd.fnal.gov/controls/public/drf2/) requests. By default `nanny` looks for `requests.txt` because this is the default name of generated requests list from GitHub.

##### List version

The `-l` and `--list-version` flags allow users to set the version number. The default value is `v0.1.0`.

##### Output path

The `-o` and `--output-path` flags allow users to set the location of the output directories [described above](#nanny). The default is the current directory, `.`.

##### Run once

The `--run-once` flag disables the "get data to now" feature.

### Validate

The `validate` sub-command is a simple program that takes a path and checks that every `*.h5` and `*.parquet` file under it, subfolders included, can be read.

### Catalog

The `catalog` sub-command indexes output files in an SQLite database: one row per file (path, name, size,
modified time, nominal window from the file name, actual data span, request-list version, number of devices)
and one row per device in each file (request, bare device name, point count, points per second, first and last
sample time, min, 25%, median, 75%, max of the values, and the mean and variance of the intervals between
consecutive samples). Each run indexes only new or changed files and drops entries for files that are gone.

`datalogger-to-ml catalog [PATH] [--db FILE]`: PATH defaults to `output.path` and FILE to `catalog` in
`config.yaml`, else `catalog.sqlite`. SQLite needs no server or open port; keep the database on local disk, not
on NFS or pnfs. Run it after `nanny` under the same lock, so it has one writer. Example queries:

```sql
-- files where a device's range overlapped [V1, V2]; for a continuous signal, it took those values
SELECT f.path FROM files f JOIN device_stats d ON d.file_id = f.id
 WHERE d.device = 'C:5LL80H' AND d.v_max >= 16.9 AND d.v_min <= 17.0;
-- files with a device below one point per second
SELECT count(*) FROM device_stats WHERE device = 'C:5PT81H' AND points_per_s < 1;
```

### Dump

The `dump` sub-command is a simple program that takes an hdf5 input file via the `-i` or `--input-file` flags and outputs a truncated text representation, `dump_output.txt`, of the data in the input file.

Optionally, the `-o` or `--output-file` flags can be used to specify the path of the output.

## Contributing

[pixi](https://pixi.sh) manages the environment and the tasks, all defined in [`pyproject.toml`](./pyproject.toml); `pixi.lock` pins every package.

- `pixi install` creates the environment (conda-forge packages, plus `pacsys` from PyPI).
- `pixi run test` runs the tests; `pixi run lint` runs flake8.
- `pixi run build` creates the wheel and source archive in [`dist`](./dist).
- `pixi run deploy` builds, then copies `dist` to the AD pip repository.
- `pixi run clean` removes build output and `nanny.log`.

Don't forget to change the version number in [`pyproject.toml`](./pyproject.toml) before publishing.
