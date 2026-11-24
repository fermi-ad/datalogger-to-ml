# Data Logger to ML

CLI for requesting AD Controls data logger data and transforming those to the desired ML output format and destination.

## Installation

In your environment, run `pip install --extra-index-url https://www-bd.fnal.gov/pip3 datalogger-to-ml`. This will install this package as a command line tool. You should now be able to run `datalogger-to-ml --help` to see what commands are available.

For development, or to run it without managing an environment, use [pixi](https://pixi.sh): `pixi install` creates the environment from `pyproject.toml` and `pixi.lock`, and `pixi run datalogger-to-ml --help` runs the tool in it.

## Usage

There are three sub-commands this CLI provides.

### Nanny

The `nanny` sub-command generates data files, HDF5 or Parquet, from the AD data loggers, one file per time window, in a `YYYYMM/DD/` directory structure under the output path. The output path may be write-once storage such as pnfs, where a file cannot be edited in place, so each file is built and closed in a local workbench directory (`output.workbench` in the config, default `./workbench`) and only then delivered: copied in under a `.partial` name, written once front to back, and renamed. A crash never leaves a half-written file under a real name, and a stale `.partial` from an interrupted copy is removed before the next try. The generated data files are named using the [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) standard for timestamps with a period. The name of the file indicates the start time and duration. The name of the file also includes a version number at the end as a marker of requests list modification. e.g., `20200101T060000ZPT1H-1_0_0.h5`

Windows are in UTC (since 1.9.0), and file names mark it with `Z`; the `YYYYMM/DD/` folders are the UTC date of the window's start. In local time with no zone, as before, the hour repeated at the autumn time change got one name and one file holding both hours, and the hour skipped in spring had no meaning. Files named before 1.9.0 (`20200101T000000PT1H-1_0_0.h5`, host local time) still count as present, read in the host's zone (a repeated hour as its first occurrence), so an existing output path carries on without a gap or a second copy. A `start` or `end` without a zone is host local time; add `Z` for UTC (`20140106T000000Z`).

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

When using the config file, a start datetime and duration can be specified. The default duration is one hour; without a start, see above. Windows of any length work (e.g. `duration: 7D`); they are counted from the start, so a start at midnight UTC on a Monday gives weeks Monday to Monday.

- `end` (or `--end-time`): pull only windows that end by this time (exclusive), e.g. a backfill that stops where a keep-up run starts.
- `pace` (or `--pace`): after each window, rest this multiple of the pull's run time before the next request (e.g. `4`: a pull of 16 s, then 64 s of rest), to spread a long backfill's load on the loggers. No rest by default.
- `retry-empty` and `retry-pause` (or `--retry-empty`, `--retry-pause`): a Parquet reply with no rows is requested again this many times (default 2), this many seconds apart (default 60), before it is delivered as is. The loggers have answered a whole hour with nothing once, and with data when asked again; a reply still empty after the tries is taken for an outage and delivered, so the window counts as pulled. Both defaults are untested; `retry-empty: 0` turns the tries off.
- `refill-empty` (or `--refill-empty`), for one run by hand: windows whose files all hold no rows count as missing; each is pulled again, and a reply with data replaces the empty files (an empty reply leaves them). Parquet only, and the output must allow replacing files. Reading every file's footer to find the empty ones takes a while on a large tree.

The output format is `hdf5` (one table per request, keyed by the request) unless `output.format` in the config or `--output-format` says `parquet`: one long table per window with columns `device` (the request), `Timestamps` (microseconds, Unix epoch), and `Data`. Long rather than one column per device, because each device carries its own timestamps; an array device becomes one `device[i]` series per element.

#### Backfill beside a keep-up run

Two nannies can share one output path: a keep-up run from cron that pulls the newest windows each hour, and a
backfill run that pulls years of history, for hours or days. Each has its own directory, so its own
`config.yaml`, `nanny.log`, and workbench, and both set the same `claims` directory. They cooperate in three ways:

- **Recency cutoff.** The backfill's config sets `recency` (or `--recency`), an ISO 8601 duration such as `P1D`:
  it then pulls only windows that ended at least that long ago, and never competes for the newest ones.
- **Claims.** Before pulling a window, each nanny takes an exclusive lock on `<claims>/<window>.lock` (local
  disk, shared by both), and skips any window the other holds; a claim lapses when its holder exits, even by a
  crash. So an old hole in the keep-up run's range is never pulled twice at once.
- **A fresh look before each pull.** The list of missing windows is made when a run starts; just before
  pulling, each nanny checks again whether the window has a file (any request-list version), so a window the
  other delivered meanwhile is skipped.

Every window still lands under one name per window, and both runs see each other's files as present. Give them
different request-list versions to tell their files apart. The catalog waits up to 10 minutes for the other
writer; running `catalog` under one shared `flock` keeps it to one writer.

```yaml
---
  local:
    file: requests.txt
    version: 2.0.0
  start: 20140106T000000Z       # a Monday, UTC
  end: 20261105T060000Z         # where the keep-up run starts
  duration: 7D                  # week-long windows for slow history
  pace: 4                       # rest 4x each pull's run time
  recency: P1D                  # backfill only: windows that ended at least a day ago
  claims: /var/tmp/nanny-claims # the same directory in both configs, on local disk
  output:
    path: /data/nanny
```

#### CLI arguments

##### Requests list

`nanny` requires a request list argument via the `-r` or `--requests-list` flags. This path points to a line separated file of [DRF](https://www-bd.fnal.gov/controls/public/drf2/) requests. By default `nanny` looks for `requests.txt` because this is the default name of generated requests list from GitHub.

##### List version

The `-l` and `--list-version` flags allow users to set the version number. The default value is `v0.1.0`.

##### Output path

The `-o` and `--output-path` flags allow users to set the location of the output directories [described above](#nanny). The default is the current directory, `.`.

##### Recency and claims

`--recency P1D`, `--claims DIR`, `--end-time 20261105T060000Z`, and `--pace 4` override `recency`, `claims`, `end`, and `pace` in the config; see
[Backfill beside a keep-up run](#backfill-beside-a-keep-up-run).

##### Run once

The `--run-once` flag disables the "get data to now" feature.

### Validate

The `validate` sub-command is a simple program that takes a path and checks that every `*.h5` and `*.parquet` file under it, subfolders included, can be read.

### Catalog

The `catalog` sub-command indexes output files in an SQLite database: one row per file (path, name, size,
modified time, nominal window from the file name, actual data span, request-list version, number of devices)
and one row per device in each file (request, bare device name, point count, points per second, first and last
sample time, min, 25%, median, 75%, max of the values, the mean and variance of the intervals between
consecutive samples, and `n_outside_window`, the samples whose time lies outside the file's window by more than
5 s). Each run indexes only new or changed files and drops entries for files that are gone. A catalog built by
an older version is rebuilt on its next run.

Stamps are written in microseconds. For each device the nanny picks the unit (microseconds, milliseconds,
seconds, or nanoseconds) that puts its stamps inside the requested window, and logs the rescaling at INFO:
the digital status channels (`*.STATUS.ON`) arrive in milliseconds. Stamps that fit no unit are written as
received, never dropped, and logged as a warning; `n_outside_window` finds them afterwards:

```sql
SELECT f.path, d.request, d.n_outside_window FROM device_stats d JOIN files f ON f.id = d.file_id
 WHERE d.n_outside_window > 0;
```

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
