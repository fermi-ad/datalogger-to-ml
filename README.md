# Data Logger to ML

CLI for requesting AD Controls data logger data and transforming those to the desired ML output format and destination.

## Installation

In your environment, run `pip install --extra-index-url https://www-bd.fnal.gov/pip3 datalogger-to-ml`. This will install this package as a command line tool. You should now be able to run `datalogger-to-ml --help` to see what commands are available.

## Usage

There are three sub-commands this CLI provides.

### Nanny

The `nanny` sub-command generates hdf5 data files from the AD data loggers. `nanny` will write files to the local path, `.`, during data collection and move them to a `YYYYMM/DD/` directory structure once the files are complete. The generated data files are named using the [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) standard for timestamps with a period. The name of the file indicates the start time and duration. The name of the file also includes a version number at the end as a marker of requests list modification. e.g., `20200101T000000PT1H-1_0_0.h5`

`nanny` attempts to determine a start date based on filenames in the `output_path`. If there is not a valid named file in the output path then `nanny` requests data from 1 hour ago to now. The duration is static, for now. If `nanny` finds a valid filename then it will calculate a `start_time` for the new data from the most recent filename. By default, `nanny` will loop through time making 1 hour requests for data until the `output_path` has a current filename.

Arguments can be passed to `nanny` in two ways. Command line flags are provided for convenience and easy troubleshooting, but it is recommended to create a config file named `config.yaml` for persistent processes.

#### Config file

`nanny` looks for `config.yaml` in the directory it is run, `.`. The config file is in the [YAML format](https://en.wikipedia.org/wiki/YAML).

Here's an example config file that uses GitHub to host the requests list:

```yaml
---
  github:
    owner: fermi-controls
    repo: linac-logger-device-cleaner
    file: linac_logger_drf_requests.txt
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
  output:
    path: .
  logging:
    level: DEBUG

```

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

The `validate` sub-command is a simple program that takes paths as arguments and will validate that all the `*.h5` files in that directory are not corrupt.

### Dump

The `dump` sub-command is a simple program that takes an hdf5 input file via the `-i` or `--input-file` flags and outputs a truncated text representation, `dump_output.txt`, of the data in the input file.

Optionally, the `-o` or `--output-file` flags can be used to specify the path of the output.

## Contributing

A [`Makefile`](./Makefile) is used for installation, building, deploying, and cleaning up.

### Installing dependencies

[`poetry`](https://python-poetry.org/) is used to manage dependencies.

`make install` installs the requirements from the [`pyproject.toml`](./pyproject.toml) file.

### Building

`make build` creates new pip archives in [`dist`](./dist) and update the [`requirements.txt`](./requirements.txt).

### Deployment

`make deploy` copies the [`dist`](./dist) folder to the AD pip repository.

Don't forget to change the version number in [`pyproject.toml`](./pyproject.toml) before publishing

### Cleaning

`make clean` removes files and folders generated as a part of the build process.
