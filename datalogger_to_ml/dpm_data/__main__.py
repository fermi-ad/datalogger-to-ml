#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import datetime
from pathlib import Path
from . import dpm_data

# https://packaging.python.org/guides/single-sourcing-package-version/#single-sourcing-the-version
try:
    from importlib import metadata
except ImportError:
    # Running on pre-3.8 Python; use importlib-metadata package
    import importlib_metadata as metadata


def get_version():
    __version__ = 'v*.*.*'

    try:
        __version__ = metadata.version(__name__)
    except metadata.PackageNotFoundError:
        pass

    return __version__


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Request data logger data.',
        prog='dpm_data'
    )
    parser.add_argument(
        '--version',
        action='version',
        version=f'%(prog)s {get_version()}'
    )
    # Add positional/optional parameters
    parser.add_argument(
        '-d',
        '--device_limit',
        type=int,
        help='Limit for number of devices.'
    )
    parser.add_argument(
        '-f',
        '--device-file',
        type=Path,
        help=('Filename containing the list of devices. '
              'Newline delimited.')
    )
    parser.add_argument(
        '-o',
        '--output-file',
        type=Path,
        help='Name of the output file for the hdf5 file.'
    )
    parser.add_argument(
        '-n',
        '--dpm-node',
        type=str,
        help='Set the DPM node to request data from.'
    )
    parser.add_argument(
        '-s',
        '--start-date',
        type=datetime.datetime.fromisoformat,
        help=('Enter the start time/date. '
              'Do not use the duration tag.'),
        required=False
    )
    parser.add_argument(
        '-e',
        '--end-date',
        type=datetime.datetime.fromisoformat,
        help=('Enter the end time/date. '
              'Do not use the duration tag.'),
        required=False
    )
    parser.add_argument(
        '-u',
        '--duration',
        help='Enter LOGGERDURATION in sec.',
        required=False,
        type=str
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable all messages.'
    )

    # Run the program with argparse arguments
    dpm_data.get_data(**vars(parser.parse_args()))
