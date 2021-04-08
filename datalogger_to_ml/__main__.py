#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
from pathlib import Path
from . import h5_dump
from . import nanny
from . import h5_validator

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


def main():
    parser = argparse.ArgumentParser(
        argument_default=argparse.SUPPRESS,
        description='Process some integers.',
        prog='datalogger-to-ml'
    )
    parser.add_argument(
        '--version',
        action='version',
        version=f'%(prog)s {get_version()}'
    )
    parser.add_argument('--verbose', '-v', action='count', default=0)

    # sub-commands
    subparsers = parser.add_subparsers(help='sub-command help')
    nanny_parser = subparsers.add_parser(
        'nanny',
        help='Start a nanny session'
    )
    nanny_parser.set_defaults(func=nanny.get_data)
    dump_parser = subparsers.add_parser(
        'dump',
        help='Dump binary file contents to a text file'
    )
    dump_parser.set_defaults(func=h5_dump.dump)
    validate_parser = subparsers.add_parser(
        'validate',
        help='Validate output file contents'
    )
    validate_parser.set_defaults(func=h5_validator.validate)

    # sub-command arguments
    nanny_parser.add_argument(
        '-r',
        '--requests-list',
        type=str,
        help='Input file with line separated DRF requests'
    )
    nanny_parser.add_argument(
        '-l',
        '--list-version',
        type=str,
        help='List version for tracking list changes'
    )
    nanny_parser.add_argument(
        '-o',
        '--output-path',
        type=Path.resolve,
        help='Output directory for completed files to be moved to'
    )
    nanny_parser.add_argument(
        '--run-once',
        action='store_true',
        help='Generate only one file before exiting.'
    )
    dump_parser.add_argument(
        '-i',
        '--input-file',
        type=Path,
        help='Binary file input of dump.'
    )
    dump_parser.add_argument(
        '-o',
        '--output-file',
        type=Path,
        default=Path('dump_output.txt'),
        help='Text output of binary dump.'
    )
    validate_parser.add_argument(
        'validate-path',
        nargs='?',
        type=Path,
        default=Path('.'),
        help='Directory of nanny output.'
    )

    args = parser.parse_args()
    # Filter None values from Namespace
    # `argument_default=argparse.SUPPRESS` above doesn't work
    kw_args = {
        key: value
        for (key, value) in vars(args).items()
        if value is not None
    }
    # Pass keyword arguments to the relevant function
    args.func(**kw_args)


if __name__ == '__main__':
    main()
