#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
import pandas as pd


def _read(file):
    """Open one output file; True if it holds any data."""
    if file.suffix == '.parquet':
        return len(pd.read_parquet(file)) > 0
    with pd.HDFStore(file, mode='r') as hdf:
        return len(hdf) > 0


def validate(**kwargs):
    """Check every HDF5 and Parquet file under the path, subfolders included (nanny writes YYYYMM/DD/)."""
    root = Path(kwargs.get('validate-path')).resolve()
    files = sorted(f for pattern in ('*.h5', '*.parquet') for f in root.rglob(pattern))

    if len(files) > 0:
        for file in files:
            try:
                print(f'{file} was successfully read' if _read(file) else f'{file} is empty')
            except (OSError, ValueError) as error:
                print(f'Could not open {file} with error: {error}')
    else:
        print('No files found to validate')
