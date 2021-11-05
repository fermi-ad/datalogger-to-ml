#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from glob import glob
from pathlib import PurePath
import pandas as pd


def validate(**kwargs):
    h5_outputs = PurePath(
        kwargs.get('validate-path').resolve()
    ).joinpath('*.h5')
    # Glob allows the use of the * wildcard
    files = glob(str(h5_outputs))

    if len(files) > 0:
        for file in files:
            try:
                with pd.HDFStore(file, mode='r') as hdf:
                    if len(hdf) > 0:
                        print(f'{file} was successfully read')
                    else:
                        print(f'{file} is empty')
            except OSError as error:
                print(f'Could not open {file} with error: {error}')
    else:
        print('No files found to validate')
