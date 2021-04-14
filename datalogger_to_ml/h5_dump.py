#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
from . import helper_methods


def dump(**kwargs):
    with pd.HDFStore(kwargs.get('input-file', kwargs.get('input_file')), 'r') as hdf:
        output = []

        for key in list(hdf.keys()):
            data_frame = hdf[key]
            output.append(f'{key}:\n{data_frame}')

        helper_methods.write_output(kwargs.get('output-file', kwargs.get('output_file')), output)
