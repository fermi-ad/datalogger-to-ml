#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd
import sys
import helper_methods


def main(input_file, output_file):
    with pd.HDFStore(input_file, 'r') as hdf:
        output = []

        for key in list(hdf.keys()):
            df = hdf[key]
            output.append(f'{key}:\n{df}')

        helper_methods.write_output(output_file, output)


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])