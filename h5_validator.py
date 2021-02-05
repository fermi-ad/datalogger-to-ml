#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from glob import glob
from os import path
import pandas as pd
import sys

def main(output_path):
    h5_outputs = path.join(output_path,'*.h5')
    # Glob allows the use of the * wildcard
    files = glob(h5_outputs)
    for file in files:
        try:
            with pd.HDFStore(file) as hdf:
                if len(hdf) > 0:
                    print(f'{file} was successfully read')
                else:
                    print(f'{file} is empty')
        except OSError as error:
            print(f'Could not open {file} with error: {error}')

if __name__ == '__main__':
    main(sys.argv[1])
    