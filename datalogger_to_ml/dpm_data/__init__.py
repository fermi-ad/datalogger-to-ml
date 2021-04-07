#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from .dpm_data import local_to_utc_ms
from .dpm_data import get_data
from .dpm_data import compare_hdf_device_list
from .dpm_data import generate_data_source

# https://packaging.python.org/guides/single-sourcing-package-version/#single-sourcing-the-version
try:
    from importlib import metadata
except ImportError:
    # Running on pre-3.8 Python; use importlib-metadata package
    import importlib_metadata as metadata

__version__ = 'v*.*.*'

try:
    __version__ = metadata.version(__name__)
except metadata.PackageNotFoundError:
    pass

__all__ = [
    '__version__',
    'local_to_utc_ms',
    'get_data',
    'compare_hdf_device_list',
    'generate_data_source'
]
