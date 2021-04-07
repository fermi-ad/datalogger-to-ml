#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from . import h5_dump, h5_validator, nanny

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
    'h5_dump',
    'h5_validator',
    'nanny'
]
