#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
import logging
import sys
import warnings
import os
import re
from contextlib import contextmanager
from pathlib import Path
import signal
import numpy as np
import pandas as pd
import pacsys
from pacsys.types import ValueType
import pytz
import requests

OUTPUT_FORMATS = {
    'hdf5': '.h5',
    'csv': '.csv',
    'parquet': '.parquet',
}

IMPLEMENTED_OUTPUT_FORMATS = {
    'hdf5': '.h5',
    'parquet': '.parquet',
}

# Set the pacsys log to DEBUG
pacsys_log = logging.getLogger('pacsys')
pacsys_log.setLevel(logging.DEBUG)

# Local logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)


def _signal_handler(signal_num, _):
    logger.warning('Signal handler called with signal %s', signal_num)
    sys.exit(130)


def local_to_utc_ms(date):
    utc_datetime_obj = date.astimezone(pytz.utc)
    time_in_ms = int(utc_datetime_obj.timestamp() * 1000)
    return time_in_ms


def compare_device_list(written_keys, device_list, status_replies):
    if len(written_keys) != len(device_list):
        logger.error((
            'Empty DAQ for certain devices. All devices from '
            'device list are not present in the output.'
        ))
        logger.debug(
            '%s devices requested, %s devices written',
            len(device_list),
            len(written_keys)
        )

        if status_replies.count(True) < len(status_replies):
            for index, reply in enumerate(status_replies):
                if reply is not True:
                    logger.error(
                        'Status reply %s for %s',
                        reply,
                        device_list[index]
                    )
        else:
            logger.debug('No status replies received!')

        return False

    return True


def compare_hdf_device_list(hdf, device_list, status_replies):
    return compare_device_list(hdf.keys(), device_list, status_replies)


# Total time allowed for one batch read. Untested: a long window over many devices has not
# been timed through pacsys yet; raise it if pulls time out.
DEFAULT_TIMEOUT = 600.0


def _with_source(request, data_source):
    """Attach the logger data source to a DRF request, unless the request names its own."""
    return request if '<-' in request else f'{request}<-{data_source}'


# Some channels' logger stamps arrive in milliseconds, not the microseconds pacsys labels them (digital
# status channels such as C:*.STATUS.ON, through pacsys 0.3.0); whether the logger, DPM, or pacsys does it is
# not known. Below 1e15 a microsecond stamp would predate September 2001, so with no window to judge by, any
# such stamp is taken as milliseconds.
MICROSECOND_FLOOR = 10**15


def as_microseconds(stamps):
    """Fallback when no window is known: stamps below MICROSECOND_FLOOR are taken as milliseconds."""
    stamps = np.asarray(stamps, dtype='int64')
    return np.where(stamps < MICROSECOND_FLOOR, stamps * 1000, stamps)


# Units a device's stamps may arrive in, as (name, multiplier to microseconds), tried in this order; the
# first wins a tie, so stamps that fit as microseconds are never rescaled.
STAMP_UNITS = (('us', 1), ('ms', 1000), ('s', 1_000_000), ('ns', 1e-3))
# Stamps may sit a little outside the requested window: in the first CMTF catch-up they lay within 1 s of
# its edges, and 5 s leaves margin. Untested beyond that.
WINDOW_SLACK_US = 5_000_000


def outside_window(micros, window_us, slack_us=WINDOW_SLACK_US):
    """Boolean mask of stamps (microseconds) outside the window [start, end), widened by slack."""
    start, end = window_us
    micros = np.asarray(micros)
    return (micros < start - slack_us) | (micros >= end + slack_us)


def to_microseconds(stamps, window_us):
    """One device's stamps in microseconds, the unit chosen by the requested window.

    Tries each of STAMP_UNITS and keeps the one that puts the most stamps inside the window, so
    milliseconds (the status channels), seconds, and nanoseconds are all caught. One unit per device,
    since a device's stamps all arrive through the same reply. Nothing is dropped: when no unit puts
    any stamp inside the window, the stamps are kept as received, for diagnosis, and the catalog counts
    stamps outside the window per device (n_outside_window).

    Returns (micros, unit, n_outside). With no window (window_us None), falls back to as_microseconds.
    """
    stamps = np.asarray(stamps, dtype='int64')
    if window_us is None:
        return as_microseconds(stamps), 'us', 0
    best = None
    for unit, factor in STAMP_UNITS:
        micros = stamps if factor == 1 else np.round(stamps * factor).astype('int64')
        n_outside = int(outside_window(micros, window_us).sum())
        if best is None or n_outside < best[2]:
            best = (micros, unit, n_outside)
    if len(stamps) and best[2] == len(stamps):  # no unit fits any stamp: keep them as received
        return stamps, 'us', len(stamps)
    return best


def window_of_source(data_source):
    """The requested window in microseconds from LOGGER:start_ms:end_ms; None for any other source."""
    match = re.fullmatch(r'LOGGER:(\d+):(\d+)', data_source or '')
    return (int(match[1]) * 1000, int(match[2]) * 1000) if match else None


def _reading_to_frame(reading, window_us=None, request=None):
    """Turn a pacsys logger reading into a frame of Timestamps (microseconds) and Data.

    pacsys joins the logger's reply chunks into one timed array. Array devices give one row per
    record, with a Data[i] column per element. Returns None when the reading carries no data.
    Stamps go through to_microseconds and every point is kept. A rescaling that fits is logged at INFO,
    since the status channels need one on every read until the cause is fixed; stamps outside the
    window are logged as a warning.
    """
    if not reading.ok or reading.value_type != ValueType.TIMED_SCALAR_ARRAY:
        return None
    data = np.asarray(reading.value['data'])
    micros, unit, n_outside = to_microseconds(reading.value['micros'], window_us)
    name = request or reading.drf
    if unit != 'us':
        logger.info('%s: logger stamps arrived in %s; converted to microseconds', name, unit)
    if n_outside:
        logger.warning('%s: %d of %d stamps fall outside the requested window; written as received',
                       name, n_outside, len(micros))
    if data.ndim == 2:
        frame = pd.DataFrame(data, columns=[f'Data[{i}]' for i in range(data.shape[1])])
        frame.insert(0, 'Timestamps', micros)
        return frame
    return pd.DataFrame({'Timestamps': micros, 'Data': data})


def _read_logger(device_list, store, data_source, dpm_host=None,
                 timeout=DEFAULT_TIMEOUT):
    """Read every device's logged history in one batch and write each to the store."""
    drfs = [_with_source(device, data_source) for device in device_list]
    logger.debug('Starting DAQ...')
    with pacsys.dpm(host=dpm_host) as backend:
        readings = backend.get_many(drfs, timeout=timeout)
    logger.info('Data acquisition complete')

    window_us = window_of_source(data_source)
    status_replies = []
    for request, reading in zip(device_list, readings):
        frame = _reading_to_frame(reading, window_us, request)
        if frame is not None and len(frame):
            store.append(request, frame)
        if not reading.ok:
            logger.warning(
                'Returned status %s for %s',
                reading.message or reading.error_code,
                request
            )
        status_replies.append(
            True if reading.ok else (reading.message or reading.error_code)
        )

    compare_device_list(store.keys(), device_list, status_replies)


class _ParquetStore:
    """Collects each device's frame the way HDFStore.append does, then writes one long table.

    Columns: device, Timestamps (microseconds), Data. Long, not one column per device, because
    devices carry their own timestamps; aligning them is left to whoever reads the file. An array
    device becomes one device[i] series per element.
    """

    def __init__(self):
        self.frames = {}

    def append(self, key, frame):
        self.frames[key] = pd.concat([self.frames[key], frame]) if key in self.frames else frame

    def keys(self):
        return list(self.frames)

    def table(self):
        parts = []
        for device, frame in self.frames.items():
            for column in frame.columns.drop('Timestamps'):
                name = device if column == 'Data' else f'{device}{column[4:]}'
                parts.append(pd.DataFrame({
                    'device': name,
                    'Timestamps': frame['Timestamps'].to_numpy(dtype='int64'),
                    'Data': frame[column].to_numpy(dtype=float),
                }))
        if not parts:
            return pd.DataFrame({'device': pd.Series(dtype=str),
                                 'Timestamps': pd.Series(dtype='int64'),
                                 'Data': pd.Series(dtype=float)})
        return pd.concat(parts, ignore_index=True)


@contextmanager
def _open_output(output_file, output_format):
    if output_format == 'hdf5':
        with pd.HDFStore(output_file) as store:
            yield store
    elif output_format == 'parquet':
        store = _ParquetStore()
        yield store
        store.table().to_parquet(output_file, index=False)
    elif output_format in OUTPUT_FORMATS:
        raise NotImplementedError(
            f'Output format {output_format!r} is not yet implemented.'
        )
    else:
        raise ValueError(
            f'Unknown output format: {output_format!r}. '
            f'Known formats: {list(OUTPUT_FORMATS.keys())}'
        )


def generate_data_source(start_date, end_date, duration):
    result = ''

    if duration and (start_date or end_date):
        print('-d and -s|-e are mutually exclusive! Exiting ...')
        # Unix uses exit status 2 to indicate CLI syntax error
        sys.exit(2)

    elif end_date and not start_date:
        print(('Just entering end date is invalid. '
               'Please enter start date AND end date, '
               'or just start date.'))
        # Unix uses exit status 2 to indicate CLI syntax error
        sys.exit(2)

    elif start_date:
        if not end_date:
            end_date = datetime.datetime.now()  # This is not midnight time

        result = (
            'LOGGER:'
            f'{local_to_utc_ms(start_date)}:'
            f'{local_to_utc_ms(end_date)}'
        )

    elif duration:
        duration = int(duration) * 1000
        result = f'LOGGERDURATION:{duration}'

    else:
        duration = 3600 * 1000
        result = f'LOGGERDURATION:{duration}'

    return result


def _write_output(path, output):
    with open(path, 'w+') as file:
        for line in output:
            file.write(f'{line}\n')


def _get_latest_device_list(output_filename=None):
    url = ('https://github.com/fermi-ad/linac-logger-device-cleaner/'
           'releases/latest/download/linac_logger_drf_requests.txt')
    req = requests.get(url, allow_redirects=False)

    if req.status_code == requests.codes.get('ok'):
        device_list = [line.strip()  # Trim whitespace
                       for line in req.text.split('\n')
                       if line]  # Remove blank lines

        if output_filename:
            _write_output(output_filename, device_list)

        return device_list

    print(f'Could not fetch {url}')

    return None


def _generate_device_list(device_limit, device_file=None):
    # The input is line separated devices.
    result = []

    if device_file is not None:
        logger.debug('Opening device file: %s', device_file)
        with open(device_file) as file_handle:
            result = [line.rstrip() for line in file_handle if line]
    else:
        logger.debug('Fetching latest device list')
        result = _get_latest_device_list()

    if device_limit > 0:
        logger.info('Limiting device list to %s device(s)', device_limit)
        result = [line for index, line in enumerate(result)
                  if index < device_limit]

    return result


def get_data(**kwargs):
    signal.signal(signal.SIGINT, _signal_handler)

    start_date = kwargs.get('start-date', kwargs.get('start_date', None))
    end_date = kwargs.get('end-date', kwargs.get('end_date', None))
    duration = kwargs.get('duration', None)
    device_limit = kwargs.get('device-limit', 0)
    device_file = kwargs.get('device-file', kwargs.get('device_file', None))
    output_format = kwargs.get(
        'output-format',
        kwargs.get('output_format', 'hdf5')
    )
    file_extension = OUTPUT_FORMATS[output_format]
    output_file = kwargs.get(
        'output-file',
        kwargs.get('output_file', Path(f'data{file_extension}'))
    )
    dpm_host = kwargs.get('dpm-host', kwargs.get('dpm_host', None))
    debug = kwargs.get('debug', False)

    # Silence STDOUT warnings
    warnings.simplefilter('ignore')

    if debug:
        logger.setLevel(logging.DEBUG)

    logger.debug(
        (
            'start_date: %s, end_date: %s, duration: %s, device_file: %s, '
            'dpm_host: %s, device_limit: %s, output_file: %s, '
            'output_format: %s, debug: %s'
        ),
        start_date,
        end_date,
        duration,
        device_file,
        dpm_host,
        device_limit,
        output_file,
        output_format,
        debug
    )

    if output_file.exists():
        os.remove(output_file)

    device_list = _generate_device_list(device_limit, device_file)
    data_source = generate_data_source(start_date, end_date, duration)
    logger.debug('data_source: %s', data_source)

    with _open_output(output_file, output_format) as store:
        _read_logger(device_list, store, data_source, dpm_host)
