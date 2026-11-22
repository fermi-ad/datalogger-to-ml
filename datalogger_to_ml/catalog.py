#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Catalog of nanny output files: one SQLite database describing every file and each device in it.

SQLite because it is one file read through Python's own sqlite3: no server, no ports, no administration.
Run it after the nanny under the same lock, so there is one writer; any number of readers may query it.
Keep the database on local disk, not on NFS or pnfs: SQLite's locking is unreliable on network
filesystems, and pnfs files cannot be rewritten at all.

Statistics, per device and file:
- n_points: rows in the file (missing values included); points_per_s: n_points over the nominal window.
- v_min, v_q25, v_median, v_q75, v_max: quantiles of the non-missing values (linear interpolation, as
  pandas computes them).
- dt_mean, dt_var: mean and population variance (ddof=0) of the intervals between consecutive sample
  times, in seconds, after sorting by time.

Example queries:
    -- files where device X's range overlapped [V1, V2] (for a continuous signal, it took those values)
    SELECT f.path FROM files f JOIN device_stats d ON d.file_id = f.id
     WHERE d.device = 'C:5LL80H' AND d.v_max >= :v1 AND d.v_min <= :v2;
    -- files where device X stayed within [V1, V2] throughout
    ... WHERE d.device = 'C:5LL80H' AND d.v_min >= :v1 AND d.v_max <= :v2;
    -- how many files have device Y below N points per second
    SELECT count(*) FROM device_stats WHERE device = 'C:5PT81H' AND points_per_s < :n;
"""

from datetime import datetime, timezone
from pathlib import Path
import sqlite3

import numpy as np
import pandas as pd

from .dpm_data.dpm_data import as_microseconds
from .nanny import load_config, parse_iso

SCHEMA = """
CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY,
    path TEXT NOT NULL UNIQUE,           -- absolute path
    name TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    modified_utc TEXT NOT NULL,          -- last write (Linux keeps no portable creation time)
    window_start REAL,                   -- nominal window from the file name, Unix seconds
    window_end REAL,
    data_start REAL,                     -- earliest and latest sample in the file, Unix seconds
    data_end REAL,
    list_version TEXT,                   -- request-list version from the file name
    n_devices INTEGER NOT NULL,
    indexed_utc TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS device_stats (
    file_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    request TEXT NOT NULL,               -- as requested, e.g. C:5LL80H@P,1000,true
    device TEXT NOT NULL,                -- bare device name, e.g. C:5LL80H
    n_points INTEGER NOT NULL,
    points_per_s REAL,
    first_ts REAL, last_ts REAL,
    v_min REAL, v_q25 REAL, v_median REAL, v_q75 REAL, v_max REAL,
    dt_mean REAL, dt_var REAL,
    PRIMARY KEY (file_id, request)
);
CREATE INDEX IF NOT EXISTS device_stats_by_device ON device_stats (device, v_min, v_max);
CREATE INDEX IF NOT EXISTS device_stats_by_rate ON device_stats (device, points_per_s);
CREATE INDEX IF NOT EXISTS files_by_window ON files (window_start);
"""


def bare_device(request):
    """C:5LL80H@P,1000,true -> C:5LL80H; drops any event (@) or data source (<-)."""
    return request.split('<-')[0].split('@')[0]


def device_frames(path):
    """{request: frame of Timestamps (microseconds) and Data} from a nanny Parquet or HDF5 file."""
    if path.suffix == '.parquet':
        table = pd.read_parquet(path, columns=['device', 'Timestamps', 'Data'])
        return {request: frame for request, frame in table.groupby('device', sort=False)}
    with pd.HDFStore(path, mode='r') as store:
        return {key.lstrip('/'): store[key] for key in store.keys()}


def device_row(request, frame, window_seconds):
    times = np.sort(as_microseconds(frame['Timestamps'])) / 1e6  # files written before 1.6.3 may hold ms
    values = frame['Data'].dropna()
    quartiles = values.quantile([0, .25, .5, .75, 1]).tolist() if len(values) else [None] * 5
    steps = np.diff(times)
    return (request, bare_device(request), len(frame),
            len(frame) / window_seconds if window_seconds else None,
            float(times[0]) if len(times) else None, float(times[-1]) if len(times) else None,
            *quartiles,
            float(steps.mean()) if len(steps) else None, float(steps.var()) if len(steps) else None)


def window_of(name):
    """Nominal window and list version from a nanny file name, or (None, None, None)."""
    stem, _, rest = name.partition('-')
    if 'P' not in stem:  # not START+DURATION; parse_iso would assume an hour
        return None, None, None
    try:
        start, duration = parse_iso(stem)
    except ValueError:
        return None, None, None
    # Names are in host local time, as the nanny writes them.
    start_s = start.astimezone().timestamp()
    return start_s, start_s + duration.total_seconds(), rest.split('.')[0].replace('_', '.')


def index_file(db, path):
    stat = path.stat()
    window_start, window_end, version = window_of(path.name)
    window_seconds = window_end - window_start if window_start is not None else None
    rows = [device_row(request, frame, window_seconds) for request, frame in device_frames(path).items()]
    firsts = [r[4] for r in rows if r[4] is not None]
    lasts = [r[5] for r in rows if r[5] is not None]
    now = datetime.now(timezone.utc).isoformat(timespec='seconds')
    with db:  # one transaction per file
        db.execute('DELETE FROM files WHERE path = ?', (str(path),))
        file_id = db.execute(
            'INSERT INTO files (path, name, size_bytes, modified_utc, window_start, window_end, data_start,'
            ' data_end, list_version, n_devices, indexed_utc) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            (str(path), path.name, stat.st_size,
             datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(timespec='seconds'),
             window_start, window_end, min(firsts, default=None), max(lasts, default=None), version,
             len(rows), now)).lastrowid
        db.executemany(
            'INSERT INTO device_stats VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            [(file_id, *row) for row in rows])


def open_db(db_path):
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(db_path)
    db.execute('PRAGMA foreign_keys = ON')
    db.executescript(SCHEMA)
    return db


def build(**kwargs):
    """Index new or changed nanny files under the output path; drop entries for files that are gone."""
    config = load_config()
    root = kwargs.get('catalog-path') or kwargs.get('catalog_path') or config.get('output', {}).get('path', '.')
    db_path = kwargs.get('db') or config.get('catalog', 'catalog.sqlite')
    root = Path(root).resolve()
    db = open_db(db_path)
    known = {path: (size, modified) for path, size, modified in
             db.execute('SELECT path, size_bytes, modified_utc FROM files')}
    files = sorted(f for pattern in ('*.parquet', '*.h5') for f in root.rglob(pattern))
    indexed = 0
    for path in files:
        stat = path.stat()
        modified = datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(timespec='seconds')
        if known.get(str(path)) == (stat.st_size, modified):
            continue
        try:
            index_file(db, path)
            indexed += 1
        except (OSError, ValueError, KeyError) as error:
            print(f'Could not index {path}: {error}')
    present = {str(f) for f in files}
    gone = [p for p in known if p.startswith(str(root)) and p not in present]
    with db:
        db.executemany('DELETE FROM files WHERE path = ?', [(p,) for p in gone])
    total = db.execute('SELECT count(*) FROM files').fetchone()[0]
    db.close()
    print(f'catalog {db_path}: {indexed} file(s) indexed, {len(gone)} removed, {total} in total')
