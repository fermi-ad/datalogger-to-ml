#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sqlite3
from datetime import datetime

import pandas as pd
import pytest

from datalogger_to_ml import catalog

# Unix seconds at the start of the fixture's first window, 20261113T000000 in host local time, as file names are
# read; stamps must lie in their file's window, or the catalog counts them outside it.
T0 = int(datetime(2026, 11, 13).timestamp())


def _parquet(path, start_us=T0 * 1_000_000):
    """Two devices over one hour: A at 1 Hz (3600 points, values 0..3599), B at 0.5 Hz (1800 points)."""
    a = pd.DataFrame({'device': 'A:ONE@P,1000,true', 'Timestamps': start_us + pd.RangeIndex(3600) * 1_000_000,
                      'Data': pd.RangeIndex(3600).astype(float)})
    b = pd.DataFrame({'device': 'B:TWO@P,2000,true', 'Timestamps': start_us + pd.RangeIndex(1800) * 2_000_000,
                      'Data': 5.0})
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.concat([a, b]).to_parquet(path, index=False)


@pytest.fixture
def tree(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)  # no config.yaml here
    out = tmp_path / 'out'
    _parquet(out / '202611' / '13' / '20261113T000000PT1H-1_0_0.parquet')
    with pd.HDFStore(out / '20261113T010000PT1H-1_0_0.h5') as store:
        stamps = [(T0 + 3600) * 1_000_000 + t for t in (0, 1_000_000, 3_000_000)]  # its window starts at 01:00
        store.append('A:ONE', pd.DataFrame({'Timestamps': stamps, 'Data': [1.0, 2.0, 3.0]}))
    return out


def _db(tmp_path):
    db = sqlite3.connect(tmp_path / 'cat.sqlite')
    db.row_factory = sqlite3.Row
    return db


def test_statistics_per_device(tree, tmp_path):
    catalog.build(catalog_path=tree, db=tmp_path / 'cat.sqlite')
    db = _db(tmp_path)
    f = db.execute("SELECT * FROM files WHERE name LIKE '%.parquet'").fetchone()
    assert f['window_end'] - f['window_start'] == 3600 and f['n_devices'] == 2 and f['list_version'] == '1.0.0'
    a = db.execute("SELECT * FROM device_stats WHERE device = 'A:ONE' AND file_id = ?", (f['id'],)).fetchone()
    assert a['request'] == 'A:ONE@P,1000,true' and a['n_points'] == 3600 and a['points_per_s'] == 1.0
    assert (a['v_min'], a['v_median'], a['v_max']) == (0.0, 1799.5, 3599.0)
    assert a['v_q25'] == pytest.approx(899.75) and a['v_q75'] == pytest.approx(2699.25)
    assert a['dt_mean'] == 1.0 and a['dt_var'] == 0.0
    b = db.execute("SELECT * FROM device_stats WHERE device = 'B:TWO'").fetchone()
    assert b['n_points'] == 1800 and b['points_per_s'] == 0.5 and b['dt_mean'] == 2.0
    h5 = db.execute("SELECT * FROM device_stats d JOIN files f ON f.id = d.file_id WHERE f.name LIKE '%.h5'").fetchone()
    assert h5['n_points'] == 3 and h5['dt_mean'] == 1.5 and h5['dt_var'] == 0.25


def test_example_queries(tree, tmp_path):
    catalog.build(catalog_path=tree, db=tmp_path / 'cat.sqlite')
    db = _db(tmp_path)
    overlapped = db.execute("SELECT f.name FROM files f JOIN device_stats d ON d.file_id = f.id "
                            "WHERE d.device = 'A:ONE' AND d.v_max >= ? AND d.v_min <= ?", (2.5, 10)).fetchall()
    assert sorted(r['name'] for r in overlapped) == ['20261113T000000PT1H-1_0_0.parquet', '20261113T010000PT1H-1_0_0.h5']
    sparse = db.execute("SELECT count(*) FROM device_stats WHERE device = 'B:TWO' AND points_per_s < 1").fetchone()[0]
    assert sparse == 1


def test_reruns_skip_unchanged_reindex_changed_drop_deleted(tree, tmp_path, capsys):
    db_path = tmp_path / 'cat.sqlite'
    catalog.build(catalog_path=tree, db=db_path)
    catalog.build(catalog_path=tree, db=db_path)
    assert '0 file(s) indexed, 0 removed, 2 in total' in capsys.readouterr().out.splitlines()[-1]

    parquet = next(tree.rglob('*.parquet'))
    _parquet(parquet, start_us=(T0 + 10) * 1_000_000)
    os.utime(parquet, (1e9, 1e9))  # a different modification time marks it changed
    (tree / '20261113T010000PT1H-1_0_0.h5').unlink()
    catalog.build(catalog_path=tree, db=db_path)
    assert '1 file(s) indexed, 1 removed, 1 in total' in capsys.readouterr().out.splitlines()[-1]
    db = _db(tmp_path)
    assert db.execute('SELECT min(first_ts) FROM device_stats').fetchone()[0] == T0 + 10
    assert db.execute('SELECT count(*) FROM device_stats').fetchone()[0] == 2  # the deleted file's rows went with it


def test_creates_the_database_directory(tree, tmp_path):
    catalog.build(catalog_path=tree, db=tmp_path / 'new' / 'dir' / 'cat.sqlite')
    assert (tmp_path / 'new' / 'dir' / 'cat.sqlite').exists()


def test_millisecond_stamps_are_corrected(tmp_path, monkeypatch):
    """Status channels arrive stamped in milliseconds; the catalog reads them as the times they are."""
    monkeypatch.chdir(tmp_path)
    out = tmp_path / 'out'
    out.mkdir()
    start = T0
    pd.DataFrame({'device': ['A:ONE', 'A:ONE', 'S:ST.STATUS.ON', 'S:ST.STATUS.ON'],
                  'Timestamps': [start * 1_000_000, (start + 1) * 1_000_000, start * 1000, (start + 2) * 1000],
                  'Data': [1.0, 2.0, 0.0, 1.0]}).to_parquet(out / 'w.parquet', index=False)
    catalog.build(catalog_path=out, db=tmp_path / 'cat.sqlite')
    db = _db(tmp_path)
    st = db.execute("SELECT * FROM device_stats WHERE device = 'S:ST.STATUS.ON'").fetchone()
    assert (st['first_ts'], st['dt_mean']) == (start, 2.0)
    assert db.execute('SELECT data_start FROM files').fetchone()[0] == start


def test_stamps_outside_the_window_are_counted_not_dropped(tmp_path, monkeypatch):
    """Per device: strays are counted in n_outside_window and still enter the statistics; ms stamps count in."""
    monkeypatch.chdir(tmp_path)
    path = tmp_path / 'out' / '202611' / '13' / '20261113T000000PT1H-1_0_0.parquet'
    path.parent.mkdir(parents=True)
    us = T0 * 1_000_000
    pd.DataFrame({
        'device': ['A:ONE'] * 3 + ['S:ST.STATUS.ON'] * 2 + ['C:ODD'] * 3,
        'Timestamps': [us, us + 1_000_000, us + 2_000_000,
                       us // 1000, us // 1000 + 1000,                       # milliseconds, inside the window
                       us, us - 86_400_000_000, us + 7_200_000_000],        # a day early and two hours late
        'Data': [1.0, 2.0, 3.0, 0.0, 1.0, 5.0, 6.0, 7.0],
    }).to_parquet(path, index=False)
    catalog.build(catalog_path=tmp_path / 'out', db=tmp_path / 'cat.sqlite')
    rows = {r['device']: r for r in _db(tmp_path).execute('SELECT * FROM device_stats')}
    assert rows['A:ONE']['n_outside_window'] == 0
    assert rows['S:ST.STATUS.ON']['n_outside_window'] == 0 and rows['S:ST.STATUS.ON']['first_ts'] == T0
    assert rows['C:ODD']['n_outside_window'] == 2 and rows['C:ODD']['n_points'] == 3
    assert rows['C:ODD']['first_ts'] == T0 - 86_400


def test_no_window_leaves_the_count_null(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    out = tmp_path / 'out'
    out.mkdir()
    pd.DataFrame({'device': ['A:ONE'], 'Timestamps': [T0 * 1_000_000], 'Data': [1.0]}).to_parquet(out / 'w.parquet')
    catalog.build(catalog_path=out, db=tmp_path / 'cat.sqlite')
    assert _db(tmp_path).execute('SELECT n_outside_window FROM device_stats').fetchone()[0] is None


def test_older_schema_is_rebuilt(tree, tmp_path, capsys):
    """A catalog without user_version (1.6.x) is emptied and indexed again with the new columns."""
    db_path = tmp_path / 'cat.sqlite'
    old = sqlite3.connect(db_path)
    old.executescript('CREATE TABLE files (id INTEGER PRIMARY KEY, path TEXT); INSERT INTO files VALUES (1, \'x\');')
    old.close()
    catalog.build(catalog_path=tree, db=db_path)
    lines = capsys.readouterr().out.splitlines()
    assert 'rebuilding' in lines[0] and '2 file(s) indexed, 0 removed, 2 in total' in lines[-1]
    db = _db(tmp_path)
    assert db.execute('PRAGMA user_version').fetchone()[0] == catalog.SCHEMA_VERSION
    assert db.execute('SELECT count(*) FROM device_stats WHERE n_outside_window = 0').fetchone()[0] == 3
