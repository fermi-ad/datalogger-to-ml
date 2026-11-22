#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sqlite3

import pandas as pd
import pytest

from datalogger_to_ml import catalog


def _parquet(path, start_us=0):
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
        store.append('A:ONE', pd.DataFrame({'Timestamps': [0, 1_000_000, 3_000_000], 'Data': [1.0, 2.0, 3.0]}))
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
    _parquet(parquet, start_us=10_000_000)
    os.utime(parquet, (1e9, 1e9))  # a different modification time marks it changed
    (tree / '20261113T010000PT1H-1_0_0.h5').unlink()
    catalog.build(catalog_path=tree, db=db_path)
    assert '1 file(s) indexed, 1 removed, 1 in total' in capsys.readouterr().out.splitlines()[-1]
    db = _db(tmp_path)
    assert db.execute('SELECT min(first_ts) FROM device_stats').fetchone()[0] == 10.0
    assert db.execute('SELECT count(*) FROM device_stats').fetchone()[0] == 2  # the deleted file's rows went with it
