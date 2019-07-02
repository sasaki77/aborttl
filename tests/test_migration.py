import json

import pytest

from aborttl.dbhandler import DbHandler
from aborttl.migration import Migration


def test_migration_from_json_both(tmpdir):
    her_aborts = [
                     {
                         "RESET": 0,
                         "NAME": "ABORT_A",
                         "TCNT": 0,
                         "TS": "2019-01-01 00:00:00.000000",
                         "MSG": "MESSAGE A",
                         "DATE": "2019-01-01 00:00:00.000000000",
                         "ACNT": 1
                     },
                     {
                         "RESET": 0,
                         "NAME": "ABORT_B",
                         "MSG": "MESSAGE B",
                         "TCNT": 1,
                         "TS": "2019-01-01 00:00:01.000000",
                         "DATE": "2019-01-01 00:00:01.000000000",
                         "ACNT": 2
                     },
                     {
                         "RESET": 1,
                         "NAME": "ABORT_C",
                         "MSG": "MESSAGE C",
                         "TCNT": 2,
                         "TS": "2019-01-01 00:00:02.100000",
                         "DATE": "2019-01-01 00:00:02.100000000",
                         "ACNT": 3
                     },
                 ]

    ler_aborts = [
                     {
                         "RESET": 0,
                         "NAME": "ABORT_D",
                         "TCNT": 0,
                         "TS": "2019-01-01 00:00:00.000000",
                         "MSG": "MESSAGE D",
                         "DATE": "2019-01-01 00:00:00.000000000",
                         "ACNT": 1
                     },
                     {
                         "RESET": 0,
                         "NAME": "ABORT_E",
                         "MSG": "MESSAGE E",
                         "TCNT": 1,
                         "TS": "2019-01-01 00:00:01.000000",
                         "DATE": "2019-01-01 00:00:01.000000000",
                         "ACNT": 2
                     },
                     {
                         "RESET": 1,
                         "NAME": "ABORT_F",
                         "MSG": "MESSAGE F",
                         "TCNT": 2,
                         "TS": "2019-01-01 00:00:02.100000",
                         "DATE": "2019-01-01 00:00:02.100000000",
                         "ACNT": 3
                     },
                 ]

    pdb = tmpdir.join("database.db")
    uri = 'sqlite:///{}'.format(str(pdb))

    root = tmpdir.mkdir('root')
    herdir = root.mkdir('HER')
    lerdir = root.mkdir('LER')
    pher = herdir.join("20190101000000.json")
    pler = lerdir.join("20190101000000.json")

    pher.write(json.dumps(her_aborts))
    pler.write(json.dumps(ler_aborts))

    mg = Migration(uri, str(root), 'HER', 'LER')
    mg.migration_from_json()

    dh = DbHandler(uri)
    pvs = dh.fetch_all_pvs()

    test_pvs = [('ABORT_A', 'HER'), ('ABORT_B', 'HER'), ('ABORT_C', 'HER'),
                ('ABORT_D', 'LER'), ('ABORT_E', 'LER'), ('ABORT_F', 'LER')]

    assert pvs == test_pvs

    signals = dh.fetch_abort_signals(include_no_abt_id=True, first=False)

    test_signals = [(1, '2019-01-01 00:00:00.000000000', 'ABORT_A',
                     'MESSAGE A', 'HER', 0, 0, 1),
                    (1, '2019-01-01 00:00:00.000000000', 'ABORT_D',
                        'MESSAGE D', 'LER', 0, 0, 1),
                    (1, '2019-01-01 00:00:01.000000000', 'ABORT_B',
                        'MESSAGE B', 'HER', 0, 1, 2),
                    (1, '2019-01-01 00:00:01.000000000', 'ABORT_E',
                        'MESSAGE E', 'LER', 0, 1, 2),
                    (1, '2019-01-01 00:00:02.100000000', 'ABORT_C',
                        'MESSAGE C', 'HER', 1, 2, 3),
                    (1, '2019-01-01 00:00:02.100000000', 'ABORT_F',
                        'MESSAGE F', 'LER', 1, 2, 3)]

    assert signals == test_signals


def test_migration_from_json_single(tmpdir):
    her_aborts = [
                     {
                         "RESET": 0,
                         "NAME": "ABORT_A",
                         "TCNT": 0,
                         "TS": "2019-01-01 00:00:00.000000",
                         "MSG": "MESSAGE A",
                         "DATE": "2019-01-01 00:00:00.000000000",
                         "ACNT": 1
                     },
                     {
                         "RESET": 0,
                         "NAME": "ABORT_B",
                         "MSG": "MESSAGE B",
                         "TCNT": 1,
                         "TS": "2019-01-01 00:00:01.000000",
                         "DATE": "2019-01-01 00:00:01.000000000",
                         "ACNT": 2
                     },
                     {
                         "RESET": 1,
                         "NAME": "ABORT_C",
                         "MSG": "MESSAGE C",
                         "TCNT": 2,
                         "TS": "2019-01-01 00:00:02.100000",
                         "DATE": "2019-01-01 00:00:02.100000000",
                         "ACNT": 3
                     },
                 ]

    ler_aborts = [
                     {
                         "RESET": 0,
                         "NAME": "ABORT_D",
                         "TCNT": 0,
                         "TS": "2019-01-01 00:00:00.000000",
                         "MSG": "MESSAGE D",
                         "DATE": "2019-01-01 00:00:01.000000000",
                         "ACNT": 1
                     },
                     {
                         "RESET": 0,
                         "NAME": "ABORT_E",
                         "MSG": "MESSAGE E",
                         "TCNT": 1,
                         "TS": "2019-01-01 00:00:01.000000",
                         "DATE": "2019-01-01 00:00:01.000000000",
                         "ACNT": 2
                     },
                     {
                         "RESET": 1,
                         "NAME": "ABORT_F",
                         "MSG": "MESSAGE F",
                         "TCNT": 2,
                         "TS": "2019-01-01 00:00:02.100000",
                         "DATE": "2019-01-01 00:00:02.100000000",
                         "ACNT": 3
                     },
                 ]

    pdb = tmpdir.join("database.db")
    uri = 'sqlite:///{}'.format(str(pdb))

    root = tmpdir.mkdir('root')
    herdir = root.mkdir('HER')
    lerdir = root.mkdir('LER')
    pher = herdir.join("20190101000000.json")
    pler = lerdir.join("20190101000001.json")

    pher.write(json.dumps(her_aborts))
    pler.write(json.dumps(ler_aborts))

    mg = Migration(uri, str(root), 'HER', 'LER')
    mg.migration_from_json()

    dh = DbHandler(uri)
    pvs = dh.fetch_all_pvs()

    test_pvs = [('ABORT_A', 'HER'), ('ABORT_B', 'HER'), ('ABORT_C', 'HER'),
                ('ABORT_D', 'LER'), ('ABORT_E', 'LER'), ('ABORT_F', 'LER')]

    assert pvs == test_pvs

    signals = dh.fetch_abort_signals(include_no_abt_id=True, first=False)

    test_signals = [(1, '2019-01-01 00:00:00.000000000', 'ABORT_A',
                     'MESSAGE A', 'HER', 0, 0, 1),
                    (1, '2019-01-01 00:00:01.000000000', 'ABORT_B',
                        'MESSAGE B', 'HER', 0, 1, 2),
                    (1, '2019-01-01 00:00:02.100000000', 'ABORT_C',
                        'MESSAGE C', 'HER', 1, 2, 3),
                    (2, '2019-01-01 00:00:01.000000000', 'ABORT_D',
                        'MESSAGE D', 'LER', 0, 0, 1),
                    (2, '2019-01-01 00:00:01.000000000', 'ABORT_E',
                        'MESSAGE E', 'LER', 0, 1, 2),
                    (2, '2019-01-01 00:00:02.100000000', 'ABORT_F',
                        'MESSAGE F', 'LER', 1, 2, 3)]

    assert signals == test_signals


def test_migration_without_ts(tmpdir):
    her_aborts = [
                     {
                         "RESET": 0,
                         "NAME": "ABORT_A",
                         "TCNT": 0,
                         "MSG": "MESSAGE A",
                         "DATE": "2019-01-01 00:00:00.000000000",
                         "ACNT": 1
                     },
                     {
                         "RESET": 0,
                         "NAME": "ABORT_B",
                         "MSG": "MESSAGE B",
                         "TCNT": 1,
                         "DATE": "2019-01-01 00:00:01.000000000",
                         "ACNT": 2
                     },
                     {
                         "RESET": 1,
                         "NAME": "ABORT_C",
                         "MSG": "MESSAGE C",
                         "TCNT": 2,
                         "DATE": "2019-01-01 00:00:02.100000000",
                         "ACNT": 3
                     },
                 ]

    pdb = tmpdir.join("database.db")
    uri = 'sqlite:///{}'.format(str(pdb))

    root = tmpdir.mkdir('root')
    herdir = root.mkdir('HER')
    lerdir = root.mkdir('LER')
    pher = herdir.join("20190101000000.json")

    pher.write(json.dumps(her_aborts))

    mg = Migration(uri, str(root), 'HER', 'LER')
    mg.migration_from_json()

    dh = DbHandler(uri)
    pvs = dh.fetch_all_pvs()

    test_pvs = [('ABORT_A', 'HER'), ('ABORT_B', 'HER'), ('ABORT_C', 'HER')]

    assert pvs == test_pvs

    signals = dh.fetch_abort_signals(include_no_abt_id=True, first=False)

    test_signals = [(1, '2019-01-01 00:00:00.000000000', 'ABORT_A',
                     'MESSAGE A', 'HER', 0, 0, 1),
                    (1, '2019-01-01 00:00:01.000000000', 'ABORT_B',
                        'MESSAGE B', 'HER', 0, 1, 2),
                    (1, '2019-01-01 00:00:02.100000000', 'ABORT_C',
                        'MESSAGE C', 'HER', 1, 2, 3)
                    ]

    assert signals == test_signals
