import pytest
import sqlalchemy as sa

from aborttl.dbhandler import DbHandler


@pytest.fixture
def dh(mock_data):
    uri = 'sqlite:///:memory:'
    dh = DbHandler(uri)
    conn = dh.engine.connect()

    conn.execute(dh.tables['pvs'].insert(), mock_data['pvs'])
    conn.execute(dh.tables['current_pvs'].insert(), mock_data['current_pvs'])
    conn.execute(dh.tables['aborts'].insert(), mock_data['aborts'])
    conn.execute(dh.tables['abort_signals'].insert(),
                 mock_data['abort_signals'])
    conn.execute(dh.tables['abort_list'].insert(), mock_data['abort_list'])

    conn.close()

    return dh


def test_table_schema(dh):
    tables = dh.meta.tables.keys()
    table_list = ['pvs', 'current_pvs', 'abort_signals',
                  'aborts', 'abort_list']
    schemas = {}

    # schema = cid, name, type, notnull, dflt_value, pk
    schemas['pvs'] = [
                        (0, 'pvname', 'TEXT', 1, None, 1),
                        (1, 'ring', 'TEXT', 0, None, 0)
                      ]

    schemas['current_pvs'] = [
                               (0, 'pvname', 'TEXT', 1, None, 1),
                               (1, 'msg', 'TEXT', 0, None, 0)
                              ]

    schemas['abort_signals'] = [
                                 (0, 'abt_signal_id', 'INTEGER', 1, None, 1),
                                 (1, 'pvname', 'TEXT', 0, None, 0),
                                 (2, 'msg', 'TEXT', 0, None, 0),
                                 (3, 'pv_ts', 'TEXT', 0, None, 0),
                                 (4, 'abt_ts', 'TEXT', 0, None, 0),
                                 (5, 'reset_cnt', 'INTEGER', 0, None, 0),
                                 (6, 'trg_cnt', 'INTEGER', 0, None, 0),
                                 (7, 'int_cnt', 'INTEGER', 0, None, 0),
                                ]

    schemas['aborts'] = [
                          (0, 'abt_id', 'INTEGER', 1, None, 1),
                          (1, 'abt_time', 'TEXT', 0, None, 0)
                         ]

    schemas['abort_list'] = [
                              (0, 'abt_id', 'INTEGER', 1, None, 1),
                              (1, 'abt_signal_id', 'INTEGER', 1, None, 2)
                             ]

    for table in table_list:
        schema = dh.engine.execute("PRAGMA TABLE_INFO('{}')".format(table))

        for r1, r2 in zip(schemas[table], schema):
            assert r1 == r2


def test_table_index(dh):
    table_list = ['abort_signals', 'aborts']

    # index = seq, name, unique, origin, partial
    indices = {}
    indices['abort_signals'] = [
                                 (0, 'ix_abort_signals_abt_ts', 0, 'c', 0),
                                 (1, 'ix_abort_signals_msg', 0, 'c', 0)
                               ]

    indices['aborts'] = [(0, 'ix_aborts_abt_time', 1, 'c', 0)]

    for table in table_list:
        index = dh.engine.execute("PRAGMA INDEX_LIST('{}')".format(table))

        for r1 in index:
            for r2 in indices[table]:
                if r1[1:] == r2[1:]:
                    break
            else:
                assert False, '{} == {}'.format(r1, r2)


def test_table_foreign_key(dh):
    table_list = ['current_pvs', 'abort_signals', 'abort_list']

    # fk = id, seq, table, from, to, on_update, on_delete, match
    fks = {}
    fks['current_pvs'] = [(0, 0, 'pvs', 'pvname', 'pvname',
                           'NO ACTION', 'NO ACTION', 'NONE')]
    fks['abort_signals'] = [
                             (0, 0, 'pvs', 'pvname', 'pvname',
                                 'NO ACTION', 'NO ACTION', 'NONE')
                            ]

    fks['abort_list'] = [
                          (0, 0, 'abort_signals', 'abt_signal_id',
                              'abt_signal_id', 'NO ACTION',
                              'NO ACTION', 'NONE'),
                          (1, 0, 'aborts', 'abt_id', 'abt_id',
                              'NO ACTION', 'NO ACTION', 'NONE')
                         ]

    for table in table_list:
        fk = dh.engine.execute("PRAGMA FOREIGN_KEY_LIST('{}')".format(table))

        for r1, r2 in zip(fks[table], fk):
            assert r1 == r2


def test_fetch_all_pvs(dh, mock_data):
    pvs = dh.fetch_all_pvs()
    for pv_mock, pv_db in zip(mock_data['pvs'], pvs):
        assert pv_mock['pvname'] == pv_db['pvname']
        assert pv_mock['ring'] == pv_db['ring']


def test_fetch_current_pvs(dh, mock_data):
    f_pvs = dh.fetch_current_pvs()
    current_pvs = mock_data['current_pvs']
    pvs = mock_data['pvs'][1:7]
    for pv_mock1, pv_mock2, pv_db in zip(current_pvs, pvs, f_pvs):
        assert pv_mock1['pvname'] == pv_db['pvname']
        assert pv_mock1['msg'] == pv_db['msg']
        assert pv_mock2['ring'] == pv_db['ring']


def test_fetch_abort_signals(dh, mock_data):
    signals = dh.fetch_abort_signals()
    for s1, s2 in zip(mock_data['as_all_result'], signals):
        assert s1['abt_id'] == s2['abt_id']
        assert s1['ts'] == s2['ts']
        assert s1['pvname'] == s2['pvname']
        assert s1['msg'] == s2['msg']
        assert s1['ring'] == s2['ring']
        assert s1['reset_cnt'] == s2['reset_cnt']
        assert s1['trg_cnt'] == s2['trg_cnt']
        assert s1['int_cnt'] == s2['int_cnt']

    signals = dh.fetch_abort_signals(first=False)
    assert len(signals) == 15

    signals = dh.fetch_abort_signals(ring='LER')
    assert len(signals) == 5

    signals = dh.fetch_abort_signals(astart='2018-01-05', aend='2018-01-06')
    assert len(signals) == 2

    signals = dh.fetch_abort_signals(sstart='2018-01-02', send='2018-01-04')
    assert len(signals) == 3


def test_update_current_pvs(dh):
    pvs = [
             {'pvname': 'A', 'msg': 'Abort A'},
             {'pvname': 'B', 'msg': 'Abort B'},
             {'pvname': 'C', 'msg': 'Abort C'},
             {'pvname': 'D', 'msg': 'Abort D'},
             {'pvname': 'E', 'msg': 'Abort E'}
            ]
    dh.update_current_pvs(pvs)

    db_pvs = dh.fetch_current_pvs()
    for pv_mock, pv_db in zip(pvs, db_pvs):
        assert pv_mock['pvname'] == pv_db['pvname']
        assert pv_mock['msg'] == pv_db['msg']


def test_insert_pvs(dh):
    pvs = [
             {'pvname': 'A', 'ring': 'HER'},
             {'pvname': 'B', 'ring': 'HER'},
             {'pvname': 'H', 'ring': 'HER'},
             {'pvname': 'I', 'ring': 'LER'}
            ]
    dh.insert_pvs(pvs)

    db_pvs = dh.fetch_all_pvs()
    for pv_mock in pvs:
        for pv_db in db_pvs:
            name_bool = (pv_mock['pvname'] == pv_db['pvname'])
            ring_bool = (pv_mock['ring'] == pv_db['ring'])
            if name_bool and ring_bool:
                break
        else:
            assert False, '{} == {}'.format(pv_mock, pv_db)


def test_insert_abort_signals(dh):
    signals = [
                 {'pvname': 'A',
                     'msg': 'Abort A',
                     'pv_ts': '2019-01-01 00:00:00.100000000',
                     'abt_ts': '2019-01-01 00:00:00.000000000',
                     'reset_cnt': 0,
                     'trg_cnt': 1,
                     'int_cnt': 2},
                 {'pvname': 'B',
                     'msg': 'Abort B',
                     'pv_ts': '2019-01-01 00:00:01.100000000',
                     'abt_ts': '2019-01-01 00:00:01.000000000',
                     'reset_cnt': 1,
                     'trg_cnt': 2,
                     'int_cnt': 3},

                 {'pvname': 'C',
                     'msg': 'Abort C',
                     'pv_ts': '2019-01-01 00:00:02.100000000',
                     'abt_ts': '2019-01-01 00:00:02.000000000',
                     'reset_cnt': 2,
                     'trg_cnt': 3,
                     'int_cnt': 5},
                 ]

    ids = dh.insert_abort_signals(signals)

    db_signals = dh.fetch_abort_signals(sstart='2019', include_no_abt_id=True)
    for s1, s2 in zip(db_signals, signals):
        assert s1['abt_id'] is None
        assert s1['ts'] == s2['abt_ts']
        assert s1['pvname'] == s2['pvname']
        assert s1['msg'] == s2['msg']
        assert s1['reset_cnt'] == s2['reset_cnt']
        assert s1['trg_cnt'] == s2['trg_cnt']
        assert s1['int_cnt'] == s2['int_cnt']

    with pytest.raises(sa.exc.IntegrityError):
        ids = dh.insert_abort_signals(signals, abt_id=7)


def test_insert_abort(dh):
    abort_time = '2019-01-01 00:00:00.000000000'

    signals = [
                 {'pvname': 'A',
                     'msg': 'Abort A',
                     'pv_ts': '2019-01-01 00:00:00.100000000',
                     'abt_ts': '2019-01-01 00:00:00.000000000',
                     'reset_cnt': 0,
                     'trg_cnt': 1,
                     'int_cnt': 2},
                 {'pvname': 'B',
                     'msg': 'Abort B',
                     'pv_ts': '2019-01-01 00:00:01.100000000',
                     'abt_ts': '2019-01-01 00:00:01.000000000',
                     'reset_cnt': 1,
                     'trg_cnt': 2,
                     'int_cnt': 3}
                 ]

    abt_id = dh.insert_abort(abort_time)

    assert abt_id == 7

    ids = dh.insert_abort_signals(signals, abt_id=7)

    db_signals = dh.fetch_abort_signals(sstart='2019', include_no_abt_id=True)
    assert len(db_signals) == 2


def test_update_abort(dh):
    abort_time = '2019-01-01 00:00:00.000000000'

    dh.update_abort(abt_id=1, timestamp=abort_time)

    aborts = dh.fetch_aborts()
    for abort in aborts:
        if abort['abt_id'] == 1 and abort['abt_time'] == abort_time:
            break
    else:
        assert False, 'Failed to update abort'
