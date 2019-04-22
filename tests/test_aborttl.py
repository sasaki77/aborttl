import threading
import time
from datetime import datetime
from collections import namedtuple

import pytest
from epics import PV
from epics.ca import CAThread

from aborttl.dbhandler import DbHandler
from aborttl.abortch import AbortCh
from aborttl.aborttl import Aborttl


Signal = namedtuple('Signal', ['abt_id', 'ts', 'pvname', 'msg',
                               'ring', 'reset', 'tcnt', 'acnt'
                               ]
                    )


def insert_current_pv_mock(uri):
    dh = DbHandler(uri)
    conn = dh.engine.connect()

    mock_pvs = [
                   {'pvname': 'ET_dummyHost:ABORTCH1', 'ring': 'HER'},
                   {'pvname': 'ET_dummyHost:ABORTCH2', 'ring': 'HER'},
                   {'pvname': 'ET_dummyHost:ABORTCH3', 'ring': 'HER'},
                   {'pvname': 'ET_dummyHost:ABORTCH4', 'ring': 'LER'},
                   {'pvname': 'ET_dummyHost:ABORTCH5', 'ring': 'LER'}
                ]

    mock_cpvs = [
                    {'pvname': 'ET_dummyHost:ABORTCH1', 'msg': 'msg 1'},
                    {'pvname': 'ET_dummyHost:ABORTCH2', 'msg': 'msg 2'},
                    {'pvname': 'ET_dummyHost:ABORTCH3', 'msg': 'msg 3'},
                    {'pvname': 'ET_dummyHost:ABORTCH4', 'msg': 'msg 4'},
                    {'pvname': 'ET_dummyHost:ABORTCH5', 'msg': 'msg 5'}
                 ]

    conn.execute(dh.tables['pvs'].insert(), mock_pvs)
    conn.execute(dh.tables['current_pvs'].insert(), mock_cpvs)

    conn.close()

    return dh


def clear_ch(ch_num):
    name = "ET_dummyHost:ABORTCH" + str(ch_num)

    pv_abort = PV(name)
    pv_acnt = PV(name + ':ACNT')
    pv_tcnt = PV(name + ':TCNT')
    pv_sec = PV(name + ':TIME_SEC')
    pv_nsec = PV(name + ':TIME_NANO')

    pv_abort.put(0,  wait=True)
    pv_acnt.put(0,  wait=True)
    pv_tcnt.put(0,  wait=True)
    pv_sec.put(0,  wait=True)
    pv_nsec.put(0,  wait=True)


def put_abort_ch(ch_num, acnt, tcnt, _t=None):
    t = time.time() if _t is None else _t
    name = "ET_dummyHost:ABORTCH" + str(ch_num)
    t_sec, t_nano = ("%.9f" % t).split(".")

    pv_abort = PV(name)
    pv_acnt = PV(name + ':ACNT')
    pv_tcnt = PV(name + ':TCNT')
    pv_sec = PV(name + ':TIME_SEC')
    pv_nsec = PV(name + ':TIME_NANO')

    pv_abort.put(1,  wait=True)
    pv_acnt.put(acnt,  wait=True)
    pv_tcnt.put(tcnt,  wait=True)
    pv_sec.put(int(t_sec),  wait=True)
    pv_nsec.put(int(t_nano),  wait=True)

    dt = datetime.fromtimestamp(int(t_sec))
    return '{}.{}'.format(dt.isoformat(' '), t_nano)


def abort_reset():
    pv = PV('ET_dummyHost:RESETw')
    pv.put(1,  wait=True)
    time.sleep(1.1)


def check_signals(ss_test, ss_db):
    assert len(ss_test) == len(ss_db)
    for s_test, s_db in zip(ss_test, ss_db):
        err_msg = '{} = {}'.format(s_db, s_test)
        assert s_db['abt_id'] == s_test.abt_id, err_msg
        assert s_db['ts'] == s_test.ts, err_msg
        assert s_db['pvname'] == s_test.pvname, err_msg
        assert s_db['msg'] == s_test.msg, err_msg
        assert s_db['ring'] == s_test.ring, err_msg
        assert s_db['reset_cnt'] == s_test.reset, err_msg
        assert s_db['trg_cnt'] == s_test.tcnt, err_msg
        assert s_db['int_cnt'] == s_test.acnt, err_msg


def set_initial_abort_state():
    put_abort_ch(3, 0, 0)
    put_abort_ch(5, 0, 0)


@pytest.fixture(scope='module')
def atl(softioc, caclient, tmpdir_factory):
    AbortCh.fields['ACNT'] = ':ACNT'
    AbortCh.fields['TCNT'] = ':TCNT'
    dburi = ('sqlite:///' +
             str(tmpdir_factory.mktemp('data').join('testdata.db'))
             )
    insert_current_pv_mock(dburi)
    set_initial_abort_state()
    atl = Aborttl(dburi, 'ET_dummyHost:RESETw')

    thread = CAThread(target=atl.run)
    thread.daemon = True
    thread.start()
    time.sleep(5)
    yield atl
    atl.stop()
    thread.join()
    time.sleep(1)


def test_initial_abort(softioc, caclient, atl):
    t1 = put_abort_ch(1, 0, 0)
    t2 = put_abort_ch(2, 1, 0)

    time.sleep(2)
    abort_reset()
    clear_ch(1)
    time.sleep(6)

    put_abort_ch(1, 0, 1)
    t4 = put_abort_ch(4, 2, 1)
    put_abort_ch(1, 1, 1)

    time.sleep(2)

    signals = atl._dh.fetch_abort_signals(include_no_abt_id=True)

    ss = [Signal(None, t1, 'ET_dummyHost:ABORTCH1', 'msg 1', 'HER', 0, 0, 0),
          Signal(None, t2, 'ET_dummyHost:ABORTCH2', 'msg 2', 'HER', 0, 0, 1),
          Signal(None, t4, 'ET_dummyHost:ABORTCH4', 'msg 4', 'LER', 1, 1, 2)]

    check_signals(ss, signals)

    for i in range(5):
        clear_ch(i+1)
    time.sleep(1)


def test_single_ring_abort(softioc, caclient, atl):
    t1 = put_abort_ch(1, 0, 0)
    t2 = put_abort_ch(2, 1, 0)

    time.sleep(2)
    abort_reset()
    clear_ch(1)
    time.sleep(6)

    put_abort_ch(1, 0, 1)
    t3 = put_abort_ch(3, 1, 1)
    t4 = put_abort_ch(4, 2, 1)
    put_abort_ch(1, 1, 1)
    t5 = put_abort_ch(5, 1, 2)

    time.sleep(2)

    signals = atl._dh.fetch_abort_signals()

    ss = [Signal(1, t1, 'ET_dummyHost:ABORTCH1', 'msg 1', 'HER', 0, 0, 0),
          Signal(1, t2, 'ET_dummyHost:ABORTCH2', 'msg 2', 'HER', 0, 0, 1),
          Signal(1, t3, 'ET_dummyHost:ABORTCH3', 'msg 3', 'HER', 1, 1, 1),
          Signal(2, t4, 'ET_dummyHost:ABORTCH4', 'msg 4', 'LER', 0, 1, 2),
          Signal(2, t5, 'ET_dummyHost:ABORTCH5', 'msg 5', 'LER', 0, 2, 1)]

    check_signals(ss, signals)

    for i in range(5):
        clear_ch(i+1)
    time.sleep(1)


def test_both_ring_abort(softioc, caclient, atl):
    init_time = time.time()
    init_dt = datetime.fromtimestamp(init_time)

    time.sleep(1)

    t1 = put_abort_ch(1, 0, 0)
    t2 = put_abort_ch(2, 1, 0)

    time.sleep(1)
    abort_reset()
    clear_ch(1)
    time.sleep(1)

    put_abort_ch(1, 0, 1)
    t3 = put_abort_ch(3, 1, 1)
    t4 = put_abort_ch(4, 2, 1)
    put_abort_ch(1, 1, 1)
    t5 = put_abort_ch(5, 1, 2)

    time.sleep(2)

    signals = atl._dh.fetch_abort_signals(astart=init_dt.isoformat(' '))

    ss = [Signal(3, t1, 'ET_dummyHost:ABORTCH1', 'msg 1', 'HER', 0, 0, 0),
          Signal(3, t2, 'ET_dummyHost:ABORTCH2', 'msg 2', 'HER', 0, 0, 1),
          Signal(3, t3, 'ET_dummyHost:ABORTCH3', 'msg 3', 'HER', 1, 1, 1),
          Signal(3, t4, 'ET_dummyHost:ABORTCH4', 'msg 4', 'LER', 1, 1, 2),
          Signal(3, t5, 'ET_dummyHost:ABORTCH5', 'msg 5', 'LER', 1, 2, 1)]

    check_signals(ss, signals)

    for i in range(5):
        clear_ch(i+1)
    time.sleep(1)


def test_new_faster_abort(softioc, caclient, atl):
    init_time = time.time()
    init_dt = datetime.fromtimestamp(init_time)

    time.sleep(1)
    t1_time = time.time()
    time.sleep(1)
    mid_time = time.time()
    mid_dt = datetime.fromtimestamp(mid_time)
    time.sleep(1)

    t2 = put_abort_ch(2, 0, 1)
    time.sleep(1)
    t1 = put_abort_ch(1, 0, 0, t1_time)
    time.sleep(1)
    t3 = put_abort_ch(3, 1, 1)

    time.sleep(2)

    signals = atl._dh.fetch_abort_signals(astart=mid_dt.isoformat(' '))
    assert signals == []

    signals = atl._dh.fetch_abort_signals(astart=init_dt.isoformat(' '))

    ss = [Signal(4, t1, 'ET_dummyHost:ABORTCH1', 'msg 1', 'HER', 0, 0, 0),
          Signal(4, t2, 'ET_dummyHost:ABORTCH2', 'msg 2', 'HER', 0, 1, 0),
          Signal(4, t3, 'ET_dummyHost:ABORTCH3', 'msg 3', 'HER', 0, 1, 1)]

    check_signals(ss, signals)

    for i in range(5):
        clear_ch(i+1)
    time.sleep(1)


def test_timestamp_update_later(softioc, caclient, atl):
    init_time = time.time()
    init_dt = datetime.fromtimestamp(init_time)

    put_abort_ch(1, 0, 0, 0)
    time.sleep(1)
    clear_ch(1)
    t1 = put_abort_ch(1, 0, 0)

    time.sleep(2)

    signals = atl._dh.fetch_abort_signals(astart=init_dt.isoformat(' '))

    ss = [Signal(5, t1, 'ET_dummyHost:ABORTCH1', 'msg 1', 'HER', 0, 0, 0)]

    check_signals(ss, signals)

    for i in range(5):
        clear_ch(i+1)
    time.sleep(3)


def test_timestamp_update_later(softioc, caclient, atl):
    init_time = time.time()
    init_dt = datetime.fromtimestamp(init_time)

    put_abort_ch(1, 0, 0, 0)
    time.sleep(1)
    clear_ch(1)
    t1 = put_abort_ch(1, 0, 0)

    time.sleep(2)

    signals = atl._dh.fetch_abort_signals(astart=init_dt.isoformat(' '))

    ss = [Signal(5, t1, 'ET_dummyHost:ABORTCH1', 'msg 1', 'HER', 0, 0, 0)]

    check_signals(ss, signals)

    for i in range(5):
        clear_ch(i+1)
    time.sleep(1)
