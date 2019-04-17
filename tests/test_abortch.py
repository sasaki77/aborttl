import time
from datetime import datetime

import pytest
from epics import PV

from aborttl.abortch import AbortCh


PVNAME = 'ET_dummyHost:ABORTCH1'


@pytest.fixture(scope='module')
def fieldset():
    AbortCh.fields['ACNT'] = ':ACNT'
    AbortCh.fields['TCNT'] = ':TCNT'


@pytest.fixture(scope='module')
def abt_ch(fieldset, caclient):
    ch = AbortCh(PVNAME, cb)
    ch._abortpv.wait_for_connection()
    ch._acntpv.wait_for_connection()
    ch._tcntpv.wait_for_connection()
    ch._secpv.wait_for_connection()
    ch._nsecpv.wait_for_connection()
    time.sleep(5)
    return ch


def cb(pvname=None, value=None):
    pass


def test_abortch_abort_property(softioc, caclient, fieldset, abt_ch):
    pv = PV(PVNAME)
    pv.put(0, timeout=1)
    time.sleep(0.1)

    pv.put(1, timeout=1)
    time.sleep(0.3)

    assert abt_ch.abort == 1

    pv.put(0, timeout=1)
    time.sleep(0.1)


def test_abortch_cnt_property(softioc, caclient, fieldset, abt_ch):
    pv_acnt = PV(PVNAME + ':ACNT')
    pv_acnt.put(2, timeout=1)
    time.sleep(0.1)
    assert abt_ch.acnt == 2

    pv_tcnt = PV(PVNAME + ':TCNT')
    pv_tcnt.put(3, timeout=1)
    time.sleep(0.1)
    assert abt_ch.tcnt == 3


def test_abortch_sec_property(softioc, caclient, fieldset, abt_ch):
    pv = PV(PVNAME)
    pv_sec = PV(PVNAME + ':TIME_SEC')
    pv_nsec = PV(PVNAME + ':TIME_NANO')

    pv.put(1, timeout=1)
    pv_sec.put(0, timeout=1)
    pv_nsec.put(0, timeout=1)
    time.sleep(0.1)
    assert abt_ch.get_timestamp() == '1970-01-01 09:00:00.000000000'

    pv_sec.put(1, timeout=1)
    pv_nsec.put(2, timeout=1)
    time.sleep(0.1)
    assert abt_ch.ts_sec == 1
    assert abt_ch.ts_nsec == 2

    pv.put(0, timeout=1)
    time.sleep(0.1)


def test_abortch_ts_property(softioc, caclient, fieldset, abt_ch):
    pt = datetime.now()
    pv = PV(PVNAME)
    pv.put(1, timeout=1)
    time.sleep(0.1)
    at = datetime.now()

    ts = datetime.strptime(abt_ch.ts[:-3], '%Y-%m-%d %H:%M:%S.%f')
    assert pt < ts < at

    pv.put(0, timeout=1)
    time.sleep(0.1)


def test_abortch_timestamp(softioc, caclient, fieldset, abt_ch):
    pv = PV(PVNAME)
    pv_sec = PV(PVNAME + ':TIME_SEC')
    pv_nsec = PV(PVNAME + ':TIME_NANO')

    pv.put(0, timeout=1)
    time.sleep(0.1)

    t = time.time()
    t_sec, t_nano = ('%.9f' % t).split('.')
    pv.put(1, timeout=1)
    pv_sec.put(int(t_sec), timeout=1)
    pv_nsec.put(int(t_nano), timeout=1)
    time.sleep(0.1)

    dt = datetime.fromtimestamp(int(t_sec))
    d = dt.isoformat(' ')
    tstr = d + '.' + str(int(t_nano)).zfill(9)
    assert tstr == abt_ch.get_timestamp()

    pv.put(0, timeout=1)
    time.sleep(0.1)


def test_abortch_timestamp_after_4sec(softioc, caclient, fieldset, abt_ch):
    pv = PV(PVNAME)
    pv_sec = PV(PVNAME + ':TIME_SEC')
    pv_nsec = PV(PVNAME + ':TIME_NANO')

    pv.put(0, timeout=1)
    time.sleep(0.1)

    t = time.time()
    t_sec, t_nano = ('%.9f' % t).split('.')
    pv_sec.put(int(t_sec), timeout=1)
    pv_nsec.put(int(t_nano), timeout=1)
    time.sleep(4)
    pv.put(1, timeout=1)

    dt = datetime.fromtimestamp(int(t_sec))
    d = dt.isoformat(' ')
    tstr = d + '.' + str(int(t_nano)).zfill(9)
    assert tstr == abt_ch.get_timestamp()

    pv.put(0, timeout=1)
    time.sleep(0.1)


def test_abortch_timestamp_after_6sec(softioc, caclient, fieldset, abt_ch):
    pv = PV(PVNAME)
    pv_sec = PV(PVNAME + ':TIME_SEC')
    pv_nsec = PV(PVNAME + ':TIME_NANO')

    pv.put(0, timeout=1)
    time.sleep(0.1)

    t = time.time()
    t_sec, t_nano = ('%.9f' % t).split('.')
    pv_sec.put(int(t_sec), timeout=1)
    pv_nsec.put(int(t_nano), timeout=1)
    time.sleep(6)
    pv.put(1, timeout=1)
    time.sleep(0.1)

    dt = datetime.fromtimestamp(int(t_sec))
    d = dt.isoformat(' ')
    tstr = d + '.' + str(int(t_nano)).zfill(9)
    act = abt_ch.get_timestamp()
    err_msg = 'abt_ch:{} dt:{}'.format(act, tstr)
    assert act == '1970-01-01 09:00:00.000000000', err_msg

    pv.put(0, timeout=1)
    time.sleep(0.1)
