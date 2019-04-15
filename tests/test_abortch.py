import time
from datetime import datetime

import pytest
from epics import caput, caget

from aborttl.abortch import AbortCh


PVNAME = 'ET_dummyHost:ABORTCH1'


@pytest.fixture(scope='module')
def fieldset():
    AbortCh.fields['ACNT'] = ':ACNT'
    AbortCh.fields['TCNT'] = ':TCNT'


@pytest.fixture(scope='module')
def abt_ch():
    return AbortCh(PVNAME, cb)


def cb(pvname=None, value=None):
    pass


def test_abortch_abort_property(softioc, caclient, fieldset, abt_ch):
    caput(PVNAME, 0, timeout=1)
    time.sleep(0.1)

    caput(PVNAME, 1, timeout=1)
    time.sleep(0.3)

    assert abt_ch.abort == 1

    caput(PVNAME, 0, timeout=1)
    time.sleep(0.1)


def test_abortch_cnt_property(softioc, caclient, fieldset, abt_ch):
    caput(PVNAME+':ACNT', 2, timeout=1)
    time.sleep(0.1)
    assert abt_ch.acnt == 2

    caput(PVNAME+':TCNT', 3, timeout=1)
    time.sleep(0.1)
    assert abt_ch.tcnt == 3


def test_abortch_sec_property(softioc, caclient, fieldset, abt_ch):
    caput(PVNAME, 1, timeout=1)
    caput(PVNAME+':TIME_SEC', 0, timeout=1)
    caput(PVNAME+':TIME_NANO', 0, timeout=1)
    time.sleep(0.1)
    assert abt_ch.get_timestamp() == '1970-01-01 09:00:00.000000000'

    caput(PVNAME+':TIME_SEC', 1, timeout=1)
    caput(PVNAME+':TIME_NANO', 2, timeout=1)
    time.sleep(0.1)
    assert abt_ch.ts_sec == 1
    assert abt_ch.ts_nsec == 2

    caput(PVNAME, 0, timeout=1)
    time.sleep(0.1)


def test_abortch_ts_property(softioc, caclient, fieldset, abt_ch):
    pt = datetime.now()
    caput(PVNAME, 1, timeout=1)
    time.sleep(0.1)
    at = datetime.now()

    ts = datetime.strptime(abt_ch.ts[:-3], '%Y-%m-%d %H:%M:%S.%f')
    assert pt < ts < at

    caput(PVNAME, 0, timeout=1)
    time.sleep(0.1)


def test_abortch_timestamp(softioc, caclient, fieldset, abt_ch):
    caput(PVNAME, 0, timeout=1)
    time.sleep(0.1)

    t = time.time()
    t_sec, t_nano = ('%.9f' % t).split('.')
    caput(PVNAME, 1, timeout=1)
    caput(PVNAME+':TIME_SEC', int(t_sec), timeout=1)
    caput(PVNAME+':TIME_NANO', int(t_nano), timeout=1)
    time.sleep(0.1)

    dt = datetime.fromtimestamp(int(t_sec))
    d = dt.isoformat(' ')
    tstr = d + '.' + str(int(t_nano)).zfill(9)
    assert tstr == abt_ch.get_timestamp()

    caput(PVNAME, 0, timeout=1)
    time.sleep(0.1)


def test_abortch_timestamp_after_4sec(softioc, caclient, fieldset, abt_ch):
    caput(PVNAME, 0, timeout=1)
    time.sleep(0.1)

    t = time.time()
    t_sec, t_nano = ('%.9f' % t).split('.')
    caput(PVNAME+':TIME_SEC', int(t_sec), timeout=1)
    caput(PVNAME+':TIME_NANO', int(t_nano), timeout=1)
    time.sleep(4)
    caput(PVNAME, 1, timeout=1)

    dt = datetime.fromtimestamp(int(t_sec))
    d = dt.isoformat(' ')
    tstr = d + '.' + str(int(t_nano)).zfill(9)
    assert tstr == abt_ch.get_timestamp()

    caput(PVNAME, 0, timeout=1)
    time.sleep(0.1)


def test_abortch_timestamp_after_6sec(softioc, caclient, fieldset, abt_ch):
    caput(PVNAME, 0, timeout=1)
    time.sleep(0.1)

    t = time.time()
    t_sec, t_nano = ('%.9f' % t).split('.')
    caput(PVNAME+':TIME_SEC', int(t_sec), timeout=1)
    caput(PVNAME+':TIME_NANO', int(t_nano), timeout=1)
    time.sleep(6)
    caput(PVNAME, 1, timeout=1)
    time.sleep(0.1)

    dt = datetime.fromtimestamp(int(t_sec))
    d = dt.isoformat(' ')
    tstr = d + '.' + str(int(t_nano)).zfill(9)
    act = abt_ch.get_timestamp()
    err_msg = 'abt_ch:{} dt:{}'.format(act, tstr)
    assert act == '1970-01-01 09:00:00.000000000', err_msg

    caput(PVNAME, 0, timeout=1)
    time.sleep(0.1)
