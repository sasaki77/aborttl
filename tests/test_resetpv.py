import time

import pytest
from epics import PV

from aborttl.resetpvcounter import ResetPVCounter


@pytest.fixture(scope='module')
def rc():
    pvname = 'ET_dummyHost:RESETw'
    ch = ResetPVCounter(pvname)
    ch._pv.wait_for_connection()
    time.sleep(5)
    return ch


def test_resetpv(softioc, caclient, rc):
    pv = PV('ET_dummyHost:RESETw')

    for i in range(5):
        pv.put(1, timeout=1)
        time.sleep(0.5)

    assert rc.count == 5
    rc.clear()
    assert rc.count == 0

    for i in range(3):
        pv.put(1, timeout=1)
        time.sleep(0.5)

    pv.put(2, timeout=1)
    time.sleep(0.5)

    assert rc.count == 3
