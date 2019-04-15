import time

import pytest
from epics import caput, caget

from aborttl.resetpvcounter import ResetPVCounter


@pytest.fixture(scope='module')
def rc():
    pvname = 'ET_dummyHost:RESETw'
    return ResetPVCounter(pvname)


def test_resetpv(softioc, caclient, rc):
    pvname = 'ET_dummyHost:RESETw'

    for i in range(5):
        caput(pvname, 1, timeout=1)
        time.sleep(0.5)

    assert rc.count == 5
    rc.clear()
    assert rc.count == 0

    for i in range(3):
        caput(pvname, 1, timeout=1)
        time.sleep(0.5)

    caput(pvname, 2, timeout=1)
    time.sleep(0.5)

    assert rc.count == 3
