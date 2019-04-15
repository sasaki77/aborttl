import time

import pytest
from epics import caput, caget

from aborttl.resetpvcounter import ResetPVCounter


def test_resetpv(softioc, caclient):
    pvname = 'ET_dummyHost:RESETw'
    rc = ResetPVCounter(pvname)

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
