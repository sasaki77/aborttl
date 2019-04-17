from threading import Lock

from epics import PV

from .logger import get_default_logger


class ResetPVCounter(object):

    def __init__(self, pvname, logger=None):
        self.logger = logger or get_default_logger()

        self._pv = PV(pvname=str(pvname), auto_monitor=True,
                      callback=self._on_value_change,
                      connection_callback=self._on_connection)

        self._lock = Lock()
        self._count = 0

    def _on_value_change(self, pvname=None, value=None, **kw):
        if value < 0 or value > 1:
            self.logger.debug('Reset Pv Value Error')
            return

        with self._lock:
            self._count += value
            self.logger.debug('Reset PV count up to {}'.format(self._count))

    def _on_connection(self, pvname=None, conn=None, **kw):
        self.logger.debug('{} connection change: {}'.format(pvname, conn))

    def clear(self):
        with self._lock:
            self._count = 0

    @property
    def count(self):
        with self._lock:
            return self._count
