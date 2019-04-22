from datetime import datetime

from epics import PV

from .logger import get_default_logger


class AbortCh(object):
    fields = {'ACNT': '.ACNT', 'TCNT': '.TCNT',
              'SEC': ':TIME_SEC', 'NSEC': ':TIME_NANO'}

    def __init__(self, pvname, cb, logger=None):
        self.logger = logger or get_default_logger()

        self._connection_update = True
        self._cb = cb

        self._abortpv = PV(pvname=str(pvname), auto_monitor=True,
                           callback=self._abort_update,
                           connection_callback=self._on_connection)

        self._acntpv = PV(pvname=str(pvname + self.fields['ACNT']),
                          connection_callback=self._on_connection)

        self._tcntpv = PV(pvname=str(pvname + self.fields['TCNT']),
                          connection_callback=self._on_connection)

        self._secpv = PV(pvname=str(pvname + self.fields['SEC']),
                         connection_callback=self._on_connection)

        self._nsecpv = PV(pvname=str(pvname + self.fields['NSEC']),
                          connection_callback=self._on_connection)

    def _abort_update(self, pvname=None, value=None, **kw):
        if self._connection_update:
            self._connection_update = False
            self.logger.debug('{}: Clear connection update'.format(pvname))
            return

        self._cb(pvname, value)

    def _on_connection(self, pvname=None, conn=None, **kw):
        if not conn:
            self._connection_update = True
        self.logger.debug('{} connection change: {}'.format(pvname, conn))

    def get_timestamp(self):
        is_ts_valid = (self._secpv.timestamp > (self._abortpv.timestamp-5) and
                       self._nsecpv.timestamp > (self._abortpv.timestamp-5)
                       )

        if not is_ts_valid:
            return '1970-01-01 09:00:00.000000000'

        dt = datetime.fromtimestamp(self._secpv.value)
        d = dt.isoformat(' ')

        return d + '.' + str(int(self._nsecpv.value)).zfill(9)

    @property
    def ts(self):
        dt = datetime.fromtimestamp(self._abortpv.posixseconds)
        d = dt.isoformat(' ')
        return d + '.' + str(int(self._abortpv.nanoseconds)).zfill(9)

    @property
    def abort(self):
        return self._abortpv.value

    @property
    def acnt(self):
        return self._acntpv.value

    @property
    def tcnt(self):
        return self._tcntpv.value

    @property
    def ts_sec(self):
        return self._secpv.value

    @property
    def ts_nsec(self):
        return self._nsecpv.value
