import threading
import time
from queue import Queue

from .logger import get_default_logger
from .dbhandler import DbHandler
from .abortch import AbortCh
from .resetpvcounter import ResetPVCounter


class AbortInfo(object):
    def __init__(self):
        self.id = None
        self.sec = None
        self.ts = None
        self.reset_offset = 0
        self.iniail_abort = True

    def clear(self):
        self.id = None
        self.sec = None
        self.ts = None
        self.reset_offset = 0
        self.iniail_abort = False


class Aborttl(object):
    '''
    _pvs = {'pvname': {'msg': , 'abortch':, 'ring': }}
    _abt_statuses: dict of pvname. False = ready, True = abort
    '''

    def __init__(self, dburi, resetpvname, logger=None):
        self._logger = logger or get_default_logger()
        self._resetpvname = resetpvname

        self._dh = DbHandler(dburi)
        self._pvs = {}

        self._abt_q = Queue()
        self._aborts = {'LER': {}, 'HER': {}}
        self._abtinfo = {'LER': AbortInfo(),
                         'HER': AbortInfo()}

        self.__is_stop = threading.Event()
        self.__stop_request = False
        self._is_running = False

        self._interval = 0.1
        self._both_ring_interval = 5

        self._init_pv()

    def _init_pv(self):
        self._resetpv = ResetPVCounter(self._resetpvname, logger=self._logger)

        self._update_pvlist()

        for pvname, item in self._pvs.items():
            item['abortch'] = AbortCh(pvname, self._cb, logger=self._logger)

    def _cb(self, pvname=None, value=None):
        self._logger.debug('Put {}, {} to queue'.format(pvname, bool(value)))
        self._abt_q.put((pvname, bool(value)))

    def _initial_abort_check(self):
        is_abort = {'LER': 0, 'HER': 0}

        for pv in self._pvs:
            pvname = pv['pvname']
            ring = pv['ring']
            abort = pv['abortch'].abort

            self._aborts[pvname] = abort
            is_abort[ring] += abort

        self._abtinfo['LER'].iniail_abort = bool(is_abort['LER'])
        self._abtinfo['HER'].iniail_abort = bool(is_abort['HER'])

    def _update_pvlist(self):
        pvs = self._dh.fetch_current_pvs()
        self._pvs = {pv['pvname']: {'ring': pv['ring'], 'msg': pv['msg']}
                     for pv in pvs}

    def _run_loop(self):
        qsize = self._abt_q.qsize()
        if qsize:
            self._logger.debug('No. of update ch = {}'.format(qsize))

        for i in range(qsize):
            pvname, abort = self._abt_q.get_nowait()
            ring = self._pvs[pvname]['ring']
            abtinfo = self._abtinfo[ring]

            self._logger.debug('Update {} abort = {}, ring = {}'
                               .format(pvname, abort, ring))

            # update abort status
            self._aborts[ring][pvname] = abort

            if not abort:
                continue

            ch = self._pvs[pvname]['abortch']
            timestamp = ch.get_timestamp()

            # timestamp is not updated yet
            if timestamp == "1970-01-01 09:00:00.000000000":
                self._abt_q.put((pvname, abort))
                self._logger.debug('{} timestamp is not updated'
                                   .format(pvname))
                continue

            # new abort is comming
            if abtinfo.iniail_abort:
                pass
            elif abtinfo.id is None:

                # Check the abort time can be treated as identical abort
                rring = 'HER' if ring == 'LER' else 'LER'
                rabtinfo = self._abtinfo[rring]
                if (
                      rabtinfo.id is not None and
                      abs(ch.ts_sec - rabtinfo.sec) < 5
                   ):
                    # Both ring abort
                    self._logger.debug('Both ring abort')

                    abtinfo.id = rabtinfo.id
                    abtinfo.sec = rabtinfo.sec
                    abtinfo.ts = rabtinfo.ts
                    abtinfo.reset_offset = rabtinfo.reset_offset
                else:
                    # Single ring abort
                    self._logger.debug('Single ring abort')

                    abtinfo.id = self._dh.insert_abort(timestamp)
                    abtinfo.sec = ch.ts_sec
                    abtinfo.ts = timestamp
                    abtinfo.reset_offset = self._resetpv.count

            # new faster abort is comming
            elif timestamp < abtinfo.ts:
                self._logger.debug('New faster abort is comming')
                self._dh.update_abort(abtinfo.id, timestamp)
                abtinfo.sec = ch.ts_sec
                abtinfo.ts = timestamp

            # insert abort signal
            msg = self._pvs[pvname]['msg']
            ts = ch.ts
            abtid = abtinfo.id

            signal = {'pvname': pvname, 'msg': msg, 'pv_ts': ts,
                      'abt_ts': timestamp,
                      'reset_cnt': self._resetpv.count - abtinfo.reset_offset,
                      'trg_cnt': ch.tcnt, 'int_cnt': ch.acnt}

            self._logger.debug('Insert abot signal: {}'.format(pvname))
            self._dh.insert_abort_signals([signal], abtid)

        # check abort status for each ring
        if qsize:
            self._update_ring_status()

    def _update_ring_status(self):
        all_aborts = 0
        rings = ['LER', 'HER']

        for ring in rings:
            aborts = self._aborts[ring]
            is_abort = sum(aborts.values())
            all_aborts += is_abort

            if not is_abort:
                self._abtinfo[ring].clear()
                self._logger.debug('Clear {} Abort Info'.format(ring))

        if not all_aborts:
            self._resetpv.clear()
            self._logger.debug('Clear Reset PV Counter')

    def run(self):
        self._is_running = True
        self.__is_stop.clear()

        try:
            self._initial_abort_check()
            while not self.__stop_request:
                self._run_loop()
                time.sleep(self._interval)
        finally:
            self._logger.info('Aborttl stopped.')
            self.__stop_request = False
            self.__is_stop.set()

        self._is_running = False

    def stop(self):
        self.__stop_request = True
        self.__is_stop.wait()
