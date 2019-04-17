import os
import time

import pytest
from epics import ca

from ioccontrol import IocControl


@pytest.fixture(scope='module')
def softioc():
    dir_path = os.path.dirname(__file__)
    db_file = os.path.join(dir_path, 'test.db')

    ioc_arg_list = ['-m', 'head=ET_dummyHost', '-d', db_file]
    iocprocess = IocControl(arg_list=ioc_arg_list)
    iocprocess.start()

    yield iocprocess

    iocprocess.stop()


@pytest.fixture(scope='session', autouse=True)
def caclient():
    ca.finalize_libca()
    time.sleep(1)

    sport = str(IocControl.server_port)
    os.environ['EPICS_CA_AUTO_ADDR_LIST'] = 'NO'
    os.environ['EPICS_CA_ADDR_LIST'] = 'localhost:{}'.format(sport)

    ca.initialize_libca()
    time.sleep(1)
    yield
    ca.finalize_libca()
    time.sleep(1)


@pytest.fixture
def mock_data():
    md = {}
    md['pvs'] = [
                  {'pvname': 'A', 'ring': 'HER'},
                  {'pvname': 'B', 'ring': 'HER'},
                  {'pvname': 'C', 'ring': 'HER'},
                  {'pvname': 'D', 'ring': 'LER'},
                  {'pvname': 'E', 'ring': 'LER'},
                  {'pvname': 'F', 'ring': 'LER'},
                  {'pvname': 'G', 'ring': 'LER'}
                 ]

    md['current_pvs'] = [
                          {'pvname': 'B', 'msg': 'Abort B'},
                          {'pvname': 'C', 'msg': 'Abort C'},
                          {'pvname': 'D', 'msg': 'Abort D'},
                          {'pvname': 'E', 'msg': 'Abort E'},
                          {'pvname': 'F', 'msg': 'Abort F'}
                         ]

    md['aborts'] = [
                      {'abt_time': '2018-01-01 00:00:00.123456789'},
                      {'abt_time': '2018-01-01 00:10:00.123456790'},
                      {'abt_time': '2018-01-02 00:00:00.123456791'},
                      {'abt_time': '2018-01-03 00:00:00.123456792'},
                      {'abt_time': '2018-01-05 00:00:00.123456793'},
                      {'abt_time': '2018-01-06 00:00:00.123456793'}
                     ]

    md['abort_signals'] = [
                            {'pvname': 'B',
                                'msg': 'Abort B',
                                'pv_ts': '2018-01-01 00:00:00.523456789',
                                'abt_ts': '2018-01-01 00:00:00.123456789',
                                'reset_cnt': 0,
                                'trg_cnt': 4,
                                'int_cnt': 1000000},
                            {'pvname': 'C',
                                'msg': 'Abort C',
                                'pv_ts': '2018-01-01 00:00:00.623456789',
                                'abt_ts': '2018-01-01 00:00:00.223456789',
                                'reset_cnt': 0,
                                'trg_cnt': 4,
                                'int_cnt': 2000000},

                            {'pvname': 'A',
                                'msg': 'Abort A',
                                'pv_ts': '2018-01-01 00:00:30.723456789',
                                'abt_ts': '2018-01-01 00:00:30.323456789',
                                'reset_cnt': 1,
                                'trg_cnt': 34,
                                'int_cnt': 3000000},

                            {'pvname': 'B',
                                'msg': 'Abort B',
                                'pv_ts': '2018-01-01 00:01:00.823456789',
                                'abt_ts': '2018-01-01 00:01:00.423456789',
                                'reset_cnt': 2,
                                'trg_cnt': 64,
                                'int_cnt': 4000000},

                            {'pvname': 'D',
                                'msg': 'Abort D',
                                'pv_ts': '2018-01-01 00:10:00.523456790',
                                'abt_ts': '2018-01-01 00:10:00.123456790',
                                'reset_cnt': 0,
                                'trg_cnt': 10,
                                'int_cnt': 1000},

                            {'pvname': 'B',
                                'msg': 'Abort B',
                                'pv_ts': '2018-01-01 00:10:00.523456790',
                                'abt_ts': '2018-01-01 00:10:00.123456790',
                                'reset_cnt': 0,
                                'trg_cnt': 10,
                                'int_cnt': 1000},

                            {'pvname': 'F',
                                'msg': 'Abort F',
                                'pv_ts': '2018-01-01 00:10:00.523456790',
                                'abt_ts': '2018-01-01 00:10:01.123456790',
                                'reset_cnt': 0,
                                'trg_cnt': 1,
                                'int_cnt': 1000},

                            {'pvname': 'B',
                                'msg': 'Abort B',
                                'pv_ts': '2018-01-02 00:00:00.523456791',
                                'abt_ts': '2018-01-02 00:00:00.523456791',
                                'reset_cnt': 0,
                                'trg_cnt': 500,
                                'int_cnt': 500},

                            {'pvname': 'C',
                                'msg': 'Abort C',
                                'pv_ts': '2018-01-02 01:00:00.523456791',
                                'abt_ts': '2018-01-02 00:00:00.523456791',
                                'reset_cnt': 5,
                                'trg_cnt': 20,
                                'int_cnt': 5001},

                            {'pvname': 'E',
                                'msg': 'Abort E',
                                'pv_ts': '2018-01-03 00:00:00.523456792',
                                'abt_ts': '2018-01-03 00:00:00.123456792',
                                'reset_cnt': 0,
                                'trg_cnt': 1000,
                                'int_cnt': 9999999},

                            {'pvname': 'F',
                                'msg': 'Abort F',
                                'pv_ts': '2018-01-04 00:00:00.523456792',
                                'abt_ts': '2018-01-04 00:00:00.123456792',
                                'reset_cnt': 2,
                                'trg_cnt': 10,
                                'int_cnt': 0},

                            {'pvname': 'B',
                                'msg': 'Abort B',
                                'pv_ts': '2018-01-05 00:00:00.523456793',
                                'abt_ts': '2018-01-05 00:00:00.123456793',
                                'reset_cnt': 0,
                                'trg_cnt': 1000,
                                'int_cnt': 10},

                            {'pvname': 'G',
                                'msg': 'Abort G',
                                'pv_ts': '2018-01-05 00:00:00.523456792',
                                'abt_ts': '2018-01-05 00:00:00.123456792',
                                'reset_cnt': 0,
                                'trg_cnt': 1000,
                                'int_cnt': 10},

                            {'pvname': 'B',
                                'msg': 'Abort B',
                                'pv_ts': '2018-01-06 00:00:00.523456793',
                                'abt_ts': '2018-01-05 00:00:00.123456793',
                                'reset_cnt': 0,
                                'trg_cnt': 1000,
                                'int_cnt': 10},

                            {'pvname': 'B',
                                'msg': 'Abort B',
                                'pv_ts': '2018-01-07 00:00:00.523456793',
                                'abt_ts': '2018-01-06 00:00:00.123456793',
                                'reset_cnt': 2,
                                'trg_cnt': 10,
                                'int_cnt': 1},
                            ]

    md['abort_list'] = [
                         {'abt_id': 1, 'abt_signal_id': 1},
                         {'abt_id': 1, 'abt_signal_id': 2},
                         {'abt_id': 1, 'abt_signal_id': 3},
                         {'abt_id': 1, 'abt_signal_id': 4},
                         {'abt_id': 2, 'abt_signal_id': 5},
                         {'abt_id': 2, 'abt_signal_id': 6},
                         {'abt_id': 2, 'abt_signal_id': 7},
                         {'abt_id': 3, 'abt_signal_id': 8},
                         {'abt_id': 3, 'abt_signal_id': 9},
                         {'abt_id': 4, 'abt_signal_id': 10},
                         {'abt_id': 4, 'abt_signal_id': 11},
                         {'abt_id': 5, 'abt_signal_id': 12},
                         {'abt_id': 5, 'abt_signal_id': 13},
                         {'abt_id': 6, 'abt_signal_id': 14},
                         {'abt_id': 6, 'abt_signal_id': 15}
                        ]

    md['as_all_result'] = [
                             {'abt_id': 1,
                              'ts': '2018-01-01 00:00:00.123456789',
                              'pvname': 'B',
                              'msg': 'Abort B',
                              'ring': 'HER',
                              'reset_cnt': 0,
                              'trg_cnt': 4,
                              'int_cnt': 1000000},

                             {'abt_id': 1,
                              'ts': '2018-01-01 00:00:00.223456789',
                              'pvname': 'C',
                              'msg': 'Abort C',
                              'ring': 'HER',
                              'reset_cnt': 0,
                              'trg_cnt': 4,
                              'int_cnt': 2000000},

                             {'abt_id': 1,
                              'ts': '2018-01-01 00:00:30.323456789',
                              'pvname': 'A',
                              'msg': 'Abort A',
                              'ring': 'HER',
                              'reset_cnt': 1,
                              'trg_cnt': 34,
                              'int_cnt': 3000000},

                             {'abt_id': 2,
                              'ts': '2018-01-01 00:10:01.123456790',
                              'pvname': 'F',
                              'msg': 'Abort F',
                              'ring': 'LER',
                              'reset_cnt': 0,
                              'trg_cnt': 1,
                              'int_cnt': 1000},

                             {'abt_id': 2,
                              'ts': '2018-01-01 00:10:00.123456790',
                              'pvname': 'B',
                              'msg': 'Abort B',
                              'ring': 'HER',
                              'reset_cnt': 0,
                              'trg_cnt': 10,
                              'int_cnt': 1000},

                             {'abt_id': 2,
                              'ts': '2018-01-01 00:10:00.123456790',
                              'pvname': 'D',
                              'msg': 'Abort D',
                              'ring': 'LER',
                              'reset_cnt': 0,
                              'trg_cnt': 10,
                              'int_cnt': 1000},

                             {'abt_id': 3,
                              'ts': '2018-01-02 00:00:00.523456791',
                              'pvname': 'B',
                              'msg': 'Abort B',
                              'ring': 'HER',
                              'reset_cnt': 0,
                              'trg_cnt': 500,
                              'int_cnt': 500},

                             {'abt_id': 3,
                              'ts': '2018-01-02 00:00:00.523456791',
                              'pvname': 'C',
                              'msg': 'Abort C',
                              'ring': 'HER',
                              'reset_cnt': 5,
                              'trg_cnt': 20,
                              'int_cnt': 5001},

                             {'abt_id': 4,
                              'ts': '2018-01-03 00:00:00.123456792',
                              'pvname': 'E',
                              'msg': 'Abort E',
                              'ring': 'LER',
                              'reset_cnt': 0,
                              'trg_cnt': 1000,
                              'int_cnt': 9999999},

                             {'abt_id': 4,
                              'ts': '2018-01-04 00:00:00.123456792',
                              'pvname': 'F',
                              'msg': 'Abort F',
                              'ring': 'LER',
                              'reset_cnt': 2,
                              'trg_cnt': 10,
                              'int_cnt': 0},

                             {'abt_id': 5,
                              'ts': '2018-01-05 00:00:00.123456793',
                              'pvname': 'B',
                              'msg': 'Abort B',
                              'ring': 'HER',
                              'reset_cnt': 0,
                              'trg_cnt': 1000,
                              'int_cnt': 10},

                             {'abt_id': 5,
                              'ts': '2018-01-05 00:00:00.123456792',
                              'pvname': 'G',
                              'msg': 'Abort G',
                              'ring': 'LER',
                              'reset_cnt': 0,
                              'trg_cnt': 1000,
                              'int_cnt': 10},

                             {'abt_id': 6,
                              'ts': '2018-01-05 00:00:00.123456793',
                              'pvname': 'B',
                              'msg': 'Abort B',
                              'ring': 'HER',
                              'reset_cnt': 0,
                              'trg_cnt': 1000,
                              'int_cnt': 10},
            ]

    return md
