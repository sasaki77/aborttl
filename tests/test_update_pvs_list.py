import pytest

from aborttl.main import update_pvs_list
from aborttl.dbhandler import DbHandler


@pytest.fixture(scope='session')
def uri(tmpdir_factory):
    pdb = tmpdir_factory.mktemp('data').join("database.db")
    uri = 'sqlite:///{}'.format(str(pdb))
    return uri


@pytest.fixture
def dh(uri):
    dh = DbHandler(uri)
    return dh


@pytest.fixture
def testdata():
    her_list = [
                  {'pvname': 'PV_HER1', 'msg': 'MESSAGE 1', 'ring': 'HER'},
                  {'pvname': 'PV_HER2', 'msg': 'MESSAGE 2', 'ring': 'HER'},
                  {'pvname': 'PV_HER3', 'msg': 'MESSAGE 3', 'ring': 'HER'},
                  {'pvname': 'PV_HER4', 'msg': 'MESSAGE 4', 'ring': 'HER'},
               ]

    ler_list = [
                  {'pvname': 'PV_LER1', 'msg': 'MESSAGE 1', 'ring': 'LER'},
                  {'pvname': 'PV_LER2', 'msg': 'MESSAGE 2', 'ring': 'LER'},
                  {'pvname': 'PV_LER3', 'msg': 'MESSAGE 3', 'ring': 'LER'},
                  {'pvname': 'PV_LER4', 'msg': 'MESSAGE 4', 'ring': 'LER'},
               ]

    return {'HER': her_list, 'LER': ler_list}


def test_first_update_pvs_list(uri, dh, testdata, tmpdir):
    pher = tmpdir.join("her.csv")
    pler = tmpdir.join("ler.csv")

    her_list = testdata['HER'][:3]
    ler_list = testdata['LER'][:3]

    for i in her_list:
        pher.write('{}, {}\n'.format(i['pvname'], i['msg']), mode='a')

    for i in ler_list:
        pler.write('{}, {}\n'.format(i['pvname'], i['msg']), mode='a')

    update_pvs_list(uri, str(pher), str(pler))

    current_pvs = dh.fetch_current_pvs()

    for pv_db, pv_list in zip(current_pvs, her_list + ler_list):
        assert pv_db['pvname'] == pv_list['pvname']
        assert pv_db['ring'] == pv_list['ring']
        assert pv_db['msg'] == pv_list['msg']


def test_second_update_pvs_list(uri, dh, testdata, tmpdir, tmpdir_factory):
    pher = tmpdir.join("her.csv")
    pler = tmpdir.join("ler.csv")
    pdb = tmpdir_factory.mktemp('data').join("database.db")

    her_list = testdata['HER'][1:]
    ler_list = testdata['LER'][1:]

    for i in her_list:
        pher.write('{}, {}\n'.format(i['pvname'], i['msg']), mode='a')

    for i in ler_list:
        pler.write('{}, {}\n'.format(i['pvname'], i['msg']), mode='a')

    update_pvs_list(uri, str(pher), str(pler))
    pvs = dh.fetch_all_pvs()

    td = (
            testdata['HER'][:3] + testdata['LER'][:3] +
            testdata['HER'][-1:] + testdata['LER'][-1:]
          )

    for pv_db, pv_list in zip(pvs, td):
        assert pv_db['pvname'] == pv_list['pvname']
        assert pv_db['ring'] == pv_list['ring']

    current_pvs = dh.fetch_current_pvs()

    for pv_db, pv_list in zip(current_pvs, her_list + ler_list):
        assert pv_db['pvname'] == pv_list['pvname']
        assert pv_db['ring'] == pv_list['ring']
        assert pv_db['msg'] == pv_list['msg']
