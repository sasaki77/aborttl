import argparse
import json
from pathlib import Path

from .dbhandler import DbHandler
from .logger import get_default_logger


class Migration(object):

    def __init__(self, uri, root, hpath, lpath, logger=None):
        self.logger = logger or get_default_logger()
        self.dh = DbHandler(uri)
        self.root = root
        self.hpath = hpath
        self.lpath = lpath

    def migration_from_json(self):
        rootpath = Path(self.root)
        herpaths = list(rootpath.glob(self.hpath + '/**/*.json'))
        lerpaths = list(rootpath.glob(self.lpath + '/**/*.json'))

        herpaths.sort()
        lerpaths.sort()

        herlen = len(herpaths)
        lerlen = len(lerpaths)
        ih = 0
        il = 0

        while(ih <= herlen and il <= lerlen):
            if ih == herlen and il == lerlen:
                break

            herpath = herpaths[ih] if ih < herlen else None
            lerpath = lerpaths[il] if il < lerlen else None

            if (lerpath is None or herpath is not None
                    and herpath.name < lerpath.name):
                self.onering_register(herpath, 'HER')
                ih += 1
            elif (herpath is None or lerpath is not None
                    and herpath.name > lerpath.name):
                self.onering_register(lerpath, 'LER')
                il += 1
            elif herpath.name == lerpath.name:
                self.bothring_register(herpath, lerpath)
                ih += 1
                il += 1

    def onering_register(self, path, ring):
        print('One ring: {}'.format(path))
        with path.open() as f:
            aborts = json.load(f)

        abt_id = self.dh.insert_abort(aborts[0]['DATE'])

        self.register_abt_signals(abt_id, ring, aborts)

    def bothring_register(self, herpath, lerpath):
        print('Both ring: {}, {}'.format(herpath, lerpath))
        with herpath.open() as f:
            heraborts = json.load(f)

        with lerpath.open() as f:
            leraborts = json.load(f)

        if heraborts[0]['DATE'] < leraborts[0]['DATE']:
            first = heraborts[0]['DATE']
        else:
            first = leraborts[0]['DATE']

        abt_id = self.dh.insert_abort(first)

        self.register_abt_signals(abt_id, 'HER', heraborts)
        self.register_abt_signals(abt_id, 'LER', leraborts)

    def register_abt_signals(self, abt_id, ring, aborts):
        abort_pvs = []
        abort_sig = []

        for abort in aborts:
            abort_pvs.append({'pvname': abort['NAME'], 'ring': ring})

            msg = abort['MSG'] if 'MSG' in abort else ''
            ts = abort['TS'] if 'TS' in abort else abort['DATE'][:-3]
            sig = {'pvname': abort['NAME'], 'msg': msg, 'pv_ts': ts,
                   'abt_ts': abort['DATE'], 'reset_cnt': abort['RESET'],
                   'trg_cnt': abort['TCNT'], 'int_cnt': abort['ACNT']}
            abort_sig.append(sig)

        self.dh.insert_pvs(abort_pvs)
        ids = self.dh.insert_abort_signals(abort_sig, abt_id)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument('-u', '--uri', dest='uri',
                        help='Database URI',
                        required=True,
                        default='sqlite:///:memory:')
    parser.add_argument('-r', '--root', dest='root',
                        help='path to root directory',
                        required=True,
                        default=None)
    parser.add_argument('-H', dest='her_dir',
                        required=True,
                        help='path to HER directory', default=None)
    parser.add_argument('-L', dest='ler_dir',
                        required=True,
                        help='path to LER directory', default=None)

    return parser.parse_args()


def main():
    args = parse_args()

    mg = Migration(args.uri, args.root, args.her_dir, args.ler_dir)
    mg.migration_from_json()


if __name__ == "__main__":
    main()
