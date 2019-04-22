import csv
import logging.config
import argparse

from .dbhandler import DbHandler
from .aborttl import Aborttl
from .logger import get_default_logger


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument('-r', '--resetpv', dest='resetpv',
                        help='Reset PV Name', required=True)
    parser.add_argument('-u', '--uri', dest='uri',
                        help='Database URI',
                        default='sqlite:///:memory:')
    parser.add_argument('-l', '--logconfig', dest='logconfig',
                        help='logging config file path.',
                        default=None)
    parser.add_argument('-H', dest='herlist',
                        help='path to HER pv list', default=None)
    parser.add_argument('-L', dest='lerlist',
                        help='path to LER pv list', default=None)

    return parser.parse_args()


def update_pvs_list(uri, herlist, lerlist, logger=None):
    logger = logger or get_default_logger()
    dh = DbHandler(uri)
    pvs_db = dh.fetch_all_pvs()

    pvs = {pv['pvname']: pv['ring'] for pv in pvs_db}

    rings = {'HER': herlist, 'LER': lerlist}

    pvs_csv = []
    for ring, csvpath in rings.items():
        with open(csvpath) as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                pv = {"pvname": row[0].strip()}
                pv["msg"] = row[1].strip() if len(row) > 1 else ''
                pv["ring"] = ring
                pvs_csv.append(pv)

    inspvs = []
    for pv in pvs_csv:
        inspvs.append({'pvname': pv['pvname'], 'ring': pv['ring']})

    if inspvs:
        dh.insert_pvs(inspvs)

    current_pvs = []
    for pv in pvs_csv:
        current_pvs.append({'pvname': pv['pvname'], 'msg': pv['msg']})

    if current_pvs:
        dh.update_current_pvs(current_pvs)


def main():
    args = parse_args()

    if args.logconfig:
        logging.config.fileConfig(args.logconfig,
                                  disable_existing_loggers=False)
        logger = logging.getLogger(__name__)
    else:
        logger = get_default_logger()

    if args.herlist and args.lerlist:
        update_pvs_list(args.uri, args.herlist, args.lerlist, logger)
    elif args.herlist or args.lerlist:
        logger.critical('PV list must be privided for both ring')
        return -1

    atl = Aborttl(args.uri, args.resetpv, logger=logger)
    atl.run()


if __name__ == "__main__":
    main()
