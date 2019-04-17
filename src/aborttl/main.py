import logging.config
import argparse

from .aborttl import Aborttl


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

    return parser.parse_args()


def main():
    args = parse_args()

    if args.logconfig:
        logger = logging.config.fileConfig(config_file,
                                           disable_existing_loggers=False)
    else:
        logger = None

    atl = Aborttl(args.uri, args.resetpv, logger=logger)
    atl.run()


if __name__ == "__main__":
    main()
