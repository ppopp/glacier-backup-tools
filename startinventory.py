import os
import sys
import csv
import logging

import credentials

import boto.glacier
import boto.glacier.layer2

_region = 'RegionInfo:us-east-1'

_logger = logging.getLogger(__name__)

def inventory(credentials_path):
    boto.glacier.connect_to_region(_region)
    cred = credentials.read(credentials_path)
    _logger.debug('aws credentials: {0}'.format(cred))
    con = boto.glacier.layer2.Layer2(
        cred[credentials.access_header],
        cred[credentials.secret_header])

    vs = con.list_vaults()
    for vault in vs:
        _logger.info('starting inventory job for {0}'.format(vault))
        jobid = vault.retrieve_inventory()
        _logger.info('vault {0}, job {1}'.format(vault, jobid))




def _print_usage():
    print '\n\n\tusage: python', os.path.basename(__file__), '<credentials>\n\n'

if __name__ == '__main__':
    logFilename = os.path.splitext(os.path.basename(__file__))[0] + '.log'
    logging.basicConfig(
        filename=logFilename,
        filemode='a',
        level=logging.INFO)
    if len(sys.argv) != 2:
        _print_usage()
    else:
        print inventory(*sys.argv[1:])


