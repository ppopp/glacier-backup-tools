import os
import sys
import csv
import logging
import pprint
import codecs
import json

import credentials

import boto.glacier
import boto.glacier.layer2

_region = 'RegionInfo:us-east-1'

_logger = logging.getLogger(__name__)

def inventory(credentials_path, vault_name, job_id, output_path):
    boto.glacier.connect_to_region(_region)
    cred = credentials.read(credentials_path)
    _logger.debug('aws credentials: {0}'.format(cred))
    con = boto.glacier.layer2.Layer2(
        cred[credentials.access_header],
        cred[credentials.secret_header])

    _logger.info('getting vault {0}'.format(vault_name))
    vault = con.get_vault(vault_name)
    _logger.info('getting job {0} from vault {1}'.format(job_id, vault))
    job = vault.get_job(job_id)

    _logger.info('writing result of job {0} to file {1}'.format(job_id, output_path))
    #job.download_to_file(output_path)
    result = job.get_output()
    pprint.pprint(result)
    json.dump(result, codecs.open(output_path, 'w', 'utf-8'))




def _print_usage():
    print '\n\n\tusage: python', os.path.basename(__file__), '<credentials> <vault> <job> <output_path>\n\n'

if __name__ == '__main__':
    logFilename = os.path.splitext(os.path.basename(__file__))[0] + '.log'
    logging.basicConfig(
        filename=logFilename,
        filemode='a',
        level=logging.INFO)
    if len(sys.argv) != 5:
        _print_usage()
    else:
        print inventory(*sys.argv[1:])


