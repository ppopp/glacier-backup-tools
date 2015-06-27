import os
import sys
import csv
import time
import logging
import socket

import boto.glacier
import boto.glacier.layer2

import credentials

_region = 'RegionInfo:us-east-1'
_multipart_filesize = 1024 * 1024 * 100 # 100 megabytes
_max_retries = 10
_retry_sleep = 60

_logger = logging.getLogger(__name__)
_estimated_bytes_per_second = 650 * 1024

class Error(Exception):
    pass

def _expected_duration(filesize):
    expected_time = filesize / float(_estimated_bytes_per_second)
    mins, secs = divmod(expected_time, 60)
    hours, mins = divmod(mins, 60)
    timestr = '%02d:%02d:%02d' % (hours, mins, secs)
    return timestr

def upload(
    credentials_path, 
    vault_name, 
    data_path, 
    retries=0, 
    max_retries=_max_retries, 
    retry_sleep=_retry_sleep):
    # wrap with exception handling and keep track of the number of retries
    # so that if a connection is temporarily dropped that we don't stop the 
    # upload process.
    try:
        _upload(credentials_path, vault_name, data_path)
    except Exception, e:
        retries += 1
        if retries > _max_retries:
            _logger.error('max retries {0} with exception {1}'.format(retries, e))
            raise
        else:
            _logger.warning('retrying after failure with exception {0}'.format(e))
            _logger.info('retry {0} of {1}'.format(retries, max_retries))
            _logger.info('sleeping for {0} seconds before retry'.format(retry_sleep))
            # sleep for a bit to allow recovery
            time.sleep(retry_sleep)
            # try uploading again
            upload(credentials_path, vault_name, data_path, retries=retries)
    

def _upload(credentials_path, vault_name, data_path):
    # check file inputs
    filesize = os.path.getsize(data_path)
    do_multipart = filesize >= _multipart_filesize

    _logger.info(
        'input file: {0} with size: {1} multipart_upload: {2}'.format(
            data_path, filesize, do_multipart))

    # load credentials
    cred = credentials.read(credentials_path)
    _logger.debug('aws credentials: {0}'.format(cred))
    # connect to glacier with loaded credentials
    _logger.info('opening vault: {0} in region {1}'.format(vault_name, _region))
    boto.glacier.connect_to_region(_region)
    con = boto.glacier.layer2.Layer2(
        cred[credentials.access_header],
        cred[credentials.secret_header])
    vault = con.get_vault(vault_name)

    # upload file
    description = socket.gethostname() + ':' + os.path.basename(data_path)
    _logger.info('beginning upload of {0} to {1} in {2} with description "{3}"'.format(
        data_path, 
        vault_name, 
        _region, 
        description))

    timestr = _expected_duration(filesize)
    _logger.info('expected upload time for {0} is {1}'.format(data_path, timestr))

    if do_multipart:
        vault.concurrent_create_archive_from_file(
            data_path, 
            description,
            num_threads=2)
    else:
        vault.create_archive_from_file(
            data_path,
            description=description)
    _logger.info('completed upload of {0} to {1} in {2}'.format(data_path, vault_name, _region))


def _upload_paths(credentials_path, vault_name, paths):
    total_filesize = sum(map(os.path.getsize, paths))
    _logger.info(
        'beginning upload of {0} files totaling {1} bytes'.format(
            len(paths), total_filesize))

    timestr = _expected_duration(total_filesize)
    _logger.info('expected upload time {0}'.format(timestr))


    for path in paths:
       upload(credentials_path, vault_name, path)

def _print_usage():
    print '\n\n\tusage: python', os.path.basename(__file__), '<credentials> <vault> [paths...]\n\n'

if __name__ == '__main__':
    logFilename = os.path.splitext(os.path.basename(__file__))[0] + '.log'
    format = '%(asctime)s [%(name)s:%(levelname)s] %(message)s'
    logging.basicConfig(
        filename=logFilename,
        filemode='a',
        format=format,
        level=logging.INFO)
    if len(sys.argv) < 4:
        _print_usage()
    else:
        cred_path = sys.argv[1]
        vault = sys.argv[2]
        # upload all paths
        _logger.info('****************BEGIN******************************')
        try:
            _upload_paths(cred_path, vault, sys.argv[3:])
        except BaseException, e:
            _logger.error('stopped with exception {0}: {1}'.format(type(e), e))
            raise
        _logger.info('****************COMPLETE******************************')


