import json
import codecs
import os
import sys
import pprint
import operator

import logging

_logger = logging.getLogger(__name__)

def _duplicates(items):
    existing = set()
    duplicate = []
    for item in items:
        if item in existing:
            duplicate.append(item)
        existing.add(item)
    return duplicate

def compare(inventory_path, paths):
    _logger.debug('loading inventory from {0}'.format(inventory_path))
    inventory = json.load(codecs.open(inventory_path, 'r', 'utf-8'))
    _logger.info('vault: "{0}"'.format(inventory.get('VaultARN')))
    archives = inventory.get('ArchiveList')
    if archives is None:
        _logger.error('no archive list found in inventory')
        return

    # get sizes of existing files
    existing_files = []
    for path in paths:
        filename = os.path.basename(path)
        filesize = os.path.getsize(path)
        existing_files.append((filename, filesize))
    
    # get sizes of archive files
    archive_files = []
    for archive in archives:
        filename = archive['ArchiveDescription'].split(':')[1]
        filesize = archive['Size']
        archive_files.append((filename, filesize))

    # get any duplicate named stuff to avoid confusion later on
    duplicate_files = set(_duplicates(
        map(operator.itemgetter(0), existing_files)))
    duplicate_archives = set(_duplicates(
        map(operator.itemgetter(0), archive_files)))

    if len(duplicate_files):
        print '\nduplicate files'
        for duplicate_file in duplicate_files:
            print '\t', duplicate_file
    if len(duplicate_archives):
        print '\nduplicate archives'
        for duplicate_archive in duplicate_archives:
            print '\t', duplicate_archive


    unmatched = []
    for filename, filesize in existing_files:
        matched = False
        matched_name = False
        pos = 0
        for archivename, archivesize in archive_files:
            if filename == archivename:
                matched_name
                if filesize == archivesize:
                    matched = True
                    break
            pos += 1

        if matched:
            # remove the archive from the list if it was matchd
            archive_files.pop(pos)
        elif matched_name:
            # save the file for reporting if it wasn't matched
            _logger.warning('matched name but not size {0} {1}'.format(filename, filesize))
            unmatched.append((filename, filesize))
        else:
            # save the file for reporting if it wasn't matched
            _logger.warning('unmatched file {0} {1}'.format(filename, filesize))
            unmatched.append((filename, filesize))

    _logger.info('found {0} unmatched files'.format(len(unmatched)))
    _logger.info('found {0} unmatched archives'.format(len(archive_files)))



    num_files = len(existing_files)
    unmatched_count = len(unmatched)
    print '\nmatched {0} of {1} files\n'.format(num_files - unmatched_count, num_files)
    if len(unmatched):
        print 'unmatched files'
        for filename, filesize in unmatched:
            print '\t',
            if filename in duplicate_files:
                print '[duplicate]',
            print filename, filesize
        print '\n'

    if len(archive_files):
        print 'unmatched archives'
        for filename, filesize in archive_files:
            print '\t',
            if filename in duplicate_archives:
                print '[duplicate]',
            print filename, filesize
        print '\n'



def _print_usage():
    print '\n\n\tusage: python', os.path.basename(__file__), '<inventory> [files...]\n\n'


if __name__ == '__main__':
    logFilename = os.path.splitext(os.path.basename(__file__))[0] + '.log'
    logging.basicConfig(
        filename=logFilename,
        filemode='a',
        level=logging.INFO)
    if len(sys.argv) < 3:
        _print_usage()
    else:
        compare(sys.argv[1], sys.argv[2:])
