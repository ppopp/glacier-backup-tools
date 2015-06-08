import os
import sys

import subprocess

def tardirs(*args):
    for path in args:
        path = path.rstrip('/')
        output = path + '.tar.gz'
        cmd = ['tar', '-cvf', output, path]
        print cmd
        if not os.path.isfile(output):
            subprocess.call(cmd)


def _print_usage():
    print '\n\n\tusage: python', os.path.basename(__file__), '[directories...]\n\n'

if __name__ == '__main__':
    if len(sys.argv) > 1:
        tardirs(*sys.argv[1:])
    else:
        _print_usage()
