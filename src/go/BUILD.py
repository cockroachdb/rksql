#!/usr/bin/env python
"""Build script for go code."""

import argparse
import os
import subprocess
import sys

DEP_BIN = 'dep'
GO_BIN = 'go'
# goimports does everything that gofmt does. In addition it also
# 1. checks for extra/missing imports,
# 2. enforces import ordering standard [before] 3rdparty [before] internal
GOIMPORTS_BIN = 'goimports'
SRC_GO = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__))))

os.environ['GOPATH'] = SRC_GO


def run(cmd):
    print 'Running: {}'.format(' '.join(cmd))
    try:
        return subprocess.check_output(cmd, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        print e.output
        raise

parser = argparse.ArgumentParser(description='Go build script')
parser.add_argument('--clean', action='store_true')
parsed_args = parser.parse_args()

if parsed_args.clean:
    print '\nCleaning go build'
    run(['rm', '-rf', '%s/bin' % SRC_GO])
    run(['rm', '-rf', '%s/pkg' % SRC_GO])
    run(['rm', '-rf', '%s/src/rubrik/vendor' % SRC_GO])
    sys.exit(0)

print '\nGo build starting'
# Clean the vendor directory on every invocation of BUILD.py
# If we do not do this, build fails when there is a dependency change, which is
# probably an issue with `dep`
print 'Cleaning vendor directory %s/src/rubrik/vendor' % SRC_GO
run(['rm', '-rf', '%s/src/rubrik/vendor' % SRC_GO])
print 'Changing working directory to %s' % os.path.join(SRC_GO, 'src/rubrik')
os.chdir(os.path.join(SRC_GO, 'src/rubrik'))

print 'Ensuring all external dependencies are met'
# The -vendor-only flag ensures that we do not update Gopkg.lock as a part of
# ensure. This is also much faster than trying to solve the whole dependency
# graph for every build. Developers should run dep ensure without -vendor-only
# when they want to add/update dependencies.
run([DEP_BIN, 'ensure', '-vendor-only', '-v'])

rubrik_packages = [
    package
    for package
    in os.listdir('.')
    if os.path.isdir(os.path.join('.', package)) and package != 'vendor'
]

for package in rubrik_packages:
    print 'Building rubrik/' + package
    run([GO_BIN, 'install', './' + package + '/...'])

for package in rubrik_packages:
    print 'Checking formatting for rubrik/' + package
    goimports_out = run([GOIMPORTS_BIN,
                         '-local', 'rubrik',
                         '-l',
                         './' + package]).strip()
    if goimports_out:
        print 'goimports fails on paths:'
        print '==='
        print goimports_out
        print '==='
        raise Exception(
            'Formatting check failed. Run `goimports -local rubrik -w '
            '$SDMAIN/src/go/src/rubrik/%s`' % package)

print 'Go build done\n'
