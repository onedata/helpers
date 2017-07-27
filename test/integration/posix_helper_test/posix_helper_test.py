"""This module tests GlusterFS helper."""

__author__ = "Bartek Kryza"
__copyright__ = """(C) 2017 ACK CYFRONET AGH,
This software is released under the MIT license cited in 'LICENSE.txt'."""

import os
import sys
import subprocess

import pytest

script_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.dirname(script_dir))
# noinspection PyUnresolvedReferences
from test_common import *
# noinspection PyUnresolvedReferences
from environment import common, docker, nfs
from posix_helper import PosixHelperProxy
from posix_test_base import *
from xattr_test_base import *

@pytest.fixture(scope='module')
def server(request):
    class Server(object):
        def __init__(self, mountpoint, uid, gid):
            self.mountpoint = mountpoint
            self.uid = uid
            self.gid = gid

    uid = 0
    gid = 0

    mountpoint = '/opt/posix_helper_test'

    assert os.system("mkdir -p %s"%(mountpoint)) == 0

    def fin():
         os.system("rm -rf %s"%(mountpoint))

    request.addfinalizer(fin)

    return Server(mountpoint, uid, gid)

@pytest.fixture
def helper(server):
    return PosixHelperProxy(
        server.mountpoint,
        server.uid,
        server.gid)
