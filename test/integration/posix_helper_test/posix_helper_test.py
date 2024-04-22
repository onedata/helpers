"""This module tests POSIX helper."""

__author__ = "Bartek Kryza"
__copyright__ = """(C) 2017 ACK CYFRONET AGH,
This software is released under the MIT license cited in 'LICENSE.txt'."""

import os
import sys
import subprocess
from os.path import expanduser

import pytest

script_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.dirname(script_dir))
# noinspection PyUnresolvedReferences
from test_common import *
# noinspection PyUnresolvedReferences
from environment import common, docker, nfs
from posix_helper import *
from posix_test_base import *
from xattr_test_base import *
from io_perf_test_base import *


@pytest.fixture(scope='module')
def server(request):
    class Server(object):
        def __init__(self, mountpoint, uid, gid):
            self.mountpoint = mountpoint
            self.uid = uid
            self.gid = gid

    home = expanduser("~")
    mountpoint = os.path.join(home, 'posix_helper_test')

    assert os.system("mkdir -p %s"%(mountpoint)) == 0

    def fin():
         os.system("rm -rf %s"%(mountpoint))

    request.addfinalizer(fin)

    return Server(mountpoint, os.geteuid(), os.getegid())


@pytest.fixture
def helper(server):
    return PosixHelperProxy(
        server.mountpoint,
        server.uid,
        server.gid)


@pytest.fixture
def helper_invalid_mountpoint(server):
    return PosixHelperProxy(
        "/tmp/no_such_directory" + random_str(),
        server.uid,
        server.gid)


def test_helper_check_availability(helper):
    helper.check_storage_availability()


def test_helper_check_availability_error(helper_invalid_mountpoint):
    with pytest.raises(RuntimeError) as excinfo:
        helper_invalid_mountpoint.check_storage_availability()

    assert 'No such file or directory' in str(excinfo)


def test_helper_should_update_params(helper, file_id):
    data = random_str()
    offset = random_int()

    assert helper.mountpoint() == "/tmp/posix_helper_test"

    helper.update_helper("/tmp/invalid_mountpoint", -1, -1)

    assert helper.mountpoint() == "/tmp/invalid_mountpoint"

    helper.update_helper("/tmp", -1, -1)

    assert helper.mountpoint() == "/tmp"
