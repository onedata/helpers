"""This module tests StorageRouter helper."""

__author__ = "Bartek Kryza"
__copyright__ = """(C) 2021 ACK CYFRONET AGH,
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
from storagerouter_helper import StorageRouterHelperProxy


@pytest.fixture(scope='module')
def server(request):
    class Server(object):
        def __init__(self, route_a, mountpoint_a, route_b, mountpoint_b, uid,
                     gid):
            self.mountpoint_a = mountpoint_a
            self.mountpoint_b = mountpoint_b
            self.route_a = route_a
            self.route_b = route_b
            self.uid = uid
            self.gid = gid

    home = expanduser("~")
    mountpoint_a = os.path.join(home, 'storagerouter_helper_test/.hidden')
    mountpoint_b = os.path.join(home, 'storagerouter_helper_test')
    route_a = '/.hidden'
    route_b = '/'

    assert os.system("mkdir -p %s" % (mountpoint_a)) == 0
    assert os.system("mkdir -p %s" % (mountpoint_b)) == 0

    def fin():
        os.system("rm -rf %s" % (mountpoint_a))
        os.system("rm -rf %s" % (mountpoint_b))

    request.addfinalizer(fin)

    return Server(route_a, mountpoint_a, route_b, mountpoint_b, os.geteuid(),
                  os.getegid())


@pytest.fixture
def helper(server):
    return StorageRouterHelperProxy(server.route_a, server.mountpoint_a,
                                    server.route_b, server.mountpoint_b,
                                    server.uid, server.gid)


@pytest.mark.readwrite_operations_tests
def test_read_should_read_written_data(helper):
    data = random_str()
    offset = random_int()

    file_id_a = '/.hidden/' + random_str()
    file_id_b = '/' + random_str()

    assert helper.write(file_id_a, data, offset) == len(data)
    assert helper.read(file_id_a, offset, len(data)) == data

    assert helper.write(file_id_b, data, offset) == len(data)
    assert helper.read(file_id_b, offset, len(data)) == data

    home = expanduser("~")
    assert helper.mountpoint(file_id_a) == os.path.join(
        home, 'storagerouter_helper_test/.hidden')
    assert helper.mountpoint(file_id_b) == os.path.join(
        home, 'storagerouter_helper_test')
