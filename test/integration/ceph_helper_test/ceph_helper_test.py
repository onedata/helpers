"""This module tests Ceph helper."""

__author__ = "Krzysztof Trzepla"
__copyright__ = """(C) 2015 ACK CYFRONET AGH,
This software is released under the MIT license cited in 'LICENSE.txt'."""

import os
import sys

import pytest
import threading

script_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.dirname(script_dir))
# noinspection PyUnresolvedReferences
from test_common import *
# noinspection PyUnresolvedReferences
from environment import ceph, common, docker
from ceph_helper import CephHelperProxy
from xattr_test_base import *
from io_perf_test_base import *


@pytest.fixture(scope='module')
def server(request):
    class Server(object):
        def __init__(self, mon_host, username, key, pool_name):
            self.mon_host = mon_host
            self.username = username
            self.key = key
            self.pool_name = pool_name

    pool_name = 'data'
    result = ceph.up('onedata/ceph', [(pool_name, '8')], 'storage',
                     common.generate_uid())

    [container] = result['docker_ids']
    username = result['username'].encode('ascii')
    key = result['key'].encode('ascii')
    mon_host = result['host_name'].encode('ascii')

    def fin():
        docker.remove([container], force=True, volumes=True)

    request.addfinalizer(fin)

    return Server(mon_host, username, key, pool_name)


@pytest.fixture
def helper(server):
    return CephHelperProxy(server.mon_host, server.username, server.key,
                           server.pool_name)


@pytest.fixture
def helper_invalid(server):
    return CephHelperProxy(server.mon_host, server.username, server.key,
                           "no_such_pool")


def test_helper_check_availability(helper):
    helper.check_storage_availability()


def test_helper_check_availability_error(helper_invalid):
    with pytest.raises(RuntimeError) as excinfo:
        helper_invalid.check_storage_availability()

    assert 'No such file or directory' in str(excinfo)


def test_helper_creation_should_not_leak_memory(server):
    for i in range(10):
        helper = CephHelperProxy(server.mon_host, server.username, server.key,
                           server.pool_name)
        test_write_should_write_data(helper)

def test_write_should_write_data(helper):
    file_id = random_str()
    data = random_str()
    offset = random_int()

    assert helper.write(file_id, data, offset) == len(data)


def test_read_should_pass_errors(helper):
    file_id = random_str()
    offset = random_int()
    size = random_int()

    with pytest.raises(RuntimeError) as excinfo:
        helper.read(file_id, offset, size)
    assert 'No such file or directory' in str(excinfo.value)


def test_read_should_read_data(helper):
    file_id = random_str()
    data = random_str()
    offset = random_int()

    assert helper.write(file_id, data, offset) == len(data)
    assert helper.read(file_id, offset, len(data)).decode('utf-8') == data


@pytest.mark.skip(reason=
  "libradosstriper does not report error when removing non existing file")
def test_unlink_should_pass_errors(helper):
    file_id = random_str()

    with pytest.raises(RuntimeError) as excinfo:
        helper.unlink(file_id, 0)
    assert 'No such file or directory' in str(excinfo.value)


def test_unlink_should_delete_data(helper):
    file_id = random_str()
    data = random_str()
    offset = random_int()

    assert helper.write(file_id, data, offset) == len(data)
    helper.unlink(file_id, offset+len(data))

    with pytest.raises(RuntimeError) as excinfo:
        helper.read(file_id, offset, len(data))
    assert 'No such file or directory' in str(excinfo.value)


def test_unlink_should_delete_file_with_dangling_shared_locks(helper):
    file_id = random_str()
    data = random_str()
    offset = random_int()
    cookie1 = random_str()
    cookie2 = random_str()
    cookie3 = random_str()

    assert helper.write(file_id, data, offset) == len(data)
    assert helper.lock(file_id, cookie1, False) == 0
    assert helper.lock(file_id, cookie2, False) == 0
    assert helper.lock(file_id, cookie3, False) == 0
    helper.unlink(file_id, offset+len(data))

    with pytest.raises(RuntimeError) as excinfo:
        helper.read(file_id, offset, len(data))
    assert 'No such file or directory' in str(excinfo.value)


def test_unlink_should_delete_file_with_dangling_exclusive_lock(helper):
    file_id = random_str()
    data = random_str()
    offset = random_int()
    cookie = random_str()

    assert helper.write(file_id, data, offset) == len(data)
    assert helper.lock(file_id, cookie, True) == 0
    helper.unlink(file_id, offset+len(data))

    with pytest.raises(RuntimeError) as excinfo:
        helper.read(file_id, offset, len(data))
    assert 'No such file or directory' in str(excinfo.value)


def test_truncate_should_truncate_nonexisting_file(helper):
    file_id = random_str()
    data = random_str()

    helper.truncate(file_id, len(data), 0)
    assert len(helper.read(file_id, 0, len(data))) == len(data)


def test_truncate_should_truncate_data(helper):
    file_id = random_str()
    data = random_str()
    size = random_int(upper_bound=len(data))

    assert helper.write(file_id, data, 0) == len(data)
    helper.truncate(file_id, size, len(data))
    assert helper.read(file_id, 0, size).decode('utf-8') == data[0:size]


def test_truncate_should_truncate_file_with_dangling_shared_locks(helper):
    file_id = random_str()
    data = random_str()
    cookie1 = random_str()
    cookie2 = random_str()
    cookie3 = random_str()
    size = random_int(upper_bound=len(data))

    assert helper.write(file_id, data, 0) == len(data)
    assert helper.lock(file_id, cookie1, False) == 0
    assert helper.lock(file_id, cookie2, False) == 0
    assert helper.lock(file_id, cookie3, False) == 0
    helper.truncate(file_id, size, len(data))
    assert helper.read(file_id, 0, size).decode('utf-8') == data[0:size]


def test_truncate_should_truncate_file_with_dangling_exclusive_lock(helper):
    file_id = random_str()
    data = random_str()
    cookie = random_str()
    size = random_int(upper_bound=len(data))

    assert helper.write(file_id, data, 0) == len(data)
    assert helper.lock(file_id, cookie, True) == 0
    helper.truncate(file_id, size, len(data))
    assert helper.read(file_id, 0, size).decode('utf-8') == data[0:size]
