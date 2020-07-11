"""This module tests HTTP helper."""

__author__ = "Bartek Kryza"
__copyright__ = """(C) 2018 ACK CYFRONET AGH,
This software is released under the MIT license cited in 'LICENSE.txt'."""

import os
import sys
import time
import subprocess
from os.path import expanduser
from urlparse import urlparse

import pytest
import hashlib

script_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.dirname(script_dir))
# noinspection PyUnresolvedReferences
from test_common import *
# noinspection PyUnresolvedReferences
from environment import common, docker, http
from http_helper import HTTPHelperProxy

from posix_test_types import *
from common_test_base import file_id

@pytest.fixture(scope='module')
def server(request):
    class Server(object):
        def __init__(self, endpoint, credentials):
            self.endpoint = endpoint
            self.credentials = credentials

    result = http.up('onedata/lighttpd:v1', 'storage',
                       common.generate_uid())

    [container] = result['docker_ids']
    credentials = result['credentials'].encode('ascii')
    endpoint = result['endpoint'].encode('ascii')

    def fin():
        docker.remove([container], force=True, volumes=True)

    request.addfinalizer(fin)

    time.sleep(5)

    return Server(endpoint, credentials)


@pytest.fixture
def helper(server):
    return HTTPHelperProxy(server.endpoint, server.credentials, "basic")

@pytest.fixture
def public_helper():
    return HTTPHelperProxy("https://packages.onedata.org", "", "none")


def get_file_index(h):
    res = []
    index_file = h.read('/test_data/index.txt', 0, 10000)
    for line in index_file.splitlines():
        (md5sum, size, timestamp) = line.split(',')
        res.append((md5sum, size, timestamp))
    return res


def test_getattr_should_get_mode_and_timestamp(helper, file_id):
    index = get_file_index(helper)

    for file_data in index:
        stat = helper.getattr('/test_data/'+file_data[0])
        assert stat.st_size == int(file_data[1])


def test_getattr_should_get_mode_and_timestamp_with_absolute_url(server, file_id):
    helper = HTTPHelperProxy(server.endpoint, server.credentials, "basic")
    index = get_file_index(helper)

    for file_data in index:
        stat = helper.getattr(server.endpoint+'/test_data/'+file_data[0])
        assert stat.st_size == int(file_data[1])


def test_read_should_read_valid_data(helper, file_id):
    index = get_file_index(helper)

    for file_data in index:
        data = helper.read('/test_data/'+file_data[0], 0, int(file_data[1]))
        assert len(data) == int(file_data[1])
        m = hashlib.md5()
        m.update(data)
        assert file_data[0] == m.hexdigest()


def test_read_should_read_valid_data_with_absolute_url(server, file_id):
    helper = HTTPHelperProxy(server.endpoint, server.credentials, "basic")
    index = get_file_index(helper)

    for file_data in index:
        data = helper.read(server.endpoint+'/test_data/'+file_data[0], 0, int(file_data[1]))
        assert len(data) == int(file_data[1])
        m = hashlib.md5()
        m.update(data)
        assert file_data[0] == m.hexdigest()


def test_read_should_return_errors(helper):
    with pytest.raises(RuntimeError) as excinfo:
        helper.read('/not_existent', 0, 1024)
    assert 'No such file or directory' in str(excinfo.value)


def test_read_should_work_with_public_servers(public_helper):
    size = public_helper.getattr('/apt/ubuntu/2002/dists/bionic/Release').st_size

    data = public_helper.read('/apt/ubuntu/2002/dists/bionic/Release', 0, size)

    assert len(data) == size


def test_read_should_work_with_public_servers_using_external_urls(helper):
    size = helper.getattr('https://packages.onedata.org/apt/ubuntu/2002/dists/bionic/Release').st_size

    data = helper.read('https://packages.onedata.org/apt/ubuntu/2002/dists/bionic/Release', 0, size)

    assert len(data) == size


def test_read_should_work_with_public_servers_using_absolute_urls(public_helper):
    size = public_helper.getattr('https://packages.onedata.org/apt/ubuntu/2002/dists/bionic/Release').st_size

    data = public_helper.read('https://packages.onedata.org/apt/ubuntu/2002/dists/bionic/Release', 0, size)

    assert len(data) == size
