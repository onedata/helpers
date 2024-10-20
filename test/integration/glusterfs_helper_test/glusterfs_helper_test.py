"""This module tests GlusterFS helper."""

__author__ = "Bartek Kryza"
__copyright__ = """(C) 2017 ACK CYFRONET AGH,
This software is released under the MIT license cited in 'LICENSE.txt'."""

import os
import sys

import pytest

script_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.dirname(script_dir))
# noinspection PyUnresolvedReferences
from test_common import *
# noinspection PyUnresolvedReferences
from environment import glusterfs, common, docker
from glusterfs_helper import GlusterFSHelperProxy
from posix_test_base import *
from xattr_test_base import *
from io_perf_test_base import *

@pytest.fixture(scope='module')
def server(request):
    class Server(object):
        def __init__(self, mountpoint, uid, gid, hostname, port,
                     volume, transport, xlatorOptions):
            self.mountpoint = mountpoint
            self.uid = uid
            self.gid = gid
            self.hostname = hostname
            self.port = port
            self.volume = volume
            self.transport = transport
            self.xlatorOptions = xlatorOptions

    uid = 0
    gid = 0
    volume = 'data'
    result = glusterfs.up('gluster/gluster-centos:gluster4u1_centos7',
                          [volume], 'storage', common.generate_uid(), 'tcp',
                          random_str()+"/"+random_str())

    [container] = result['docker_ids']
    hostname = result['host_name'].encode('ascii')
    port = result['port']
    transport = result['transport'].encode('ascii')
    mountpoint = result['mountpoint']

    def fin():
        docker.remove([container], force=True, volumes=True)

    request.addfinalizer(fin)

    return Server(mountpoint, uid, gid, hostname, port, volume, transport, "")


@pytest.fixture
def helper(server):
    return GlusterFSHelperProxy(
        server.mountpoint,
        server.uid,
        server.gid,
        server.hostname,
        server.port,
        server.volume,
        server.transport,
        server.xlatorOptions)


@pytest.fixture
def helper_invalid_hostname(server):
    return GlusterFSHelperProxy(
        server.mountpoint,
        server.uid,
        server.gid,
        "no_such_host.invalid:80800",
        server.port,
        server.volume,
        server.transport,
        server.xlatorOptions)


@pytest.fixture
def helper_invalid_volume(server):
    return GlusterFSHelperProxy(
        server.mountpoint,
        server.uid,
        server.gid,
        server.hostname,
        server.port,
        "no_such_volume",
        server.transport,
        server.xlatorOptions)


def test_helper_check_availability(helper):
    helper.check_storage_availability()


def test_helper_check_availability_error_invalid_host(helper_invalid_hostname):
    with pytest.raises(RuntimeError) as excinfo:
        helper_invalid_hostname.check_storage_availability()

    assert "Transport endpoint is not connected" in str(excinfo)


def test_helper_check_availability_error_invalid_bucket(helper_invalid_volume):
    with pytest.raises(RuntimeError) as excinfo:
        helper_invalid_volume.check_storage_availability()

    assert 'No such file or directory' in str(excinfo)

