"""This module tests NFS helper."""

__author__ = "Bartek Kryza"
__copyright__ = """(C) 2021 ACK CYFRONET AGH,
This software is released under the MIT license cited in 'LICENSE.txt'."""

import os
import sys
import md5
import tempfile

import pytest

script_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.dirname(script_dir))
# noinspection PyUnresolvedReferences
from test_common import *
# noinspection PyUnresolvedReferences
from environment import nfs, common, docker
from nfs_helper import NFSHelperProxy
from posix_test_base import *
from io_perf_test_base import *

@pytest.fixture(scope='module')
def server(request):
    class Server(object):
        def __init__(self, tmp_dir, uid, gid, version, host, port, volume):
            self.tmp_dir = tmp_dir
            self.version = version
            self.uid = uid
            self.gid = gid
            self.host = host
            self.port = port
            self.volume = volume

    uid = 0
    gid = 0
    volume = '/nfsshare'
    version = 3
    tmp_dir = tempfile.mkdtemp(dir=common.HOST_STORAGE_PATH, prefix="helpers_nfs_")
    port = 2049

    # result = nfs.up('onedata/nfs:v1', common.generate_uid(), 'storage', tmp_dir)

    # [container] = result['docker_ids']
    # host = result['ip'].encode('ascii')
    host = "172.17.0.2"


    def fin():
        docker.remove([container], force=True, volumes=True)
        check_output(['rm', '-rf', tmp_dir])

    # request.addfinalizer(fin)

    return Server(tmp_dir, uid, gid, version, host, port, volume)


@pytest.fixture
def helper(server):
    return NFSHelperProxy(
        server.host,
        server.port,
        server.volume,
        server.uid,
        server.gid,
        server.version)
