"""This module tests CephRados helper."""

__author__ = "Bartek Kryza"
__copyright__ = """(C) 2018 ACK CYFRONET AGH,
This software is released under the MIT license cited in 'LICENSE.txt'."""

import os
import sys
import threading
import random
from Queue import Queue, Empty

import pytest

script_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.dirname(script_dir))
# noinspection PyUnresolvedReferences
from test_common import *
from environment import common, docker, ceph
from cephrados_helper import CephRadosHelperProxy
from key_value_test_base import *
from io_perf_test_base import *

VALIDATE_PATTERN = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"*20

@pytest.fixture(scope='module')
def server(request):
    class Server(object):
        def __init__(self, mon_host, username, key, pool_name):
            self.mon_host = mon_host
            self.username = username
            self.key = key
            self.pool_name = pool_name
            self.container = None

        def list(self, file_id):
            # The only way to list objects in Ceph which start with prefix 'file_id'
            # is to list all objects and then grep through the results.
            output = docker.exec_(self.container,
                    ['bash', '-c', "rados -p {} ls | grep {} || true".format(
                        self.pool_name, file_id)], output=True, stdout=sys.stdout)
            return output.splitlines()

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

    server = Server(mon_host, username, key, pool_name)
    server.container = container

    return  server

@pytest.fixture
def helper(server):
    return CephRadosHelperProxy(server.mon_host, server.username, server.key,
                           server.pool_name, THREAD_NUMBER, BLOCK_SIZE, "flat")


def read_and_validate_block(h, results, file_id, iteration_count, offset_range):
    for _ in range(iteration_count):
        offset = random.randint(0, offset_range)
        moff = offset % len(VALIDATE_PATTERN)
        block = h.read(file_id, offset, len(VALIDATE_PATTERN))
        if block != VALIDATE_PATTERN[moff:]+VALIDATE_PATTERN[:moff]:
            results.put(block + "!=" + VALIDATE_PATTERN[moff:]+VALIDATE_PATTERN[:moff])
            return


def test_multithread_read_should_work(helper, file_id):
    blocks_num = 1024
    data = VALIDATE_PATTERN
    threads_num = 8
    iteration_count = 1000
    threads = []
    results = Queue(threads_num)

    # Prepare the test file
    for i in range(blocks_num):
        assert helper.write(file_id, data, i*len(data)) == len(data)

    for _ in range(threads_num):
        t = threading.Thread(target=read_and_validate_block,
                args=(helper, results, file_id, iteration_count,
                      len(data)*(blocks_num-1)))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    while True:
        try:
            res = results.get(block=False)
            assert res == ""
        except Empty:
            break
