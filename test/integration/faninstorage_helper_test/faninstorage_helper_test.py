"""This module tests fan in POSIX helper."""

__author__ = "Bartek Kryza"
__copyright__ = """(C) 2021 ACK CYFRONET AGH,
This software is released under the MIT license cited in 'LICENSE.txt'."""

import os
import sys
import random
import subprocess
from os.path import expanduser
import pprint

import pytest

script_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.dirname(script_dir))
# noinspection PyUnresolvedReferences
from test_common import *
# noinspection PyUnresolvedReferences
from environment import common, docker, nfs
from faninstorage_helper import FanInStorageHelperProxy
from posix_test_types import *

content = 'TEST'


def make_test_directory_tree(mountpoints):
    branches = [
        ('dir1.1', 'dir2.1', 'dir3.1', 'dir4.1'),
        ('dir1.2', 'dir2.2', 'dir3.2', 'dir4.2'),
        ('dir1.3', 'dir2.3', 'dir3.3'),
        ('dir1.4', 'dir2.4', 'dir3.4', 'dir4.4', 'dir5.4')
    ]

    for branch in branches:
        for mp in mountpoints:
            os.system("mkdir -p %s"%(os.path.join(mp, *branch)))

    # Create N files in each sub directory including root
    for n in range(1000):
        p = os.path.join(random.choice(mountpoints), 'file%s.txt'%(n))
        with open(p, 'w') as f:
            f.write(content)

    for branch in branches:
        for i in range(len(branch)):
            if i==0:
                continue
            for n in range(1000):
                toks = [random.choice(mountpoints)]
                toks.extend(branch[0:i])
                toks.append('file%s.txt'%(n))
                p = os.path.join(*toks)
                with open(p, 'w') as f:
                    f.write(content)


@pytest.fixture(scope='module')
def server(request):
    class Server(object):
        def __init__(self, mountpoints, uid, gid):
            self.mountpoints = mountpoints
            self.uid = uid
            self.gid = gid

    tmp = expanduser("/tmp/")
    mountpoint_base = os.path.join(tmp, 'onedata', 'faninstorage_helper_test')

    assert os.system("mkdir -p %s"%(mountpoint_base)) == 0

    mountpoints = [os.path.join(mountpoint_base, 'mnt', 'volume%s'%(str(i))) for i in range(3)]
    make_test_directory_tree(mountpoints)

    def fin():
        os.system("rm -rf %s"%(mountpoint_base))

    request.addfinalizer(fin)

    return Server(":".join(mountpoints), os.geteuid(), os.getegid())


@pytest.fixture
def helper(server):
    return FanInStorageHelperProxy(
        server.mountpoints,
        server.uid,
        server.gid)


@pytest.mark.readwrite_operations_tests
def test_getattr_should_work_on_files(helper):
    assert Flag.IFREG in maskToFlags(
        helper.getattr(os.path.join('dir1.1', 'file1.txt')).st_mode)


@pytest.mark.directory_operations_tests
def test_getattr_should_work_on_dirs(helper):
    assert Flag.IFREG not in maskToFlags(
        helper.getattr(os.path.join('dir1.1', 'dir2.1')).st_mode)


@pytest.mark.readwrite_operations_tests
def test_read_should_read_data(helper):
    assert helper.read(os.path.join('dir1.1', 'file1.txt'), 0, len(content)) == content


@pytest.mark.readwrite_operations_tests
def test_read_should_read_data(helper):
    assert helper.read(os.path.join('dir1.1', 'file1.txt'), 0, len(content)) == content


@pytest.mark.directory_operations_tests
@pytest.mark.parametrize(
    "dir_id",
    [
        '/',
        '/dir1.1',
        '/dir1.1/',
        'dir1.1',
        'dir1.1/',
        '/dir1.1/dir2.1/dir3.1'
    ],
)
def test_readdir_should_list_files_in_directory(helper, dir_id):

    dirs = list(helper.readdir(dir_id, 0, 5000))

    assert 'file1.txt' in dirs
    assert 'file100.txt' in dirs
    assert 'file999.txt' in dirs

    dirs_paging = []
    offset = 0
    step = 100
    while(len(dirs_paging) < len(dirs)):
        dirs_paging.extend(helper.readdir(dir_id, offset, step))
        offset += step

    assert dirs == dirs_paging