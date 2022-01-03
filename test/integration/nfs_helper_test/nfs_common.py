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
from subprocess import check_output

from posix_test_base import \
    test_read_should_read_written_data, \
    test_read_should_return_empty_with_offset_beyond_range, \
    test_read_should_error_file_not_found, \
    test_mkdir_should_create_directory, \
    test_rename_directory_should_rename, \
    test_readdir_should_list_files_in_directory, \
    test_rmdir_should_remove_directory, \
    test_unlink_should_pass_errors, \
    test_unlink_should_delete_file, \
    test_link_should_create_hard_link, \
    test_mknod_should_set_premissions, \
    test_mknod_should_create_regular_file_by_default, \
    test_chown_should_change_user_and_group, \
    test_truncate_should_not_create_file, \
    test_read_write_large_file_should_maintain_consistency, \
    test_read_should_not_read_after_end_of_file


@pytest.fixture
def mountpoint(server):
    return server.tmp_dir


@pytest.fixture
def helper(server):
    return NFSHelperProxy(
        server.host,
        server.volume,
        server.uid,
        server.gid,
        server.version)


def create_server(request, version, volume):
    class Server(object):
        def __init__(self, tmp_dir, uid, gid, version, host, volume):
            self.tmp_dir = tmp_dir
            self.version = version
            self.uid = uid
            self.gid = gid
            self.host = host
            self.volume = volume

    uid = 0
    gid = 0

    tmp_dir = tempfile.mkdtemp(dir=common.HOST_STORAGE_PATH, prefix="nfs_helper_test_")
    os.chmod(tmp_dir, 0o777)

    result = nfs.up('onedata/nfs:v1', common.generate_uid(), 'storage', tmp_dir)

    time.sleep(3)

    [container] = result['docker_ids']
    host = result['host'].encode('ascii')

    def fin():
        docker.remove([container], force=True, volumes=True)
        check_output(['rm', '-rf', tmp_dir])
        time.sleep(1)

    request.addfinalizer(fin)

    return Server(tmp_dir, uid, gid, version, host, volume)


def do_test_symlink_should_create_link(helper, mountpoint, file_id):
    dir_id = random_str()
    data = random_str()

    try:
        helper.mkdir(dir_id, 0777)
        helper.write(dir_id+"/"+file_id, data, 0)
    except:
        pytest.fail("Couldn't create directory: %s"%(dir_id))

    helper.symlink(dir_id+"/"+file_id, file_id+".lnk")

    link = helper.readlink(file_id+".lnk")

    assert link == dir_id+"/"+file_id
    assert helper.read(link, 0, 1024) == data
