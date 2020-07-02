"""This module tests XRootD helper."""

__author__ = "Bartek Kryza"
__copyright__ = """(C) 2020 ACK CYFRONET AGH,
This software is released under the MIT license cited in 'LICENSE.txt'."""

import os
import sys
import time
import subprocess
from os.path import expanduser
from urlparse import urlparse

import pytest

script_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.dirname(script_dir))
# noinspection PyUnresolvedReferences
from test_common import *
# noinspection PyUnresolvedReferences
from environment import common, docker, xrootd
from xrootd_helper import XRootDHelperProxy

#
# Import test cases selectively, the commented out tests indicate
# test cases which will not work on XRootD helper
#
from common_test_base import \
    file_id, \
    test_write_should_write_empty_data, \
    test_write_should_write_data, \
    test_write_should_append_data, \
    test_write_should_prepend_data, \
    test_write_should_merge_data, \
    test_write_should_overwrite_data_left, \
    test_write_should_overwrite_data_right, \
    test_write_should_overwrite_data_middle, \
    test_read_shoud_not_read_data, \
    test_read_should_read_data, \
    test_read_should_read_all_possible_ranges, \
    test_read_should_pad_prefix_with_zeros, \
    test_read_should_read_data_with_holes, \
    test_read_should_read_empty_segment, \
    test_unlink_should_delete_empty_data, \
    test_truncate_should_increase_file_size, \
    test_truncate_should_decrease_file_size


from io_perf_test_base import \
    test_write, \
    test_write_read
    # test_read_write_truncate_unlink
    # test_truncate

from posix_test_base import \
    test_read_should_read_written_data, \
    test_read_should_error_file_not_found, \
    test_mkdir_should_create_directory, \
    test_rename_directory_should_rename, \
    test_readdir_should_list_files_in_directory, \
    test_unlink_should_pass_errors, \
    test_unlink_should_delete_file, \
    test_mknod_should_create_regular_file_by_default, \
    test_chown_should_change_user_and_group, \
    test_read_should_not_read_after_end_of_file, \
    test_read_write_large_file_should_maintain_consistency
    # test_symlink_should_create_link
    # test_link_should_create_hard_link
    # test_truncate_should_not_create_file
    # test_mknod_should_set_premissions


@pytest.fixture(scope='module')
def server(request):
    class Server(object):
        def __init__(self, url):
            self.url = url

    result = xrootd.up('onedata/xrootd:v1', 'storage',
                       common.generate_uid())

    [container] = result['docker_ids']
    url = result['url'].encode('ascii')

    def fin():
        docker.remove([container], force=True, volumes=True)

    request.addfinalizer(fin)

    time.sleep(2)

    return Server(url)


@pytest.fixture
def helper(server):
    return XRootDHelperProxy(server.url)


@pytest.mark.directory_operations_tests
def test_rmdir_should_remove_directory(helper, file_id):
    dir_id = file_id
    file1_id = random_str()
    file2_id = random_str()
    data = random_str()
    offset = random_int()

    try:
        helper.mkdir(dir_id, 0777)
        helper.write(dir_id+"/"+file1_id, data, offset)
        helper.write(dir_id+"/"+file2_id, data, offset)
    except:
        pytest.fail("Couldn't create directory: %s"%(dir_id))

    helper.unlink(dir_id+"/"+file1_id, 0)
    helper.unlink(dir_id+"/"+file2_id, 0)

    with pytest.raises(RuntimeError) as excinfo:
        helper.read(dir_id+"/"+file1_id, offset, len(data))
    assert 'No such file or directory' in str(excinfo.value)

    helper.rmdir(dir_id)

    with pytest.raises(RuntimeError) as excinfo:
        helper.readdir(dir_id, 0, 1024)
    assert 'No such file or directory' in str(excinfo.value)


def test_readdir_should_handle_offset_properly(helper):
    def to_python_list(readdir_result):
        return [str(e) for e in readdir_result]

    test_dir = 'offset_test'

    helper.mkdir(test_dir, 0777)

    files = ['file{}.txt'.format(i,) for i in (1, 2, 3, 4, 5)]

    for file in files:
        helper.write(test_dir+'/'+file, random_str(), 0)

    dirs = to_python_list(helper.readdir(test_dir, 0, 100))
    assert len(dirs) == len(files)

    files_in_xrootd_order = dirs

    dirs = to_python_list(helper.readdir(test_dir, 0, 1))
    assert dirs == files_in_xrootd_order[0:1]

    dirs = to_python_list(helper.readdir(test_dir, 0, 2))
    assert dirs == files_in_xrootd_order[0:2]

    dirs = to_python_list(helper.readdir(test_dir, 3, 100))
    assert dirs == files_in_xrootd_order[3:5]

    dirs = to_python_list(helper.readdir(test_dir, 100, 100))
    assert dirs == []

    dirs = to_python_list(helper.readdir(test_dir, 0, 0))
    assert dirs == []
