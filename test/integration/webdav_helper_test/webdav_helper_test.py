"""This module tests WebDAV helper."""

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

script_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.dirname(script_dir))
# noinspection PyUnresolvedReferences
from test_common import *
# noinspection PyUnresolvedReferences
from environment import common, docker, webdav
from webdav_helper import WebDAVHelperProxy

#
# Import test cases selectively, the commented out tests indicate
# test cases which will not work on WebDAV helper
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
    test_read_should_not_read_after_end_of_file, \
    test_read_should_read_empty_segment, \
    test_unlink_should_delete_empty_data, \
    test_truncate_should_increase_file_size
    # test_truncate_should_decrease_file_size


from io_perf_test_base import \
    test_write, \
    test_write_read, \
    test_read_write_truncate_unlink
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
    test_read_write_large_file_should_maintain_consistency
    # test_symlink_should_create_link
    # test_link_should_create_hard_link
    # test_mknod_should_set_premissions
    # test_truncate_should_not_create_file

from xattr_test_base import \
    test_setxattr_should_set_extended_attribute, \
    test_setxattr_should_set_large_extended_attribute, \
    test_setxattr_should_set_extended_attribute_with_empty_value, \
    test_getxattr_should_return_extended_attribute, \
    test_listxattr_should_list_extended_attribute
    # test_removexattr_should_remove_extended_attribute, \
    # test_setxattr_should_handle_create_replace_flags


@pytest.fixture(scope='module')
def server(request):
    class Server(object):
        def __init__(self, endpoint, credentials):
            self.endpoint = endpoint
            self.credentials = credentials

    result = webdav.up('onedata/sabredav:v2', 'storage',
                       common.generate_uid())

    [container] = result['docker_ids']
    credentials = result['credentials'].encode('ascii')
    endpoint = result['endpoint'].encode('ascii')

    def fin():
        docker.remove([container], force=True, volumes=True)

    request.addfinalizer(fin)

    time.sleep(15)

    return Server(endpoint, credentials)


@pytest.fixture
def helper(server):
    return WebDAVHelperProxy(server.endpoint, server.credentials)


@pytest.fixture
def helper_redirect(server):
    redirect_port = "8080"
    endpoint = urlparse(server.endpoint)
    redirect_url = endpoint._replace(
        netloc=endpoint.netloc.replace(
            str(endpoint.port), redirect_port)).geturl()

    return WebDAVHelperProxy(redirect_url, server.credentials)

def test_read_should_follow_temporary_redirect(helper, helper_redirect, file_id):
    data = random_str()
    helper.write(file_id, data, 0)
    data2 = helper_redirect.read(file_id, 0, len(data))

    assert data == data2

@pytest.mark.directory_operations_tests
def test_mknod_should_return_enoent_on_missing_parent(helper, file_id):
    dir1_id = random_str()
    dir2_id = random_str()
    dir3_id = random_str()
    data = random_str()
    offset = random_int()

    with pytest.raises(RuntimeError) as excinfo:
        helper.write(dir1_id+"/"+dir2_id+"/"+dir3_id+"/"+file_id,
                data, offset)

    assert 'No such file or directory' in str(excinfo.value)


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


def test_getattr_should_return_default_permissions(helper, file_id):
    dir_id = file_id
    data = random_str()
    offset = random_int()
    default_dir_mode = 0775
    default_file_mode = 0644

    try:
        helper.mkdir(dir_id, 0777)
        helper.write(dir_id+"/"+file_id, data, offset)
    except:
        pytest.fail("Couldn't create directory: %s"%(dir_id))

    # WebDAV doesn't store permissions, so the dir_id directory will
    # return the permissions defined in the helper not the ones used
    # in mkdir call
    assert helper.getattr(dir_id).st_mode&0777 == default_dir_mode
    assert helper.getattr(dir_id+"/"+file_id).st_mode&0777 == default_file_mode
