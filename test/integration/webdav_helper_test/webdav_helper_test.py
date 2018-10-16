"""This module tests WebDAV helper."""

__author__ = "Bartek Kryza"
__copyright__ = """(C) 2018 ACK CYFRONET AGH,
This software is released under the MIT license cited in 'LICENSE.txt'."""

import os
import sys
import time
import subprocess
from os.path import expanduser

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
    test_rmdir_should_remove_directory, \
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

    result = webdav.up('onedata/sabredav', 'storage',
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
