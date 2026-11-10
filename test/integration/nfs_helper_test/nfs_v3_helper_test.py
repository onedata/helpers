"""This module tests NFS v3 helper."""

__author__ = "Bartek Kryza"
__copyright__ = """(C) 2021 ACK CYFRONET AGH,
This software is released under the MIT license cited in 'LICENSE.txt'."""

import os
import sys
import tempfile

import pytest

script_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.dirname(script_dir))

from nfs_common import *

@pytest.fixture(scope='module')
def server(request):
    return create_server(request, 3, '/nfsshare')

@pytest.mark.links_operations_tests
def test_symlink_should_create_link(helper, mountpoint, file_id):
    do_test_symlink_should_create_link(helper, mountpoint, file_id)
