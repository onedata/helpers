"""This module tests S3 helper with canonical paths."""

__author__ = "Bartek Kryza"
__copyright__ = """(C) 2019 ACK CYFRONET AGH,
This software is released under the MIT license cited in 'LICENSE.txt'."""

import os
import sys

import boto3
import pytest

script_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.dirname(script_dir))
# noinspection PyUnresolvedReferences
from test_common import *
from environment import common, docker, s3
from boto.s3.connection import S3Connection, OrdinaryCallingFormat
from key_value_canonical_test_base import *
from s3_helper import S3HelperProxy
from io_perf_test_base import \
    test_write, \
    test_write_read, \
    test_read_write_truncate_unlink
from posix_test_base import \
    test_read_should_read_written_data, \
    test_read_should_error_file_not_found, \
    test_mkdir_should_create_directory, \
    test_unlink_should_pass_errors, \
    test_unlink_should_delete_file, \
    test_truncate_should_not_create_file, \
    test_read_should_not_read_after_end_of_file


@pytest.fixture(scope='module')
def server(request):
    class Server(object):
        def __init__(self, scheme, hostname, bucket, access_key, secret_key, prefix = ""):
            [ip, port] = hostname.split(':')
            self.scheme = scheme
            self.hostname = hostname
            self.access_key = access_key
            self.secret_key = secret_key
            self.bucket = bucket
            self.prefix = prefix
            self.s3 = boto3.resource('s3', endpoint_url=scheme+"://"+hostname,
                                aws_access_key_id = self.access_key,
                                aws_secret_access_key = self.secret_key)

        def list(self, file_id):
            test_bucket = self.s3.Bucket(self.bucket)
            return [o.key for o in
                    test_bucket.objects.filter(Prefix=os.path.join(self.prefix, file_id) + '/', Delimiter='/')]

    bucket = 'data'
    result = s3.up('onedata/minio:v1', [bucket], 'storage',
                   common.generate_uid())
    [container] = result['docker_ids']

    def fin():
        docker.remove([container], force=True, volumes=True)

    request.addfinalizer(fin)

    return Server('http', result['host_name'], bucket, result['access_key'],
                  result['secret_key'])


@pytest.fixture
def helper(server):
    return S3HelperProxy(server.scheme, server.hostname, server.bucket+server.prefix,
                         server.access_key, server.secret_key, THREAD_NUMBER,
                         0, "canonical", 20)


def truncate_test(helper, op_num, size):
    """
    On canonical S3, read should return only the existing bytes without padding
    with zeros to the requested size.
    """
    for _ in range(op_num):
        file_id = random_str()

        helper.write(file_id, 'X'*size, 0)
        assert helper.read(file_id, 0, size).decode('utf-8') == 'X'*size
        helper.truncate(file_id, 1, size)
        assert helper.read(file_id, 0, size).decode('utf-8') == 'X'


def test_read_should_throw_for_write_beyond_supported_range(helper, file_id):
    max_range = 2 * 1024 * 1024
    data = random_str()

    with pytest.raises(RuntimeError) as excinfo:
        helper.write(file_id, data, max_range+1)

    assert 'Numerical result out of range' in str(excinfo.value)


def test_helper_params(helper):
    assert helper.block_size() == 0
    assert helper.block_size_for_path("/any") == 0
    assert helper.storage_path_type() == "canonical"