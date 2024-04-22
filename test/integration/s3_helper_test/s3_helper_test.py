"""This module tests S3 helper."""

__author__ = "Krzysztof Trzepla"
__copyright__ = """(C) 2016 ACK CYFRONET AGH,
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
from key_value_test_base import *
from s3_helper import S3HelperProxy
from io_perf_test_base import *
from posix_test_types import *


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
            self.s3 = boto3.resource('s3', endpoint_url=self.scheme+"://"+self.hostname,
                                aws_access_key_id = self.access_key,
                                aws_secret_access_key = self.secret_key)

        def list(self, file_id):
            test_bucket = self.s3.Bucket(self.bucket)
            return [o.key for o in
                    test_bucket.objects.filter(
                        Prefix=os.path.join(self.prefix, file_id) + '/',
                        Delimiter='/')]

        def set_bucket(self, bucket_name):
            self.bucket = bucket_name

        def make_bucket(self, new_bucket):
            self.s3.create_bucket(Bucket=new_bucket)

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
                         BLOCK_SIZE, "flat", 20)


@pytest.fixture
def helper_invalid(server):
    return S3HelperProxy(server.scheme, server.hostname, "no_such_bucket",
                         server.access_key, server.secret_key, THREAD_NUMBER,
                         BLOCK_SIZE, "flat", 2)


@pytest.fixture
def helper_invalid_host(server):
    return S3HelperProxy(server.scheme, "no_such_host.invalid:80888", "no_such_bucket",
                         server.access_key, server.secret_key, THREAD_NUMBER,
                         BLOCK_SIZE, "flat", 2)


@pytest.fixture
def helper_multipart(server):
    return S3HelperProxy(server.scheme, server.hostname, server.bucket+server.prefix,
                         server.access_key, server.secret_key, THREAD_NUMBER,
                         6*1024*1024, "flat", 20)


def test_helper_check_availability(helper):
    helper.check_storage_availability()


def test_helper_check_availability_error_invalid_bucket(helper_invalid):
    with pytest.raises(RuntimeError) as excinfo:
        helper_invalid.check_storage_availability()

    assert 'No such file or directory' in str(excinfo)


def test_helper_check_availability_error_invalid_host(helper_invalid_host):
    with pytest.raises(RuntimeError) as excinfo:
        helper_invalid_host.check_storage_availability()

    assert "Couldn't resolve host name" in str(excinfo)


def test_parameters_update_new_bucket(helper, server, file_id):
    block_num = 20
    seed = random_str(BLOCK_SIZE)
    data = seed * block_num

    assert helper.write(file_id, data, 0) == len(data)
    assert helper.read(file_id, 0, len(data)).decode('utf-8') == data
    assert len(server.list(file_id)) == block_num

    server.make_bucket('data2')
    server.set_bucket('data2')

    helper.update_helper(server.scheme, server.hostname, 'data2',
                        server.access_key, server.secret_key, THREAD_NUMBER,
                        BLOCK_SIZE, 'flat')

    assert len(server.list(file_id)) == 0
    assert helper.write(file_id, data, 0) == len(data)
    assert helper.read(file_id, 0, len(data)).decode('utf-8') == data
    assert len(server.list(file_id)) == block_num

    helper.unlink(file_id, len(data))
    assert len(server.list(file_id)) == 0

    server.set_bucket('data')

    assert len(server.list(file_id)) == block_num


def test_helper_params(helper):
    assert helper.block_size() == BLOCK_SIZE
    assert helper.block_size_for_path("/any") == BLOCK_SIZE
    assert helper.is_object_storage() == True
    assert helper.storage_path_type() == "flat"