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
        def __init__(self, scheme, hostname, bucket, access_key, secret_key):
            [ip, port] = hostname.split(':')
            self.scheme = scheme
            self.hostname = hostname
            self.access_key = access_key
            self.secret_key = secret_key
            self.bucket = bucket
            self.s3 = boto3.resource('s3', endpoint_url=scheme+"://"+hostname,
                                aws_access_key_id = self.access_key,
                                aws_secret_access_key = self.secret_key)

        def list(self, file_id):
            test_bucket = self.s3.Bucket(self.bucket)
            return [o.key for o in
                    test_bucket.objects.filter(Prefix=file_id + '/', Delimiter='/')]

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
    return S3HelperProxy(server.scheme, server.hostname, server.bucket,
                         server.access_key, server.secret_key, THREAD_NUMBER,
                         BLOCK_SIZE)
