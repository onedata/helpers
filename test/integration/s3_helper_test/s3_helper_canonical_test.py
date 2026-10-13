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


@pytest.fixture
def aws_argo_gdac_public_helper():
    """Fixture for testing with a public AWS S3 bucket via the global AWS endpoint.

    The bucket 'argo-gdac-sandbox' resides in eu-west-3. When accessed via the
    global endpoint 's3.amazonaws.com', AWS returns a 301 Permanent Redirect to
    the regional endpoint. This fixture verifies that such redirects are handled
    correctly by the S3 helper.
    """
    return S3HelperProxy(
        "https",
        "s3.amazonaws.com",
        "argo-gdac-sandbox",
        "",
        "",
        THREAD_NUMBER,
        0,
        "canonical",
        30,
        "")  # no explicit region — will be discovered via HeadBucket probe


@pytest.mark.public_aws_bucket
def test_list_objects_on_public_aws_bucket_via_global_endpoint(
        aws_argo_gdac_public_helper):
    """
    Tests that a public AWS S3 bucket (argo-gdac-sandbox, located in eu-west-3)
    registered using the global AWS endpoint (s3.amazonaws.com) can be accessed
    and objects can be listed successfully. AWS returns a 301 Permanent Redirect
    for buckets not in the default region — the S3 helper must follow it.
    """
    _marker, objects = aws_argo_gdac_public_helper.listobjects("", "", 5)

    assert len(objects) > 0, \
        "Expected to list at least one object from public bucket " \
        "'argo-gdac-sandbox' via global AWS endpoint 's3.amazonaws.com'"


@pytest.fixture
def aws_argo_gdac_public_helper_with_region():
    """Fixture for testing the optimised path where the bucket region is
    provided explicitly alongside the global AWS endpoint.  The helper must
    derive s3.eu-west-3.amazonaws.com directly without any HeadBucket probe.
    """
    return S3HelperProxy(
        "https",
        "s3.amazonaws.com",
        "argo-gdac-sandbox",
        "",
        "",
        THREAD_NUMBER,
        0,
        "canonical",
        30,
        "eu-west-3")


@pytest.mark.public_aws_bucket
def test_list_objects_on_public_aws_bucket_via_global_endpoint_with_explicit_region(
        aws_argo_gdac_public_helper_with_region):
    """
    Tests the optimised initialisation path: when the bucket region is supplied
    explicitly (eu-west-3) together with the global AWS endpoint
    (s3.amazonaws.com), the S3 helper must build the regional endpoint
    s3.eu-west-3.amazonaws.com directly — without a HeadBucket probe — and
    successfully list objects.
    """
    _marker, objects = aws_argo_gdac_public_helper_with_region.listobjects(
        "", "", 5)

    assert len(objects) > 0, \
        "Expected to list at least one object from public bucket " \
        "'argo-gdac-sandbox' via global AWS endpoint with explicit region " \
        "'eu-west-3'"


@pytest.fixture
def aws_argo_gdac_public_helper_with_scheme_in_hostname():
    """Fixture that passes the scheme as part of the hostname string
    (https://s3.amazonaws.com) to verify that the params layer strips the
    prefix before the helper processes it.
    """
    return S3HelperProxy(
        "https",
        "https://s3.amazonaws.com",
        "argo-gdac-sandbox",
        "",
        "",
        THREAD_NUMBER,
        0,
        "canonical",
        30,
        "eu-west-3")


@pytest.mark.public_aws_bucket
def test_list_objects_on_public_aws_bucket_with_scheme_prefix_in_hostname(
        aws_argo_gdac_public_helper_with_scheme_in_hostname):
    """
    Tests that passing 'https://s3.amazonaws.com' as the hostname (with the
    scheme prefix included) works correctly.  The params layer must strip the
    prefix so that downstream code sees a bare 's3.amazonaws.com', allowing
    both isGlobalAwsEndpoint detection and endpointOverride to function as
    expected.
    """
    _marker, objects = \
        aws_argo_gdac_public_helper_with_scheme_in_hostname.listobjects(
            "", "", 5)

    assert len(objects) > 0, \
        "Expected to list at least one object from public bucket " \
        "'argo-gdac-sandbox' when hostname contains scheme prefix " \
        "'https://s3.amazonaws.com'"
