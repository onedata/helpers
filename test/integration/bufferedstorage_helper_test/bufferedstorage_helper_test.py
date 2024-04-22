"""This module tests BufferedStorage helper."""

__author__ = "Bartek Kryza"
__copyright__ = """(C) 2021 ACK CYFRONET AGH,
This software is released under the MIT license cited in 'LICENSE.txt'."""

import os
import sys
import subprocess
from os.path import expanduser

import boto3
import pytest

script_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.dirname(script_dir))
# noinspection PyUnresolvedReferences
from test_common import *
# noinspection PyUnresolvedReferences
from environment import common, docker, s3
from boto.s3.connection import S3Connection, OrdinaryCallingFormat
from bufferedstorage_helper import BufferedStorageHelperProxy
from posix_test_types import *

import stat

THREAD_NUMBER = 8
BLOCK_SIZE = 6*1000*1000

def to_python_list(listobjects_result):
    r = [e for e in listobjects_result]
    r.sort(key=lambda x: x[0])
    return r

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
                    test_bucket.objects.all()]

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
    return BufferedStorageHelperProxy(server.scheme, server.hostname,
            server.bucket+server.prefix, server.access_key, server.secret_key,
            THREAD_NUMBER, BLOCK_SIZE)


@pytest.mark.readwrite_operations_tests
def test_read_should_read_written_data(helper, server):
    data = 'x'*int(2*BLOCK_SIZE+BLOCK_SIZE/2)
    offset = 0

    file_id = '/space1/' + random_str()

    assert helper.write(file_id, data, offset) == len(data)
    assert len(server.list('/')) == 3

    helper.flushBuffer(file_id, len(data))

    assert len(server.list('/')) == 1

    assert helper.read(file_id, offset, len(data)) == data

    helper.unlink(file_id, len(data))

    assert len(server.list('/')) == 0


@pytest.mark.readwrite_operations_tests
def test_write_should_write_from_nonzero_offset(helper, server):

    offset = BLOCK_SIZE # + BLOCK_SIZE/2
    data = '\0'*offset + 'x'*(BLOCK_SIZE)

    assert len(data) == 2*BLOCK_SIZE# + BLOCK_SIZE/2

    file_id = '/space1/' + random_str()

    assert helper.write(file_id, 'x'*(BLOCK_SIZE), offset) \
            == BLOCK_SIZE

    assert len(server.list('/')) == 1

    helper.flushBuffer(file_id, len(data))

    assert len(server.list('/')) == 1

    assert helper.read(file_id, 0, len(data)) == data

    helper.unlink(file_id, len(data))

    assert len(server.list('/')) == 0


@pytest.mark.readwrite_operations_tests
def test_write_should_modify_data_on_main_storage(helper, server):
    data = 'x'*int(2*BLOCK_SIZE+BLOCK_SIZE/2)
    offset = 0

    file_id = '/space1/' + random_str()

    assert helper.write(file_id, data, offset) == len(data)

    helper.flushBuffer(file_id, len(data))

    data2 = data[:BLOCK_SIZE-2] + 'yyy' + data[BLOCK_SIZE+1:]

    assert len(data) == len(data2)

    assert helper.write(file_id, 'yyy', BLOCK_SIZE-2) == 3

    assert len(server.list('/')) == 2+1 # 2 modified blocks in buffer + original file

    helper.flushBuffer(file_id, len(data2))

    assert helper.read(file_id, 0, len(data2)) == data2

    assert len(server.list('/')) == 1

    helper.unlink(file_id, len(data))

    assert len(server.list('/')) == 0


@pytest.mark.directory_operations_tests
def test_readdir_should_list_files_in_directory(helper):
    dir_id = "/space1/" + random_str()
    file1_id = random_str()
    file2_id = random_str()
    data = random_str()
    offset = random_int()

    try:
        helper.mkdir(dir_id, 0o777)
    except:
        pytest.fail("Couldn't create directory: %s"%(dir_id))

    helper.write(dir_id+"/"+file1_id, data, offset)
    helper.flushBuffer(dir_id+"/"+file1_id, len(data)+offset)

    helper.write(dir_id+"/"+file2_id, data, offset)
    helper.flushBuffer(dir_id+"/"+file2_id, len(data)+offset)

    objects = to_python_list(helper.listobjects(dir_id, "", 1024))

    assert len(objects) == 2

    assert dir_id+"/"+file1_id in objects

    assert dir_id+"/"+file2_id in objects


def test_truncate_should_truncate_to_size(helper, server):
    file_id = "/space1/" + random_str()
    blocks_num = 4
    size = blocks_num * BLOCK_SIZE

    helper.truncate(file_id, size, 0)
    assert len(helper.read(file_id, 0, size + 1)) == len('\0' * size)
    assert helper.read(file_id, 0, size + 1) == '\0' * size


def test_truncate_should_pad_block(helper, server):
    file_id = "/space1/" + random_str()
    data = random_str()

    assert helper.write(file_id, data, BLOCK_SIZE) == len(data)
    helper.flushBuffer(file_id, BLOCK_SIZE+len(data))

    helper.truncate(file_id, BLOCK_SIZE, len(data)+BLOCK_SIZE)

    assert helper.read(file_id, 0, BLOCK_SIZE + 1) == '\0' * BLOCK_SIZE
    assert helper.write(file_id, data, BLOCK_SIZE) == len(data)


def test_truncate_should_truncate_to_zero(helper, server):
    file_id = "/space1/" + random_str()
    blocks_num = 4
    data = random_str(blocks_num * BLOCK_SIZE)

    assert helper.write(file_id, data, 0) == len(data)
    helper.flushBuffer(file_id, len(data))

    helper.truncate(file_id, 0, len(data))
    assert helper.getattr(file_id).st_size == 0


def test_mknod_should_create_empty_file(helper, server):
    file_id = "/space1/" + random_str()
    data = ''

    helper.mknod(file_id, 0o664, maskToFlags(stat.S_IFREG))
    helper.access(file_id)
    assert helper.getattr(file_id).st_size == 0


def test_mknod_should_throw_eexist_error(helper, server):
    file_id = "/space1/" + random_str()
    flags = maskToFlags(stat.S_IFREG)
    helper.mknod(file_id, 0o664, flags)

    with pytest.raises(RuntimeError) as excinfo:
        helper.mknod(file_id, 0o664, flags)

    assert 'File exists' in str(excinfo.value)


def test_unlink_should_delete_data(helper, server):
    file_id = "/space1/" + random_str()
    data = random_str()
    offset = random_int()

    assert helper.write(file_id, data, offset) == len(data)
    helper.flushBuffer(file_id, offset+len(data))
    helper.unlink(file_id, offset+len(data))


def test_unlink_should_delete_empty_file(helper, server):
    file_id = "/space1/" + random_str()
    data = random_str()
    offset = random_int()

    helper.mknod(file_id, 0o664, maskToFlags(stat.S_IFREG))
    helper.unlink(file_id, 0)

    with pytest.raises(RuntimeError) as excinfo:
        helper.getattr(file_id)

    assert 'Object not found' in str(excinfo.value)


def test_write_should_overwrite_multiple_blocks_part(helper):
    file_id = "/space1/" + random_str()
    block_num = 4
    updates_num = 5
    seed = random_str(BLOCK_SIZE)
    data = seed * block_num

    assert helper.write(file_id, data, 0) == len(data)
    for _ in range(updates_num):
        offset = random_int(lower_bound=0, upper_bound=len(data))
        block = random_str(BLOCK_SIZE)
        data = data[:offset] + block + data[offset + len(block):]
        helper.write(file_id, block, offset) == len(block)
        helper.flushBuffer(file_id, len(data))
        assert helper.read(file_id, 0, len(data)) == data


def test_read_should_read_multi_block_data_with_holes(helper):
    file_id = "/space1/" + random_str()
    data = random_str(10)
    empty_block = '\0' * BLOCK_SIZE
    block_num = 4

    assert helper.write(file_id, data, 0) == len(data)
    assert helper.write(file_id, data, block_num * BLOCK_SIZE) == len(data)
    helper.flushBuffer(file_id, len(data) + block_num * BLOCK_SIZE)

    data = data + empty_block[len(data):] + (block_num - 1) * empty_block + data
    assert helper.read(file_id, 0, len(data)) == data


def test_read_should_read_large_file_from_offset(helper):
    file_id = "/space1/" + random_str()
    data = 'A'*1000 + 'B'*1000 + 'C'*1000

    assert helper.write(file_id, data, 0) == len(data)
    helper.flushBuffer(file_id, len(data))

    assert helper.read(file_id, 0, 1000) == 'A'*1000
    assert helper.read(file_id, 1000, 1000) == 'B'*1000
    assert helper.read(file_id, 2000, 1000) == 'C'*1000


def test_write_should_fill_new_files_with_non_zero_offset(helper):
    file_id = "/space1/" + random_str()
    offset = 10
    data = 'A'*10
    new_object = '\0'*10 + 'A'*10
    helper.write(file_id, data, offset)
    assert helper.read(file_id, 0, len(data) + offset) == new_object

    helper.flushBuffer(file_id, len(data) + offset)

    assert helper.read(file_id, 0, 1000) == new_object


def test_helper_params(helper):
    assert helper.blockSize() == 6000000
    assert helper.storagePathType() == "flat"