"""This module contains test for KeyValue helpers with canonical paths
   and blockSize set to 0."""

__author__ = "Bartek Kryza"
__copyright__ = """(C) 2019 ACK CYFRONET AGH,
This software is released under the MIT license cited in 'LICENSE.txt'."""

from test_common import *
from common_test_base import *
from posix_test_types import *

import stat

import pytest

THREAD_NUMBER = 8
BLOCK_SIZE = 1024


def to_python_list(listobjects_result):
    r = [e for e in listobjects_result]
    r.sort(key=lambda x: x[0])
    return r


def test_mknod_should_create_empty_file(helper, file_id, server):
    data = ''

    helper.mknod(file_id, 0o664, maskToFlags(stat.S_IFREG))
    helper.access(file_id)
    assert helper.getattr(file_id).st_size == 0

def test_mknod_should_throw_eexist_error(helper, file_id, server):
    flags = maskToFlags(stat.S_IFREG)
    helper.mknod(file_id, 0o664, flags)

    with pytest.raises(RuntimeError) as excinfo:
        helper.mknod(file_id, 0o664, flags)

    assert 'File exists' in str(excinfo.value)

def test_write_should_write_multiple_blocks(helper, file_id, server):
    block_num = 20
    seed = random_str(BLOCK_SIZE)
    data = seed * block_num

    assert helper.write(file_id, data, 0) == len(data)
    assert helper.read(file_id, 0, len(data)).decode('utf-8') == data


def test_unlink_should_delete_data(helper, file_id, server):
    data = random_str()
    offset = random_int()
    file_id2 = random_str()

    assert helper.write(file_id, data, offset) == len(data)
    helper.unlink(file_id, offset+len(data))

    assert helper.write(file_id2, data, offset) == len(data)
    helper.unlink('/'+file_id2, offset+len(data))


def test_unlink_should_delete_empty_file(helper, file_id, server):
    data = random_str()
    offset = random_int()
    file_id2 = random_str()

    helper.mknod(file_id, 0o664, maskToFlags(stat.S_IFREG))
    helper.unlink(file_id, 0)

    with pytest.raises(RuntimeError) as excinfo:
        helper.getattr(file_id)

    assert 'Object not found' in str(excinfo.value)

def test_truncate_should_truncate_to_size(helper, file_id, server):
    blocks_num = 10
    size = blocks_num * BLOCK_SIZE

    helper.mknod(file_id, 0o654, maskToFlags(stat.S_IFREG))
    helper.truncate(file_id, size, 0)
    assert len(helper.read(file_id, 0, size + 1)) == len('\0' * size)
    assert helper.read(file_id, 0, size + 1).decode('utf-8') == '\0' * size


def test_truncate_should_pad_block(helper, file_id, server):
    data = random_str()

    assert helper.write(file_id, data, BLOCK_SIZE) == len(data)
    helper.truncate(file_id, BLOCK_SIZE, len(data)+BLOCK_SIZE)
    assert helper.read(file_id, 0, BLOCK_SIZE + 1).decode('utf-8') == '\0' * BLOCK_SIZE
    assert helper.write(file_id, data, BLOCK_SIZE) == len(data)


def test_truncate_should_truncate_to_zero(helper, file_id, server):
    blocks_num = 10
    data = random_str(blocks_num * BLOCK_SIZE)

    assert helper.write(file_id, data, 0) == len(data)
    helper.truncate(file_id, 0, len(data))
    assert helper.getattr(file_id).st_size == 0


def test_write_should_overwrite_multiple_blocks_part(helper, file_id):
    block_num = 10
    updates_num = 100
    seed = random_str(BLOCK_SIZE)
    data = seed * block_num

    assert helper.write(file_id, data, 0) == len(data)
    for _ in range(updates_num):
        offset = random_int(lower_bound=0, upper_bound=len(data))
        block = random_str(BLOCK_SIZE)
        data = data[:offset] + block + data[offset + len(block):]
        helper.write(file_id, block, offset) == len(block)
        assert helper.read(file_id, 0, len(data)).decode('utf-8') == data


def test_read_should_read_multi_block_data_with_holes(helper, file_id):
    data = random_str(10)
    empty_block = '\0' * BLOCK_SIZE
    block_num = 10

    assert helper.write(file_id, data, 0) == len(data)
    assert helper.write(file_id, data, block_num * BLOCK_SIZE) == len(data)

    data = data + empty_block[len(data):] + (block_num - 1) * empty_block + data
    assert helper.read(file_id, 0, len(data)).decode('utf-8') == data


def test_read_should_read_large_file_from_offset(helper, file_id):
    data = 'A'*1000 + 'B'*1000 + 'C'*1000

    assert helper.write(file_id, data, 0) == len(data)

    assert helper.read(file_id, 0, 1000).decode('utf-8') == 'A'*1000
    assert helper.read(file_id, 1000, 1000).decode('utf-8') == 'B'*1000
    assert helper.read(file_id, 2000, 1000).decode('utf-8') == 'C'*1000


def test_write_should_modify_inner_object_on_canonical_storage(helper):
    # originalObject: [----------------------------------------]
    # buf           :       [----------]

    file_id = random_str()
    original_object = 'A'*10 + 'B'*10 + 'C'*10
    helper.write(file_id, original_object, 0)
    helper.write(file_id, 'X'*10, 10)
    new_object = 'A'*10 + 'X'*10 + 'C'*10

    assert helper.read(file_id, 0, 1000).decode('utf-8') == new_object


def test_write_should_modify_non_overlapping_object_on_canonical_storage(helper):
    # originalObject: [-------]
    # buf           :                       [------------------]

    file_id = random_str()
    original_object = 'A'*10
    helper.write(file_id, original_object, 0)
    helper.write(file_id, 'X'*10, 20)
    new_object = 'A'*10 + '\0'*10 + 'X'*10

    assert helper.read(file_id, 0, 1000).decode('utf-8') == new_object


def test_write_should_modify_overlapping_object_on_canonical_storage(helper):
    # originalObject: [-------------------------]
    # buf           :                       [------------------]

    file_id = random_str()
    original_object = 'A'*10 + 'B'*10 + 'C'*10
    helper.write(file_id, original_object, 0)
    helper.write(file_id, 'X'*10, 25)
    new_object = 'A'*10 + 'B'*10 + 'C'*5 + 'X'*10

    assert helper.read(file_id, 0, 1000).decode('utf-8') == new_object


def test_write_should_fill_new_files_with_non_zero_offset(helper):
    # originalObject: []
    # buf           :             [------------------]

    file_id = random_str()
    original_object = 'A'*10
    helper.write(file_id, original_object, 10)
    new_object = '\0'*10 + 'A'*10

    assert helper.read(file_id, 0, 1000).decode('utf-8') == new_object


def test_getattr_should_return_default_permissions(helper):
    dir_id = random_str()
    data = random_str()
    default_dir_mode = 0o775
    default_file_mode = 0o664
    file_id = random_str()
    file_id2 = random_str()

    try:
        helper.write(dir_id+'/'+file_id, data, 0)
        helper.write('/'+dir_id+'/'+file_id2, data, 0)
    except:
        pytest.fail("Couldn't create directory: %s"%(dir_id))

    assert oct(helper.getattr('').st_mode & 0o777) == oct(default_dir_mode)
    assert oct(helper.getattr('/').st_mode & 0o777) == oct(default_dir_mode)

    assert oct(helper.getattr(dir_id+'/'+file_id).st_mode & 0o777) == oct(default_file_mode)
    assert oct(helper.getattr('/'+dir_id+'/'+file_id).st_mode & 0o777) == oct(default_file_mode)
    assert oct(helper.getattr(dir_id).st_mode & 0o777) == oct(default_dir_mode)
    assert oct(helper.getattr(dir_id+'/').st_mode & 0o777) == oct(default_dir_mode)
    assert oct(helper.getattr('/'+dir_id).st_mode & 0o777) == oct(default_dir_mode)
    assert oct(helper.getattr('/'+dir_id+'/').st_mode & 0o777) == oct(default_dir_mode)

    assert oct(helper.getattr(dir_id+'/'+file_id2).st_mode & 0o777) == oct(default_file_mode)
    assert oct(helper.getattr('/'+dir_id+'/'+file_id2).st_mode & 0o777) == oct(default_file_mode)
    assert oct(helper.getattr(dir_id).st_mode & 0o777) == oct(default_dir_mode)
    assert oct(helper.getattr(dir_id+'/').st_mode & 0o777) == oct(default_dir_mode)
    assert oct(helper.getattr('/'+dir_id).st_mode & 0o777) == oct(default_dir_mode)
    assert oct(helper.getattr('/'+dir_id+'/').st_mode & 0o777) == oct(default_dir_mode)


def test_listobjects_should_handle_subdirectories(helper):
    dir1 = 'dir1'
    dir2 = 'dir2'
    dir3 = 'dir3'

    files = ['file{}.txt'.format(i,) for i in range(1, 6)]

    result = []

    # Listing empty prefix should return empty result
    next_token, keys = helper.listobjects('dir1', '', 100)
    assert len(next_token) == 0

    objects = to_python_list(keys)
    assert objects == []

    for file in files:
        helper.write('/'+dir1+'/'+dir2+'/'+file, random_str(), 0)
        result.append('/'+dir1+'/'+dir2+'/'+file)
        helper.write('/'+dir1+'/'+dir3+'/'+file, random_str(), 0)
        result.append('/'+dir1+'/'+dir3+'/'+file)
        helper.write('/'+dir1+'/'+file, random_str(), 0)
        result.append('/'+dir1+'/'+file)

    # List objects from root should include leading '/'
    # objects as a list of tuples (path, stat)
    next_token, keys = helper.listobjects('', '', 100)
    objects = to_python_list(keys)

    assert len(next_token) == 0

    assert len(objects) > 0

    for o in objects:
        assert o[0] == '/'

    next_token, keys = helper.listobjects('/', '', 100)
    assert len(next_token) == 0

    objects = to_python_list(keys)
    for o in objects:
        assert o[0] == '/'

    # List only objects starting with 'dir1/dir2' prefix
    next_token, keys = helper.listobjects('dir1/dir2', '', 100)
    assert len(next_token) == 0
    objects = to_python_list(keys)
    assert objects == ['/dir1/dir2/file{}.txt'.format(i,) for i in range(1, 6)]

    # Check that the same results are returned for paths with and without
    # forward slash
    next_token, keys_slash = helper.listobjects('/dir1/dir2', '', 100)
    next_token, keys_no_slash = helper.listobjects('dir1/dir2', '', 100)
    assert to_python_list(keys_slash) == to_python_list(keys_no_slash)

    # Make sure that all results are returned for single query
    next_token, keys = helper.listobjects('dir1', '', 100)
    objects = to_python_list(keys)
    assert set(objects) == set(result)

    # Make sure that all results are returned in chunks
    objects = []
    token = ""
    chunk_size = 3
    while True:
        next_token, chunks = helper.listobjects('dir1', token, chunk_size)
        chunk = to_python_list(chunks)
        if len(next_token) == 0:
            break

        token = next_token

        objects.extend(chunk)


def test_listobjects_should_handle_multiple_subdirs_with_offset(helper):
    test_dir = random_str()
    contents = []

    dirs = ['/'+test_dir+'/'+'dir{}'.format(i,) for i in range(100)]
    files = ['/'+test_dir+'/'+'file{}.txt'.format(i,) for i in range(100)]

    step = 7

    for d in dirs:
        helper.write(d+'/file.txt', random_str(), 0)
        contents.append(d+'/file.txt')

    for f in files:
        helper.write(f, random_str(), 0)
        contents.append(f)

    res = []
    i = 0
    token = ''
    while i < len(contents):
        next_token, chunks = helper.listobjects(test_dir, token, step)
        res.extend(to_python_list(chunks))
        i += step
        if len(next_token) == 0:
            break

        token = next_token

    assert len(contents) == len(res)
    assert set(contents) == set(res)


def test_listobjects_should_not_return_root_dir(helper):
    test_dir = random_str()
    contents = []

    helper.write('/{}/dir1/file.txt'.format(test_dir), random_str(), 0)
    helper.write('/{}/dir2/file.txt'.format(test_dir), random_str(), 0)
    helper.write('/{}/file.txt'.format(test_dir), random_str(), 0)

    next_token, chunks = helper.listobjects('/{}'.format(test_dir), "", 100)
    assert len(next_token) == 0

    res = to_python_list(chunks)

    assert len(res) == 3
