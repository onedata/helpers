"""This module tests null device helper."""

__author__ = "Bartek Kryza"
__copyright__ = """(C) 2018 ACK CYFRONET AGH,
This software is released under the MIT license cited in 'LICENSE.txt'."""

import os
import sys
import subprocess
import datetime
from os.path import expanduser

import pytest

script_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.dirname(script_dir))
# noinspection PyUnresolvedReferences
from test_common import *
# noinspection PyUnresolvedReferences
from environment import common, docker, nfs
from nulldevice_helper import NullDeviceHelperProxy
from posix_test_types import *

def measureElapsed(start):
    diff = datetime.datetime.now() - start
    return (diff.days * 86400000) + (diff.seconds * 1000)\
            + (diff.microseconds / 1000)

@pytest.fixture
def file_id():
    return random_str(32)

@pytest.fixture(scope='module')
def server(request):
    class Server(object):
        def __init__(self, latencyMin, latencyMax, timeoutProbability, filter):
            self.latencyMin = latencyMin
            self.latencyMax = latencyMax
            self.timeoutProbability = timeoutProbability
            self.filter = filter

    def fin():
         pass

    request.addfinalizer(fin)

    return Server(0, 0, 0.0, "*")

@pytest.fixture(scope='module')
def slowServer(request):
    class Server(object):
        def __init__(self, latencyMin, latencyMax, timeoutProbability, filter):
            self.latencyMin = latencyMin
            self.latencyMax = latencyMax
            self.timeoutProbability = timeoutProbability
            self.filter = filter

    def fin():
         pass

    request.addfinalizer(fin)

    return Server(25, 75, 0.0, "read,write")

@pytest.fixture(scope='module')
def busyServer(request):
    class Server(object):
        def __init__(self, latencyMin, latencyMax, timeoutProbability, filter):
            self.latencyMin = latencyMin
            self.latencyMax = latencyMax
            self.timeoutProbability = timeoutProbability
            self.filter = filter

    def fin():
         pass

    request.addfinalizer(fin)

    return Server(0, 0, 0.7, "read,write")

@pytest.fixture(scope='module')
def simulatedFilesystemServer(request):
    class Server(object):
        def __init__(self, latencyMin, latencyMax, timeoutProbability, filter,
                     simulatedFilesystemParameters, simulatedFilesystemGrowSpeed):
            self.latencyMin = latencyMin
            self.latencyMax = latencyMax
            self.timeoutProbability = timeoutProbability
            self.filter = filter
            self.simulatedFilesystemParameters = simulatedFilesystemParameters
            self.simulatedFilesystemGrowSpeed = simulatedFilesystemGrowSpeed

    def fin():
         pass

    request.addfinalizer(fin)

    return Server(0, 0, 0.0, "*", "5-20:10-20:5-1:0-100:1000000", 0.0)

@pytest.fixture(scope='module')
def simulatedGrowingFilesystemServer(request):
    class Server(object):
        def __init__(self, latencyMin, latencyMax, timeoutProbability, filter,
                     simulatedFilesystemParameters, simulatedFilesystemGrowSpeed):
            self.latencyMin = latencyMin
            self.latencyMax = latencyMax
            self.timeoutProbability = timeoutProbability
            self.filter = filter
            self.simulatedFilesystemParameters = simulatedFilesystemParameters
            self.simulatedFilesystemGrowSpeed = simulatedFilesystemGrowSpeed

    def fin():
         pass

    request.addfinalizer(fin)

    return Server(0, 0, 0.0, "*", "4-4:0-1", 0.5)

@pytest.fixture(scope='module')
def dataVerifyingFilesystemServer(request):
    class Server(object):
        def __init__(self, latencyMin, latencyMax, timeoutProbability, filter,
                     simulatedFilesystemParameters, simulatedFilesystemGrowSpeed):
            self.latencyMin = latencyMin
            self.latencyMax = latencyMax
            self.timeoutProbability = timeoutProbability
            self.filter = filter
            self.simulatedFilesystemParameters = simulatedFilesystemParameters
            self.simulatedFilesystemGrowSpeed = simulatedFilesystemGrowSpeed
            self.enableDataVerification = True

    def fin():
        pass

    request.addfinalizer(fin)

    return Server(0, 0, 0.0, "*", "", 0)

@pytest.fixture
def helper(server):
    """
    Create a helper to ideal server
    """
    return NullDeviceHelperProxy(
        server.latencyMin,
        server.latencyMax,
        server.timeoutProbability,
        server.filter,
        "", 0.0, False)

@pytest.fixture
def slowStorageHelper(slowServer):
    """
    Create a helper to slow (non-zero latency) server
    """
    return NullDeviceHelperProxy(
        slowServer.latencyMin,
        slowServer.latencyMax,
        slowServer.timeoutProbability,
        slowServer.filter,
        "", 0.0, False)

@pytest.fixture
def busyStorageHelper(busyServer):
    """
    Create a helper to busy (returns timeouts with some probability) server
    """
    return NullDeviceHelperProxy(
        busyServer.latencyMin,
        busyServer.latencyMax,
        busyServer.timeoutProbability,
        busyServer.filter,
        "", 0.0, False)

@pytest.fixture
def simulatedFilesystemStorageHelper(simulatedFilesystemServer):
    """
    Create a helper which simulates existing filesystem
    """
    return NullDeviceHelperProxy(
        simulatedFilesystemServer.latencyMin,
        simulatedFilesystemServer.latencyMax,
        simulatedFilesystemServer.timeoutProbability,
        simulatedFilesystemServer.filter,
        simulatedFilesystemServer.simulatedFilesystemParameters,
        simulatedFilesystemServer.simulatedFilesystemGrowSpeed,
        False)

@pytest.fixture
def simulatedGrowingFilesystemStorageHelper(simulatedGrowingFilesystemServer):
    """
    Create a helper which simulates a growing filesystem
    """
    return NullDeviceHelperProxy(
        simulatedGrowingFilesystemServer.latencyMin,
        simulatedGrowingFilesystemServer.latencyMax,
        simulatedGrowingFilesystemServer.timeoutProbability,
        simulatedGrowingFilesystemServer.filter,
        simulatedGrowingFilesystemServer.simulatedFilesystemParameters,
        simulatedGrowingFilesystemServer.simulatedFilesystemGrowSpeed,
        False)


@pytest.fixture
def dataVerifyingFilesystemStorageHelper(dataVerifyingFilesystemServer):
    """
    Create a helper which simulates a growing filesystem
    """
    return NullDeviceHelperProxy(
        dataVerifyingFilesystemServer.latencyMin,
        dataVerifyingFilesystemServer.latencyMax,
        dataVerifyingFilesystemServer.timeoutProbability,
        dataVerifyingFilesystemServer.filter,
        dataVerifyingFilesystemServer.simulatedFilesystemParameters,
        dataVerifyingFilesystemServer.simulatedFilesystemGrowSpeed,
        dataVerifyingFilesystemServer.enableDataVerification)


def test_helper_check_availability(helper):
    helper.check_storage_availability()


@pytest.mark.readwrite_operations_tests
def test_read_should_read_written_data(helper, file_id):
    data = random_str()
    offset = random_int()

    assert helper.write(file_id, data, offset) == len(data)


@pytest.mark.readwrite_operations_tests
def test_read_should_read_nonexistent_file(helper, file_id):
    offset = random_int()
    size = random_int()

    assert len(helper.read(file_id, offset, size)) == size


@pytest.mark.readwrite_operations_tests
def test_read_should_read_buffered_content(helper, file_id):
    offset = 20
    size = 4

    assert len(helper.read(file_id, offset, size)) == size
    assert helper.read(file_id, offset, size).decode('utf-8') == 'xxxx'


@pytest.mark.directory_operations_tests
def test_mkdir_should_create_directory(helper, file_id):
    dir_id = random_str()
    data = random_str()
    offset = random_int()

    try:
        helper.mkdir(dir_id, 0o777)
    except:
        pytest.fail("Couldn't create directory: %s"%(dir_id))

    assert helper.write(dir_id+"/"+file_id, data, offset) == len(data)


@pytest.mark.directory_operations_tests
def test_rename_directory_should_rename(helper, file_id):
    dir1_id = random_str()
    dir2_id = random_str()
    data = random_str()
    offset = random_int()

    helper.mkdir(dir1_id, 0o777)
    helper.rename(dir1_id, dir2_id)

    assert helper.write(dir2_id+"/"+file_id, data, offset) == len(data)


@pytest.mark.directory_operations_tests
def test_readdir_should_list_files_in_directory(helper, file_id):
    dir_id = file_id

    assert len(helper.readdir(dir_id, 0, 10)) == 10


@pytest.mark.directory_operations_tests
def test_rmdir_should_remove_directory(helper, file_id):
    dir_id = file_id

    helper.rmdir(dir_id)


@pytest.mark.remove_operations_tests
def test_unlink_should_pass_errors(helper, file_id):

    helper.unlink(file_id, 0)


@pytest.mark.links_operations_tests
def test_symlink_should_create_link(helper, file_id):
    dir_id = random_str()
    data = random_str()

    helper.symlink(dir_id+"/"+file_id, file_id+".lnk")


@pytest.mark.links_operations_tests
def test_link_should_create_hard_link(helper, file_id):
    dir_id = file_id
    data = random_str()

    helper.link(dir_id+"/"+file_id, file_id+".lnk")


@pytest.mark.mknod_operations_tests
def test_mknod_should_set_premissions(helper, file_id):
    dir_id = file_id
    data = random_str()

    flags = FlagsSet()

    helper.mknod(file_id, 0o654, flags)


@pytest.mark.mknod_operations_tests
def test_mknod_should_create_regular_file_by_default(helper, file_id):
    dir_id = file_id
    data = random_str()

    flags = FlagsSet()

    helper.mknod(file_id, 0o654, flags)


@pytest.mark.ownership_operations_tests
def test_chown_should_change_user_and_group(helper, file_id):

    helper.chown(file_id, 1001, 2002)


@pytest.mark.truncate_operations_tests
def test_truncate_should_not_create_file(helper, file_id):

    size = random_int() + 1

    helper.truncate(file_id, size, 0)


@pytest.mark.readwrite_operations_tests
def test_write_should_write_with_latency_in_bounds(slowStorageHelper, file_id):
    data = random_str()
    offset = random_int()

    slowStorageHelper.write(file_id, data, offset)

    for _ in range(100):
        start = datetime.datetime.now()
        slowStorageHelper.write(file_id, data, offset)
        elapsed_ms = measureElapsed(start)
        assert elapsed_ms >= 25
        assert elapsed_ms < 75 + 10


@pytest.mark.readwrite_operations_tests
def test_write_should_not_generate_timeouts_on_ideal_storage(helper, file_id):
    data = random_str()
    offset = random_int()

    completed = 0
    failed = 0

    for _ in range(1000):
        try:
            helper.write(file_id, data, offset)
            completed += 1
        except RuntimeError:
            failed += 1

    assert completed == 1000
    assert failed == 0


@pytest.mark.readwrite_operations_tests
def test_write_should_generate_timeouts_on_busy_storage(busyStorageHelper, file_id):
    data = random_str()
    offset = random_int()

    completed = 0
    failed = 0

    for _ in range(1000):
        try:
            busyStorageHelper.write(file_id, data, offset)
            completed += 1
        except RuntimeError:
            failed += 1

    assert completed > 0
    assert failed > 0


@pytest.mark.simulated_filesystem_tests
@pytest.mark.skip("VFS-10552")
def test_simulated_filesystem_should_simulate_directories(simulatedFilesystemStorageHelper):
    """
    Test example specification defined in simulatedFilesystemServer fixture:

        5-20:10-20:5-1:0-100:1000000
    """

    S_IFDIR = 0o040000
    S_IFREG = 0o100000

    assert len(simulatedFilesystemStorageHelper.readdir("/", 0, 10)) == 10
    assert len(simulatedFilesystemStorageHelper.readdir("/", 0, 100)) == 5+20
    assert len(simulatedFilesystemStorageHelper.readdir("/1", 0, 100)) == 10+20
    assert len(simulatedFilesystemStorageHelper.readdir("/1/1", 0, 100)) == 5+1
    assert len(simulatedFilesystemStorageHelper.readdir("/1/1/1", 0, 150)) == 0+100
    assert len(simulatedFilesystemStorageHelper.readdir("/5/10/5", 0, 150)) == 0+100

    assert simulatedFilesystemStorageHelper.getattr("/1").st_mode & S_IFDIR
    assert simulatedFilesystemStorageHelper.getattr("/6").st_mode & S_IFREG
    assert simulatedFilesystemStorageHelper.getattr("/1/1").st_mode & S_IFDIR
    assert simulatedFilesystemStorageHelper.getattr("/5/10/5/100").st_mode & S_IFREG

    assert simulatedFilesystemStorageHelper.getattr("/6").st_size == 1000000
    assert simulatedFilesystemStorageHelper.getattr("/5/10/5/100").st_size == 1000000


@pytest.mark.simulated_filesystem_tests
@pytest.mark.skip("VFS-10552")
def test_simulated_filesystem_should_grow_at_specified_rate(simulatedGrowingFilesystemStorageHelper):
    """
    Test example specification defined in simulatedFilesystemServer fixture:

        4-4:0-4
    """

    assert len(simulatedGrowingFilesystemStorageHelper.readdir("/", 0, 10)) < 4+4
    assert len(simulatedGrowingFilesystemStorageHelper.readdir("/1", 0, 10)) == 0
    time.sleep(9)
    assert len(simulatedGrowingFilesystemStorageHelper.readdir("/", 0, 10)) >= 4+4
    assert simulatedGrowingFilesystemStorageHelper.getattr("/0/0").st_size == 1024


@pytest.mark.simulated_filesystem_tests
def test_simulated_filesystem_should_handle_parameter_update(simulatedFilesystemStorageHelper):
    """
    Test example specification defined in simulatedFilesystemServer fixture:

        5-20:10-20:5-1:0-100:1000000
    """
    assert simulatedFilesystemStorageHelper.getattr("/6").st_size == 1000000

    simulatedFilesystemStorageHelper.updateHelper('0', '0', '0.0', '*',
                                                  '5-20:10-20:5-1:0-100:1024',
                                                  '0', 'false')

    assert simulatedFilesystemStorageHelper.getattr("/6").st_size == 1024


@pytest.mark.simulated_filesystem_tests
def test_data_verifying_null_helper(dataVerifyingFilesystemStorageHelper):
    file_id = random_str()

    pattern_str = b'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890+='

    assert len(pattern_str) == 64

    assert dataVerifyingFilesystemStorageHelper.read(file_id, 0, 4) == b'abcd'
    assert dataVerifyingFilesystemStorageHelper.read(file_id, 64, 4) == b'abcd'
    assert dataVerifyingFilesystemStorageHelper.read(file_id, 1024*1024 + 4, 4) == b'efgh'

    assert dataVerifyingFilesystemStorageHelper.write(file_id, b'abcd', 0) == 4
    assert dataVerifyingFilesystemStorageHelper.write(file_id, b'efgh', 1024*1024 + 4) == 4

    assert dataVerifyingFilesystemStorageHelper.write(file_id, pattern_str+pattern_str, 0) \
           == 2*len(pattern_str)

    with pytest.raises(RuntimeError) as excinfo:
        dataVerifyingFilesystemStorageHelper.write(file_id, b'abcd', 1)

    assert 'Input/output error' in str(excinfo.value)

    random_offset = random_int()
    block_size = 10000000
    assert dataVerifyingFilesystemStorageHelper.write(file_id,
        dataVerifyingFilesystemStorageHelper.read(file_id, random_offset, block_size),
                                                      random_offset) == block_size
