"""This module contains native IO performance helper tests."""

__author__ = "Bartek Kryza"
__copyright__ = """(C) 2018 ACK CYFRONET AGH,
This software is released under the MIT license cited in 'LICENSE.txt'."""

import os
import sys
import subprocess
from os.path import expanduser

import functools
import pytest
import threading
import time

script_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.dirname(script_dir))
# noinspection PyUnresolvedReferences
from test_common import *

# Units for customizing test sizes
KB = 1024
MB = 1024*1024
GB = 1024*1024*1024


class IOPerfParam(Parameter):
    """Defines parameters which should be passed to IO performance tests."""

    @staticmethod
    def thr_num(num):
        return Parameter('thr_num', 'Number of parallel IO threads.', num)

    @staticmethod
    def op_num(num):
        return Parameter('op_num', 'Number of IO operations per thread.', num)

    @staticmethod
    def op_size(num):
        return Parameter('op_size', 'Size of IO operations in MB.', num)


class IOPerformanceTestThread(threading.Thread):
    """
    Custom IO performance test thread.

    This custom class is necessary to check if each thread has completed
    successfully or not, since in Python threading assert exceptions are
    not propagated outside of thread.
    """

    def __init__(self, results, tid, group=None, target=None, name=None, args=(),
            kwargs=None, verbose=None):
        super(IOPerformanceTestThread,self).__init__(group=group, target=target,
                name=name)
        self.args = args
        self.results = results
        self.tid = tid
        self.target = target
        self.kwargs = kwargs
        return

    def run(self):
        try:
            self.target(*self.args)
            self.results[self.tid] = True
        except Exception as e:
            self.results[self.tid] = False
            pytest.fail(e)


class IOPerformanceTest:
    """
    Wrapper class for running a specific IO performance test in a thread pool.
    """

    def __init__(self, test_, helper_, thr_num_, op_num_, op_size_):
        self.test = test_
        self.helper = helper_
        self.thr_num = thr_num_
        self.op_num = op_num_
        self.op_size = op_size_
        self.threads = [None] * self.thr_num
        self.results = [False] * self.thr_num

    def __call__(self):
        for i in range(self.thr_num):
            self.threads[i] = IOPerformanceTestThread(self.results, i, target=self.test,
                    args=[self.helper, self.op_num, self.op_size*10*KB])
            self.threads[i].start()

        for i in range(self.thr_num):
            self.threads[i].join()

        assert all([x is True for x in self.results])


def perf_write_base(helper, op_num, size):
    for _ in range(op_num):
        file_id = random_str()
        data = random_str(size)

        assert helper.write(file_id, data, 0) == len(data)

def perf_write_read_base(helper, op_num, size):
    for i in range(op_num):
        file_id = random_str()
        data = random_str(size)

        assert helper.write(file_id, data, 0) == len(data)
        assert helper.read(file_id, 0, len(data)).decode('utf-8') == data

def perf_truncate_base(helper, op_num, size):
    for _ in range(op_num):
        file_id = random_str()

        helper.write(file_id, 'X'*size, 0)
        helper.truncate(file_id, 1, size)
        assert helper.read(file_id, 0, 1).decode('utf-8') == 'X'

def perf_write_read_truncate_unlink_base(helper, op_num, size):
    for i in range(op_num):
        file_id = random_str()
        data = random_str(size)

        assert helper.write(file_id, data, 0) == len(data)
        assert helper.read(file_id, 0, len(data)).decode('utf-8') == data
        helper.truncate(file_id, 0, len(data))
        helper.unlink(file_id, 0)

def test_write(result, helper):
    io_test = IOPerformanceTest(perf_write_base, helper, 1, 1, 1)
    io_test()

def test_write_read(result, helper):
    io_test = IOPerformanceTest(perf_write_read_base, helper, 1, 1, 1)
    io_test()

def test_truncate(result, helper):
    io_test = IOPerformanceTest(perf_truncate_base, helper, 1, 1, 1)
    io_test()

def test_read_write_truncate_unlink(result, helper):
    io_test = IOPerformanceTest(perf_write_read_truncate_unlink_base, helper,
            1, 1, 1)
    io_test()
