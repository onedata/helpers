"""This module tests ODATA helper."""

__author__ = "Jakub Valenta"
__copyright__ = """(C) 2017 Cesnet z.s.p.o,
This software is released under the MIT license cited in 'LICENSE.txt'."""

import os
import sys
import subprocess
import time
from os.path import expanduser

import pytest

script_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.dirname(script_dir))
# noinspection PyUnresolvedReferences
from test_common import *
# noinspection PyUnresolvedReferences
from environment import common, docker
from odata_helper import ODataHelperProxy
from posix_test_types import *

@pytest.fixture(scope='module')
def server(request):
    class Server(object):
        def __init__(self, url):
            self.url = url
    container = docker.run(image="jakubvalenta27/dhus",
            hostname="dhus-test",
            name="dhus-test",
            detach=True)
    settings = docker.inspect(container)
    ip = settings['NetworkSettings']['IPAddress']

    def fin():
        docker.remove([container], force=True, volumes=True)

    request.addfinalizer(fin)
    
    return Server(("http://" + ip + ":8081").encode('ascii'))

@pytest.fixture
def helper(server):
    proxy = ODataHelperProxy(server.url)
    while not proxy.isReady():
        time.sleep(1);
    return proxy

def test_readdir(helper):
    try:
        files = helper.readdir('/Sentinel-2', 0,  5)
    except:
       assert False
    assert 3 == len(files)

def test_getattr(helper):
    try:
        attr = helper.getattr('/Sentinel-2')
    except:
        assert False
    assert 0777&(attr.st_mode) == 0555
    assert attr.st_size == 0

def test_access(helper):
    try:
        helper.access('/Sentinel-2', 0)
    except:
        assert False
    with pytest.raises(RuntimeError) as excinfo:
            helper.access("invalid", 0)
    assert 'No such file' in str(excinfo.value)

def test_open(helper):
    try:
        helper.open('/Sentinel-2/2018-03-21/S2B_MSIL1C_20180321T103019_N0206_R108_T30PUU_20180321T123706/manifest.safe', 0)
        assert True
    except:
        assert False

def test_readfile(helper):
    try:
        data = helper.read('/Sentinel-2/2018-03-21/S2B_MSIL1C_20180321T103019_N0206_R108_T30PUU_20180321T123706/manifest.safe', 0, 100000)
    except:
        assert False
    assert 57985 == len(data)

