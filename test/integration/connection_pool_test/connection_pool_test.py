"""This module tests connection pool."""

__author__ = "Konrad Zemek"
__copyright__ = """(C) 2015 ACK CYFRONET AGH,
This software is released under the MIT license cited in 'LICENSE.txt'."""

import os
import sys

import pytest
from concurrent.futures import ThreadPoolExecutor

script_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.dirname(script_dir))
from test_common import *
# noinspection PyUnresolvedReferences
from environment import appmock, common, docker
# noinspection PyUnresolvedReferences
import connection_pool


@pytest.yield_fixture
def endpoint(appmock_client):
    app = appmock_client.tcp_endpoint(443)
    yield app
    appmock_client.reset_tcp_history()


@pytest.yield_fixture
def cp(endpoint):
    pool = connection_pool.ConnectionPoolProxy(25, 4, endpoint.ip, endpoint.port)
    yield pool
    pool.stop()


def test_cp_should_stop(endpoint, msg_num = 10, msg_size = 1000, repeats = 20):
    """Starts and stops connection pool multiple times to ensure the pool
       stops without deadlock."""

    for _ in range(repeats):
        msg = random_str(msg_size).encode('utf-8')

        cp = connection_pool.ConnectionPoolProxy(
            10, 4, endpoint.ip, endpoint.port)

        for _ in range(msg_num):
            cp.send(msg)

        endpoint.wait_for_specific_messages(msg, msg_num, timeout_sec=60)

        cp.stop()


@pytest.mark.performance(
    parameters=[Parameter.msg_num(10000), Parameter.msg_size(100, 'B')],
    configs={
        'multiple_small_messages': {
            'description': 'Sends multiple small messages using connection '
                           'pool.',
            'parameters': [Parameter.msg_num(1000000)]
        },
        'multiple_large_messages': {
            'description': 'Sends multiple large messages using connection '
                           'pool.',
            'parameters': [Parameter.msg_num(1000),
                           Parameter.msg_size(1, 'MB')]
        }
    })
def test_cp_should_send_messages(result, endpoint, cp, msg_num, msg_size):
    """Sends multiple messages using connection pool and checks whether they
    have been received."""

    msg = random_str(msg_size).encode('utf-8')

    send_time = Duration()
    for _ in range(msg_num):
        with measure(send_time):
            cp.send(msg)

    with measure(send_time):
        endpoint.wait_for_specific_messages(msg, msg_num, timeout_sec=60)

    result.set([
        Parameter.send_time(send_time),
        Parameter.mbps(msg_num, msg_size, send_time),
        Parameter.msgps(msg_num, send_time)
    ])

def test_cp_should_send_messages_parallel(endpoint, cp, msg_num=10000, msg_size=100, workers=100):
    msg = random_str(msg_size).encode('utf-8')

    def send(m):
        cp.send(m)

    with ThreadPoolExecutor(max_workers=workers) as executor:
        executor.map(send, [msg for _ in range(msg_num)])

    endpoint.wait_for_specific_messages(msg, msg_num, timeout_sec=60)

@pytest.mark.performance(
    parameters=[Parameter.msg_num(2000), Parameter.msg_size(100, 'B')],
    configs={
        'multiple_small_messages': {
            'description': 'Receives multiple small messages using '
                           'connection pool.',
            'parameters': [Parameter.msg_num(2500)]
        },
        'multiple_large_messages': {
            'description': 'Receives multiple large messages using '
                           'connection pool.',
            'parameters': [Parameter.msg_size(1, 'MB')]
        }
    })
def test_cp_should_receive_messages(result, endpoint, cp, msg_num, msg_size):
    """Receives multiple messages using connection pool."""

    msgs = [random_str(msg_size) for _ in range(msg_num)]

    recv_time = Duration()
    for msg in msgs:
        with measure(recv_time):
            endpoint.send(msg.encode('utf-8'))

    recv = []
    for _ in msgs:
        with measure(recv_time):
            recv.append(cp.popMessage())

    assert len(msgs) == len(recv)
    assert msgs.sort() == recv.sort()

    result.set([
        Parameter.recv_time(recv_time),
        Parameter.mbps(msg_num, msg_size, recv_time),
        Parameter.msgps(msg_num, recv_time)
    ])
