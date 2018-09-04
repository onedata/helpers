"""This module tests communication stack."""

__author__ = "Konrad Zemek"
__copyright__ = """(C) 2015 ACK CYFRONET AGH,
This software is released under the MIT license cited in 'LICENSE.txt'."""

import os
import sys

import pytest

script_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.dirname(script_dir))
from test_common import *
# noinspection PyUnresolvedReferences
from environment import appmock, common, docker
import communication_stack


@pytest.fixture
def endpoint(appmock_client):
    return appmock_client.tcp_endpoint(443)


@pytest.fixture
def com3(endpoint):
    return communication_stack.Communicator(3, 1, endpoint.ip, endpoint.port, False)


@pytest.fixture
def com1(endpoint):
    return communication_stack.Communicator(1, 1, endpoint.ip, endpoint.port, True)


@pytest.mark.performance(
    parameters=[Parameter.msg_num(1), Parameter.msg_size(100, 'B')],
    configs={
        'multiple_small_messages': {
            'description': 'Sends multiple small messages using '
                           'communicator.',
            'parameters': [Parameter.msg_num(1000000)]
        },
        'multiple_large_messages': {
            'description': 'Sends multiple large messages using '
                           'communicator.',
            'parameters': [Parameter.msg_num(50), Parameter.msg_size(1, 'MB')]
        }
    })
def test_send(result, msg_num, msg_size, endpoint, com3):
    """Sends multiple messages using communicator."""

    com3.connect()
    msg = random_str(msg_size)

    send_time = Duration()
    sent_bytes = 0

    for _ in xrange(msg_num):
        with measure(send_time):
            sent_bytes = com3.send(msg)

    with measure(send_time):
        endpoint.wait_for_specific_messages(sent_bytes, msg_num,
                                            timeout_sec=60)

    result.set([
        Parameter.send_time(send_time),
        Parameter.mbps(msg_num, msg_size, send_time),
        Parameter.msgps(msg_num, send_time)
    ])


@pytest.mark.performance(
    repeats=10,
    parameters=[Parameter.msg_num(1), Parameter.msg_size(100, 'B')],
    configs={
        'multiple_small_messages': {
            'description': 'Receives multiple small messages using '
                           'communicator.',
            'parameters': [Parameter.msg_num(1000)]
        },
        'multiple_large_messages': {
            'description': 'Receives multiple large messages using '
                           'communicator.',
            'parameters': [Parameter.msg_num(50), Parameter.msg_size(1, 'MB')]
        }
    })
def test_communicate(result, msg_num, msg_size, endpoint, com3):
    """Sends multiple messages and receives replies using communicator."""

    com3.connect()

    endpoint.wait_for_connections(accept_more=True)
    msg = random_str(msg_size)

    communicate_time = Duration()
    for _ in xrange(msg_num):
        with measure(communicate_time):
            request = com3.communicate(msg)

        reply = communication_stack.prepareReply(request, msg)

        with measure(communicate_time):
            endpoint.wait_for_specific_messages(request)
            endpoint.send(reply)

        with measure(communicate_time):
            assert reply == com3.communicateReceive()

    result.set([
        Parameter.communicate_time(communicate_time),
        Parameter.mbps(msg_num, msg_size, communicate_time),
        Parameter.msgps(msg_num, communicate_time)
    ])


@pytest.mark.skip()
def test_successful_handshake(endpoint, com1):
    handshake = com1.setHandshake("handshake", False)
    com1.connect()

    # Skip message stream request
    endpoint.wait_for_any_messages(msg_count=1)

    com1.sendAsync("this is another request")

    endpoint.wait_for_specific_messages(handshake)

    assert 1 == endpoint.all_messages_count()-1

    reply = communication_stack.prepareReply(handshake, "handshakeReply")

    endpoint.send(reply)

    assert com1.handshakeResponse() == reply
    endpoint.wait_for_any_messages(msg_count=2)


@pytest.mark.skip()
def test_unsuccessful_handshake(endpoint, com3):
    handshake = com3.setHandshake("anotherHanshake", True)
    com3.connect()

    # Skip message stream request
    endpoint.wait_for_any_messages(msg_count=1)

    endpoint.wait_for_specific_messages(handshake, msg_count=3)

    reply = communication_stack.prepareReply(handshake, "anotherHandshakeR")
    endpoint.send(reply)

    # The connections should now be recreated and another handshake sent
    endpoint.wait_for_specific_messages(handshake, msg_count=6)
