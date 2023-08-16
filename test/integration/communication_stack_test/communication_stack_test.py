"""This module tests communication stack."""

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
import communication_stack
from proto import messages_pb2, common_messages_pb2

@pytest.yield_fixture
def endpoint(appmock_client):
    app = appmock_client.tcp_endpoint(443)
    yield app
    appmock_client.reset_tcp_history()


@pytest.yield_fixture
def com3(endpoint):
    c = communication_stack.Communicator(3, 1, endpoint.ip, endpoint.port, False)
    yield c
    c.stop()


@pytest.yield_fixture
def com1(endpoint):
    c = communication_stack.Communicator(1, 1, endpoint.ip, endpoint.port, True)
    yield c
    c.stop()


def test_send(result, endpoint, com3, msg_num = 1, msg_size = 100):
    """Sends multiple messages using communicator."""

    com3.connect()
    msg = random_str(msg_size).encode('utf-8')

    send_time = Duration()
    sent_bytes = 0

    for _ in range(msg_num):
        with measure(send_time):
            sent_bytes = com3.send(msg)

    with measure(send_time):
        endpoint.wait_for_specific_messages(sent_bytes.encode('utf-8'), msg_num,
                                            timeout_sec=60)

    result.set([
        Parameter.send_time(send_time),
        Parameter.mbps(msg_num, msg_size, send_time),
        Parameter.msgps(msg_num, send_time)
    ])


def test_communicate(result, endpoint, com3, msg_num = 1000, msg_size = 100):
    """Sends multiple messages and receives replies using communicator."""

    com3.connect()

    endpoint.wait_for_connections(accept_more=True)
    msg = random_str(msg_size).encode('utf-8')

    communicate_time = Duration()
    for _ in range(msg_num):
        with measure(communicate_time):
            request = com3.communicate(msg)

        reply = communication_stack.prepareReply(request, msg).encode('utf-8')

        with measure(communicate_time):
            endpoint.wait_for_specific_messages(request.encode('utf-8'))
            endpoint.send(reply)

        with measure(communicate_time):
            assert reply == com3.communicateReceive().encode('utf-8')

    result.set([
        Parameter.communicate_time(communicate_time),
        Parameter.mbps(msg_num, msg_size, communicate_time),
        Parameter.msgps(msg_num, communicate_time)
    ])


def test_successful_handshake(appmock_client):
    for _ in range(0, 10):
        endpoint = appmock_client.tcp_endpoint(443)

        com1 = communication_stack.Communicator(
            1, 1, endpoint.ip, endpoint.port, True)

        handshake = com1.setHandshake("handshake".encode('utf-8'), False)

        com1.connect()

        # Skip message stream request
        endpoint.wait_for_any_messages(msg_count=1)

        com1.sendAsync("this is another request")

        server_message = messages_pb2.ServerMessage()
        server_message.handshake_response.status = common_messages_pb2.Status.ok
        endpoint.send(server_message.SerializeToString())

        endpoint.wait_for_any_messages(msg_count=3)

        com1.stop()

        appmock_client.reset_tcp_history()
        appmock_client.reset_rest_history()




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
