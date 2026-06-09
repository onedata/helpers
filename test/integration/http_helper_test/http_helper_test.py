"""This module tests HTTP helper."""

__author__ = "Bartek Kryza"
__copyright__ = """(C) 2018 ACK CYFRONET AGH,
This software is released under the MIT license cited in 'LICENSE.txt'."""

import os
import sys
import time
import subprocess
from os.path import expanduser
from urllib.parse import urlparse
import socket

import pytest
import hashlib
from http.server import SimpleHTTPRequestHandler
import socketserver
from urllib.parse import urlparse, parse_qs
import re
import threading
import requests

script_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.dirname(script_dir))
# noinspection PyUnresolvedReferences
from test_common import *
# noinspection PyUnresolvedReferences
from environment import common, docker, http
from http_helper import HTTPHelperProxy

from posix_test_types import *
from common_test_base import file_id

MOCK_HTTP_SERVER_PORT = 9876

class RangeHTTPRequestHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        self.alphabet_content = "abcdefghijklmnopqrstuvwxyz"
        super().__init__(*args, **kwargs)

    def do_GET(self):
        parsed_path = urlparse(self.path)


        try:
            if parsed_path.path == "/with_ranges":
                self.handle_with_ranges()
            elif parsed_path.path == "/without_ranges":
                self.handle_without_ranges()
            elif parsed_path.path == "/without_ranges_without_content_length":
                self.handle_without_ranges_without_content_length()
            elif parsed_path.path == "/chunked":
                self.handle_chunked()
            else:
                self.send_error(404, "Not Found")
        except ConnectionResetError as e:
            pass

    def do_HEAD(self):
        parsed_path = urlparse(self.path)

        if parsed_path.path == "/with_ranges":
            self.handle_with_ranges_head()
        elif parsed_path.path == "/without_ranges":
            self.handle_without_ranges_head()
        elif parsed_path.path == "/without_ranges_without_content_length":
            self.send_error(405, "Method Not Allowed")
        elif parsed_path.path == "/chunked":
            self.handle_chunked_head()
        else:
            self.send_error(404, "Not Found")

    def handle_with_ranges(self):
        content = self.alphabet_content.encode('utf-8')
        content_length = len(content)

        range_header = self.headers.get('Range')

        if range_header:
            range_match = re.match(r'bytes=(\d+)-(\d*)', range_header)
            if range_match:
                start = int(range_match.group(1))
                end = int(range_match.group(2)) if range_match.group(2) else content_length - 1

                if start < content_length and end < content_length and start <= end:
                    partial_content = content[start:end + 1]

                    self.send_response(206)
                    self.send_header('Content-Type', 'text/plain')
                    self.send_header('Content-Length', str(len(partial_content)))
                    self.send_header('Content-Range', f'bytes {start}-{end}/{content_length}')
                    self.end_headers()

                    self.wfile.write(partial_content)
                    return

        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.send_header('Content-Length', str(content_length))
        self.send_header('Accept-Ranges', 'bytes')
        self.end_headers()

        self.wfile.write(content)

    def handle_without_ranges(self):
        content = self.alphabet_content.encode('utf-8')
        content_length = len(content)

        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.send_header('Content-Length', str(content_length))
        self.end_headers()

        self.wfile.write(content)

    def handle_without_ranges_without_content_length(self):
        parsed_path = urlparse(self.path)
        query_params = parse_qs(parsed_path.query)
        size = int(query_params.get('size', [100])[0])

        content = (self.alphabet_content * ((size // len(self.alphabet_content)) + 1))[:size].encode('utf-8')

        self.send_response(200)
        self.send_header('Content-Type', 'application/octet-stream')
        self.end_headers()

        self.wfile.write(content)

    def handle_with_ranges_head(self):
        content = self.alphabet_content.encode('utf-8')
        content_length = len(content)

        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.send_header('Content-Length', str(content_length))
        self.send_header('Accept-Ranges', 'bytes')
        self.end_headers()

    def handle_without_ranges_head(self):
        content = self.alphabet_content.encode('utf-8')
        content_length = len(content)

        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.send_header('Content-Length', str(content_length))
        self.end_headers()

    def handle_chunked_head(self):
        self.handle_chunked()

    def handle_chunked(self):
        parsed_path = urlparse(self.path)
        query_params = parse_qs(parsed_path.query)
        
        # Get parameters from query string with defaults
        size = int(query_params.get('size', [1000])[0])
        chunks = int(query_params.get('chunks', [3])[0])
        
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.send_header('Transfer-Encoding', 'chunked')
        self.end_headers()
        
        # For HEAD requests, only send headers
        if self.command == 'HEAD':
            return
        
        # Generate random content of requested size for GET requests
        import random
        import string
        content = ''.join(random.choices(string.ascii_letters + string.digits + ' \n', k=size)).encode('utf-8')
        
        # Calculate chunk sizes
        chunk_size = len(content) // chunks
        remainder = len(content) % chunks
        
        # Send content in chunks
        for i in range(chunks):
            start = i * chunk_size
            if i == chunks - 1:
                # Last chunk gets remainder
                end = start + chunk_size + remainder
            else:
                end = start + chunk_size
            
            chunk_data = content[start:end]
            chunk_size_hex = hex(len(chunk_data))[2:].encode('utf-8')
            
            # Send chunk size in hex followed by CRLF
            self.wfile.write(chunk_size_hex + b'\r\n')
            # Send chunk data followed by CRLF
            self.wfile.write(chunk_data + b'\r\n')
        
        # Send final chunk (size 0) to indicate end
        self.wfile.write(b'0\r\n\r\n')


@pytest.fixture(scope='module')
def mock_server(request):
    port = MOCK_HTTP_SERVER_PORT

    # Check if port is available
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        if s.connect_ex(('127.0.0.1', port)) == 0:
            raise RuntimeError(f"Port {port} is already in use")

    server_started = threading.Event()
    httpd_ref = [None]

    def server_thread():
        try:
            with socketserver.TCPServer(("", port), RangeHTTPRequestHandler) as httpd:
                httpd_ref[0] = httpd
                print(f"Server running on http://127.0.0.1:{port}")
                print("Endpoints:")
                print(f"  http://127.0.0.1:{port}/with_ranges - Supports range requests")
                print(f"  http://127.0.0.1:{port}/without_ranges - Does not support range requests")
                print(f"  http://127.0.0.1:{port}/without_ranges_without_content_length?size=100 - No HEAD, no ranges, no content-length")
                print(f"  http://127.0.0.1:{port}/chunked?size=1000&chunks=3 - Chunked transfer encoding")
                server_started.set()
                try:
                    httpd.serve_forever()
                except KeyboardInterrupt:
                    print("\nServer stopped")
        except Exception as e:
            print(f"Failed to start mock server: {e}")
            server_started.set()

    thread = threading.Thread(target=server_thread, daemon=True)
    thread.start()

    # Wait for server to start
    server_started.wait(timeout=10)

    # Verify server is responding
    for attempt in range(5):
        try:
            response = requests.get(f'http://127.0.0.1:{port}/without_ranges', timeout=1)
            if response.status_code == 200:
                break
        except requests.exceptions.RequestException:
            pass
        time.sleep(0.5)
    else:
        raise RuntimeError(f"Mock server failed to become ready on port {port}")

    def cleanup():
        if httpd_ref[0]:
            httpd_ref[0].shutdown()

    request.addfinalizer(cleanup)
    return thread


@pytest.fixture(scope='module')
def server(request):
    class Server(object):
        def __init__(self, endpoint, credentials):
            self.endpoint = endpoint
            self.credentials = credentials

    result = http.up('onedata/lighttpd:v2', 'storage',
                       common.generate_uid())

    [container] = result['docker_ids']
    credentials = result['credentials']
    endpoint = result['endpoint']

    def fin():
        docker.remove([container], force=True, volumes=True)

    request.addfinalizer(fin)

    time.sleep(5)

    return Server(endpoint, credentials)


@pytest.fixture
def helper(server):
    return HTTPHelperProxy(server.endpoint, server.credentials, "basic", False, 0)


@pytest.fixture
def mock_helper(mock_server):
    yield HTTPHelperProxy(f'http://127.0.0.1:{MOCK_HTTP_SERVER_PORT}', "", "none", False, 0)


@pytest.fixture
def mock_helper_emulate_range_read(mock_server):
    yield HTTPHelperProxy(f'http://127.0.0.1:{MOCK_HTTP_SERVER_PORT}', "", "none", True, 1024)


@pytest.fixture
def public_helper():
    return HTTPHelperProxy("https://packages.onedata.org", "", "none", False, 0)


@pytest.fixture
def helper_invalid_hostname(server):
    return HTTPHelperProxy("http://no_such_host.invalid", server.credentials, "basic", False, 0)


def test_helper_check_availability(helper):
    helper.check_storage_availability()


def test_helper_check_availability_error_invalid_host(helper_invalid_hostname):
    with pytest.raises(RuntimeError) as excinfo:
        helper_invalid_hostname.check_storage_availability()

    assert "Failed to resolve address for" in str(excinfo)


def get_file_index(h):
    res = []
    index_file = h.read('/test_data/index.txt', 0, 10000).decode('utf-8')
    for line in index_file.splitlines():
        (md5sum, size, timestamp) = line.split(',')
        res.append((md5sum, size, timestamp))
    return res


def test_getattr_should_get_mode_and_timestamp(helper, file_id):
    index = get_file_index(helper)

    for file_data in index:
        stat = helper.getattr(f'/test_data/{file_data[0]}')
        assert stat.st_size == int(file_data[1])


def test_getattr_should_get_mode_and_timestamp_with_absolute_url(server, file_id):
    helper = HTTPHelperProxy(server.endpoint, server.credentials, "basic", False, 0)
    index = get_file_index(helper)

    for file_data in index:
        stat = helper.getattr(f'{server.endpoint}/test_data/{file_data[0]}')
        assert stat.st_size == int(file_data[1])


def test_read_should_read_valid_data(helper, file_id):
    index = get_file_index(helper)

    for file_data in index:
        data = helper.read(f'/test_data/{file_data[0]}', 0, int(file_data[1]))
        assert len(data) == int(file_data[1])
        m = hashlib.md5()
        m.update(data)
        assert file_data[0] == m.hexdigest()


def test_read_should_read_valid_data_with_absolute_url(server, file_id):
    helper = HTTPHelperProxy(server.endpoint, server.credentials, "basic", False, 0)
    index = get_file_index(helper)

    for file_data in index:
        data = helper.read(f'{server.endpoint}/test_data/{file_data[0]}', 0, int(file_data[1]))
        assert len(data) == int(file_data[1])
        m = hashlib.md5()
        m.update(data)
        assert file_data[0] == m.hexdigest()


def test_read_should_read_valid_data_with_query_string(server, file_id):
    helper = HTTPHelperProxy(server.endpoint, server.credentials, "basic", False, 0)
    index = get_file_index(helper)

    for file_data in index:
        data = helper.read(f'{server.endpoint}/test_data/direct?file={file_data[0]}', 0, int(file_data[1]))
        assert len(data) == int(file_data[1])
        m = hashlib.md5()
        m.update(data)
        assert file_data[0] == m.hexdigest()


def test_read_should_return_errors(helper):
    with pytest.raises(RuntimeError) as excinfo:
        helper.read('/not_existent', 0, 1024)
    assert 'No such file or directory' in str(excinfo.value)


def test_read_should_work_with_public_servers(public_helper):
    size = public_helper.getattr('/apt/ubuntu/2002/dists/bionic/Release').st_size

    data = public_helper.read('/apt/ubuntu/2002/dists/bionic/Release', 0, size)

    assert len(data) == size


def test_read_should_work_with_public_servers_using_external_urls(helper):
    size = helper.getattr('http://packages.onedata.org/apt/ubuntu/2002/dists/bionic/Release').st_size

    data = helper.read('http://packages.onedata.org/apt/ubuntu/2002/dists/bionic/Release', 0, size)

    assert len(data) == size


def test_getattr_should_return_enotsup_without_ranges(mock_helper):
    f = f'http://127.0.0.1:{MOCK_HTTP_SERVER_PORT}/without_ranges'

    with pytest.raises(RuntimeError) as excinfo:
        mock_helper.getattr(f)

    assert "Operation not supported" in str(excinfo.value)


def test_read_should_return_enotsup_without_ranges(mock_helper):
    f = f'http://127.0.0.1:{MOCK_HTTP_SERVER_PORT}/without_ranges'

    with pytest.raises(RuntimeError) as excinfo:
        mock_helper.read(f, 0, 10)

    assert "Operation not supported" in str(excinfo.value)


def test_read_should_work_with_public_servers_using_absolute_urls(public_helper):
    size = public_helper.getattr('https://packages.onedata.org/apt/ubuntu/2002/dists/bionic/Release').st_size

    data = public_helper.read('https://packages.onedata.org/apt/ubuntu/2002/dists/bionic/Release', 0, size)

    assert len(data) == size


def test_getattr_should_return_enotsup_with_chunked_encoding(mock_helper):
    f = f'http://127.0.0.1:{MOCK_HTTP_SERVER_PORT}/chunked'

    with pytest.raises(RuntimeError) as excinfo:
        mock_helper.getattr(f)

    assert "Operation not supported" in str(excinfo.value)


def test_read_should_return_enotsup_with_chunked_encoding(mock_helper):
    f = f'http://127.0.0.1:{MOCK_HTTP_SERVER_PORT}/chunked'

    with pytest.raises(RuntimeError) as excinfo:
        mock_helper.read(f, 0, 10)

    assert "Operation not supported" in str(excinfo.value)


def test_getattr_should_emulate_range_read_with_content_length(mock_helper_emulate_range_read):
    f = f'http://127.0.0.1:{MOCK_HTTP_SERVER_PORT}/without_ranges'

    stat = mock_helper_emulate_range_read.getattr(f)

    assert stat.st_size == 26


def test_getattr_should_emulate_range_read_without_content_length(mock_helper_emulate_range_read):
    f = f'http://127.0.0.1:{MOCK_HTTP_SERVER_PORT}/without_ranges_without_content_length'

    stat = mock_helper_emulate_range_read.getattr(f)

    assert stat.st_size == 100


def test_read_should_emulate_range_read_without_content_length(mock_helper_emulate_range_read):
    f = f'http://127.0.0.1:{MOCK_HTTP_SERVER_PORT}/without_ranges_without_content_length'

    data = mock_helper_emulate_range_read.read(f, 10, 15)

    assert len(data) == 15


def test_getattr_should_emulate_range_read_with_chunked_encoding(mock_helper_emulate_range_read):
    f = f'http://127.0.0.1:{MOCK_HTTP_SERVER_PORT}/chunked'

    stat = mock_helper_emulate_range_read.getattr(f)

    assert stat.st_size == 1000


def test_read_should_emulate_range_read_with_chunked_encoding(mock_helper_emulate_range_read):
    f = f'http://127.0.0.1:{MOCK_HTTP_SERVER_PORT}/chunked'

    data = mock_helper_emulate_range_read.read(f, 10, 15)

    assert len(data) == 15