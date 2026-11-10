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
            elif parsed_path.path == "/with_ranges_conformant":
                self.handle_with_ranges_conformant()
            elif parsed_path.path == "/with_ranges_extra_data":
                self.handle_with_ranges_extra_data()
            elif parsed_path.path == "/with_ranges_from_zero":
                self.handle_with_ranges_from_zero()
            elif parsed_path.path == "/with_ranges_star_total":
                self.handle_with_ranges_star_total()
            elif parsed_path.path == "/empty_file":
                self.handle_empty_file()
            elif parsed_path.path == "/empty_file_no_ranges":
                self.handle_empty_file_no_ranges()
            elif parsed_path.path == "/redirect":
                self.handle_redirect()
            elif parsed_path.path == "/accept_ranges_none":
                self.handle_accept_ranges_none()
            elif parsed_path.path == "/head_no_content_length":
                self.handle_head_no_content_length()
            else:
                self.send_error(404, "Not Found")
        except ConnectionError as e:
            pass

    def do_HEAD(self):
        parsed_path = urlparse(self.path)

        try:
            if parsed_path.path == "/with_ranges":
                self.handle_with_ranges_head()
            elif parsed_path.path == "/without_ranges":
                self.handle_without_ranges_head()
            elif parsed_path.path == "/without_ranges_without_content_length":
                self.send_error(405, "Method Not Allowed")
            elif parsed_path.path == "/chunked":
                self.handle_chunked_head()
            elif parsed_path.path in ("/with_ranges_conformant",
                                      "/with_ranges_extra_data",
                                      "/with_ranges_from_zero",
                                      "/with_ranges_star_total"):
                self.handle_with_ranges_head()
            elif parsed_path.path == "/empty_file":
                self.handle_empty_file_head()
            elif parsed_path.path == "/empty_file_no_ranges":
                self.handle_empty_file_no_ranges_head()
            elif parsed_path.path == "/redirect":
                self.handle_redirect()
            elif parsed_path.path == "/accept_ranges_none":
                self.handle_accept_ranges_none_head()
            elif parsed_path.path == "/head_no_content_length":
                self.handle_head_no_content_length_head()
            else:
                self.send_error(404, "Not Found")
        except ConnectionError as e:
            pass

    def parse_range_header(self, content_length):
        range_header = self.headers.get('Range')
        if not range_header:
            return None
        range_match = re.match(r'bytes=(\d+)-(\d*)', range_header)
        if not range_match:
            return None
        start = int(range_match.group(1))
        end = int(range_match.group(2)) if range_match.group(2) \
            else content_length - 1
        return (start, end)

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

    def handle_with_ranges_conformant(self):
        """Fully RFC 7233 conformant server: clamps range end to resource
        size and returns 416 for unsatisfiable ranges."""
        content = self.alphabet_content.encode('utf-8')
        content_length = len(content)

        r = self.parse_range_header(content_length)
        if r:
            start, end = r
            if start >= content_length:
                self.send_response(416)
                self.send_header('Content-Range', f'bytes */{content_length}')
                self.send_header('Content-Length', '0')
                self.end_headers()
                return

            end = min(end, content_length - 1)
            partial_content = content[start:end + 1]

            self.send_response(206)
            self.send_header('Content-Type', 'text/plain')
            self.send_header('Content-Length', str(len(partial_content)))
            self.send_header('Content-Range',
                             f'bytes {start}-{end}/{content_length}')
            self.end_headers()
            self.wfile.write(partial_content)
            return

        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.send_header('Content-Length', str(content_length))
        self.send_header('Accept-Ranges', 'bytes')
        self.end_headers()
        self.wfile.write(content)

    def handle_with_ranges_extra_data(self):
        """Broken server: returns correct Content-Range header for the
        requested range, but the body contains more data than requested
        (everything from start until EOF)."""
        content = self.alphabet_content.encode('utf-8')
        content_length = len(content)

        r = self.parse_range_header(content_length)
        if r:
            start, end = r
            end = min(end, content_length - 1)
            if start < content_length:
                body = content[start:]

                self.send_response(206)
                self.send_header('Content-Type', 'text/plain')
                self.send_header('Content-Length', str(len(body)))
                self.send_header('Content-Range',
                                 f'bytes {start}-{end}/{content_length}')
                self.end_headers()
                self.wfile.write(body)
                return

        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.send_header('Content-Length', str(content_length))
        self.send_header('Accept-Ranges', 'bytes')
        self.end_headers()
        self.wfile.write(content)

    def handle_with_ranges_from_zero(self):
        """Broken server: responds to any Range request with 206 but
        always returns the entire resource from the beginning, with an
        honest Content-Range header starting at 0."""
        content = self.alphabet_content.encode('utf-8')
        content_length = len(content)

        r = self.parse_range_header(content_length)
        if r:
            self.send_response(206)
            self.send_header('Content-Type', 'text/plain')
            self.send_header('Content-Length', str(content_length))
            self.send_header('Content-Range',
                             f'bytes 0-{content_length - 1}/{content_length}')
            self.end_headers()
            self.wfile.write(content)
            return

        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.send_header('Content-Length', str(content_length))
        self.send_header('Accept-Ranges', 'bytes')
        self.end_headers()
        self.wfile.write(content)

    def handle_with_ranges_star_total(self):
        """Server which returns valid range responses but with unknown
        total size in Content-Range header (bytes X-Y/*)."""
        content = self.alphabet_content.encode('utf-8')
        content_length = len(content)

        r = self.parse_range_header(content_length)
        if r:
            start, end = r
            end = min(end, content_length - 1)
            if start < content_length:
                partial_content = content[start:end + 1]

                self.send_response(206)
                self.send_header('Content-Type', 'text/plain')
                self.send_header('Content-Length',
                                 str(len(partial_content)))
                self.send_header('Content-Range', f'bytes {start}-{end}/*')
                self.end_headers()
                self.wfile.write(partial_content)
                return

        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.send_header('Content-Length', str(content_length))
        self.send_header('Accept-Ranges', 'bytes')
        self.end_headers()
        self.wfile.write(content)

    def handle_empty_file(self):
        """Zero-length resource on a range-supporting server. Any Range
        request on an empty resource is unsatisfiable -> 416."""
        if self.headers.get('Range'):
            self.send_response(416)
            self.send_header('Content-Range', 'bytes */0')
            self.send_header('Content-Length', '0')
            self.end_headers()
            return

        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.send_header('Content-Length', '0')
        self.send_header('Accept-Ranges', 'bytes')
        self.end_headers()

    def handle_empty_file_head(self):
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.send_header('Content-Length', '0')
        self.send_header('Accept-Ranges', 'bytes')
        self.end_headers()

    def handle_empty_file_no_ranges(self):
        """Zero-length resource on a server which ignores Range headers."""
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.send_header('Content-Length', '0')
        self.end_headers()

    def handle_empty_file_no_ranges_head(self):
        self.handle_empty_file_no_ranges()

    def handle_redirect(self):
        """Redirects (302) to /with_ranges."""
        self.send_response(302)
        self.send_header(
            'Location',
            f'http://127.0.0.1:{MOCK_HTTP_SERVER_PORT}/with_ranges')
        self.send_header('Content-Length', '0')
        self.end_headers()

    def handle_accept_ranges_none(self):
        """Server explicitly declaring 'Accept-Ranges: none', ignoring
        Range headers in GET requests."""
        content = self.alphabet_content.encode('utf-8')

        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.send_header('Content-Length', str(len(content)))
        self.send_header('Accept-Ranges', 'none')
        self.end_headers()
        self.wfile.write(content)

    def handle_accept_ranges_none_head(self):
        content = self.alphabet_content.encode('utf-8')

        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.send_header('Content-Length', str(len(content)))
        self.send_header('Accept-Ranges', 'none')
        self.end_headers()

    def handle_head_no_content_length(self):
        """Server which advertises range support in HEAD but returns no
        Content-Length, and ignores Range headers in GET requests."""
        content = self.alphabet_content.encode('utf-8')

        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.end_headers()
        self.wfile.write(content)

    def handle_head_no_content_length_head(self):
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.send_header('Accept-Ranges', 'bytes')
        self.end_headers()

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


ALPHABET = b"abcdefghijklmnopqrstuvwxyz"


def alphabet_pattern(size):
    """Content served by /without_ranges_without_content_length?size=N"""
    return (ALPHABET * (size // len(ALPHABET) + 1))[:size]


def mock_url(path):
    return f'http://127.0.0.1:{MOCK_HTTP_SERVER_PORT}{path}'


#
# Range server - data correctness tests
#
def test_read_should_return_correct_range_data(mock_helper):
    data = mock_helper.read(mock_url('/with_ranges'), 10, 5)

    assert data == b"klmno"


def test_read_should_return_first_byte(mock_helper):
    data = mock_helper.read(mock_url('/with_ranges'), 0, 1)

    assert data == b"a"


def test_read_should_return_last_byte(mock_helper):
    data = mock_helper.read(mock_url('/with_ranges'), 25, 1)

    assert data == b"z"


def test_read_zero_size_should_return_empty(mock_helper):
    data = mock_helper.read(mock_url('/with_ranges'), 0, 0)

    assert data == b""


def test_read_full_file_should_return_all_data(mock_helper):
    data = mock_helper.read(mock_url('/with_ranges'), 0, 26)

    assert data == ALPHABET


#
# Range server - EOF boundary tests
#
def test_read_crossing_eof_should_return_remaining_bytes_conformant(mock_helper):
    # Conformant server clamps bytes=20-29 to bytes 20-25/26
    data = mock_helper.read(mock_url('/with_ranges_conformant'), 20, 10)

    assert data == b"uvwxyz"


def test_read_at_eof_should_return_empty_conformant(mock_helper):
    # Server returns 416 for bytes=26-... on a 26 byte resource
    data = mock_helper.read(mock_url('/with_ranges_conformant'), 26, 5)

    assert data == b""


def test_read_beyond_eof_should_return_empty_conformant(mock_helper):
    data = mock_helper.read(mock_url('/with_ranges_conformant'), 100, 10)

    assert data == b""


def test_read_oversized_should_return_short_read_conformant(mock_helper):
    # bytes=0-99 on a 26 byte resource clamped to bytes 0-25/26
    data = mock_helper.read(mock_url('/with_ranges_conformant'), 0, 100)

    assert data == ALPHABET


def test_read_crossing_eof_on_server_ignoring_invalid_range(mock_helper):
    # /with_ranges responds with 200 and full content to a Range request
    # exceeding the resource size (bytes=25-29), instead of clamping it.
    # A FUSE read at EOF boundary must still return the last byte.
    data = mock_helper.read(mock_url('/with_ranges'), 25, 5)

    assert data == b"z"


#
# Broken/non-conformant range servers
#
def test_read_should_handle_server_returning_extra_data(mock_helper):
    # Server returns correct Content-Range but body contains all bytes
    # from start until EOF
    data = mock_helper.read(mock_url('/with_ranges_extra_data'), 10, 5)

    assert data == b"klmno"


def test_read_full_file_from_server_returning_extra_data(mock_helper):
    data = mock_helper.read(mock_url('/with_ranges_extra_data'), 0, 26)

    assert data == ALPHABET


def test_read_should_handle_content_range_with_star_total(mock_helper):
    # Content-Range: bytes 10-14/* is valid per RFC 7233
    data = mock_helper.read(mock_url('/with_ranges_star_total'), 10, 5)

    assert data == b"klmno"


def test_read_should_return_enotsup_when_server_ignores_range_offset(mock_helper):
    # Server responds 206 with Content-Range starting always at 0 -
    # without range emulation this cannot be handled
    with pytest.raises(RuntimeError) as excinfo:
        mock_helper.read(mock_url('/with_ranges_from_zero'), 10, 5)

    assert "Operation not supported" in str(excinfo.value)


def test_read_emulate_should_handle_server_ignoring_range_offset(mock_helper_emulate_range_read):
    # With range emulation, the 206 response starting at 0 should be
    # trimmed to the requested range
    data = mock_helper_emulate_range_read.read(
        mock_url('/with_ranges_from_zero'), 10, 5)

    assert data == b"klmno"


def test_getattr_should_return_enotsup_with_accept_ranges_none(mock_helper):
    with pytest.raises(RuntimeError) as excinfo:
        mock_helper.getattr(mock_url('/accept_ranges_none'))

    assert "Operation not supported" in str(excinfo.value)


def test_getattr_emulate_should_handle_accept_ranges_none(mock_helper_emulate_range_read):
    stat = mock_helper_emulate_range_read.getattr(
        mock_url('/accept_ranges_none'))

    assert stat.st_size == 26


def test_read_emulate_should_handle_accept_ranges_none(mock_helper_emulate_range_read):
    data = mock_helper_emulate_range_read.read(
        mock_url('/accept_ranges_none'), 10, 5)

    assert data == b"klmno"


def test_getattr_should_return_enotsup_without_content_length_in_head(mock_helper):
    with pytest.raises(RuntimeError) as excinfo:
        mock_helper.getattr(mock_url('/head_no_content_length'))

    assert "Operation not supported" in str(excinfo.value)


def test_getattr_emulate_should_handle_head_without_content_length(mock_helper_emulate_range_read):
    stat = mock_helper_emulate_range_read.getattr(
        mock_url('/head_no_content_length'))

    assert stat.st_size == 26


#
# Redirects
#
def test_read_should_follow_redirect(mock_helper):
    data = mock_helper.read(mock_url('/redirect'), 10, 5)

    assert data == b"klmno"


def test_read_should_follow_redirect_from_zero_offset(mock_helper):
    data = mock_helper.read(mock_url('/redirect'), 0, 26)

    assert data == ALPHABET


def test_read_emulate_should_follow_redirect(mock_helper_emulate_range_read):
    data = mock_helper_emulate_range_read.read(mock_url('/redirect'), 10, 5)

    assert data == b"klmno"


@pytest.mark.skip(reason="getattr does not update effectiveFileId from "
                         "redirect location and does not limit redirect "
                         "count - infinite redirect loop hangs the test")
def test_getattr_should_follow_redirect(mock_helper):
    stat = mock_helper.getattr(mock_url('/redirect'))

    assert stat.st_size == 26


#
# Empty (zero-length) files
#
def test_getattr_empty_file(mock_helper):
    stat = mock_helper.getattr(mock_url('/empty_file'))

    assert stat.st_size == 0


def test_read_empty_file_should_return_empty(mock_helper):
    data = mock_helper.read(mock_url('/empty_file'), 0, 10)

    assert data == b""


def test_read_empty_file_first_byte_should_return_empty(mock_helper):
    data = mock_helper.read(mock_url('/empty_file'), 0, 1)

    assert data == b""


def test_getattr_emulate_empty_file(mock_helper_emulate_range_read):
    stat = mock_helper_emulate_range_read.getattr(
        mock_url('/empty_file_no_ranges'))

    assert stat.st_size == 0


def test_read_emulate_empty_file_should_return_empty(mock_helper_emulate_range_read):
    data = mock_helper_emulate_range_read.read(
        mock_url('/empty_file_no_ranges'), 0, 10)

    assert data == b""


def test_read_emulate_empty_file_nonzero_offset_should_return_empty(mock_helper_emulate_range_read):
    data = mock_helper_emulate_range_read.read(
        mock_url('/empty_file_no_ranges'), 5, 10)

    assert data == b""


#
# Range read emulation - data correctness
#
def test_read_emulate_should_return_correct_data(mock_helper_emulate_range_read):
    data = mock_helper_emulate_range_read.read(
        mock_url('/without_ranges'), 10, 5)

    assert data == b"klmno"


def test_read_emulate_full_file(mock_helper_emulate_range_read):
    data = mock_helper_emulate_range_read.read(
        mock_url('/without_ranges'), 0, 26)

    assert data == ALPHABET


def test_read_emulate_oversized_should_return_short_read(mock_helper_emulate_range_read):
    data = mock_helper_emulate_range_read.read(
        mock_url('/without_ranges'), 0, 100)

    assert data == ALPHABET


def test_read_emulate_first_byte(mock_helper_emulate_range_read):
    data = mock_helper_emulate_range_read.read(
        mock_url('/without_ranges'), 0, 1)

    assert data == b"a"


def test_read_emulate_last_byte(mock_helper_emulate_range_read):
    data = mock_helper_emulate_range_read.read(
        mock_url('/without_ranges'), 25, 1)

    assert data == b"z"


def test_read_emulate_zero_size_should_return_empty(mock_helper_emulate_range_read):
    data = mock_helper_emulate_range_read.read(
        mock_url('/without_ranges'), 0, 0)

    assert data == b""


def test_read_emulate_crossing_eof_should_return_remaining_bytes(mock_helper_emulate_range_read):
    data = mock_helper_emulate_range_read.read(
        mock_url('/without_ranges'), 20, 10)

    assert data == b"uvwxyz"


def test_read_emulate_at_eof_should_return_empty(mock_helper_emulate_range_read):
    data = mock_helper_emulate_range_read.read(
        mock_url('/without_ranges'), 26, 10)

    assert data == b""


def test_read_emulate_beyond_eof_should_return_empty(mock_helper_emulate_range_read):
    data = mock_helper_emulate_range_read.read(
        mock_url('/without_ranges'), 100, 10)

    assert data == b""


def test_read_emulate_sequential_reads(mock_helper_emulate_range_read):
    # Emulated range reads may abort connections mid-download - make sure
    # subsequent reads on fresh sessions still return correct data
    f = mock_url('/without_ranges')

    for i in range(5):
        offset = i * 5
        size = min(5, 26 - offset)
        data = mock_helper_emulate_range_read.read(f, offset, 5)
        assert data == ALPHABET[offset:offset + size]


def test_read_emulate_without_content_length_correct_data(mock_helper_emulate_range_read):
    f = mock_url('/without_ranges_without_content_length?size=100')

    data = mock_helper_emulate_range_read.read(f, 30, 20)

    assert data == alphabet_pattern(100)[30:50]


def test_read_emulate_without_content_length_crossing_eof(mock_helper_emulate_range_read):
    f = mock_url('/without_ranges_without_content_length?size=100')

    data = mock_helper_emulate_range_read.read(f, 95, 10)

    assert data == alphabet_pattern(100)[95:100]


def test_read_emulate_without_content_length_beyond_eof(mock_helper_emulate_range_read):
    f = mock_url('/without_ranges_without_content_length?size=100')

    data = mock_helper_emulate_range_read.read(f, 150, 10)

    assert data == b""


def test_getattr_emulate_should_fail_above_max_file_size(mock_helper_emulate_range_read):
    # maxEmulatedRangeReadFileSize is 1024
    f = mock_url('/without_ranges_without_content_length?size=2000')

    with pytest.raises(RuntimeError) as excinfo:
        mock_helper_emulate_range_read.getattr(f)

    assert "too large" in str(excinfo.value).lower()


def test_getattr_emulate_should_work_at_exactly_max_file_size(mock_helper_emulate_range_read):
    f = mock_url('/without_ranges_without_content_length?size=1024')

    stat = mock_helper_emulate_range_read.getattr(f)

    assert stat.st_size == 1024


def test_getattr_emulate_should_fail_one_byte_above_max_file_size(mock_helper_emulate_range_read):
    f = mock_url('/without_ranges_without_content_length?size=1025')

    with pytest.raises(RuntimeError) as excinfo:
        mock_helper_emulate_range_read.getattr(f)

    assert "too large" in str(excinfo.value).lower()


#
# Range read emulation - chunked transfer encoding boundaries
#
def test_read_emulate_chunked_full_size(mock_helper_emulate_range_read):
    f = mock_url('/chunked?size=1000&chunks=7')

    data = mock_helper_emulate_range_read.read(f, 0, 1000)

    assert len(data) == 1000


def test_read_emulate_chunked_crossing_eof(mock_helper_emulate_range_read):
    f = mock_url('/chunked?size=1000&chunks=3')

    data = mock_helper_emulate_range_read.read(f, 990, 20)

    assert len(data) == 10


def test_read_emulate_chunked_at_eof(mock_helper_emulate_range_read):
    f = mock_url('/chunked?size=1000&chunks=3')

    data = mock_helper_emulate_range_read.read(f, 1000, 10)

    assert len(data) == 0


#
# Range read emulation on a server which does support ranges - ranges
# should still be used directly
#
def test_getattr_emulate_with_range_server(mock_helper_emulate_range_read):
    stat = mock_helper_emulate_range_read.getattr(mock_url('/with_ranges'))

    assert stat.st_size == 26


def test_read_emulate_with_range_server(mock_helper_emulate_range_read):
    data = mock_helper_emulate_range_read.read(mock_url('/with_ranges'), 10, 5)

    assert data == b"klmno"


# NOTE: keep this test last - reading the first byte of an empty file in
# emulated range read mode exercises HTTPGET::onEOM firstByteRequest path
# with an empty body (pop_front() on empty IOBufQueue), which may crash
# the process
def test_read_emulate_empty_file_first_byte_should_return_empty(mock_helper_emulate_range_read):
    data = mock_helper_emulate_range_read.read(
        mock_url('/empty_file_no_ranges'), 0, 1)

    assert data == b""