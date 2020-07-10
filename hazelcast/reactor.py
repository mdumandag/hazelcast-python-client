import asyncore
import errno
import logging
import os
import select
import socket
import sys
import threading
import time

from collections import deque
from functools import total_ordering
from hazelcast.config import PROTOCOL
from hazelcast.connection import Connection, BUFFER_SIZE
from hazelcast.exception import HazelcastError
from hazelcast.future import Future
from hazelcast.six.moves import queue

try:
    import ssl
except ImportError:
    ssl = None

_dispatcher_map = {}


class _FileWrapper(object):
    def __init__(self, fd):
        self._fd = fd

    def fileno(self):
        return self._fd

    def close(self):
        os.close(self._fd)

    def getsockopt(self, level, optname, buflen=None):
        if level == socket.SOL_SOCKET and optname == socket.SO_ERROR and not buflen:
            return 0
        raise NotImplementedError("Only asyncore specific behaviour is implemented.")


class _AbstractWaker(asyncore.dispatcher):

    def __init__(self):
        asyncore.dispatcher.__init__(self, map=_dispatcher_map)
        self._awake = False

    def writable(self):
        return False

    def wake(self):
        raise NotImplementedError("wake")


class _PipedWaker(_AbstractWaker):
    def __init__(self):
        _AbstractWaker.__init__(self)
        self._read_fd, self._write_fd = os.pipe()
        self.set_socket(_FileWrapper(self._read_fd))

    def wake(self):
        if not self._awake:
            os.write(self._write_fd, b"x")
            self._awake = True

    def handle_read(self):
        while len(os.read(self._read_fd, 4096)) == 4096:
            pass
        self._awake = False

    def close(self):
        _AbstractWaker.close(self)
        os.close(self._write_fd)


class _SocketedWaker(_AbstractWaker):

    def __init__(self):
        _AbstractWaker.__init__(self)
        self._writer = socket.socket()
        self._writer.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        a = socket.socket()
        a.bind(("127.0.0.1", 0))
        a.listen(1)
        addr = a.getsockname()

        try:
            self._writer.connect(addr)
            self._reader, _ = a.accept()
        finally:
            a.close()

        self.set_socket(self._reader)
        self._writer.settimeout(0)
        self._reader.settimeout(0)

    def wake(self):
        if not self._awake:
            try:
                self._writer.send(b"x")
                self._awake = True
            except (IOError, socket.error, ValueError):
                pass

    def handle_read(self):
        try:
            while True:
                result = self._reader.recv(1024)
                if not result:
                    break
            self._awake = False
        except (IOError, socket.error):
            pass

    def close(self):
        _AbstractWaker.close(self)
        self._writer.close()


class _AbstractLoop(object):
    logger = logging.getLogger("HazelcastClient.AsyncoreReactor")

    def __init__(self, logger_extras):
        self._logger_extras = logger_extras
        self._timers = queue.PriorityQueue()
        self._loop_lock = threading.Lock()
        self._is_live = False

    def loop(self):
        self.logger.debug("Starting Reactor Thread", extra=self._logger_extras)
        Future._threading_locals.is_reactor_thread = True
        while self._is_live:
            try:
                self.run_loop()
                self._check_timers()
            except select.error:
                # TODO: parse error type to catch only error "9"
                self.logger.warning("Connection closed by server", extra=self._logger_extras)
                pass
            except:
                self.logger.exception("Error in Reactor Thread", extra=self._logger_extras)
                # TODO: shutdown client
                return
        self.logger.debug("Reactor Thread exited. %s" % self._timers.qsize(), extra=self._logger_extras)
        self._cleanup_all_timers()

    def add_timer(self, delay, callback):
        timer = Timer(delay + time.time(), callback, self._cleanup_timer)
        self._timers.put_nowait((timer.end, timer))
        self.wake_loop()
        return timer

    def _check_timers(self):
        now = time.time()
        while not self._timers.empty():
            try:
                _, timer = self._timers.queue[0]
            except IndexError:
                return

            if timer.check_timer(now):
                try:
                    self._timers.get_nowait()
                except queue.Empty:
                    pass
            else:
                return

    def _cleanup_timer(self, timer):
        try:
            self.logger.debug("Cancel timer %s" % timer, extra=self._logger_extras)
            self._timers.queue.remove((timer.end, timer))
        except ValueError:
            pass

    def _cleanup_all_timers(self):
        while not self._timers.empty():
            try:
                _, timer = self._timers.get_nowait()
                timer.timer_ended_cb()
            except queue.Empty:
                return

    def check_loop(self):
        raise NotImplementedError("check_loop")

    def run_loop(self):
        raise NotImplementedError("run_loop")

    def wake_loop(self):
        raise NotImplementedError("wake_loop")

    def shutdown(self):
        raise NotImplementedError("shutdown")


class AsyncoreReactor(object):
    _thread = None
    _is_live = False

    def __init__(self, logger_extras=None):
        self._logger_extras = logger_extras
        self._timers = queue.PriorityQueue()
        self._map = {}

    def start(self):
        self._is_live = True
        self._thread = threading.Thread(target=self._loop, name="hazelcast-reactor")
        self._thread.daemon = True
        self._thread.start()

    def add_timer(self, delay, callback):
        return self.add_timer_absolute(delay + time.time(), callback)

    def shutdown(self):
        if not self._is_live:
            return
        self._is_live = False
        for connection in list(self._map.values()):
            try:
                connection.close(HazelcastError("Client is shutting down"))
            except OSError as connection:
                if connection.args[0] == socket.EBADF:
                    pass
                else:
                    raise
        self._map.clear()
        self._thread.join()

    def new_connection(self, address, connect_timeout, socket_options, connection_closed_callback, message_callback,
                       network_config):
        return AsyncoreConnection(self._map, address, connect_timeout, socket_options,
                                  connection_closed_callback, message_callback, network_config, self._logger_extras)



class AsyncoreConnection(Connection, asyncore.dispatcher):
    sent_protocol_bytes = False
    read_buffer_size = BUFFER_SIZE

    def __init__(self, map, address, connect_timeout, socket_options, connection_closed_callback,
                 message_callback, network_config, logger_extras=None):
        asyncore.dispatcher.__init__(self, map=map)
        Connection.__init__(self, address, connection_closed_callback, message_callback, logger_extras)

        self._write_lock = threading.Lock()
        self._write_queue = deque()
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.settimeout(connect_timeout)

        # set tcp no delay
        self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        # set socket buffer
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, BUFFER_SIZE)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, BUFFER_SIZE)

        for socket_option in socket_options:
            if socket_option.option is socket.SO_RCVBUF:
                self.read_buffer_size = socket_option.value

            self.socket.setsockopt(socket_option.level, socket_option.option, socket_option.value)

        self.connect(self._address)

        ssl_config = network_config.ssl_config
        if ssl and ssl_config.enabled:
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)

            protocol = ssl_config.protocol

            # Use only the configured protocol
            try:
                if protocol != PROTOCOL.SSLv2:
                    ssl_context.options |= ssl.OP_NO_SSLv2
                if protocol != PROTOCOL.SSLv3 and protocol != PROTOCOL.SSL:
                    ssl_context.options |= ssl.OP_NO_SSLv3
                if protocol != PROTOCOL.TLSv1:
                    ssl_context.options |= ssl.OP_NO_TLSv1
                if protocol != PROTOCOL.TLSv1_1:
                    ssl_context.options |= ssl.OP_NO_TLSv1_1
                if protocol != PROTOCOL.TLSv1_2 and protocol != PROTOCOL.TLS:
                    ssl_context.options |= ssl.OP_NO_TLSv1_2
                if protocol != PROTOCOL.TLSv1_3:
                    ssl_context.options |= ssl.OP_NO_TLSv1_3
            except AttributeError:
                pass

            ssl_context.verify_mode = ssl.CERT_REQUIRED

            if ssl_config.cafile:
                ssl_context.load_verify_locations(ssl_config.cafile)
            else:
                ssl_context.load_default_certs()

            if ssl_config.certfile:
                ssl_context.load_cert_chain(ssl_config.certfile, ssl_config.keyfile, ssl_config.password)

            if ssl_config.ciphers:
                ssl_context.set_ciphers(ssl_config.ciphers)

            self.socket = ssl_context.wrap_socket(self.socket)

        # the socket should be non-blocking from now on
        self.socket.settimeout(0)

        self._write_queue.append(b"CB2")

    def handle_connect(self):
        self.start_time_in_seconds = time.time()
        self.logger.debug("Connected to %s", self._address, extra=self._logger_extras)

    def handle_read(self):
        reader = self._reader
        while True:
            data = self.recv(self.read_buffer_size)
            reader.read(data)
            self.last_read_in_seconds = time.time()
            if len(data) < self.read_buffer_size:
                break

        if reader.length:
            reader.process()

    def handle_write(self):
        while True:
            try:
                data = self._write_queue.popleft()
            except IndexError:
                return

            sent = self.send(data)
            self.last_write_in_seconds = time.time()
            self.sent_protocol_bytes = True
            if sent < len(data):
                self._write_queue.appendleft(data[sent:])

    def handle_close(self):
        self.logger.warning("Connection closed by server", extra=self._logger_extras)
        self.close(IOError("Connection closed by server"))

    def handle_error(self):
        error = sys.exc_info()[1]
        if sys.exc_info()[0] is socket.error:
            if error.errno != errno.EAGAIN and error.errno != errno.EDEADLK:
                self.logger.exception("Received error", extra=self._logger_extras)
                self.close(IOError(error))
        else:
            self.logger.warning("Received unexpected error: " + str(error), extra=self._logger_extras)

    def readable(self):
        return not self._closed and self.sent_protocol_bytes

    def write(self, data):
        self._write_queue.append(data)

    def writable(self):
        return len(self._write_queue) > 0

    def close(self, cause):
        if not self._closed:
            self._closed = True
            asyncore.dispatcher.close(self)
            self._connection_closed_callback(self, cause)


@total_ordering
class Timer(object):
    canceled = False

    def __init__(self, end, timer_ended_cb, timer_canceled_cb):
        self.end = end
        self.timer_ended_cb = timer_ended_cb
        self.timer_canceled_cb = timer_canceled_cb

    def __eq__(self, other):
        return self.end == other.end

    def __ne__(self, other):
        return not (self == other)

    def __lt__(self, other):
        return self.end < other.end

    def cancel(self):
        self.canceled = True
        self.timer_canceled_cb(self)

    def check_timer(self, now):
        if self.canceled:
            return True

        if now > self.end:
            self.timer_ended_cb()
            return True
