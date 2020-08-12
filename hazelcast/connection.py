import logging
import struct
import sys
import threading
import time
import io

from hazelcast.exception import AuthenticationError, TargetDisconnectedError
from hazelcast.future import ImmediateFuture, ImmediateExceptionFuture
from hazelcast.invocation import Invocation
from hazelcast.protocol.client_message import BEGIN_END_FLAG, ClientMessage, ClientMessageBuilder, \
    SIZE_OF_FRAME_LENGTH_AND_FLAGS, Frame, InboundMessage
from hazelcast.protocol.codec import client_authentication_codec, client_ping_codec
from hazelcast.serialization import INT_SIZE_IN_BYTES, FMT_LE_INT
from hazelcast.util import AtomicInteger, parse_addresses, calculate_version, UNKNOWN_VERSION
from hazelcast.version import CLIENT_TYPE, CLIENT_VERSION, SERIALIZATION_VERSION
from hazelcast import six, HazelcastClient

BUFFER_SIZE = 128000
PROTOCOL_VERSION = 1


class ConnectionManager(object):
    """
    ConnectionManager is responsible for managing :mod:`Connection` objects.
    """
    logger = logging.getLogger("HazelcastClient.ConnectionManager")

    def __init__(self, client, new_connection_func, address_translator):
        self._new_connection_mutex = threading.RLock()
        self._io_thread = None
        self._client = client
        self.connections = {}
        self._pending_connections = {}
        self._socket_map = {}
        self._new_connection_func = new_connection_func
        self._connection_listeners = []
        self._address_translator = address_translator
        self._logger_extras = {"client_name": client.name, "cluster_name": client.config.cluster_name}

    def add_listener(self, on_connection_opened=None, on_connection_closed=None):
        """
        Registers a ConnectionListener. If the same listener is registered multiple times, it will be notified multiple
        times.

        :param on_connection_opened: (Function), function to be called when a connection is opened.
        :param on_connection_closed: (Function), function to be called when a connection is removed.
        """
        self._connection_listeners.append((on_connection_opened, on_connection_closed))

    def get_connection(self, address):
        """
        Gets the existing connection for a given address or connects. This call is silent.

        :param address: (:class:`~hazelcast.core.Address`), the address to connect to.
        :return: (:class:`~hazelcast.connection.Connection`), the found connection, or None if no connection exists.
        """
        try:
            return self.connections[address]
        except KeyError:
            return None

    def _cluster_authenticator(self, connection):
        uuid = self._client.cluster.uuid
        owner_uuid = self._client.cluster.owner_uuid

        request = client_authentication_codec.encode_request(
            username=self._client.config.group_config.name,
            password=self._client.config.group_config.password,
            uuid=uuid,
            owner_uuid=owner_uuid,
            is_owner_connection=False,
            client_type=CLIENT_TYPE,
            serialization_version=SERIALIZATION_VERSION,
            client_hazelcast_version=CLIENT_VERSION)

        def callback(f):
            parameters = client_authentication_codec.decode_response(f.result())
            if parameters["status"] != 0:
                raise AuthenticationError("Authentication failed.")
            connection.endpoint = parameters["address"]
            self.owner_uuid = parameters["owner_uuid"]
            self.uuid = parameters["uuid"]
            connection.server_version_str = parameters.get("server_hazelcast_version", "")
            connection.server_version = calculate_version(connection.server_version_str)
            return connection

        return self._client.invoker.invoke_on_connection(request, connection).continue_with(callback)

    def get_or_connect(self, address, authenticator=None):
        """
        Gets the existing connection for a given address. If it does not exist, the system will try to connect
        asynchronously. In this case, it returns a Future. When the connection is established at some point in time, it
        can be retrieved by using the get_connection(:class:`~hazelcast.core.Address`) or from Future.

        :param address: (:class:`~hazelcast.core.Address`), the address to connect to.
        :param authenticator: (Function), function to be used for authentication (optional).
        :return: (:class:`~hazelcast.connection.Connection`), the existing connection or it returns a Future which includes asynchronously.
        """
        if address in self.connections:
            return ImmediateFuture(self.connections[address])
        else:
            with self._new_connection_mutex:
                if address in self._pending_connections:
                    return self._pending_connections[address]
                else:
                    authenticator = authenticator or self._cluster_authenticator
                    try:
                        translated_address = self._address_translator.translate(address)
                        if translated_address is None:
                            raise ValueError("Address translator could not translate address: {}".format(address))
                        connection = self._new_connection_func(translated_address,
                                                               self._client.config.network_config.connection_timeout,
                                                               self._client.config.network_config.socket_options,
                                                               connection_closed_callback=self._connection_closed,
                                                               message_callback=self._client.invoker._handle_client_message,
                                                               network_config=self._client.config.network_config)
                    except IOError:
                        return ImmediateExceptionFuture(sys.exc_info()[1], sys.exc_info()[2])

                    future = authenticator(connection).continue_with(self.on_auth, connection, address)
                    if not future.done():
                        self._pending_connections[address] = future
                    return future

    def on_auth(self, f, connection, address):
        """
        Checks for authentication of a connection.

        :param f: (:class:`~hazelcast.future.Future`), future that contains the result of authentication.
        :param connection: (:class:`~hazelcast.connection.Connection`), newly established connection.
        :param address: (:class:`~hazelcast.core.Address`), the adress of new connection.
        :return: Result of authentication.
        """
        if f.is_success():
            self.logger.info("Authenticated with %s", f.result(), extra=self._logger_extras)
            with self._new_connection_mutex:
                self.connections[connection.endpoint] = f.result()
                try:
                    self._pending_connections.pop(address)
                except KeyError:
                    pass
            for on_connection_opened, _ in self._connection_listeners:
                if on_connection_opened:
                    on_connection_opened(f.result())
            return f.result()
        else:
            self.logger.debug("Error opening %s", connection, extra=self._logger_extras)
            with self._new_connection_mutex:
                try:
                    self._pending_connections.pop(address)
                except KeyError:
                    pass
            six.reraise(f.exception().__class__, f.exception(), f.traceback())

    def _connection_closed(self, connection, cause):
        # if connection was authenticated, fire event
        if connection.endpoint:
            try:
                self.connections.pop(connection.endpoint)
            except KeyError:
                pass
            for _, on_connection_closed in self._connection_listeners:
                if on_connection_closed:
                    on_connection_closed(connection, cause)
        else:
            # clean-up unauthenticated connection
            self._client.invoker.cleanup_connection(connection, cause)

    def close_connection(self, address, cause):
        """
        Closes the connection with given address.

        :param address: (:class:`~hazelcast.core.Address`), address of the connection to be closed.
        :param cause: (Exception), the cause for closing the connection.
        :return: (bool), ``true`` if the connection is closed, ``false`` otherwise.
        """
        try:
            connection = self.connections[address]
            connection.close(cause)
        except KeyError:
            self.logger.warning("No connection with %s was found to close.", address, extra=self._logger_extras)
            return False


class HeartbeatManager(object):
    _heartbeat_timer = None
    logger = logging.getLogger("HazelcastClient.HeartbeatManager")

    def __init__(self, client):
        self._client = client
        self._conn_manager = client.connection_manager
        self._logger_extras = {"client_name": client.name, "cluster_name": client.config.cluster_name}

        props = client.properties
        self._heartbeat_timeout = props.get_seconds_positive_or_default(props.HEARTBEAT_TIMEOUT)
        self._heartbeat_interval = props.get_seconds_positive_or_default(props.HEARTBEAT_INTERVAL)

    def start(self):
        """
        Starts sending periodic HeartBeat operations.
        """
        def _heartbeat():
            if not self._conn_manager.is_alive:
                return

            now = time.time()
            for conn in self._conn_manager.get_active_connections():
                self._check_connection(now, conn)
            self._heartbeat_timer = self._client.reactor.add_timer(self._heartbeat_interval, _heartbeat)

        self._heartbeat_timer = self._client.reactor.add_timer(self._heartbeat_interval, _heartbeat)

    def shutdown(self):
        """
        Stops HeartBeat operations.
        """
        if self._heartbeat_timer:
            self._heartbeat_timer.cancel()

    def _check_connection(self, now, connection):
        if not connection.is_alive():
            return

        if (now - connection.last_read_time) > self._heartbeat_timeout:
            if connection.is_alive():
                self.logger.warning("Heartbeat failed over the connection: %s" % connection)
                connection.close("Heartbeat timed out",
                                 TargetDisconnectedError("Heartbeat timed out to connection %s" % connection))

        if (now - connection.last_write_time) > self._heartbeat_interval:
            request = client_ping_codec.encode_request()
            invoker = self._client.invoker
            invocation = Invocation(invoker, request, connection=connection)
            invoker.invoke_urgent(invocation)


_frame_header = struct.Struct('<iH')


class _Reader(object):
    def __init__(self, builder):
        self._buf = io.BytesIO()
        self._builder = builder
        self._bytes_read = 0
        self._bytes_written = 0
        self._frame_size = 0
        self._frame_flags = 0
        self._message = None

    def read(self, data):
        self._buf.seek(self._bytes_written)
        self._buf.write(data)
        self._bytes_written += len(data)

    def process(self):
        message = self._read_message()
        while message:
            self._builder.on_message(message)
            # TODO: handle fragmented messages
            message = self._read_message()

    def _read_message(self):
        while True:
            if self._read_frame():
                if self._message.end_frame.is_end_frame():
                    msg = self._message
                    self._reset()
                    return msg
            else:
                return None

    def _read_frame(self):
        n = self.length
        if n < SIZE_OF_FRAME_LENGTH_AND_FLAGS:
            # we don't have even the frame length and flags ready
            return False

        if self._frame_size == 0:
            self._read_frame_size_and_flags()

        if n < self._frame_size:
            return False

        self._buf.seek(self._bytes_read)
        data = self._buf.read(self._frame_size)
        self._bytes_read += self._frame_size
        self._frame_size = 0
        # No need to reset flags since it will be overwritten on the next read_frame_size_and_flags call
        frame = Frame(data, self._frame_flags)
        if not self._message:
            self._message = InboundMessage(frame)
        else:
            self._message.add_frame(frame)
        return True

    def _read_frame_size_and_flags(self):
        self._buf.seek(self._bytes_read)
        header_data = self._buf.read(SIZE_OF_FRAME_LENGTH_AND_FLAGS)
        self._frame_size, self._frame_flags = _frame_header.unpack_from(header_data, self._bytes_read)
        self._bytes_read += SIZE_OF_FRAME_LENGTH_AND_FLAGS

    def _reset(self):
        if self._bytes_written == self._bytes_read:
            self._buf.seek(0)
            self._buf.truncate()
            self._bytes_written = 0
            self._bytes_read = 0
        self._message = None

    @property
    def length(self):
        return self._bytes_written - self._bytes_read


class Connection(object):
    """
    Connection object which stores connection related information and operations.
    """

    def __init__(self, conn_manager, conn_id, message_callback, logger_extras=None):
        self.remote_address = None
        self.remote_uuid = None
        self.last_read_time = 0
        self.last_write_time = 0
        self.start_time = 0
        self.logger = logging.getLogger("HazelcastClient.Connection[%d]" % conn_id)
        self.server_version = UNKNOWN_VERSION
        self.server_version_str = None
        self.live = True

        self._conn_manager = conn_manager
        self._logger_extras = logger_extras
        self._id = conn_id
        self._builder = ClientMessageBuilder(message_callback)
        self._reader = _Reader(self._builder)

    def send_message(self, message):
        """
        Sends a message to this connection.

        :param message: (Message), message to be sent to this connection.
        :return: (bool),
        """
        if not self.live:
            return False

        self._write(message.buf)
        return True

    def close(self, reason, cause):
        """
        Closes the connection.

        :param reason: (str), The reason this connection is going to be closed. Is allowed to be None.
        :param cause: (Exception), The exception responsible for closing this connection. Is allowed to be None.
        """
        if not self.live:
            return

        self.live = False
        self._log_close(reason, cause)
        try:
            self._inner_close()
        except:
            self.logger.exception("Error while closing the the connection")
        self._conn_manager.on_conn_close(self)

    def _log_close(self, reason, cause):
        msg = "%s closed. Reason: %s"
        if reason:
            r = reason
        elif cause:
            r = cause
        else:
            r = "Socket explicitly closed"

        if self._conn_manager.live:
            self.logger.info(msg % (self, r))
        else:
            self.logger.debug(msg % (self, r))

    def _inner_close(self):
        raise NotImplementedError()

    def _write(self, buf):
        raise NotImplementedError()

    def __repr__(self):
        return "Connection(address=%s, id=%s)" % (self.remote_address, self._id)

    def __eq__(self, other):
        return isinstance(other, Connection) and self._id == other._id

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return self._id


class DefaultAddressProvider(object):
    """
    Provides initial addresses for client to find and connect to a node.
    Loads addresses from the Hazelcast configuration.
    """
    def __init__(self, network_config):
        self._network_config = network_config

    def load_addresses(self):
        """
        :return: (Sequence), The possible member addresses to connect to.
        """
        return parse_addresses(self._network_config.addresses)


class DefaultAddressTranslator(object):
    """
    DefaultAddressTranslator is a no-op. It always returns the given address.
    """
    def translate(self, address):
        """
        :param address: (:class:`~hazelcast.core.Address`), address to be translated.
        :return: (:class:`~hazelcast.core.Address`), translated address.
        """
        return address

    def refresh(self):
        """Refreshes the internal lookup table if necessary."""
        pass
