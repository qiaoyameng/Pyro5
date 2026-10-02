import socket
import threading
import time

import pytest

import Pyro5.client
import Pyro5.core
import Pyro5.errors
import Pyro5.protocol
import Pyro5.serializers
import Pyro5.server
import Pyro5.socketutil


@Pyro5.server.expose
@Pyro5.server.interface_version(1)
class VersionedApiV1:
    def __init__(self):
        self.calls = 0
        self.value_writes = 0
        self._value = 1

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self.value_writes += 1
        self._value = value

    def call(self):
        self.calls += 1
        return "v1"

    @Pyro5.server.oneway
    def call_oneway(self):
        self.calls += 1


@Pyro5.server.expose
@Pyro5.server.interface_version(2)
class VersionedApiV2:
    def __init__(self):
        self.calls = 0
        self.value_writes = 0
        self._value = 2

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self.value_writes += 1
        self._value = value

    def call(self):
        self.calls += 1
        return "v2"

    @Pyro5.server.oneway
    def call_oneway(self):
        self.calls += 1


@Pyro5.server.expose
class UnversionedApi:
    def call(self):
        return "unversioned"


class MemoryBuffer:
    def __init__(self, closing):
        self.data = b""
        self.condition = threading.Condition()
        self.closing = closing

    def add(self, data):
        with self.condition:
            self.data += bytes(data)
            self.condition.notify_all()

    def recv(self, size):
        with self.condition:
            while len(self.data) < size:
                if not self.condition.wait(0.05) and self.closing.is_set():
                    raise Pyro5.errors.ConnectionClosedError("memory connection closed")
            data, self.data = self.data[:size], self.data[size:]
            return data


class MemorySocket:
    family = socket.AF_INET

    def __init__(self, inbound, outbound):
        self.inbound = inbound
        self.outbound = outbound

    def send(self, data):
        self.inbound.add(data)

    def sendall(self, data):
        self.send(data)

    def recv(self, size, flags=0):
        return self.outbound.recv(size)

    def getsockname(self):
        return "memory", 0

    def getpeername(self):
        return "memory", 0

    def gettimeout(self):
        return None

    def settimeout(self, timeout):
        pass

    @property
    def timeout(self):
        return None

    @timeout.setter
    def timeout(self, value):
        pass

    def shutdown(self, how):
        pass

    def close(self):
        pass


class MemoryDaemonThread(threading.Thread):
    def __init__(self, daemon, connection, closing):
        super().__init__()
        self.daemon = True
        self.pyrodaemon = daemon
        self.connection = connection
        self.closing = closing

    def run(self):
        if not self.pyrodaemon._handshake(self.connection):
            return
        for _ in range(50):
            if self.closing.is_set():
                return
            try:
                self.pyrodaemon.handleRequest(self.connection)
            except Pyro5.errors.ConnectionClosedError:
                return


def create_connected_proxy(client_socket, object_id):
    serializer = Pyro5.serializers.serializers["serpent"]
    data = serializer.dumps({"handshake": "hello", "object": object_id})
    message = Pyro5.protocol.SendingMessage(
        Pyro5.protocol.MSG_CONNECT, 0, 0, serializer.serializer_id, data)
    client_socket.sendall(message.data)
    response = Pyro5.protocol.recv_stub(
        Pyro5.socketutil.SocketConnection(client_socket, object_id, True),
        [Pyro5.protocol.MSG_CONNECTOK])
    handshake = serializer.loads(response.data)
    proxy = Pyro5.client.Proxy(Pyro5.core.URI("PYRO:%s@memory:0" % object_id))
    proxy._pyroConnection = Pyro5.socketutil.SocketConnection(client_socket, object_id, True)
    proxy._pyroLocalSocket = client_socket.getsockname()
    proxy._pyroGetMetadata(known_metadata=handshake["meta"])
    return proxy


@pytest.fixture
def memory_connection():
    closing = threading.Event()
    client_to_server = MemoryBuffer(closing)
    server_to_client = MemoryBuffer(closing)
    server_socket = MemorySocket(client_to_server, server_to_client)
    client_socket = MemorySocket(server_to_client, client_to_server)
    daemon = Pyro5.server.Daemon(connected_socket=server_socket)
    server_connection = daemon.transportServer.conn
    thread = MemoryDaemonThread(daemon, server_connection, closing)
    thread.start()
    try:
        yield daemon, client_socket
    finally:
        closing.set()
        daemon.shutdown()
        thread.join(1)


class TestInterfaceVersion:
    def test_refresh_required_after_method_and_property_replacement(self, memory_connection):
        daemon, client_socket = memory_connection
        old_api = VersionedApiV1()
        new_api = VersionedApiV2()
        uri = daemon.register(old_api, "versioned-api")

        proxy = create_connected_proxy(client_socket, uri.object)

        with proxy:
            assert proxy._pyroInterfaceVersion == 1
            assert proxy.call() == "v1"
            assert proxy.value == 1

            daemon.register(new_api, uri.object, force=True)

            with pytest.raises(Pyro5.errors.StaleInterfaceError) as exc:
                proxy.call()
            assert exc.value.actual == 1
            assert exc.value.expected == 2
            with pytest.raises(Pyro5.errors.StaleInterfaceError):
                proxy.value
            with pytest.raises(Pyro5.errors.StaleInterfaceError):
                proxy.value = 99
            assert new_api.calls == 0
            assert new_api.value_writes == 0
            assert not isinstance(exc.value, Pyro5.errors.CommunicationError)

            proxy._pyroRefresh()
            assert proxy._pyroInterfaceVersion == 2
            assert proxy.call() == "v2"
            assert proxy.value == 2
            proxy.value = 99
            assert new_api._value == 99
            assert new_api.value_writes == 1
            assert new_api.calls == 1

    def test_batch_and_oneway_are_validated_before_dispatch(self, memory_connection):
        daemon, client_socket = memory_connection
        old_api = VersionedApiV1()
        new_api = VersionedApiV2()
        uri = daemon.register(old_api, "versioned-batch-api")

        proxy = create_connected_proxy(client_socket, uri.object)

        with proxy:
            assert proxy.call() == "v1"
            daemon.register(new_api, uri.object, force=True)

            batch = Pyro5.client.BatchProxy(proxy)
            batch.call()
            with pytest.raises(Pyro5.errors.StaleInterfaceError):
                next(batch())
            assert new_api.calls == 0

            with pytest.raises(Pyro5.errors.StaleInterfaceError):
                proxy.call_oneway()
            time.sleep(0.2)
            assert new_api.calls == 0

            proxy._pyroRefreshMetadata()
            proxy.call_oneway()
            deadline = time.time() + 1
            while time.time() < deadline and new_api.calls == 0:
                threading.Event().wait(0.01)
            assert new_api.calls == 1

    def test_unversioned_object_remains_compatible(self, memory_connection):
        daemon, client_socket = memory_connection
        api = UnversionedApi()
        uri = daemon.register(api, "unversioned-api")

        proxy = create_connected_proxy(client_socket, uri.object)

        with proxy:
            assert proxy._pyroInterfaceVersionNegotiated is True
            assert proxy._pyroInterfaceVersion is None
            assert proxy.call() == "unversioned"
