"""
Tests for opt-in invocation deduplication.
"""

import threading
import time

import pytest

import Pyro5.client
import Pyro5.dedup
import Pyro5.protocol
import Pyro5.server
import Pyro5.serializers
from Pyro5 import config
from Pyro5.errors import PyroError, SecurityError
from support import ConnectionMock


class FakeSocket:
    def getpeername(self):
        return ("127.0.0.1", 12345)


@Pyro5.server.expose
class DedupService(object):
    def __init__(self):
        self.calls = 0
        self.lock = threading.Lock()

    @Pyro5.server.dedup
    def append(self, amount):
        with self.lock:
            self.calls += amount
        return {"calls": self.calls, "amount": amount}

    @Pyro5.server.dedup
    def fail(self, amount):
        with self.lock:
            self.calls += amount
        raise RuntimeError("boom")

    def unmarked(self):
        with self.lock:
            self.calls += 1
        return self.calls

    @Pyro5.server.dedup
    def stream_values(self):
        yield "one"
        yield "two"


def disabled_server():
    config.reset(False)
    daemon = object.__new__(Pyro5.server.Daemon)
    daemon.objectsById = {}
    daemon._pyroInstances = {}
    daemon.streaming_responses = {}
    daemon.housekeeper_lock = threading.Lock()
    daemon._disconnect_lock = threading.Lock()
    daemon.create_single_instance_lock = threading.Lock()
    daemon.methodcall_error_handler = Pyro5.server._default_methodcall_error_handler
    daemon._dedup_enabled = False
    daemon.deduplication_store = None
    service = DedupService()
    service._pyroId = "dedup-service"
    service._pyroDaemon = daemon
    daemon.objectsById["dedup-service"] = service
    return daemon, service


@pytest.fixture
def server():
    config.reset(False)
    daemon = object.__new__(Pyro5.server.Daemon)
    daemon.objectsById = {}
    daemon._pyroInstances = {}
    daemon.streaming_responses = {}
    daemon.housekeeper_lock = threading.Lock()
    daemon._disconnect_lock = threading.Lock()
    daemon.create_single_instance_lock = threading.Lock()
    daemon.methodcall_error_handler = Pyro5.server._default_methodcall_error_handler
    daemon._dedup_enabled = True
    daemon.deduplication_store = Pyro5.dedup.DeduplicationStore()
    service = DedupService()
    service._pyroId = "dedup-service"
    service._pyroDaemon = daemon
    daemon.objectsById["dedup-service"] = service
    try:
        yield daemon, service
    finally:
        pass


def invoke_message(seq, method, args=(), kwargs=None, key=None, scope="scope-a", flags=0):
    serializer = Pyro5.serializers.SerpentSerializer()
    annotations = {}
    if key is not None:
        annotations[Pyro5.dedup.ANNOTATION] = Pyro5.dedup.encode_call_key(key)
    data = serializer.dumpsCall("dedup-service", method, args, kwargs or {})
    msg = Pyro5.protocol.SendingMessage(Pyro5.protocol.MSG_INVOKE, flags, seq,
                                        serializer.serializer_id, data, annotations)
    return ConnectionMock(msg)


def handle(daemon, connection):
    connection.pyroInstances = {}
    connection.sock = FakeSocket()
    if not hasattr(connection, "pyro_dedup_scope"):
        connection.pyro_dedup_scope = "scope-a"
    try:
        daemon.handleRequest(connection)
    except SecurityError:
        pass
    response = Pyro5.protocol.recv_stub(ConnectionMock(connection.received), [Pyro5.protocol.MSG_RESULT])
    serializer = Pyro5.serializers.serializers_by_id[response.serializer_id]
    return response, serializer.loads(response.data)


class TestDeduplicationProtocol:
    def test_retry_after_first_reply_is_discarded(self, server):
        daemon, service = server
        first_connection = invoke_message(1, "append", (2,), key="call-1")
        first_response, result = handle(daemon, first_connection)
        assert result == {"calls": 2, "amount": 2}
        assert not (first_response.flags & Pyro5.protocol.FLAGS_EXCEPTION)
        # Simulate the client losing the complete first response immediately after the server wrote it.
        del first_response, result
        assert service.calls == 2

        retry_connection = invoke_message(2, "append", (2,), key="call-1")
        retry_response, retry_result = handle(daemon, retry_connection)
        assert retry_result == {"calls": 2, "amount": 2}
        assert not (retry_response.flags & Pyro5.protocol.FLAGS_EXCEPTION)
        assert service.calls == 2

    def test_same_key_with_different_arguments_rejected(self, server):
        daemon, service = server
        _, result = handle(daemon, invoke_message(1, "append", (3,), key="call-2"))
        assert result == {"calls": 3, "amount": 3}

        response, result = handle(daemon, invoke_message(2, "append", (4,), key="call-2"))
        with pytest.raises(PyroError, match="different arguments"):
            raise result
        assert response.flags & Pyro5.protocol.FLAGS_EXCEPTION
        assert service.calls == 3

    def test_scope_is_part_of_identity(self, server):
        daemon, service = server
        conn = invoke_message(1, "append", (5,), key="call-3")
        conn.pyro_dedup_scope = "scope-a"
        _, first = handle(daemon, conn)
        assert first == {"calls": 5, "amount": 5}

        conn = invoke_message(2, "append", (5,), key="call-3")
        conn.pyro_dedup_scope = "scope-b"
        _, second = handle(daemon, conn)
        assert second == {"calls": 10, "amount": 5}
        assert service.calls == 10

    def test_failure_is_replayed(self, server):
        daemon, service = server
        response, result = handle(daemon, invoke_message(1, "fail", (2,), key="call-4"))
        assert response.flags & Pyro5.protocol.FLAGS_EXCEPTION
        with pytest.raises(RuntimeError, match="boom"):
            raise result

        response, result = handle(daemon, invoke_message(2, "fail", (2,), key="call-4"))
        assert response.flags & Pyro5.protocol.FLAGS_EXCEPTION
        with pytest.raises(RuntimeError, match="boom"):
            raise result
        assert service.calls == 2

    def test_untrusted_caller_cannot_dedup(self, server):
        daemon, service = server
        conn = invoke_message(1, "append", (1,), key="call-5")
        conn.pyro_dedup_scope = None
        response, result = handle(daemon, conn)
        assert response.flags & Pyro5.protocol.FLAGS_EXCEPTION
        with pytest.raises(SecurityError):
            raise result
        assert service.calls == 0

    def test_streaming_result_rejects_dedup(self, server):
        daemon, service = server
        response, result = handle(daemon, invoke_message(1, "stream_values", key="call-7"))
        assert response.flags & Pyro5.protocol.FLAGS_EXCEPTION
        assert not (response.flags & Pyro5.protocol.FLAGS_ITEMSTREAMRESULT)
        with pytest.raises(PyroError, match="streaming results"):
            raise result

    def test_daemon_must_explicitly_enable_dedup(self):
        daemon, service = disabled_server()
        response, result = handle(daemon, invoke_message(1, "append", (1,), key="call-8"))
        assert response.flags & Pyro5.protocol.FLAGS_EXCEPTION
        with pytest.raises(PyroError, match="disabled"):
            raise result
        assert service.calls == 0

    def test_unmarked_method_rejects_dedup(self, server):
        daemon, service = server
        response, result = handle(daemon, invoke_message(1, "unmarked", key="call-6"))
        assert response.flags & Pyro5.protocol.FLAGS_EXCEPTION
        with pytest.raises(PyroError, match="not marked"):
            raise result
        assert service.calls == 0


class TestDeduplicationStore:
    def test_in_flight_wait_replays_result(self):
        store = Pyro5.dedup.DeduplicationStore(completed_ttl=30, failed_ttl=30)
        identity = ("scope", "object", "method", "key")
        token = store.begin(identity, ((1,), {}))
        errors = []

        def duplicate():
            try:
                replay = store.begin(identity, ((1,), {}))
                assert replay.replayed
                payload, failed, serializer_id = replay.response()
                assert not failed
                assert payload == b"ok"
                assert serializer_id == 7
                replay.release()
            except Exception as exc:
                errors.append(exc)

        waiter = threading.Thread(target=duplicate)
        waiter.start()
        time.sleep(0.02)
        token.complete(b"ok", 7)
        waiter.join(1)
        assert not errors
        assert not waiter.is_alive()

    def test_in_flight_reject(self):
        store = Pyro5.dedup.DeduplicationStore()
        store.begin(("scope", "object", "method", "key"), ((), {}))
        with pytest.raises(Pyro5.dedup.CallInProgressError):
            store.begin(("scope", "object", "method", "key"), ((), {}), in_flight="reject")

    def test_records_have_count_and_ttl_limits(self):
        store = Pyro5.dedup.DeduplicationStore(completed_ttl=0.001, failed_ttl=0.001, max_records=1)
        first = store.begin(("scope", "object", "first", "key"), ((), {}))
        first.complete(b"one", 7)
        time.sleep(0.002)
        second = store.begin(("scope", "object", "second", "key"), ((), {}))
        second.complete(b"two", 7)
        store.prune()
        assert len(store._records) == 1

    def test_parameter_mismatch_does_not_use_payload_identity(self):
        store = Pyro5.dedup.DeduplicationStore()
        identity_a = ("scope-a", "object", "method", "key")
        identity_b = ("scope-b", "object", "method", "key")
        token = store.begin(identity_a, ((b"abc",), {}))
        token.complete(b"ok", 7)
        replay = store.begin(identity_a, ((b"abc",), {}))
        assert replay.replayed
        replay.release()
        token_b = store.begin(identity_b, ((b"xyz",), {}))
        assert not token_b.replayed




class FakeProtocolConnection:
    def __init__(self, response):
        self.received = b""
        self.response = response
        self.objectId = "dedup-service"

    def send(self, data):
        self.received += data

    def recv(self, size):
        chunk = self.response[:size]
        self.response = self.response[size:]
        return chunk

    def close(self):
        pass

    def family(self):
        return "IPv4"

    def getpeername(self):
        return ("127.0.0.1", 1)


class TestDeduplicationClientApi:
    def test_call_key_is_only_attached_to_business_call(self):
        metadata = {"methods": {"ping"}, "attrs": set(), "oneway": set()}
        serializer = Pyro5.serializers.SerpentSerializer()
        response_payload = serializer.dumps(metadata)
        response = Pyro5.protocol.SendingMessage(
            Pyro5.protocol.MSG_RESULT, 0, 1, serializer.serializer_id, response_payload).data
        proxy = Pyro5.client.Proxy("PYRO:dedup-service@localhost:9999")
        connection = FakeProtocolConnection(response)
        proxy._pyroConnection = connection
        proxy._pyroSeq = 0
        Pyro5.client.current_context.annotations = {}
        with Pyro5.dedup.call_key("metadata-key"):
            proxy._pyroGetMetadata(known_metadata=None)
        Pyro5.client.current_context.annotations = {}
        message = Pyro5.protocol.recv_stub(FakeProtocolConnection(connection.received),
                                          [Pyro5.protocol.MSG_INVOKE])
        assert Pyro5.dedup.ANNOTATION not in message.annotations
        proxy._pyroConnection = None

    def test_business_call_carries_deduplication_annotation(self):
        serializer = Pyro5.serializers.SerpentSerializer()
        response = Pyro5.protocol.SendingMessage(
            Pyro5.protocol.MSG_RESULT, 0, 1, serializer.serializer_id, serializer.dumps(None)).data
        proxy = Pyro5.client.Proxy("PYRO:dedup-service@localhost:9999")
        proxy._pyroMethods = {"ping"}
        Pyro5.client.current_context.annotations = {}
        connection = FakeProtocolConnection(response)
        proxy._pyroConnection = connection
        with Pyro5.dedup.call_key("call-key"):
            assert proxy.ping() is None
        message = Pyro5.protocol.recv_stub(FakeProtocolConnection(connection.received),
                                          [Pyro5.protocol.MSG_INVOKE])
        assert Pyro5.dedup.decode_call_key(message.annotations[Pyro5.dedup.ANNOTATION]) == "call-key"
        proxy._pyroConnection = None

    def test_reserved_annotation_cannot_be_supplied(self):
        serializer = Pyro5.serializers.SerpentSerializer()
        response = Pyro5.protocol.SendingMessage(
            Pyro5.protocol.MSG_RESULT, 0, 1, serializer.serializer_id, serializer.dumps(None)).data
        proxy = Pyro5.client.Proxy("PYRO:dedup-service@localhost:9999")
        proxy._pyroMethods = {"ping"}
        proxy._pyroConnection = FakeProtocolConnection(response)
        Pyro5.client.current_context.annotations = {Pyro5.dedup.ANNOTATION: b"forged"}
        try:
            with Pyro5.dedup.call_key("call-key"):
                with pytest.raises(PyroError, match="reserved"):
                    proxy.ping()
        finally:
            Pyro5.client.current_context.annotations = {}
            proxy._pyroConnection = None
