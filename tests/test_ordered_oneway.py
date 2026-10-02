import threading
import time

import pytest

import Pyro5.client
import Pyro5.errors
import Pyro5.orderedoneway
import Pyro5.protocol
import Pyro5.server
from Pyro5 import config


class OrderedTestTarget:
    def __init__(self):
        self.event = threading.Event()
        self.log = []
        self.lock = threading.Lock()

    def append(self, value):
        if value == "blocked":
            assert self.event.wait(5)
        with self.lock:
            self.log.append(value)

    def fail(self, value):
        with self.lock:
            self.log.append(value)
        raise RuntimeError("boom")

    def handle_ordered_oneway_error(self, info):
        self.errors.append(info)


class DummyDaemon:
    def __init__(self):
        self.methodcall_error_handler = Pyro5.server._default_methodcall_error_handler


def wait_until(predicate, timeout=2):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("condition was not reached")


def submit(connection, dispatcher, target, sequence, value, policy=Pyro5.orderedoneway.POLICY_CONTINUE):
    options = {
        "id": "unit-channel",
        "sequence": sequence,
        "maxsize": 2,
        "policy": policy,
        "error_uri": None,
        "error_method": "handle_ordered_oneway_error",
    }
    return dispatcher.submit(connection, "object", target.append, (value,), {}, options, {
        "client": None,
        "client_sock_addr": None,
        "seq": sequence,
        "msg_flags": 0,
        "serializer_id": 0,
        "annotations": {},
        "response_annotations": {},
        "correlation_id": None,
        "channel_id": "unit-channel",
    })


class TestOrderedOnewayDispatcher:
    def setup_method(self):
        config.ORDERED_ONEWAY_QUEUE_SIZE = 2
        config.ORDERED_ONEWAY_WORKERS = 1
        self.dispatcher = Pyro5.orderedoneway._OrderedOnewayDispatcher(DummyDaemon())
        self.connection = object()
        self.target = OrderedTestTarget()
        self.dispatcher.start()

    def teardown_method(self):
        self.target.event.set()
        self.dispatcher.close()

    def test_queue_bound_and_order(self):
        receipts = [submit(self.connection, self.dispatcher, self.target, 1, "blocked")]
        wait_until(lambda: self.dispatcher.channels["unit-channel"].active_call is not None)
        receipts.append(submit(self.connection, self.dispatcher, self.target, 2, "second"))
        receipts.append(submit(self.connection, self.dispatcher, self.target, 3, "third"))
        with pytest.raises(Pyro5.errors.OrderedOnewayQueueFullError):
            submit(self.connection, self.dispatcher, self.target, 4, "fourth")
        assert [receipt["sequence"] for receipt in receipts] == [1, 2, 3]
        assert [receipt["pending"] for receipt in receipts] == [0, 1, 2]
        self.target.event.set()
        wait_until(lambda: self.target.log == ["blocked", "second", "third"])

    def test_failure_continues_by_policy(self):
        target = self.target

        def fail(value):
            target.fail(value)

        self.dispatcher.submit(self.connection, "object", fail, ("failed",), {}, {
            "id": "continue-channel", "sequence": 1, "maxsize": 2,
            "policy": Pyro5.orderedoneway.POLICY_CONTINUE,
            "error_uri": None, "error_method": "handle_ordered_oneway_error",
        }, {"client": None, "client_sock_addr": None, "seq": 1, "msg_flags": 0,
            "serializer_id": 0, "annotations": {}, "response_annotations": {},
            "correlation_id": None, "channel_id": "continue-channel"})
        self.dispatcher.submit(self.connection, "object", target.append, ("continued",), {}, {
            "id": "continue-channel", "sequence": 2, "maxsize": 2,
            "policy": Pyro5.orderedoneway.POLICY_CONTINUE,
            "error_uri": None, "error_method": "handle_ordered_oneway_error",
        }, {"client": None, "client_sock_addr": None, "seq": 2, "msg_flags": 0,
            "serializer_id": 0, "annotations": {}, "response_annotations": {},
            "correlation_id": None, "channel_id": "continue-channel"})
        wait_until(lambda: target.log == ["failed", "continued"])

    def test_failed_error_receiver_does_not_block_channel(self):
        class FailingErrorProxy:
            def __init__(self, uri):
                self.uri = uri

            def __getattr__(self, name):
                def method(info):
                    raise RuntimeError("receiver unavailable")
                return method

            def release(self):
                pass

            def close(self):
                pass

        original_proxy = Pyro5.client.Proxy
        Pyro5.client.Proxy = FailingErrorProxy
        try:
            target = self.target

            def fail(value):
                target.fail(value)

            self.dispatcher.submit(self.connection, "object", fail, ("failed",), {}, {
                "id": "bad-receiver-channel", "sequence": 1, "maxsize": 2,
                "policy": Pyro5.orderedoneway.POLICY_CONTINUE,
                "error_uri": "PYRO:errors@localhost:1", "error_method": "handle_ordered_oneway_error",
            }, {"client": None, "client_sock_addr": None, "seq": 1, "msg_flags": 0,
                "serializer_id": 0, "annotations": {}, "response_annotations": {},
                "correlation_id": None, "channel_id": "bad-receiver-channel"})
            self.dispatcher.submit(self.connection, "object", target.append, ("continued",), {}, {
                "id": "bad-receiver-channel", "sequence": 2, "maxsize": 2,
                "policy": Pyro5.orderedoneway.POLICY_CONTINUE,
                "error_uri": "PYRO:errors@localhost:1", "error_method": "handle_ordered_oneway_error",
            }, {"client": None, "client_sock_addr": None, "seq": 2, "msg_flags": 0,
                "serializer_id": 0, "annotations": {}, "response_annotations": {},
                "correlation_id": None, "channel_id": "bad-receiver-channel"})
            wait_until(lambda: target.log == ["failed", "continued"])
        finally:
            Pyro5.client.Proxy = original_proxy

    def test_failure_stops_accepted_following_calls(self):
        reports = []

        class RecordingErrorProxy:
            def __init__(self, uri):
                pass

            def __getattr__(self, name):
                def method(info):
                    reports.append(info)
                return method

            def _pyroRelease(self):
                pass

            _pyroTimeout = None

        def fail(value):
            self.target.fail(value)

        original_proxy = Pyro5.client.Proxy
        Pyro5.client.Proxy = RecordingErrorProxy
        try:
            self.dispatcher.submit(self.connection, "object", fail, ("failed",), {}, {
                "id": "stop-channel", "sequence": 1, "maxsize": 2,
                "policy": Pyro5.orderedoneway.POLICY_STOP,
                "error_uri": "PYRO:errors@localhost:1", "error_method": "handle_ordered_oneway_error",
            }, {"client": None, "client_sock_addr": None, "seq": 1, "msg_flags": 0,
                "serializer_id": 0, "annotations": {}, "response_annotations": {},
                "correlation_id": None, "channel_id": "stop-channel"})
            self.dispatcher.submit(self.connection, "object", self.target.append, ("skipped",), {}, {
                "id": "stop-channel", "sequence": 2, "maxsize": 2,
                "policy": Pyro5.orderedoneway.POLICY_STOP,
                "error_uri": "PYRO:errors@localhost:1", "error_method": "handle_ordered_oneway_error",
            }, {"client": None, "client_sock_addr": None, "seq": 2, "msg_flags": 0,
                "serializer_id": 0, "annotations": {}, "response_annotations": {},
                "correlation_id": None, "channel_id": "stop-channel"})
            wait_until(lambda: len(reports) == 2)
            assert self.target.log == ["failed"]
            assert [info["sequence"] for info in reports] == [1, 2]
            assert reports[1]["phase"] == "aborted"
        finally:
            Pyro5.client.Proxy = original_proxy

    def test_local_error_receiver_failure_does_not_block_channel(self):
        reports = []

        def receiver(info):
            reports.append(info)
            raise RuntimeError("local receiver failed")

        def fail(value):
            self.target.fail(value)

        self.dispatcher.submit(self.connection, "object", fail, ("failed",), {}, {
            "id": "local-error-channel", "sequence": 1, "maxsize": 2,
            "policy": Pyro5.orderedoneway.POLICY_CONTINUE,
            "error_uri": receiver, "error_method": "ignored",
        }, {"client": None, "client_sock_addr": None, "seq": 1, "msg_flags": 0,
            "serializer_id": 0, "annotations": {}, "response_annotations": {},
            "correlation_id": None, "channel_id": "local-error-channel"})
        self.dispatcher.submit(self.connection, "object", self.target.append, ("continued",), {}, {
            "id": "local-error-channel", "sequence": 2, "maxsize": 2,
            "policy": Pyro5.orderedoneway.POLICY_CONTINUE,
            "error_uri": receiver, "error_method": "ignored",
        }, {"client": None, "client_sock_addr": None, "seq": 2, "msg_flags": 0,
            "serializer_id": 0, "annotations": {}, "response_annotations": {},
            "correlation_id": None, "channel_id": "local-error-channel"})
        wait_until(lambda: self.target.log == ["failed", "continued"])
        wait_until(lambda: len(reports) == 1)

    def test_close_aborts_pending_external_report(self):
        reports = []

        class RecordingErrorProxy:
            def __init__(self, uri):
                pass

            def __getattr__(self, name):
                def method(info):
                    reports.append(info)
                return method

            def _pyroRelease(self):
                pass

            _pyroTimeout = None

        self.dispatcher.daemon.locationStr = "localhost:1"
        original_proxy = Pyro5.client.Proxy
        Pyro5.client.Proxy = RecordingErrorProxy
        try:
            options = {"id": "unit-channel", "maxsize": 2,
                       "policy": Pyro5.orderedoneway.POLICY_CONTINUE,
                       "error_uri": "PYRO:errors@otherhost:2",
                       "error_method": "handle_ordered_oneway_error"}
            context = {"client": None, "client_sock_addr": None, "msg_flags": 0,
                       "serializer_id": 0, "annotations": {}, "response_annotations": {},
                       "correlation_id": None, "channel_id": "unit-channel"}
            for sequence, value in [(1, "blocked"), (2, "second"), (3, "third")]:
                options["sequence"] = sequence
                context["seq"] = sequence
                self.dispatcher.submit(self.connection, "object", self.target.append, (value,), {},
                                       options, dict(context))
                if sequence == 1:
                    wait_until(lambda: self.dispatcher.channels["unit-channel"].active_call is not None)
            self.dispatcher.close()
            wait_until(lambda: len(reports) == 2)
        finally:
            Pyro5.client.Proxy = original_proxy
        assert self.target.log == []
        assert {info["sequence"] for info in reports} == {2, 3}
        assert all(info["phase"] == "aborted" for info in reports)

    def test_client_rejects_invalid_arguments(self):
        class DummyProxy:
            _pyroCapabilities = {Pyro5.protocol.CAP_ORDERED_ONEWAY}

            def _pyroBind(self):
                return True

        with pytest.raises(ValueError):
            Pyro5.client.OrderedOnewayChannel(DummyProxy(), queue_size=0)
        with pytest.raises(ValueError):
            Pyro5.client.OrderedOnewayChannel(DummyProxy(), on_failure="abandon")
        with pytest.raises(TypeError):
            Pyro5.client.OrderedOnewayChannel(DummyProxy(), on_error=123)


@Pyro5.server.expose
class IntegrationTarget:
    def __init__(self):
        self.gate = threading.Event()
        self.log = []
        self.lock = threading.Lock()
        self.errors = []
        self.bad_receiver_called = threading.Event()

    def blocked(self, value):
        self.gate.wait(5)
        with self.lock:
            self.log.append(value)

    def record(self, value):
        with self.lock:
            self.log.append(value)

    def fail(self, value):
        with self.lock:
            self.log.append(value)
        raise RuntimeError("business failure")

    def handle_ordered_oneway_error(self, info):
        with self.lock:
            self.errors.append(info)

    def bad_receiver(self, info):
        self.bad_receiver_called.set()
        raise RuntimeError("error receiver failed")


@Pyro5.server.expose
class BadErrorReceiver:
    invoked = threading.Event()

    def handle_ordered_oneway_error(self, info):
        type(self).invoked.set()
        raise RuntimeError("receiver boom")


class DaemonThread(threading.Thread):
    def __init__(self, daemon):
        super().__init__(daemon=True)
        self.daemon = daemon
        self.started = threading.Event()

    def run(self):
        self.started.set()
        self.daemon.requestLoop()


class LegacyOrderedDaemon(Pyro5.server.Daemon):
    def _handshake(self, conn, denied_reason=None):
        import Pyro5.protocol
        import Pyro5.serializers
        msg = Pyro5.protocol.recv_stub(conn, [Pyro5.protocol.MSG_CONNECT])
        serializer = Pyro5.serializers.serializers_by_id[msg.serializer_id]
        data = serializer.dumps({
            "handshake": "hello",
            "meta": self.objectsById[Pyro5.core.DAEMON_NAME].get_metadata("ordered-target"),
        })
        response = Pyro5.protocol.SendingMessage(Pyro5.protocol.MSG_CONNECTOK, 0,
                                                 msg.seq, msg.serializer_id, data)
        conn.send(response.data)
        return True


class TestOrderedOnewayIntegration:
    def setup_method(self):
        config.SERIALIZER = "serpent"
        config.ORDERED_ONEWAY_QUEUE_SIZE = 2
        config.ORDERED_ONEWAY_ERROR_TIMEOUT = 1
        self.daemon = Pyro5.server.Daemon(port=0)
        self.target = IntegrationTarget()
        self.uri = self.daemon.register(self.target, "ordered-target")
        self.daemon_thread = DaemonThread(self.daemon)
        self.daemon_thread.start()
        self.daemon_thread.started.wait()
        time.sleep(0.05)

    def teardown_method(self):
        self.target.gate.set()
        self.daemon.shutdown()
        self.daemon_thread.join(2)

    def test_acceptance_queue_full_order_and_error_report(self):
        with Pyro5.client.Proxy(self.uri) as proxy:
            with Pyro5.client.OrderedOnewayChannel(
                    proxy, queue_size=2, on_error=self.uri) as channel:
                first = channel.submit("blocked", "first")
                time.sleep(0.1)
                second = channel.submit("fail", "second")
                third = channel.submit("record", "third")
                with pytest.raises(Pyro5.errors.OrderedOnewayQueueFullError):
                    channel.submit("record", "fourth")
                assert [receipt["sequence"] for receipt in (first, second, third)] == [1, 2, 3]
                assert [receipt["pending"] for receipt in (first, second, third)] == [0, 1, 2]
                self.target.gate.set()
                wait_until(lambda: self.target.log == ["first", "second", "third"])
                wait_until(lambda: len(self.target.errors) == 1)
                assert self.target.errors[0]["sequence"] == 2
                assert self.target.errors[0]["error_type"] == "RuntimeError"

    def test_error_receiver_failure_keeps_channel_running(self):
        bad_receiver = BadErrorReceiver()
        error_uri = self.daemon.register(bad_receiver, "bad-receiver")
        with Pyro5.client.Proxy(self.uri) as proxy:
            with Pyro5.client.OrderedOnewayChannel(
                    proxy, queue_size=2, on_error=error_uri,
                    error_method="handle_ordered_oneway_error") as channel:
                channel.submit("fail", "failed")
                channel.submit("record", "continued")
                wait_until(lambda: self.target.log == ["failed", "continued"])
                wait_until(bad_receiver.invoked.is_set)

    def test_disconnect_does_not_cancel_accepted_calls(self):
        proxy = Pyro5.client.Proxy(self.uri)
        channel = Pyro5.client.OrderedOnewayChannel(proxy, queue_size=2)
        channel.submit("blocked", "after-disconnect")
        channel.submit("record", "queued")
        time.sleep(0.1)
        proxy._pyroRelease()
        proxy._pyroBind()
        with pytest.raises(Pyro5.errors.ProtocolError):
            channel.submit("record", "after-disconnect-rejected")
        self.target.gate.set()
        wait_until(lambda: self.target.log == ["after-disconnect", "queued"])

    def test_daemon_close_rejects_new_submission(self):
        dispatcher = self.daemon._ordered_oneway_dispatcher
        self.daemon.close()
        with pytest.raises(Pyro5.errors.OrderedOnewayChannelClosedError):
            dispatcher.submit(None, "object", lambda: None, (), {}, {
                "id": "closed", "sequence": 1, "maxsize": 2,
                "policy": Pyro5.orderedoneway.POLICY_CONTINUE,
                "error_uri": None, "error_method": "handle_ordered_oneway_error",
            }, {})


class TestOrderedOnewayCapability:
    def test_unsupported_peer_is_rejected(self):
        daemon = LegacyOrderedDaemon(port=0)
        target = IntegrationTarget()
        uri = daemon.register(target, "ordered-target")
        thread = DaemonThread(daemon)
        thread.start()
        thread.started.wait()
        time.sleep(0.05)
        try:
            with Pyro5.client.Proxy(uri) as proxy:
                with pytest.raises(Pyro5.errors.OrderedOnewayUnsupportedError):
                    Pyro5.client.OrderedOnewayChannel(proxy, queue_size=2)
        finally:
            target.gate.set()
            daemon.shutdown()
            thread.join(2)
