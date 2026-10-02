"""
Tests for the optional ordered oneway channel.

Pyro - Python Remote Objects.  Copyright by Irmen de Jong (irmen@razorvine.net).
"""

import marshal
import threading
import time
from types import SimpleNamespace

import pytest

import Pyro5.client
import Pyro5.callcontext
import Pyro5.errors
import Pyro5.protocol
import Pyro5.serializers
import Pyro5.server
from Pyro5 import config
from support import ConnectionMock


@Pyro5.server.expose
class OrderedService(object):
    def __init__(self):
        self.started = threading.Event()
        self.release = threading.Event()
        self.log = []

    def block_first(self):
        self.started.set()
        self.release.wait(5)
        self.log.append("first")

    def append(self, value):
        self.log.append(value)

    def fail(self, value):
        raise ValueError(value)


@Pyro5.server.expose
class ErrorHandler(object):
    def __init__(self, errors=None, block=None, fail=False):
        self.errors = errors
        self.block = block
        self.fail = fail

    def ordered_oneway_error(self, report):
        if self.block is not None:
            self.block.set()
            time.sleep(2)
        if self.fail:
            raise RuntimeError("receiver failure")
        self.errors.append(report)


class DaemonLoopThread(threading.Thread):
    def __init__(self, daemon):
        super().__init__()
        self.daemon = True
        self.pyro_daemon = daemon
        self.running = threading.Event()

    def run(self):
        self.running.set()
        self.pyro_daemon.requestLoop()


@pytest.fixture
def ordered_service(request):
    old_queue_size = config.ORDERED_ONEWAY_QUEUE_SIZE
    config.ORDERED_ONEWAY_QUEUE_SIZE = 2
    on_failure = getattr(request, "param", "continue")
    daemon = Pyro5.server.Daemon(port=0, ordered_oneway_on_failure=on_failure)
    service = OrderedService()
    uri = daemon.register(service, "ordered")
    thread = DaemonLoopThread(daemon)
    thread.start()
    thread.running.wait()
    time.sleep(0.05)
    try:
        yield daemon, service, uri
    finally:
        service.release.set()
        time.sleep(0.05)
        daemon.shutdown()
        thread.join(timeout=5)
        config.ORDERED_ONEWAY_QUEUE_SIZE = old_queue_size


def wait_for(condition, timeout=3):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if condition():
            return True
        time.sleep(0.01)
    return condition()


def test_server_acceptance_order_and_full_rejection_without_socket():
    service = OrderedService()
    daemon = object.__new__(Pyro5.server.Daemon)
    daemon.ordered_oneway_error_receiver = None
    daemon.objectsById = {"ordered": service}
    daemon.methodcall_error_handler = lambda *args: None
    daemon.annotations = lambda: {}
    daemon.ordered_oneway_channel = Pyro5.server._OrderedOnewayChannel(daemon, 2, "continue")
    serializer = Pyro5.serializers.serializers["marshal"]

    def invoke(sequence, method, args=()):
        Pyro5.callcontext.current_context.response_annotations = {}
        Pyro5.callcontext.current_context.client_sock_addr = None
        annotations = {
            "O1WY": marshal.dumps((
                sequence, "continue", "ordered_oneway_error",
                serializer.serializer_id, b""
            ))
        }
        message = SimpleNamespace(
            flags=Pyro5.protocol.FLAGS_ORDERED_ONEWAY,
            annotations=annotations
        )
        connection = ConnectionMock()
        daemon._handleOrderedOneway(
            connection, sequence, serializer, message,
            "ordered", method, args, {}
        )
        reply = Pyro5.protocol.recv_stub(connection, [Pyro5.protocol.MSG_RESULT])
        return serializer.loads(reply.data)

    first = invoke(1, "block_first")
    assert service.started.wait(1)
    second = invoke(2, "append", ("second",))
    third = invoke(3, "append", ("third",))
    assert first["accepted"] is True
    assert first["sequence"] == 1
    assert second["sequence"] == 2
    assert third["accepted"] is True
    assert third["sequence"] == 3
    assert third["queue_size"] == 2
    with pytest.raises(Pyro5.errors.OrderedOnewayQueueFullError):
        invoke(4, "append", ("fourth",))
    service.release.set()
    assert wait_for(lambda: service.log == ["first", "second", "third"])
    daemon.ordered_oneway_channel.close()


def test_acceptance_order_and_full_rejection(ordered_service):
    daemon, service, uri = ordered_service
    with Pyro5.client.Proxy(uri) as proxy:
        channel = proxy.pyroOrderedOneway(on_failure="continue")
        first = channel.submit(proxy.block_first)
        assert service.started.wait(1)
        second = channel.submit(proxy.append, "second")
        third = channel.submit(proxy.append, "third")
        assert first["accepted"] is True
        assert first["sequence"] == 1
        assert second["sequence"] == 2
        assert third["sequence"] == 3
        assert third["queue_size"] == 2
        with pytest.raises(Pyro5.errors.OrderedOnewayQueueFullError):
            channel.submit(proxy.append, "fourth")
        assert service.log == []
        service.release.set()
        assert wait_for(lambda: service.log == ["first", "second", "third"])


def start_callback_daemon(handler, object_id="errors"):
    daemon = Pyro5.server.Daemon(port=0)
    uri = daemon.register(handler, object_id)
    thread = DaemonLoopThread(daemon)
    thread.start()
    thread.running.wait()
    time.sleep(0.05)
    return daemon, uri, thread


def test_failure_reported_and_channel_continues(ordered_service):
    daemon, service, uri = ordered_service
    failures = []
    callback_daemon, handler_uri, callback_thread = start_callback_daemon(
        ErrorHandler(errors=failures))
    try:
        with Pyro5.client.Proxy(handler_uri) as error_proxy, Pyro5.client.Proxy(uri) as proxy:
            channel = proxy.pyroOrderedOneway(on_failure="continue", error_receiver=error_proxy)
            channel.submit(proxy.fail, "boom")
            channel.submit(proxy.append, "after-error")
            assert wait_for(lambda: bool(failures))
            assert wait_for(lambda: service.log == ["after-error"])
            assert failures[0]["sequence"] == 1
            assert failures[0]["error"] == "boom"
    finally:
        callback_daemon.shutdown()
        callback_thread.join(timeout=5)


def test_blocking_or_failing_error_receiver_does_not_stop_continue_channel(ordered_service):
    daemon, service, uri = ordered_service
    blocked = threading.Event()
    daemon.ordered_oneway_error_receiver = ErrorHandler(errors=[], block=blocked, fail=True)
    with Pyro5.client.Proxy(uri) as proxy:
        channel = proxy.pyroOrderedOneway(on_failure="continue")
        channel.submit(proxy.fail, "blocking-failure")
        assert blocked.wait(1)
        channel.submit(proxy.append, "still-runs")
        assert wait_for(lambda: service.log == ["still-runs"])


@pytest.mark.parametrize("ordered_service", ["stop"], indirect=True)
def test_stop_policy_rejects_after_failure(ordered_service):
    daemon, service, uri = ordered_service
    failures = []
    callback_daemon, handler_uri, callback_thread = start_callback_daemon(
        ErrorHandler(errors=failures))
    try:
        with Pyro5.client.Proxy(handler_uri) as error_proxy, Pyro5.client.Proxy(uri) as proxy:
            channel = proxy.pyroOrderedOneway(on_failure="stop", error_receiver=error_proxy)
            channel.submit(proxy.fail, "stop")
            assert wait_for(lambda: bool(failures))
            assert wait_for(lambda: daemon.ordered_oneway_channel.stopped)
            with pytest.raises(Pyro5.errors.DaemonError):
                channel.submit(proxy.append, "rejected-after-stop")
    finally:
        callback_daemon.shutdown()
        callback_thread.join(timeout=5)


def test_unsupported_peer_rejected_without_sending():
    proxy = Pyro5.client.Proxy("PYRO:x@127.0.0.1:9")
    proxy._pyroConnection = object()
    proxy._pyroServerFeatures = {}
    with pytest.raises(Pyro5.errors.UnsupportedFeatureError):
        Pyro5.client.OrderedOnewayChannel(proxy).submit("ping")


def test_channel_error_receiver_failure_does_not_block_continue_policy():
    service = OrderedService()
    blocked = threading.Event()

    def blocking_bad_receiver(report):
        blocked.set()
        time.sleep(2)
        raise RuntimeError("receiver failure")

    daemon = SimpleNamespace(methodcall_error_handler=lambda *args: None)
    channel = Pyro5.server._OrderedOnewayChannel(daemon, 2, "continue")
    channel.submit(Pyro5.server._OrderedOnewayJob(
        1, service, "fail", None, ("boom",), {}, None,
        Pyro5.callcontext.current_context.to_global(),
        "continue", blocking_bad_receiver, "ordered_oneway_error"
    ))
    assert blocked.wait(1)
    channel.submit(Pyro5.server._OrderedOnewayJob(
        2, service, "append", None, ("after-error",), {}, None,
        Pyro5.callcontext.current_context.to_global(),
        "continue", None, "ordered_oneway_error"
    ))
    assert wait_for(lambda: service.log == ["after-error"])
    channel.close()


def test_channel_stop_policy_drains_pending_and_rejects_new_work():
    service = OrderedService()
    failures = []
    daemon = SimpleNamespace(methodcall_error_handler=lambda *args: None)
    channel = Pyro5.server._OrderedOnewayChannel(daemon, 2, "stop")
    channel.submit(Pyro5.server._OrderedOnewayJob(
        1, service, "fail", None, ("boom",), {}, None,
        Pyro5.callcontext.current_context.to_global(),
        "stop", failures.append, "ordered_oneway_error"
    ))
    channel.submit(Pyro5.server._OrderedOnewayJob(
        2, service, "append", None, ("pending",), {}, None,
        Pyro5.callcontext.current_context.to_global(),
        "stop", None, "ordered_oneway_error"
    ))
    assert wait_for(lambda: bool(failures))
    assert wait_for(lambda: channel.stopped)
    with pytest.raises(Pyro5.errors.DaemonError):
        channel.submit(Pyro5.server._OrderedOnewayJob(
            3, service, "append", None, ("new",), {}, None,
            Pyro5.callcontext.current_context.to_global(),
            "stop", None, "ordered_oneway_error"
        ))
    assert service.log == []
    channel.close()
