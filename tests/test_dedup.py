"""
Tests for the opt-in call deduplication protocol (@dedup + call_key).

Pyro - Python Remote Objects.  Copyright by Irmen de Jong (irmen@razorvine.net).
"""

import time
import threading
import pytest
import Pyro5.client
import Pyro5.server
import Pyro5.errors
from Pyro5 import config


@Pyro5.server.expose
class DedupTestObject(object):
    def __init__(self):
        self.calls = 0
        self.gate = threading.Event()
        self.started = threading.Event()

    @Pyro5.server.dedup
    def increment(self, amount):
        self.calls += 1
        return "incremented %d by %d" % (self.calls, amount)

    @Pyro5.server.dedup
    def slow_increment(self, delay):
        time.sleep(delay)
        self.calls += 1
        return "slow call number %d" % self.calls

    @Pyro5.server.dedup
    def failing(self):
        self.calls += 1
        raise ValueError("boom")

    @Pyro5.server.dedup
    def gated(self, value):
        self.started.set()
        self.gate.wait(timeout=10)
        self.calls += 1
        return value

    def plain(self):
        # not marked with @dedup: call keys must be ignored for this one
        self.calls += 1
        return "plain call number %d" % self.calls


class DaemonLoopThread(threading.Thread):
    def __init__(self, pyrodaemon):
        super().__init__()
        self.daemon = True
        self.pyrodaemon = pyrodaemon
        self.running = threading.Event()

    def run(self):
        self.running.set()
        try:
            self.pyrodaemon.requestLoop()
        except Pyro5.errors.CommunicationError:
            pass


class TestCallDedup:
    def setup_method(self):
        self.config_items = ("DEDUP_MAX_RECORDS", "DEDUP_RECORD_LIFETIME", "DEDUP_WAIT",
                             "DEDUP_WAIT_TIMEOUT", "SERIALIZER", "SERVERTYPE")
        self.saved_config = {item: getattr(config, item) for item in self.config_items}
        config.SERIALIZER = "serpent"
        self.obj = DedupTestObject()
        self.daemon = Pyro5.server.Daemon(port=0)
        self.uri = self.daemon.register(self.obj, "deduptest")
        self.daemonthread = DaemonLoopThread(self.daemon)
        self.daemonthread.start()
        self.daemonthread.running.wait()
        time.sleep(0.05)

    def teardown_method(self):
        time.sleep(0.05)
        self.daemon.shutdown()
        self.daemonthread.join()
        for item, value in self.saved_config.items():
            setattr(config, item, value)

    def test_normal_call_with_key(self):
        with Pyro5.client.Proxy(self.uri) as p:
            with Pyro5.client.call_key("key-1"):
                result = p.increment(5)
            assert result == "incremented 1 by 5"
            assert self.obj.calls == 1

    def test_dropped_first_reply_then_retry_with_same_key(self):
        # simulate a lost reply: the client times out while the server is still
        # busy executing the call, then retries with the same call key.
        with Pyro5.client.Proxy(self.uri) as p:
            p._pyroTimeout = 0.2
            with Pyro5.client.call_key("lost-reply"):
                with pytest.raises(Pyro5.errors.TimeoutError):
                    p.slow_increment(1.0)
        time.sleep(1.2)  # let the server finish the first call
        assert self.obj.calls == 1
        # retry with the same key over a new connection: must get the recorded
        # result of the first call, without executing the method again
        with Pyro5.client.Proxy(self.uri) as p:
            with Pyro5.client.call_key("lost-reply"):
                result = p.slow_increment(1.0)
            assert result == "slow call number 1"
            assert self.obj.calls == 1

    def test_same_key_different_args_rejected(self):
        with Pyro5.client.Proxy(self.uri) as p:
            with Pyro5.client.call_key("conflict"):
                assert p.increment(1) == "incremented 1 by 1"
            with Pyro5.client.call_key("conflict"):
                with pytest.raises(Pyro5.errors.CallDedupError):
                    p.increment(999)
            assert self.obj.calls == 1  # the second call was not executed

    def test_unmarked_method_ignores_key(self):
        with Pyro5.client.Proxy(self.uri) as p:
            with Pyro5.client.call_key("some-key"):
                assert p.plain() == "plain call number 1"
                assert p.plain() == "plain call number 2"
            assert self.obj.calls == 2

    def test_failure_is_recorded_and_replayed(self):
        with Pyro5.client.Proxy(self.uri) as p:
            with Pyro5.client.call_key("failing-call"):
                with pytest.raises(ValueError) as exc1:
                    p.failing()
            assert self.obj.calls == 1
        with Pyro5.client.Proxy(self.uri) as p:
            with Pyro5.client.call_key("failing-call"):
                with pytest.raises(ValueError) as exc2:
                    p.failing()
            assert str(exc1.value) == str(exc2.value) == "boom"
            assert self.obj.calls == 1  # still only one actual execution

    def test_concurrent_duplicate_waits_for_result(self):
        results = {}
        barrier = threading.Barrier(2)

        def call(name):
            with Pyro5.client.Proxy(self.uri) as p:
                barrier.wait()
                with Pyro5.client.call_key("concurrent"):
                    results[name] = p.gated("gated-result")

        t1 = threading.Thread(target=call, args=("t1",))
        t2 = threading.Thread(target=call, args=("t2",))
        t1.start()
        t2.start()
        time.sleep(0.5)  # both calls are now in flight; one of them executes
        self.obj.gate.set()
        t1.join()
        t2.join()
        assert results == {"t1": "gated-result", "t2": "gated-result"}
        assert self.obj.calls == 1

    def test_concurrent_duplicate_nowait(self):
        config.DEDUP_WAIT = False
        self.obj.gate.clear()

        def first_call():
            with Pyro5.client.Proxy(self.uri) as p:
                with Pyro5.client.call_key("nowait"):
                    p.gated("x")

        t1 = threading.Thread(target=first_call)
        t1.start()
        assert self.obj.started.wait(timeout=5)  # the first call is now executing on the server
        try:
            with Pyro5.client.Proxy(self.uri) as p:
                with Pyro5.client.call_key("nowait"):
                    with pytest.raises(Pyro5.errors.CallInProgressError):
                        p.gated("x")
        finally:
            self.obj.gate.set()
            t1.join()
        assert self.obj.calls == 1

    def test_record_lifetime_expiry(self):
        config.DEDUP_RECORD_LIFETIME = 0.3
        with Pyro5.client.Proxy(self.uri) as p:
            with Pyro5.client.call_key("expiring"):
                assert p.increment(1) == "incremented 1 by 1"
            time.sleep(0.6)
            with Pyro5.client.call_key("expiring"):
                # record expired: the call is executed again
                assert p.increment(1) == "incremented 2 by 1"
            assert self.obj.calls == 2

    def test_max_records_eviction(self):
        config.DEDUP_MAX_RECORDS = 2
        with Pyro5.client.Proxy(self.uri) as p:
            with Pyro5.client.call_key("k1"):
                p.increment(1)
            with Pyro5.client.call_key("k2"):
                p.increment(1)
            with Pyro5.client.call_key("k3"):
                p.increment(1)
            assert self.obj.calls == 3
            # k1 was evicted (only 2 records are kept), so it executes again
            with Pyro5.client.call_key("k1"):
                assert p.increment(1) == "incremented 4 by 1"
            # k3 is still recorded and gets replayed
            with Pyro5.client.call_key("k3"):
                assert p.increment(1) == "incremented 3 by 1"
            assert self.obj.calls == 4

    def test_key_cleared_after_context(self):
        with Pyro5.client.Proxy(self.uri) as p:
            with Pyro5.client.call_key("scoped"):
                p.increment(1)
            # outside the context no key is attached, calls execute normally
            assert p.increment(1) == "incremented 2 by 1"
            assert self.obj.calls == 2

    def test_invalid_keys_rejected_client_side(self):
        with Pyro5.client.Proxy(self.uri) as p:
            with pytest.raises(ValueError):
                with Pyro5.client.call_key(""):
                    p.increment(1)
            with pytest.raises(ValueError):
                with Pyro5.client.call_key("x" * 300):
                    p.increment(1)
            with pytest.raises(ValueError):
                with Pyro5.client.call_key(None):
                    p.increment(1)
        assert self.obj.calls == 0


if __name__ == "__main__":
    pytest.main([__file__])
