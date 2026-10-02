"""
Ordered oneway channel support.

Pyro - Python Remote Objects.  Copyright by Irmen de Jong (irmen@razorvine.net).
"""

import logging
import queue
import threading
from collections import deque

from . import config, core, client, errors
from .callcontext import current_context

log = logging.getLogger("Pyro5.orderedoneway")

POLICY_CONTINUE = "continue"
POLICY_STOP = "stop"
_POLICIES = (POLICY_CONTINUE, POLICY_STOP)


class OrderedOnewayCall:
    __slots__ = ("sequence", "object_id", "method", "vargs", "kwargs", "context",
                 "error_uri", "error_method", "policy")

    def __init__(self, sequence, object_id, method, vargs, kwargs, context,
                 error_uri, error_method, policy):
        self.sequence = sequence
        self.object_id = object_id
        self.method = method
        self.vargs = vargs
        self.kwargs = kwargs
        self.context = context
        self.error_uri = error_uri
        self.error_method = error_method
        self.policy = policy


class _OrderedOnewayChannelState:
    def __init__(self, channel_id, maxsize, policy, connection):
        self.id = channel_id
        self.maxsize = maxsize
        self.policy = policy
        self.connection = connection
        self.active_call = None
        self.pending = deque()
        self.lock = threading.Lock()
        self.connection_lost = False
        self.closed = False
        self.stopped = False
        self.next_sequence = 1

    def add(self, call):
        with self.lock:
            if self.closed:
                raise errors.OrderedOnewayChannelClosedError("ordered oneway channel is closed")
            if self.stopped:
                raise errors.OrderedOnewayChannelClosedError("ordered oneway channel is stopped after an error")
            if self.connection_lost:
                raise errors.OrderedOnewayChannelClosedError("ordered oneway channel connection was lost")
            if call.sequence != self.next_sequence:
                raise errors.ProtocolError("ordered oneway sequence out of order, got %d expected %d"
                                           % (call.sequence, self.next_sequence))
            if len(self.pending) >= self.maxsize:
                error = errors.OrderedOnewayQueueFullError(
                    "ordered oneway channel queue is full (max=%d)" % self.maxsize)
                error.maxsize = self.maxsize
                error.pending = len(self.pending)
                raise error
            self.pending.append(call)
            self.next_sequence += 1
            should_schedule = self.active_call is None
            if should_schedule:
                self.active_call = self.pending.popleft()
            return should_schedule, len(self.pending)

    def next_call(self):
        with self.lock:
            if self.closed or self.stopped or self.active_call is None:
                return None
            return self.active_call

    def complete(self, failed):
        aborted = []
        with self.lock:
            self.active_call = None
            if failed and self.policy == POLICY_STOP:
                self.stopped = True
            if self.closed or self.stopped:
                aborted = list(self.pending)
                self.pending.clear()
            elif self.pending:
                self.active_call = self.pending.popleft()
        return aborted, self.stopped

    def close(self):
        with self.lock:
            self.closed = True
            aborted = list(self.pending)
            self.pending.clear()
            return aborted

    def disconnect(self, connection):
        with self.lock:
            if self.connection is connection:
                self.connection = None
                self.connection_lost = True
        return False


class _OrderedOnewayErrorReporter:
    def __init__(self, workers, queue_size, timeout):
        self.timeout = timeout
        self.queue = queue.Queue(maxsize=queue_size)
        self.pending = 0
        self.condition = threading.Condition()
        self.stopped = False
        self.threads = []
        for _ in range(max(1, workers)):
            thread = threading.Thread(target=self._run, name="pyro-ordered-oneway-error", daemon=True)
            thread.start()
            self.threads.append(thread)

    def report(self, call, exception, phase="business"):
        if self.stopped or not call.error_uri:
            return
        info = self._error_info(call, exception, phase)
        with self.condition:
            if self.stopped or self.pending >= self.queue.maxsize:
                log.warning("ordered oneway error report queue is full; dropping report for call %d",
                            call.sequence, exc_info=True)
                return
            self.pending += 1
        try:
            self.queue.put_nowait((call.error_uri, call.error_method, info))
        except queue.Full:
            with self.condition:
                self.pending -= 1
                self.condition.notify_all()
            log.warning("ordered oneway error report queue is full; dropping report for call %d",
                        call.sequence, exc_info=True)

    def _error_info(self, call, exception, phase):
        if exception.__traceback__ is not None:
            traceback = "".join(errors.format_traceback(type(exception), exception, exception.__traceback__,
                                                        detailed=config.DETAILED_TRACEBACK))
        else:
            traceback = ""
        return {
            "channel_id": call.context.get("channel_id"),
            "sequence": call.sequence,
            "object": call.object_id,
            "method": getattr(call.method, "__qualname__", str(call.method)),
            "phase": phase,
            "error_type": type(exception).__name__,
            "error_message": str(exception),
            "traceback": traceback,
            "policy": call.policy,
        }

    def _run(self):
        while True:
            item = self.queue.get()
            try:
                if item is not None:
                    receiver, method_name, info = item
                    try:
                        if callable(receiver):
                            receiver(info)
                        else:
                            proxy = client.Proxy(core.URI(receiver))
                            try:
                                proxy._pyroTimeout = self.timeout
                                getattr(proxy, method_name)(info)
                            finally:
                                proxy._pyroRelease()
                    except Exception:
                        log.warning("ordered oneway error receiver %s failed; continuing channel",
                                    receiver, exc_info=True)
            finally:
                if item is not None:
                    with self.condition:
                        self.pending -= 1
                        self.condition.notify_all()

    def close(self):
        with self.condition:
            self.stopped = True

    def wait_drained(self, timeout=None):
        with self.condition:
            return self.condition.wait_for(lambda: self.pending == 0, timeout)

    def terminate_threads(self):
        for _ in self.threads:
            self.queue.put(None)


class _OrderedOnewayDispatcher:
    def __init__(self, daemon):
        self.daemon = daemon
        self.ready = queue.Queue()
        self.channels = {}
        self.lock = threading.Lock()
        self.shutdown = False
        self.started = False
        self.threads = []
        self.reporter = None

    def start(self):
        with self.lock:
            if not self.started:
                self.reporter = _OrderedOnewayErrorReporter(
                    config.ORDERED_ONEWAY_ERROR_WORKERS,
                    config.ORDERED_ONEWAY_ERROR_QUEUE_SIZE,
                    config.ORDERED_ONEWAY_ERROR_TIMEOUT)
                self.started = True
            while len(self.threads) < max(1, config.ORDERED_ONEWAY_WORKERS):
                thread = threading.Thread(target=self._worker, name="pyro-ordered-oneway", daemon=True)
                thread.start()
                self.threads.append(thread)

    def submit(self, connection, object_id, method, vargs, kwargs, options, call_context):
        channel_id = options["id"]
        sequence = options["sequence"]
        policy = options["policy"]
        error_uri = options.get("error_uri")
        error_method = options.get("error_method", "handle_ordered_oneway_error")
        requested_maxsize = options.get("maxsize", config.ORDERED_ONEWAY_QUEUE_SIZE)
        if not isinstance(requested_maxsize, int) or isinstance(requested_maxsize, bool):
            raise ValueError("ordered oneway queue size must be a positive integer")
        maxsize = min(requested_maxsize, int(config.ORDERED_ONEWAY_QUEUE_SIZE))
        if maxsize <= 0:
            raise ValueError("ordered oneway queue size must be a positive number")
        call_context["channel_id"] = channel_id
        call = OrderedOnewayCall(sequence, object_id, method, vargs, kwargs, call_context,
                                 error_uri, error_method, policy)
        with self.lock:
            if self.shutdown:
                raise errors.OrderedOnewayChannelClosedError("daemon is closed")
            state = self.channels.get(channel_id)
            if state is None:
                if len(self.channels) >= config.ORDERED_ONEWAY_MAX_CHANNELS:
                    raise errors.DaemonError("maximum number of ordered oneway channels reached")
                state = _OrderedOnewayChannelState(channel_id, maxsize, policy, connection)
                self.channels[channel_id] = state
            else:
                if state.connection is not connection:
                    raise errors.ProtocolError("ordered oneway channel is already active on another connection")
                if state.maxsize != maxsize or state.policy != policy:
                    raise errors.ProtocolError("ordered oneway channel options do not match existing channel")
            should_schedule, pending = state.add(call)
        if should_schedule:
            self.ready.put(state)
        return {"accepted": True, "channel": channel_id, "sequence": sequence,
                "maxsize": maxsize, "pending": pending}

    def disconnect_connection(self, connection):
        with self.lock:
            for state in list(self.channels.values()):
                state.disconnect(connection)

    def close(self):
        aborted_calls = []
        reporter = None
        threads = []
        with self.lock:
            if self.shutdown:
                return
            self.shutdown = True
            states = list(self.channels.values())
            self.channels.clear()
            if self.started:
                reporter = self.reporter
                threads = list(self.threads)
        for state in states:
            aborted_calls.extend((state, call) for call in state.close())
        for state, call in aborted_calls:
            if reporter and self._should_report_on_close(call):
                reporter.report(call, errors.OrderedOnewayChannelClosedError(
                    "ordered oneway channel was closed before this call ran"), phase="aborted")
        if reporter:
            reporter.close()
            reporter.wait_drained(timeout=1.0)
        for _ in threads:
            self.ready.put(None)
        if reporter:
            reporter.wait_drained(timeout=1.0)
            reporter.terminate_threads()

    def _worker(self):
        while True:
            state = self.ready.get()
            if state is None:
                return
            call = state.next_call()
            if call is None:
                continue
            current_context.from_global(call.context)
            failure = None
            try:
                call.method(*call.vargs, **call.kwargs)
            except Exception as exception:
                failure = exception
                self.daemon.methodcall_error_handler(self.daemon, call.context.get("client_sock_addr"),
                                                     call.method, call.vargs, call.kwargs, exception)
                self.reporter.report(call, exception)
            aborted, stopped = state.complete(failure is not None)
            for aborted_call in aborted:
                self._report_aborted(aborted_call, stopped=stopped)
            if not state.closed and not state.stopped and state.active_call is not None and not self.shutdown:
                self.ready.put(state)

    def _report_aborted(self, call, stopped=False):
        if self.reporter:
            message = "ordered oneway channel was stopped after a failed call" if stopped \
                else "ordered oneway channel was closed before this call ran"
            error = errors.OrderedOnewayChannelClosedError(message)
            self.reporter.report(call, error, phase="aborted")

    def _should_report_on_close(self, call):
        receiver = call.error_uri
        if callable(receiver) or receiver is None:
            return False
        try:
            return core.URI(receiver).location != self.daemon.locationStr
        except errors.PyroError:
            return True
