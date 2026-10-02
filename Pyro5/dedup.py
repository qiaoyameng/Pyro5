"""Opt-in invocation deduplication."""

import contextlib
import marshal
import threading
import time

from . import config, errors
from .callcontext import current_context


ANNOTATION = "DEDU"
PROTOCOL_MARKER = 1
STATUS_IN_FLIGHT = 1
STATUS_COMPLETED = 2
STATUS_FAILED = 3


@contextlib.contextmanager
def call_key(key):
    """Attach a deduplication key to Pyro calls made within this context."""
    previous = current_context.dedup_key
    current_context.dedup_key = key
    try:
        yield
    finally:
        current_context.dedup_key = previous


def dedup(method=None, in_flight="wait", wait_timeout=None):
    """Mark an exposed method as supporting deduplicated retries."""
    if in_flight not in ("wait", "reject"):
        raise ValueError("in_flight must be 'wait' or 'reject'")
    if wait_timeout is not None and wait_timeout < 0:
        raise ValueError("wait_timeout must not be negative")

    def mark(func):
        func._pyroDedup = (in_flight, wait_timeout)
        return func

    if method is None:
        return mark
    return mark(method)


def encode_call_key(key):
    if not isinstance(key, str) or not key:
        raise DeduplicationError("deduplication key must be a non-empty string")
    if len(key) > config.DEDUP_KEY_MAX_LENGTH:
        raise DeduplicationError("deduplication key is too long")
    return marshal.dumps((PROTOCOL_MARKER, key))


def decode_call_key(data):
    try:
        value = marshal.loads(bytes(data))
    except Exception as exc:
        raise DeduplicationError("invalid deduplication annotation") from exc
    if not isinstance(value, tuple) or len(value) != 2 or value[0] != PROTOCOL_MARKER:
        raise DeduplicationError("invalid deduplication annotation")
    key = value[1]
    if not isinstance(key, str) or not key or len(key) > config.DEDUP_KEY_MAX_LENGTH:
        raise DeduplicationError("invalid deduplication key")
    return key


class _Record(object):
    __slots__ = ("identity", "parameters", "condition", "status", "payload",
                 "serializer_id", "completed_at", "waiters")

    def __init__(self, lock, identity, parameters):
        self.identity = identity
        self.parameters = parameters
        self.condition = lock
        self.status = STATUS_IN_FLIGHT
        self.payload = None
        self.serializer_id = None
        self.completed_at = None
        self.waiters = 0


class DeduplicationStore(object):
    """In-memory, per-daemon storage for deduplicated invocation results."""

    def __init__(self, completed_ttl=None, failed_ttl=None, max_records=None):
        self.completed_ttl = config.DEDUP_RESULT_TTL if completed_ttl is None else completed_ttl
        self.failed_ttl = config.DEDUP_FAILURE_TTL if failed_ttl is None else failed_ttl
        self.max_records = config.DEDUP_MAX_RECORDS if max_records is None else max_records
        if self.completed_ttl < 0 or self.failed_ttl < 0 or self.max_records < 1:
            raise ValueError("invalid deduplication retention setting")
        self._records = {}
        self._lock = threading.Condition()

    def prune(self, now=None):
        with self._lock:
            self._prune_locked(now or time.monotonic())

    def begin(self, identity, parameters, in_flight="wait", wait_timeout=None):
        if in_flight not in ("wait", "reject"):
            raise ValueError("invalid in-flight duplicate behavior")
        wait_record = None
        with self._lock:
            self._prune_locked(time.monotonic())
            record = self._records.get(identity)
            if record is not None:
                if not parameters_equal(parameters, record.parameters):
                    raise DeduplicationError("deduplication key was reused with different arguments")
                if record.status == STATUS_IN_FLIGHT:
                    if in_flight == "reject":
                        raise CallInProgressError("the original call is still in progress")
                    record.waiters += 1
                    wait_record = record
                else:
                    return _ReplayedCall(record, store=self)
            else:
                record = _Record(self._lock, identity, parameters)
                self._records[identity] = record
                return _DeduplicatedCall(self, record)
        with self._lock:
            while wait_record.status == STATUS_IN_FLIGHT:
                if not self._lock.wait(wait_timeout):
                    wait_record.waiters -= 1
                    raise CallInProgressError("timed out waiting for the original call")
        return _ReplayedCall(wait_record, retain_waiter=True, store=self)

    def complete(self, token, payload, serializer_id):
        self._finish(token.record, STATUS_COMPLETED, payload, serializer_id)

    def fail(self, token, payload, serializer_id):
        self._finish(token.record, STATUS_FAILED, payload, serializer_id)

    def remove(self, record):
        with self._lock:
            self._records.pop(record.identity, None)

    def _finish(self, record, status, payload, serializer_id):
        with self._lock:
            if self._records.get(record.identity) is not record:
                return
            record.status = status
            record.payload = payload
            record.serializer_id = serializer_id
            record.completed_at = time.monotonic()
            self._lock.notify_all()
            self._prune_locked(record.completed_at)

    def _prune_locked(self, now):
        terminals = []
        for identity, record in self._records.items():
            if record.status in (STATUS_COMPLETED, STATUS_FAILED) and record.completed_at is not None:
                terminals.append((record.completed_at, identity))
        terminals.sort()
        for completed_at, identity in terminals:
            record = self._records.get(identity)
            if record is None or record.waiters:
                continue
            ttl = self.completed_ttl if record.status == STATUS_COMPLETED else self.failed_ttl
            if ttl <= 0 or now - completed_at > ttl:
                self._records.pop(identity, None)
        terminals = [
            (record.completed_at, identity)
            for identity, record in self._records.items()
            if record.status in (STATUS_COMPLETED, STATUS_FAILED) and record.completed_at is not None
        ]
        terminals.sort()
        while len(terminals) > self.max_records:
            completed_at, identity = terminals.pop(0)
            record = self._records.get(identity)
            if record is not None and not record.waiters:
                self._records.pop(identity, None)
            else:
                break


class _ReplayedCall(object):
    __slots__ = ("store", "record", "_retained")

    def __init__(self, record, retain_waiter=False, store=None):
        self.store = store
        self.record = record
        self._retained = retain_waiter
        if not retain_waiter:
            with store._lock:
                record.waiters += 1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.release()
        return False

    @property
    def replayed(self):
        return True

    def response(self):
        return self.record.payload, self.record.status == STATUS_FAILED, self.record.serializer_id

    def release(self):
        if self._retained:
            with self.record.condition:
                self.record.waiters -= 1
                self.record.condition.notify_all()
            with self.store._lock:
                self.store._prune_locked(time.monotonic())
            self._retained = False


class _DeduplicatedCall(object):
    __slots__ = ("store", "record", "_finished")

    def __init__(self, store, record):
        self.store = store
        self.record = record
        self._finished = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    @property
    def replayed(self):
        return False

    def complete(self, payload, serializer_id):
        self._finished = True
        self.store.complete(self, payload, serializer_id)

    def fail(self, payload, serializer_id):
        self._finished = True
        self.store.fail(self, payload, serializer_id)


def parameters_equal(left, right):
    if type(left) is not type(right):
        return False
    if isinstance(left, dict):
        return left.keys() == right.keys() and all(parameters_equal(left[key], right[key]) for key in left)
    if isinstance(left, (list, tuple)):
        return len(left) == len(right) and all(parameters_equal(a, b) for a, b in zip(left, right))
    if isinstance(left, (set, frozenset)):
        if len(left) != len(right):
            return False
        unmatched = list(right)
        for value in left:
            for index, candidate in enumerate(unmatched):
                if parameters_equal(value, candidate):
                    del unmatched[index]
                    break
            else:
                return False
        return not unmatched
    return left == right


DeduplicationError = errors.DeduplicationError
CallInProgressError = errors.CallInProgressError


dedup_key = call_key
