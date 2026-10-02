"""
Name Server and helper functions.

Pyro - Python Remote Objects.  Copyright by Irmen de Jong (irmen@razorvine.net).
"""

import warnings
import re
import sys
import logging
import socket
import time
import contextlib
import threading
import json
from collections import deque
from collections.abc import MutableMapping
try:
    import sqlite3
except ImportError:
    pass
from . import config, core, socketutil, server, errors
from .errors import NamingError, PyroError, ProtocolError, StaleRevisionError


__all__ = ["start_ns_loop", "start_ns"]

log = logging.getLogger("Pyro5.naming")


class MemoryStorage(dict):
    """
    Storage implementation that is just an in-memory dict.
    (because it inherits from dict it is automatically a collections.MutableMapping)
    Stopping the nameserver will make the server instantly forget about everything.
    """
    def __init__(self, event_history_limit=1000, **kwargs):
        super(MemoryStorage, self).__init__(**kwargs)
        if event_history_limit is not None and (
            not isinstance(event_history_limit, int)
            or isinstance(event_history_limit, bool)
            or event_history_limit < 0
        ):
            raise ValueError("event history limit must be a non-negative integer or None")
        self.revision = 0
        self.event_history_limit = event_history_limit
        self.events = deque()
        self.oldest_event_revision = None
        self.storage_lock = threading.RLock()

    @staticmethod
    def _make_changes(name, uri=None, metadata=None):
        return [{"name": name, "uri": uri, "metadata": sorted(metadata or [])}]

    def _append_event(self, operation, changes, batch=False):
        self.revision += 1
        if self.event_history_limit is None:
            self.events.append(self._event_dict(self.revision, operation, changes, batch))
        elif self.event_history_limit > 0:
            if len(self.events) >= self.event_history_limit:
                self.events.popleft()
            self.events.append(self._event_dict(self.revision, operation, changes, batch))
        self.oldest_event_revision = self.events[0]["revision"] if self.events else self.revision
        return self.revision

    @staticmethod
    def _event_dict(revision, operation, changes, batch):
        return {
            "revision": revision,
            "operation": operation,
            "batch": batch,
            "changes": changes
        }

    def register(self, name, uri, metadata):
        with self.storage_lock:
            operation = "replace" if name in self else "register"
            self[name] = uri, frozenset(metadata or [])
            self._append_event(operation, self._make_changes(name, uri, metadata))
            return self.revision

    def set_metadata(self, name, metadata):
        with self.storage_lock:
            uri, old_metadata = self[name]
            self[name] = uri, frozenset(metadata or [])
            self._append_event("set_metadata", self._make_changes(name, uri, metadata))
            return self.revision

    def remove_item(self, name):
        with self.storage_lock:
            if name not in self:
                return False
            del self[name]
            self._append_event("remove", self._make_changes(name))
            return True

    def __setitem__(self, key, value):
        uri, metadata = value
        super(MemoryStorage, self).__setitem__(key, (uri, metadata if metadata is not None else frozenset()))

    def optimized_prefix_list(self, prefix, return_metadata=False):
        return None

    def optimized_regex_list(self, regex, return_metadata=False):
        return None

    def optimized_metadata_search(self, metadata_all=None, metadata_any=None, return_metadata=False):
        return None

    def everything(self, return_metadata=False):
        if return_metadata:
            return self.copy()
        return {name: uri for name, (uri, metadata) in self.items()}

    def remove_items(self, items):
        with self.storage_lock:
            removed = []
            for item in sorted(items):
                if item in self:
                    removed.append(item)
                    del self[item]
            if removed:
                self._append_event(
                    "remove",
                    [{"name": name, "uri": None, "metadata": []} for name in removed],
                    True
                )
            return len(removed)

    def snapshot(self):
        with self.storage_lock:
            return self.everything(return_metadata=True), self.revision

    def changes_since(self, revision):
        with self.storage_lock:
            if revision > self.revision:
                raise ValueError("revision is from the future")
            if self.oldest_event_revision is not None and revision + 1 < self.oldest_event_revision:
                raise StaleRevisionError("revision cursor is too old; fetch a new snapshot")
            events = []
            for event in self.events:
                if event["revision"] > revision:
                    events.append({
                        "revision": event["revision"],
                        "operation": event["operation"],
                        "batch": event["batch"],
                        "changes": [dict(change) for change in event["changes"]]
                    })
            if revision < self.revision and not events:
                raise StaleRevisionError("revision cursor is too old; fetch a new snapshot")
            return events, self.revision

    def clear(self):
        with self.storage_lock:
            super().clear()
            self.revision = 0
            self.events.clear()
            self.oldest_event_revision = None

    def close(self):
        pass


class SqlStorage(MutableMapping):
    """
    Sqlite-based storage.
    It is just a single (name,uri) table for the names and another table for the metadata.
    Sqlite db connection objects aren't thread-safe, so a new connection is created in every method.
    """
    def __init__(self, dbfile, event_history_limit=1000):
        if dbfile == ":memory:":
            raise ValueError("We don't support the sqlite :memory: database type. Just use the default volatile in-memory store.")
        if event_history_limit is not None and (
            not isinstance(event_history_limit, int)
            or isinstance(event_history_limit, bool)
            or event_history_limit < 0
        ):
            raise ValueError("event history limit must be a non-negative integer or None")
        self.dbfile = dbfile
        self.event_history_limit = event_history_limit
        with sqlite3.connect(dbfile) as db:
            db.execute("PRAGMA foreign_keys=ON")
            table_exists = lambda table: bool(db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
            ).fetchone())
            try:
                db.execute("SELECT COUNT(*) FROM pyro_names").fetchone()
            except sqlite3.OperationalError:
                # the table does not yet exist
                self._create_schema(db)
            else:
                # check if we need to update the existing schema
                try:
                    db.execute("SELECT COUNT(*) FROM pyro_metadata").fetchone()
                except sqlite3.OperationalError:
                    # metadata schema needs to be created and existing data migrated
                    db.execute("ALTER TABLE pyro_names RENAME TO pyro_names_old")
                    self._create_schema(db)
                    db.execute("INSERT INTO pyro_names(name, uri) SELECT name, uri FROM pyro_names_old")
                    db.execute("DROP TABLE pyro_names_old")
                if not table_exists("pyro_state"):
                    self._create_state_schema(db)
                elif db.execute("SELECT 1 FROM pyro_state WHERE key='oldest_event_revision'").fetchone() is None:
                    db.execute("INSERT INTO pyro_state(key, value) VALUES('oldest_event_revision', NULL)")
                if not table_exists("pyro_events"):
                    self._create_events_schema(db)
            db.commit()

    def _create_schema(self, db):
        db.execute("""CREATE TABLE pyro_names
            (
                id integer PRIMARY KEY,
                name nvarchar NOT NULL UNIQUE,
                uri nvarchar NOT NULL
            );""")
        db.execute("""CREATE TABLE pyro_metadata
            (
                object integer NOT NULL,
                metadata nvarchar NOT NULL,
                FOREIGN KEY(object) REFERENCES pyro_names(id)
            );""")
        self._create_state_schema(db)
        self._create_events_schema(db)

    @staticmethod
    def _create_state_schema(db):
        db.execute("CREATE TABLE pyro_state (key nvarchar PRIMARY KEY, value integer);")
        db.execute("INSERT INTO pyro_state(key, value) VALUES('revision', 0)")
        db.execute("INSERT INTO pyro_state(key, value) VALUES('oldest_event_revision', NULL)")

    @staticmethod
    def _oldest_event_revision(db):
        return db.execute("SELECT value FROM pyro_state WHERE key='oldest_event_revision'").fetchone()[0]

    @staticmethod
    def _set_oldest_event_revision(db, revision):
        db.execute("UPDATE pyro_state SET value=? WHERE key='oldest_event_revision'", (revision,))

    @staticmethod
    def _create_events_schema(db):
        db.execute("""CREATE TABLE pyro_events
            (
                revision integer PRIMARY KEY,
                operation nvarchar NOT NULL,
                batch integer NOT NULL,
                changes nvarchar NOT NULL
            );""")

    @staticmethod
    def _current_revision(db):
        return db.execute("SELECT value FROM pyro_state WHERE key='revision'").fetchone()[0]

    @staticmethod
    def _set_revision(db, revision):
        db.execute("UPDATE pyro_state SET value=? WHERE key='revision'", (revision,))

    @staticmethod
    def _write_name(cursor, name, uri, metadata):
        cursor.execute("DELETE FROM pyro_metadata WHERE object IN (SELECT id FROM pyro_names WHERE name=?)", (name,))
        cursor.execute("DELETE FROM pyro_names WHERE name=?", (name,))
        cursor.execute("INSERT INTO pyro_names(name, uri) VALUES(?,?)", (name, uri))
        object_id = cursor.lastrowid
        for item in metadata or ():
            cursor.execute("INSERT INTO pyro_metadata(object, metadata) VALUES (?,?)", (object_id, item))

    @staticmethod
    def _load_names(db):
        names = {}
        rows = db.execute("SELECT id, name, uri FROM pyro_names").fetchall()
        for dbid, name, uri in rows:
            metadata = {m[0] for m in db.execute(
                "SELECT metadata FROM pyro_metadata WHERE object=?", (dbid,)
            ).fetchall()}
            names[name] = uri, metadata
        return names

    def _append_event(self, db, operation, changes, batch=False):
        revision = self._current_revision(db) + 1
        db.execute(
            "INSERT INTO pyro_events(revision, operation, batch, changes) VALUES(?,?,?,?)",
            (revision, operation, 1 if batch else 0, json.dumps(changes))
        )
        self._set_revision(db, revision)
        if self.event_history_limit == 0:
            db.execute("DELETE FROM pyro_events")
            oldest_revision = revision
        elif self.event_history_limit is not None:
            db.execute(
                "DELETE FROM pyro_events WHERE revision < "
                "(SELECT revision FROM pyro_events ORDER BY revision DESC LIMIT 1 OFFSET ?)",
                (self.event_history_limit - 1,)
            )
            oldest_revision = db.execute("SELECT MIN(revision) FROM pyro_events").fetchone()[0]
        else:
            oldest_revision = db.execute("SELECT MIN(revision) FROM pyro_events").fetchone()[0]
        self._set_oldest_event_revision(db, oldest_revision)
        return revision

    @staticmethod
    def _decode_event(row):
        revision, operation, batch, changes = row
        return {
            "revision": revision,
            "operation": operation,
            "batch": bool(batch),
            "changes": json.loads(changes)
        }

    @staticmethod
    def _make_changes(name, uri=None, metadata=None):
        return [{"name": name, "uri": uri, "metadata": sorted(metadata or [])}]

    def register(self, name, uri, metadata):
        try:
            with sqlite3.connect(self.dbfile) as db:
                db.execute("PRAGMA foreign_keys=ON")
                operation = db.execute(
                    "SELECT 1 FROM pyro_names WHERE name=?", (name,)
                ).fetchone()
                operation = "replace" if operation else "register"
                cursor = db.cursor()
                self._write_name(cursor, name, uri, metadata or ())
                revision = self._append_event(db, operation, self._make_changes(name, uri, metadata))
                cursor.close()
                db.commit()
                return revision
        except sqlite3.DatabaseError as e:
            raise NamingError("sqlite error in register: " + str(e))

    def set_metadata(self, name, metadata):
        try:
            with sqlite3.connect(self.dbfile) as db:
                db.execute("PRAGMA foreign_keys=ON")
                row = db.execute("SELECT uri FROM pyro_names WHERE name=?", (name,)).fetchone()
                if not row:
                    raise KeyError(name)
                cursor = db.cursor()
                self._write_name(cursor, name, row[0], metadata or ())
                revision = self._append_event(db, "set_metadata", self._make_changes(name, row[0], metadata))
                cursor.close()
                db.commit()
                return revision
        except sqlite3.DatabaseError as e:
            raise NamingError("sqlite error in set_metadata: " + str(e))

    def remove_item(self, name):
        try:
            with sqlite3.connect(self.dbfile) as db:
                db.execute("PRAGMA foreign_keys=ON")
                if not db.execute("SELECT 1 FROM pyro_names WHERE name=?", (name,)).fetchone():
                    db.rollback()
                    return False
                db.execute("DELETE FROM pyro_metadata WHERE object IN (SELECT id FROM pyro_names WHERE name=?)", (name,))
                db.execute("DELETE FROM pyro_names WHERE name=?", (name,))
                self._append_event(db, "remove", self._make_changes(name))
                db.commit()
                return True
        except sqlite3.DatabaseError as e:
            raise NamingError("sqlite error in remove_item: " + str(e))

    def __getattr__(self, item):
        raise NotImplementedError("SqlStorage doesn't implement method/attribute '" + item + "'")

    def __getitem__(self, item):
        try:
            with sqlite3.connect(self.dbfile) as db:
                result = db.execute("SELECT id, uri FROM pyro_names WHERE name=?", (item,)).fetchone()
                if result:
                    dbid, uri = result
                    metadata = {m[0] for m in db.execute("SELECT metadata FROM pyro_metadata WHERE object=?", (dbid,)).fetchall()}
                    return uri, metadata
                else:
                    raise KeyError(item)
        except sqlite3.DatabaseError as e:
            raise NamingError("sqlite error in getitem: " + str(e))

    def __setitem__(self, key, value):
        uri, metadata = value
        try:
            with sqlite3.connect(self.dbfile) as db:
                cursor = db.cursor()
                cursor.execute("PRAGMA foreign_keys=ON")
                dbid = cursor.execute("SELECT id FROM pyro_names WHERE name=?", (key,)).fetchone()
                if dbid:
                    dbid = dbid[0]
                    cursor.execute("DELETE FROM pyro_metadata WHERE object=?", (dbid,))
                    cursor.execute("DELETE FROM pyro_names WHERE id=?", (dbid,))
                cursor.execute("INSERT INTO pyro_names(name, uri) VALUES(?,?)", (key, uri))
                if metadata:
                    object_id = cursor.lastrowid
                    for m in metadata:
                        cursor.execute("INSERT INTO pyro_metadata(object, metadata) VALUES (?,?)", (object_id, m))
                cursor.close()
                db.commit()
        except sqlite3.DatabaseError as e:
            raise NamingError("sqlite error in setitem: " + str(e))

    def __len__(self):
        try:
            with sqlite3.connect(self.dbfile) as db:
                return db.execute("SELECT count(*) FROM pyro_names").fetchone()[0]
        except sqlite3.DatabaseError as e:
            raise NamingError("sqlite error in len: " + str(e))

    def __contains__(self, item):
        try:
            with sqlite3.connect(self.dbfile) as db:
                return db.execute("SELECT EXISTS(SELECT 1 FROM pyro_names WHERE name=? LIMIT 1)", (item,)).fetchone()[0]
        except sqlite3.DatabaseError as e:
            raise NamingError("sqlite error in contains: " + str(e))

    def __delitem__(self, key):
        try:
            with sqlite3.connect(self.dbfile) as db:
                db.execute("PRAGMA foreign_keys=ON")
                dbid = db.execute("SELECT id FROM pyro_names WHERE name=?", (key,)).fetchone()
                if dbid:
                    dbid = dbid[0]
                    db.execute("DELETE FROM pyro_metadata WHERE object=?", (dbid,))
                    db.execute("DELETE FROM pyro_names WHERE id=?", (dbid,))
                db.commit()
        except sqlite3.DatabaseError as e:
            raise NamingError("sqlite error in delitem: " + str(e))

    def __iter__(self):
        try:
            with sqlite3.connect(self.dbfile) as db:
                result = db.execute("SELECT name FROM pyro_names")
                return iter([n[0] for n in result.fetchall()])
        except sqlite3.DatabaseError as e:
            raise NamingError("sqlite error in iter: " + str(e))

    def clear(self):
        try:
            with sqlite3.connect(self.dbfile) as db:
                db.execute("PRAGMA foreign_keys=ON")
                db.execute("DELETE FROM pyro_metadata")
                db.execute("DELETE FROM pyro_names")
                db.execute("DELETE FROM pyro_events")
                db.execute("UPDATE pyro_state SET value=0 WHERE key='revision'")
                self._set_oldest_event_revision(db, None)
                db.commit()
            with sqlite3.connect(self.dbfile, isolation_level=None) as db:
                db.execute("VACUUM")  # this cannot run inside a transaction.
        except sqlite3.DatabaseError as e:
            raise NamingError("sqlite error in clear: " + str(e))

    def optimized_prefix_list(self, prefix, return_metadata=False):
        try:
            with sqlite3.connect(self.dbfile) as db:
                names = {}
                if return_metadata:
                    for dbid, name, uri in db.execute("SELECT id, name, uri FROM pyro_names WHERE name LIKE ?", (prefix + '%',)).fetchall():
                        metadata = {m[0] for m in db.execute("SELECT metadata FROM pyro_metadata WHERE object=?", (dbid,)).fetchall()}
                        names[name] = uri, metadata
                else:
                    for name, uri in db.execute("SELECT name, uri FROM pyro_names WHERE name LIKE ?", (prefix + '%',)).fetchall():
                        names[name] = uri
                return names
        except sqlite3.DatabaseError as e:
            raise NamingError("sqlite error in optimized_prefix_list: " + str(e))

    def optimized_regex_list(self, regex, return_metadata=False):
        # defining a regex function isn't much better than simply regexing ourselves over the full table.
        return None

    def optimized_metadata_search(self, metadata_all=None, metadata_any=None, return_metadata=False):
        try:
            with sqlite3.connect(self.dbfile) as db:
                if metadata_any:
                    # any of the given metadata
                    params = list(metadata_any)
                    sql = "SELECT id, name, uri FROM pyro_names WHERE id IN (SELECT object FROM pyro_metadata WHERE metadata IN ({seq}))" \
                          .format(seq=",".join(['?'] * len(metadata_any)))
                else:
                    # all of the given metadata
                    params = list(metadata_all)
                    params.append(len(metadata_all))
                    sql = "SELECT id, name, uri FROM pyro_names WHERE id IN (SELECT object FROM pyro_metadata WHERE metadata IN ({seq}) " \
                          "GROUP BY object HAVING COUNT(metadata)=?)".format(seq=",".join(['?'] * len(metadata_all)))
                result = db.execute(sql, params).fetchall()
                if return_metadata:
                    names = {}
                    for dbid, name, uri in result:
                        metadata = {m[0] for m in db.execute("SELECT metadata FROM pyro_metadata WHERE object=?", (dbid,)).fetchall()}
                        names[name] = uri, metadata
                else:
                    names = {name: uri for (dbid, name, uri) in result}
                return names
        except sqlite3.DatabaseError as e:
            raise NamingError("sqlite error in optimized_metadata_search: " + str(e))

    def remove_items(self, items):
        try:
            with sqlite3.connect(self.dbfile) as db:
                db.execute("PRAGMA foreign_keys=ON")
                changes = []
                for item in sorted(items):
                    dbid = db.execute("SELECT id FROM pyro_names WHERE name=?", (item,)).fetchone()
                    if dbid:
                        dbid = dbid[0]
                        db.execute("DELETE FROM pyro_metadata WHERE object=?", (dbid,))
                        db.execute("DELETE FROM pyro_names WHERE id=?", (dbid,))
                        changes.append({"name": item, "uri": None, "metadata": []})
                if changes:
                    self._append_event(db, "remove", changes, True)
                db.commit()
                return len(changes)
        except sqlite3.DatabaseError as e:
            raise NamingError("sqlite error in remove_items: " + str(e))

    def everything(self, return_metadata=False):
        try:
            with sqlite3.connect(self.dbfile) as db:
                names = {}
                if return_metadata:
                    for dbid, name, uri in db.execute("SELECT id, name, uri FROM pyro_names").fetchall():
                        metadata = {m[0] for m in db.execute("SELECT metadata FROM pyro_metadata WHERE object=?", (dbid,)).fetchall()}
                        names[name] = uri, metadata
                else:
                    for name, uri in db.execute("SELECT name, uri FROM pyro_names").fetchall():
                        names[name] = uri
                return names
        except sqlite3.DatabaseError as e:
            raise NamingError("sqlite error in everything: " + str(e))

    def snapshot(self):
        try:
            db = sqlite3.connect(self.dbfile)
            try:
                db.execute("PRAGMA foreign_keys=ON")
                db.execute("BEGIN")
                names = self._load_names(db)
                revision = self._current_revision(db)
                db.commit()
                return names, revision
            except Exception:
                db.rollback()
                raise
            finally:
                db.close()
        except sqlite3.DatabaseError as e:
            raise NamingError("sqlite error in snapshot: " + str(e))

    def changes_since(self, revision):
        try:
            db = sqlite3.connect(self.dbfile)
            try:
                db.execute("BEGIN")
                current_revision = self._current_revision(db)
                if revision > current_revision:
                    raise ValueError("revision is from the future")
                oldest_revision = self._oldest_event_revision(db)
                if oldest_revision is not None and revision + 1 < oldest_revision:
                    raise StaleRevisionError("revision cursor is too old; fetch a new snapshot")
                rows = db.execute(
                    "SELECT revision, operation, batch, changes FROM pyro_events WHERE revision > ? ORDER BY revision",
                    (revision,)
                ).fetchall()
                events = [self._decode_event(row) for row in rows]
                if revision < current_revision and not events:
                    raise StaleRevisionError("revision cursor is too old; fetch a new snapshot")
                db.commit()
                return events, current_revision
            except Exception:
                db.rollback()
                raise
            finally:
                db.close()
        except sqlite3.DatabaseError as e:
            raise NamingError("sqlite error in changes_since: " + str(e))

    def close(self):
        pass


@server.expose
class NameServer(object):
    """
    Pyro name server. Provides a simple flat name space to map logical object names to Pyro URIs.
    Default storage is done in an in-memory dictionary. You can provide custom storage types.
    """
    def __init__(self, storageProvider=None):
        self.storage = storageProvider
        if storageProvider is None:
            self.storage = MemoryStorage()
            log.debug("using volatile in-memory dict storage")
        self.lock = threading.RLock()

    def _storage_supports_revisions(self):
        return all(
            hasattr(self.storage, name)
            for name in ("snapshot", "changes_since", "register", "set_metadata", "remove_item", "remove_items")
        )

    def count(self):
        """Returns the number of name registrations."""
        return len(self.storage)

    def lookup(self, name, return_metadata=False):
        """
        Lookup the given name, returns an URI if found.
        Returns tuple (uri, metadata) if return_metadata is True.
        """
        try:
            uri, metadata = self.storage[name]
            uri = core.URI(uri)
            if return_metadata:
                return uri, set(metadata or [])
            return uri
        except KeyError:
            raise NamingError("unknown name: " + name)

    def register(self, name, uri, safe=False, metadata=None):
        """Register a name with an URI. If safe is true, name cannot be registered twice.
        The uri can be a string or an URI object. Metadata must be None, or a collection of strings."""
        if isinstance(uri, core.URI):
            uri = str(uri)
        elif not isinstance(uri, str):
            raise TypeError("only URIs or strings can be registered")
        else:
            core.URI(uri)  # check if uri is valid
        if not isinstance(name, str):
            raise TypeError("name must be a str")
        if isinstance(metadata, str):
            raise TypeError("metadata should not be a str, but another iterable (set, list, etc)")
        metadata and iter(metadata)  # validate that metadata is iterable
        with self.lock:
            if safe and name in self.storage:
                raise NamingError("name already registered: " + name)
            metadata = set(metadata) if metadata is not None else None
            if self._storage_supports_revisions():
                return self.storage.register(name, uri, metadata)
            self.storage[name] = uri, metadata

    def set_metadata(self, name, metadata):
        """update the metadata for an existing registration"""
        if not isinstance(name, str):
            raise TypeError("name must be a str")
        if isinstance(metadata, str):
            raise TypeError("metadata should not be a str, but another iterable (set, list, etc)")
        metadata and iter(metadata)  # validate that metadata is iterable
        with self.lock:
            try:
                uri, old_meta = self.storage[name]
                metadata = set(metadata) if metadata is not None else None
                if self._storage_supports_revisions():
                    return self.storage.set_metadata(name, metadata)
                self.storage[name] = uri, metadata
            except KeyError:
                raise NamingError("unknown name: " + name)

    def remove(self, name=None, prefix=None, regex=None):
        """Remove a registration. returns the number of items removed."""
        if name and name != core.NAMESERVER_NAME:
            with self.lock:
                if self._storage_supports_revisions():
                    return 1 if self.storage.remove_item(name) else 0
                if name in self.storage:
                    del self.storage[name]
                    return 1
                return 0
        if prefix:
            with self.lock:
                items = list(self.list(prefix=prefix).keys())
                if core.NAMESERVER_NAME in items:
                    items.remove(core.NAMESERVER_NAME)
                if self._storage_supports_revisions():
                    return self.storage.remove_items(items)
                count = 0
                for item in items:
                    del self.storage[item]
                    count += 1
                return count
        if regex:
            with self.lock:
                items = list(self.list(regex=regex).keys())
                if core.NAMESERVER_NAME in items:
                    items.remove(core.NAMESERVER_NAME)
                if self._storage_supports_revisions():
                    return self.storage.remove_items(items)
                count = 0
                for item in items:
                    del self.storage[item]
                    count += 1
                return count
        return 0

    # noinspection PyNoneFunctionAssignment
    def list(self, prefix=None, regex=None, return_metadata=False):
        """
        Retrieve the registered items as a dictionary name-to-URI. The URIs in the resulting dict
        are strings, not URI objects. You can filter by prefix or by regex.
        """
        if prefix and regex:
            raise ValueError("you can only filter on one thing at a time")
        with self.lock:
            if prefix:
                result = self.storage.optimized_prefix_list(prefix, return_metadata)
                if result is not None:
                    return result
                result = {}
                for name in self.storage:
                    if name.startswith(prefix):
                        result[name] = self.storage[name] if return_metadata else self.storage[name][0]
                return result
            elif regex:
                result = self.storage.optimized_regex_list(regex, return_metadata)
                if result is not None:
                    return result
                result = {}
                try:
                    regex = re.compile(regex)
                except re.error as x:
                    raise errors.NamingError("invalid regex: " + str(x))
                else:
                    for name in self.storage:
                        if regex.match(name):
                            result[name] = self.storage[name] if return_metadata else self.storage[name][0]
                    return result
            else:
                # just return (a copy of) everything
                return self.storage.everything(return_metadata)

    def snapshot(self):
        """Return a metadata-enabled snapshot of all registrations and the matching revision."""
        with self.lock:
            if not self._storage_supports_revisions():
                raise NotImplementedError("storage provider does not support revisioned snapshots")
            names, revision = self.storage.snapshot()
            return {name: (uri, set(metadata or [])) for name, (uri, metadata) in names.items()}, revision

    def changes_since(self, revision):
        """Return ordered changes after revision and the current revision.
        Raises StaleRevisionError when the requested revision has left the history window."""
        if not isinstance(revision, int) or isinstance(revision, bool) or revision < 0:
            raise ValueError("revision must be a non-negative integer")
        with self.lock:
            if not self._storage_supports_revisions():
                raise NotImplementedError("storage provider does not support revisioned changes")
            return self.storage.changes_since(revision)

    # noinspection PyNoneFunctionAssignment
    def yplookup(self, meta_all=None, meta_any=None, return_metadata=True):
        """
        Do a yellow-pages lookup for registrations that have all or any of the given metadata tags.
        By default returns the actual metadata in the result as well.
        """
        if meta_all and meta_any:
            raise ValueError("you can't use meta_all or meta_any at the same time")
        with self.lock:
            if meta_all:
                # return the entries which have all of the given metadata as (a subset of) their metadata
                if isinstance(meta_all, str):
                    raise TypeError("metadata_all should not be a str, but another iterable (set, list, etc)")
                meta_all and iter(meta_all)   # validate that metadata is iterable
                result = self.storage.optimized_metadata_search(metadata_all=meta_all, return_metadata=return_metadata)
                if result is not None:
                    return result
                meta_all = frozenset(meta_all)
                result = {}
                for name, (uri, meta) in self.storage.everything(return_metadata=True).items():
                    if meta_all.issubset(meta):
                        result[name] = (uri, meta) if return_metadata else uri
                return result
            elif meta_any:
                # return the entries which have any of the given metadata as part of their metadata
                if isinstance(meta_any, str):
                    raise TypeError("metadata_any should not be a str, but another iterable (set, list, etc)")
                meta_any and iter(meta_any)   # validate that metadata is iterable
                result = self.storage.optimized_metadata_search(metadata_any=meta_any, return_metadata=return_metadata)
                if result is not None:
                    return result
                meta_any = frozenset(meta_any)
                result = {}
                for name, (uri, meta) in self.storage.everything(return_metadata=True).items():
                    if meta_any & meta:
                        result[name] = (uri, meta) if return_metadata else uri
                return result
            else:
                return {}

    def ping(self):
        """A simple test method to check if the name server is running correctly."""
        pass


class NameServerDaemon(server.Daemon):
    """Daemon that contains the Name Server."""

    def __init__(self, host=None, port=None, unixsocket=None, nathost=None, natport=None, storage=None):
        if host is None:
            host = config.HOST
        elif not isinstance(host, str):
            host = str(host)  # take care of the occasion where host is an ipaddress.IpAddress
        if port is None:
            port = config.NS_PORT
        if nathost is None:
            nathost = config.NATHOST
        elif not isinstance(nathost, str):
            nathost = str(nathost)  # take care of the occasion where host is an ipaddress.IpAddress
        if natport is None:
            natport = config.NATPORT or None
        storage = storage or "memory"
        if storage == "memory":
            log.debug("using volatile in-memory dict storage")
            self.nameserver = NameServer(MemoryStorage())
        elif storage.startswith("sql:") and len(storage) > 4:
            sqlfile = storage[4:]
            log.debug("using persistent sql storage in file %s", sqlfile)
            self.nameserver = NameServer(SqlStorage(sqlfile))
        else:
            raise ValueError("invalid storage type '%s'" % storage)
        existing_count = self.nameserver.count()
        if existing_count > 0:
            log.debug("number of existing entries in storage: %d", existing_count)
        super(NameServerDaemon, self).__init__(host, port, unixsocket, nathost=nathost, natport=natport)
        self.register(self.nameserver, core.NAMESERVER_NAME)
        metadata = {"class:Pyro5.nameserver.NameServer"}
        self.nameserver.register(core.NAMESERVER_NAME, self.uriFor(self.nameserver), metadata=metadata)
        if config.NS_AUTOCLEAN > 0:
            if not AutoCleaner.override_autoclean_min and config.NS_AUTOCLEAN < AutoCleaner.min_autoclean_value:
                raise ValueError("NS_AUTOCLEAN cannot be smaller than " + str(AutoCleaner.min_autoclean_value))
            log.debug("autoclean enabled")
            self.cleaner_thread = AutoCleaner(self.nameserver)
            self.cleaner_thread.start()
        else:
            log.debug("autoclean not enabled")
            self.cleaner_thread = None
        log.info("nameserver daemon created")

    def close(self):
        super(NameServerDaemon, self).close()
        if self.nameserver is not None:
            self.nameserver.storage.close()
            self.nameserver = None
        if self.cleaner_thread:
            self.cleaner_thread.stop = True
            self.cleaner_thread.join()
            self.cleaner_thread = None

    def __enter__(self):
        if not self.nameserver:
            raise PyroError("cannot reuse this object")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.nameserver is not None:
            self.nameserver.storage.close()
        self.nameserver = None
        if self.cleaner_thread:
            self.cleaner_thread.stop = True
            self.cleaner_thread.join()
            self.cleaner_thread = None
        return super(NameServerDaemon, self).__exit__(exc_type, exc_value, traceback)

    def handleRequest(self, conn):
        try:
            return super(NameServerDaemon, self).handleRequest(conn)
        except ProtocolError as x:
            # Notify the user that a protocol error occurred.
            # This is useful for instance when a wrong serializer is used, it helps
            # a lot to immediately see what is going wrong.
            warnings.warn("Pyro protocol error occurred: " + str(x))
            raise


class AutoCleaner(threading.Thread):
    """
    Takes care of checking every registration in the name server.
    If it cannot be contacted anymore, it will be removed after ~20 seconds.
    """
    min_autoclean_value = 3
    max_unreachable_time = 20.0
    loop_delay = 2.0
    override_autoclean_min = False   # only for unit test purposes

    def __init__(self, nameserver):
        assert config.NS_AUTOCLEAN > 0
        if not self.override_autoclean_min and config.NS_AUTOCLEAN < self.min_autoclean_value:
            raise ValueError("NS_AUTOCLEAN cannot be smaller than " + str(self.min_autoclean_value))
        super(AutoCleaner, self).__init__()
        self.nameserver = nameserver
        self.stop = False
        self.daemon = True
        self.last_cleaned = time.time()
        self.unreachable = {}   # name->since when

    def run(self):
        while not self.stop:
            time.sleep(self.loop_delay)
            time_since_last_autoclean = time.time() - self.last_cleaned
            if time_since_last_autoclean < config.NS_AUTOCLEAN:
                continue
            for name, uri in self.nameserver.list().items():
                if name in (core.DAEMON_NAME, core.NAMESERVER_NAME):
                    continue
                try:
                    uri_obj = core.URI(uri)
                    timeout = config.COMMTIMEOUT or 5
                    sock = socketutil.create_socket(connect=(uri_obj.host, uri_obj.port), timeout=timeout)
                    sock.close()
                    # if we get here, the listed server is still answering on its port
                    if name in self.unreachable:
                        del self.unreachable[name]
                except socket.error:
                    if name not in self.unreachable:
                        self.unreachable[name] = time.time()
                    if time.time() - self.unreachable[name] >= self.max_unreachable_time:
                        log.info("autoclean: unregistering %s; cannot connect uri %s for %d sec", name, uri, self.max_unreachable_time)
                        self.nameserver.remove(name)
                        del self.unreachable[name]
                        continue
            self.last_cleaned = time.time()
            if self.unreachable:
                log.debug("autoclean: %d/%d names currently unreachable", len(self.unreachable), self.nameserver.count())


class BroadcastServer(object):
    class TransportServerAdapter(object):
        # this adapter is used to be able to pass the BroadcastServer to Daemon.combine() to integrate the event loops.
        def __init__(self, bcserver):
            self.sockets = [bcserver]

        def events(self, eventobjects):
            for bc in eventobjects:
                bc.processRequest()

    def __init__(self, nsUri, bchost=None, bcport=None, ipv6=False):
        self.transportServer = self.TransportServerAdapter(self)
        self.nsUri = nsUri
        if bcport is None:
            bcport = config.NS_BCPORT
        if bchost is None:
            bchost = config.NS_BCHOST
        elif not isinstance(bchost, str):
            bchost = str(bchost)  # take care of the occasion where host is an ipaddress.IpAddress
        if ":" in nsUri.host or ipv6:   # match nameserver's ip version
            bchost = bchost or "::"
            self.sock = socketutil.create_bc_socket((bchost, bcport, 0, 0), reuseaddr=config.SOCK_REUSE, timeout=2.0)
        else:
            self.sock = socketutil.create_bc_socket((bchost, bcport), reuseaddr=config.SOCK_REUSE, timeout=2.0)
        self._sockaddr = self.sock.getsockname()
        bchost = bchost or self._sockaddr[0]
        bcport = bcport or self._sockaddr[1]
        if ":" in bchost:  # ipv6
            self.locationStr = "[%s]:%d" % (bchost, bcport)
        else:
            self.locationStr = "%s:%d" % (bchost, bcport)
        log.info("ns broadcast server created on %s - %s", self.locationStr, socketutil.family_str(self.sock))
        self.running = True

    def close(self):
        log.debug("ns broadcast server closing")
        self.running = False
        with contextlib.suppress(OSError, socket.error):
            self.sock.shutdown(socket.SHUT_RDWR)
        self.sock.close()

    def getPort(self):
        return self.sock.getsockname()[1]

    def fileno(self):
        return self.sock.fileno()

    def runInThread(self):
        """Run the broadcast server loop in its own thread."""
        thread = threading.Thread(target=self.__requestLoop)
        thread.daemon = True
        thread.start()
        log.debug("broadcast server loop running in own thread")
        return thread

    def __requestLoop(self):
        while self.running:
            self.processRequest()
        log.debug("broadcast server loop terminating")

    def processRequest(self):
        with contextlib.suppress(socket.error):
            data, addr = self.sock.recvfrom(100)
            if data == b"GET_NSURI":
                responsedata = core.URI(self.nsUri)
                if responsedata.host == "0.0.0.0":
                    # replace INADDR_ANY address by the interface IP address that connects to the requesting client
                    with contextlib.suppress(socket.error):
                        interface_ip = socketutil.get_interface(addr[0]).ip
                        responsedata.host = str(interface_ip)
                log.debug("responding to broadcast request from %s: interface %s", addr[0], responsedata.host)
                responsedata = str(responsedata).encode("iso-8859-1")
                self.sock.sendto(responsedata, 0, addr)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def start_ns_loop(host=None, port=None, enableBroadcast=True, bchost=None, bcport=None,
                  unixsocket=None, nathost=None, natport=None, storage=None, 
                  loopCondition=lambda: True):
    """utility function that starts a new Name server and enters its requestloop."""
    daemon = NameServerDaemon(host, port, unixsocket, nathost=nathost, natport=natport, storage=storage)
    nsUri = daemon.uriFor(daemon.nameserver)
    internalUri = daemon.uriFor(daemon.nameserver, nat=False)
    bcserver = None
    if unixsocket:
        hostip = "Unix domain socket"
    else:
        hostip = daemon.sock.getsockname()[0]
        if daemon.sock.family == socket.AF_INET6:       # ipv6 doesn't have broadcast. We should probably use multicast instead...
            print("Not starting broadcast server for IPv6.")
            log.info("Not starting NS broadcast server because NS is using IPv6")
            enableBroadcast = False
        elif hostip.startswith("127.") or hostip in ("localhost", "::1"):
            print("Not starting broadcast server for localhost.")
            log.info("Not starting NS broadcast server because NS is bound to localhost")
            enableBroadcast = False
        if enableBroadcast:
            # Make sure to pass the internal uri to the broadcast responder.
            # It is almost always useless to let it return the external uri,
            # because external systems won't be able to talk to this thing anyway.
            bcserver = BroadcastServer(internalUri, bchost, bcport, ipv6=daemon.sock.family == socket.AF_INET6)
            print("Broadcast server running on %s" % bcserver.locationStr)
            bcserver.runInThread()
    existing = daemon.nameserver.count()
    if existing > 1:   # don't count our own nameserver registration
        print("Persistent store contains %d existing registrations." % existing)
    print("NS running on %s (%s)" % (daemon.locationStr, hostip))
    if daemon.natLocationStr:
        print("internal URI = %s" % internalUri)
        print("external URI = %s" % nsUri)
    else:
        print("URI = %s" % nsUri)
    sys.stdout.flush()
    try:
        daemon.requestLoop(loopCondition=loopCondition)
    finally:
        daemon.close()
        if bcserver is not None:
            bcserver.close()
    print("NS shut down.")


def start_ns(host=None, port=None, enableBroadcast=True, bchost=None, bcport=None,
             unixsocket=None, nathost=None, natport=None, storage=None):
    """utility fuction to quickly get a Name server daemon to be used in your own event loops.
    Returns (nameserverUri, nameserverDaemon, broadcastServer)."""
    daemon = NameServerDaemon(host, port, unixsocket, nathost=nathost, natport=natport, storage=storage)
    bcserver = None
    nsUri = daemon.uriFor(daemon.nameserver)
    if not unixsocket:
        hostip = daemon.sock.getsockname()[0]
        if hostip.startswith("127.") or hostip in ("localhost", "::1"):
            # not starting broadcast server for localhost.
            enableBroadcast = False
        if enableBroadcast:
            internalUri = daemon.uriFor(daemon.nameserver, nat=False)
            bcserver = BroadcastServer(internalUri, bchost, bcport, ipv6=daemon.sock.family == socket.AF_INET6)
    return nsUri, daemon, bcserver


def lookup(nameserver, name, return_metadata=False, delay_time=0):
    """
    Utility function to call nameserver.lookup,
    with the possibility of a retry loop until the asked name becomes available.
    You have to set the delay_time (or the corresponding config item)
    to the maximum number of seconds you are willing to wait.
    """
    delay_time = delay_time or config.NS_LOOKUP_DELAY
    start = time.time()
    while time.time()-start <= delay_time:
        try:
            return nameserver.lookup(name, return_metadata)
        except (errors.NamingError, errors.TimeoutError) as x:
            pass
        time.sleep(max(0.2, delay_time / 5))
    return nameserver.lookup(name, return_metadata)


def yplookup(nameserver, meta_all=None, meta_any=None, return_metadata=True, delay_time=0):
    """
    Utility function to call nameserver.yplookup,
    with the possibility of a retry loop until the asked name becomes available.
    You have to set the delay_time (or the corresponding config item)
    to the maximum number of seconds you are willing to wait.
    """
    delay_time = delay_time or config.NS_LOOKUP_DELAY
    start = time.time()
    while time.time()-start <= delay_time:
        try:
            result = nameserver.yplookup(meta_all, meta_any, return_metadata)
            if result:
                return result
        except (errors.NamingError, errors.TimeoutError) as x:
            pass
        time.sleep(max(0.2, delay_time / 5))
    return nameserver.yplookup(meta_all, meta_any, return_metadata)


def main(args=None):
    from argparse import ArgumentParser
    parser = ArgumentParser(description="Pyro name server command line launcher.")
    parser.add_argument("-n", "--host", dest="host", help="hostname to bind server on")
    parser.add_argument("-p", "--port", dest="port", type=int, help="port to bind server on (0=random)")
    parser.add_argument("-u", "--unixsocket", help="Unix domain socket name to bind server on")
    parser.add_argument("-s", "--storage", help="Storage system to use (memory, sql:file)", default="memory")
    parser.add_argument("--bchost", dest="bchost", help="hostname to bind broadcast server on (default is \"\")")
    parser.add_argument("--bcport", dest="bcport", type=int, help="port to bind broadcast server on (0=random)")
    parser.add_argument("--nathost", dest="nathost", help="external hostname in case of NAT")
    parser.add_argument("--natport", dest="natport", type=int, help="external port in case of NAT")
    parser.add_argument("-x", "--nobc", dest="enablebc", action="store_false", default=True,
                        help="don't start a broadcast server")
    options = parser.parse_args(args)
    start_ns_loop(options.host, options.port, enableBroadcast=options.enablebc,
                  bchost=options.bchost, bcport=options.bcport, unixsocket=options.unixsocket,
                  nathost=options.nathost, natport=options.natport, storage=options.storage)


if __name__ == "__main__":
    main()
