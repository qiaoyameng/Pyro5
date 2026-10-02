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
import secrets
import math
from collections.abc import MutableMapping
try:
    import sqlite3
except ImportError:
    pass
from . import config, core, socketutil, server, errors
from .errors import NamingError, PyroError, ProtocolError


__all__ = ["start_ns_loop", "start_ns"]

log = logging.getLogger("Pyro5.naming")


class MemoryStorage(dict):
    """
    Storage implementation that is just an in-memory dict.
    (because it inherits from dict it is automatically a collections.MutableMapping)
    Stopping the nameserver will make the server instantly forget about everything.
    """
    def __init__(self, **kwargs):
        super(MemoryStorage, self).__init__(**kwargs)

    def __getitem__(self, item):
        uri, metadata, token, expires_at, ttl = super(MemoryStorage, self).__getitem__(item)
        return uri, metadata

    def __setitem__(self, key, value):
        if len(value) == 2:
            uri, metadata = value
            token = expires_at = ttl = None
        elif len(value) == 5:
            uri, metadata, token, expires_at, ttl = value
        else:
            raise ValueError("registration must contain uri and metadata, and optionally lease information")
        super(MemoryStorage, self).__setitem__(key, (uri, metadata if metadata is not None else frozenset(), token, expires_at, ttl))

    def put_registration(self, name, uri, metadata, token=None, expires_at=None, ttl=None):
        self[name] = uri, metadata, token, expires_at, ttl

    def get_lease(self, name):
        uri, metadata, token, expires_at, ttl = super(MemoryStorage, self).__getitem__(name)
        return token, expires_at, ttl

    def active_names(self, now):
        return {
            name
            for name, (uri, metadata, token, expires_at, ttl) in self.items()
            if expires_at is None or expires_at > now
        }

    def renew_registration(self, name, token, now, expires_at, ttl):
        try:
            uri, metadata, current_token, current_expires, current_ttl = super().__getitem__(name)
        except KeyError:
            return False
        if current_token != token or current_expires is None or current_expires <= now:
            return False
        self[name] = uri, metadata, token, expires_at, ttl if ttl is not None else current_ttl
        return True

    def remove_registration(self, name, token, now):
        try:
            current_token, expires_at, ttl = self.get_lease(name)
        except KeyError:
            return False
        if current_token != token or expires_at is None or expires_at <= now:
            return False
        del self[name]
        return True

    def remove_expired_registrations(self, now, name=None, token=None):
        if (name is None) != (token is None):
            raise ValueError("name and token must both be provided")
        expired = [
            current_name
            for current_name, (uri, metadata, current_token, expires_at, ttl) in self.items()
            if expires_at is not None and expires_at <= now
            and (name is None or current_name == name and current_token == token)
        ]
        self.remove_items(expired)
        return len(expired)

    def optimized_prefix_list(self, prefix, return_metadata=False):
        return None

    def optimized_regex_list(self, regex, return_metadata=False):
        return None

    def optimized_metadata_search(self, metadata_all=None, metadata_any=None, return_metadata=False):
        return None

    def everything(self, return_metadata=False):
        if return_metadata:
            return {name: (uri, metadata) for name, (uri, metadata, token, expires_at, ttl) in self.items()}
        return {name: uri for name, (uri, metadata, token, expires_at, ttl) in self.items()}

    def remove_items(self, items):
        for item in items:
            if item in self:
                del self[item]

    def close(self):
        pass


class SqlStorage(MutableMapping):
    """
    Sqlite-based storage.
    It is just a single (name,uri) table for the names and another table for the metadata.
    Sqlite db connection objects aren't thread-safe, so a new connection is created in every method.
    """
    def __init__(self, dbfile):
        if dbfile == ":memory:":
            raise ValueError("We don't support the sqlite :memory: database type. Just use the default volatile in-memory store.")
        self.dbfile = dbfile
        with sqlite3.connect(dbfile) as db:
            db.execute("PRAGMA foreign_keys=ON")
            try:
                db.execute("SELECT token, expires_at, ttl FROM pyro_names LIMIT 1").fetchone()
            except sqlite3.OperationalError:
                try:
                    db.execute("SELECT COUNT(*) FROM pyro_metadata").fetchone()
                except sqlite3.OperationalError:
                    try:
                        db.execute("SELECT COUNT(*) FROM pyro_names").fetchone()
                    except sqlite3.OperationalError:
                        self._create_schema(db)
                    else:
                        db.execute("ALTER TABLE pyro_names RENAME TO pyro_names_old")
                        self._create_schema(db)
                        db.execute("INSERT INTO pyro_names(name, uri) SELECT name, uri FROM pyro_names_old")
                        db.execute("DROP TABLE pyro_names_old")
                else:
                    db.execute("ALTER TABLE pyro_names RENAME TO pyro_names_old")
                    db.execute("ALTER TABLE pyro_metadata RENAME TO pyro_metadata_old")
                    self._create_schema(db)
                    db.execute("""
                        INSERT INTO pyro_names(name, uri, token, expires_at, ttl)
                        SELECT name, uri, NULL, NULL, NULL FROM pyro_names_old
                    """)
                    db.execute("""
                        INSERT INTO pyro_metadata(object, metadata)
                        SELECT names.id, old_metadata.metadata
                        FROM pyro_metadata_old old_metadata
                        JOIN pyro_names_old old_names ON old_names.id = old_metadata.object
                        JOIN pyro_names names ON names.name = old_names.name
                    """)
                    db.execute("DROP TABLE pyro_metadata_old")
                    db.execute("DROP TABLE pyro_names_old")
            db.commit()

    def _create_schema(self, db):
        db.execute("""CREATE TABLE pyro_names
            (
                id integer PRIMARY KEY,
                name nvarchar NOT NULL,
                uri nvarchar NOT NULL,
                token nvarchar,
                expires_at float,
                ttl float
            );""")
        db.execute("CREATE INDEX idx_pyro_names_name ON pyro_names(name)")
        db.execute("CREATE INDEX idx_pyro_names_expires ON pyro_names(expires_at)")
        db.execute("""CREATE TABLE pyro_metadata
            (
                object integer NOT NULL,
                metadata nvarchar NOT NULL,
                FOREIGN KEY(object) REFERENCES pyro_names(id) ON DELETE CASCADE
            );""")

    def __getattr__(self, item):
        raise NotImplementedError("SqlStorage doesn't implement method/attribute '" + item + "'")

    def __getitem__(self, item):
        try:
            with sqlite3.connect(self.dbfile) as db:
                result = db.execute("SELECT id, uri FROM pyro_names WHERE name=? ORDER BY id DESC LIMIT 1", (item,)).fetchone()
                if result:
                    dbid, uri = result
                    metadata = {m[0] for m in db.execute("SELECT metadata FROM pyro_metadata WHERE object=?", (dbid,)).fetchall()}
                    return uri, metadata
                else:
                    raise KeyError(item)
        except sqlite3.DatabaseError as e:
            raise NamingError("sqlite error in getitem: " + str(e))

    def __setitem__(self, key, value):
        if len(value) == 2:
            uri, metadata = value
            token = expires_at = ttl = None
        elif len(value) == 5:
            uri, metadata, token, expires_at, ttl = value
        else:
            raise ValueError("registration must contain uri and metadata, and optionally lease information")
        try:
            with sqlite3.connect(self.dbfile) as db:
                cursor = db.cursor()
                cursor.execute("PRAGMA foreign_keys=ON")
                cursor.execute(
                    "INSERT INTO pyro_names(name, uri, token, expires_at, ttl) VALUES(?,?,?,?,?)",
                    (key, uri, token, expires_at, ttl)
                )
                if metadata:
                    object_id = cursor.lastrowid
                    for m in metadata:
                        cursor.execute("INSERT INTO pyro_metadata(object, metadata) VALUES (?,?)", (object_id, m))
                cursor.close()
                db.commit()
        except sqlite3.DatabaseError as e:
            raise NamingError("sqlite error in setitem: " + str(e))

    def put_registration(self, name, uri, metadata, token=None, expires_at=None, ttl=None):
        self[name] = uri, metadata, token, expires_at, ttl

    def get_lease(self, name):
        try:
            with sqlite3.connect(self.dbfile) as db:
                result = db.execute(
                    "SELECT token, expires_at, ttl FROM pyro_names WHERE name=? ORDER BY id DESC LIMIT 1",
                    (name,)
                ).fetchone()
                if result:
                    return result
                raise KeyError(name)
        except sqlite3.DatabaseError as e:
            raise NamingError("sqlite error in get_lease: " + str(e))

    def active_names(self, now):
        try:
            with sqlite3.connect(self.dbfile) as db:
                rows = db.execute(
                    """
                    SELECT name FROM pyro_names n
                    WHERE n.id=(SELECT MAX(id) FROM pyro_names n2 WHERE n2.name=n.name)
                      AND (n.expires_at IS NULL OR n.expires_at > ?)
                    """,
                    (now,)
                ).fetchall()
                return {row[0] for row in rows}
        except sqlite3.DatabaseError as e:
            raise NamingError("sqlite error in active_names: " + str(e))

    def renew_registration(self, name, token, now, expires_at, ttl=None):
        try:
            with sqlite3.connect(self.dbfile) as db:
                cursor = db.execute("""
                    UPDATE pyro_names
                    SET expires_at=?, ttl=COALESCE(?, ttl)
                    WHERE name=? AND token=? AND expires_at IS NOT NULL AND expires_at > ?
                      AND id=(SELECT MAX(id) FROM pyro_names latest WHERE latest.name=pyro_names.name)
                """, (expires_at, ttl, name, token, now))
                db.commit()
                return cursor.rowcount == 1
        except sqlite3.DatabaseError as e:
            raise NamingError("sqlite error in renew_registration: " + str(e))

    def remove_registration(self, name, token, now):
        try:
            with sqlite3.connect(self.dbfile) as db:
                db.execute("PRAGMA foreign_keys=ON")
                cursor = db.execute(
                    """
                    DELETE FROM pyro_names
                    WHERE name=? AND token=? AND expires_at IS NOT NULL AND expires_at > ?
                      AND id=(SELECT MAX(id) FROM pyro_names latest WHERE latest.name=pyro_names.name)
                    """,
                    (name, token, now)
                )
                db.commit()
                return cursor.rowcount == 1
        except sqlite3.DatabaseError as e:
            raise NamingError("sqlite error in remove_registration: " + str(e))

    def remove_expired_registrations(self, now, name=None, token=None):
        if (name is None) != (token is None):
            raise ValueError("name and token must both be provided")
        try:
            with sqlite3.connect(self.dbfile) as db:
                db.execute("PRAGMA foreign_keys=ON")
                params = [now]
                sql = "DELETE FROM pyro_names WHERE expires_at IS NOT NULL AND expires_at <= ?"
                if name is not None:
                    sql += " AND name=? AND token=?"
                    params.extend((name, token))
                cursor = db.execute(sql, params)
                db.commit()
                return cursor.rowcount
        except sqlite3.DatabaseError as e:
            raise NamingError("sqlite error in remove_expired_registrations: " + str(e))

    def __len__(self):
        try:
            with sqlite3.connect(self.dbfile) as db:
                return db.execute("SELECT count(*) FROM pyro_names").fetchone()[0]
        except sqlite3.DatabaseError as e:
            raise NamingError("sqlite error in len: " + str(e))

    def __contains__(self, item):
        try:
            with sqlite3.connect(self.dbfile) as db:
                return db.execute(
                    "SELECT EXISTS(SELECT 1 FROM pyro_names n WHERE n.name=? AND n.id=(SELECT MAX(id) FROM pyro_names latest WHERE latest.name=n.name))",
                    (item,)
                ).fetchone()[0]
        except sqlite3.DatabaseError as e:
            raise NamingError("sqlite error in contains: " + str(e))

    def __delitem__(self, key):
        try:
            with sqlite3.connect(self.dbfile) as db:
                db.execute("PRAGMA foreign_keys=ON")
                db.execute("DELETE FROM pyro_names WHERE name=?", (key,))
                db.commit()
        except sqlite3.DatabaseError as e:
            raise NamingError("sqlite error in delitem: " + str(e))

    def __iter__(self):
        try:
            with sqlite3.connect(self.dbfile) as db:
                result = db.execute("""
                    SELECT name FROM pyro_names n
                    WHERE n.id=(SELECT MAX(id) FROM pyro_names latest WHERE latest.name=n.name)
                """)
                return iter([n[0] for n in result.fetchall()])
        except sqlite3.DatabaseError as e:
            raise NamingError("sqlite error in iter: " + str(e))

    def clear(self):
        try:
            with sqlite3.connect(self.dbfile) as db:
                db.execute("PRAGMA foreign_keys=ON")
                db.execute("DELETE FROM pyro_metadata")
                db.execute("DELETE FROM pyro_names")
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
                    rows = db.execute("""
                        SELECT n.id, n.name, n.uri FROM pyro_names n
                        WHERE n.name LIKE ?
                          AND n.id=(SELECT MAX(id) FROM pyro_names latest WHERE latest.name=n.name)
                    """, (prefix + '%',)).fetchall()
                    for dbid, name, uri in rows:
                        metadata = {m[0] for m in db.execute("SELECT metadata FROM pyro_metadata WHERE object=?", (dbid,)).fetchall()}
                        names[name] = uri, metadata
                else:
                    for name, uri in db.execute("""
                        SELECT n.name, n.uri FROM pyro_names n
                        WHERE n.name LIKE ?
                          AND n.id=(SELECT MAX(id) FROM pyro_names latest WHERE latest.name=n.name)
                    """, (prefix + '%',)).fetchall():
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
                    sql = """SELECT id, name, uri FROM (
                        SELECT n.* FROM pyro_names n
                        WHERE n.id=(SELECT MAX(id) FROM pyro_names latest WHERE latest.name=n.name)
                    ) WHERE id IN (SELECT object FROM pyro_metadata WHERE metadata IN ({seq}))""" \
                          .format(seq=",".join(['?'] * len(metadata_any)))
                else:
                    # all of the given metadata
                    params = list(metadata_all)
                    params.append(len(metadata_all))
                    sql = """SELECT id, name, uri FROM (
                        SELECT n.* FROM pyro_names n
                        WHERE n.id=(SELECT MAX(id) FROM pyro_names latest WHERE latest.name=n.name)
                    ) WHERE id IN (SELECT object FROM pyro_metadata WHERE metadata IN ({seq})
                        GROUP BY object HAVING COUNT(metadata)=?)""".format(seq=",".join(['?'] * len(metadata_all)))
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
                for item in items:
                    dbid = db.execute(
                        "SELECT MAX(id) FROM pyro_names WHERE name=?",
                        (item,)
                    ).fetchone()
                    if dbid:
                        dbid = dbid[0]
                        db.execute("DELETE FROM pyro_metadata WHERE object=?", (dbid,))
                        db.execute("DELETE FROM pyro_names WHERE id=?", (dbid,))
                db.commit()
        except sqlite3.DatabaseError as e:
            raise NamingError("sqlite error in remove_items: " + str(e))

    def everything(self, return_metadata=False):
        try:
            with sqlite3.connect(self.dbfile) as db:
                names = {}
                if return_metadata:
                    rows = db.execute("""
                        SELECT n.id, n.name, n.uri FROM pyro_names n
                        WHERE n.id=(SELECT MAX(id) FROM pyro_names latest WHERE latest.name=n.name)
                    """).fetchall()
                    for dbid, name, uri in rows:
                        metadata = {m[0] for m in db.execute("SELECT metadata FROM pyro_metadata WHERE object=?", (dbid,)).fetchall()}
                        names[name] = uri, metadata
                else:
                    for name, uri in db.execute("""
                        SELECT n.name, n.uri FROM pyro_names n
                        WHERE n.id=(SELECT MAX(id) FROM pyro_names latest WHERE latest.name=n.name)
                    """).fetchall():
                        names[name] = uri
                return names
        except sqlite3.DatabaseError as e:
            raise NamingError("sqlite error in everything: " + str(e))

    def close(self):
        pass


@server.expose
class NameServer(object):
    """
    Pyro name server. Provides a simple flat name space to map logical object names to Pyro URIs.
    Default storage is done in an in-memory dictionary. You can provide custom storage types.
    """
    def __init__(self, storageProvider=None, time_func=None):
        self.storage = storageProvider
        if storageProvider is None:
            self.storage = MemoryStorage()
            log.debug("using volatile in-memory dict storage")
        self.time_func = time_func or time.time
        self.lock = threading.RLock()

    def _now(self):
        return self.time_func()

    def _get_lease(self, name):
        if hasattr(self.storage, "get_lease"):
            return self.storage.get_lease(name)
        return None, None, None

    def _active_names(self):
        if hasattr(self.storage, "active_names"):
            return self.storage.active_names(self._now())
        return set(self.storage)

    def _is_active(self, name):
        token, expires_at, ttl = self._get_lease(name)
        return expires_at is None or expires_at > self._now()

    def _filter_active(self, result):
        active_names = self._active_names()
        return {name: value for name, value in result.items() if name in active_names}

    def _validate_ttl(self, ttl):
        if ttl is None:
            return None
        if not isinstance(ttl, (int, float)) or isinstance(ttl, bool) or not math.isfinite(ttl) or ttl <= 0:
            raise ValueError("ttl must be a positive finite number of seconds")
        return float(ttl)

    def _new_token(self):
        return secrets.token_urlsafe(32)

    def count(self):
        """Returns the number of name registrations."""
        with self.lock:
            return len(self._active_names())

    def lookup(self, name, return_metadata=False):
        """
        Lookup the given name, returns an URI if found.
        Returns tuple (uri, metadata) if return_metadata is True.
        """
        now = self._now()
        try:
            stored = self.storage[name]
            uri, metadata = stored[0], stored[1]
            token, expires_at, ttl = self._get_lease(name)
            if expires_at is not None and expires_at <= now:
                raise KeyError(name)
            uri = core.URI(uri)
            if return_metadata:
                return uri, set(metadata or [])
            return uri
        except KeyError:
            raise NamingError("unknown name: " + name)

    def register(self, name, uri, safe=False, metadata=None, ttl=None):
        """Register a name with an URI. If safe is true, name cannot be registered twice.
        The uri can be a string or an URI object. Metadata must be None, or a collection of strings.
        If ttl is given, the registration expires after that number of seconds and an opaque owner token is returned."""
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
        ttl = self._validate_ttl(ttl)
        with self.lock:
            if safe and name in self.storage and self._is_active(name):
                raise NamingError("name already registered: " + name)
            token = None
            expires_at = None
            stored_ttl = None
            if ttl is not None:
                token = self._new_token()
                expires_at = self._now() + ttl
                stored_ttl = ttl
            metadata = set(metadata) if metadata is not None else None
            if hasattr(self.storage, "put_registration"):
                self.storage.put_registration(name, uri, metadata, token, expires_at, stored_ttl)
            elif ttl is not None:
                raise NamingError("storage provider does not support ttl registrations")
            else:
                self.storage[name] = uri, metadata
            return token

    def set_metadata(self, name, metadata):
        """update the metadata for an existing registration"""
        if not isinstance(name, str):
            raise TypeError("name must be a str")
        if isinstance(metadata, str):
            raise TypeError("metadata should not be a str, but another iterable (set, list, etc)")
        metadata and iter(metadata)  # validate that metadata is iterable
        with self.lock:
            try:
                stored = self.storage[name]
                uri = stored[0]
                token, expires_at, ttl = self._get_lease(name)
                if expires_at is not None and expires_at <= self._now():
                    raise KeyError(name)
                new_metadata = set(metadata) if metadata is not None else None
                if hasattr(self.storage, "put_registration"):
                    self.storage.put_registration(name, uri, new_metadata, token, expires_at, ttl)
                else:
                    self.storage[name] = uri, new_metadata
            except KeyError:
                raise NamingError("unknown name: " + name)

    def remove(self, name=None, prefix=None, regex=None, token=None):
        """Remove a registration. returns the number of items removed."""
        if name and name != core.NAMESERVER_NAME:
            with self.lock:
                if token is not None:
                    self.release(name, token)
                    return 1
                try:
                    token, expires_at, ttl = self._get_lease(name)
                except KeyError:
                    return 0
                if expires_at is not None:
                    raise NamingError("ttl registration must be released with its owner token")
                del self.storage[name]
                return 1
        if prefix:
            with self.lock:
                items = [
                    item for item in self.list(prefix=prefix).keys()
                    if self._get_lease(item)[1] is None
                ]
                if core.NAMESERVER_NAME in items:
                    items.remove(core.NAMESERVER_NAME)
                self.storage.remove_items(items)
                return len(items)
        if regex:
            with self.lock:
                items = [
                    item for item in self.list(regex=regex).keys()
                    if self._get_lease(item)[1] is None
                ]
                if core.NAMESERVER_NAME in items:
                    items.remove(core.NAMESERVER_NAME)
                self.storage.remove_items(items)
                return len(items)
        return 0

    def renew(self, name, token, ttl=None):
        """Renew a TTL registration. The name and owner token must both match."""
        if not isinstance(name, str):
            raise TypeError("name must be a str")
        if not isinstance(token, str):
            raise TypeError("token must be a str")
        ttl = self._validate_ttl(ttl)
        with self.lock:
            try:
                current_token, expires_at, current_ttl = self._get_lease(name)
            except KeyError:
                raise NamingError("unknown or expired name: " + name)
            if current_token != token or expires_at is None or expires_at <= self._now():
                raise NamingError("unknown or expired name: " + name)
            ttl = ttl if ttl is not None else current_ttl
            expires_at = self._now() + ttl
            if not self.storage.renew_registration(name, token, self._now(), expires_at, ttl):
                raise NamingError("unknown or expired name: " + name)
            return expires_at

    def release(self, name, token):
        """Explicitly release a TTL registration. The name and owner token must both match."""
        if not isinstance(name, str):
            raise TypeError("name must be a str")
        if not isinstance(token, str):
            raise TypeError("token must be a str")
        with self.lock:
            if not self.storage.remove_registration(name, token, self._now()):
                raise NamingError("unknown or expired name: " + name)

    def cleanup_expired(self, name=None, token=None):
        """Delete expired records, optionally matching a name and owner token. Returns the number removed."""
        if (name is None) != (token is None):
            raise ValueError("name and token must both be provided")
        if name is not None and (not isinstance(name, str) or not isinstance(token, str)):
            raise TypeError("name and token must be strings")
        with self.lock:
            if not hasattr(self.storage, "remove_expired_registrations"):
                return 0
            return self.storage.remove_expired_registrations(self._now(), name, token)

    refresh = renew
    unregister = release
    prune_expired = cleanup_expired

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
                    return self._filter_active(result)
                result = {}
                for name, value in self.storage.everything(return_metadata=True).items():
                    if name.startswith(prefix):
                        result[name] = value if return_metadata else value[0]
                return self._filter_active(result)
            elif regex:
                result = self.storage.optimized_regex_list(regex, return_metadata)
                if result is not None:
                    return self._filter_active(result)
                result = {}
                try:
                    regex = re.compile(regex)
                except re.error as x:
                    raise errors.NamingError("invalid regex: " + str(x))
                else:
                    for name, value in self.storage.everything(return_metadata=True).items():
                        if regex.match(name):
                            result[name] = value if return_metadata else value[0]
                    return self._filter_active(result)
            else:
                # just return (a copy of) everything
                return self._filter_active(self.storage.everything(return_metadata))

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
                    return self._filter_active(result)
                meta_all = frozenset(meta_all)
                result = {}
                for name, (uri, meta) in self.storage.everything(return_metadata=True).items():
                    if meta_all.issubset(meta):
                        result[name] = (uri, meta) if return_metadata else uri
                return self._filter_active(result)
            elif meta_any:
                # return the entries which have any of the given metadata as part of their metadata
                if isinstance(meta_any, str):
                    raise TypeError("metadata_any should not be a str, but another iterable (set, list, etc)")
                meta_any and iter(meta_any)   # validate that metadata is iterable
                result = self.storage.optimized_metadata_search(metadata_any=meta_any, return_metadata=return_metadata)
                if result is not None:
                    return self._filter_active(result)
                meta_any = frozenset(meta_any)
                result = {}
                for name, (uri, meta) in self.storage.everything(return_metadata=True).items():
                    if meta_any & meta:
                        result[name] = (uri, meta) if return_metadata else uri
                return self._filter_active(result)
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
                if self.nameserver._get_lease(name)[1] is not None:
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
