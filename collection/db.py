import datetime
import enum
from functools import lru_cache
import re
import sqlite3
from typing import Dict, Set, Hashable, NamedTuple, Type, TypeVar, Optional, Union

# generic row type
R = TypeVar("R", bound=NamedTuple)
# generic value type
T = TypeVar("T", bound=Hashable)

INSERT_MODES: Dict[Type[R], 'InsertMode'] = {}
PK_NAMES: Dict[Type[R], str] = {}
FK_NAMES: Dict[Type[R], Set[str]] = {}
ALT_PK_NAMES: Dict[Type[R], Set[str]] = {}
SQL_CONVERTERS = {
    datetime.datetime: datetime.datetime.fromisoformat,
    datetime.date: datetime.date.fromisoformat,
}


class InsertMode(enum.IntFlag):
    # if a tuple has a primary key set, check for existence in the associated table; if present,
    # update associated row using columns not in the primary key. otherwise, insert a new row.
    InsertIfNewPKElseUpdate = 1
    # if a tuple has an alternate key set, check for existence in the associated table; if present,
    # update the associated row using columns not in the alternate key or primary key. otherwise,
    # insert a new row.
    InsertIfNewAltPKElseUpdate = 2
    # if a tuple has a primary key set, check for existence in the associated table; if present,
    # do nothing. otherwise, insert a new row.
    InsertIfNewPKElseIgnore = 4
    # if a tuple has an alternate key set, check for existence in the associated table; if present,
    # do nothing. otherwise, insert a new row.
    InsertIfNewAltPKElseIgnore = 8
    # always insert a row if a tuple lacks a simple primary key. This can be a fallback for the other
    # modes for tuples without primary keys set when specified
    InsertIfNoPK = 16


def register_insert_mode(mode: InsertMode):
    def dec(type_: Type[R]):
        INSERT_MODES[type_] = mode
        return type_

    return dec


def register_pk_name(pkname: str):
    def dec(type_: Type[R]):
        if pkname not in type_._fields:
            raise NameError("Type {} has no field named {!r}".format(type_.__name__, pkname))
        PK_NAMES[type_] = pkname
        return type_

    return dec


def register_fk_names(*fknames_: str):
    def dec(type_: Type[R]) -> Type[R]:
        fknames = set(fknames_)
        missing = [fk for fk in fknames if fk not in type_._fields]
        if missing:
            raise NameError("Type {} has no fields named {!r}".format(type_.__name__, missing))
        FK_NAMES[type_] = {n + "_id" for n in fknames}
        return type_

    return dec


def register_alt_pk_names(*names_: str):
    def dec(type_: Type[R]) -> Type[R]:
        names = set(names_)
        missing = [fk for fk in names if fk not in type_._fields]
        if missing:
            raise NameError("Type {} has no fields named {!r}".format(type_.__name__, missing))
        types = type_._field_types
        names = {name + '_id' if issubclass(types[name], tuple) else name for name in names}
        ALT_PK_NAMES[type_] = names
        return type_

    return dec


def insert_mode(value: R) -> InsertMode:
    return _insert_mode(type(value))


@lru_cache(None)
def _insert_mode(type_: Type[R]) -> InsertMode:
    mode = INSERT_MODES.get(type_)
    if mode is None:
        mode = InsertMode.InsertIfNewPKElseUpdate | InsertMode.InsertIfNewAltPKElseUpdate | InsertMode.InsertIfNoPK
        mode: InsertMode
        return mode


def primary_key_name(value: R) -> str:
    return _primary_key_name(type(value))


@lru_cache(None)
def _primary_key_name(type_: Type[R]) -> Optional[str]:
    pkname = PK_NAMES.get(type_)
    if pkname is not None:
        return pkname
    tname = _table_name(type_)
    pkname = tname + "_id"
    if pkname not in type_._fields:
        return None
    return pkname


def foreign_key_names(value: R) -> Set[str]:
    return _foreign_key_names(type(value))


@lru_cache(None)
def _foreign_key_names(type_: Type[R]) -> Set[str]:
    fknames = FK_NAMES.get(type_)
    if fknames is not None:
        return fknames
    return {n + "_id" for n, t in type._field_types.items() if issubclass(t, tuple)}


def table_name(value: R) -> str:
    return _table_name(type(value))


@lru_cache(None)
def _table_name(type_: Type[R]) -> str:
    typename = type_.__name__
    return '_'.join(map(str.lower, re.findall('[A-Z][a-z]+', typename)))


def convert_sql_result_value(value, type_: Type[T]) -> T:
    if isinstance(value, type_):
        return value
    conv = SQL_CONVERTERS.get(type_)
    if conv is None:
        return value
    return conv(value)


class DB:
    def __init__(self, conn: Union[sqlite3.Connection, str], readonly: bool = False):
        if isinstance(conn, str):
            conn = sqlite3.connect(conn)
        self.readonly = readonly
        self.conn = conn
        if self.readonly:
            self.conn.execute("PRAGMA foreign_keys = ON")

    def init_schema(self, schema_path: str) -> sqlite3.Cursor:
        with open(schema_path, "r") as f:
            return self.conn.executescript(f.read())

    def __str__(self):
        return "%s(%r, readonly=%r)" % (type(self).__name__, self.conn, self.readonly)

    def insert_or_update(self, value: R, check_if_pk_not_null: bool = True) -> int:
        if self.readonly:
            raise ValueError("readonly = True for DB {}; can't mutate".format(self))
        tablename = table_name(value)
        pkname = primary_key_name(value)
        type_ = type(value)

        insert_values = []
        insert_columns = []
        insert_dependency_columns = []
        insert_dependencies = []
        insert_dependency_pks = []
        for c, v in zip(type_._fields, value):
            if c == pkname:
                continue
            elif isinstance(v, tuple):
                v: NamedTuple
                insert_dependency_columns.append(c)
                insert_dependencies.append(v)
            else:
                insert_columns.append(c)
                insert_values.append(v)

        pkvalue = getattr(value, pkname)

        if (pkvalue is not None) and check_if_pk_not_null:
            update = self._contains(tablename, pkname, pkvalue)
        else:
            # otherwise assume caller knows what they're doing at this is an insert
            if pkvalue is not None:
                update = False
                insert_columns.append(pkname)
                insert_values.append(pkvalue)

        for subvalue in insert_dependencies:
            inserted_pk = self.insert_or_update(subvalue, check_if_pk_not_null=check_if_pk_not_null)
            insert_dependency_pks.append(inserted_pk)

        insert_columns.extend(c + "_id" for c in insert_dependency_columns)
        insert_values.extend(insert_dependency_pks)

        if update:
            stmt = "UPDATE %s SET %s WHERE %s = ?" % (
                tablename,
                ", ".join(map("%s = ?".__mod__, insert_columns)),
                pkname,
            )
            params = insert_values
            params.append(pkvalue)
        else:
            stmt = "INSERT INTO %s(%s) VALUES (%s)" % (
                tablename,
                ", ".join(insert_columns),
                ", ".join("?" * len(insert_columns)),
            )
            params = insert_values

        cur = self.conn.execute(stmt, params)
        return pkvalue if update else cur.lastrowid

    def _update_with_rowid(self):
        ...

    def _update_with_alternate_key(self):
        ...

    def get(self, type_: Type[R], pkvalue: int) -> Optional[R]:
        tablename = _table_name(type_)
        pkname = _primary_key_name(type_)
        select = "SELECT (%s) FROM %s WHERE %s = ?" % (
            ", ".join(type_._fields),
            tablename,
            pkname,
        )
        cur = self.conn.execute(select, (pkvalue,))
        row = cur.fetchone()
        if row is None:
            return None

        fknames = _foreign_key_names(type_)
        kwargs = {}

        for tup, val in zip(cur.description, row):
            n = tup[0]
            if n in fknames:
                fieldname = n[:-3] if n.endswith("_id") else n
                fieldtype = type_._field_types[fieldname]
                kwargs[n] = self.get(fieldtype, val)
            else:
                kwargs[n] = convert_sql_result_value(val, type_._field_types[n])

        return type_(**kwargs)

    @lru_cache(1_000_000)
    def _contains(self, tablename: str, pkname: str, pkvalue: int) -> bool:
        select = "SELECT %s FROM %s WHERE %s = ?" % (
            pkname, tablename, pkname
        )
        return self.conn.execute(select, (pkvalue,)).fetchone() is not None
