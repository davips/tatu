#  Copyright (c) 2020. Davi Pereira dos Santos
#      This file is part of the tatu project.
#      Please respect the license. Removing authorship by any means
#      (by code make up or closing the sources) or ignoring property rights
#      is a crime and is unethical regarding the effort and time spent here.
#      Relevant employers or funding agencies will be notified accordingly.
#
#      tatu is free software: you can redistribute it and/or modify
#      it under the terms of the GNU General Public License as published by
#      the Free Software Foundation, either version 3 of the License, or
#      (at your option) any later version.
#
#      tatu is distributed in the hope that it will be useful,
#      but WITHOUT ANY WARRANTY; without even the implied warranty of
#      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#      GNU General Public License for more details.
#
#      You should have received a copy of the GNU General Public License
#      along with tatu.  If not, see <http://www.gnu.org/licenses/>.
#

import json
import warnings
from abc import abstractmethod, ABC
from typing import Optional

from aiuna.compression import unpack, pack
from aiuna.content.data import Data, Picklable
from cruipto.uuid import UUID
from tatu.storage import Storage, DuplicateEntryException, LockedEntryException, MissingEntryException


class SQL(Storage, ABC):
    cursor = None
    storage_info = None

    # TODO: check if all queries are safely interpolated

    def _delete_(self, data: Data, check_missing=True):
        # TODO: delete dangling matrices? aproveitar checagem interna de chave estrangeira do SQL pra isso?
        # TODO:
        if check_missing:
            self.query(f"select t from data where id=?", [data.id])
            if self.get_one() is None:
                raise MissingEntryException("Does not exist:", data.id)
        self.query(f"delete from data where id=?", [data.id])

    # TODO: remove training_data_uuid from here and put it inside transformations
    def _store_(self, data: Data, check_dup=True):
        # The sequence of queries is planned to minimize traffic and CPU load,
        # otherwise it would suffice to just send 'insert or ignore' of dumps.
        uuid = data.uuid
        self.query(f"select t from data where id=?", [uuid.id])
        rone = self.get_one()

        if rone:
            locked = rone["t"] == "0000-00-00 00:00:00"
            if check_dup:
                raise DuplicateEntryException("Already exists:", uuid.id)
        else:
            locked = False

        # Check if dumps of matrices/vectors already exist.
        qmarks = ",".join(["?"] * len(data.uuids))
        print(">>>>>>>>", qmarks)
        print("idslllllllllllll", data.ids_lst)
        self.query(f"select id from dump where id in ({qmarks})", data.ids_lst)
        rall = self.get_all()
        stored_hashes = [row["id"] for row in rall]

        # Insert only matrices that are missing in storage
        dic = {}
        for name, u in data.uuids.items():
            if u.id not in stored_hashes:
                dic[u.id] = data.field_dump(name)
        self.store_dump(dic)

        # Insert history.  #TODO: replace by recursive table PARENT
        dic = {hid: pack(stepstr) for hid, stepstr in data.history.items()}
        self.store_dump(dic)

        # Insert Data object.
        if not locked:
            # ensure UNIQUE constraint (just in case something changed in the meantime since select*)
            if check_dup:
                sql = f"insert into data values (NULL, ?, ?, ?, ?, ?, {self._now_function()})"
            else:
                sql = f"replace into data values (NULL, ?, ?, ?, ?, ?, {self._now_function()})"
            data_args = [uuid.id, data.inner and data.inner.id, data.matrix_names_str, data.ids_str, data.history_str]
        else:
            sql = f"update data set inn=?, names=?, matrices=?, history=?, t={self._now_function()} where id=?"
            data_args = [data.inner and data.inner.id, data.matrix_names_str, data.ids_str, data.history_str, uuid.id]

        # from sqlite3 import IntegrityError as IntegrityErrorSQLite
        # from pymysql import IntegrityError as IntegrityErrorMySQL
        # try:
        self.query(sql, data_args)
        # unfortunately,
        # it seems that FKs generate the same exception as reinsertion.
        # so, missing FKs might not be detected here.
        # not a worrying issue whatsoever.
        # TODO: it seems to be capturing errors other these here:
        # except IntegrityErrorSQLite as e:
        #     print(f'Unexpected: Data already stored before!', uuid)
        # except IntegrityErrorMySQL as e:
        #     print(f'Unexpected: Data already stored before!', uuid)
        # else:
        print(f": Data inserted", uuid)

    def _fetch_(self, data: Data, lock=False) -> Optional[Picklable]:
        # Fetch data info.
        did = data if isinstance(data, str) else data.id
        self.query(f"select * from data where id=?", [did])
        result = self.get_one()
        return self._fetch_core_(data, result, lock)

    def _fetch_core_(self, data, result, lock) -> Optional[Picklable]:
        # Fetch data info.
        did = data if isinstance(data, str) else data.id

        # Fetch data info.
        if result is None:
            if lock:
                self.lock(did)
            return None
        # values_by_id = {row['id']: row['value'] for row in rall}

        if result["names"] == "":
            print("W: Previously locked by other process.", did)
            raise LockedEntryException(did)

        names = result["names"].split(",")
        mids = result["matrices"].split(",")
        hids = result["history"].split(",")
        inner = result["inn"]

        name_by_mid = dict(zip(mids, names))

        # Fetch matrices (lazily, if storage_info is provided).
        new_mids = [mid for mid in mids if isinstance(data, str) or mid not in data.ids_lst]
        matrices = {} if isinstance(data, str) else data.matrices
        if self.storage_info is None:
            matrices_by_mid = self.fetch_dumps(new_mids)
            for mid in new_mids:
                matrices[name_by_mid[mid]] = matrices_by_mid[mid]
        else:
            for mid in new_mids:
                matrices[name_by_mid[mid]] = UUID(mid)

        # Fetch history.
        serialized_hist = self.fetch_dumps(hids)
        # TODO: deserializar antes de por no histórico

        # TODO: failure and timeout should be stored/fetched!
        # TODO: would it be worth to update uuid/uuids here, instead of recalculating it from the start at Data.init?
        uuids = {} if isinstance(data, str) else data.uuids
        uuids.update(dict(zip(names, map(UUID, mids))))
        return Picklable(uuid=UUID(did), uuids=uuids, history=serialized_hist, storage_info=self.storage_info, inner=inner, **matrices)

    def fetch_matrix(self, id):
        # TODO: quando faz select em algo que não existe, fica esperando
        #  infinitamente algum lock liberar
        self.query(f"select value from dump where id=?", [id])
        rone = self.get_one()
        if rone is None:
            raise Exception("Matrix not found!", id)
        return unpack(rone["value"])

    def fetch_dumps(self, duids, aslist=False):
        if len(duids) == 0:
            return [] if aslist else dict()
        qmarks = ",".join(["?"] * len(duids))
        sql = f"select id,value from dump where id in ({qmarks}) order by n"
        self.query(sql, duids)
        rall = self.get_all()
        id_value = {row["id"]: unpack(row["value"]) for row in rall}
        if aslist:
            return [id_value[duid] for duid in duids]
        else:
            return {duid: id_value[duid] for duid in duids}

    def _unlock_(self, data):
        # locked = rone and rone['t'] == '0000-00-00 00:00:00'
        # if not locked:
        #     raise UnlockedEntryException('Cannot unlock if it is not locked!')
        self.query(f"delete from data where id=?", [data.uuid.id])

    @staticmethod
    @abstractmethod
    def _on_conflict(fields=None):
        pass

    @staticmethod
    @abstractmethod
    def _keylimit():
        pass

    @staticmethod
    @abstractmethod
    def _now_function():
        pass

    @staticmethod
    @abstractmethod
    def _auto_incr():
        pass

    def _setup(self):
        print("creating tables...")

        # Data - Up to 102 matrices and 3277 transformations per row
        # ========================================================
        self.query(
            f"""
            create table if not exists data (
                n integer NOT NULL primary key {self._auto_incr()},
                id char(23) NOT NULL UNIQUE,
                inn char(23),
                names VARCHAR(255) NOT NULL,
                matrices VARCHAR(2048), 
                history TEXT,
                t TIMESTAMP 
            )"""
        )
        # FOREIGN KEY (inn) REFERENCES data(id)  <- REMINDER problems with locking an outer data

        self.query(
            f"""
            create table if not exists dump (
                n integer NOT NULL primary key {self._auto_incr()},
                id char(23) NOT NULL UNIQUE,
                value LONGBLOB NOT NULL
            )"""
        )

        # Table to speed up look up for already synced Data objects.
        self.query(
            f"""
            create table if not exists sync (
                n integer NOT NULL primary key {self._auto_incr()},
                storage char(23) NOT NULL,
                last char(23) NOT NULL,
                t TIMESTAMP, 
                unique (storage, last)
            )"""
        )

    def get_one(self) -> Optional[dict]:
        """
        Get a single result after a query, no more than that.
        :return:
        """
        row = self.cursor.fetchone()
        if row is None:
            return None
        row2 = self.cursor.fetchone()
        if row2 is not None:
            print("first row", row)
            while row2:
                print("extra row", row2)
                row2 = self.cursor.fetchone()
            raise Exception("  Excess of rows")
        return dict(row)

    def get_all(self) -> list:
        """
        Get a list of results after a query.
        :return:
        """
        rows = self.cursor.fetchall()
        return [dict(row) for row in rows]

    def store_dump(self, lst):
        """Store the given pair uuid-dump of a matrix/vector."""
        from tatu.sql.sqlite import SQLite
        lst = [(duid, memoryview(dump) if isinstance(self, SQLite) else dump) for duid, dump in lst.items()]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            self.insert_many(lst, "dump")

    def lock(self, data):
        did = data if isinstance(data, str) else data.id
        if self.debug:
            print("Locking...", did)

        sql = f"insert into data values (null,?,?,?,?,?,'0000-00-00 00:00:00')"
        args = [did, "", "", "", ""]
        from sqlite3 import IntegrityError as IntegrityErrorSQLite
        from pymysql import IntegrityError as IntegrityErrorMySQL

        try:  # REMINDER that exception would be on the way of mysql lock() due to inner 'inn' field FK constraint
            self.query(sql, args)
        except IntegrityErrorSQLite as e:
            print(f"Unexpected lock! " f"Giving up my turn on {did} ppy/se", e)
            exit()
        except IntegrityErrorMySQL as e:
            print(f"Unexpected lock! " f"Giving up my turn on {did} ppy/se", e)
            exit()
        else:
            print(f"Now locked for {did}")

    def query(self, sql, args=None):
        if self.read_only and not sql.startswith("select "):
            print("========================================\n", "Attempt to write onto read-only storage!", sql)
            self.cursor.execute("select 1")
            return
        if args is None:
            args = []
        from tatu.sql.mysql import MySQL

        msg = self._interpolate(sql, args)
        if self.debug:
            print(msg)
        if isinstance(self, MySQL):
            sql = sql.replace("?", "%s")
            sql = sql.replace("insert or ignore", "insert ignore")
            # self.connection.ping(reconnect=True)

        try:
            self.cursor.execute(sql, args)
        except Exception as ex:
            # From a StackOverflow answer...
            import sys
            import traceback

            msg = self.info + "\n" + msg
            # Gather the information from the original exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            # Format the original exception for a nice printout:
            traceback_string = "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
            # Re-raise a new exception of the same class as the original one
            raise type(ex)("%s\norig. trac.:\n%s\n" % (msg, traceback_string))

    def __del__(self):
        try:
            self.connection.close()
        except Exception as e:
            # print('Couldn\'t close database, but that\'s ok...', e)
            pass

    @staticmethod
    def _interpolate(sql, lst0):
        lst = [str(w)[:100] for w in lst0]
        zipped = zip(sql.replace("?", '"?"').split("?"), map(str, lst + [""]))
        return "".join(list(sum(zipped, ()))).replace('"None"', "NULL")

    def insert_many(self, list_of_tuples, table):
        sql = f"insert or ignore INTO {table} VALUES(NULL, ?, ?)"
        from tatu.sql.mysql import MySQL
        if isinstance(self, MySQL):
            sql = sql.replace("?", "%s")
            sql = sql.replace("insert or ignore", "insert ignore")
        self.cursor.executemany(sql, list_of_tuples)

    def _fetch_at_(self, position):
        self.query(f"select * from data ORDER BY n LIMIT {position},1")
        result = self.get_one()
        if result is None:
            return None
        return self._fetch_core_(result["id"], result, lock=False)

    def _size_(self):
        self.query("select count(1) as n from data")
        rone = self.get_one()
        if rone is None:
            return 0
        return rone["n"]

    def _last_synced_(self, storage, only_id=True):
        if not only_id:
            raise NotImplementedError
        self.query("select last from sync where storage=? order by n desc limit 1", [storage])
        rone = self.get_one()
        if rone is None:
            return None
        return rone["last"]

    def _mark_synced_(self, synced, storage):
        qmarks = ",".join(["?"] * len(synced))
        self.query(f"delete from sync where storage=? and last in ({qmarks})", [storage] + synced)
        self.insert_many([[storage, did, {self._now_function()}] for did in synced], "sync")

        # FOREIGN KEY (attr) REFERENCES attr(aid)
        # self.query(f'CREATE INDEX nam0 ON dataset (des{self._keylimit()})')
        # self.query(f'CREATE INDEX nam1 ON dataset (attr)')
        # insl timestamp NOT NULL     # unique(dataset, hist),
        # spent FLOAT,        # fail TINYINT,      # start TIMESTAMP NOT NULL,
        # update data set {','.join([f'{k}=?' for k in to_update.keys()])}
        # insd=insd, upd={self._now_function()} where did=?
        #     x = coalesce(values(x), x),
        #     from res left join data on dout = did
        #     left join dataset on dataset = dsid where
