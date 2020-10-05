import json
import warnings
from abc import abstractmethod, ABC
from typing import Optional

from aiuna.compression import unpack, pack
from aiuna.content.data import Data, Picklable
from cruipto.uuid import UUID
from tatu.persistence import Persistence, DuplicateEntryException, LockedEntryException, MissingEntryException
from transf.customjsonencoder import CustomJSONEncoder


class SQL(Persistence, ABC):
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
            already_exists = not locked
        else:
            locked = False
            already_exists = False

        # Check if dumps of matrices/vectors already exist.
        qmarks = ",".join(["?"] * len(data.uuids))
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
        dic = {}
        for transf in data.history:
            dump = pack(json.dumps(transf["desc"], sort_keys=True, ensure_ascii=False, cls=CustomJSONEncoder))
            dic[transf["id"]] = dump
        self.store_dump(dic)

        # Insert Data object.
        if not already_exists:
            if check_dup and not locked:
                # check_dup==True means allow SQL to enforce UNIQUE constraint,
                # because data could have been inserted in the mean time
                sql = f"insert into data values (NULL, ?, ?, ?, ?, {self._now_function()})"
            else:
                sql = f"insert or ignore into data values (NULL, ?, ?, ?, ?, {self._now_function()})"
            data_args = [uuid.id, data.matrix_names_str, data.ids_str, data.history_str]
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

    def _fetch_picklable_(self, data: Data, lock=False) -> Optional[Picklable]:
        # Fetch data info.
        uuid = data.uuid
        self.query(f"select * from data where id=?", [uuid.id])
        result = self.get_one()

        if result is None:
            if lock:
                self.lock(data)
            return None
        # values_by_id = {row['id']: row['value'] for row in rall}

        if result["names"] == "":
            print("W: Previously locked by other process.", data)
            raise LockedEntryException(data)

        names = result["names"].split(",")
        mids = result["matrices"].split(",")
        hids = result["history"].split(",")

        name_by_mid = dict(zip(mids, names))

        # Fetch matrices (lazily, if storage_info is provided).
        new_mids = [mid for mid in mids if mid not in data.ids_lst]
        matrices = data.matrices
        if self.storage_info is None:
            matrices_by_mid = self.fetch_dumps(new_mids)
            for mid in new_mids:
                matrices[name_by_mid[mid]] = matrices_by_mid[mid]
        else:
            for mid in new_mids:
                matrices[name_by_mid[mid]] = UUID(mid)

        # Fetch history.
        serialized_hist = [
            {"id": id_, "desc": json.loads(strtransf)} for id_, strtransf in self.fetch_dumps(hids).items()
        ]
        # TODO: deserializar antes de por no histórico

        # TODO: failure and timeout should be stored/fetched!
        # TODO: would it be worth to update uuid/uuids here, instead of recalculating it from the start at Data.init?
        uuids = data.uuids
        uuids.update(dict(zip(names, map(UUID, mids))))
        return Picklable(uuid=uuid, uuids=uuids, history=serialized_hist,
                         failure=None, storage_info=self.storage_info, **matrices)

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

    def list_by_name(self, substring, only_historyless=True):
        # TODO: Pra fins de fetchbylist, pode ser usado o próprio Data se a
        #       implementação passar a ser lazy. (ORM-like behavior)
        pass

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
                id char(18) NOT NULL UNIQUE,
                names VARCHAR(255) NOT NULL,
                matrices VARCHAR(2048), 
                history VARCHAR(65535),
                t TIMESTAMP 
            )"""
        )
        self.query(
            f"""
            create table if not exists dump (
                n integer NOT NULL primary key {self._auto_incr()},
                id char(18) NOT NULL UNIQUE,
                value LONGBLOB NOT NULL
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
        did = data.uuid.id
        if self.debug:
            print("Locking...", did)

        sql = f"insert into data values (null,?,?,?,?,'0000-00-00 00:00:00')"
        args = [did, "", "", ""]
        from sqlite3 import IntegrityError as IntegrityErrorSQLite
        from pymysql import IntegrityError as IntegrityErrorMySQL

        try:
            self.query(sql, args)
        except IntegrityErrorSQLite as e:
            print(f"Unexpected lock! " f"Giving up my turn on {did} ppy/se", e)
        except IntegrityErrorMySQL as e:
            print(f"Unexpected lock! " f"Giving up my turn on {did} ppy/se", e)
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
