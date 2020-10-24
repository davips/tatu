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
from abc import ABC

from aiuna.content.data import Data
from cruipto.uuid import UUID
from tatu.sql.abs.mixin.setup import withSetup
from tatu.sql.result import Result
from tatu.storage import Storage
from transf.step import Step


class SQLReadOnly(Storage, withSetup, ABC):
    cursor = None
    read_only = True

    def _hasdata_(self, id, include_empty=True):  # TODO checar lock null?
        if include_empty:
            sql = f"select 1 from data where id=?"
        else:
            # REMINDER Inner join ensures a Data row with fields.
            nonempty = f"select 1 from data d INNER JOIN field f ON d.id=f.data where d.id=? limit 1"
            withstream = "select 1 from data where id=? and stream=true"
            sql = f"{withstream} UNION {nonempty}"
        return self.read(sql, [id, id]).fetchone() is not None

    def _getdata_(self, id, include_empty=True):
        cols = "step,inn,stream,parent,locked,name as field_name,content as field_id"
        sql = f"select {cols} from data d {'left' if include_empty else 'inner'} join field f on d.id=f.data where d.id=?"
        r = self.read(sql, [id]).fetchall()
        if not r:
            return
        uuids = {}
        for row in r:
            if row["locked"]:
                raise Exception("Cannot get a locked Data object.")
            if row["field_name"]:
                uuids[row["field_name"]] = row["field_id"]
        return {"uuids": uuids, "step": row["step"], "parent": row["parent"], "inner": row["inn"], "stream": row["stream"]}

    def _getstep_(self, id):
        row = self.read("select name,path,params from step s inner join config c on s.id=c.step where s.id=?", [id]).fetchone()
        if row is None:
            return
        desc = {"name": row["name"], "path": row["path"], "config": row["params"]}
        return desc

    def _getfields_(self, id, names):
        sql = f"select value from field inner join content on content=id where data=? and name in ({('?,' * len(names))[:-1]})"
        return [row["value"] for row in self.read(sql, [id] + names).fetchall()]  # TODO retornar iterator; pra isso, precisa de uma conexão fora da thread, e gets são bloqueantes anyway

    def _hasstep_(self, id):
        return self.read(f"select 1 from step where id=?", [id]).fetchone() is not None

    def _hascontent_(self, id):
        return self.read(f"select 1 from content where id=?", [id]).fetchone() is not None

    def _hasfield_(self, id):
        return self.read(f"select 1 from content where id=?", [id]).fetchone() is not None

    def _missing_(self, ids):
        lst = [row["id"] for row in self.read(f"select id from content where id in ({('?,' * len(ids))[:-1]})", ids).fetchall()]
        return [id for id in ids if id not in lst]

    # def _fetch_(self, data: Data, lock=False) -> Optional[Picklable]:
    #     # Fetch data info.
    #     did = data if isinstance(data, str) else data.id
    #     self.query(f"select * from data where id=?", [did])
    #     result = self.get_one()
    #
    #     # Fetch data info.
    #     if result is None:
    #         if lock:
    #             self.lock(data)
    #         return None
    #
    #     if result["names"] == "":
    #         print("W: Previously locked by other process.", did)
    #         raise LockedEntryException(did)
    #
    #     names = result["names"].split(",")
    #     mids = result["fields"].split(",")
    #     inner = result["inn"]
    #     name_by_mid = dict(zip(mids, names))
    #
    #     # Fetch matrices (lazily, if storage_info is provided).
    #     new_mids = [mid for mid in mids if isinstance(data, str) or mid not in data.ids_lst]
    #     matrices = {} if isinstance(data, str) else data.matrices
    #     if self.storage_info is None:
    #         matrices_by_mid = self.fetch_dumps(new_mids)
    #         for mid in new_mids:
    #             matrices[name_by_mid[mid]] = matrices_by_mid[mid]
    #     else:
    #         for mid in new_mids:
    #             matrices[name_by_mid[mid]] = UUID(mid)
    #     # Fetch history.
    #     serialized_hist = self.fetch_dumps(hids)
    #     # REMINDER: não deserializar antes de por no histórico, pois o data.picklable manda serializado; senão, não fica picklable
    #     #   a única forma seria implementar a travessia recusrsiva de subcomponentes para deixar como dicts (picklable) e depois jsonizar apenas aqui.
    #     #   é preciso ver se há alguma vantagem nisso; talvez desempenho e acessibilidade de chaves dos dicts; por outro lado,
    #     #   da forma atual o json faz tudo junto num travessia única  DECIDI fazer a travessia e só jsonizar/dejasonizar dentro de storage
    #
    #     # TODO: failure and timeout should be stored/fetched! ver como fica na versao lazy, tipo: é só guardar _matrices? ou algo mais?
    #     uuids = {} if isinstance(data, str) else data.uuids
    #     uuids.update(dict(zip(names, map(UUID, mids))))
    #     return Picklable(uuid=UUID(did), uuids=uuids, history=serialized_hist, storage_info=self.storage_info, inner=inner, **matrices)
    #
    # def _fetch_children_(self, data: Data) -> List[AbsData]:
    #     self.query(f"select id from data where parent=?", [data.id])
    #     return [self._build_fetched("exnihilo", result) for result in self.get_all()]
    #
    # def fetch_field(self, id):
    #     # TODO: quando faz select em algo que não existe, fica esperando
    #     #  infinitamente algum lock liberar
    #     self.query(f"select value from content where id=?", [id])
    #     rone = self.get_one()
    #     if rone is None:
    #         raise Exception("Matrix not found!", id)
    #     return unpack(rone["value"])
    #
    # def fetch_dumps(self, duids, aslist=False):
    #     if len(duids) == 0:
    #         return [] if aslist else dict()
    #     qmarks = ",".join(["?"] * len(duids))
    #     sql = f"select id,value from content where id in ({qmarks}) order by n"
    #     self.query(sql, duids)
    #     rall = self.get_all()
    #     id_value = {row["id"]: unpack(row["value"]) for row in rall}
    #     if aslist:
    #         return [id_value[duid] for duid in duids]
    #     else:
    #         return {duid: id_value[duid] for duid in duids}
    #
    # @staticmethod
    # @abstractmethod
    # def _on_conflict(fields=None):
    #     pass
    #
    # @staticmethod
    # @abstractmethod
    # def _keylimit():
    #     pass
    #
    # @staticmethod
    # @abstractmethod
    # def _now_function():
    #     pass
    #
    # @staticmethod
    # @abstractmethod
    # def _auto_incr():
    #     pass
    #
    # def get_one(self) -> Optional[dict]:
    #     """
    #     Get a single result after a query, no more than that.
    #     :return:
    #     """
    #     row = self.cursor.fetchone()
    #     if row is None:
    #         return None
    #     row2 = self.cursor.fetchone()
    #     if row2 is not None:
    #         print("first row", row)
    #         while row2:
    #             print("extra row", row2)
    #             row2 = self.cursor.fetchone()
    #         raise Exception("  Excess of rows")
    #     return dict(row)
    #
    # def get_all(self) -> list:
    #     """
    #     Get a list of results after a query.
    #     :return:
    #     """
    #     rows = self.cursor.fetchall()
    #     return [dict(row) for row in rows]
    #
    # # noinspection PyDefaultArgument
    # def query2(self, sql, args=[], cursor=None):
    #     if cursor is None:
    #         cursor = self.cursor
    #     if self.read_only and not sql.startswith("select "):
    #         print("========================================\n", "Attempt to write onto read-only storage!", sql)
    #         cursor.execute("select 1")
    #         return
    #     if self.debug:
    #         msg = self._interpolate(sql, args)
    #         print(self.name + ":\t>>>>> " + msg)
    #     sql = sql.replace("?", self.placeholder)
    #     sql = sql.replace("insert or ignore", self.insert_ignore)
    #     # self.connection.ping(reconnect=True)
    #     cursor.execute(sql, args)
    #
    # @abstractmethod
    # def placeholder(self):
    #     pass
    #
    # def __del__(self):
    #     try:
    #         self.connection.close()
    #     except Exception as e:
    #         print('Couldn\'t close database, but that\'s ok...', e)
    #         pass
    #
    #
    #
    # #     #     # From a StackOverflow answer...
    # #     #     import sys
    # #     #     import traceback
    # #     #
    # #     #     msg = "STORAGE DBG:" + self.info + "\n" + msg
    # #     #     # Gather the information from the original exception:
    # #     #     exc_type, exc_value, exc_traceback = sys.exc_info()
    # #     #     # Format the original exception for a nice printout:
    # #     #     traceback_string = "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
    # #     #     # Re-raise a new exception of the same class as the original one
    # #     #     raise type(ex)("%s\norig. trac.:\n%s\n" % (msg, traceback_string))

    def read(self, sql, args=[], cursor=None):
        if not (sql.startswith("select ") or sql.startswith("(select ")):
            raise Exception("========================================\n", "Attempt to write onto read-only storage!", sql)
        self.query2(sql, args, cursor)
        return Result(self.connection, cursor or self.cursor)

    # noinspection PyDefaultArgument
    def query2(self, sql, args=[], cursor=None):
        # TODO with / catch / finalize connection?
        cursor = cursor or self.cursor
        args = [int(c) if isinstance(c, bool) else c for c in args]
        sql = sql.replace("insert or ignore", self._insert_ignore)
        if self.debug:
            msg = self._interpolate(sql, args)
            print(self.name + ":\t>>>>> " + msg)
        sql = sql.replace("?", self._placeholder)
        # self.connection.ping(reconnect=True)
        return cursor.execute(sql, args)

    @staticmethod
    def _interpolate(sql, lst0):
        lst = [str(w)[:100] for w in lst0]
        zipped = zip(sql.replace("?", '"?"').split("?"), map(str, lst + [""]))
        return "".join(list(sum(zipped, ()))).replace('"None"', "NULL")

    def commit(self):
        self.connection.commit()
