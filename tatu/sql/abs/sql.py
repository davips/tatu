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
import warnings
from abc import abstractmethod, ABC

from aiuna.compression import unpack
from cruipto.uuid import UUID
from tatu.sql.abs.sqlreadonly import SQLReadOnly
from transf.customjson import CustomJSONEncoder


class SQL(SQLReadOnly, ABC):
    cursor = None
    read_only = False

    def _delete_data(self, id):
        return 1 == self.write(f"delete from data where id=?", [id]).commit()

    def _lock_(self, id):
        # Placeholder values: identitystep/'3oawXk8ZTPtS5DBsghkFNnz' and rootparent/'00000000000000000000001'
        sql = f"insert into data values (null,'?','3oawXk8ZTPtS5DBsghkFNnz',null,'00000000000000000000001',true,{self._now_function()})", [id]
        return 1 == self.write(sql).commit()

    # def _putdata_(self, **row):
    #     qmarks = ",".join(["?"] * len(row))
    #     self.query(f"INSERT INTO data ({','.join(row.keys())}, t) VALUES ({qmarks}, {self._now_function()})", list(row.values()))
    #
    # def _putcontent_(self, id, value):
    #     self.query(f"INSERT INTO content VALUES (NULL, ?, ?)", [id, value])
    #
    # def _putstep_(self, id, name, path, config, dump=None):
    #     self.query(f"INSERT INTO step VALUES (NULL, ?, ?, ?, ?, ?)", [id, name, path, config, dump])
    #
    # def _store_(self, data: Data, check_dup=True):
    #     uuid = data.uuid
    #     parentid = data.parent_uuid.id
    #     self.query2(f"select t from data where id=?", [uuid.id])
    #     rone = self.get_one()
    #
    #     if rone:
    #         locked = rone["t"] == "0000-00-00 00:00:00"
    #         if check_dup:
    #             raise DuplicateEntryException("Already exists:", uuid.id)
    #     else:
    #         locked = False
    #
    #     # Check if dumps of matrices/vectors already exist.
    #     qmarks = ",".join(["?"] * len(data.uuids))
    #     self.query(f"select id from content where id in ({qmarks})", data.ids_lst)
    #     rall = self.get_all()
    #     stored_hashes = [row["id"] for row in rall]
    #
    #     # Insert only matrices that are missing in storage
    #     dic = {}
    #     for name, u in data.uuids.items():
    #         if u.id not in stored_hashes:
    #             dic[u.id] = data.field_dump(name)
    #     self.store_dump(dic)
    #
    #     # Insert history.
    #     dic = {dic["id"]: json.dumps(dic["desc"], cls=CustomJSONEncoder, sort_keys=True, ensure_ascii=False) for dic in data.history.asdicts}
    #     self.store_dump(dic)
    #
    #     # Insert Data object.
    #     if not locked:
    #         # ensure UNIQUE constraint (just in case something changed in the meantime since select*)
    #         if check_dup:
    #             sql = f"insert into data values (NULL, ?, ?, ?, ?, ?, ?, {self._now_function()})"
    #         else:
    #             sql = f"replace into data values (NULL, ?, ?, ?, ?, ?, ?, {self._now_function()})"
    #         data_args = [uuid.id, data.inner and data.inner.id, parentid, data.matrix_names_str, data.ids_str, data.history.last.id]
    #     else:
    #         sql = f"update data set inn=?, parent=?, names=?, fields=?, history=?, t={self._now_function()} where id=?"
    #         data_args = [data.inner and data.inner.id, data.parent_uuid.id, data.matrix_names_str, data.ids_str, data.history.last.id, uuid.id]
    #     # from sqlite3 import IntegrityError as IntegrityErrorSQLite
    #     # from pymysql import IntegrityError as IntegrityErrorMySQL
    #     # try:
    #     self.query(sql, data_args)
    #     # unfortunately,
    #     # it seems that FKs generate the same exception as reinsertion.
    #     # so, missing FKs might not be detected here.
    #     # not a worrying issue whatsoever.
    #     # TODO: it seems to be capturing errors other these here:
    #     # except IntegrityErrorSQLite as e:
    #     #     print(f'Unexpected: Data already stored before!', uuid)
    #     # except IntegrityErrorMySQL as e:
    #     #     print(f'Unexpected: Data already stored before!', uuid)
    #     # else:
    #     print(f": Data inserted", uuid)
    #
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
    # def _unlock_(self, data):
    #     # locked = rone and rone['t'] == '0000-00-00 00:00:00'
    #     # if not locked:
    #     #     raise UnlockedEntryException('Cannot unlock if it is not locked!')
    #     self.query(f"delete from data where id=?", [data.uuid.id])
    #
    # def store_dump(self, lst):
    #     """Store the given pair uuid-dump of a matrix/vector."""
    #     from tatu.sql.sqlite import SQLite
    #     lst = [(duid, memoryview(dump) if isinstance(self, SQLite) else dump) for duid, dump in lst.items()]
    #     with warnings.catch_warnings():
    #         warnings.simplefilter("ignore")
    #         self.insert_many(lst, "content")
    #
    # def lock(self, data):
    #     # REMINDER relaxing constraints
    #     # if isinstance(data, str):
    #     #     raise Exception("Cannot lock only by data UUID, a Data object is required because the data parent UUID is needed by DBMS constraints.")
    #     did, pid = data.id, data.parent_uuid.id
    #     if self.debug:
    #         print("Locking...", did)
    #
    #     sql = f"insert into data values (null,?,?,?,?,?,?,'0000-00-00 00:00:00')"
    #     args = [did, "", pid, "", "", ""]
    #     from sqlite3 import IntegrityError as IntegrityErrorSQLite
    #     from pymysql import IntegrityError as IntegrityErrorMySQL
    #
    #     try:  # REMINDER that exception would be on the way of mysql lock() due to inner 'inn' field FK constraint
    #         self.query(sql, args)
    #     except IntegrityErrorSQLite as e:
    #         print(f"Unexpected lock! " f"Giving up my turn on {did} ppy/se", e)
    #         exit()
    #     except IntegrityErrorMySQL as e:
    #         print(f"Unexpected lock! " f"Giving up my turn on {did} ppy/se", e)
    #         exit()
    #     else:
    #         print(f"Now locked for {did}")
    #
    # def __del__(self):
    #     try:
    #         self.connection.close()
    #     except Exception as e:
    #         # print('Couldn\'t close database, but that\'s ok...', e)
    #         pass
    #
    # @staticmethod
    # def _interpolate(sql, lst0):
    #     lst = [str(w)[:100] for w in lst0]
    #     zipped = zip(sql.replace("?", '"?"').split("?"), map(str, lst + [""]))
    #     return "".join(list(sum(zipped, ()))).replace('"None"', "NULL")
    #
    # def insert_many(self, list_of_tuples, table):
    #     sql = f"insert or ignore INTO {table} VALUES(NULL, ?, ?)"
    #     from tatu.sql.mysql import MySQL
    #     if isinstance(self, MySQL):
    #         sql = sql.replace("?", "%s")
    #         sql = sql.replace("insert or ignore", "insert ignore")
    #     self.cursor.executemany(sql, list_of_tuples)
    #
    # def _update_remote_(self, storage):
    #     stid = storage.id
    #
    #     # List all Data uuids since last synced one, but insert only the ones not already there.
    #     lastid = f"select last from sync where storage='{stid}'"
    #     lastn = f"IFNULL((select n from data where id in ({lastid})), -1)"
    #     self.query(f"select id from data where n > {lastn} order by n")
    #     cursor2 = self.connection.cursor()
    #     for row0 in self.cursor:
    #         did = row0["id"]
    #
    #         # em que ordem inserir isso?  col n é importante p/ pegar datas apos o ultimo. do data da p/ saber o resto
    #         # a ordem original de datas garante integridade na inserção remota
    #         # has stream
    #         # has step
    #         # has config
    #         # has content
    #
    #         if not storage.hasdata(did):
    #             # Get rest of data info.
    #             self.query(f"select * from data where id=?", [did], cursor2)
    #             row = dict(cursor2.fetchone())
    #
    #             # Send fields
    #             for fieldid in row["fields"].split(",") + row["history"].split(","):
    #                 if not storage.hascontent(fieldid):
    #                     self.query("select id, value from content where id=?", [fieldid], cursor2)
    #                     storage.putcontent(**cursor2.fetchone())
    #
    #             # Send data.
    #             del row["n"]
    #             del row["t"]
    #             storage.putdata(**row)
    #
    #             # Update table sync as soon as possible, to behave well in case of interruption of a long list of inserts.
    #             # TODO a single query to insert / update
    #             self.query(f"delete from sync where storage=?", [stid], cursor2)
    #             self.query(f"insert into sync values (NULL, ?, ?, {self._now_function()})", [stid, did], cursor2)

    def write(self, sql, args=[], cursor=None):
        self._query(sql, args, cursor)
        return self.connection
