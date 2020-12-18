#  Copyright (c) 2020. Davi Pereira dos Santos
#  This file is part of the tatu project.
#  Please respect the license - more about this in the section (*) below.
#
#  tatu is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  tatu is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with tatu.  If not, see <http://www.gnu.org/licenses/>.
#
#  (*) Removing authorship by any means, e.g. by distribution of derived
#  works or verbatim, obfuscated, compiled or rewritten versions of any
#  part of this work is a crime and is unethical regarding the effort and
#  time spent here.
#  Relevant employers or funding agencies will be notified accordingly.
from abc import ABC

from tatu.abs.mixin.setup import withSetup
from tatu.abs.storage import LockedEntryException
from tatu.storageinterface import StorageInterface


class SQLReadOnly(StorageInterface, withSetup, ABC):
    read_only = True

    def _hasdata_(self, id, include_empty):  # TODO checar lock null?
        if include_empty:
            sql, args = f"select 1 from data where id=?", [id]
        else:
            # REMINDER Inner join ensures a Data row with fields.
            nonempty = f"select 1 from data d INNER JOIN field f ON d.id=f.data where d.id=? limit 1"
            withstream = "select 1 from data where id=? and stream=1"
            sql, args = f"{withstream} UNION {nonempty}", [id, id]
        with self.cursor() as c:
            self.run(c, sql, args)
            return c.fetchone() is not None

    def _hasstream_(self, data):
        sql = f"select 1 from stream where id=? and pos=0"
        with self.cursor() as c:
            self.run(c, sql, [data])
            return c.fetchone() is not None

    def _getdata_(self, id, include_empty):
        cols = "step,inn,stream,parent,locked,name as field_name,content as field_id"
        sql = f"select {cols} from data d {'left' if include_empty else 'inner'} join field f on d.id=f.data where d.id=?"
        with self.cursor() as c:
            self.run(c, sql, [id])
            r = c.fetchall()
        if not r:
            return
        uuids = {}
        for row in r:
            if row["locked"]:
                raise LockedEntryException("Cannot get a locked Data object.")
            if row["field_name"]:
                uuids[row["field_name"]] = row["field_id"]
        return {"uuids": uuids, "step": row["step"], "parent": row["parent"], "inner": row["inn"],
                "stream": row["stream"]}

    def _getstream_(self, data):
        sql = "select data,pos,chunk from stream where data=?"
        with self.cursor() as c:
            self.run(c, sql, [data])
            # TODO: can this be passed around as an iterator? is it worth or will just increase the amount of latencies?
            row = c.fetchall()
        if len(row) == 0:
            return
        return row

    def _getstep_(self, id):
        sql = "select name,path,params from step s inner join config c on s.config=c.id where s.id=?"
        with self.cursor() as c:
            self.run(c, sql, [id])
            row = c.fetchone()
        if row is None:
            return
        desc = {"name": row["name"], "path": row["path"], "config": row["params"]}
        return desc

    def _getfields_(self, id):
        sql = f"select content as cid,value from field inner join content on content=id where data=?"
        with self.cursor() as c:
            self.run(c, sql, id)
            rows = c.fetchall()
        return {row["cid"]: row["value"] for row in
                rows}  # TODO retornar iterator; pra isso, precisa de uma conexão fora da thread, e gets são bloqueantes anyway

    def _getcontent_(self, id):
        sql = f"select value from content where id=?"
        with self.cursor() as c:
            self.run(c, sql, [id])
            row = c.fetchone()
        return row and row["value"]

    def _hasstep_(self, id):
        sql = f"select 1 from step where id=?"
        with self.cursor() as c:
            self.run(c, sql, [id])
            return c.fetchone() is not None

    def _hascontent_(self, ids):
        sql = f"select id from content where id in ({('?,' * len(ids))[:-1]})"  # REMINDER: -1 deletes comma
        with self.cursor() as c:
            self.run(c, sql, ids)
            return [r["id"] for r in c.fetchall()]

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

    # noinspection PyDefaultArgument
    def prepare(self, sql, args=[]):
        # TODO with / catch / finalize connection?
        args = [int(c) if isinstance(c, bool) else c for c in args]
        sql = sql.replace("insert or ignore", self._insert_ignore)
        if self.debug:
            msg = self._interpolate(sql, args)
            print(self.name + ":\t>>>>> " + msg)
        # self.connection.ping(reconnect=True)
        return sql.replace("?", self._placeholder), args

    @staticmethod
    def _interpolate(sql, lst0):
        lst = [str(w)[:100] for w in lst0]
        zipped = zip(sql.replace("?", '"?"').split("?"), map(str, lst + [""]))
        return "".join(list(sum(zipped, ()))).replace('"None"', "NULL")

    def commit(self):
        #print("LOGGING:::  commit")
        self.connection.commit()
