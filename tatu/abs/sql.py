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
from sqlite3 import IntegrityError as sqliteIntegError

from pymysql import IntegrityError as myIntegError

from akangatu.transf.noop import NoOp
from garoupa.uuid import UUID
from tatu.abs.sqlreadonly import SQLReadOnly
from tatu.abs.storage import LockedEntryException, DuplicateEntryException


class SQL(SQLReadOnly, ABC):
    read_only = False

    def _close_(self):
        from tatu.sql.sqlite import SQLite
        if self.connection and not isinstance(self, SQLite) and self.connection.open:
            self.connection.close()

    def _deldata_(self, id):
        with self.cursor() as c:
            self.run(c, f"delete from data where id=?", [id])
            self.commit()
            r = c.rowcount
        return r == 1

    def _handle_integrity_error(self, id, sql, args):
        try:
            with self.cursor() as c:
                self.run(c, sql, args)
                self.commit()
                r = c.rowcount
            return r == 1
        except (myIntegError, sqliteIntegError) as e:
            with self.cursor() as c2:
                self.run(c2, "select 1 as r from data where id=? and locked=1", [id])
                r2 = c2.fetchone()
                if r2 and "r" in r2:
                    raise LockedEntryException(id, str(e))
                else:
                    raise DuplicateEntryException(id, str(e))

    def _lock_(self, id, ignoredup=False):
        # Placeholder values: step=identity and parent=own-id
        try:
            sql = f"insert {'or ignore' if ignoredup else ''} into data values (null,?,'{NoOp().id}',null,0,?,1)"
            return self._handle_integrity_error(id, sql, [id, id])
        except:
            sql = f"update data set locked=1 where id=?"
            return self._handle_integrity_error(id, sql, [id])

    def _unlock_(self, id):
        with self.cursor() as c:
            self.run(c, self._fkcheck(False))
            try:
                self.run(c, "delete from data where id=? and locked=1", [id])
                r = c.rowcount
            finally:
                self.run(c, self._fkcheck(True))
                self.commit()
            return r == 1

    def _putdata_(self, id, step, inn, stream, parent, locked, ignoredup=False):
        # if id == "0iQlsaDROpjL7YRZBcFr0Py":
        #     raise Exception("--------------------")
        sql = f"insert {'or ignore' if ignoredup else ''} INTO data values (null,?,?,?,?,?,?)"
        return self._handle_integrity_error(id, sql, [id, step, inn, stream, parent, locked])

    def _putstream_(self, rows, ignoredup=False):
        # try:
        with self.cursor() as c:
            self.write_many(c, rows, "stream", ignoredup)
            self.commit()
            r = c.rowcount
        return r

    # except Exception as e:
    #     import traceback
    #     traceback.print_exc()
    #     print(e)

    def _putfields_(self, rows, ignoredup=False):
        with self.cursor() as c:
            self.write_many(c, rows, "field", ignoredup)
            self.commit()
            return c.rowcount

    def _putcontent_(self, id, value, ignoredup=False):
        with self.cursor() as c:
            self.run(c, f"insert {'or ignore' if ignoredup else ''} INTO content VALUES (?,?)", [id, value])
            self.commit()
            return c.rowcount == 1

    def _putstep_(self, id, name, path, config, dump=None, ignoredup=False):
        configid = UUID(config.encode()).id
        # ALmost never two steps will have the same config,
        #   except the shortest ones which render worthless the avoidance of a second 'insert' attempt.
        with self.cursor() as c:
            self.run(c, f"insert or ignore INTO config VALUES (?,?)", [configid, config])
            sql = f"insert {'or ignore' if ignoredup else ''} INTO step VALUES (NULL,?,?,?,?,?)"
            self.run(c, sql, [id, name, path, configid, dump])
            self.commit()
            return c.rowcount == 1

    def write_many(self, cursor, list_of_tuples, table, ignore_dup=True):
        command = self._insert_ignore if ignore_dup else 'insert'
        sql = f"{command} INTO {table} VALUES({('?,' * len(list_of_tuples[0]))[:-1]})"

        newlist_of_tuples = []
        for row in list_of_tuples:
            newrow = [int(c) if isinstance(c, bool) else c for c in row]
            newlist_of_tuples.append(newrow)
            if self.debug:
                msg = self._interpolate(sql, newrow)
                print(self.name + ":\t>>>>> " + msg)

        sql = sql.replace("?", self._placeholder)
        return cursor.executemany(sql, newlist_of_tuples)
