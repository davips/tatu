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

import socket
import sqlite3

from cruipto.decorator import classproperty
from cruipto.uuid import UUID
from tatu.abs.sql import SQL


class SQLite(SQL):
    def __init__(self, db="tatu-sqlite", threaded=True, storage_info=None, debug=not False, read_only=False):
        self.info = db
        self.read_only = read_only
        self.hostname = socket.gethostname()
        self.database = db + ".db"
        self.storage_info = storage_info
        self.debug = debug
        self._uuid = UUID((self.__class__.__name__ + db).encode())
        super().__init__(threaded, timeout=2)

    def _uuid_(self):
        return self._uuid

    def _open_(self):
        # isolation_level=None -> SQLite autocommiting
        # isolation_level='DEFERRED' -> SQLite transactioning
        self.connection = sqlite3.connect(self.database, isolation_level='DEFERRED')
        self.connection.row_factory = sqlite3.Row

        # Create tables if they don't exist yet.
        try:
            with self.cursor() as c:
                c.execute(f"select 1 from data")
        except:
            if self.debug:
                print("creating database", self.database, "...")
            self._setup()
            self.commit()

    @classproperty
    def _now_function(cls):
        return "datetime()"

    @classproperty
    def _keylimit(cls):
        return ""

    @classproperty
    def _auto_incr(cls):
        return "AUTOINCREMENT"

    @classmethod
    def _on_conflict(cls, cols):
        return f"ON CONFLICT{cols} DO UPDATE SET"

    @classmethod
    def _fkcheck(cls, enable):
        return f"PRAGMA foreign_keys = {'ON' if enable else 'OFF'};"

    @classproperty
    def _insert_ignore(cls):
        return "insert or ignore"

    @classproperty
    def _placeholder(cls):
        return "?"

    def newcursor(self):
        return self.connection.cursor()
