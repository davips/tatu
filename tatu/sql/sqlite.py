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
import socket
import sqlite3

from cruipto.uuid import UUID
from tatu.sql.abc.sql import SQL


class SQLite(SQL):
    def __init__(self, db="tatu-sqlite", threaded=True, storage_info=None, debug=not False, read_only=False):
        self.info = db
        self.read_only = read_only
        self.hostname = socket.gethostname()
        self.database = db + ".db"
        self.storage_info = storage_info
        self.debug = debug
        self._uuid = UUID(db.encode())
        super().__init__(threaded, timeout=2)

    def _uuid_(self):
        return self._uuid

    def _open_(self):
        # isolation_level=None -> SQLite autocommiting
        # isolation_level='DEFERRED' -> SQLite transactioning
        self.connection = sqlite3.connect(self.database, isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.cursor = self.connection.cursor()

        # Create tables if they don't exist yet.
        try:
            self.query(f"select 1 from data")
        except:
            if self.debug:
                print("creating database", self.database, "...")
            self._setup()

    @staticmethod
    def _now_function():
        return "datetime()"

    @staticmethod
    def _auto_incr():
        return "AUTOINCREMENT"

    @staticmethod
    def _keylimit():
        return ""

    @staticmethod
    def _on_conflict(fields=""):
        return f"ON CONFLICT{fields} DO UPDATE SET"
