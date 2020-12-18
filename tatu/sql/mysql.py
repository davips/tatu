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

import socket

import pymysql
import pymysql.cursors
from pymysql.constants import CLIENT

from garoupa.decorator import classproperty
from garoupa.uuid import UUID
from tatu.abs.sql import SQL


class MySQL(SQL):
    def __init__(self,
                 db="user:pass@ip/db", threaded=True, close_when_idle=False, storage_info=None, debug=False,
                 read_only=False):
        self._uuid = UUID((self.__class__.__name__ + db).encode())
        if "@" not in db:
            raise Exception("Missing @ at db url:", db)
        server = db.split("/")[0]
        db = db.split("/")[1]
        self.info = "STORAGE DBG:" + server + ", " + db
        self.read_only = read_only
        self.database = server
        credentials, self.host = server.split("@")
        self.user, self.password = credentials.split(":")
        self.db = db  # TODO sensitive information should disappear after init
        self.storage_info = storage_info
        self.debug = debug
        if "-" in db:
            raise Exception("'-' not allowed in db name!")
        self.hostname = socket.gethostname()
        super().__init__(threaded, timeout=8, close_when_idle=close_when_idle)

    def _uuid_(self):
        return self._uuid

    def _open_(self):
        """
        Each reconnection has a cost of approximately 150ms in ADSL (ping=30ms).
        :return:
        """
        if self.debug:
            print("getting connection...")
        self.connection = pymysql.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            charset="utf8",
            # cursorclass=pymysql.cursors.DictCursor,  # set at the bottom of this file
            # client_flag=CLIENT.MULTI_STATEMENTS
        )
        self.connection.client_flag &= pymysql.constants.CLIENT.MULTI_STATEMENTS
        self.connection.autocommit(False)
        # self.connection.server_status

        if self.debug:
            print("getting cursor...")

        # Create db if it doesn't exist yet.
        with self.cursor() as c:
            c.execute(*self.prepare(f"SHOW DATABASES LIKE '{self.db}'"))
            setup = c.fetchone() is None
            if setup:
                if self.debug:
                    print("creating database", self.db, "on", self.database, "...")
                c.execute("create database if not exists " + self.db)
                self.commit()

        if self.debug:
            print("using database", self.db, "on", self.database, "...")
        with self.cursor() as c:
            c.execute("use " + self.db)
            c.execute(f"show tables")

        # Create tables if they don't exist yet.
        try:
            with self.cursor() as c:
                c.execute(f"select 1 from data")
        except:
            if self.debug:
                print("creating database", self.database, "...")
            self._setup()
            self.commit()

        return self

    @classproperty
    def _now_function(cls):
        return "now()"

    @classproperty
    def _keylimit(cls):
        return "(190)"

    @classproperty
    def _auto_incr(cls):
        return "AUTO_INCREMENT"

    @classmethod
    def _on_conflict(cls, cols):
        return "ON DUPLICATE KEY UPDATE"

    @classproperty
    def _insert_ignore(cls):
        return "insert ignore"

    @classmethod
    def _fkcheck(cls, enable):
        return f"SET FOREIGN_KEY_CHECKS={'1' if enable else '0'};"

    @classproperty
    def _placeholder(cls):
        return "%s"

    def newcursor(self):
        return self.connection.cursor(pymysql.cursors.DictCursor)
