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

import pymysql
import pymysql.cursors

from cruipto.uuid import UUID
from tatu.sql.abc.sql import SQL


class MySQL(SQL):
    def __init__(self, db="user:pass@ip/db", threaded=True, storage_info=None, debug=True, read_only=False):
        server = db.split("/")[0]
        db = db.split("/")[1]
        self.info = server + ", " + db
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

        self._uuid = UUID(db.encode())
        super().__init__(threaded, timeout=8)

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
            cursorclass=pymysql.cursors.DictCursor,
        )
        # self.connection.client_flag &= pymysql.constants.CLIENT.MULTI_STATEMENTS
        self.connection.autocommit(True)

        if self.debug:
            print("getting cursor...")
        self.cursor = self.connection.cursor()

        # Create db if it doesn't exist yet.
        self.query(f"SHOW DATABASES LIKE '{self.db}'")
        setup = self.get_one() is None
        if setup:
            if self.debug:
                print("creating database", self.db, "on", self.database, "...")
            self.cursor.execute("create database if not exists " + self.db)

        if self.debug:
            print("using database", self.db, "on", self.database, "...")
        self.cursor.execute("use " + self.db)
        self.query(f"show tables")

        # Create tables if they don't exist yet.
        try:
            self.query(f"select 1 from data")
        except:
            if self.debug:
                print("creating database", self.database, "...")
            self._setup()

        return self

    @staticmethod
    def _now_function():
        return "now()"

    @staticmethod
    def _auto_incr():
        return "AUTO_INCREMENT"

    @staticmethod
    def _keylimit():
        return "(190)"

    @staticmethod
    def _on_conflict(fields=None):
        return "ON DUPLICATE KEY UPDATE"
