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
from tatu.abs.storage import Storage


class Tatu(Storage):
    storage = None

    def __init__(self, url="sqlite://tatu-sqlite", threaded=True, alias=None, close_when_idle=False):
        # print("STORAGE:", url)
        if "://" not in url:
            raise Exception("Missing '://' in url:", url)
        backend, db = url.split("://")
        if backend == "sqlite":
            from tatu.sql.sqlite import SQLite
            self.storage = SQLite(db, threaded, close_when_idle)
        elif backend == "mysql":
            from tatu.sql.mysql import MySQL
            self.storage = MySQL(db, threaded, close_when_idle)
        elif backend == "oka":
            from tatu.okast import OkaSt
            token, url = db.split("@")
            self.storage = OkaSt(token, alias, threaded, "http://" + url,
                                 close_when_idle)  # TODO Accept user/login in OkaSt?
        else:
            raise Exception("Unknown DBMS backend:", url)

    def _uuid_(self):
        return self.storage.uuid

    # TODO make all args/kwargs explicity for better docs/IDE integration
    def fetch(self, data, lock=False, lazy=True):
        return self.storage.fetch(data, lock, lazy)

    def store(self, data, unlock=False, ignoredup=False, lazy=False):
        return self.storage.store(data, unlock, ignoredup, lazy)

    def fetchhistory(self, id):
        return self.storage.fetchhistory(id)

    def fetchstep(self, id):
        return self.storage.fetchstep(id)

    def hasdata(self, id, include_empty=False):
        return self.storage.hasdata(id, include_empty)

    def hasstream(self, data):
        return self.storage.hasstream(data)

    def getdata(self, id, include_empty=True):
        return self.storage.getdata(id, include_empty)

    def getstream(self, data):
        return self.storage.getstream(data)

    def hasstep(self, id):
        return self.storage.hasstep(id)

    def getstep(self, id):
        return self.storage.getstep(id)

    def getfields(self, id):
        return self.storage.getfields(id)

    def getcontent(self, id):
        return self.storage.getcontent(id)

    def hascontent(self, ids):
        return self.storage.hascontent(ids)

    def removedata(self, data, check_existence=True, recursive=True):
        return self.storage.removedata(data, check_existence, recursive)

    def lock(self, id, check_existence=True):
        return self.storage.lock(id, check_existence)

    def deldata(self, id, check_success=True):
        return self.storage.deldata(id, check_success)

    def unlock(self, id, check_success=True):
        return self.storage.unlock(id, check_success)

    def putdata(self, id, step, inn, stream, parent, locked, ignoredup=False):
        return self.storage.putdata(id, step, inn, stream, parent, locked, ignoredup)

    def putstream(self, rows, ignoredup=False):
        return self.storage.putstream(rows, ignoredup)

    def putcontent(self, id, value, ignoredup=False):
        return self.storage.putcontent(id, value, ignoredup)

    def putfields(self, rows, ignoredup=False):
        return self.storage.putfields(rows, ignoredup)

    def storestep(self, step, dump=None, ignoredup=False):
        return self.storage.storestep(step, dump, ignoredup)

    def putstep(self, id, name, path, config, dump=None, ignoredup=False):
        return self.storage.putstep(id, name, path, config, dump, ignoredup)

    def open(self):
        return self.storage.open()

    def close(self):
        return self.storage.close()
