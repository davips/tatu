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

    def __init__(self, url="sqlite://tatu-sqlite", threaded=True, alias=None):
        if "://" not in url:
            raise Exception("Missing '://' in url:", url)
        backend, db = url.split("://")
        if backend == "sqlite":
            from tatu.sql.sqlite import SQLite
            self.storage = SQLite(db, threaded)
        elif backend == "mysql":
            from tatu.sql.mysql import MySQL
            self.storage = MySQL(db, threaded)
        elif backend == "oka":
            from tatu.okast import OkaSt
            token, url = db.split("@")
            self.storage = OkaSt(token, alias, threaded, url=url)  # TODO Accept user/login in OkaSt?
        else:
            raise Exception("Unknown DBMS backend:", url)

    def _uuid_(self):
        return self.storage.uuid

    # TODO make all args/kwargs explicity for better docs/IDE integration
    def fetch(self, *args, **kwargs):
        return self.storage.fetch(*args, **kwargs)

    def store(self, *args, **kwargs):
        return self.storage.store(*args, **kwargs)

    def fetchhistory(self, *args, **kwargs):
        return self.storage.fetchhistory(*args, **kwargs)

    def fetchstep(self, *args, **kwargs):
        return self.storage.fetchstep(*args, **kwargs)

    def hasdata(self, *args, **kwargs):
        return self.storage.hasdata(*args, **kwargs)

    def getdata(self, *args, **kwargs):
        return self.storage.getdata(*args, **kwargs)

    def hasstep(self, *args, **kwargs):
        return self.storage.hasstep(*args, **kwargs)

    def getstep(self, *args, **kwargs):
        return self.storage.getstep(*args, **kwargs)

    def getfields(self, *args, **kwargs):
        return self.storage.getfields(*args, **kwargs)

    def getcontent(self, *args, **kwargs):
        return self.storage.getcontent(*args, **kwargs)

    def hascontent(self, *args, **kwargs):
        return self.storage.hascontent(*args, **kwargs)

    def removedata(self, *args, **kwargs):
        return self.storage.removedata(*args, **kwargs)

    def lock(self, *args, **kwargs):
        return self.storage.lock(*args, **kwargs)

    def deldata(self, *args, **kwargs):
        return self.storage.deldata(*args, **kwargs)

    def unlock(self, *args, **kwargs):
        return self.storage.unlock(*args, **kwargs)

    def putdata(self, *args, **kwargs):
        return self.storage.putdata(*args, **kwargs)

    def putcontent(self, *args, **kwargs):
        return self.storage.putcontent(*args, **kwargs)

    def putfields(self, *args, **kwargs):
        return self.storage.putfields(*args, **kwargs)

    def storestep(self, *args, **kwargs):
        return self.storage.storestep(*args, **kwargs)

    def putstep(self, *args, **kwargs):
        return self.storage.putstep(*args, **kwargs)
    #
    # @classmethod
    # @classproperty
    # def _now_function(cls):
    #     return cls.storage._now_function
    #
    # @classmethod
    # @classproperty
    # def _keylimit(cls):
    #     return cls.storage._keylimit
    #
    # @classmethod
    # @classproperty
    # def _auto_incr(cls):
    #     return cls.storage._auto_incr
    #
    # @classmethod
    # def _fkcheck(cls, enable):
    #     return cls.storage._fkcheck
    #
    # @classmethod
    # def _on_conflict(cls, cols):
    #     return cls.storage._on_conflict
    #
    # @classmethod
    # @classproperty
    # def _insert_ignore(cls):
    #     return cls.storage._insert_ignore
    #
    # @classmethod
    # @classproperty
    # def _placeholder(cls):
    #     return cls.storage._placeholder
