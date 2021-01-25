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
from abc import abstractmethod, ABC
from contextlib import contextmanager

from aiuna.content.root import Root
from akangatu.transf.noop import NoOp
from garoupa.decorator import classproperty
from garoupa.uuid import UUID


class Setup:
    """
    >>> 1
    1
    """


class withSetup(ABC):
    connection = None

    @contextmanager
    def cursor(self):
        cursor = self.newcursor()
        try:
            yield cursor
        finally:
            cursor.close()

    def run(self, cursor, sql, args=[]):
        return cursor.execute(*self.prepare(sql, args))

    @abstractmethod
    def newcursor(self):
        pass

    @abstractmethod
    def prepare(self, sql, arg=[]):
        pass

    @classmethod
    @abstractmethod
    @classproperty
    def _now_function(cls):
        pass

    @classmethod
    @abstractmethod
    @classproperty
    def _keylimit(cls):
        pass

    @classmethod
    @abstractmethod
    @classproperty
    def _auto_incr(cls):
        pass

    @classmethod
    @abstractmethod
    def _fkcheck(cls, enable):
        pass

    @classmethod
    @abstractmethod
    def _on_conflict(cls, cols):
        pass

    @classmethod
    @classproperty
    @abstractmethod
    def _insert_ignore(cls):
        pass

    @classmethod
    @abstractmethod
    @classproperty
    def _placeholder(cls):
        pass

    def _setup(self):
        print("creating tables...")
        # REMINDER 'inner' is a reserved SQL keyword.
        # REMINDER d.parent = d.uuid / step.uuid; the field is there just to allow a single recursive query directly on
        # the server.
        # REMINDER char automatically pads with spaces but ignores them when comparing, perfect instead of varchar that
        # is slower
        # REMINDER field char(24) is almost arbitrary, perhaps someone wants to name a field with a uuid;
        #      23+1 just shows that it is not a uuid column.

        # Table for field contents (matrices, ...) and dumps (pickled models, ...).
        sql = "create table if not exists content (id char(23) NOT NULL primary key, value LONGBLOB NOT NULL)"
        with self.cursor() as c:
            self.run(c, sql)

        # Table with values for parameters of Step objects. Insert the empty config as the fisrt one.
        sql = "create table if not exists config (id char(23) NOT NULL primary key, params text NOT NULL)"
        with self.cursor() as c:
            self.run(c, sql)
            try:  # REMINDER Only MariaDB accepts 'CREATE INDEX if not exists'
                self.run(c, f"CREATE INDEX  idx1 ON config (params{self._keylimit})")
            except:
                pass
            self.run(c, f"insert into config values ('{UUID(b'{}').id}', " + "'{}')")

        # Table with steps.
        # The mythical NoOp step that created the Root data is inserted as the first one (due to constraint issues).
        sql = f"""
            create table if not exists step (
                n integer NOT NULL primary key {self._auto_incr},
                id char(23) NOT NULL UNIQUE,
                name char(60) NOT NULL,
                path varchar(250) NOT NULL,
                config char(23) NOT NULL,
                content char(23),
                FOREIGN KEY (config) REFERENCES config(id),
                FOREIGN KEY (content) REFERENCES content(id)
            )"""
        with self.cursor() as c:
            self.run(c, sql)
            try:
                self.run(c, f'CREATE INDEX idx2 ON step (id)')
                self.run(c, f'CREATE INDEX idx3 ON step (name)')
                self.run(c, f'CREATE INDEX idx4 ON step (path)')
            except:
                pass
        no = NoOp()
        sql = f"insert into step values (null, '{no.id}', '{no.name}', '{no.context}', '{UUID(b'{}').id}', null)"
        with self.cursor() as c:
            self.run(c, sql)

        # Table data.
        sql = f"""
            create table if not exists data (
                n integer NOT NULL primary key {self._auto_incr},
                id char(23) NOT NULL UNIQUE,
                step char(23) NOT NULL,
                inn char(23),
                stream boolean not null,
                parent char(23) not null,
                locked boolean,
                unique(step, parent),
                FOREIGN KEY (step) REFERENCES step(id),
                FOREIGN KEY (inn) REFERENCES data(id),
                FOREIGN KEY (parent) REFERENCES data(id)
            )"""
        with self.cursor() as c:
            self.run(c, sql)
            try:
                self.run(c, f'CREATE INDEX  idx5 ON data (id)')
                self.run(c, f'CREATE INDEX  idx6 ON data (step)')
                self.run(c, f'CREATE INDEX  idx7 ON data (parent)')
            except:
                pass
        sql = f"insert into data values (null, '{Root.id}', '{UUID.identity.id}', null, 0, '{Root.id}', 0)"
        with self.cursor() as c:
            self.run(c, sql)

        sql = f"""
            create table if not exists field (
                data char(23) NOT NULL,
                name char(24) NOT NULL,
                content char(23) NOT NULL,
                primary key (data, name),
                FOREIGN KEY (data) REFERENCES data(id),
                FOREIGN KEY (content) REFERENCES content(id)
            )"""
        with self.cursor() as c:
            self.run(c, sql)
            try:
                self.run(c, f'CREATE INDEX  idx8 ON field (name)')
                self.run(c, f'CREATE INDEX  idx9 ON field (data)')
            except:
                pass

        # Table to speed up lookup for not yet synced Data objects.
        sql = f"""
            create table if not exists storage (
                id char(23) NOT NULL primary key,
                data char(23) NOT NULL,
                t DATETIME,
                FOREIGN KEY (data) REFERENCES data(id)
            )"""
        with self.cursor() as c:
            self.run(c, sql)
            try:
                self.run(c, f'CREATE INDEX  idx10 ON storage (data)')
            except:
                pass

        # Table to record stream, like folds of cross-validation (or chunks of a datastream?).
        sql = f"""
            create table if not exists stream (
                data char(23) NOT NULL,
                pos integer not null,
                chunk char(23) NOT NULL,
                unique (data, pos),
                FOREIGN KEY (data) REFERENCES data(id),
                FOREIGN KEY (chunk) REFERENCES data(id)
            )"""
        with self.cursor() as c:
            self.run(c, sql)
            try:
                self.run(c, f'CREATE INDEX  idx11 ON stream (chunk)')
            except:
                pass

        # Table to record volatile info, i.e. related to a specific run of a step*
        #     (e.g. person specific or several runs to assess time).
        # 'id' here is a universal time based UUID(),
        #   instead of being based on a hash or on a multiplication like the other ones.
        # * -> related to a specific generation of a Data object - more precisely.
        #  Many rows can exist for the same Data object.
        # 'alive': time of last ping from 'node'
        # 'duration'=NULL while not finished
        sql = f"""
            create table if not exists run (
                id char(23) NOT NULL primary key,
                data char(23) NOT NULL,
                duration float,
                node char(255) NOT NULL,
                alive DATETIME,
                t DATETIME,
                FOREIGN KEY (data) REFERENCES data(id)
            )"""
        with self.cursor() as c:
            self.run(c, sql)
            try:
                self.run(c, f'CREATE INDEX  idx12 ON run (data)')
                self.run(c, f'CREATE INDEX  idx13 ON run (node)')
            except:
                pass

        # fail TINYINT
        # update data set {','.join([f'{k}=?' for k in to_update.keys()])}
        #     x = coalesce(values(x), x),   # Return the first non-null value in a list
        #     from res left join data on dout = did
        #     left join dataset on dataset = dsid where
