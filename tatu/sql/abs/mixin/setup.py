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
from abc import abstractmethod, ABC


class withSetup(ABC):
    @abstractmethod
    def _auto_incr(self):
        pass

    @abstractmethod
    def query2(self, x):
        pass

    @abstractmethod
    def _now_function(self):
        pass

    @abstractmethod
    def _keylimit(self):
        pass

    def _setup(self):
        print("creating tables...")
        # REMINDER 'inner' is a reserved SQL keyword.
        # REMINDER d.parent = d.uuid / step.uuid; the field is there just to allow a single recursive query directly on the server.
        # REMINDER char automatically pads with spaces but ignores them when comparing, perfect instead of varchar that is slower
        # REMINDER field char(24) is almost arbitrary, perhaps someone wants to name a field with a uuid;
        #          23+1 just shows that it is not a uuid column.

        # Table for field contents (matrices, ...) and dumps (pickled models, ...).
        self.query2(
            f"""
            create table if not exists content (
                id char(23) NOT NULL primary key,
                value LONGBLOB NOT NULL
            )"""
        )
        self.query2(f'CREATE INDEX idx0 ON content (id)')

        # Table with values for parameters of Step objects. Insert the empty config as the fisrt one.
        # '04fmE1EWKIsDKlrhOf6vky3' = UUID("{}".encode()).id
        self.query2(
            f"""
            create table if not exists config (
                id char(23) NOT NULL primary key,
                params text NOT NULL
            )"""
        )
        self.query2(f'CREATE INDEX idx1 ON config (params({self._keylimit()}))')
        self.query2("insert into config values ('04fmE1EWKIsDKlrhOf6vky3', '{}')")

        # Table with steps. The mythical NoOp step that created the Root data is inserted as the first one (due to constraint issues).
        self.query2(
            f"""
            create table if not exists step (
                n integer NOT NULL primary key {self._auto_incr()},
                id char(23) NOT NULL UNIQUE,
                name char(60) NOT NULL,
                path varchar(250) NOT NULL,
                config char(23) NOT NULL,
                content char(23),
                FOREIGN KEY (config) REFERENCES config(id),
                FOREIGN KEY (content) REFERENCES content(id)
            )"""
        )
        self.query2(f'CREATE INDEX idx2 ON step (id)')
        self.query2(f'CREATE INDEX idx3 ON step (name)')
        self.query2(f'CREATE INDEX idx4 ON step (path)')
        self.query2("insert into step values (null, '048e4WDl9tFnovD6HYHePEb', 'NoOp', 'akangatu.noop', '04fmE1EWKIsDKlrhOf6vky3', null)")

        # Table data.
        self.query2(
            f"""
            create table if not exists data (
                n integer NOT NULL primary key {self._auto_incr()},
                id char(23) NOT NULL UNIQUE,
                step char(23) NOT NULL,
                inn char(23),
                stream boolean,
                parent char(23) not null,
                locked boolean not null,
                t DATETIME,
                unique(stepid, parent),
                FOREIGN KEY (step) REFERENCES step(id),
                FOREIGN KEY (inn) REFERENCES data(id),
                FOREIGN KEY (parent) REFERENCES data(id)
            )"""
        )
        self.query2(f'CREATE INDEX idx5 ON data (id)')
        self.query2(f'CREATE INDEX idx6 ON data (step)')
        self.query2(f'CREATE INDEX idx7 ON data (parent)')
        self.query2(f"insert into data values (null, '00000000000000000000001', '048e4WDl9tFnovD6HYHePEb', null, false, '00000000000000000000001', false, {self._now_function()}")

        self.query2(
            f"""
            create table if not exists field (
                data char(23) NOT NULL,
                name char(24) NOT NULL,
                content char(23) NOT NULL,
                primary key (data, name),
                FOREIGN KEY (data) REFERENCES data(id),
                FOREIGN KEY (content) REFERENCES content(id)
            )"""
        )
        self.query2(f'CREATE INDEX idx8 ON field (name)')
        self.query2(f'CREATE INDEX idx9 ON field (data)')

        # Table to speed up lookup for not yet synced Data objects.
        self.query2(
            f"""
            create table if not exists storage (
                id char(23) NOT NULL primary key,
                data char(23) NOT NULL,
                t DATETIME,
                FOREIGN KEY (data) REFERENCES data(id)
            )"""
        )
        self.query2(f'CREATE INDEX idx10 ON storage (dataid)')

        # Table to record stream, like folds of cross-validation (or chunks of a datastream?).
        self.query2(
            f"""
            create table if not exists stream (
                data char(23) NOT NULL primary key,
                pos integer not null,
                chunk char(23) NOT NULL,
                primary key (data, pos),
                FOREIGN KEY (data) REFERENCES data(id),
                FOREIGN KEY (chunk) REFERENCES data(id)
            )"""
        )
        self.query2(f'CREATE INDEX idx11 ON stream (chunk)')

        # spent FLOAT,        # fail TINYINT,      # start TIMESTAMP NOT NULL,
        # update data set {','.join([f'{k}=?' for k in to_update.keys()])}
        #     x = coalesce(values(x), x),   # Return the first non-null value in a list
        #     from res left join data on dout = did
        #     left join dataset on dataset = dsid where
