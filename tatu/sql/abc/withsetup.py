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
import warnings
from abc import abstractmethod, ABC
from typing import Optional, List

from aiuna.compression import unpack, pack
from aiuna.content.data import Data, Picklable
from cruipto.uuid import UUID
from tatu.storage import Storage, DuplicateEntryException, LockedEntryException, MissingEntryException
from transf.customjson import CustomJSONEncoder


class withSetup:
    @abstractmethod
    def _auto_incr(self):
        pass

    @abstractmethod
    def query(self, x):
        pass

    def _setup(self):
        print("creating tables...")
        # REMINDER 'inner' is a reserved SQL keyword.
        # REMINDER d.parent = d.uuid / step.uuid; the field is here just to allow a single recursive query,
        # instead of having to calculate the division at the client.
        # TODO create index on column 'id' (and FKs if needed)
        # parent=NULL means child of Root
        self.query(
            f"""
            create table if not exists data (
                n integer NOT NULL primary key {self._auto_incr()},
                id char(23) NOT NULL UNIQUE,
                inn char(23),
                parent char(23) UNIQUE,
                names TEXT,
                fields TEXT, 
                step char(23),
                t TIMESTAMP 
            )"""
        )
        # REMINDER não pode ter [FOREIGN KEY (parent) REFERENCES data(id)] pq nem sempre o parent vai pra base
        # FOREIGN KEY (inn) REFERENCES data(id)  <- REMINDER problems with locking an outer data

        self.query(
            f"""
            create table if not exists content (
                n integer NOT NULL primary key {self._auto_incr()},
                id char(23) NOT NULL UNIQUE,
                value LONGBLOB NOT NULL
            )"""
        )

        self.query(
            f"""
            create table if not exists step (
                n integer NOT NULL primary key {self._auto_incr()},
                id char(23) NOT NULL UNIQUE,
                name varchar(60),
                path varchar(250),
                config text,
                dump LONGBLOB
            )"""
        )

        # Table to speed up look up for already synced Data objects.
        self.query(
            f"""
            create table if not exists sync (
                n integer NOT NULL primary key {self._auto_incr()},
                storage char(23) NOT NULL unique,
                last char(23) NOT NULL,
                t TIMESTAMP
            )"""
        )
        # FOREIGN KEY (attr) REFERENCES attr(aid)
        # self.query(f'CREATE INDEX nam0 ON dataset (des{self._keylimit()})')
        # self.query(f'CREATE INDEX nam1 ON dataset (attr)')
        # insl timestamp NOT NULL     # unique(dataset, hist),
        # spent FLOAT,        # fail TINYINT,      # start TIMESTAMP NOT NULL,
        # update data set {','.join([f'{k}=?' for k in to_update.keys()])}
        # insd=insd, upd={self._now_function()} where did=?
        #     x = coalesce(values(x), x),
        #     from res left join data on dout = did
        #     left join dataset on dataset = dsid where
