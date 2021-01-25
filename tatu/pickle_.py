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

import json
import os
import traceback
from glob import glob
from pathlib import Path

import _pickle as pickle

from aiuna.content.data import Data
from garoupa.uuid import UUID
from tatu.abs.storage import MissingEntryException, LockedEntryException, DuplicateEntryException, \
    UnlockedEntryException
from tatu.disk import save, load
from tatu.storageinterface import StorageInterface


class Pickle(StorageInterface):
    def _hasdata_(self, id, include_empty):
        pass

    def _hasstream_(self, data):
        pass

    def _getdata_(self, id, include_empty):
        pass

    def _getstream_(self, data):
        pass

    def _hasstep_(self, id):
        pass

    def _getstep_(self, id):
        pass

    def _getfields_(self, id):
        pass

    def _hascontent_(self, ids):
        pass

    def _getcontent_(self, id):
        pass

    def _lock_(self, id):
        pass

    def _putdata_(self, id, step, inn, stream, parent, locked, ignoredup):
        pass

    def _putstream_(self, rows, ignoredup):
        pass

    def _putfields_(self, rows, ignoredup):
        pass

    def _putcontent_(self, id, value, ignoredup):
        pass

    def _putstep_(self, id, name, path, config, dump, ignoredup):
        pass

    def _deldata_(self, id):
        pass

    def _fetch_children_(self, data: Data):
        raise Exception("Storage Pickle cannot retrieve children!")

    def __init__(self, threaded=True, db="tatu-sqlite", compress=True, close_when_idle=False):
        self.db = db
        self.compress = compress
        if not Path(db).exists():
            os.mkdir(db)
        self._uuid = UUID(json.dumps([db, compress], sort_keys=True, ensure_ascii=False).encode())
        super().__init__(threaded, timeout=1, close_when_idle=close_when_idle)

    def _uuid_(self):
        return self._uuid

    def _delete_(self, data: Data, check_missing=True):
        locked = self._filename("", data)
        if check_missing and not Path(locked).exists():
            raise MissingEntryException("Does not exist:", data.id)
        os.remove(locked)

    def _fetch_(self, data: Data, lock=False):
        # TODO: deal with fields and missing fields?
        filename = self._filename("*", data)

        # Not started yet?
        if not Path(filename).exists():
            print('W: Not started.', filename)
            if lock:
                print("W: Locking...", filename)
                Path(filename).touch()
            return None

        # Locked?
        if Path(filename).stat().st_size == 0:
            print("W: Previously locked by other process.", filename)
            raise LockedEntryException(filename)

        transformed_data = self._load(filename)

        # # Failed?
        # if transformed_data.failure is not None:
        #     raise FailedEntryException(transformed_data.failure)

        return transformed_data

    def _store_(self, data, check_dup=True):
        """The dataset name of data_out will be the filename prefix for
        convenience."""

        # TODO: reput name on Data?
        filename = self._filename("", data)
        # filename = self._filename(data.name, data, training_data_uuid)

        # sleep(0.020)  # Latency simulator.

        # Already exists?
        if check_dup and Path(filename).exists():
            raise DuplicateEntryException("Already exists:", filename)

        locked = self._filename("", data)
        if Path(locked).exists():
            os.remove(locked)

        self._dump(data, filename)

    def _filename(self, prefix, data):
        zip = "compressed" if self.compress else ""
        # Not very efficient.  TODO: memoize extraction of fields from JSON?
        # uuids = [json.loads(tr)['uuid'][:6] for tr in data.history]
        # rest = f"-".join(uuids) + f".{zip}.dump"
        rest = f"{data if isinstance(data, str) else data.id}.{zip}.dump"
        if prefix == "*":
            query = self.db + "/*" + rest
            lst = glob(query)
            if len(lst) > 1:
                raise Exception("Multiple files found:", query, lst)
            if len(lst) == 1:
                return lst[0]
            else:
                return self.db + "/" + rest
        else:
            return self.db + "/" + prefix + rest

    def _load(self, filename):
        """
        Retrieve a Data object from disk.
        :param filename: file dataset
        :return: Data
        """
        try:
            if self.compress:
                data = load(filename)
            else:
                f = open(filename, "rb")
                res = pickle.load(f)
                f.close()
                data = res
            return data
        except Exception as e:
            traceback.print_exc()
            print("Problems loading", filename)
            exit(0)

    def _dump(self, data, filename):
        """
        Dump a Data object to disk.
        :param data: Data
        :param filename: file dataset
        :return: None
        """
        print("W: Storing...", filename)
        data = data.picklable
        if self.compress:
            save(filename, data)
        else:
            f = open(filename, "wb")
            pickle.dump(data, f)
            f.close()

    def _unlock_(self, data):
        filename = self._filename("*", data)
        if not Path(filename).exists():
            raise UnlockedEntryException("Cannot unlock something that is not " "locked!", filename)
        print("W: Unlocking...", filename)
        os.remove(filename)

    def _open_(self):
        pass

    def _close_(self):
        pass

    def fetch_field(self, _id):
        raise NotImplementedError

    # def list_by_name(self, substring, only_original=True):
    #     datas = []
    #     path = self.db + f"/*{substring}*-*.dump"
    #     for file in sorted(glob(path), key=os.path.getmtime):
    #     data = self._load(file)
    #     if only_original and len(data.history) == 1:
    #         datas.append(data)  # Data is now fully lazy, so nothing will be retrieved.
    #     return datas


# def sqlite_Test():
#     from tatu.sql.sqlite import SQLite
#     from aiuna.step.file import File
#     data = File("iris.arff").data
#     SQLite().delete(data, check_missing=False)
#     SQLite().store(data)
#     print(SQLite().visual_history(File("iris.arff").data.id))

# from tatu.sql.sqlite import SQLite
# PickleServer().store(File("iris.arff").data)
# print(PickleServer().visual_history(File("iris.arff").data.id))
# exit()

# from tatu.sql.mysql import MySQL
# MySQL(db="tatu:xxxxx@localhost/tatu").store(File("iris.arff").data)
# print(MySQL(db="tatu:xxxxxx@localhost/tatu").visual_history(File("iris.arff").data.id))
# exit()

# sqlite_Test()
