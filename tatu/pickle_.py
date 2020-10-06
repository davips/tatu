import _pickle as pickle
import os
import traceback
from glob import glob
from pathlib import Path
from typing import Optional

from aiuna.content.data import Data, Picklable
from tatu.disk import save, load
from tatu.persistence import (
    Persistence,
    LockedEntryException,
    FailedEntryException,
    DuplicateEntryException,
    UnlockedEntryException, MissingEntryException,
)


class Pickle(Persistence):
    def __init__(self, blocking=False, db="tatu-sqlite", compress=True):
        self.db = db
        self.compress = compress
        if not Path(db).exists():
            os.mkdir(db)
        super().__init__(blocking, timeout=1)

    def _delete_(self, data: Data, check_missing=True):
        locked = self._filename("", data)
        if check_missing and not Path(locked).exists():
            raise MissingEntryException("Does not exist:", data.id)
        os.remove(locked)

    def _fetch_picklable_(self, data: Data, lock=False) -> Optional[Picklable]:
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

        # Failed?
        if transformed_data.failure is not None:
            raise FailedEntryException(transformed_data.failure)

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

    def list_by_name(self, substring, only_original=True):  # TODO: take advantage of lazy data, instead of using hollow
        datas = []
        path = self.db + f"/*{substring}*-*.dump"
        for file in sorted(glob(path), key=os.path.getmtime):
            data = self._load(file)
            if only_original and len(data.history) == 1:
                datas.append(data.hollow(tuple()))
        return datas

    def fetch_matrix(self, id):
        raise NotImplementedError

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

    def _open(self):
        pass


# from tatu.sql.sqlite import SQLite
# PickleServer().store(File("iris.arff").data)
# print(PickleServer().visual_history(File("iris.arff").data.id))
# exit()

def sqlite_Test():
    from tatu.sql.sqlite import SQLite
    from aiuna.file import File
    data = File("iris.arff").data
    SQLite().delete(data, check_missing=False)
    SQLite().store(data)
    print(SQLite().visual_history(File("iris.arff").data.id))

# from tatu.sql.mysql import MySQL
# MySQL(db="tatu:xxxxx@localhost/tatu").store(File("iris.arff").data)
# print(MySQL(db="tatu:xxxxxx@localhost/tatu").visual_history(File("iris.arff").data.id))
# exit()

# sqlite_Test()
