from aiuna.config import STORAGE_CONFIG
from aiuna.content.data import Data
from tatu.amnesia import Amnesia
from tatu.persistence import Persistence
from tatu.pickleserver import PickleServer
from tatu.worker2 import Worker2


class Storage(Worker2, Persistence):
    """Multithreaded* persistence.

    * -> Two options:
        monothread (Python threading peculiar concept)
        multiprocess (Python multiprocessing concept)

    ps. É global, mas sem perder a integridade referencial.
    'New' garante o mesmo objeto para mesmo argumento."""

    def __new__(cls, alias):
        if alias not in STORAGE_CONFIG["storages"]:
            STORAGE_CONFIG["storages"][alias] = object.__new__(cls)
        return STORAGE_CONFIG["storages"][alias]

    def __init__(self, alias):
        super().__init__()
        self.alias = alias

    def store(self, data: Data, check_dup=True):
        self.put("store", {"data": data.picklable, "check_dup": check_dup})

    def _fetch_impl(self, data: Data, lock=False) -> Data:
        return self.put("fetch", {"data": data.picklable, "lock": lock}, wait=True)

    def fetch_matrix(self, id):
        return self.put("fetch_matrix", {"id": id}, wait=True)

    def unlock(self, data):
        self.put("unlock", {"data": data.picklable})

    def list_by_name(self, substring, only_historyless=True):
        pass

    # TIP: self.lock and SQLite cannot be passed to threads, due to pickle err
    # TIP: sqlite3.ProgrammingError: SQLite objects created in a thread can
    # only be used in that same thread.
    # ==================================================

    @staticmethod
    def backend(alias):
        kwargs = STORAGE_CONFIG.get(alias, {}).copy()
        engine = kwargs.pop("engine") if "engine" in kwargs else alias

        if engine == "amnesia":
            return Amnesia()
        elif engine == "mysql":
            from tatu.sql.mysql import MySQL
            # TODO: does mysql already have extra settings now?
            return MySQL(storage_info=alias, **kwargs)
        elif engine == "okapost":
            from tatu.okaserver import OkaServer
            return OkaServer(storage_info=alias, post=True, **kwargs)
        elif engine == "oka":
            from tatu.okaserver import OkaServer
            return OkaServer(storage_info=alias, post=False, **kwargs)
        elif engine == "sqlite":
            from tatu.sql.sqlite import SQLite
            return SQLite(storage_info=alias, **kwargs)
        elif engine == "dump":
            return PickleServer(**kwargs)
        else:
            raise Exception("Unknown engine:", engine)
