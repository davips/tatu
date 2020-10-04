import threading
from abc import ABC, abstractmethod
from multiprocessing import JoinableQueue, Queue
from queue import Empty
from typing import Optional

from aiuna.content.data import Data, PickableData
from aiuna.content.specialdata import UUIDData
from cruipto.uuid import UUID
from transf.absdata import AbsData
from transf.step import Step


class Persistence(ABC):
    """
    This class stores and recovers results from some place.
    The children classes are expected to provide storage in e.g.:
     SQLite, remote/local MongoDB, MySQL server, pickled or even CSV files.
    """
    queue = Queue()
    outqueue = JoinableQueue()
    # process_lock = multiprocessing.Lock()
    thread_lock = threading.Lock()
    mythread = None

    # Time spent hoping the thread will be useful again.

    def __init__(self, timeout=2):
        self.timeout = timeout
        if self.__class__.mythread is None:
            # self.__class__.mythread = multiprocessing.Process(target=self._worker, daemon=False)
            self.__class__.mythread = threading.Thread(target=self._worker, daemon=False)
            print("nnnova.........")
            self.mythread.start()

    def _worker(self):
        print("work.....")
        while True:
            try:
                print("........get")
                job = self.queue.get(timeout=self.timeout)
                if "store" in job:
                    print("........store")
                    self._store_(job["store"], job["check_dup"])
                    print("........storeddddddddd")
                elif "fetch" in job:
                    print("........fetch")
                    # if self.queue.empty()
                    ret = self._fetch_pickable_(job["fetch"], job["lock"])
                    self.outqueue.put(ret)
                    self.outqueue.join()
                else:
                    print("Unexpected job:", job)
            except Empty:
                break
            # else:
            #     break

    # @classmethod
    # @contextmanager
    # def safety(cls):
    #     with cls.thread_lock:  # , cls.process_lock:
    #         yield

    # @abstractmethod
    # def dump(self,):
    #     """Dump component"""

    @abstractmethod
    def _store_(self, data: Data, check_dup=True):
        pass

    def store(self, data: Data, check_dup=True):
        """
        Parameters
        ----------
        data
            Data object to store.
        check_dup
            Whether to waste time checking duplicates

        Returns
        -------
        Data or None

        Exception
        ---------
        DuplicateEntryException
        """
        self.queue.put({"store": data.picklable, "check_dup": check_dup})

    def fetch(self, data: Data, lock=False) -> AbsData:
        """Fetch data from DB.

        Parameters
        ----------
        data
            Data object before being transformed by a pipeline.
        lock
            Whether to mark entry (input data and pipeline combination) as
            locked, when no data is found for the entry.

        Returns
        -------
        Data or None

        Exception
        ---------
        LockedEntryException, FailedEntryException
        """
        if not data.ishollow:
            raise Exception("Persistence expects a hollow Data object!")
        data = self.fetch_pickable(data, lock)
        return data and data.unpicklable

    def fetch_pickable(self, data: Data, lock=False) -> Optional[PickableData]:
        if not data.ishollow:
            raise Exception("Persistence expects a hollow Data object!")
        self.queue.put({"fetch": data.picklable, "lock": lock})

        # Wait for result.
        try:
            ret = self.outqueue.get()
            self.outqueue.task_done()
            return ret
        except Exception as e:
            print("Problem while expecting storage reply:", e)
            try:
                self.outqueue.get()
                self.outqueue.task_done()
            finally:
                exit(0)

    @abstractmethod
    def _fetch_pickable_(self, data: Data, lock=False) -> Optional[Data]:
        pass

    @abstractmethod
    def fetch_matrix(self, _id):
        pass

    @abstractmethod
    def list_by_name(self, substring, only_historyless=True):
        """Convenience method to retrieve a list of currently stored Data
        objects by name, ordered cronologically by insertion.

        They are PhantomData objects, i.e. empty ones.

        Parameters
        ----------
        substring
            part of the name to look for
        only_historyless
            When True, return only fresh datasets, i.e. Data objects never
            transformed before.

        Returns
        -------
        List of empty Data objects (PhantomData), i.e. without matrices.

        """
        pass

    @abstractmethod
    def unlock(self, data):
        pass

    def visual_history(self, id_, folder=None):
        """List with all steps/Data objects before the current one. The current avatar is also generated."""
        uuid = UUID()
        data = None
        lastuuid = UUID(id_) if isinstance(id_, str) else id_
        firstdata = self.fetch(UUIDData(lastuuid))
        if firstdata is None:
            print(f"Data {id_} not found!")
            exit()
        # TODO: solve this check in aiuna
        if firstdata.history is None:
            firstdata.history = []
        history = (list(firstdata.history) == 0) or firstdata.history
        if folder:
            lastuuid.generate_avatar(f"{folder}/{f'{id_}.jpg'}")
        lst = []
        for step in history:
            if isinstance(step, Step):
                name = step.name
                transformeruuid = step.uuid
            else:
                name = step["name"]
                transformeruuid = step["uuid"]
            dic = {
                "label": uuid.id, "name": name, "help": str(step), "stored": data is not None
            }
            if folder:
                filename = f"{uuid}.jpg"
                dic["avatar"] = filename
                uuid.generate_avatar(f"{folder}/{filename}")
            lst.append(dic)
            uuid = uuid * transformeruuid
            data = self.fetch(UUIDData(uuid))

        return lst


class UnlockedEntryException(Exception):
    """No node locked entry for this input data and transformation
    combination."""


class LockedEntryException(Exception):
    """Another node is generating output data for this input data
    and transformation combination."""


class FailedEntryException(Exception):
    """This input data and transformation combination have already failed
    before."""


class DuplicateEntryException(Exception):
    """This input data and transformation combination have already been inserted
    before."""


