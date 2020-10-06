import threading
from abc import ABC, abstractmethod
from contextlib import contextmanager
from functools import reduce
from multiprocessing import JoinableQueue, Queue
from queue import Empty
from typing import Optional

from aiuna.content.data import Data, Picklable
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
    open = False

    # Time spent hoping the thread will be useful again.

    def __init__(self, timeout=5):
        self.timeout = timeout
        if self.__class__.mythread is None:
            # self.__class__.mythread = multiprocessing.Process(target=self._worker, daemon=False)
            self.__class__.mythread = threading.Thread(target=self._worker, daemon=False)
            print("Starting thread for", self.__class__.__name__)
            self.mythread.start()

    def _worker(self):
        with self.safety():
            if not self.open:
                try:
                    self._open()
                    self.open = True
                except Exception as e:
                    print(e)
                    self.outqueue.put(False)
                    exit()
        while self.open:
            try:
                t, dt = 0, 0.25
                job = None
                while job is None and t < self.timeout:
                    try:
                        job = self.queue.get(timeout=dt)
                    except Empty:
                        if not threading.main_thread().is_alive():
                            break
                    t += dt
                if job is None:
                    break

                try:
                    if "unlock" in job:
                        self.unlock(job["unlock"])
                    elif "delete" in job:
                        self._delete_(job["delete"], job["check_missing"])
                    elif "store" in job:
                        self._store_(job["store"], job["check_dup"])
                    elif "fetch" in job:
                        ret = self._fetch_picklable_(job["fetch"], job["lock"])
                        self.outqueue.put(ret)
                        self.outqueue.join()
                    else:
                        print("Unexpected job:", job)
                except Exception as e:
                    print(f"Problem while processing job {job}:", e)
                    if threading.main_thread().is_alive():
                        self.outqueue.put(False)
                    raise Exception
                    break

            except Empty:
                break

    @classmethod
    @contextmanager
    def safety(cls):
        with cls.thread_lock:  # , cls.process_lock:
            yield

    # @abstractmethod
    # def dump(self,):
    #     """Dump component"""

    @abstractmethod
    def _open(self):
        pass

    @abstractmethod
    def _delete_(self, data: Data, check_missing=True):
        pass

    @abstractmethod
    def _store_(self, data: Data, check_dup=True):
        pass

    def delete(self, data: Data, check_missing=True, recursive=True):
        while data:
            self.queue.put({"delete": data.picklable, "check_missing": check_missing})
            if not recursive:
                break
            data = data.inner

    def store(self, data: Data, check_dup=True, recursive=True):
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
        if data.stream:
            print("Cannot store Data objects containing a stream.")
            exit()
        # traverse all nested inner Data objects
        lst = []
        while data:
            lst.append({"store": data.picklable, "check_dup": check_dup})
            if not recursive:
                break
            data = data.inner
            check_dup = False
            # We disable check_dup here because the fetch attempt to verify existence
            # only happened (at most) for outer data (moreover, sending of matrices is optimized, anyway).
            # REMINDER: Cache could not traverse fetching/storing because it doesn't know
            # how to process inner data, it only knows how to apply a step to the outer data as a whole.
        # insert from last to first due to foreign key constraint on inner->data.id
        for job in reversed(lst):
            self.queue.put(job)

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
        # TODO: accept id string
        # TODO: create Hollow: jsonable vai precisar conter tipo da classe (Picklable, Hollow, ...)
        data = self.fetch_picklable(data, lock)
        return data and data.unpicklable

    def fetch_picklable(self, data: Data, lock=False) -> Optional[Picklable]:
        data = data.picklable
        lst = []
        while data:
            self.queue.put({"fetch": data, "lock": lock})

            # Wait for result.
            output = self.outqueue.get()
            if not (output is None or isinstance(output, AbsData)):
                id = data if isinstance(data, str) else data.id
                print("type:", type(output), output)
                print(f"Couldn't fetch {id}. Quiting...")
                self.outqueue.task_done()
                exit()
            self.outqueue.task_done()

            if output is None or not (output.inner is None or isinstance(output.inner, str)):
                # Task complete if None or if inner is already build (e.g. coming from Pickle).
                return output

            data = output.inner
            lst.append(output)

        # reconstruct lineage
        return reduce(lambda inner, outer: outer.replace([], inner=inner), reversed(lst))

    @abstractmethod
    def _fetch_picklable_(self, data: Data, lock=False) -> Optional[Data]:
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
    def _unlock_(self, data):
        pass

    def unlock(self, data):
        self.queue.put({"unlock": data})

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
    """No locked entry for this input data."""


class LockedEntryException(Exception):
    """Another node is/was generating output data for this input data."""


class FailedEntryException(Exception):
    """This input data has already failed before."""


class DuplicateEntryException(Exception):
    """This input data has already been inserted before."""


class MissingEntryException(Exception):
    """This input data is missing."""
