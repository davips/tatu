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

import threading
from abc import ABC, abstractmethod
from functools import reduce
from multiprocessing import JoinableQueue, Queue
from queue import Empty
from typing import Optional, List, Union

from aiuna.content.data import Data
from tatu.sql.abs.mixin.thread import asThread
from transf.absdata import AbsData
from transf.mixin.identification import withIdentification


class Storage(asThread, withIdentification, ABC):
    """
    This class stores and recovers results from some place.
    The children classes are expected to provide storage in e.g.:
     SQLite, remote/local MongoDB, MySQL server, pickled or even CSV files.
    """

    def fetch(self, data, lock=False, recursive=True):
        lst = []
        while data is not None:
            if lock:
                if not self.lock(data.id):
                    raise Exception("Could not lock data:", id)
parei aqui
            output = self.getdata(data.id)

            if output is None or not (output.inner is None or isinstance(output.inner, str)):
                # Task complete if None or if inner is already build (e.g. coming from Pickle).
                return output

            lst.append(output)
            if not recursive:
                break
            data = output.inner

        # reconstruct lineage
        # if data is None:   <- nao embro o motivo de por isso errado aqui
        #     return None
        return reduce(lambda inner, outer: outer.replace([], inner=inner), reversed(lst))

    def hasdata(self, id, check_fields=False):
        """ check_fields: whether to assess the existence of fields, instead of just the data row"""
        return self.do(self._hasdata_, locals(), wait=True)

    def getdata(self, id):
        return self.do(self._getdata_, locals(), wait=True)

    def hasstep(self, id):
        return self.do(self._hasstep_, locals(), wait=True)

    def delete_data(self, data: Data, check_existence=True, recursive=True):
        """Remove Data object, but keeps contents of its fields (even if not used by anyone else).

        Returns list of deleted Data object uuids
        """
        ids = []
        while data:
            id = data.id
            if check_existence and not self.hasdata(id):
                raise Exception("Cannot delete, data does not exist:", id)
            if not self.do(self._delete_data, {"data": id}, wait=True):
                raise Exception("Could not delete data:", id)
            ids.append(id)
            if not recursive:
                break
            data = data.inner
        return ids

    def lock(self, id, check_existence=True):
        """Returns whether it succeeded."""
        if check_existence and self.hasdata(id):
                raise Exception("Cannot lock, data already exists:", id)
        return self.do(self._lock_, {"id": id}, wait=True)

    # ================================================================================
    #     @abstractmethod
    #     def _putdata_(self, **row):
    #         pass
    #
    #     @abstractmethod
    #     def _putcontent_(self, id, value):
    #         pass
    #
    #     @abstractmethod
    #     def _putstep_(self, id, name, path, config, dump=None):
    #         pass
    #
    #     @abstractmethod
    #     def _store_(self, data: Data, check_dup=True):
    #         pass
    #
    #     def delete(self, data: Data, check_missing=True, recursive=True):
    #         """Remove Data object, but keeps contents of the fields (even if not used by anyone else).
    #
    #         Returns list of deleted Data object uuids
    #         """
    #         uuids = []
    #         while data:
    #             if self.threaded:
    #                 self.queue.put({"delete": data.picklable, "check_missing": check_missing})
    #             else:
    #                 self._delete_(data.picklable, check_missing)
    #             uuids.append(data.uuid)
    #             if not recursive:
    #                 break
    #             data = data.inner
    #         return uuids
    #
    #     def store(self, data: Data, check_dup=True, recursive=True):
    #         """
    #         # The sequence of queries is planned to minimize traffic and CPU load,
    #         # otherwise it would suffice to just send 'insert or ignore' of dumps.
    #
    #         Parameters
    #         ----------
    #         data
    #             Data object to store.
    #         check_dup
    #             Whether to waste time checking duplicates
    #
    #         Returns
    #         -------
    #         List of inserted (or hoped to be inserted for threaded storages) Data ids (only meaningful for Data objects with inner)
    #
    #         Exception
    #         ---------
    #         DuplicateEntryException
    #         :param data:
    #         :param check_dup:
    #         :param recursive:
    #         """
    #         if data.stream:
    #             print("Cannot store Data objects containing a stream.")
    #             exit()
    #         # traverse all nested inner Data objects
    #         lst = []
    #         while data:
    #             lst.append({"store": data.picklable, "check_dup": check_dup})
    #             if not recursive:
    #                 break
    #             data = data.inner
    #             check_dup = False
    #             # We disable check_dup here because the fetch attempt to verify existence
    #             # only happened (at most) for outer data (moreover, sending of matrices is optimized, anyway).
    #             # REMINDER: Cache could not traverse fetching/storing because it doesn't know
    #             # how to process inner data, it only knows how to apply a step to the outer data as a whole.
    #         # insert from last to first due to foreign key constraint on inner->data.id
    #         for job in reversed(lst):
    #             if self.threaded:
    #                 self.queue.put(job)
    #             else:
    #                 self._store_(job["store"], check_dup)
    #         return [job["store"].id for job in reversed(lst)]
    #
    #     def fetch(self, data: Data, lock=False, recursive=True) -> AbsData:
    #         """Fetch data from DB.
    #
    #         Parameters
    #         ----------
    #         data
    #             Data object before being transformed by a pipeline.
    #         lock
    #             Whether to mark entry (input data and pipeline combination) as
    #             locked, when no data is found for the entry.
    #
    #         Returns
    #         -------
    #         Data or None
    #
    #         Exception
    #         ---------
    #         LockedEntryException, FailedEntryException
    #         :param data:
    #         :param lock:
    #         :param recursive:
    #         """
    #         # TODO: accept id string
    #         data = self.fetch_picklable(data, lock, recursive)
    #         return data and data.unpicklable
    #
    #     def fetch_picklable(self, data: Data, lock=False, recursive=True) -> Union[AbsData, Data]:
    #         data = data.picklable if isinstance(data, AbsData) else data
    #
    #     @abstractmethod
    #     def _fetch_(self, data: Data, lock=False) -> Optional[Data]:
    #         pass
    #
    #     @abstractmethod
    #     def fetch_field(self, _id):
    #         pass
    #
    #     @abstractmethod
    #     def _unlock_(self, data):
    #         pass
    #
    #     def unlock(self, data):
    #         if self.threaded:
    #             self.queue.put({"unlock": data})
    #         else:
    #             self._unlock_(data)
    #
    #     def _name_(self):
    #         return self.__class__.__name__
    #
    #     def _context_(self):
    #         return self.__class__.__module__
    #
    #     def update_remote(self, storage):
    #         """Sync, sending Data objects from this storage to the provided one."""
    #         if self.threaded:
    #             if storage.isopen:
    #                 raise Exception("A threaded storage cannot update a remote that was already opened before.")
    #             import dill
    #             self.queue.put({"update_remote": dill.dumps(storage)})
    #         else:
    #             self._update_remote_(storage)
    #
    #     def hascontent(self, id):
    #         if self.threaded:
    #             self.queue.put({"hascontent": id})
    #             return self._waited_result()
    #         else:
    #             return self._hascontent_(id)
    #
    #     def putdata(self, **row):
    #         if self.threaded:
    #             self.queue.put({"putdata": row})
    #         else:
    #             self._putdata_(**row)
    #
    #     def putcontent(self, id, value):
    #         if self.threaded:
    #             self.queue.put({"putcontent": id, "value": value})
    #         else:
    #             self._putcontent_(id, value)
    #
    #     def putstep(self, id, name, path, config, dump=None):
    #         if self.threaded:
    #             self.queue.put({"putstep": id, "name": name, "path": path, "config": config, "dump": dump})
    #         else:
    #             self._putstep_(id, name, path, config, dump)
    #
    #     @abstractmethod
    #     def _update_remote_(self, storage_func):
    #         pass
    #
    #     @abstractmethod
    #     def _fetch_children_(self, data: Data) -> List[AbsData]:
    #         pass
    #
    #     # TODO o q fazer qnd uuid nao existe? ta dizendo q nao tem filho
    #     def fetch_children(self, data: Data) -> List[AbsData]:
    #         self.queue.put({"children": data})
    #
    #         # Wait for result.
    #         output = self.outqueue.get()
    #         if not isinstance(output, list):
    #             print("type:", type(output), output)
    #             print(f"Couldn't fetch children of {id}. Quiting...")
    #             self.outqueue.task_done()
    #             exit()
    #         self.outqueue.task_done()
    #         return output
    #
    #
    # class UnlockedEntryException(Exception):
    #     """No locked entry for this input data."""
    #
    #
    # class LockedEntryException(Exception):
    #     """Another node is/was generating output data for this input data."""
    #
    #
    # class FailedEntryException(Exception):
    #     """This input data has already failed before."""
    #
    #
    # class DuplicateEntryException(Exception):
    #     """This input data has already been inserted before."""
    #
    #
    # class MissingEntryException(Exception):
    #     """This input data is missing."""

    # ================================================================================
    @abstractmethod
    def _hasdata_(self, id, check_fields=False):
        pass

    @abstractmethod
    def _getdata_(self, id, include_empty=True):
        pass

    @abstractmethod
    def _hasstep_(self, id):
        pass

    @abstractmethod
    def _delete_data(self, id):
        """Return whether it succeeded."""
        pass

    @abstractmethod
    def _lock_(self, id):
        pass
