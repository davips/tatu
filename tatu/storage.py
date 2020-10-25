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

from abc import ABC, abstractmethod

from aiuna.compression import unpack, pack
from aiuna.content.data import Data
from aiuna.content.root import Root
from aiuna.history import History
from linalghelper import islazy
from tatu.sql.abs.mixin.thread import asThread
from transf.mixin.identification import withIdentification
from transf.noop import NoOp
from transf.step import Step


class Storage(asThread, withIdentification, ABC):
    """
    This class stores and recovers results from some place.
    The children classes are expected to provide storage in e.g.:
     SQLite, remote/local MongoDB, MySQL server, pickled or even CSV files.
    """

    # TODO (strict) fetch by uuid
    # TODO (strict) store
    def lazyfetch(self, data, lock=False):  # , recursive=True):
        data_id = data if isinstance(data, str) else data
        # lst = []
        print("Fetching...", data_id)
        # while True:
        ret = self.getdata(data_id)
        if ret is None:
            if lock and not self.lock(data_id):
                raise Exception("Could not lock data:", data_id)
            return

        # Build a lazy Data object  TODO essa parte só serve qnd temos apenas o data uuid
        # step_func = lambda: self.getstep(ret["step"])
        # step_func.name = f"_{ret['step']}_from_storage_" + self.id
        #
        # step_funcs = lambda: self.gethistory(ret["parent"])
        # step_funcs.name = f"_history_from_storage_" + self.id

        fields = {}
        for field in ret["uuids"]:
            if field == "inner":
                fields[field] = lambda: self.lazyfetch(ret["inner"])
            elif field == "stream":
                fields[field] = lambda: self.getstream(ret["stream"])  # TODO getstream() as iterator of lazyfetches
            else:
                fields[field] = (lambda name: lambda: unpack(self.getfields(data_id, [name])[0]))(field)

            if field == "changed":
                fields[field] = unpack(ret["changed"])
            else:
                # Call each lambda by a friendly name.
                fields[field].__name__ = "_" + fields[field].__name__ + "_from_storage_" + self.id

        if isinstance(data, str):
            history = History(NoOp())
        else:
            history = data.history
        return Data(data_id, ret["uuids"], history, **fields)

    def lazystore(self, data: Data, ignoredup=False):
        """
        # The sequence of queries is planned to minimize traffic and CPU load,
        # otherwise it would suffice to just send 'insert or ignore' of dumps.

        Parameters
        ----------
        data
            Data object to store.
        check_dup
            Whether to waste time checking duplicates

        Returns
        -------
        List of inserted (or hoped to be inserted for threaded storages) Data ids (only meaningful for Data objects with inner)

        Exception
        ---------
        DuplicateEntryException
        :param ignoredup:
        :param data:
        :param check_dup:
        :param recursive:
        """
        if not ignoredup and self.hasdata(data.id):
            raise DuplicateEntryException(data.id)

        # Embed lazy storers inside the Data object.
        lst = []

        def func(held_data, name, field_funcs, puts):
            def lamb():
                for k, v in field_funcs.items():
                    if islazy(v):
                        v = v()
                    held_data.field_funcs_m[
                        k] = v  # The old value may not be lazy, but the new one can be due to this very lazystore.
                    id = held_data.uuids[k].id
                    if id in puts:
                        self.putcontent(id, pack(v))
                self.putfields([(held_data.id, fname, fuuid.id) for fname, fuuid in held_data.uuids.items()])
                return held_data.field_funcs_m[name]

            return lamb

        while True:
            # Fields.
            field_funcs_copy = data.field_funcs_m.copy()
            missing = self.missing([u.id for u in data.uuids.values()])
            for field in data.field_funcs_m:
                data.field_funcs_m[field] = func(data, field, field_funcs_copy, missing)
                data.field_funcs_m[field].__name__ = "_" + data.uuids[field].id + "_to_storage_" + self.id
                print("!!!!!!!!!!!!!!!!    missing", field, data.field_funcs_m[field])

            lst.append(data)
            if not data.hasinner:
                break
            data = data.inner

        for i, d in reversed(list(enumerate(lst))):
            if i == 0:
                self.unlock(d.id)

            # History.
            datauuid, ok = Root.uuid, False
            for step in d.history:
                if not self.hasstep(step.id):
                    self.putstep(step.id, step.name, step.context, step.config_json)

                parent_uuid = datauuid
                datauuid = datauuid * step.uuid
                # Here, locked=NULL means 'placeholder', which can be updated in the future if the same data happens to be truly stored.
                # We assume it is faster to do a single insertignore than select+insert, hence ignoredup=True here.
                self.putdata(datauuid.id, step.id, None, False, parent_uuid.id, None, ignoredup=True)

        return lst[0]

    def fetchstep(self, id):
        """Return a Step object."""
        print("Fetching step...", id)
        r = self.getstep(id)
        print("       ...fetched step?", id, bool(r))
        return Step.fromdict({"id": id, "desc": r})

    def hasdata(self, id, include_empty=False):
        """ include_empty: whether to assess the existence of fields, instead of just the data row"""
        return self.do(self._hasdata_, locals(), wait=True)

    def getdata(self, id):
        """Return a info for a Data object."""
        print("Getting...", id)
        r = self.do(self._getdata_, locals(), wait=True)
        print("       ...got?", id, bool(r))
        return r

    def getstep(self, id):
        """Return info for a Step object."""
        print("Getting step...", id)
        r = self.do(self._getstep_, locals(), wait=True)
        print("       ...got step?", id, bool(r))
        return r

    def getfields(self, id, names):
        """Return info for a field."""
        print("Getting step...", id)
        r = self.do(self._getfields_, locals(), wait=True)
        print("       ...got step?", id, bool(r))
        return r

    def hasstep(self, id):
        return self.do(self._hasstep_, locals(), wait=True)

    def hasfield(self, id):
        return self.do(self._hasfield_, locals(), wait=True)

    def missing(self, ids):
        """Return which contents are missing from the given list."""
        return self.do(self._missing_, locals(), wait=True)

    def delete_data(self, data: Data, check_existence=True, recursive=True):
        """Remove Data object, but keeps contents of its fields (even if not used by anyone else).

        Returns list of deleted Data object uuids
        """
        print("Deleting...", data.id)
        ids = []
        while True:
            id = data.id
            if not self.do(self._delete_data, {"id": id}, wait=True) and check_existence:
                raise Exception("Cannot delete, data does not exist:", id)
            ids.append(id)
            if not recursive or not data.hasinner:
                break
            data = data.inner
        print("         ...deleted!", ids)
        return ids

    def lock(self, id, check_existence=True):
        """Return whether it succeeded."""
        print("Locking...", id)
        if check_existence and self.hasdata(id):
            raise Exception("Cannot lock, data already exists:", id)
        r = self.do(self._lock_, {"id": id}, wait=True)
        print("    ...locked?", id, bool(r))
        return r

    def unlock(self, id, check_success=True):
        """Return whether it succeeded."""
        print("Unlocking...", id)
        r = self.do(self._unlock_, {"id": id}, wait=True)
        if check_success and not r:
            raise Exception("Cannot lock, data does not exist:", id)
        print("    ...unlocked?", id, bool(r))
        return r

    def putdata(self, id, step, inn, stream, parent, locked, ignoredup=False):
        """Return whether it succeeded."""
        print("Putting data...", id)
        r = self.do(self._putdata_, locals(), wait=True)
        print("    ...putdata?", id, bool(r))
        return r

    def putcontent(self, id, value, ignoredup=False):
        """Return whether it succeeded."""
        print("Putting content...", id)
        r = self.do(self._putcontent_, locals(), wait=True)
        print("    ...putcontent?", id, bool(r))
        return r

    def putfields(self, rows, ignoredup=False):
        """Return whether it succeeded."""
        print("Putting fields...", rows)
        r = self.do(self._putfields_, locals(), wait=True)
        print("    ...put fields?", bool(r), rows)
        return r

    def putstep(self, id, name, path, config, dump=None, ignoredup=False):
        """Return whether it succeeded."""
        print("Putting step...", id)
        r = self.do(self._putstep_, locals(), wait=True)
        print("    ...put step?", id, bool(r))
        return r

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
    #
    #
    # class FailedEntryException(Exception):
    #     """This input data has already failed before."""
    #

    # ================================================================================
    @abstractmethod
    def _hasdata_(self, id, include_empty=True):
        pass

    @abstractmethod
    def _getdata_(self, id, include_empty=True):
        pass

    @abstractmethod
    def _hasstep_(self, id):
        pass

    @abstractmethod
    def _hasfield_(self, id):
        pass

    @abstractmethod
    def _delete_data(self, id):
        """Return whether it succeeded."""
        pass

    @abstractmethod
    def _lock_(self, id):
        pass

    @abstractmethod
    def _unlock_(self, id):
        pass

    @abstractmethod
    def _putdata_(self, id, step, inn, stream, parent, locked, ignoredup=False):
        pass

    @abstractmethod
    def _putfields_(self, rows, ignoredup=False):
        pass

    @abstractmethod
    def _putcontent_(self, id, value, ignoredup=False):
        pass

    @abstractmethod
    def _putstep_(self, id, name, path, config, dump=None, ignoredup=False):
        pass

    @abstractmethod
    def _getstep_(self, id):
        pass

    @abstractmethod
    def _getfields_(self, id, names):
        pass

    @abstractmethod
    def _missing_(self, ids):
        pass

    def _name_(self):
        return self.__class__.__name__

    def _context_(self):
        return self.__class__.__module__


class UnlockedEntryException(Exception):
    """No locked entry for this input data."""


class LockedEntryException(Exception):
    """Another node is/was generating output data for this input data."""


class DuplicateEntryException(Exception):
    """This input data has already been inserted before."""


class MissingEntryException(Exception):
    """This input data is missing."""
