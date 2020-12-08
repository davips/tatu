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
from abc import ABC, abstractmethod

from aiuna.compression import unpack, fpack
from aiuna.content.data import Data
from aiuna.content.root import Root
from aiuna.history import History
from cruipto.uuid import UUID
from akangatu.linalghelper import islazy
from tatu.abs.mixin.thread import asThread
from tatu.abs.storage import Storage, DuplicateEntryException
from akangatu.transf.step import Step


class StorageInterface(asThread, Storage, ABC):
    # TODO (strict) fetch by uuid
    # TODO (strict) store
    def fetch(self, data, lock=False, lazy=True):  # , recursive=True):
        """Fetch the data object fields on-demand.
         data: uuid string or a (probably still not fully evaluated) Data object."""
        data_id = data if isinstance(data, str) else data.id
        # lst = []
        print("Fetching...", data_id)
        # while True:
        ret = self.getdata(data_id, include_empty=False)
        if ret is None:
            if lock and not self.lock(data_id):
                raise Exception("Could not lock data:", data_id)
            return

        fields = {}
        for field, fid in ret["uuids"].items():
            if field == "stream":
                fields[field] = lambda: self.getcontent(fid)  # TODO getstream() as iterator of lazyfetches
            elif field == "changed":
                fields[field] = unpack(self.getcontent(fid)) if isinstance(data, str) else data.changed
            elif field not in ["inner"] and (isinstance(data, str) or field in data.changed):
                if lazy:
                    fields[field] = (lambda fid_: lambda: unpack(self.getcontent(fid_)))(fid)
                else:
                    fields[field] = unpack(self.getcontent(fid))
            if lazy and field != "changed":
                # Call each lambda by a friendly name.
                fields[field].__name__ = "_" + fields[field].__name__ + "_from_storage_" + self.id

        if isinstance(data, str):
            if lazy:
                raise Exception("lazy ainda não retorna histórico para data dado por uuid-string")  # <-- TODO
            history = self.fetchhistory(data)
        else:
            history = data.history
        print("> > > > > > > > > fetched?", data_id, ret)
        return Data(UUID(data_id), {k: UUID(v) for k, v in ret["uuids"].items()}, history, **fields)

    def store(self, data: Data, unlock=False, ignoredup=False, lazy=False):
        """Store all Data object fields as soon as one of them is evaluated.

        # The sequence of queries is planned to minimize traffic and CPU load,
        # otherwise it would suffice to just send 'insert or ignore' of dumps.

        Parameters
        ----------
        data
            Data object to store.
        ignore_dup
            Whether to send the query anyway, ignoring errors due to already existent registries.

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
                        v = v()  # Cannot call data[k] due to infinite loop.
                    held_data.field_funcs_m[
                        k] = v  # The old value may not be lazy, but the new one can be due to this very lazystore.
                    id = held_data.uuids[k].id
                    if id in puts:
                        if k != "inner":
                            self.putcontent(id, fpack(held_data, k))
                rows = [(held_data.id, fname, fuuid.id) for fname, fuuid in held_data.uuids.items() if fname != "inner"]
                self.putfields(rows)
                return held_data.field_funcs_m[name]

            return lamb

        while True:
            # Fields.
            cids = [u.id for u in data.uuids.values()]
            missing = [cid for cid in cids if cid not in self.hascontent(cids)]
            if lazy:
                field_funcs_copy = data.field_funcs_m.copy()
                for field in data.field_funcs_m:
                    data.field_funcs_m[field] = func(data, field, field_funcs_copy, missing)
                    data.field_funcs_m[field].__name__ = "_" + data.uuids[field].id + "_to_storage_" + self.id
            else:
                for k, v in data.items():
                    id = data.uuids[k].id
                    if id in missing:
                        content = v.id.encode() if k == "inner" else fpack(data, k)
                        self.putcontent(id, content)

            lst.append(data)
            if not data.hasinner:
                break
            data = data.inner

        for i, d in reversed(list(enumerate(lst))):
            if i == 0 and unlock:
                self.unlock(d.id)

            # History.
            datauuid, ok = Root.uuid, False
            for step in list(d.history):
                if not self.hasstep(step.id):
                    self.storestep(step)

                parent_uuid = datauuid
                datauuid = datauuid * step.uuid
                # Here, locked=NULL means 'placeholder', which can be updated in the future if the same data happens to be truly stored.
                # We assume it is faster to do a single insertignore than select+insert, hence ignoredup=True here.
                self.putdata(datauuid.id, step.id, None, False, parent_uuid.id, None, ignoredup=True)

            if not lazy:
                self.putfields([(d.id, fname, fuuid.id) for fname, fuuid in d.uuids.items()])
        return lst[0]

    def fetchhistory(self, id):
        print("Fetching history...", id)
        steps = []
        while True:
            ret = self.getdata(id)
            steps.append(self.fetchstep(ret["step"]))
            id = ret["parent"]
            if id == UUID().id:
                break
        history = History(steps[-1])
        for step in reversed(steps[:-1]):
            history <<= step
        print("   ...history fetched!", id)
        return history

    def fetchstep(self, id):
        """Return a Step object or None."""
        print("Fetching step...", id)
        r = self.getstep(id)
        print("       ...fetched step?", id, bool(r), r)
        if r is None:
            return None
        r["config"] = json.loads(r["config"])
        return Step.fromdict({"id": id, "desc": r})

    def hasdata(self, id, include_empty=False):
        """ include_empty: whether to assess the existence of fields, instead of just the data row"""
        return self.do(self._hasdata_, locals(), wait=True)

    def getdata(self, id, include_empty=True):
        """Return a info for a Data object."""
        print("Getting data...", id)
        r = self.do(self._getdata_, locals(), wait=True)
        print("       ...got data?", id, bool(r))
        print(r)
        return r

    def hasstep(self, id):
        return self.do(self._hasstep_, locals(), wait=True)

    def getstep(self, id):
        """Return info for a Step object."""
        print("Getting step...", id)
        r = self.do(self._getstep_, locals(), wait=True)
        print("       ...got step?", id, bool(r))
        return r

    # REMINDER we check missing fields through hascontent()
    def getfields(self, id):
        """Return fields and content for a Data object."""
        print("Getting fields...", id)
        r = self.do(self._getfields_, locals(), wait=True)
        print("       ...got fields?", id, bool(r))
        return r

    def getcontent(self, id):
        """Return content."""
        print("Getting content...", id)
        r = self.do(self._getcontent_, locals(), wait=True)
        print("       ...got content?", id, bool(r))
        return r

    def hascontent(self, ids):
        return self.do(self._hascontent_, {"ids": ids}, wait=True)

    def removedata(self, data: Data, check_existence=True, recursive=True):
        """Remove Data object, but keeps contents of its fields (even if not used by anyone else).

        Returns list of deleted Data object uuids
        """
        print("Deleting...", data.id)
        ids = []
        while True:
            id = data.id
            self.deldata(id)
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

    def deldata(self, id, check_success=True):
        """Return whether it succeeded."""
        print("Deleting data...", id)
        r = self.do(self._deldata_, {"id": id}, wait=True)
        if check_success and not r:
            raise Exception("Cannot unlock, data does not exist:", id)
        print("    ...deleted?", id, bool(r))
        return r

    def unlock(self, id, check_success=True):
        """Return whether it succeeded."""
        print("Unlocking...", id)
        r = self.do(self._unlock_, {"id": id}, wait=True)
        if check_success and not r:
            raise Exception("Cannot unlock, data does not exist:", id)
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

    def storestep(self, step, dump=None, ignoredup=False):
        """Return whether it succeeded."""
        return self.putstep(step.id, step.name, step.context, step.config_json, dump and step.dump, ignoredup)

    def putstep(self, id, name, path, config, dump=None, ignoredup=False):
        """Return whether it succeeded."""
        print("Putting step...", id)
        r = self.do(self._putstep_, locals(), wait=True)
        print("    ...put step?", id, bool(r))
        return r

    # ================================================================================
    #     @abstractmethod
    #     def _putdata_(self, **row):
    #     pass
    #
    #     @abstractmethod
    #     def _putcontent_(self, id, value):
    #     pass
    #
    #     @abstractmethod
    #     def _putstep_(self, id, name, path, config, dump=None):
    #     pass
    #
    #     @abstractmethod
    #     def _store_(self, data: Data, check_dup=True):
    #     pass
    #
    #     def delete(self, data: Data, check_missing=True, recursive=True):
    #     """Remove Data object, but keeps contents of the fields (even if not used by anyone else).
    #
    #     Returns list of deleted Data object uuids
    #     """
    #     uuids = []
    #     while data:
    #         if self.threaded:
    #             self.queue.put({"delete": data.picklable, "check_missing": check_missing})
    #         else:
    #             self._delete_(data.picklable, check_missing)
    #         uuids.append(data.uuid)
    #         if not recursive:
    #             break
    #         data = data.inner
    #     return uuids
    #
    #     def store(self, data: Data, check_dup=True, recursive=True):
    #     """
    #     # The sequence of queries is planned to minimize traffic and CPU load,
    #     # otherwise it would suffice to just send 'insert or ignore' of dumps.
    #
    #     Parameters
    #     ----------
    #     data
    #         Data object to store.
    #     check_dup
    #         Whether to waste time checking duplicates
    #
    #     Returns
    #     -------
    #     List of inserted (or hoped to be inserted for threaded storages) Data ids (only meaningful for Data objects with inner)
    #
    #     Exception
    #     ---------
    #     DuplicateEntryException
    #     :param data:
    #     :param check_dup:
    #     :param recursive:
    #     """
    #     if data.stream:
    #         print("Cannot store Data objects containing a stream.")
    #         exit()
    #     # traverse all nested inner Data objects
    #     lst = []
    #     while data:
    #         lst.append({"store": data.picklable, "check_dup": check_dup})
    #         if not recursive:
    #             break
    #         data = data.inner
    #         check_dup = False
    #         # We disable check_dup here because the fetch attempt to verify existence
    #         # only happened (at most) for outer data (moreover, sending of matrices is optimized, anyway).
    #         # REMINDER: Cache could not traverse fetching/storing because it doesn't know
    #         # how to process inner data, it only knows how to apply a step to the outer data as a whole.
    #     # insert from last to first due to foreign key constraint on inner->data.id
    #     for job in reversed(lst):
    #         if self.threaded:
    #             self.queue.put(job)
    #         else:
    #             self._store_(job["store"], check_dup)
    #     return [job["store"].id for job in reversed(lst)]
    #
    #     def fetch(self, data: Data, lock=False, recursive=True) -> AbsData:
    #     """Fetch data from DB.
    #
    #     Parameters
    #     ----------
    #     data
    #         Data object before being transformed by a pipeline.
    #     lock
    #         Whether to mark entry (input data and pipeline combination) as
    #         locked, when no data is found for the entry.
    #
    #     Returns
    #     -------
    #     Data or None
    #
    #     Exception
    #     ---------
    #     LockedEntryException, FailedEntryException
    #     :param data:
    #     :param lock:
    #     :param recursive:
    #     """
    #     # TODO: accept id string
    #     data = self.fetch_picklable(data, lock, recursive)
    #     return data and data.unpicklable
    #
    #     def fetch_picklable(self, data: Data, lock=False, recursive=True) -> Union[AbsData, Data]:
    #     data = data.picklable if isinstance(data, AbsData) else data
    #
    #     @abstractmethod
    #     def _fetch_(self, data: Data, lock=False) -> Optional[Data]:
    #     pass
    #
    #     @abstractmethod
    #     def fetch_field(self, _id):
    #     pass
    #
    #     @abstractmethod
    #     def _unlock_(self, data):
    #     pass
    #
    #     def unlock(self, data):
    #     if self.threaded:
    #         self.queue.put({"unlock": data})
    #     else:
    #         self._unlock_(data)
    #
    #     def update_remote(self, storage):
    #     """Sync, sending Data objects from this storage to the provided one."""
    #     if self.threaded:
    #         if storage.isopen:
    #             raise Exception("A threaded storage cannot update a remote that was already opened before.")
    #         import dill
    #         self.queue.put({"update_remote": dill.dumps(storage)})
    #     else:
    #         self._update_remote_(storage)
    #
    #     def hascontent(self, id):
    #     if self.threaded:
    #         self.queue.put({"hascontent": id})
    #         return self._waited_result()
    #     else:
    #         return self._hascontent_(id)
    #
    #     def putdata(self, **row):
    #     if self.threaded:
    #         self.queue.put({"putdata": row})
    #     else:
    #         self._putdata_(**row)
    #
    #     def putcontent(self, id, value):
    #     if self.threaded:
    #         self.queue.put({"putcontent": id, "value": value})
    #     else:
    #         self._putcontent_(id, value)
    #
    #     def putstep(self, id, name, path, config, dump=None):
    #     if self.threaded:
    #         self.queue.put({"putstep": id, "name": name, "path": path, "config": config, "dump": dump})
    #     else:
    #         self._putstep_(id, name, path, config, dump)
    #
    #     @abstractmethod
    #     def _update_remote_(self, storage_func):
    #     pass
    #
    #     @abstractmethod
    #     def _fetch_children_(self, data: Data) -> List[AbsData]:
    #     pass
    #
    #     # TODO o q fazer qnd uuid nao existe? ta dizendo q nao tem filho
    #     def fetch_children(self, data: Data) -> List[AbsData]:
    #     self.queue.put({"children": data})
    #
    #     # Wait for result.
    #     output = self.outqueue.get()
    #     if not isinstance(output, list):
    #         print("type:", type(output), output)
    #         print(f"Couldn't fetch children of {id}. Quiting...")
    #         self.outqueue.task_done()
    #         exit()
    #     self.outqueue.task_done()
    #     return output
    #
    #
    #
    #
    # class FailedEntryException(Exception):
    #     """This input data has already failed before."""
    #

    # ================================================================================
    @abstractmethod
    def _hasdata_(self, id, include_empty):
        pass

    @abstractmethod
    def _getdata_(self, id, include_empty):
        pass

    @abstractmethod
    def _hasstep_(self, id):
        pass

    @abstractmethod
    def _getstep_(self, id):
        pass

    @abstractmethod
    def _getfields_(self, id):
        pass

    @abstractmethod
    def _hascontent_(self, ids):
        pass

    @abstractmethod
    def _getcontent_(self, id):
        pass

    @abstractmethod
    def _lock_(self, id):
        pass

    @abstractmethod
    def _unlock_(self, id):
        pass

    @abstractmethod
    def _putdata_(self, id, step, inn, stream, parent, locked, ignoredup):
        pass

    @abstractmethod
    def _putfields_(self, rows, ignoredup):
        pass

    @abstractmethod
    def _putcontent_(self, id, value, ignoredup):
        pass

    @abstractmethod
    def _putstep_(self, id, name, path, config, dump, ignoredup):
        pass

    @abstractmethod
    def _deldata_(self, id):
        pass
