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
from garoupa.uuid import UUID
from akangatu.linalghelper import islazy
from tatu.abs.mixin.thread import asThread
from tatu.abs.storage import Storage, DuplicateEntryException, LockedEntryException
from akangatu.transf.step import Step


class StorageInterface(asThread, Storage, ABC):
    # TODO (strict) fetch by uuid
    # TODO (strict) store
    def fetch(self, data, lock=False, lazy=True):
        # , recursive=True):   # TODO: pensar no include_empty=False se faz sentido
        """Fetch the data object fields on-demand.
         data: uuid string or a (probably still not fully evaluated) Data object."""
        data_id = data if isinstance(data, str) else data.id
        # lst = []
        # print("LOGGING:::  Fetching...", data_id)
        # while True:
        ret = self.getdata(data_id, include_empty=True)

        if ret is None or not ret["uuids"]:
            # REMINDER: check_existence false porque pode ser um data vazio
            # [e é para o Cache funcionar mesmo que ele tenha sido interrompido]
            if lock and not self.lock(data_id, check_existence=False):
                raise Exception("Could not lock data:", data_id)
            return

        fields = {} if isinstance(data, str) else data.field_funcs_m
        for field, fid in list(ret["uuids"].items()) + [("stream", None)]:
            if field == "inner":
                fields[field] = lambda: self.fetch(fid)
            elif field == "stream":
                fields[field] = lambda: self.fetchstream(data_id, lazy)
            elif field == "changed":
                fields[field] = unpack(self.getcontent(fid)) if isinstance(data, str) else data.changed
            elif field not in ["inner"] and (isinstance(data, str) or field in data.changed):
                if lazy:
                    fields[field] = (lambda fid_: lambda: unpack(self.getcontent(fid_)))(fid)
                else:
                    fields[field] = unpack(self.getcontent(fid))

            # Call each lambda by a friendly name.
            if lazy and field != "changed":
                fields[field].__name__ = "_" + fields[field].__name__ + "_from_storage_" + self.id

        if isinstance(data, str):
            if lazy:
                raise Exception("lazy ainda não retorna histórico para data dado por uuid-string")  # <-- TODO
            history = self.fetchhistory(data)
        else:
            history = data.history
        # print("LOGGING:::  > > > > > > > > > fetched?", data_id, ret)
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
                            # TODO/REMINDER: exceptionally two datasets can have some equal contents, like Xd;
                            #   so we send it again while the hash is not based on content
                            self.putcontent(id, fpack(held_data, k), ignoredup=True)
                rows = [(held_data.id, fname, fuuid.id) for fname, fuuid in held_data.uuids.items() if fname != "inner"]
                self.putfields(rows)
                return held_data.field_funcs_m[name]

            return lamb

        streams = {}
        while True:
            # Fields.
            cids = [u.id for u in data.uuids.values()]
            missing = [cid for cid in cids if cid not in self.hascontent(cids)]
            if lazy:
                # All fields will be evaluated at the time one of them is called.
                field_funcs_copy = data.field_funcs_m.copy()
                for field in data.field_funcs_m:
                    if field == "stream":
                        # data.field_funcs_m["stream"] = map(
                        #     lambda d: self.store(d, unlock=True, lazy=False),
                        #     data.field_funcs_m["stream"]
                        # )
                        raise Exception("A lazy storage cannot handle streams for now.")
                    data.field_funcs_m[field] = func(data, field, field_funcs_copy, missing)
                    data.field_funcs_m[field].__name__ = "_" + data.uuids[field].id + "_to_storage_" + self.id
            else:
                for k, v in data.items():
                    if k == "stream":
                        # Consume stream, to be stored after putdata().
                        streams[data.id] = list(data.stream)
                    else:
                        id = data.uuids[k].id
                        if id in missing and k != "inner":
                            content = fpack(data, k)
                            # TODO/REMINDER: exceptionally two datasets can have some equal contents, like Xd;
                            #   so we send it again while the hash is not based on content
                            self.putcontent(id, content, ignoredup=True)

            lst.append(data)
            if not data.hasinner:
                break
            data = data.inner

        for i, d in reversed(list(enumerate(lst))):
            if i == 0 and unlock:
                self.unlock(d.id)

            # History.
            ancestor_duuid = Root.uuid
            for step in list(d.history):
                # print("LOGGING:::  ssssssssSSSSSSSSSSSS", step.id)
                if not self.hasstep(step.id):
                    self.storestep(step)

                parent_uuid = ancestor_duuid
                ancestor_duuid = ancestor_duuid * step.uuid
                # Here, locked=NULL means 'placeholder', which can be updated in the future if the same data happens to be truly stored.
                # We assume it is faster to do a single insertignore than select+insert, hence ignoredup=True here.
                if ancestor_duuid == d.uuid:
                    hasstream, inner = d.hasstream, d.inner if d.hasinner else None
                else:
                    hasstream, inner = False, None
                self.putdata(ancestor_duuid.id, step.id, inner, hasstream, parent_uuid.id, None, ignoredup=True)
                # TODO: adopt logging    print(datauuid, 3333333333333333333333333333333333333333)

            if lazy:
                # TODO: adopt logging    print(d.id, 7777777777777777777777777777777)
                pass
            else:
                if d.id in streams:
                    rows = []
                    for pos, streamed_data in enumerate(streams[d.id]):
                        self.store(streamed_data, ignoredup=True)
                        rows.append((d.id, str(pos), streamed_data.id))
                    self.putstream(rows)

                    # Return a new iterator in the place of the original stream.
                    d.field_funcs_m["stream"] = iter(streams[d.id])
                self.putfields([(d.id, fname, fuuid.id) for fname, fuuid in d.uuids.items()], ignoredup=True)
        return lst[0]

    def fetchhistory(self, id):
        # print("LOGGING:::  Fetching history...", id)
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
        # print("LOGGING:::     ...history fetched!", id)
        return history

    def fetchstream(self, dataid, lazy=True):
        """Return a stream iterator or None."""
        # print("LOGGING:::  Fetching step...", id)
        rows = self.getstream(dataid)
        # print("LOGGING:::         ...fetched step?", id, bool(r), r)
        if rows is None:
            return None
        for r in rows:
            yield self.fetch(r["chunk"], lazy=lazy)

    def fetchstep(self, id):
        """Return a Step object or None."""
        # print("LOGGING:::  Fetching step...", id)
        r = self.getstep(id)
        # print("LOGGING:::         ...fetched step?", id, bool(r), r)
        if r is None:
            return None
        r["config"] = json.loads(r["config"])
        return Step.fromdict({"id": id, "desc": r})

    def hasdata(self, id, include_empty=False):
        """ include_empty: whether to assess the existence of fields, instead of just the data row"""
        return self.do(self._hasdata_, locals(), wait=True)

    def getdata(self, id, include_empty=True):
        """Return a info for a Data object."""
        # print("LOGGING:::  Getting data...", id)
        r = self.do(self._getdata_, locals(), wait=True)
        # print("LOGGING:::         ...got data?", id, bool(r))
        # TODO: adopt logging    print(r)
        return r

    def hasstream(self, data):
        """Verify existence of a stream for the given dataid."""
        return self.do(self._hasstream_, locals(), wait=True)

    def getstream(self, data):
        """Return rows with info for the stream of a given Data id."""
        # print("LOGGING:::  Getting data...", id)
        r = self.do(self._getstream_, locals(), wait=True)
        # print("LOGGING:::         ...got data?", id, bool(r))
        # TODO: adopt logging    print(r)
        return r

    def hasstep(self, id):
        return self.do(self._hasstep_, locals(), wait=True)

    def getstep(self, id):
        """Return info for a Step object."""
        # print("LOGGING:::  Getting step...", id)
        r = self.do(self._getstep_, locals(), wait=True)
        # print("LOGGING:::         ...got step?", id, bool(r))
        return r

    # REMINDER we check missing fields through hascontent()
    def getfields(self, id):
        """Return fields and content for a Data object."""
        # print("LOGGING:::  Getting fields...", id)
        r = self.do(self._getfields_, locals(), wait=True)
        # print("LOGGING:::         ...got fields?", id, bool(r))
        return r

    def getcontent(self, id):
        """Return content."""
        # print("LOGGING:::  Getting content...", id)
        r = self.do(self._getcontent_, locals(), wait=True)
        # print("LOGGING:::         ...got content?", id, bool(r))
        return r

    def hascontent(self, ids):
        return self.do(self._hascontent_, {"ids": ids}, wait=True)

    def removedata(self, data: Data, check_existence=True, recursive=True):
        """Remove Data object, but keeps contents of its fields (even if not used by anyone else).

        Returns list of deleted Data object uuids
        """
        # print("LOGGING:::  Deleting...", data.id)
        ids = []
        while True:
            id = data.id
            self.deldata(id)
            ids.append(id)
            if not recursive or not data.hasinner:
                break
            data = data.inner
        # print("LOGGING:::           ...deleted!", ids)
        return ids

    def lock(self, id, check_existence=True):
        """Return whether it succeeded."""
        # print("LOGGING:::  Locking...", id)
        if check_existence and self.hasdata(id):
            raise Exception("Cannot lock, data already exists:", id)
        r = self.do(self._lock_, {"id": id}, wait=True)
        # print("LOGGING:::      ...locked?", id, bool(r))
        return r

    def deldata(self, id, check_success=True):
        """Return whether it succeeded."""
        # print("LOGGING:::  Deleting data...", id)
        r = self.do(self._deldata_, {"id": id}, wait=True)
        if check_success and not r:
            raise Exception("Cannot unlock, data does not exist:", id)
        # print("LOGGING:::      ...deleted?", id, bool(r))
        return r

    def unlock(self, id, check_success=True):
        """Return whether it succeeded."""
        # print("LOGGING:::  Unlocking...", id)
        r = self.do(self._unlock_, {"id": id}, wait=True)
        if check_success and not r:
            raise Exception("Cannot unlock, data does not exist:", id)
        # print("LOGGING:::      ...unlocked?", id, bool(r))
        return r

    def putdata(self, id, step, inn, stream, parent, locked, ignoredup=False):
        """Return whether it succeeded."""
        # print("LOGGING:::  Putting data...", id)
        r = self.do(self._putdata_, locals(), wait=True)
        # print("LOGGING:::      ...putdata?", id, bool(r))
        return r

    def putstream(self, rows, ignoredup=False):
        """Return whether it succeeded."""
        # print("LOGGING:::  Putting stream...", id)
        r = self.do(self._putstream_, locals(), wait=True)
        # print("LOGGING:::      ...putstream?", id, bool(r))
        return r

    def putcontent(self, id, value, ignoredup=False):
        """Return whether it succeeded."""
        # print("LOGGING:::  Putting content...", id)
        r = self.do(self._putcontent_, locals(), wait=True)
        # print("LOGGING:::      ...putcontent?", id, bool(r))
        return r

    def putfields(self, rows, ignoredup=False):
        """Return number of fields put."""
        # print("LOGGING:::  Putting fields...", rows)
        r = self.do(self._putfields_, locals(), wait=True)
        # print("LOGGING:::      ...put fields?", r, rows)
        return r

    def storestep(self, step, dump=None, ignoredup=False):
        """Return whether it succeeded."""
        return self.putstep(step.id, step.name, step.context, step.config_json, dump and step.dump, ignoredup)

    def putstep(self, id, name, path, config, dump=None, ignoredup=False):
        """Return whether it succeeded."""
        # print("LOGGING:::  Putting step...", id)
        r = self.do(self._putstep_, locals(), wait=True)
        # print("LOGGING:::      ...put step?", id, bool(r))
        return r

    @abstractmethod
    def _hasdata_(self, id, include_empty):
        pass

    @abstractmethod
    def _hasstream_(self, data):
        pass

    @abstractmethod
    def _getdata_(self, id, include_empty):
        pass

    @abstractmethod
    def _getstream_(self, data):
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
    def _putstream_(self, rows, ignoredup):
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
