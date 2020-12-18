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
from abc import ABC, abstractmethod

from aiuna.content.data import Data
from akangatu.transf.mixin.identification import withIdentification


class Storage(withIdentification, ABC):
    """
    Store and recover results from some place.
    The children classes are expected to provide storage in e.g.:
     SQLite, remote/local MongoDB, MySQL server, pickled or even CSV files.
    """

    @abstractmethod
    def fetch(self, data, lock=False, lazy=True):  # , recursive=True):
        """Fetch the Data object fields on-demand if lazy=True.
         data: uuid string or a (probably still not fully evaluated) Data object."""

    @abstractmethod
    def store(self, data: Data, unlock=False, ignoredup=False, lazy=False):
        """Store all Data object fields as soon as one of them is evaluated.

        # The sequence of queries is planned to minimize traffic and CPU load,
        # otherwise it would suffice to just send 'insert or ignore' of dumps.

        Parameters
        ----------
        data
            Data object to store.
        ignoredup
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

    @abstractmethod
    def fetchhistory(self, id):
        """Fetch History for the given Data object id."""

    @abstractmethod
    def fetchstep(self, id):
        """Return a Step object."""

    @abstractmethod
    def hasdata(self, id, include_empty=False):
        """ include_empty: whether to assess the existence of fields, instead of just the data row"""

    @abstractmethod
    def hasstream(self, data):
        """Verify existence of stream for a given dataid."""

    @abstractmethod
    def getdata(self, id, include_empty=True):
        """Return info for a Data object."""

    @abstractmethod
    def getstream(self, data):
        """Return rows with info for the stream of a given Data id."""

    @abstractmethod
    def hasstep(self, id):
        """Does the step with 'id' exist?"""

    @abstractmethod
    def getstep(self, id):
        """Return info for a Step object."""

    @abstractmethod
    def getfields(self, id):
        """Return fields and content for a Data object."""

    @abstractmethod
    def getcontent(self, id):
        """Return content with the given id."""

    @abstractmethod
    def hascontent(self, ids):
        """Does the content with 'id' exist?"""

    @abstractmethod
    def removedata(self, data: Data, check_existence=True, recursive=True):
        """Remove Data object, but keeps contents of its fields (even if not used by anyone else).

        Returns list of deleted Data object uuids
        """

    @abstractmethod
    def lock(self, id, check_existence=True):
        """Return whether it succeeded."""

    @abstractmethod
    def deldata(self, id, check_success=True):
        """Return whether it succeeded."""

    @abstractmethod
    def unlock(self, id, check_success=True):
        """Return whether it succeeded."""

    @abstractmethod
    def putstream(self, rows, ignoredup=False):
        """Return whether it succeeded."""

    @abstractmethod
    def putdata(self, id, step, inn, stream, parent, locked, ignoredup=False):
        """Return whether it succeeded."""

    @abstractmethod
    def putcontent(self, id, value, ignoredup=False):
        """Return whether it succeeded."""

    @abstractmethod
    def putfields(self, rows, ignoredup=False):
        """Return whether it succeeded."""

    @abstractmethod
    def storestep(self, step, dump=None, ignoredup=False):
        """Return whether it succeeded."""

    @abstractmethod
    def putstep(self, id, name, path, config, dump=None, ignoredup=False):
        """Insert a step registry with the given fields"""

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
