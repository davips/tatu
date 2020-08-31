from abc import ABC, abstractmethod

from pjdata.aux.uuid import UUID
from pjdata.content.specialdata import UUIDData
from pjdata.types import Data

class Persistence(ABC):
    """
    This class stores and recovers results from some place.
    The children classes are expected to provide storage in e.g.:
     SQLite, remote/local MongoDB, MySQL server, pickled or even CSV files.
    """

    @abstractmethod
    def store(self, data, fields=None, training_data_uuid='', check_dup=True):
        """
        Parameters
        ----------
        blocking
        training_data_uuid
        data
            Data object to store.
        fields
            List of names of the matrices to store (for performance reasons).
            When None, store them all.
        check_dup
            Whether to waste time checking duplicates

        Returns
        -------
        Data or None

        Exception
        ---------
        DuplicateEntryException
        """
        pass

    @abstractmethod
    def _fetch_impl(self, hollow_data, fields, training_data_uuid='', lock=False):
        pass

    def fetch(self, hollow_data, fields, training_data_uuid='', lock=False):
        """Fetch data from DB.

        Parameters
        ----------
        hollow_data
            Data object before being transformed by a pipeline.
        fields
            List of names of the matrices to fetch (for performance reasons).
            When None, fetch them all.
        lock
            Whether to mark entry (input data and pipeline combination) as
            locked, when no data is found for the entry.

        Returns
        -------
        Data or None

        Exception
        ---------
        LockedEntryException, FailedEntryException
        :param training_data_uuid:
        """
        return self._fetch_impl(hollow_data, fields, training_data_uuid, lock)

    @abstractmethod
    def fetch_matrix(self, name):
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
    def unlock(self, hollow_data, training_data_uuid=None):
        pass

    def visual_history(self, id_, folder=None):
        uuid = UUID(id_)
        data = self.fetch(UUIDData(uuid))
        lst = []
        for transformer in reversed(list(data.history)[0:]):  # Discards data birth (e.g. File). TODO
            data = self.fetch(UUIDData(uuid))
            dic = {
                "label": uuid.id, "name": transformer.name, "help": str(transformer), "stored": data is not None
            }
            if folder:
                filename = f"{uuid}.jpg"
                dic["avatar"] = filename
                uuid.generate_avatar(f"{folder}/{filename}")
            lst.append(dic)
            uuid = uuid / transformer.uuid  # Revert to previous data uuid.

        # Ether (source of all Data objects)
        uuid.generate_avatar(f"{folder}/{f'{uuid}.jpg'}")
        dic = {
            "label": uuid.id, "name": "", "help": "Ether, the origin of all datasets.", "stored": False,
            "avatar": f"{uuid}.jpg"
        }
        lst.append(dic)

        return list(reversed(lst))


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
