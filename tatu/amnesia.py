from typing import Optional, List

from aiuna.content.data import Data
from cruipto.uuid import UUID
from tatu.storage import Storage


class Amnesia(Storage):
    def _fetch_children_(self, data: Data):
        raise Exception("(Pseudo)Storage Amnesia cannot retrieve children!")

    def __init__(self):
        super().__init__(threaded=False, timeout=None)

    def _uuid_(self):
        return UUID(b"Amnesia")

    def _fetch_(self, data: Data, lock=False) -> Optional[Data]:
        return None

    def _delete_(self, data: Data, check_missing=True):
        pass

    def _store_(self, data: Data, check_dup=True):
        pass

    def _unlock_(self, data):
        pass
