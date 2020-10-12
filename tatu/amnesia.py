from typing import Optional, List

from aiuna.content.data import Data
from tatu.storage import Storage
from transf.absdata import AbsData


class Amnesia(Storage):
    def _fetch_children_(self, data: Data) -> List[AbsData]:
        raise Exception("(Pseudo)Storage Amnesia cannot retrieve children!")

    def __init__(self):
        super().__init__(blocking=True, timeout=None)

    def _fetch_picklable_(self, data: Data, lock=False) -> Optional[Data]:
        return None

    def _delete_(self, data: Data, check_missing=True):
        pass

    def _open(self):
        pass

    def _store_(self, data: Data, check_dup=True):
        pass

    def fetch_matrix(self, id):
        return None

    def _unlock_(self, data):
        pass

    def list_by_name(self, substring, only_historyless=True):
        return []
