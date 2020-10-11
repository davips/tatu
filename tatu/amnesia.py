from typing import Optional

from aiuna.content.data import Data
from tatu.storage import Storage


class Amnesia(Storage):
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
