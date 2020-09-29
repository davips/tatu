from aiuna.content.data import Data
from tatu.persistence import Persistence


class Amnesia(Persistence):
    def store(self, data: Data, check_dup = True):
        pass

    def fetch_matrix(self, id):
        pass

    def unlock(self, data, training_data_uuid=None):
        pass

    def list_by_name(self, substring, only_historyless=True):
        return []

    def _fetch_impl(self, data: Data, lock = False) -> Data:
        pass
