import json
from io import BytesIO
from typing import Optional, List

import requests

from tatu.storage import Storage
from aiuna.compression import pack, unpack

from aiuna.content.data import Data
from transf.absdata import AbsData


class OkaSt(Storage):
    """se data já existir, não tenta criar post!"""

    def _fetch_children_(self, data: Data) -> List[AbsData]:
        raise Exception("not implemented")

    def _open(self):
        pass

    # TODO should okast point to a real url by default?
    def __init__(self, token, alias=None, blocking=False, storage_info=None, url="http://localhost:5000"):
        self.headers = {'Authorization': 'Bearer ' + token}
        self.storage_info = storage_info
        self.url = url
        self.alias = alias
        super().__init__(blocking, timeout=6)  # TODO: check if threading will destroi oka

    def _store_(self, data: Data, check_dup=True):
        packed = pack(data)  # TODO: consultar previamente o que falta enviar, p/ minimizar trafego
        #  TODO: enviar por field
        #  TODO: override store() para evitar travessia na classe mãe?
        files = {
            'json': BytesIO(json.dumps({'alias': self.alias}).encode()),
            'file': BytesIO(packed)
        }
        r = requests.post(self.url + "/api/tatu", files=files, headers=self.headers)

    def _fetch_picklable_(self, data: Data, lock=False) -> Optional[Data]:
        did = data if isinstance(data, str) else data.id
        response = requests.get(self.url + f"/api/tatu?uuid={did}", headers=self.headers)
        content = response.content
        if b"errors" in content:
            raise Exception("Invalid token", content, self.url + f"?uuid={did}")
        return content and unpack(content)

    def _delete_(self, data: Data, check_missing=True):
        raise NotImplemented

    def fetch_matrix(self, id):
        raise NotImplemented

    def list_by_name(self, substring, only_historyless=True):
        raise NotImplemented

    def _unlock_(self, data):
        raise NotImplemented
