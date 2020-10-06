import json
from io import BytesIO
from typing import Optional

import requests

from tatu.persistence import Persistence
from aiuna.compression import pack, unpack

from aiuna.content.data import Data


class OkaSt(Persistence):
    """se data já existir, não tenta criar post!"""

    def _open(self):
        pass

    def __init__(self, token, blocking=False, storage_info=None, post=False, url="http://localhost:5000"):
        self.headers = {'Authorization': 'Bearer ' + token}
        self.storage_info = storage_info
        self.url = url
        self.post = post
        super().__init__(blocking, timeout=6)  # TODO: check if threading will destroi oka

    def _store_(self, data: Data, check_dup=True):
        packed = pack(data)  # TODO: consultar previamente o que falta enviar, p/ minimizar trafego
        files = {
            'json': BytesIO(json.dumps({'create_post': self.post}).encode()),
            'file': BytesIO(packed)
        }
        r = requests.post(self.url + "/api/tatu", files=files, headers=self.headers)

    def _fetch_picklable_(self, data: Data, lock=False) -> Optional[Data]:
        response = requests.get(self.url + f"/api/tatu?uuid={data.id}", headers=self.headers)
        content = response.content
        if b"errors" in content:
            raise Exception("Invalid token", content, self.url + f"?uuid={data.id}")
        return content and unpack(content)

    def _delete_(self, data: Data, check_missing=True):
        raise NotImplemented

    def fetch_matrix(self, id):
        raise NotImplemented

    def list_by_name(self, substring, only_historyless=True):
        raise NotImplemented

    def _unlock_(self, data):
        raise NotImplemented
