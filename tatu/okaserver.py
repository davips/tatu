import json
from io import BytesIO
from typing import Optional

import requests

from tatu.persistence import Persistence
from aiuna.compression import pack, unpack

from aiuna.content.data import Data


class OkaServer(Persistence):
    """se data já existir, não tenta criar post!"""
    def __init__(self, token, storage_info=None, post=False, url="http://localhost:5000"):
        self.headers = {'Authorization': 'Bearer ' + token}
        self.storage_info = storage_info
        self.url = url
        self.post = post

    def store(self, data: Data, check_dup=True):
        packed = pack(data)
        files = {
            'json': BytesIO(json.dumps({'create_post': self.post}).encode()),
            'file': BytesIO(packed)
        }
        r = requests.post(self.url + "/api/tatu", files=files, headers=self.headers)

    def _fetch_pickable_impl(self, data: Data, lock=False) -> Optional[Data]:
        response = requests.get(self.url + f"/api/tatu?uuid={data.id}", headers=self.headers)
        content = response.content
        if b"errors" in content:
            raise Exception("Invalid token", content, self.url + f"?uuid={data.id}")
        return content and unpack(content)

    def fetch_matrix(self, id):
        pass

    def list_by_name(self, substring, only_historyless=True):
        pass

    def unlock(self, data, training_data_uuid=None):
        pass
