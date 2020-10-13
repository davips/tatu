#  Copyright (c) 2020. Davi Pereira dos Santos
#      This file is part of the tatu project.
#      Please respect the license. Removing authorship by any means
#      (by code make up or closing the sources) or ignoring property rights
#      is a crime and is unethical regarding the effort and time spent here.
#      Relevant employers or funding agencies will be notified accordingly.
#
#      tatu is free software: you can redistribute it and/or modify
#      it under the terms of the GNU General Public License as published by
#      the Free Software Foundation, either version 3 of the License, or
#      (at your option) any later version.
#
#      tatu is distributed in the hope that it will be useful,
#      but WITHOUT ANY WARRANTY; without even the implied warranty of
#      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#      GNU General Public License for more details.
#
#      You should have received a copy of the GNU General Public License
#      along with tatu.  If not, see <http://www.gnu.org/licenses/>.
#

import json
from io import BytesIO
from typing import Optional

import requests

from cruipto.uuid import UUID
from tatu.storage import Storage
from aiuna.compression import pack, unpack

from aiuna.content.data import Data


class OkaSt(Storage):
    """se data já existir, não tenta criar post!"""
    # TODO should okast point to a real url by default?
    def __init__(self, token, alias=None, blocking=False, storage_info=None, url="http://localhost:5000"):
        self.headers = {'Authorization': 'Bearer ' + token}
        self.storage_info = storage_info
        self.url = url
        self.alias = alias
        super().__init__(blocking, timeout=6)  # TODO: check if threading will destroy oka

    def _uuid_(self):
        # REMINDER syncing needs to know the underlying storage of okast, because the token is not constant as an identity
        response = requests.get(self.url + f"/api/tatu?uuid=storage", headers=self.headers)
        if b"errors" in response.content:
            raise Exception("Invalid token", response.content, self.url + f"?uuid=storage")
        return UUID(json.loads(response.text)["uuid"])

    def _store_(self, data: Data, check_dup=True):
        packed = pack(data)  # TODO: consultar previamente o que falta enviar, p/ minimizar trafego
        #  TODO: enviar por field
        #  TODO: override store() para evitar travessia na classe mãe?
        files = {
            'json': BytesIO(json.dumps({'alias': self.alias}).encode()),
            'file': BytesIO(packed)
        }
        r = requests.post(self.url + "/api/tatu", files=files, headers=self.headers)

    def _fetch_(self, data: Data, lock=False) -> Optional[Data]:
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

    def _unlock_(self, data):
        raise NotImplemented

    def _fetch_at_(self, position):
        raise Exception("OkaSt storage cannot fetch at a given position for now.")  # TODO add route

    def _size_(self):
        raise Exception("OkaSt storage cannot know its size for now.")  # TODO add route

    def _open(self):
        pass

    def _last_synced_(self, storage, only_id=True):
        raise Exception("OkaSt storage cannot know about syncing for now.")  # TODO add route

    def _mark_synced_(self, synced, storage):
        raise Exception("OkaSt storage cannot record about syncing for now.")  # TODO add route
