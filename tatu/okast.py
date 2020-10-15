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
from tatu.sql.mysql import MySQL
from tatu.storage import Storage
from aiuna.compression import pack, unpack

from aiuna.content.data import Data


class OkaSt(Storage):
    """se data já existir, não tenta criar post!"""

    # TODO should okast point to a real url by default?
    def __init__(self, token, alias=None, threaded=True, storage_info=None, url="http://localhost:5000"):
        self.headers = {'Authorization': 'Bearer ' + token}
        self.storage_info = storage_info
        self.url = url
        self.alias = alias
        super().__init__(threaded, timeout=6)  # TODO: check if threading will destroy oka

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

    def _unlock_(self, data):
        raise NotImplemented

    def _open_(self):
        pass

    def fetch_field(self, _id):
        raise NotImplementedError

    def _update_remote_(self, storage_func):
        raise NotImplementedError

    def _hasdata_(self, id):
        response = requests.get(self.url + f"/api/sync/{id}?dryrun=true", headers=self.headers)
        content = response.text
        print(response.text)
        if "errors" in content:
            raise Exception("Invazxczvzxvxzlid token", content, self.url + f"/api/sync/{id}")
        return content

    def _hascontent_(self, id):
        raise NotImplementedError

    def _hasstep_(self, id):
        raise NotImplementedError

    def _putdata_(self, **row):
        # tem que criar post
        # vincular pra só enviar content se data der certo
        # /sync/hj354jg43kj4g34?inn=hj354jg43kj4g34&names=str&fields=str&history=str
        raise NotImplementedError

    def _putcontent_(self, id, value):
        # tem que criar post
        # /sync/hj354jg43kj4g34?value=b'21763267f36f3d'
        raise NotImplementedError

    def _putstep_(self, id, name, path, config, dump=None):
        raise NotImplementedError


def test():
    url = "http://localhost:5000"
    oka = OkaSt(threaded=False, token="eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpYXQiOjE2MDI2ODkzOTUsIm5iZiI6MTYwMjY4OTM5NSwianRpIjoiZDFmMTFlM2YtM2FmZi00Y2EwLWExMjAtMGYwMGZjNzY5NGYzIiwiaWRlbnRpdHkiOiJkYXZpcHMiLCJmcmVzaCI6ZmFsc2UsInR5cGUiOiJhY2Nlc3MifQ.O31GaVLaGoVWoI_Muo0bCku5YJMBAsjJQBITyL8pdiY", url=url)
    # b = MySQL(db="tatu:kururu@localhost/tatu").hasdata("2RPtuDLKutP81qX4TaPhu6C")
    from tatu.sql.sqlite import SQLite
    b = oka.hasdata("37sjwjtmUgUY3AyRyhlr0xS")
    print(f"{b}", SQLite().hasdata("37sjwjtmUgUY3AyRyhlr0xS"))


test()
