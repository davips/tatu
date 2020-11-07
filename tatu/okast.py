#  Copyright (c) 2020. Davi Pereira dos Santos and Rafael Amatte Bisão
#  This file is part of the tatu project.
#  Please respect the license - more about this in the section (*) below.
#
#  tatu is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  tatu is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with tatu.  If not, see <http://www.gnu.org/licenses/>.
#
#  (*) Removing authorship by any means, e.g. by distribution of derived
#  works or verbatim, obfuscated, compiled or rewritten versions of any
#  part of this work is a crime and is unethical regarding the effort and
#  time spent here.
#  Relevant employers or funding agencies will be notified accordingly.

import requests

from cruipto.uuid import UUID
from tatu.storageinterface import StorageInterface


def j(r):
    """Helper function needed because flask test_client() provide json as a property(?), not as a method."""
    return r.json() if callable(r.json) else r.json


class OkaSt(StorageInterface):
    """Central remote storage"""

    def __init__(self, token, alias=None, threaded=True, url="http://localhost:5000"):
        if not isinstance(url, str):
            self.requests = url
            self.headers = None
        else:
            self.requests = requests
            self.headers = {'Authorization': 'Bearer ' + token}
        self.url = url
        self.alias = alias
        self.prefix = self.url if isinstance(self.url, str) else ""
        super().__init__(threaded, timeout=6)  # TODO: check if threading will destroy oka

    def _uuid_(self):
        r = self.requests.get(self.prefix + f"/api/sync", headers=self.headers)
        return UUID(j(r)["uuid"])

    def _hasdata_(self, id, include_empty):
        url = self.prefix + f"/api/sync/{id}?cat=data&fetch=false&empty={include_empty}"
        r = self.requests.get(url, headers=self.headers)
        return j(r)["has"]

    def _getdata_(self, id, include_empty):
        url = self.prefix + f"/api/sync/{id}?cat=data&fetch=true&empty={include_empty}"
        r = self.requests.get(url, headers=self.headers)
        return j(r)

    def _hasstep_(self, id):
        url = self.prefix + f"/api/sync/{id}?cat=step&fetch=false"
        r = self.requests.get(url, headers=self.headers)
        return j(r)["has"]

    def _getstep_(self, id):
        url = self.prefix + f"/api/sync/{id}?cat=step&fetch=true"
        r = self.requests.get(url, headers=self.headers)
        return j(r)

    def _getfields_(self, id):
        url = self.prefix + f"/api/sync/{id}/fields"
        r = self.requests.get(url, headers=self.headers)
        return j(r)

    def _hascontent_(self, ids):
        return NotImplemented
        # url = self.prefix + f"/api/syncaaaaaaaa?cat=content&fetch=false"
        # r = self.requests.get(url, headers=self.headers)
        # return j(r)["has"]

    def _getcontent_(self, id):
        url = self.prefix + f"/api/sync/{id}/content"
        r = self.requests.get(url, headers=self.headers)
        return None if r.data == b'null\n' else r.data

    def _lock_(self, id):
        # url = self.prefix + f"/api/sync/{id}/lock"
        # r = self.requests.put(url, headers=self.headers)
        # # return j(r)
        return NotImplemented

    def _unlock_(self, id):
        return NotImplemented

    def _putdata_(self, id, step, inn, stream, parent, locked, ignoredup):
        return NotImplemented

    def _putfields_(self, rows, ignoredup):
        url = self.prefix + f"/api/sync/fields?ignoredup={ignoredup}"
        r = self.requests.post(url, headers=self.headers, json=rows)
        return r.json["put"]

    def _putcontent_(self, id, value, ignoredup):
        url = self.prefix + f"/api/sync/{id}/content"
        packed = fpack(value)
        r = requests.post(url, files={'file': BytesIO(packed)}, headers=self.headers)
        print(2222222222222222222222222, r)
        return NotImplemented

    def _putstep_(self, id, name, path, config, dump, ignoredup):
        return NotImplemented

    # def _hasdata_(self, ids, include_empty=True):
    #     response = requests.get(self.url + f"/api/sync/{id}?dryrun=true", headers=self.headers)
    #     content = response.text
    #     print(response.text)
    #     if "errors" in content:
    #     raise Exception("Invazxczvzxvxzlid token", content, self.url + f"/api/sync/{id}")
    #     return content

    # # api/sync?cat=data&uuid=ua&uuid=ub&empty=0  ->  lista de dicts
    # def _getdata_(self, ids, include_empty=True):
    #     response = requests.get(self.url + f"/api/tatu?uuid={id}", headers=self.headers)
    #     content = response.content
    #     if b"errors" in content:
    #     raise Exception("Invalid token", content, self.url + f"?uuid={did}")
    #     return content

    # # api/sync  ->  string
    # def _uuid_(self):
    #     # REMINDER syncing needs to know the underlying storage of okast, because the token is not constant as an identity
    #     response = requests.get(self.url + f"/api/tatu?uuid=storage", headers=self.headers)
    #     if b"errors" in response.content:
    #     raise Exception("Invalid token", response.content, self.url + f"?uuid=storage")
    #     return UUID(json.loads(response.text)["uuid"])

    # # api/sync?cat=data&uuid=ua  ->  sucesso?
    # # id, step, inn, stream, parent, locked, ignoredup
    # def _putdata_(self, id, step, inn, stream, parent, locked, ignoredup=False):
    #     # tem que criar post
    #     # vincular pra só enviar content se data der certo
    #     # /sync/hj354jg43kj4g34?inn=hj354jg43kj4g34&names=str&fields=str&history=str
    #
    #     packed = pack(data)  # TODO: consultar previamente o que falta enviar, p/ minimizar trafego
    #     #  TODO: enviar por field
    #     #  TODO: override store() para evitar travessia na classe mãe?
    #     files = {
    #     'json': BytesIO(json.dumps({'alias': self.alias}).encode()),
    #     'file': BytesIO(packed)
    #     }
    #     r = requests.post(self.url + "/api/tatu", files=files, headers=self.headers)
    #     pass

    def _deldata_(self, id):
        raise Exception(f"OkaSt cannot delete Data entries! HINT: deactivate post {id} on Oka.")

    def _open_(self):
        pass  # nothing to open for okast
