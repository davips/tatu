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

import requests

from cruipto.uuid import UUID
from tatu.storageinterface import StorageInterface


def j(r):
    """Helper function needed because flask test_client() provide json as a property(?), not as a method."""
    return r.json() if callable(r.json) else r.json


class OkaSt(StorageInterface):
    """se data já existir, não tenta criar post!"""

    def __init__(self, token, alias=None, threaded=True, url="http://localhost:5000"):
        if not isinstance(url, str):
            self.requester = url
            self.headers = None
        else:
            self.requester = requests
            self.headers = {'Authorization': 'Bearer ' + token}
        self.url = url
        self.alias = alias
        super().__init__(threaded, timeout=6)  # TODO: check if threading will destroy oka

    def _uuid_(self):
        prefix = self.url if isinstance(self.url, str) else ""
        r = self.requester.get(prefix + f"/api/sync", headers=self.headers)
        return UUID(j(r)["uuid"])

    def _hasdata_(self, id, include_empty=True):
        response = requests.get(self.url + f"/api/sync/{id}?dryrun=true", headers=self.headers)

    def _getdata_(self, id, include_empty=True):
        pass

    def _hasstep_(self, id):
        pass

    def _getstep_(self, id):
        pass

    def _getfields_(self, id):
        pass

    def _hascontent_(self, ids):
        pass

    def _getcontent_(self, id):
        pass

    def _lock_(self, id):
        pass

    def _unlock_(self, id):
        pass

    def _putdata_(self, id, step, inn, stream, parent, locked, ignoredup=False):
        pass

    def _putfields_(self, rows, ignoredup=False):
        pass

    def _putcontent_(self, id, value, ignoredup=False):
        pass

    def _putstep_(self, id, name, path, config, dump=None, ignoredup=False):
        pass

    # def _hasdata_(self, ids, include_empty=True):
    #     response = requests.get(self.url + f"/api/sync/{id}?dryrun=true", headers=self.headers)
    #     content = response.text
    #     print(response.text)
    #     if "errors" in content:
    #         raise Exception("Invazxczvzxvxzlid token", content, self.url + f"/api/sync/{id}")
    #     return content

    # # api/sync?cat=data&uuid=ua&uuid=ub&empty=0  ->  lista de dicts
    # def _getdata_(self, ids, include_empty=True):
    #     response = requests.get(self.url + f"/api/tatu?uuid={id}", headers=self.headers)
    #     content = response.content
    #     if b"errors" in content:
    #         raise Exception("Invalid token", content, self.url + f"?uuid={did}")
    #     return content

    # # api/sync  ->  string
    # def _uuid_(self):
    #     # REMINDER syncing needs to know the underlying storage of okast, because the token is not constant as an identity
    #     response = requests.get(self.url + f"/api/tatu?uuid=storage", headers=self.headers)
    #     if b"errors" in response.content:
    #         raise Exception("Invalid token", response.content, self.url + f"?uuid=storage")
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
    #         'json': BytesIO(json.dumps({'alias': self.alias}).encode()),
    #         'file': BytesIO(packed)
    #     }
    #     r = requests.post(self.url + "/api/tatu", files=files, headers=self.headers)
    #     pass

    def _deldata_(self, id):
        raise Exception(f"OkaSt cannot delete Data objects! HINT: deactivate post {id} it on Oka")

    # ################################################################################
    def _open_(self):
        pass
