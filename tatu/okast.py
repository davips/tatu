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
from typing import Union

import requests as req

from garoupa.uuid import UUID
from tatu.storageinterface import StorageInterface

default_url = 'http://data.analytics.icmc.usp.br'


def j(r):
    """Helper function needed because flask test_client() provide json as a property(?), not as a method."""
    return r.json() if callable(r.json) else r.json


class OkaSt(StorageInterface):
    """Central remote storage"""

    def _config_(self):
        return self._config

    def __init__(self, token="Invalid",
                 alias=None, threaded=True, url: Union[callable, str] = "http://localhost:5000", close_when_idle=False):
        self._config = locals().copy()
        del self._config["self"]
        del self._config["__class__"]
        # print("STORAGE: ", url)
        self.token = token
        self.external_requests = url if callable(url) else self.request
        self.url = url
        self.alias = alias
        self.prefix = self.url if isinstance(self.url, str) else ""
        # TODO: check if threading will destroy oka
        super().__init__(threaded, timeout=6, close_when_idle=close_when_idle)

    def request(self, route, method, **kwargs):
        headers = {'Authorization': 'Bearer ' + self.token} if self.token else {}
        r = getattr(req, method)(self.url + route, headers=headers, **kwargs)
        if r.status_code == 401:
            print("Please login before.")
            from tatu.auth import get_token
            self.token = get_token(self.url)
            return self.request(route, method, **kwargs)
        elif r.status_code == 422:
            # msg = j(r)["errors"]["json"]
            # print(j(r))
            pass
        else:
            if r.ok:
                return r
            print(r.content)
            print(j(r))
            print(j(r)["errors"])
            msg = j(r)["errors"]["json"]
            print(msg)
            raise Exception(msg)
    
    def _uuid_(self):
        # REMINDER syncing needs to know the underlying storage of okast, because the token is not constant as an identity
        return UUID(j(self.request(f"/api/sync_uuid", "get"))["uuid"])

    def _hasdata_(self, id, include_empty):
        url = f"/api/sync?uuids={id}&cat=data&fetch=false&empty={include_empty}"
        return j(self.request(url, "get"))["has"]

    def _hasstream_(self, data):
        url = f"/api/sync?uuids={data.id}&cat=stream&fetch=false"
        return j(self.request(url, "get"))["has"]

    def _getdata_(self, id, include_empty):
        url = f"/api/sync?uuids={id}&cat=data&fetch=true&empty={include_empty}"
        return j(self.request(url, "get"))

    def _getstream_(self, data):
        url = f"/api/sync?uuids={data}&cat=stream&fetch=true"
        return j(self.request(url, "get"))

    def _hasstep_(self, id):
        url = f"/api/sync?uuids={id}&cat=step&fetch=false"
        return j(self.request(url, "get"))["has"]

    def _getstep_(self, id):
        url = f"/api/sync?uuids={id}&cat=step&fetch=true"
        return j(self.request(url, "get"))

    def _getfields_(self, id):
        url = f"/api/sync/{id}/many&cat=fields"
        return j(self.request(url, "get"))

    def _hascontent_(self, ids):
        uuids = "&".join([f"uuids={id}" for id in ids])
        url = f"/api/sync?{uuids}&cat=content&fetch=false"
        return j(self.request(url, "get"))["has"]

    def _getcontent_(self, id):
        url = f"/api/sync/{id}/content"
        r = self.request(url, "get")
        return None if r.content == b'null\n' else r.content

    def _lock_(self, id):
        url = f"/api/sync/{id}/lock"
        return j(self.request(url, "put"))["success"]

    def _unlock_(self, id):
        url = f"/api/sync/{id}/unlock"
        return j(self.request(url, "put"))["success"]

    def _putdata_(self, id, step, inn, stream, parent, locked, ignoredup):
        kwargs = locals().copy()
        del kwargs["self"]
        url = f"/api/sync?cat=data"
        return j(self.request(url, "post", json={"kwargs": kwargs}))["success"]

    def _putstream_(self, rows, ignoredup):
        url = f"/api/sync/many?cat=stream&ignoredup={ignoredup}"
        return j(self.request(url, "post", json={"rows": rows}))["n"]

    def _putfields_(self, rows, ignoredup):
        url = f"/api/sync/many?cat=fields&ignoredup={ignoredup}"
        return j(self.request(url, "post", json={"rows": rows}))["n"]

    def _putcontent_(self, id, value, ignoredup):
        url = f"/api/sync/{id}/content?ignoredup={ignoredup}"
        return j(self.request(url, "post", files={'bina': value}))["success"]

    def _putstep_(self, id, name, path, config, dump, ignoredup):
        kwargs = locals().copy()
        del kwargs["self"]
        url = f"/api/sync?cat=step"
        return j(self.request(url, "post", json={"kwargs": kwargs}))["success"]

    def _deldata_(self, id):
        raise Exception(f"OkaSt cannot delete Data entries! HINT: deactivate post {id} on Oka.")

    def _open_(self):
        pass  # nothing to open for okast

    def _close_(self):
        pass  # nothing to close for okast

# TODO: consultar previamente o que falta enviar, p/ minimizar trafego
#     #  TODO: enviar por field
#     #  TODO: override store() para evitar travessia na classe mãe?
