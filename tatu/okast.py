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

from garoupa.uuid import UUID
from tatu.storageinterface import StorageInterface


def j(r):
    """Helper function needed because flask test_client() provide json as a property(?), not as a method."""
    return r.json() if callable(r.json) else r.json


class OkaSt(StorageInterface):
    """Central remote storage"""

    def __init__(self, token, alias=None, threaded=True, url="http://localhost:5000", close_when_idle=False):
        # print("STORAGE: ", url)
        if not isinstance(url, str):
            self.requests = url
            self.headers = None
        else:
            self.requests = requests
            self.headers = {'Authorization': 'Bearer ' + token}
        self.url = url
        self.alias = alias
        self.prefix = self.url if isinstance(self.url, str) else ""
        super().__init__(threaded, timeout=6,
                         close_when_idle=close_when_idle)  # TODO: check if threading will destroy oka

    def intercept_errors(self, url, method, **kwargs):
        r = getattr(self.requests, method)(self.prefix + url, headers=self.headers, **kwargs)
        if r.ok:
            return r
        raise Exception(j(r)["errors"]["json"])

    def _uuid_(self):
        # REMINDER syncing needs to know the underlying storage of okast, because the token is not constant as an identity
        return UUID(j(self.intercept_errors(f"/api/sync_uuid", "get"))["uuid"])

    def _hasdata_(self, id, include_empty):
        url = f"/api/sync?uuids={id}&cat=data&fetch=false&empty={include_empty}"
        return j(self.intercept_errors(url, "get"))["has"]

    def _hasstream_(self, data):
        url = f"/api/sync?uuids={data.id}&cat=stream&fetch=false"
        return j(self.intercept_errors(url, "get"))["has"]

    def _getdata_(self, id, include_empty):
        url = f"/api/sync?uuids={id}&cat=data&fetch=true&empty={include_empty}"
        return j(self.intercept_errors(url, "get"))

    def _getstream_(self, data):
        url = f"/api/sync?uuids={data}&cat=stream&fetch=true"
        return j(self.intercept_errors(url, "get"))

    def _hasstep_(self, id):
        url = f"/api/sync?uuids={id}&cat=step&fetch=false"
        return j(self.intercept_errors(url, "get"))["has"]

    def _getstep_(self, id):
        url = f"/api/sync?uuids={id}&cat=step&fetch=true"
        return j(self.intercept_errors(url, "get"))

    def _getfields_(self, id):
        url = f"/api/sync/{id}/many&cat=fields"
        return j(self.intercept_errors(url, "get"))

    def _hascontent_(self, ids):
        uuids = "&".join([f"uuids={id}" for id in ids])
        url = f"/api/sync?{uuids}&cat=content&fetch=false"
        return j(self.intercept_errors(url, "get"))["has"]

    def _getcontent_(self, id):
        url = f"/api/sync/{id}/content"
        r = self.intercept_errors(url, "get")
        return None if r.content == b'null\n' else r.content

    def _lock_(self, id):
        url = f"/api/sync/{id}/lock"
        return j(self.intercept_errors(url, "put"))["success"]

    def _unlock_(self, id):
        url = f"/api/sync/{id}/unlock"
        return j(self.intercept_errors(url, "put"))["success"]

    def _putdata_(self, id, step, inn, stream, parent, locked, ignoredup):
        kwargs = locals().copy()
        del kwargs["self"]
        url = f"/api/sync?cat=data"
        return j(self.intercept_errors(url, "post", json={"kwargs": kwargs}))["success"]

    def _putstream_(self, rows, ignoredup):
        url = f"/api/sync/many?cat=stream&ignoredup={ignoredup}"
        return j(self.intercept_errors(url, "post", json={"rows": rows}))["n"]

    def _putfields_(self, rows, ignoredup):
        url = f"/api/sync/many?cat=fields&ignoredup={ignoredup}"
        return j(self.intercept_errors(url, "post", json={"rows": rows}))["n"]

    def _putcontent_(self, id, value, ignoredup):
        url = f"/api/sync/{id}/content?ignoredup={ignoredup}"
        return j(self.intercept_errors(url, "post", files={'bina': value}))["success"]

    def _putstep_(self, id, name, path, config, dump, ignoredup):
        kwargs = locals().copy()
        del kwargs["self"]
        url = f"/api/sync?cat=step"
        return j(self.intercept_errors(url, "post", json={"kwargs": kwargs}))["success"]

    def _deldata_(self, id):
        raise Exception(f"OkaSt cannot delete Data entries! HINT: deactivate post {id} on Oka.")

    def _open_(self):
        pass  # nothing to open for okast

    def _close_(self):
        pass  # nothing to close for okast

# TODO: consultar previamente o que falta enviar, p/ minimizar trafego
#     #  TODO: enviar por field
#     #  TODO: override store() para evitar travessia na classe mãe?
