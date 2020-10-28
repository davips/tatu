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

import random
from unittest import TestCase

import requests

from tatu.okast import OkaSt
from tatu.sql.mysql import MySQL


def user(username=None, password=None, email=None, base_url="http://localhost:5000"):
    """Create a new user."""

    username = username or ("username" + str(random.randint(1, 100000)))
    password = password or ("password" + str(random.randint(1, 100000)))
    email = email or ("email@" + str(random.randint(1, 100000)) + ".com")

    url_createuser = base_url + '/api/users'
    data_createuser = {"username": username, "password": password, "name": "Teste", "email": email}
    response_createuser = requests.post(url_createuser, json=data_createuser)
    print(response_createuser.text)
    return {"username": username, "password": password, "email": email}


def token(username, password, base_url="http://localhost:5000", email=None):
    """Create a new permanent token for the given user."""
    url_login = base_url + '/api/auth/login'
    data_login = {"username": username, "password": password}
    response_login = requests.post(url_login, json=data_login)

    # Temporary token
    access_token = response_login.json()['access_token']
    print("####################TOKEN####################\n" + access_token)

    # Permanent token
    headers = {'Authorization': 'Bearer ' + access_token}
    response_login = requests.post(base_url + "/api/auth/create-api-token", headers=headers)
    return response_login.json()['api_token']


class TestOkaSt(TestCase):
    def test__hasdata_(self):
        self.fail()

    def test__getdata_(self):
        self.fail()

    def test__hasstep_(self):
        self.fail()

    def test__getstep_(self):
        self.fail()

    def test__getfields_(self):
        self.fail()

    def test__hascontent_(self):
        self.fail()

    def test__getcontent_(self):
        self.fail()

    def test__lock_(self):
        self.fail()

    def test__unlock_(self):
        self.fail()

    def test__putdata_(self):
        self.fail()

    def test__putfields_(self):
        self.fail()

    def test__putcontent_(self):
        self.fail()

    def test__putstep_(self):
        self.fail()

    def test__uuid_(self):
        url = "http://localhost:5000"
        okatoken = token(**user("davips", "pass123", base_url=url), base_url=url)
        self.assertEqual(OkaSt(okatoken).id, MySQL(db="tatu:kururu@localhost/tatu").id)
