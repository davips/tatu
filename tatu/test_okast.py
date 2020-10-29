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
from unittest import TestCase

from app import create_app, db
from app.config import Config

from tatu.okast import OkaSt


class TestConfig(Config):
    TESTING = True
    SQLALCHEMY_DATABASE_URI = 'sqlite://'


class TestOkaSt(TestCase):
    create_user1 = {
        "username": "user1111",
        "password": "password123",
        "email": "teste1@teste.com",
        "name": "Teste1"
    }

    def setUp(self):
        self.app = create_app(TestConfig)
        self.app_context = self.app.app_context()
        self.app_context.push()
        db.create_all()

    def tearDown(self):
        db.session.remove()
        db.drop_all()
        self.app_context.pop()

    def create_user(self, c):
        response = c.post("/api/users", json=self.create_user1)
        self.assertEqual(response.status_code, 201)
        return json.loads(response.get_data(as_text=True))

    def get_token(self, c):
        login1 = self.create_user1.copy()
        del login1["email"]
        del login1["name"]
        response = c.post("/api/auth/login", json=login1)

        self.assertEqual(200, response.status_code)
        data = json.loads(response.get_data(as_text=True))
        c.environ_base["HTTP_AUTHORIZATION"] = "Bearer " + data["access_token"]

        return data["access_token"]

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
        from tatu.sql.sqlite import SQLite
        self.app.config['JWT_BLACKLIST_ENABLED'] = False
        self.app.config['TATU_URL'] = "sqlite://:memory:"
        with self.app.test_client() as c:
            self.create_user(c)
            self.assertEqual(OkaSt(self.get_token(c), url=c).id, SQLite(db=":memory:").id)
