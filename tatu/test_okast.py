# #  Copyright (c) 2020. Davi Pereira dos Santos
# #  This file is part of the tatu project.
# #  Please respect the license - more about this in the section (*) below.
# #
# #  tatu is free software: you can redistribute it and/or modify
# #  it under the terms of the GNU General Public License as published by
# #  the Free Software Foundation, either version 3 of the License, or
# #  (at your option) any later version.
# #
# #  tatu is distributed in the hope that it will be useful,
# #  but WITHOUT ANY WARRANTY; without even the implied warranty of
# #  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# #  GNU General Public License for more details.
# #
# #  You should have received a copy of the GNU General Public License
# #  along with tatu.  If not, see <http://www.gnu.org/licenses/>.
# #
# #  (*) Removing authorship by any means, e.g. by distribution of derived
# #  works or verbatim, obfuscated, compiled or rewritten versions of any
# #  part of this work is a crime and is unethical regarding the effort and
# #  time spent here.
# #  Relevant employers or funding agencies will be notified accordingly.
# import json
# import warnings
# from unittest import TestCase
#
# from aiuna.compression import unpack, pack
# from aiuna.content.root import Root
# from aiuna.step.dataset import Dataset
# from aiuna.step.let import Let
# from app import create_app, db
# from app.config import Config
# from kururu.tool.enhancement.attribute.binarize import Binarize
#
# from tatu import Tatu
# from tatu.okast import OkaSt
# from tatu.sql.sqlite import SQLite
# from akangatu.transf.noop import NoOp
#
#
# class TestConfig(Config):
#     TESTING = True
#     SQLALCHEMY_DATABASE_URI = 'sqlite://'
#     TATU_URL = "sqlite://:memory:"
#
# class TestOkaSt(TestCase):
#     create_user1 = {
#         "username": "user1111",
#         "password": "password123",
#         "email": "teste1@teste.com",
#         "name": "Teste1"
#     }
#
#     def setUp(self):
#         warnings.simplefilter('ignore', (DeprecationWarning, UserWarning, ImportWarning))
#         self.app = create_app(TestConfig)
#         self.app_context = self.app.app_context()
#         self.app_context.push()
#         db.create_all()
#         self.app.config['JWT_BLACKLIST_ENABLED'] = False
#         self.iris = Dataset().data
#
#         # Create a storage identical to the one used in app, to check for its uuid in test__uuid_().
#         self.db = SQLite(db=":memory:")
#
#     def tearDown(self):
#         self.app.config["TATU_SERVER"].close()
#         self.db.close()
#         db.session.remove()
#         db.drop_all()
#         self.app_context.pop()
#
#     def create_user(self, c):
#         response = c.post("/api/users", json=self.create_user1)
#         self.assertEqual(response.status_code, 201)
#         return json.loads(response.get_data(as_text=True))
#
#     def get_token(self, c):
#         login1 = self.create_user1.copy()
#         del login1["email"]
#         del login1["name"]
#         response = c.post("/api/auth/login", json=login1)
#
#         self.assertEqual(200, response.status_code)
#         data = json.loads(response.get_data(as_text=True))
#         c.environ_base["HTTP_AUTHORIZATION"] = "Bearer " + data["access_token"]
#
#         return data["access_token"]
#
#     # def test__hasdata_(self):  # bool [ok not anymore, hascontent changed?]
#     #     with self.app.test_client() as c:
#     #         self.create_user(c)
#     #         o = OkaSt(self.get_token(c), url=c)
#     #         o.store(self.iris, ignoredup=True)
#     #         self.assertFalse(o.hasdata("nonexistent uuid"))
#     #         self.assertFalse(o.hasdata(Root.id))
#     #         self.assertTrue(o.hasdata(Root.id, include_empty=True))
#     #         self.assertTrue(o.hasdata(self.iris.id))
#
#     def test__getdata_(self):  # None or jsonable dict [ok]
#         with self.app.test_client() as c:
#             self.create_user(c)
#             o = OkaSt(self.get_token(c), url=c)
#             self.assertIsNone(o.getdata(Root.id, include_empty=False))
#             self.assertEquals(Root.id, o.getdata(Root.id, include_empty=True)["parent"])
#
#     def test__uuid_(self):  # str [ok]
#         with self.app.test_client() as c:
#             self.create_user(c)
#             o = OkaSt(self.get_token(c), url=c)
#             self.assertEqual(o.id, self.db.id)
#
#     def test__hasstep_(self):  # bool [ok]
#         with self.app.test_client() as c:
#             self.create_user(c)
#             o = OkaSt(self.get_token(c), url=c)
#             self.assertFalse(o.hasstep("nonexistent uuid"))
#             self.assertTrue(o.hasstep(NoOp().id))
#
#     def test__getstep_(self):  # None or jsonable dict [ok]
#         with self.app.test_client() as c:
#             self.create_user(c)
#             o = OkaSt(self.get_token(c), url=c)
#             self.assertIsNone(o.getstep("nonexistent uuid"))
#             self.assertEquals(NoOp().context, o.getstep(NoOp().id)["path"])
#
#     # def test__getfields_(self):  # None or dict of binaries []
#     #     with self.app.test_client() as c:
#     #         self.create_user(c)
#     #         o = OkaSt(self.get_token(c), url=c)
#     #         self.assertIsNone(o.getfields("nonexistent uuid"))
#     #         self.assertDictEqual(o.getfields(Root.id), {})
#     #         self.assertEquals({k: pack(v) for k, v in self.iris.items()}, o.getfields(self.iris.id))
#
#     # def test__hascontent_(self):  # entra e sai lista de str-uuids []
#     #     with self.app.test_client() as c:
#     #         self.create_user(c)
#     #         o = OkaSt(self.get_token(c), url=c)
#     #         self.assertListEqual(o.hascontent(["nonexistent uuid"]), [])
#     #         self.assertListEqual(o.hascontent([self.iris.uuids["X"].id]), [self.iris.uuids["X"].id])
#
#     # def test__getcontent_(self):  # None or binary [ok]
#     #     with self.app.test_client() as c:
#     #         self.create_user(c)
#     #         o = OkaSt(self.get_token(c), url=c)
#     #         o.store(self.iris, ignoredup=True)
#     #         self.assertFalse(o.getcontent("nonexistent uuid"))
#     #         self.assertEquals(pack(self.iris.X), o.getcontent(self.iris.uuids["X"].id))
#
#     # def test__lock_(self):  # bool []
#     #     iris2 = self.iris >> Let("F", [1, 2])
#     #     with self.app.test_client() as c:
#     #         self.create_user(c)
#     #         o = OkaSt(self.get_token(c), url=c)
#     #         # self.assertFalse(o.lock(self.iris.id, check_existence=False))
#     #         self.assertTrue(o.lock(iris2.id))
#
#     # def test__unlock_(self):  # bool []
#     #     iris2 = self.iris >> Let("F", [1, 2])
#     #     with self.app.test_client() as c:
#     #         self.create_user(c)
#     #         o = OkaSt(self.get_token(c), url=c)
#     #         # self.assertFalse(o.unlock(self.iris.id))
#     #         self.assertTrue(o.unlock(iris2.id))
#
#     # def test__putcontent_(self):  # bool: send binary []
#     #     iris3 = self.iris >> Let("F", [1, 2, 3])
#     #     with self.app.test_client() as c:
#     #         self.create_user(c)
#     #         o = OkaSt(self.get_token(c), url=c)
#     #         self.assertFalse(o.putcontent(iris3.uuids["X"].id, iris3.X))
#     #         self.assertTrue((unpack(o.getcontent(iris3.uuids["X"].id)) == iris3.X).all)
#     #         print()
#     #         print("-------------------------------")
#     #         # self.assertTrue(o.putcontent(iris3.uuids["F"].id, iris3.F))
#     #         # self.assertTrue((unpack(o.getcontent(iris3.uuids["F"].id)) == iris3.F).all)
#
#     # def test__putdata_(self):  # bool:  send jsonable dict []
#     #     iris4 = self.iris >> Let("F", [1, 2, 3, 4])
#     #     with self.app.test_client() as c:
#     #         self.create_user(c)
#     #         o = OkaSt(self.get_token(c), url=c)
#     #         dic = {
#     #             "id": iris4.id,
#     #             "step": iris4.step_uuid.id,
#     #             "inn": None,
#     #             "stream": False,
#     #             "parent": "nonexistent uuid",
#     #             "locked": False
#     #         }
#     #         self.assertFalse(o.putdata(**dic))
#     #
#     #         dic["parent"] = Root.id
#     #         self.assertTrue(o.putdata(**dic))
#
#     # def test__putfields_(self):  # bool: send jsonable dict []
#     #     iris5 = self.iris >> Let("F", [1, 2, 3, 4, 5])
#     #     with self.app.test_client() as c:
#     #         self.create_user(c)
#     #         o = OkaSt(self.get_token(c), url=c)
#     #         rows = {"rows": [
#     #             {"data": iris5.id, "name": "X", "content": iris5.uuids["X"].id},
#     #             {"data": iris5.id, "name": "Y", "content": iris5.uuids["Y"].id}
#     #         ]}
#     #
#     #         self.assertFalse(o.putfields(rows))
#     #         self.assertTrue(o.putcontent(iris5.uuids["X"], pack(iris5.X)))
#     #         self.assertTrue(o.putcontent(iris5.uuids["Y"], pack(iris5.Y)))
#     #         self.assertTrue(o.putcontent(iris5.uuids["F"], pack(iris5.F)))
#     #         self.assertTrue(o.putfields(rows))
#
#     # def test__putstep_(self):  # bool: send jsonable dict []
#     #     with self.app.test_client() as c:
#     #         self.create_user(c)
#     #         o = OkaSt(self.get_token(c), url=c)
#     #         dic = {
#     #             "id": NoOp().id,
#     #             "name": NoOp().name,
#     #             "path": NoOp().context,
#     #             "config": {}
#     #         }
#     #         self.assertFalse(o.putstep(**dic))
#     #
#     #         dic = {
#     #             "id": Binarize().id,
#     #             "name": Binarize().name,
#     #             "path": Binarize().context,
#     #             "config": {}
#     #         }
#     #         self.assertTrue(o.putstep(**dic))
