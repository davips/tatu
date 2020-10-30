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
import warnings
from unittest import TestCase

from aiuna.content.root import Root
from tatu.sql.sqlite import SQLite


class TestStorage(TestCase):
    def setUp(self):
        warnings.simplefilter('ignore', (DeprecationWarning, UserWarning, ImportWarning))
        self.db = SQLite(db=":memory:")

    def test_hasdata(self):
        self.assertFalse(self.db.hasdata("xxxxxxxxx"))
        self.assertFalse(self.db.hasdata(Root.id))
        self.assertTrue(self.db.hasdata(Root.id, include_empty=True))

    def test__getdata_(self):
        self.assertIsNone(self.db.getdata(Root.id, include_empty=False))
        self.assertEquals(Root.id, self.db.getdata(Root.id, include_empty=True)["parent"])
