#  Copyright (c) 2020. Davi Pereira dos Santos
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
#
import warnings
from unittest import TestCase

from aiuna.content.root import Root
from aiuna.step.dataset import Dataset
from aiuna.step.let import Let
from tatu import Tatu
from akangatu.transf.noop import NoOp


class TestTatu(TestCase):
    def setUp(self):
        warnings.simplefilter('ignore', (DeprecationWarning, UserWarning, ImportWarning))
        self.db = Tatu("sqlite://:memory:", threaded=False)
        d = Dataset().data
        d["X"] = [[1, 2, 3]]
        d["y"] = [1, 2, 3]
        self.d = d
        self.db.store(d)
        self.letstep = Let(field="X", value=[[1, 2, 3]])  # * Let(field="y", value=[1, 2, 3])

    def tearDown(self):
        self.db.close()

    def test__uuid_(self):
        self.assertEqual("01Dnf8SfdrCmMTWbGbwnoKu", self.db.id)

    def test_store__fetch__remove(self):
        d = Dataset().data
        self.db.store(d)
        self.assertEqual(d, self.db.fetch(d.id, lazy=False))
        # self.assertEqual(d, self.db.fetch(d.id, lazy=True))
        self.assertEqual(d, self.db.fetch(d))
        self.db.removedata(d)
        self.assertEqual(None, self.db.fetch(d))

    def test_fetchhistory(self):
        h = self.db.fetchhistory(self.d.id)
        self.assertEqual(self.d.history, h)

    def test_fetchstep(self):
        self.assertIsNone(self.db.fetchstep("xxxxxxxxx"))
        self.assertEqual(NoOp(), self.db.fetchstep(NoOp().id))
        self.assertEqual(self.letstep, self.db.fetchstep(self.letstep.id))

    def test_hasdata(self):
        self.assertFalse(self.db.hasdata("xxxxxxxxx"))
        self.assertFalse(self.db.hasdata(Root.id))
        self.assertTrue(self.db.hasdata(Root.id, include_empty=True))

    #
    def test_getdata(self):
        self.assertIsNone(self.db.getdata(Root.id, include_empty=False))
        self.assertEquals(Root.id, self.db.getdata(Root.id, include_empty=True)["parent"])
        # self.assertEquals(self.letstep, self.db.getdata((self.d >> self.letstep).id, include_empty=True)["parent"])

    # def test_hasstep(self):
    #     self.fail()
    #
    # def test_getstep(self):
    #     self.fail()
    #
    # def test_getfields(self):
    #     self.fail()
    #
    # def test_getcontent(self):
    #     self.fail()
    #
    # def test_hascontent(self):
    #     self.fail()
    #
    #
    # def test_lock(self):
    #     self.fail()
    #
    # def test_deldata(self):
    #     self.fail()
    #
    # def test_unlock(self):
    #     self.fail()
    #
    # def test_putdata(self):
    #     self.fail()
    #
    # def test_putcontent(self):
    #     self.fail()
    #
    # def test_putfields(self):
    #     self.fail()
    #
    # def test_storestep(self):
    #     self.fail()
    #
    # def test_putstep(self):
    #     self.fail()
