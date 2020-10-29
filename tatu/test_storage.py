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
from unittest import TestCase, TestLoader

from tatu.sql.mysql import MySQL

TestLoader.sortTestMethodsUsing = None  # Needed to ensure database operations are tested beginning with the most independent ones.


class TestStorage(TestCase):
    def test_lock(self):
        MySQL(db="tatu:xxxx")

    def test_unlock(self):
        self.fail()

    def test_putdata(self):
        self.fail()

    def test_putcontent(self):
        self.fail()

    def test_putfields(self):
        self.fail()

    def test_putstep(self):
        self.fail()

    def test_fetch(self):
        self.fail()

    def test_store(self):
        self.fail()

    def test_fetchhistory(self):
        self.fail()

    def test_fetchstep(self):
        self.fail()

    def test_hasdata(self):
        self.fail()

    def test_getdata(self):
        self.fail()

    def test_getstep(self):
        self.fail()

    def test_getfields(self):
        self.fail()

    def test_hasstep(self):
        self.fail()

    def test_hascontent(self):
        self.fail()

    def test_missing(self):
        self.fail()

    def test_delete_data(self):
        self.fail()

