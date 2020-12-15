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

from typing import Optional, List

from aiuna.content.data import Data
from garoupa.uuid import UUID
from tatu.storageinterface import StorageInterface


class Amnesia(StorageInterface):
    def _fetch_children_(self, data: Data):
        raise Exception("(Pseudo)Storage Amnesia cannot retrieve children!")

    def __init__(self):
        super().__init__(threaded=False, timeout=None, close_when_idle=False)

    def _uuid_(self):
        return UUID(b"Amnesia")

    def _fetch_(self, data: Data, lock=False) -> Optional[Data]:
        return None

    def _delete_(self, data: Data, check_missing=True):
        pass

    def _store_(self, data: Data, check_dup=True):
        pass

    def _unlock_(self, data):
        pass
