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

from aiuna.file import File
from tatu.okast import OkaSt
from aiuna.creation import read_arff

with open("token.txt", "r") as f:
    token = json.load(f)["token"]

storage = OkaSt(post=True, token=token)

print("Reading file...")
data = File("iris.arff")

print("Storing...")
storage.store(data)  # TODO: it is always sending the file, even when not needed

print("Fetching...")
d = storage.fetch(data.id)
print(d)
