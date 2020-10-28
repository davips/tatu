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

from aiuna.compression import pack, unpack


def save(filename, obj):
    """Compress and save a python object (Data, Step) as a file."""
    f = open(filename, "wb")
    f.write(pack(obj))
    f.close()


def load(filename):
    """Load a compressed python object (Data, Step) from file."""
    f = open(filename, "rb")
    res = unpack(f.read())
    f.close()
    return res


def save_txt(filename, text):
    """Save text as a file."""
    f = open(filename, "w")
    f.write(text)
    f.close()
