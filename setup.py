#  Copyright (c) 2020. Davi Pereira dos Santos
#  This file is part of the tatu project.
#  Please respect the license. More about this in the section (*) below.
#  Relevant employers or funding agencies will be notified accordingly.
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

import setuptools

NAME = "tatu"


VERSION = "0.2101.4"


AUTHOR = 'Davi Pereira dos Santos'


AUTHOR_EMAIL = 'dpsbac@gmail.com'


DESCRIPTION = 'Persistent Science'


with open('README.md', 'r') as fh:
    LONG_DESCRIPTION = fh.read()


LICENSE = 'GPL3'


URL = 'https://github.com/davips/tatu'


DOWNLOAD_URL = 'https://github.com/davips/tatu/releases'


CLASSIFIERS = ['Intended Audience :: Science/Research',
               'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
               'Natural Language :: English',
               'Programming Language :: Python',
               'Topic :: Scientific/Engineering',
#posix               'Operating System :: Linux',
               'Programming Language :: Python :: 3.8']


INSTALL_REQUIRES = [
    'numpy', 'sklearn', 'liac-arff', 'pymysql', 'aiuna', 'requests'
]


EXTRAS_REQUIRE = {
}

SETUP_REQUIRES = ['wheel']

setuptools.setup(
    name=NAME,
    version=VERSION,
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    classifiers=CLASSIFIERS,
    description=DESCRIPTION,
    download_url=DOWNLOAD_URL,
    extras_require=EXTRAS_REQUIRE,
    install_requires=INSTALL_REQUIRES,
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    license=LICENSE,
    packages=setuptools.find_packages(),
    setup_requires=SETUP_REQUIRES,
    url=URL,
)

package_dir = {'': 'tatu'}  # For IDEs like Intellij to recognize the package.

