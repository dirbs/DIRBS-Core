"""
Setuptools configuration file

Copyright (c) 2018-2021 Qualcomm Technologies, Inc.
All rights reserved.
Redistribution and use in source and binary forms, with or without modification, are permitted (subject to the
limitations in the disclaimer below) provided that the following conditions are met:
- Redistributions of source code must retain the above copyright notice, this list of conditions and the following
  disclaimer.
- Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
  disclaimer in the documentation and/or other materials provided with the distribution.
- Neither the name of Qualcomm Technologies, Inc. nor the names of its contributors may be used to endorse or promote
  products derived from this software without specific prior written permission.
- The origin of this software must not be misrepresented; you must not claim that you wrote the original software.
  If you use this software in a product, an acknowledgment is required by displaying the trademark/logo as per the
  details provided here: https://www.qualcomm.com/documents/dirbs-logo-and-brand-guidelines
- Altered source versions must be plainly marked as such, and must not be misrepresented as being the original software.
- This notice may not be removed or altered from any source distribution.
NO EXPRESS OR IMPLIED LICENSES TO ANY PARTY'S PATENT RIGHTS ARE GRANTED BY THIS LICENSE. THIS SOFTWARE IS PROVIDED BY
THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.

This file based on the example from the PyPA sample project, whose copyright is
included below:

Copyright (c) 2016 The Python Packaging Authority (PyPA)

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

See:
https://packaging.python.org/en/latest/distributing.html
https://github.com/pypa/sampleproject
"""

# Always prefer setuptools over distutils
from setuptools import setup, find_packages
# To use a consistent encoding
from codecs import open
from os import path
import re
import sys

here = path.abspath(path.dirname(__file__))

if sys.version_info[0] != 3:
    sys.exit('Sorry, only Python 3.x supported')

# Get the long description from the README file
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()


# Version snippet from the following URL:
# https://github.com/pypa/python-packaging-user-guide/blob/master/source/single_source_version.rst
#
# The Python Packaging User Guide is licensed under a Creative Commons
# Attribution-ShareAlike license: http://creativecommons.org/licenses/by-sa/3.0
#
def read(*names):
    """
    Method to read the names provided.

    Arguments:
        *names: names of the files in form of tuples
    Returns:
        file pointer
    """
    with open(path.join(here, *names), encoding="utf8") as fp:
        return fp.read()


def find_version(*file_paths):
    """
    Method to find the current version of dirbs to bump in release.

    Arguments:
        *file_paths: path to the file containing version string

    Returns:
        version str
    """
    version_file = read(*file_paths)
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")

setup(
    name='dirbs',

    # Versions should comply with PEP440.  For a discussion on single-sourcing
    # the version across setup.py and the project code, see
    # https://packaging.python.org/en/latest/single_source_version.html
    version=find_version("src/dirbs", "__init__.py"),

    description='Device Identification, Registration, and Blocking System',
    long_description=long_description,

    # The project's main homepage.
    url='https://github.com/dirbs/DIRBS-Core',

    # Author details
    author='Qualcomm Technologies Inc.',
    author_email='',

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        "Private :: Do Not Upload"
    ],

    # You can just specify the packages manually here if your project is
    # simple. Or you can use find_packages().
    packages=find_packages('src'),
    package_dir={'': 'src'},

    # Do not place third-party / open source dependencies in here. Please
    # place them in opensource_requirements.txt. This is to ensure that
    # our package installation doesn't download or install any opensource
    # packages without the cosent of the end user.
    install_requires=[],
    python_requires='>=3.8',

    # If there are data files included in your packages that need to be
    # installed, specify them here.  If using Python 2.6 or less, then these
    # have to be included in MANIFEST.in as well.
    package_data={
        'dirbs': [
            'css/*.css',
            'js/*.js',
            'sql/base/*.sql',
            'sql/migration_scripts/*.sql',
            'templates/*.html'
        ]
    },

    include_package_data=True,
    zip_safe=False,

    # To provide executable scripts, use entry points in preference to the
    # "scripts" keyword. Entry points provide cross-platform support and allow
    # pip to create the appropriate form of executable for the target platform.
    entry_points={
        'console_scripts': [
            'dirbs-import=dirbs.cli.importer:cli',
            'dirbs-classify=dirbs.cli.classify:cli',
            'dirbs-report=dirbs.cli.report:cli',
            'dirbs-db=dirbs.cli.db:cli',
            'dirbs-listgen=dirbs.cli.listgen:cli',
            'dirbs-prune=dirbs.cli.prune:cli',
            'dirbs-catalog=dirbs.cli.catalog:cli',
            'dirbs-whitelist=dirbs.cli.whitelist:cli'
        ],
    },
)
