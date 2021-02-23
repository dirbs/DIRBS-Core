#
# DIRBS project Makefile
#
# Copyright (c) 2018-2021 Qualcomm Technologies, Inc.
#
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification, are permitted (subject to the
# limitations in the disclaimer below) provided that the following conditions are met:
#
# - Redistributions of source code must retain the above copyright notice, this list of conditions and the following
#  disclaimer.
# - Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
#  disclaimer in the documentation and/or other materials provided with the distribution.
# - Neither the name of Qualcomm Technologies, Inc. nor the names of its contributors may be used to endorse or promote
#  products derived from this software without specific prior written permission.
# - The origin of this software must not be misrepresented; you must not claim that you wrote the original software.
#  If you use this software in a product, an acknowledgment is required by displaying the trademark/logo as per the
#  details provided here: https://www.qualcomm.com/documents/dirbs-logo-and-brand-guidelines
# - Altered source versions must be plainly marked as such, and must not be misrepresented as being the original software.
# - This notice may not be removed or altered from any source distribution.
#
# NO EXPRESS OR IMPLIED LICENSES TO ANY PARTY'S PATENT RIGHTS ARE GRANTED BY THIS LICENSE. THIS SOFTWARE IS PROVIDED BY
# THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
# THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
# COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
# BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
#

.PHONY: clean-pyc install-dev install dist audit test start-dev


clean: clean-pyc
	rm -rf dist .cache docs/build build dirbs.egg-info dirbs-testreport.xml coverage.xml \
		   tests/dirbs-testreport.xml


clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name *.pyc | grep __pycache__ | xargs rm -rf


install-dev:
	pip3 install -r opensource_requirements.txt
	pip3 install -e .


install: dist
	pip3 install -r dist/opensource_requirements.txt
	pip3 install dist/*.whl


dist: clean-pyc
	rm -rf dist
	python3 setup.py bdist_wheel
	cp -a etc dist/
	mkdir -p dist/docker
	cp -a docker/base docker/prd dist/docker/
	cp -a opensource_requirements.txt dist/
	cp -a test_requirements.txt.dist dist/test_requirements.txt
	mv dist/docker/prd/Makefile.mk dist/docker/prd/Makefile
	mkdir -p dist/tests
	rsync -aq --exclude='.*' --exclude='__pycache__' --exclude='*.xml' tests/ dist/tests
	cp -a pytest.ini.dist dist/tests/pytest.ini


dist_test: dist
	pip3 install -r dist/test_requirements.txt
	cd dist/tests && py.test --verbose


audit:
	pip3 install --upgrade flake8==3.8.3 pep8-naming==0.11.1 flake8-SQL==0.4.0 flake8-builtins==1.5.3 \
						   flake8-debugger==3.1.0 flake8-mutable==1.2.0 flake8-import-order==0.18.1 \
						   flake8-pep3101==1.3.0 flake8-string-format==0.3.0 flake8-quotes==3.2.0 \
						   flake8-docstrings==1.5.0 pydocstyle==5.0.2
	flake8 src tests
	eslint src/dirbs/js/*.js


test:
	pip3 install -r test_requirements.txt
	# We need to run the test in development mode for coverage stats to work
	python3 setup.py develop
	py.test --verbose


start-dev:
	pip3 install -r opensource_requirements.txt
	flask run -h 0.0.0.0 -p 5000
