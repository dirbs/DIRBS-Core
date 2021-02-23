#! /bin/bash
#
# DIRBS Core MOTD script
#
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

echo
echo "*************************************************************************"
echo "Welcome to DIRBS Core (running inside an Ubuntu 16.04 Docker container)"
echo

export LC_ALL=C.UTF-8
export LANG=C.UTF-8

source /home/dirbs/.dirbsenv

DIRBS_CODE_VERSION="Not installed"
DIRBS_CODE_DB_SCHEMA_VERSION="n/a"
DIRBS_DB_SCHEMA_VERSION="n/a"
if [ -x "/home/dirbs/dirbs-venv/bin/dirbs-db" ]; then
    DIRBS_CODE_VERSION=`/home/dirbs/dirbs-venv/bin/dirbs-db --version 2> /dev/null | sed -e 's/dirbs-db, version //'`
    DIRBS_DB_CHECK_OUTPUT=`gosu dirbs /home/dirbs/dirbs-venv/bin/dirbs-db check 2>&1`
    if [[ $DIRBS_DB_CHECK_OUTPUT =~ ^.*Code\ schema\ version:.*[0-9]+.*$ ]]; then
        DIRBS_CODE_DB_SCHEMA_VERSION=`echo $DIRBS_DB_CHECK_OUTPUT | sed -e 's/.*Code schema version: \([0-9]\+\).*/\1/'`
    fi
    if [[ $DIRBS_DB_CHECK_OUTPUT =~ ^.*DB\ schema\ version:.*[0-9]+.*$ ]]; then
        DIRBS_DB_SCHEMA_VERSION=`echo $DIRBS_DB_CHECK_OUTPUT | sed -e 's/.*DB schema version: \([0-9]\+\).*/\1/'`
    fi
fi

echo "DIRBS code version: $DIRBS_CODE_VERSION"
echo "DIRBS DB schema version (according to code): $DIRBS_CODE_DB_SCHEMA_VERSION"
echo "DIRBS DB schema version (according to database): $DIRBS_DB_SCHEMA_VERSION"
echo

DIRBS_BUILD_TAG="Unknown"
if [ -f "/etc/dirbs_build_tag" ]; then
    DIRBS_BUILD_TAG=`cat /etc/dirbs_build_tag`
fi

DIRBS_ENVIRONMENT="Unknown"
if [ -f "/etc/dirbs_environment" ]; then
    DIRBS_ENVIRONMENT=`cat /etc/dirbs_environment`
fi

echo "DIRBS Jenkins build source: $DIRBS_BUILD_TAG"
echo "DIRBS deployment environment: $DIRBS_ENVIRONMENT"
echo
echo "***************************************************************************"
