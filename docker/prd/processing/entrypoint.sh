#! /bin/bash
#
# Production entrypoint script for data processing blade
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

set -e

CMD=$@
OLD_IFS=$IFS

if [ -z ${DIRBS_OPERATORS} ]; then
    echo "DIRBS_OPERATORS env variable not set. This should be an ordered, comma-separated list of operator names"
    echo "** No operator data directories will be created as a result of the missing DIRBS_OPERATORS env variable"
fi

# Store run-time environment
echo ${DIRBS_ENV:-Unknown} > /etc/dirbs_environment

# Assumes the following user/group IDs
#
# Users:
#   9001: dirbs
#   9002: <first operator>
#   9003: <second operator>
#   ...
#
# Groups
#   9001: dirbs (for dirbs user)
echo "Creating required directory structure for data processing"

ROOT_DIR=/data
if ! [ -d $ROOT_DIR ]; then
    echo "Root directory supplied does not exist or is not a directory"
    exit 1
fi

chown 9001:9001 $ROOT_DIR
chmod 2775 $ROOT_DIR

mkdir -p $ROOT_DIR/reports
chown 9001:9001 $ROOT_DIR/reports
chmod 2775 $ROOT_DIR/reports

mkdir -p $ROOT_DIR/lists
chown 9001:9001 $ROOT_DIR/lists
chmod 2775 $ROOT_DIR/lists

mkdir -p $ROOT_DIR/stolen_list
chown 9001:9001 $ROOT_DIR/stolen_list
chmod 2770 $ROOT_DIR/stolen_list

mkdir -p $ROOT_DIR/golden_list
chown 9001:9001 $ROOT_DIR/golden_list
chmod 2770 $ROOT_DIR/golden_list

mkdir -p $ROOT_DIR/pairing_list
chown 9001:9001 $ROOT_DIR/pairing_list
chmod 2770 $ROOT_DIR/pairing_list

mkdir -p $ROOT_DIR/barred_list
chown 9001:9001 $ROOT_DIR/barred_list
chmod 2770 $ROOT_DIR/barred_list

mkdir -p $ROOT_DIR/barred_tac_list
chown 9001:9001 $ROOT_DIR/barred_tac_list
chmod 2770 $ROOT_DIR/barred_tac_list

mkdir -p $ROOT_DIR/subscribers_list
chown 9001:9001 $ROOT_DIR/subscribers_list
chmod 2770 $ROOT_DIR/subscribers_list

mkdir -p $ROOT_DIR/registration_list
chown 9001:9001 $ROOT_DIR/registration_list
chmod 2770 $ROOT_DIR/registration_list

mkdir -p $ROOT_DIR/gsma_tac
chown 9001:9001 $ROOT_DIR/gsma_tac
chmod 2770 $ROOT_DIR/gsma_tac

mkdir -p $ROOT_DIR/operator
chown 9001:9001 $ROOT_DIR/operator
chmod 2775 $ROOT_DIR/operator
mkdir -p $ROOT_DIR/operator/processing
chown 9001:9001 $ROOT_DIR/operator/processing
chmod 2770 $ROOT_DIR/operator/processing

if [ ! -z ${DIRBS_DB_DATABASE} ]; then
    echo "Saving DIRBS_DB_DATABASE to /home/dirbs/.dirbsenv"
    echo "export DIRBS_DB_DATABASE=${DIRBS_DB_DATABASE}" >> /home/dirbs/.dirbsenv
fi

if [ ! -z ${DIRBS_DB_HOST} ]; then
    echo "Saving DIRBS_DB_HOST to /home/dirbs/.dirbsenv"
    echo "export DIRBS_DB_HOST=${DIRBS_DB_HOST}" >> /home/dirbs/.dirbsenv
fi

if [ ! -z ${DIRBS_DB_PORT} ]; then
    echo "Saving DIRBS_DB_PORT to /home/dirbs/.dirbsenv"
    echo "export DIRBS_DB_PORT=${DIRBS_DB_PORT}" >> /home/dirbs/.dirbsenv
fi

if [ ! -z ${DIRBS_DB_USER} ]; then
    echo "Saving DIRBS_DB_USER to /home/dirbs/.dirbsenv"
    echo "export DIRBS_DB_USER=${DIRBS_DB_USER}" >> /home/dirbs/.dirbsenv
fi

if [ ! -z ${DIRBS_STATSD_HOST} ]; then
    echo "Saving DIRBS_STATSD_HOST to ~/.dirbsenv"
    echo "export DIRBS_STATSD_HOST=${DIRBS_STATSD_HOST}" >> /home/dirbs/.dirbsenv
fi

if [ ! -z ${DIRBS_ENV} ]; then
    echo "Saving DIRBS_ENV to /home/dirbs/.dirbsenv"
    echo "export DIRBS_ENV=${DIRBS_ENV}" >> /home/dirbs/.dirbsenv
fi

touch /home/dirbs/.dirbsenv
chown dirbs.dirbs /home/dirbs/.dirbsenv
chmod 600 /home/dirbs/.dirbsenv
echo "source /home/dirbs/.dirbsenv" >> /home/dirbs/.bashrc

if [ ! -z ${DIRBS_DB_PASSWORD} ]; then
    echo "Saving DIRBS_DB_PASSWORD to /home/dirbs/.pgpass"
    HOST="${DIRBS_DB_HOST:-*}"
    PORT="${DIRBS_DB_PORT:-*}"
    DATABASE="${DIRBS_DB_DATABASE:-*}"
    USERNAME="${DIRBS_DB_USER:-*}"
    echo "${HOST}:${PORT}:${DATABASE}:${USERNAME}:${DIRBS_DB_PASSWORD}" >> /home/dirbs/.pgpass
    chown dirbs.dirbs /home/dirbs/.pgpass
    chmod 600 /home/dirbs/.pgpass
fi

operators=$(echo ${DIRBS_OPERATORS} | tr "," "\n" | tr " " "_")
for o in ${operators}
do
    echo "Creating per-operator processing directory structure for operator ${o}"
    mkdir -p $ROOT_DIR/operator/processing/${o}
    chown -R 9001:9001 $ROOT_DIR/operator/processing/${o}
    chmod 2770 $ROOT_DIR/operator/processing/${o}
done

if [ ! -z ${DIRBS_AUTO_DB} ] && [ ${DIRBS_AUTO_DB} = 1 ]; then
    echo "Install DIRBS Core roles..."
    sh -c 'gosu dirbs dirbs-db install_roles'

    echo "Checking dirbs database version..."
    if sh -c 'gosu dirbs dirbs-db check' -eq 0; then
        echo "Upgrading dirbs database schema/procedures..."
        sh -c 'gosu dirbs dirbs-db upgrade'
    else
        echo "Installing dirbs database schema..."
        sh -c 'gosu dirbs dirbs-db install'
    fi
else
    echo "Skipping dirbs-db install/upgrade as DIRBS_AUTO_DB environment variable not set"
fi

IFS=$OLD_IFS
set -- $CMD
exec "$@"
