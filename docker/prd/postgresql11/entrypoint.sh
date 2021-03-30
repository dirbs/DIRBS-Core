#!/bin/bash
#
# Entrypoint for PostgreSQL server container
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

if [ "$1" = 'postgres' ]; then
    mkdir -p /data/config
    mkdir -p /data/db
    chown -R postgres.postgres /data
    chmod 700 /data
    chmod 700 /data/config
    chmod 700 /data/db

    mkdir -p /run/postgresql
    chmod g+s /run/postgresql
    chown -R postgres /run/postgresql

    if [ ! -f "/data/config/postgresql.conf" ]; then
        echo "Copying default postgresql.conf since no copy exists in volume"
        cp /etc/db_config/postgresql.conf /data/config/postgresql.conf

        if [ ! -z ${DB_ENABLE_AUTO_EXPLAIN} ] && [ ${DB_ENABLE_AUTO_EXPLAIN} = 1 ]; then
            echo "Enabling auto_explain feature"
            if grep -q "shared_preload_libraries" /data/config/postgresql.conf; then
                sed -ri "s/(shared_preload_libraries)\s*=\s*['\"]([^'\"]*)['\"]/\1='\2,auto_explain'/" /data/config/postgresql.conf
            else
                echo "shared_preload_libraries = 'auto_explain'" >> /data/config/postgresql.conf
            fi

            echo "auto_explain.log_min_duration = '10s'" >> /data/config/postgresql.conf
            echo "auto_explain.log_nested_statements = on" >> /data/config/postgresql.conf
        fi

        if [ ! -z ${DB_TRACK_IO_TIMING} ] && [ ${DB_TRACK_IO_TIMING} = 1 ]; then
            echo "Enabling track_io_timing feature"
            echo "track_io_timing = on" >> /data/config/postgresql.conf
        fi

        chown postgres.postgres /data/config/postgresql.conf
    fi

    if [ ! -f "/data/config/pg_hba.conf" ]; then
        echo "Copying default pg_hba.conf since no copy exists in volume"
        cp /etc/db_config/pg_hba.conf /data/config/pg_hba.conf
        chown postgres.postgres /data/config/pg_hba.conf
    fi

    if [ ! -f "/data/config/pg_ident.conf" ]; then
        echo "Copying default pg_ident.conf since no copy exists in volume"
        cp /etc/db_config/pg_ident.conf /data/config/pg_ident.conf
        chown postgres.postgres /data/config/pg_ident.conf
    fi

    if [ ! -s "/data/db/PG_VERSION" ]; then
        echo
        echo 'No PostgreSQL DB found in /data/db. Creating new cluster...'
        echo
        gosu postgres initdb -D /data/db

        echo
        echo 'Deleting configuration created by initdb - using config in /data/config instead'
        echo
        gosu postgres rm /data/db/postgresql.conf
        gosu postgres rm /data/db/pg_hba.conf
        gosu postgres rm /data/db/pg_ident.conf

        echo
        echo 'PostgreSQL cluster creation complete'
        echo

        if [ ! -z ${DB_ROOT_USER} ]; then
            echo
            echo 'Starting local-only PostgreSQL server so that database root user can be created '
            echo
            gosu postgres pg_ctl -D /data/db \
                -o "-c listen_addresses='localhost' -c config_file='/data/config/postgresql.conf'" \
                -w start

            if [ ! -z ${DB_ROOT_PASSWORD} ]; then
                echo
                echo 'Creating PostgreSQL root user for creating databases and roles'
                echo
                gosu postgres psql -v ON_ERROR_STOP=1 -c \
                    "CREATE USER ${DB_ROOT_USER} WITH SUPERUSER LOGIN ENCRYPTED PASSWORD '${DB_ROOT_PASSWORD}';"
            else
                echo
                echo 'Creating PostgreSQL root user for creating databases and roles (WARNING: NO password set)'
                echo
                gosu postgres psql -v ON_ERROR_STOP=1 -c \
                    "CREATE USER ${DB_ROOT_USER} WITH SUPERUSER LOGIN;"
                echo "DB_ROOT_PASSWORD environment variable not set!"
                echo "--> If not for local development, you should assign a password to the PostgreSQL root user!"
            fi

            echo
            echo 'Stopping local-only PostgreSQL server'
            echo
            gosu postgres pg_ctl -D /data/db -m fast \
                -o "-c config_file='/data/config/postgresql.conf'" \
                -w stop
        else
            echo "DB_ROOT_USER environment variable not set!"
            echo "--> You will need to create a superuser so that databases and roles can be maintained."
            echo "--> This new account should not be used as an application account."""
        fi

        echo
        echo 'New cluster init complete'
        echo
    fi

    exec gosu postgres postgres --config-file=/data/config/postgresql.conf
fi

exec "$@"