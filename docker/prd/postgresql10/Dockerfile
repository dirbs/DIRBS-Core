#
# Dockerfile for PostgreSQL 10 server
#
# Copyright (c) 2018 Qualcomm Technologies, Inc.
#
# All rights reserved.
#
#
#
# Redistribution and use in source and binary forms, with or without modification, are permitted (subject to the
# limitations in the disclaimer below) provided that the following conditions are met:
#
#
# * Redistributions of source code must retain the above copyright notice, this list of conditions and the following
# disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
# disclaimer in the documentation and/or other materials provided with the distribution.
#
# * Neither the name of Qualcomm Technologies, Inc. nor the names of its contributors may be used to endorse or promote
# products derived from this software without specific prior written permission.
#
# NO EXPRESS OR IMPLIED LICENSES TO ANY PARTY'S PATENT RIGHTS ARE GRANTED BY THIS LICENSE. THIS SOFTWARE IS PROVIDED BY
# THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
# THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
# COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
# OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
# TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.


FROM ubuntu:16.04

# Set environment (set proper unicode locale, hush debconfig, etc.
# Set PATH so that subsequent pip3 commands install into virtualenv.
# activate command does not work within Docker for some reason
ENV DEBIAN_FRONTEND=noninteractive \
    LC_ALL=C.UTF-8 \
    LANG=C.UTF-8 \
    PATH=/usr/lib/postgresql/10/bin:$PATH \
    GOSU_VERSION=1.10 \
    HLL_VERSION=2.10.2

#
# - Set default shell to bash,
# - Update package lists
# - Install APT depdendencies
#
RUN set -x && \
    unlink /bin/sh; ln -s bash /bin/sh && \
    apt-get -q update --fix-missing && \
    apt-get -q install -y --no-install-recommends locales wget ca-certificates unzip build-essential && \
    apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

#
# Set default locale
#
RUN update-locale LC_ALL=C.UTF-8 LANG=C.UTF-8

# Add PostgreSQL user and group
RUN set -x && groupadd -r postgres --gid=999 && useradd -r -g postgres --uid=999 postgres

# Install Postgres 10, but don't create a cluster
RUN set -x && \
    echo 'deb http://apt.postgresql.org/pub/repos/apt/ xenial-pgdg main' > /etc/apt/sources.list.d/pgdg.list && \
    wget -q -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - && \
    apt-get -q update --fix-missing && \
    apt-get -q install -y --no-install-recommends postgresql-common pgtop postgresql-server-dev-10 && \
    sed -ri 's/#(create_main_cluster) .*$/\1 = false/' /etc/postgresql-common/createcluster.conf && \
    apt-get -q install -y --no-install-recommends postgresql-10 postgresql-contrib-10 && \
    apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Install gosu
RUN set -x && \
    wget -q -O /usr/local/bin/gosu "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$(dpkg --print-architecture)" && \
    chmod +x /usr/local/bin/gosu && \
    gosu nobody true

# install hll
RUN set -x && \
    wget -q -O hll.zip https://github.com/citusdata/postgresql-hll/archive/v$HLL_VERSION.zip && \
    unzip hll.zip && \
    cd postgresql-hll-$HLL_VERSION && \
    make install

# Create /var/run/postgresql folder
RUN set -x && mkdir -p /var/run/postgresql && chown -R postgres /var/run/postgresql

# Copy config to the data directory
COPY docker/prd/postgresql10/*.conf /etc/db_config/

# Copy entrypoint
COPY docker/prd/postgresql10/entrypoint.sh /

# Make sure permissions are set properly on entrypoint
RUN set -x && chmod a+x /entrypoint.sh

# Expose port
EXPOSE 5432

# Set entrypoint
ENTRYPOINT ["/entrypoint.sh"]

# Set default command (argument to entrypoint.sh)
CMD ["postgres"]