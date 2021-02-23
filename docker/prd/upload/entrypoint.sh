#! /bin/bash
#
# Production entrypoint script for operator upload blade
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
    exit 1
fi

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
echo "Creating required directory structure for operator uploads"

ROOT_DIR=/data
if ! [ -d $ROOT_DIR ]; then
    echo "Root directory supplied does not exist or is not a directory"
    exit 1
fi

chown 9001:9001 $ROOT_DIR
chmod 2775 $ROOT_DIR

operators=$(echo ${DIRBS_OPERATORS} | tr "," "\n" | tr " " "_")
op_id=9002
for o in ${operators}
do
    echo "Creating user environment for operator ${o}"

    # At the moment, this sample system allows full shell access. Home directory
    # permissions are locked down so that operators can not view each other's files,
    # but in the real system you probably want to allow only a chroot'ed SFTP
    # environment.
    useradd -m -d /home/${o} -u ${op_id} -g users ${o}
    passwd -l ${o}
    chmod 700 /home/${o}
    mkdir -p /home/${o}/.ssh

    # At the moment, just re-use the same authorized_keys file for each operator.
    # In real deployment, this would be replaced by the operator's actual public
    # key being echo'ed into ~/.ssh/authorized_keys.
    cp /home/dirbs/.ssh/authorized_keys /home/${o}/.ssh

    chown -R ${o}.users /home/${o}/.ssh
    chmod -R 700 /home/${o}/.ssh
    ln -s /data/${o}/uploading /home/${o}/uploading
    ln -s /data/${o}/ready_for_import /home/${o}/ready_for_import

    echo "Creating operator directory structure for operator ${o} with user id ${op_id}"

    mkdir -p $ROOT_DIR/${o}/uploading
    mkdir -p $ROOT_DIR/${o}/ready_for_import
    chown -R ${op_id}:9001 $ROOT_DIR/${o}
    chmod 2750 $ROOT_DIR/${o}
    chmod 2750 $ROOT_DIR/${o}/uploading
    chmod 2750 $ROOT_DIR/${o}/ready_for_import

    op_id=$((op_id+1))
done

IFS=$OLD_IFS
set -- $CMD
exec "$@"
