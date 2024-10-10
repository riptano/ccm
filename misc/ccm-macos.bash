#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

CLUSTER_NAME=${CLUSTER_NAME:-test}
NUM_NODES=${NUM_NODES:-3}
CASSANDRA_DIR=${CASSANDRA_DIR:-$HOME/cassandra}
PYTHON_VERSION=${PYTHON_VERSION:-python3}

function fail()
{
    echo "ERROR: $1"
    exit 1
}


if [ ! -z $VIRTUAL_ENV ]; then
    if [ ! -d ${CASSANDRA_DIR}/pylib/venv ]; then

        if [ "${PYTHON_VERSION}" != "python3" -a "${PYTHON_VERSION}" != "python2" ]; then
            fail "Specify Python version python3 or python2"
        fi

        # Initialize the virtualenv used for Cassandra's pylib
        virtualenv --python=$PYTHON_VERSION ${CASSANDRA_DIR}/pylib/venv
        source ${CASSANDRA_DIR}/pylib/venv/bin/activate
        pip install -r ${CASSANDRA_DIR}/pylib/requirements.txt
        pip freeze
    else
        # use Cassandra's pylib virtual environment
        source ${CASSANDRA_DIR}/pylib/venv/bin/activate
    fi
fi

ccm remove $CLUSTER_NAME

for i in $(seq 1 $(($NUM_NODES - 1)));
do
    if=127.0.0.$((i + 1))
    echo $if
    if ! ifconfig lo0 | grep "$if" > /dev/null; then
        echo "Configuring interface $if"
        sudo ifconfig lo0 alias $if || fail "Unable to configure interface $if"
    fi
done

ccm create $CLUSTER_NAME -n $NUM_NODES --install-dir=${CASSANDRA_DIR}
ccm populate -d -n $NUM_NODES
ccm start
