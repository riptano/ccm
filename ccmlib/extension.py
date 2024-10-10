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


PRE_CLUSTER_START_HOOKS = []
POST_CLUSTER_START_HOOKS = []
PRE_CLUSTER_STOP_HOOKS = []
POST_CLUSTER_STOP_HOOKS = []
APPEND_TO_CLUSTER_CONFIG_HOOKS = []
LOAD_FROM_CLUSTER_CONFIG_HOOKS = []
APPEND_TO_SERVER_ENV_HOOKS = []
APPEND_TO_CLIENT_ENV_HOOKS = []
APPEND_TO_CQLSH_ARGS_HOOKS = []


def pre_cluster_start(cluster):
    for hook in PRE_CLUSTER_START_HOOKS:
        hook(cluster)


def post_cluster_start(cluster):
    for hook in POST_CLUSTER_START_HOOKS:
        hook(cluster)


def pre_cluster_stop(cluster):
    for hook in PRE_CLUSTER_STOP_HOOKS:
        hook(cluster)


def post_cluster_stop(cluster):
    for hook in POST_CLUSTER_STOP_HOOKS:
        hook(cluster)


def append_to_cluster_config(cluster, config_map):
    for hook in APPEND_TO_CLUSTER_CONFIG_HOOKS:
        hook(cluster, config_map)


def load_from_cluster_config(cluster, config_map):
    for hook in LOAD_FROM_CLUSTER_CONFIG_HOOKS:
        hook(cluster, config_map)


def append_to_server_env(node, env):
    for hook in APPEND_TO_SERVER_ENV_HOOKS:
        hook(node, env)


def append_to_client_env(node, env):
    for hook in APPEND_TO_CLIENT_ENV_HOOKS:
        hook(node, env)


def append_to_cqlsh_args(node, env, args):
    for hook in APPEND_TO_CQLSH_ARGS_HOOKS:
        hook(node, env, args)
