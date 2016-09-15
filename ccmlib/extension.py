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
