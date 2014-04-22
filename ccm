#!/usr/bin/env python

import os, sys

from six import print_

from ccmlib import common
from ccmlib.cmds import command, cluster_cmds, node_cmds

def get_command(kind, cmd):
    cmd_name = kind.lower().capitalize() + cmd.lower().capitalize() + "Cmd"
    try:
        klass = (cluster_cmds if kind.lower() == 'cluster' else node_cmds).__dict__[cmd_name]
    except KeyError:
        return None
    if not issubclass(klass, command.Cmd):
        return None
    return klass()

def print_global_usage():
    print_("Usage:")
    print_("  ccm <cluster_cmd> [options]")
    print_("  ccm <node_name> <node_cmd> [options]")
    print_("")
    print_("Where <cluster_cmd> is one of")
    for cmd_name in cluster_cmds.cluster_cmds():
        cmd = get_command("cluster", cmd_name)
        if not cmd:
            print_("Internal error, unknown command {0}".format(cmd_name))
            exit(1)
        print_("  {0:14} {1}".format(cmd_name, cmd.description()))
    print_("or <node_name> is the name of a node of the current cluster and <node_cmd> is one of")
    for cmd_name in node_cmds.node_cmds():
        cmd = get_command("node", cmd_name)
        if not cmd:
            print_("Internal error, unknown command {0}".format(cmd_name))
            exit(1)
        print_("  {0:14} {1}".format(cmd_name, cmd.description()))
    exit(1)

common.check_win_requirements()

if len(sys.argv) <= 1:
    print_("Missing arguments")
    print_global_usage()

arg1 = sys.argv[1].lower()

if arg1 in cluster_cmds.cluster_cmds():
    kind = 'cluster'
    cmd = arg1
    cmd_args = sys.argv[2:]
else:
    if len(sys.argv) <= 2:
        print_("Missing arguments")
        print_global_usage()
    kind = 'node'
    node = arg1
    cmd = sys.argv[2]
    cmd_args = [node] + sys.argv[3:]

cmd = get_command(kind, cmd)
if not cmd:
    print_("Unknown node or command: {0}".format(arg1))
    exit(1)

parser = cmd.get_parser()

(options, args) = parser.parse_args(cmd_args)
cmd.validate(parser, options, args)

cmd.run()
