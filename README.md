CCM (Cassandra Cluster Manager)
====================================================

A script/library to create, launch and remove an Apache Cassandra cluster on
localhost.

The goal of ccm and ccmlib is to make it easy to create, manage and destroy a
small Cassandra cluster on a local box. It is meant for testing a Cassandra cluster.


Requirements
------------

- A working python installation (tested to work with python 2.7).
- pyYAML (http://pyyaml.org/ -- `sudo easy_install pyYaml`)
- six (https://pypi.python.org/pypi/six -- `sudo easy_install six`)
- ant (http://ant.apache.org/, on Mac OS X, `brew install ant`)
- psutil (https://pypi.python.org/pypi/psutil)
- Java (which version depends on the version of Cassandra you plan to use. If
  unsure, use Java 7 as it is known to work with current versions of Cassandra).
- ccm only works on localhost for now. If you want to create multiple
  node clusters, the simplest way is to use multiple loopback aliases. On
  modern linux distributions you probably don't need to do anything, but
  on Mac OS X, you will need to create the aliases with

      sudo ifconfig lo0 alias 127.0.0.2 up
      sudo ifconfig lo0 alias 127.0.0.3 up
      ...

  Note that the usage section assumes that at least 127.0.0.1, 127.0.0.2 and
  127.0.0.3 are available.

Known issues
------------
Windows only:
  - `node start` pops up a window, stealing focus.
  - cli and cqlsh started from ccm show incorrect prompts on command-prompt
  - non nodetool-based command-line options fail (sstablesplit, scrub, etc)
  - cli_session does not accept commands.
  - To install psutil, you must use the .msi from pypi. pip install psutil will not work
  - You will need ant.bat in your PATH in order to build C* from source
  - You must run with an Unrestricted Powershell Execution-Policy if using Cassandra 2.1.0+
  - Ant installed via [chocolatey](https://chocolatey.org/) will not be found by ccm, so you must create a symbolic
    link in order to fix the issue (as administrator):
    - cmd /c mklink C:\ProgramData\chocolatey\bin\ant.bat C:\ProgramData\chocolatey\bin\ant.exe

Installation
------------

ccm uses python distutils so from the source directory run:

    sudo ./setup.py install

ccm is available on the [Python Package Index][pip]:

    pip install ccm

There is also a [Homebrew package][brew] available:

    brew install ccm

  [pip]: https://pypi.python.org/pypi/ccm
  [brew]: https://github.com/Homebrew/homebrew/blob/master/Library/Formula/ccm.rb

Usage
-----

Let's say you wanted to fire up a 3 node Cassandra cluster.

### Short version

    ccm create test -v 2.0.5 -n 3 -s

You will of course want to replace `2.0.5` by whichever version of Cassandra
you want to test.

### Longer version

ccm works from a Cassandra source tree (not the jars). There are two ways to
tell ccm how to find the sources:
  1. If you have downloaded *and* compiled Cassandra sources, you can ask ccm
     to use those by initiating a new cluster with:

        ccm create test --install-dir=<path/to/cassandra-sources>

     or, from that source tree directory, simply

          ccm create test

  2. You can ask ccm to use a released version of Cassandra. For instance to
     use Cassandra 2.0.5, run

          ccm create test -v 2.0.5

     ccm will download the binary (from http://archive.apache.org/dist/cassandra),
     and set the new cluster to use it. This means
     that this command can take a few minutes the first time you
     create a cluster for a given version. ccm saves the compiled
     source in `~/.ccm/repository/`, so creating a cluster for that
     version will be much faster the second time you run it
     (note however that if you create a lot of clusters with
     different versions, this will take up disk space).

Once the cluster is created, you can populate it with 3 nodes with:

    ccm populate -n 3

Note: If you’re running on Mac OSX, create a new interface for every node besides the first, for example if you populated your cluster with 3 nodes, create interfaces for 127.0.0.2 and 127.0.0.3 like so:

    sudo ifconfig lo0 alias 127.0.0.2
    sudo ifconfig lo0 alias 127.0.0.3

Otherwise you will get the following error message:

    (...) Inet address 127.0.0.1:9042 is not available: [Errno 48] Address already in use

After that execute:

    ccm start

That will start 3 nodes on IP 127.0.0.[1, 2, 3] on port 9160 for thrift, port
7000 for the internal cluster communication and ports 7100, 7200 and 7300 for JMX.
You can check that the cluster is correctly set up with

    ccm node1 ring

You can then bootstrap a 4th node with

    ccm add node4 -i 127.0.0.4 -j 7400 -b

(populate is just a shortcut for adding multiple nodes initially)

ccm provides a number of conveniences, like flushing all of the nodes of
the cluster:

    ccm flush

or only one node:

    ccm node2 flush

You can also easily look at the log file of a given node with:

    ccm node1 showlog

Finally, you can get rid of the whole cluster (which will stop the node and
remove all the data) with

    ccm remove

The list of other provided commands is available through

    ccm

Each command is then documented through the `-h` (or `--help`) flag. For
instance `ccm add -h` describes the options for `ccm add`.

### Source Distribution

If you'd like to use a source distribution instead of the default binary each time (for example, for Continuous Integration), you can prefix cassandra version with `source:`, for example:

```
ccm create test -v source:2.0.5 -n 3 -s
```

### Automatic Version Fallback

If 'binary:' or 'source:' are not explicitly specified in your version string, then ccm will fallback to building the requested version from git if it cannot access the apache mirrors.

### Git and GitHub

To use the latest version from the [canonical Apache Git repository](https://git-wip-us.apache.org/repos/asf?p=cassandra.git), use the version name `git:branch-name`, e.g.:

```
ccm create trunk -v git:trunk -n 5
```

and to download a branch from a GitHub fork of Cassandra, you can prefix the repository and branch with `github:`, e.g.:

```
ccm create patched -v github:jbellis/trunk -n 1
```

### Bash command-line completion
ccm has many sub-commands for both cluster commands as well as node commands, and sometimes you don't quite remember the name of the sub-command you want to invoke. Also, command lines may be long due to long cluster or node names.

Leverage bash's *programmable completion* feature to make ccm use more pleasant. Copy `misc/ccm-completion.bash` to somewhere in your home directory (or /etc if you want to make it accessible to all users of your system) and source it in your `.bash_profile`:
```
. ~/scripts/ccm-completion.bash
```

Once set up, `ccm sw<tab>` expands to `ccm switch `, for example. The `switch` sub-command has extra completion logic to help complete the cluster name. So `ccm switch cl<tab>` would expand to `ccm switch cluster-58` if cluster-58 is the only cluster whose name starts with "cl". If there is ambiguity, hitting `<tab>` a second time shows the choices that match:
```
$ ccm switch cl<tab>
    ... becomes ...
$ ccm switch cluster-
    ... then hit tab twice ...
cluster-56  cluster-85  cluster-96
$ ccm switch cluster-8<tab>
    ... becomes ...
$ ccm switch cluster-85
```

It dynamically determines available sub-commands based on the ccm being invoked. Thus, users running multiple ccm's (or a ccm that they are continuously updating with new commands) will automagically work.

The completion script relies on ccm having two hidden subcommands:
* show-cluster-cmds - emits the names of cluster sub-commands.
* show-node-cmds - emits the names of node sub-commands.

Thus, it will not work with sufficiently old versions of ccm.

Remote debugging
-----------------------

If you would like to connect to your Cassandra nodes with a remote debugger you have to pass the `-d` (or `--debug`) flag to the populate command:

    ccm populate -d -n 3

That will populate 3 nodes on IP 127.0.0.[1, 2, 3] setting up the remote debugging on ports 2100, 2200 and 2300.
The main thread will not be suspended so you don't have to connect with a remote debugger to start a node.

Alternatively you can also specify a remote port with the `-r` (or `--remote-debug-port`) flag while adding a node

    ccm add node4 -r 5005 -i 127.0.0.4 -j 7400 -b

Where things are stored
-----------------------

By default, ccm stores all the node data and configuration files under `~/.ccm/cluster_name/`.
This can be overridden using the `--config-dir` option with each command.

DataStax Enterprise
-------------------

CCM 2.0 supports creating and interacting with DSE clusters. The --dse
option must be used with the `ccm create` command. See the `ccm create -h`
help for assistance.

CCM Lib
-------

The ccm facilities are available programmatically through ccmlib. This could
be used to implement automated tests against Cassandra. A simple example of
how to use ccmlib follows:

    import ccmlib.cluster

    CLUSTER_PATH="."
    cluster = ccmlib.cluster.Cluster(CLUSTER_PATH, 'test', cassandra_version='2.1.14')
    cluster.populate(3).start()
    [node1, node2, node3] = cluster.nodelist()

    # do some tests on the cluster/nodes. To connect to a node through thrift,
    # the host and port to a node is available through
    #   node.network_interfaces['thrift']

    cluster.flush()
    node2.compact()

    # do some other tests

    # after the test, you can leave the cluster running, you can stop all nodes
    # using cluster.stop() but keep the data around (in CLUSTER_PATH/test), or
    # you can remove everything with cluster.remove()


--
Sylvain Lebresne <sylvain@datastax.com>
