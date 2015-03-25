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

Installation
------------

ccm uses python distutils so from the source directory run:

    sudo ./setup.py install

MacPorts has ccm available as a port:

    sudo port -v sync && sudo port -v install ccm

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

     ccm will download the source (from http://archive.apache.org/dist/cassandra),
     compile it, and set the new cluster to use it. This means
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

### Binary Distribution

If you'd like to use a binary distribution instead of compiling from sources each time (for example, for Continuous Integration), you can prefix cassandra version with `binary:`, for example:

```
ccm create test -v binary:2.0.5 -n 3 -s
```

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
be used to implement automated tests again Cassandra. A simple example of
how to use ccmlib follows:

    import ccmlib

    CLUSTER_PATH="."
    cluster = ccmlib.Cluster(CLUSTER_PATH, 'test', cassandra_version='2.0.5')
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
