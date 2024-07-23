CCM (Cassandra Cluster Manager)
====================================================

WARNING - CCM configuration changes using updateconf does not happen according to CASSANDRA-17379
-------------------------------------------------------------------------------------------------

After CASSANDRA-15234, to support the Python upgrade tests CCM updateconf is replacing
new key name and value in case the old key name and value is provided.  
For example, if you add to config `permissions_validity_in_ms`, it will replace 
`permissions_validity` in default cassandra.yaml 
This was needed to ensure correct overloading as CCM cassandra.yaml has keys 
sorted lexicographically. CASSANDRA-17379 was opened to improve the user experience 
and deprecate the overloading of parameters in cassandra.yaml. In CASSANDRA 4.1+, by default, 
we refuse starting Cassandra with a config containing both old and new config keys for the
same parameter. Start Cassandra with `-Dcassandra.allow_new_old_config_keys=true` to override.
For historical reasons duplicate config keys in cassandra.yaml are allowed by default, start 
Cassandra with `-Dcassandra.allow_duplicate_config_keys=false` to disallow this. Please note
that key_cache_save_period, row_cache_save_period, counter_cache_save_period will be affected 
only by `-Dcassandra.allow_duplicate_config_keys`. Ticket CASSANDRA-17949 was opened to decide
the future of CCM updateconf post CASSANDRA-17379, until then - bear in mind that old replace 
new parameters' in cassandra.yaml when using updateconf even if 
`-Dcassandra.allow_new_old_config_keys=false` is set by default. 

TLDR Do not exercise overloading of parameters in CCM if possible. Also, the mentioned changes
are done only in master branch. Probably the best way to handle cassandra 4.1 in CCM at this 
point is to set `-Dcassandra.allow_new_old_config_keys=false` and 
`-Dcassandra.allow_duplicate_config_keys=false` 
to prohibit any kind of overloading when using CCM master and CCM released versions


CCM (Cassandra Cluster Manager)
====================================================

A script/library to create, launch and remove an Apache Cassandra cluster on
localhost.

The goal of ccm and ccmlib is to make it easy to create, manage and destroy a
small Cassandra cluster on a local box. It is meant for testing a Cassandra cluster.

New to Python development?
--------------------------
Python has moved on since CCM started development. `pip` is the new `easy_install`,
Python 3 is the new 2.7, and pyenv and virtualenv are strongly recommended for managing
multiple Python versions and dependencies for specific Python applications.

A typical MacOS setup would be to install [Homebrew](https://docs.brew.sh/Installation),
then `brew install pyenv` to manage Python versions and then use virtualenv to
manage the dependencies for CCM. Make sure to add [brew's bin directory to your path in
your ~/.zshenv](https://www.zerotohero.dev/zshell-startup-files/). This would be
`/usr/local` for MacOS Intel and `/opt/homebrew/` for MacOS on Apple Silicon.

Now you are ready to install Python using pyenv. To avoid getting a bleeding edge version that will fail with 
some aspect of CCM you can `pyenv install 3.9.16`.

To create the virtualenv run `python3 -m venv --prompt ccm venv` with your git repo as the
current working directory to create a virtual environment for CCM. Then `source venv/bin/activate` to
enable the venv for the current terminal and `deactivate` to exit.

Now you a ready to set up the venv with CCM and its test dependencies. `pip install -e <path_to_ccm_repo>`
to install CCM, and its runtime dependencies from `requirements.txt`, so that the version of
CCM you are running points to the code you are actively working on. There is no build or package step because you
are editing the Python files being run every time you invoke CCM.

Almost there. Now you just need to add the test dependencies that are not in `requirements.txt`.
`pip install mock pytest requests` to finish setting up your dev environment!

Another caveat that has recently appeared Cassandra versions 4.0 and below ship with a version of JNA that isn't
compatible with Apple Silicon and there are no plans to update JNA on those versions. One work around if you are
generally building Cassandra from source to use with CCM is to replace the JNA jar in your Maven repo with a [newer
one](https://search.maven.org/artifact/net.java.dev.jna/jna/5.8.0/jar) that supports Apple Silicon.
Which you version you need to replace will vary depending on the Cassandra version, but it will normally be in
`~/.m2/repository/net/java/dev/jna/jna/<someversion>`. You can also replace the library in
`~/.ccm/repository/<whatever>/lib`.

Also don't forget to disable `AirPlay Receiver` on MacOS which also listens on port 7000.

Requirements
------------

- A working python installation (tested to work with python 2.7).
- See `requirements.txt` for runtime requirements
- `mock` and `pytest` for tests
- ant (http://ant.apache.org/, on Mac OS X, `brew install ant`)
- Java, Cassandra currently builds with either 8 or 11 and is restricted to JDK 8 language
  features and dependencies. There are several sources for the JDK and Azul Zulu is one good option.
- If you want to create multiple node clusters, the simplest way is to use
  multiple loopback aliases. On modern linux distributions you probably don't
  need to do anything, but on Mac OS X, you will need to create the aliases with

      sudo ifconfig lo0 alias 127.0.0.2 up
      sudo ifconfig lo0 alias 127.0.0.3 up
      ...

  Note that the usage section assumes that at least 127.0.0.1, 127.0.0.2 and
  127.0.0.3 are available.

### Optional Requirements

- Paramiko (http://www.paramiko.org/): Paramiko adds the ability to execute CCM
                                       remotely; `pip install paramiko`

__Note__: The remote machine must be configured with an SSH server and a working
          CCM. When working with multiple nodes each exposed IP address must be
          in sequential order. For example, the last number in the 4th octet of
          a IPv4 address must start with `1` (e.g. 192.168.33.11). See
          [Vagrantfile](misc/Vagrantfile) for help with configuration of remote
          CCM machine.


Known issues
------------
Windows only:
  - `node start` pops up a window, stealing focus.
  - cqlsh started from ccm show incorrect prompts on command-prompt
  - non nodetool-based command-line options fail (sstablesplit, scrub, etc)
  - To install psutil, you must use the .msi from pypi. pip install psutil will not work
  - You will need ant.bat in your PATH in order to build C* from source
  - You must run with an Unrestricted Powershell Execution-Policy if using Cassandra 2.1.0+
  - Ant installed via [chocolatey](https://chocolatey.org/) will not be found by ccm, so you must create a symbolic
    link in order to fix the issue (as administrator):
    - cmd /c mklink C:\ProgramData\chocolatey\bin\ant.bat C:\ProgramData\chocolatey\bin\ant.exe

MaxOS only:
  - Airplay listens for incoming connections on 7000 so disable `Settings` -> `General` -> `AirDrop & Handoff` -> `AirPlay Receiver`

Remote Execution only:
  - Using `--config-dir` and `--install-dir` with `create` may not work as
    expected; since the configuration directory and the installation directory
    contain lots of files they will not be copied over to the remote machine
    like most other options for cluster and node operations
  - cqlsh started from ccm using remote execution will not start
    properly (e.g.`ccm --ssh-host 192.168.33.11 node1 cqlsh`); however
    `-x <CMDS>` or `--exec=CMDS` can still be used to execute a CQLSH command
    on a remote node.

Installation
------------

ccm uses python distutils so from the source directory run:

    sudo ./setup.py install

ccm is available on the [Python Package Index][pip]:

    pip install ccm

There is also a [Homebrew package][brew] available:

    brew install ccm

  [pip]: https://pypi.org/project/ccm/
  [brew]: https://github.com/Homebrew/homebrew-core/blob/master/Formula/ccm.rb

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

For Mac OSX, create a new interface for every node besides the first, for example if you populated your cluster with 3 nodes, create interfaces for 127.0.0.2 and 127.0.0.3 like so:

    sudo ifconfig lo0 alias 127.0.0.2
    sudo ifconfig lo0 alias 127.0.0.3

Note these aliases will disappear on reboot. For permanent network aliases on Mac OSX see ![Network Aliases](./NETWORK_ALIASES.md).

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

### Remote Usage (SSH/Paramiko)

All the usage examples above will work exactly the same for a remotely
configured machine; however remote options are required in order to establish a
connection to the remote machine before executing the CCM commands:

| Argument | Value | Description |
| :--- | :--- | :--- |
| --ssh-host | string | Hostname or IP address to use for SSH connection |
| --ssh-port | int | Port to use for SSH connection<br/>Default is 22 |
| --ssh-username | string | Username to use for username/password or public key authentication |
| --ssh-password | string | Password to use for username/password or private key passphrase using public key authentication |
| --ssh-private-key | filename | Private key to use for SSH connection |

#### Special Handling

Some commands require files to be located on the remote server. Those commands
are pre-processed, file transfers are initiated, and updates are made to the
argument value for the remote execution of the CCM command:

| Parameter | Description |
| :--- | :--- |
| `--dse-credentials` | Copy local DSE credentials file to remote server |
| `--node-ssl` | Recursively copy node SSL directory to remote server |
| `--ssl` | Recursively copy SSL directory to remote server |

#### Short Version

    ccm --ssh-host=192.168.33.11 --ssh-username=vagrant --ssh-password=vagrant create test -v 2.0.5 -n 3 -i 192.168.33.1 -s

__Note__: `-i` is used to add an IP prefix during the create process to ensure
          that the nodes communicate using the proper IP address for their node

### Source Distribution

If you'd like to use a source distribution instead of the default binary each time (for example, for Continuous Integration), you can prefix cassandra version with `source:`, for example:

```
ccm create test -v source:2.0.5 -n 3 -s
```

### Automatic Version Fallback

If 'binary:' or 'source:' are not explicitly specified in your version string, then ccm will fallback to building the requested version from git if it cannot access the apache mirrors.

### Git and GitHub

To use the latest version from the [canonical Apache Git repository](https://gitbox.apache.org/repos/asf?p=cassandra.git), use the version name `git:branch-name`, e.g.:

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

Testing
-----------------------

Create a virtual environment i.e.:

    python3 -m venv ccm

`pip install` all dependencies as well as `mock` and `pytest`. Run `pytest` from the repository root to run the tests.

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
