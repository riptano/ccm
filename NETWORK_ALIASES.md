Network aliases required for CCM
--------------------------------
CCM needs a local ip address for each Cassandra node it creates. By default CCM assumes each node's ip address is 127.0.0.x, 
where x is the node id.

For example if you populated your cluster with 3 nodes, create interfaces for 127.0.0.2 and 127.0.0.3 
(the first node of course uses 127.0.0.1).

Relevant CCM logs:

* Missing an alias:

```
    (...) Inet address 127.0.0.2:9042 is not available; a cluster may already be running or you may need to add the loopback alias
```

* Another node is already using a specific alias:

```
    (...) Inet address 127.0.0.2:9042 is not available: [Errno 48] Address already in use
```

Mac OSX temporary network aliases
---------------------------------
To get up and running right now, create a temporary alias for every node except the first:

```
sudo ifconfig lo0 alias 127.0.0.2
sudo ifconfig lo0 alias 127.0.0.3
```
Note that these aliases are only temporary and will disappear on reboot.

Mac OSX persistent network aliases
----------------------------------
Persist network aliases if you use CCM often or so infrequently you forget this step. 

To persist a network alias on Mac OSX you need to create a RunAtLoad launch daemon which OSX automatically loads on startup. For example:

Create a shell script:
```
sudo vim /Library/LaunchDaemons/com.ccm.lo0.alias.sh
```

Contents of the script:
```
#!/bin/sh
sudo /sbin/ifconfig lo0 alias 127.0.0.2;
sudo /sbin/ifconfig lo0 alias 127.0.0.3;
```

Set access of the script:
```
sudo chmod 755 /Library/LaunchDaemons/com.ccm.lo0.alias.sh
```

Create a plist to launch the script:
```
sudo vim /Library/LaunchDaemons/com.ccm.lo0.alias.plist
```

Contents of the plist:
```
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.ccm.lo0.alias</string>
    <key>RunAtLoad</key>
    <true/>
    <key>ProgramArguments</key>
    <array>
      <string>/Library/LaunchDaemons/com.ccm.lo0.alias.sh</string>
    </array>
    <key>StandardErrorPath</key>
    <string>/var/log/loopback-alias.log</string>
    <key>StandardOutPath</key>
    <string>/var/log/loopback-alias.log</string>
</dict>
</plist>
```

Set access of the plist:
```
sudo chmod 0644 /Library/LaunchDaemons/com.ccm.lo0.alias.plist
sudo chown root:staff /Library/LaunchDaemons/com.ccm.lo0.alias.plist
```

Launch the daemon now. OSX will automatically reload it on startup.
```
sudo launchctl load /Library/LaunchDaemons/com.ccm.lo0.alias.plist
```

Verify you can ping 127.0.0.2 and 127.0.0.3.

If you ever want to permanently kill the daemon, simply delete its plist from /Library/LaunchDaemons/.
