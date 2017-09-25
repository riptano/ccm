#
# Remote execution helper functionality for executing CCM commands on a remote
# machine with CCM installed
#

from __future__ import absolute_import

import argparse
import logging
import os
import re
import select
import stat
import sys
import tempfile

# Paramiko is an optional library as SSH is optional for CCM
PARAMIKO_IS_AVAILABLE = False
try:
    import paramiko
    PARAMIKO_IS_AVAILABLE = True
except ImportError:
    pass


def get_remote_usage():
    """
    Get the usage for the remote exectuion options

    :return Usage for the remote execution options
    """
    return RemoteOptionsParser().usage()


def get_remote_options():
    """
    Parse the command line arguments and split out the CCM arguments and the remote options

    :return: A tuple defining the arguments parsed and actions to take
               * remote_options - Remote options only
               * ccm_args       - CCM arguments only
    :raises Exception if private key is not a file
    """
    return RemoteOptionsParser().parse_known_options()


def execute_ccm_remotely(remote_options, ccm_args):
    """
    Execute CCM operation(s) remotely

    :return A tuple defining the execution of the command
              * output      - The output of the execution if the output was not displayed
              * exit_status - The exit status of remotely executed script
    :raises Exception if invalid options are passed for `--dse-credentials`, `--ssl`, or
                      `--node-ssl` when initiating a remote execution; also if
                      error occured during ssh connection
    """
    if not PARAMIKO_IS_AVAILABLE:
        logging.warn("Paramiko is not Availble: Skipping remote execution of CCM command")
        return None, None

    # Create the SSH client
    ssh_client = SSHClient(remote_options.ssh_host, remote_options.ssh_port,
                           remote_options.ssh_username, remote_options.ssh_password,
                           remote_options.ssh_private_key)

    # Handle CCM arguments that require SFTP
    for index, argument in enumerate(ccm_args):
        # Determine if DSE credentials argument is being used
        if "--dse-credentials" in argument:
            # Get the filename being used for the DSE credentials
            tokens = argument.split("=")
            credentials_path = os.path.join(os.path.expanduser("~"), ".ccm", ".dse.ini")
            if len(tokens) == 2:
                credentials_path = tokens[1]

            # Ensure the credential file exists locally and copy to remote host
            if not os.path.isfile(credentials_path):
                raise Exception("DSE Credentials File Does not Exist: %s"
                                % credentials_path)
            ssh_client.put(credentials_path, ssh_client.ccm_config_dir)

            # Update the DSE credentials argument
            ccm_args[index] = "--dse-credentials"

        # Determine if SSL or node SSL path argument is being used
        if "--ssl" in argument or "--node-ssl" in argument:
            # Get the directory being used for the path
            tokens = argument.split("=")
            if len(tokens) != 2:
                raise Exception("Path is not Specified: %s" % argument)
            ssl_path = tokens[1]

            # Ensure the path exists locally and copy to remote host
            if not os.path.isdir(ssl_path):
                raise Exception("Path Does not Exist: %s" % ssl_path)
            remote_ssl_path = ssh_client.temp + os.path.basename(ssl_path)
            ssh_client.put(ssl_path, remote_ssl_path)

            # Update the argument
            ccm_args[index] = tokens[0] + "=" + remote_ssl_path

    # Execute the CCM request, return output and exit status
    return ssh_client.execute_ccm_command(ccm_args)


class SSHClient:
    """
    SSH client class used to handle SSH operations to the remote host
    """

    def __init__(self, host, port, username, password, private_key=None):
        """
        Create the SSH client

        :param host: Hostname or IP address to connect to
        :param port: Port number to use for SSH
        :param username: Username credentials for SSH access
        :param password: Password credentials for SSH access (or private key passphrase)
        :param private_key: Private key to bypass clear text password (default: None - Username and
                            password credentials)
        """
        # Reduce the noise from the logger for paramiko
        logging.getLogger("paramiko").setLevel(logging.WARNING)

        # Establish the SSH connection
        self.ssh = self.__connect(host, port, username, password, private_key)

        # Gather information about the remote OS
        information = self.__server_information()
        self.separator = information[1]
        self.home = information[0] + self.separator
        self.temp = information[2] + self.separator
        self.platform = information[3]
        self.profile = information[4]
        self.distribution = information[5]

        # Create the CCM configuration directory variable
        self.ccm_config_dir = self.home + self.separator + ".ccm" + self.separator

    @staticmethod
    def __connect(host, port, username, password, private_key):
        """
        Establish remote connection

        :param host: Hostname or IP address to connect to
        :param port: Port number to use for SSH
        :param username: Username credentials for SSH access
        :param password: Password credentials for SSH access (or private key passphrase)
        :param private_key: Private key to bypass clear text password
        :return: Paramiko SSH client instance if connection was established
        :raises Exception if connection was unsuccessful
        """
        # Initialize the SSH connection
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        if private_key is not None and password is not None:
            private_key = paramiko.RSAKey.from_private_key_file(private_key, password)
        elif private_key is not None:
            private_key = paramiko.RSAKey.from_private_key_file(private_key, password)

        # Establish the SSH connection
        try:
            ssh.connect(host, port, username, password, private_key)
        except Exception as e:
            raise e

        # Return the established SSH connection
        return ssh

    def execute(self, command, is_displayed=True, profile=None):
        """
        Execute a command on the remote server

        :param command: Command to execute remotely
        :param is_displayed: True if information should be display; false to return output
                             (default: true)
        :param profile: Profile to source (unix like system only should set this)
                        (default: None)
        :return: A tuple defining the execution of the command
                   * output      - The output of the execution if the output was not displayed
                   * exit_status - The exit status of remotely executed script
        """
        # Modify the command for remote execution
        command = " ".join("'{0}'".format(argument) for argument in command)

        # Execute the command and initialize for reading (close stdin/writes)
        if not profile is None and not profile is "None":
            command = "source " + profile + ";" + command
        stdin, stdout, stderr = self.ssh.exec_command(command)
        stdin.channel.shutdown_write()
        stdin.close()


        # Print or gather output as is occurs
        output = None
        if not is_displayed:
            output = []
            output.append(stdout.channel.recv(len(stdout.channel.in_buffer)).decode("utf-8"))
            output.append(stderr.channel.recv(len(stderr.channel.in_buffer)).decode("utf-8"))
        channel = stdout.channel
        while not channel.closed or channel.recv_ready() or channel.recv_stderr_ready():
            # Ensure the channel was not closed prematurely and all data has been ready
            is_data_present = False
            handles = select.select([channel], [], [])
            for read in handles[0]:
                # Read stdout and/or stderr if data is present
                buffer = None
                if read.recv_ready():
                    buffer = channel.recv(len(read.in_buffer)).decode("utf-8")
                    if is_displayed:
                        sys.stdout.write(buffer)
                if read.recv_stderr_ready():
                    buffer = stderr.channel.recv_stderr(len(read.in_stderr_buffer)).decode("utf-8")
                    if is_displayed:
                        sys.stderr.write(buffer)

                # Determine if the output should be updated and displayed
                if buffer is not None:
                    is_data_present = True
                    if not is_displayed:
                        output.append(buffer)

            # Ensure all the data has been read and exit loop if completed
            if (not is_data_present and channel.exit_status_ready()
                and not stderr.channel.recv_stderr_ready()
                and not channel.recv_ready()):
                # Stop reading and close the channel to stop processing
                channel.shutdown_read()
                channel.close()
                break

        # Close file handles for stdout and stderr
        stdout.close()
        stderr.close()

        # Process the output (if available)
        if output is not None:
            output = "".join(output)

        # Return the output from the executed command
        return output, channel.recv_exit_status()

    def execute_ccm_command(self, ccm_args, is_displayed=True):
        """
        Execute a CCM command on the remote server

        :param ccm_args: CCM arguments to execute remotely
        :param is_displayed: True if information should be display; false to return output
                             (default: true)
        :return: A tuple defining the execution of the command
                   * output      - The output of the execution if the output was not displayed
                   * exit_status - The exit status of remotely executed script
        """
        return self.execute(["ccm"] + ccm_args, profile=self.profile)

    def execute_python_script(self, script):
        """
        Execute a python script of the remote server

        :param script: Inline script to convert to a file and execute remotely
        :return: The output of the script execution
        """
        # Create the local file to copy to remote
        file_handle, filename = tempfile.mkstemp()
        temp_file = os.fdopen(file_handle, "wt")
        temp_file.write(script)
        temp_file.close()

        # Put the file into the remote user directory
        self.put(filename, "python_execute.py")
        command = ["python", "python_execute.py"]

        # Execute the python script on the remote system, clean up, and return the output
        output = self.execute(command, False)
        self.remove("python_execute.py")
        os.unlink(filename)
        return output

    def put(self, local_path, remote_path=None):
        """
        Copy a file (or directory recursively) to a location on the remote server

        :param local_path: Local path to copy to; can be file or directory
        :param remote_path: Remote path to copy to (default: None - Copies file or directory to
                            home directory directory on the remote server)
        """
        # Determine if local_path should be put into remote user directory
        if remote_path is None:
            remote_path = os.path.basename(local_path)

        ftp = self.ssh.open_sftp()
        if os.path.isdir(local_path):
            self.__put_dir(ftp, local_path, remote_path)
        else:
            ftp.put(local_path, remote_path)
        ftp.close()

    def __put_dir(self, ftp, local_path, remote_path=None):
        """
        Helper function to perform copy operation to remote server

        :param ftp: SFTP handle to perform copy operation(s)
        :param local_path: Local path to copy to; can be file or directory
        :param remote_path: Remote path to copy to (default: None - Copies file or directory to
                            home directory directory on the remote server)
        """
        # Determine if local_path should be put into remote user directory
        if remote_path is None:
            remote_path = os.path.basename(local_path)
        remote_path += self.separator

        # Iterate over the local path and perform copy operations to remote server
        for current_path, directories, files in os.walk(local_path):
            # Create the remote directory (if needed)
            try:
                ftp.listdir(remote_path)
            except IOError:
                ftp.mkdir(remote_path)

            # Copy the files in the current directory to the remote path
            for filename in files:
                ftp.put(os.path.join(current_path, filename), remote_path + filename)
            # Copy the directory in the current directory to the remote path
            for directory in directories:
                self.__put_dir(ftp, os.path.join(current_path, directory), remote_path + directory)

    def remove(self, remote_path):
        """
        Delete a file or directory recursively on the remote server

        :param remote_path: Remote path to remove
        """
        # Based on the remote file stats; remove a file or directory recursively
        ftp = self.ssh.open_sftp()
        if stat.S_ISDIR(ftp.stat(remote_path).st_mode):
            self.__remove_dir(ftp, remote_path)
        else:
            ftp.remove(remote_path)
        ftp.close()

    def __remove_dir(self, ftp, remote_path):
        """
        Helper function to perform delete operation on the remote server

        :param ftp: SFTP handle to perform delete operation(s)
        :param remote_path: Remote path to remove
        """
        # Iterate over the remote path and perform remove operations
        files = ftp.listdir(remote_path)
        for filename in files:
            # Attempt to remove the file (if exception then path is directory)
            path = remote_path + self.separator + filename
            try:
                ftp.remove(path)
            except IOError:
                self.__remove_dir(ftp, path)

        # Remove the original directory requested
        ftp.rmdir(remote_path)

    def __server_information(self):
        """
        Get information about the remote server:
            * User's home directory
            * OS separator
            * OS temporary directory
            * Platform information
            * Profile to source (Available only on unix-like platforms (including Mac OS))
            * Platform distribution ((Available only on unix-like platforms)
        :return: Remote executed script with the above information (line by line in order)
        """
        return self.execute_python_script("""import os
import platform
import sys
import tempfile

# Standard system information
print(os.path.expanduser("~"))
print(os.sep)
print(tempfile.gettempdir())
print(platform.system())

# Determine the profile for unix like systems
if sys.platform == "darwin" or sys.platform == "linux" or sys.platform == "linux2":
    if os.path.isfile(".profile"):
        print(os.path.expanduser("~") + os.sep + ".profile")
    elif os.path.isfile(".bash_profile"):
        print(os.path.expanduser("~") + os.sep + ".bash_profile")
    else:
        print("None")
else:
    print("None")

# Get the disto information for unix like system (excluding Mac OS)
if sys.platform == "linux" or sys.platform == "linux2":
    print(platform.linux_distribution())
else:
    print("None")
""")[0].splitlines()


class RemoteOptionsParser():
    """
    Parser class to facilitate remote execution of CCM operations via command
    line arguments
    """

    def __init__(self):
        """
        Create the parser for the remote CCM operations allowed
        """
        self.parser = argparse.ArgumentParser(description="Remote",
                                              add_help=False)

        # Add the SSH arguments for the remote parser
        self.parser.add_argument(
            "--ssh-host",
            default=None,
            type=str,
            help="Hostname or IP address to use for SSH connection"
        )
        self.parser.add_argument(
            "--ssh-port",
            default=22,
            type=self.port,
            help="Port to use for SSH connection"
        )
        self.parser.add_argument(
            "--ssh-username",
            default=None,
            type=str,
            help="Username to use for username/password or public key authentication"
        )
        self.parser.add_argument(
            "--ssh-password",
            default=None,
            type=str,
            help="Password to use for username/password or private key passphrase using public "
                 "key authentication"
        )
        self.parser.add_argument(
            "--ssh-private-key",
            default=None,
            type=self.ssh_key,
            help="Private key to use for SSH connection"
        )

    def parse_known_options(self):
        """
        Parse the command line arguments and split out the remote options and CCM arguments

        :return: A tuple defining the arguments parsed and actions to take
                   * remote_options    - Remote options only
                   * ccm_args          - CCM arguments only
        """
        # Parse the known arguments and return the remote options and CCM arguments
        remote_options, ccm_args = self.parser.parse_known_args()
        return remote_options, ccm_args

    @staticmethod
    def ssh_key(key):
        """
        SSH key parser validator (ensure file exists)

        :param key: Filename/Key to validate (ensure exists)
        :return: The filename/key passed in (if valid)
        :raises Exception if filename/key is not a valid file
        """
        value = str(key)
        # Ensure the file exists locally
        if not os.path.isfile(value):
            raise Exception("File Does not Exist: %s" % key)
        return value

    @staticmethod
    def port(port):
        """
        Port validator

        :param port: Port to validate (1 - 65535)
        :return: Port passed in (if valid)
        :raises ArgumentTypeError if port is not valid
        """
        value = int(port)
        if value <= 0 or value > 65535:
            raise argparse.ArgumentTypeError("%s must be between [1 - 65535]" % port)
        return value

    def usage(self):
        """
        Get the usage for the remote exectuion options

        :return Usage for the remote execution options
        """
        # Retrieve the text for just the arguments
        usage = self.parser.format_help().split("optional arguments:")[1]

        # Remove any blank lines and return
        return "Remote Options:" + os.linesep + \
               os.linesep.join([s for s in usage.splitlines() if s])
