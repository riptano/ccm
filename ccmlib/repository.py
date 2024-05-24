# downloaded sources handling
from __future__ import absolute_import, division, with_statement

import json
import logging
from logging import handlers
import os
import re
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
import time
from distutils.version import LooseVersion  # pylint: disable=import-error, no-name-in-module

from six import next, print_

try:
    import ConfigParser
except ImportError:
    import configparser as ConfigParser

from ccmlib import common
from ccmlib.common import (ArgumentError, CCMError,
                           update_java_version, get_default_path, get_jdk_version_int,
                           platform_binary, rmdirs, validate_install_dir)
from six.moves import urllib

DSE_ARCHIVE = "http://downloads.datastax.com/enterprise/dse-%s-bin.tar.gz"
OPSC_ARCHIVE = "http://downloads.datastax.com/enterprise/opscenter-%s.tar.gz"
ARCHIVE = "http://archive.apache.org/dist/cassandra"
GIT_REPO = "https://gitbox.apache.org/repos/asf/cassandra.git"
GITHUB_REPO = "https://github.com/apache/cassandra.git"
GITHUB_TAGS = "https://api.github.com/repos/apache/cassandra/git/refs/tags"
CCM_CONFIG = ConfigParser.RawConfigParser()
CCM_CONFIG.read(os.path.join(os.path.expanduser("~"), ".ccm", "config"))


def setup(version, verbose=False):
    binary = True
    fallback = True

    if version.startswith('git:'):
        clone_development(GIT_REPO, version, verbose=verbose)
        return (version_directory(version), None)
    elif version.startswith('local:'):
        # local: slugs take the form of: "local:/some/path/:somebranch"
        try:
            _, path, branch = version.split(':')
        except ValueError:
            raise CCMError("local version ({}) appears to be invalid. Please format as local:/some/path/:somebranch".format(version))

        clone_development(path, version, verbose=verbose)
        version_dir = version_directory(version)

        if version_dir is None:
            raise CCMError("Path provided in local slug appears invalid ({})".format(path))
        return (version_dir, None)

    elif version.startswith('binary:'):
        version = version.replace('binary:', '')
        fallback = False

    elif version.startswith('github:'):
        user_name, _ = github_username_and_branch_name(version)
        # make sure to use http for cloning read-only repos such as 'github:apache/cassandra-2.1'
        if user_name == "apache":
            clone_development(GITHUB_REPO, version, verbose=verbose)
        else:
            clone_development(github_repo_for_user(user_name), version, verbose=verbose)
        return (directory_name(version), None)

    elif version.startswith('source:'):
        version = version.replace('source:', '')

    elif version.startswith('clone:'):
        # locally present C* source tree
        version = version.replace('clone:', '')
        return (version, None)

    elif version.startswith('alias:'):
        alias = version.split(":")[1].split("/")[0]
        try:
            git_repo = CCM_CONFIG.get("aliases", alias)
            clone_development(git_repo, version, verbose=verbose, alias=True)
            return (directory_name(version), None)
        except ConfigParser.NoOptionError as e:
            common.warning("Unable to find alias {} in configuration file.".format(alias))
            raise e

    if version in ('stable', 'oldstable', 'testing'):
        version = get_tagged_version_numbers(version)[0]

    cdir = version_directory(version)
    if cdir is None:
        try:
            download_version(version, verbose=verbose, binary=binary)
            cdir = version_directory(version)
        except Exception as e:
            # If we failed to download from ARCHIVE,
            # then we build from source from the git repo,
            # as it is more reliable.
            # We don't do this if binary: or source: were
            # explicitly specified.
            if fallback:
                common.warning("Downloading {} failed, trying to build from git instead.\n"
                               "The error was: {}".format(version, e))
                version = 'git:cassandra-{}'.format(version)
                clone_development(GIT_REPO, version, verbose=verbose)
                return (version_directory(version), None)
            else:
                raise e
    return (cdir, version)


def setup_dse(version, username, password, verbose=False):
    cdir = version_directory(version)
    if cdir is None:
        download_dse_version(version, username, password, verbose=verbose)
        cdir = version_directory(version)
    return (cdir, version)


def setup_opscenter(opscenter, username, password, verbose=False):
    ops_version = 'opsc' + opscenter
    odir = version_directory(ops_version)
    if odir is None:
        download_opscenter_version(opscenter, username, password, ops_version, verbose=verbose)
        odir = version_directory(ops_version)
    return odir


def validate(path):
    if path.startswith(__get_dir()):
        _, version = os.path.split(os.path.normpath(path))
        setup(version)


def clone_development(git_repo, version, verbose=False, alias=False):
    print_(git_repo, version)
    target_dir = directory_name(version)
    assert target_dir
    if 'github' in version:
        git_repo_name, git_branch = github_username_and_branch_name(version)
    elif 'local:' in version:
        git_repo_name = 'local_{}'.format(git_repo)  # add git repo location to distinguish cache location for differing repos
        git_branch = version.split(':')[-1]  # last token on 'local:...' slugs should always be branch name
    elif alias:
        git_repo_name = 'alias_{}'.format(version.split('/')[0].split(':')[-1])
        git_branch = version.split('/')[-1]
    else:
        git_repo_name = 'apache'
        git_branch = version.split(':', 1)[1]
    local_git_cache = os.path.join(__get_dir(), '_git_cache_' + git_repo_name)

    logfile = lastlogfilename()
    logger = get_logger(logfile)

    try:
        # Checkout/fetch a local repository cache to reduce the number of
        # remote fetches we need to perform:
        if not os.path.exists(local_git_cache):
            common.info("Cloning Cassandra...")
            process = subprocess.Popen(
                ['git', 'clone', '--mirror', git_repo, local_git_cache],
                cwd=__get_dir(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out, _, _ = log_info(process, logger)
            assert out == 0, "Could not do a git clone"
        else:
            common.info("Fetching Cassandra updates...")
            process = subprocess.Popen(
                ['git', 'fetch', '-fup', 'origin', '+refs/*:refs/*'],
                cwd=local_git_cache, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out, _, _ = log_info(process, logger)
            assert out == 0, "Could not update git"

        # Checkout the version we want from the local cache:
        if not os.path.exists(target_dir):
            # development branch doesn't exist. Check it out.
            common.info("Cloning Cassandra (from local cache)")

            # git on cygwin appears to be adding `cwd` to the commands which is breaking clone
            if sys.platform == "cygwin":
                local_split = local_git_cache.split(os.sep)
                target_split = target_dir.split(os.sep)
                process = subprocess.Popen(
                    ['git', 'clone', local_split[-1], target_split[-1]],
                    cwd=__get_dir(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                out, _, _ = log_info(process, logger)
                assert out == 0, "Could not do a git clone"
            else:
                process = subprocess.Popen(
                    ['git', 'clone', local_git_cache, target_dir],
                    cwd=__get_dir(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                out, _, _ = log_info(process, logger)
                assert out == 0, "Could not do a git clone"

            # determine if the request is for a branch
            is_branch = False
            try:
                branch_listing = subprocess.check_output(['git', 'branch', '--all'], cwd=target_dir).decode('utf-8')
                branches = [b.strip() for b in branch_listing.replace('remotes/origin/', '').split()]
                is_branch = git_branch in branches
            except subprocess.CalledProcessError as cpe:
                common.error("Error Running Branch Filter: {}\nAssumming request is not for a branch".format(cpe.output))

            # now check out the right version
            branch_or_sha_tag = 'branch' if is_branch else 'SHA/tag'
            common.info("Checking out requested {} ({})".format(branch_or_sha_tag, git_branch))
            if is_branch:
                # we use checkout -B with --track so we can specify that we want to track a specific branch
                # otherwise, you get errors on branch names that are also valid SHAs or SHA shortcuts, like 10360
                # we use -B instead of -b so we reset branches that already exist and create a new one otherwise
                process = subprocess.Popen(['git', 'checkout', '-B', git_branch,
                                            '--track', 'origin/{git_branch}'.format(git_branch=git_branch)],
                                           cwd=target_dir, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                out, _, _ = log_info(process, logger)
            else:
                process = subprocess.Popen(
                    ['git', 'checkout', git_branch],
                    cwd=target_dir, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                out, _, _ = log_info(process, logger)
            if int(out) != 0:
                raise CCMError('Could not check out git branch {branch}. '
                               'Is this a valid branch name? (see {lastlog} or run '
                               '"ccm showlastlog" for details)'.format(
                                   branch=git_branch, lastlog=logfile
                               ))
            # now compile
            compile_version(git_branch, target_dir, verbose)
        else:  # branch is already checked out. See if it is behind and recompile if needed.
            process = subprocess.Popen(
                ['git', 'fetch', 'origin'],
                cwd=target_dir, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out, _, _ = log_info(process, logger)
            assert out == 0, "Could not do a git fetch"
            process = subprocess.Popen(['git', 'status', '-sb'], cwd=target_dir, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            _, status, _ = log_info(process, logger)
            if str(status).find('[behind') > -1:  # If `status` looks like '## cassandra-2.2...origin/cassandra-2.2 [behind 9]\n'
                common.info("Branch is behind, recompiling")
                process = subprocess.Popen(['git', 'pull'], cwd=target_dir, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                out, _, _ = log_info(process, logger)
                assert out == 0, "Could not do a git pull"
                process = subprocess.Popen([platform_binary('ant'), 'realclean'], cwd=target_dir, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                out, _, _ = log_info(process, logger)
                assert out == 0, "Could not run 'ant realclean'"

                # now compile
                compile_version(git_branch, target_dir, verbose)
            elif re.search('\[.*?(ahead|behind).*?\]', status.decode("utf-8")) is not None:  # status looks like  '## trunk...origin/trunk [ahead 1, behind 29]\n'
                 # If we have diverged in a way that fast-forward merging cannot solve, raise an exception so the cache is wiped
                common.error("Could not ascertain branch status, please resolve manually.")
                raise Exception
            else:  # status looks like '## cassandra-2.2...origin/cassandra-2.2\n'
                common.debug("Branch up to date, not pulling.")
    except Exception as e:
        # wipe out the directory if anything goes wrong. Otherwise we will assume it has been compiled the next time it runs.
        try:
            rmdirs(target_dir)
            common.error("Deleted {} due to error".format(target_dir))
        except:
            print_('Building C* version {version} failed. Attempted to delete {target_dir} '
                   'but failed. This will need to be manually deleted'.format(
                       version=version,
                       target_dir=target_dir
                   ))
        finally:
            raise e


def download_dse_version(version, username, password, verbose=False):
    url = DSE_ARCHIVE
    if CCM_CONFIG.has_option('repositories', 'dse'):
        url = CCM_CONFIG.get('repositories', 'dse')

    url = url % version
    _, target = tempfile.mkstemp(suffix=".tar.gz", prefix="ccm-")
    try:
        if username is None:
            common.warning("No dse username detected, specify one using --dse-username or passing in a credentials file using --dse-credentials.")
        if password is None:
            common.warning("No dse password detected, specify one using --dse-password or passing in a credentials file using --dse-credentials.")
        __download(url, target, username=username, password=password, show_progress=verbose)
        common.debug("Extracting {} as version {} ...".format(target, version))
        tar = tarfile.open(target)
        dir = tar.next().name.split("/")[0]  # pylint: disable=all
        tar.extractall(path=__get_dir())
        tar.close()
        target_dir = os.path.join(__get_dir(), version)
        if os.path.exists(target_dir):
            rmdirs(target_dir)
        shutil.move(os.path.join(__get_dir(), dir), target_dir)
    except urllib.error.URLError as e:
        msg = "Invalid version %s" % version if url is None else "Invalid url %s" % url
        msg = msg + " (underlying error is: %s)" % str(e)
        raise ArgumentError(msg)
    except tarfile.ReadError as e:
        raise ArgumentError("Unable to uncompress downloaded file: %s" % str(e))


def download_opscenter_version(version, username, password, target_version, verbose=False):
    url = OPSC_ARCHIVE
    if CCM_CONFIG.has_option('repositories', 'opscenter'):
        url = CCM_CONFIG.get('repositories', 'opscenter')

    url = url % version
    _, target = tempfile.mkstemp(suffix=".tar.gz", prefix="ccm-")
    try:
        if username is None:
            common.warning("No dse username detected, specify one using --dse-username or passing in a credentials file using --dse-credentials.")
        if password is None:
            common.warning("No dse password detected, specify one using --dse-password or passing in a credentials file using --dse-credentials.")
        __download(url, target, username=username, password=password, show_progress=verbose)
        common.info("Extracting {} as version {} ...".format(target, target_version))
        tar = tarfile.open(target)
        dir = tar.next().name.split("/")[0]  # pylint: disable=all
        tar.extractall(path=__get_dir())
        tar.close()
        target_dir = os.path.join(__get_dir(), target_version)
        if os.path.exists(target_dir):
            rmdirs(target_dir)
        shutil.move(os.path.join(__get_dir(), dir), target_dir)
    except urllib.error.URLError as e:
        msg = "Invalid version {}".format(version) if url is None else "Invalid url {}".format(url)
        msg = msg + " (underlying error is: {})".format(str(e))
        raise ArgumentError(msg)
    except tarfile.ReadError as e:
        raise ArgumentError("Unable to uncompress downloaded file: {}".format(str(e)))


def download_version(version, url=None, verbose=False, binary=False):
    """Download, extract, and build Cassandra tarball.

    if binary == True, download precompiled tarball, otherwise build from source tarball.
    """

    archive_url = ARCHIVE
    if CCM_CONFIG.has_option('repositories', 'cassandra'):
        archive_url = CCM_CONFIG.get('repositories', 'cassandra')

    if binary:
        archive_url = "%s/%s/apache-cassandra-%s-bin.tar.gz" % (archive_url, version, version) if url is None else url
    else:
        archive_url = "%s/%s/apache-cassandra-%s-src.tar.gz" % (archive_url, version, version) if url is None else url
    _, target = tempfile.mkstemp(suffix=".tar.gz", prefix="ccm-")
    try:
        __download(archive_url, target, show_progress=verbose)
        common.info("Extracting {} as version {} ...".format(target, version))
        tar = tarfile.open(target)
        dir = tar.next().name.split("/")[0]  # pylint: disable=all
        tar.extractall(path=__get_dir())
        tar.close()
        target_dir = os.path.join(__get_dir(), version)
        if os.path.exists(target_dir):
            rmdirs(target_dir)
        shutil.move(os.path.join(__get_dir(), dir), target_dir)

        if binary:
            # Binary installs don't have a build.xml that is needed
            # for pulling the version from. Write the version number
            # into a file to read later in common.get_version_from_build()
            with open(os.path.join(target_dir, '0.version.txt'), 'w') as f:
                f.write(version)
        else:
            compile_version(version, target_dir, verbose=verbose)

    except urllib.error.URLError as e:
        msg = "Invalid version {}".format(version) if url is None else "Invalid url {}".format(url)
        msg = msg + " (underlying error is: {})".format(str(e))
        raise ArgumentError(msg)
    except tarfile.ReadError as e:
        raise ArgumentError("Unable to uncompress downloaded file: {}".format(str(e)))
    except CCMError as e:
        # wipe out the directory if anything goes wrong. Otherwise we will assume it has been compiled the next time it runs.
        try:
            rmdirs(target_dir)
            common.error("Deleted {} due to error".format(target_dir))
        except:
            raise CCMError("Building C* version {} failed. Attempted to delete {} but failed. This will need to be manually deleted".format(version, target_dir))
        raise e


def compile_version(version, target_dir, verbose=False):
    # compiling cassandra and the stress tool
    logfile = lastlogfilename()
    logger = get_logger(logfile)

    common.info("Compiling Cassandra {} ...".format(version))
    logger.info("--- Cassandra Build -------------------\n")

    env = update_java_version(install_dir=target_dir, for_build=True, info_message='Cassandra {} build'.format(version))

    default_build_properties = os.path.join(common.get_default_path(), 'build.properties.default')
    if os.path.exists(default_build_properties):
        target_build_properties = os.path.join(target_dir, 'build.properties')
        logger.info("Copying %s to %s\n" % (default_build_properties, target_build_properties))
        shutil.copyfile(default_build_properties, target_build_properties)

    try:
        # Patch for pending Cassandra issue: https://issues.apache.org/jira/browse/CASSANDRA-5543
        # Similar patch seen with buildbot
        attempt = 0
        ret_val = 1
        gradlew = os.path.join(target_dir, platform_binary('gradlew'))
        if os.path.exists(gradlew):
            cmd = [gradlew, 'jar']
        else:
            # No gradle, use ant
            cmd = [platform_binary('ant'), 'jar']
            if get_jdk_version_int(env=env) >= 11:
                cmd.append('-Duse.jdk11=true')
        while attempt < 3 and ret_val != 0:
            if attempt > 0:
                logger.info("\n\n`{}` failed. Retry #{}...\n\n".format(' '.join(cmd), attempt))
            process = subprocess.Popen(cmd, cwd=target_dir, env=env,
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            ret_val, stdout, stderr = log_info(process, logger)
            attempt += 1
        if ret_val != 0:
            raise CCMError('Error compiling Cassandra. See {logfile} or run '
                           '"ccm showlastlog" for details, stdout=\'{stdout}\' stderr=\'{stderr}\''.format(
                logfile=logfile, stdout=stdout.decode(), stderr=stderr.decode()))
    except OSError as e:
        raise CCMError("Error compiling Cassandra. Is ant installed? See %s for details" % logfile)

    stress_dir = os.path.join(target_dir, "tools", "stress") if (
        version >= "0.8.0") else \
        os.path.join(target_dir, "contrib", "stress")

    build_xml = os.path.join(stress_dir, 'build.xml')
    if os.path.exists(build_xml):  # building stress separately is only necessary pre-1.1
        logger.info("\n\n--- cassandra/stress build ------------\n")
        try:
            # set permissions correctly, seems to not always be the case
            stress_bin_dir = os.path.join(stress_dir, 'bin')
            for f in os.listdir(stress_bin_dir):
                full_path = os.path.join(stress_bin_dir, f)
                os.chmod(full_path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)

            process = subprocess.Popen([platform_binary('ant'), 'build'], cwd=stress_dir, env=env,
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            ret_val, _, _ = log_info(process, logger)
            if ret_val != 0:
                process = subprocess.Popen([platform_binary('ant'), 'stress-build'], cwd=target_dir, env=env,
                                           stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                ret_val, _, _ = log_info(process, logger)
                if ret_val != 0:
                    raise CCMError("Error compiling Cassandra stress tool.  "
                                   "See %s for details (you will still be able to use ccm "
                                   "but not the stress related commands)" % logfile)
        except IOError as e:
            raise CCMError("Error compiling Cassandra stress tool: %s (you will "
                           "still be able to use ccm but not the stress related commands)" % str(e))


def directory_name(version):
    version = version.replace(':', 'COLON')  # handle git branches like 'git:trunk'.
    version = version.replace('/', 'SLASH')  # handle git branches like 'github:mambocab/trunk'.
    return os.path.join(__get_dir(), version)


def github_username_and_branch_name(version):
    assert version.startswith('github')
    return version.split(':', 1)[1].split('/', 1)


def github_repo_for_user(username):
    return 'https://github.com/{username}/cassandra.git'.format(username=username)


def version_directory(version):
    dir = directory_name(version)
    if os.path.exists(dir):
        try:
            validate_install_dir(dir)
            return dir
        except ArgumentError:
            rmdirs(dir)
            return None
    else:
        return None


def clean_all():
    rmdirs(__get_dir())


def get_tagged_version_numbers(series='stable'):
    """Retrieve git tags and find version numbers for a release series

    series - 'stable', 'oldstable', or 'testing'"""
    releases = []
    if series == 'testing':
        # Testing releases always have a hyphen after the version number:
        tag_regex = re.compile('^refs/tags/cassandra-([0-9]+\.[0-9]+\.[0-9]+-.*$)')
    else:
        # Stable and oldstable releases are just a number:
        tag_regex = re.compile('^refs/tags/cassandra-([0-9]+\.[0-9]+\.[0-9]+$)')

    tag_url = urllib.request.urlopen(GITHUB_TAGS)
    for ref in (i.get('ref', '') for i in json.loads(tag_url.read())):
        m = tag_regex.match(ref)
        if m:
            releases.append(LooseVersion(m.groups()[0]))

    # Sort by semver:
    releases.sort(reverse=True)

    stable_major_version = LooseVersion(str(releases[0].version[0]) + "." + str(releases[0].version[1]))
    stable_releases = [r for r in releases if r >= stable_major_version]
    oldstable_releases = [r for r in releases if r not in stable_releases]
    oldstable_major_version = LooseVersion(str(oldstable_releases[0].version[0]) + "." + str(oldstable_releases[0].version[1]))
    oldstable_releases = [r for r in oldstable_releases if r >= oldstable_major_version]

    if series == 'testing':
        return [r.vstring for r in releases]
    elif series == 'stable':
        return [r.vstring for r in stable_releases]
    elif series == 'oldstable':
        return [r.vstring for r in oldstable_releases]
    else:
        raise AssertionError("unknown release series: {series}".format(series=series))


def __download(url, target, username=None, password=None, show_progress=False):
    if username is not None:
        password_mgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()
        password_mgr.add_password(None, url, username, password)
        handler = urllib.request.HTTPBasicAuthHandler(password_mgr)
        opener = urllib.request.build_opener(handler)
        urllib.request.install_opener(opener)  # pylint: disable=E1121

    u = urllib.request.urlopen(url)
    f = open(target, 'wb')
    meta = u.info()
    file_size = int(meta.get("Content-Length"))
    common.info("Downloading {} to {} ({:.3f}MB)".format(url, target, float(file_size) / (1024 * 1024)))

    file_size_dl = 0
    block_sz = 8192
    status = None
    attempts = 0
    while file_size_dl < file_size:
        buffer = u.read(block_sz)
        if not buffer:
            attempts = attempts + 1
            if attempts >= 5:
                raise CCMError("Error downloading file (nothing read after {} attempts, downloded only {} of {} bytes)".format(attempts, file_size_dl, file_size))
            time.sleep(0.5 * attempts)
            continue
        else:
            attempts = 0

        file_size_dl += len(buffer)
        f.write(buffer)
        if show_progress:
            status = r"%10d  [%3.2f%%]" % (file_size_dl, file_size_dl * 100. / file_size)
            status = chr(8) * (len(status) + 1) + status
            print_(status, end='')

    f.close()
    u.close()


def __get_dir():
    repo = os.path.join(get_default_path(), 'repository')
    if not os.path.exists(repo):
        os.mkdir(repo)
    return repo


def lastlogfilename():
    return os.path.join(__get_dir(), "ccm-repository.log")


def get_logger(log_file):
    logger = logging.getLogger('repository')
    logger.addHandler(handlers.RotatingFileHandler(log_file, maxBytes=1024 * 1024 * 5, backupCount=5))
    return logger


def log_info(process, logger):
    stdoutdata, stderrdata = process.communicate()
    rc = process.returncode
    logger.info(stdoutdata.decode())
    logger.info(stderrdata.decode())
    return rc, stdoutdata, stderrdata
