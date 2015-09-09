# downloaded sources handling
from __future__ import with_statement

import json
import os
import re
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
import time
from distutils.version import LooseVersion

from six import print_

from ccmlib.common import (ArgumentError, CCMError, get_default_path,
                           platform_binary, rmdirs, validate_install_dir)
from six.moves import urllib

DSE_ARCHIVE = "http://downloads.datastax.com/enterprise/dse-%s-bin.tar.gz"
OPSC_ARCHIVE = "http://downloads.datastax.com/community/opscenter-%s.tar.gz"
ARCHIVE = "http://archive.apache.org/dist/cassandra"
GIT_REPO = "http://git-wip-us.apache.org/repos/asf/cassandra.git"
GITHUB_TAGS = "https://api.github.com/repos/apache/cassandra/git/refs/tags"


def setup(version, verbose=False):
    binary = False
    if version.startswith('git:'):
        clone_development(GIT_REPO, version, verbose=verbose)
        return (version_directory(version), None)
    elif version.startswith('binary:'):
        version = version.replace('binary:', '')
        binary = True
    elif version.startswith('github:'):
        user_name, _ = github_username_and_branch_name(version)
        clone_development(github_repo_for_user(user_name), version, verbose=verbose)
        return (directory_name(version), None)
    if version in ('stable', 'oldstable', 'testing'):
        version = get_tagged_version_numbers(version)[0]
    cdir = version_directory(version)
    if cdir is None:
        download_version(version, verbose=verbose, binary=binary)
        cdir = version_directory(version)
    return (cdir, version)


def setup_dse(version, username, password, verbose=False):
    cdir = version_directory(version)
    if cdir is None:
        download_dse_version(version, username, password, verbose=verbose)
        cdir = version_directory(version)
    return (cdir, version)


def setup_opscenter(opscenter, verbose=False):
    ops_version = 'opsc' + opscenter
    odir = version_directory(ops_version)
    if odir is None:
        download_opscenter_version(opscenter, ops_version, verbose=verbose)
        odir = version_directory(ops_version)
    return odir


def validate(path):
    if path.startswith(__get_dir()):
        _, version = os.path.split(os.path.normpath(path))
        setup(version)


def clone_development(git_repo, version, verbose=False):
    print_(git_repo, version)
    target_dir = directory_name(version)
    assert target_dir
    if 'github' in version:
        git_repo_name, git_branch = github_username_and_branch_name(version)
    else:
        git_repo_name = 'apache'
        git_branch = version.split(':', 1)[1]
    local_git_cache = os.path.join(__get_dir(), '_git_cache_' + git_repo_name)
    logfile = lastlogfilename()
    with open(logfile, 'w') as lf:
        try:
            # Checkout/fetch a local repository cache to reduce the number of
            # remote fetches we need to perform:
            if not os.path.exists(local_git_cache):
                if verbose:
                    print_("Cloning Cassandra...")
                out = subprocess.call(
                    ['git', 'clone', '--mirror', git_repo, local_git_cache],
                    cwd=__get_dir(), stdout=lf, stderr=lf)
                assert out == 0, "Could not do a git clone"
            else:
                if verbose:
                    print_("Fetching Cassandra updates...")
                out = subprocess.call(
                    ['git', 'fetch', '-fup', 'origin', '+refs/*:refs/*'],
                    cwd=local_git_cache, stdout=lf, stderr=lf)

            # Checkout the version we want from the local cache:
            if not os.path.exists(target_dir):
                # development branch doesn't exist. Check it out.
                if verbose:
                    print_("Cloning Cassandra (from local cache)")

                # git on cygwin appears to be adding `cwd` to the commands which is breaking clone
                if sys.platform == "cygwin":
                    local_split = local_git_cache.split(os.sep)
                    target_split = target_dir.split(os.sep)
                    subprocess.call(['git', 'clone', local_split[-1], target_split[-1]], cwd=__get_dir(), stdout=lf, stderr=lf)
                else:
                    subprocess.call(['git', 'clone', local_git_cache, target_dir], cwd=__get_dir(), stdout=lf, stderr=lf)

                # now check out the right version
                if verbose:
                    print_("Checking out requested branch (%s)" % git_branch)
                out = subprocess.call(['git', 'checkout', git_branch], cwd=target_dir, stdout=lf, stderr=lf)
                if int(out) != 0:
                    raise CCMError('Could not check out git branch {branch}. '
                                   'Is this a valid branch name? (see {lastlog} or run '
                                   '"ccm showlastlog" for details)'.format(
                                       branch=git_branch, lastlog=logfile
                                   ))
                # now compile
                compile_version(git_branch, target_dir, verbose)
            else:  # branch is already checked out. See if it is behind and recompile if needed.
                out = subprocess.call(['git', 'fetch', 'origin'], cwd=target_dir, stdout=lf, stderr=lf)
                assert out == 0, "Could not do a git fetch"
                status = subprocess.Popen(['git', 'status', '-sb'], cwd=target_dir, stdout=subprocess.PIPE, stderr=lf).communicate()[0]
                if str(status).find('[behind') > -1:
                    if verbose:
                        print_("Branch is behind, recompiling")
                    out = subprocess.call(['git', 'pull'], cwd=target_dir, stdout=lf, stderr=lf)
                    assert out == 0, "Could not do a git pull"
                    out = subprocess.call([platform_binary('ant'), 'realclean'], cwd=target_dir, stdout=lf, stderr=lf)
                    assert out == 0, "Could not run 'ant realclean'"

                    # now compile
                    compile_version(git_branch, target_dir, verbose)
        except:
            # wipe out the directory if anything goes wrong. Otherwise we will assume it has been compiled the next time it runs.
            try:
                rmdirs(target_dir)
                print_("Deleted %s due to error" % target_dir)
            except:
                raise CCMError("Building C* version %s failed. Attempted to delete %s but failed. This will need to be manually deleted" % (version, target_dir))
            raise


def download_dse_version(version, username, password, verbose=False):
    url = DSE_ARCHIVE % version
    _, target = tempfile.mkstemp(suffix=".tar.gz", prefix="ccm-")
    try:
        __download(url, target, username=username, password=password, show_progress=verbose)
        if verbose:
            print_("Extracting %s as version %s ..." % (target, version))
        tar = tarfile.open(target)
        dir = tar.next().name.split("/")[0]
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


def download_opscenter_version(version, target_version, verbose=False):
    url = OPSC_ARCHIVE % version
    _, target = tempfile.mkstemp(suffix=".tar.gz", prefix="ccm-")
    try:
        __download(url, target, show_progress=verbose)
        if verbose:
            print_("Extracting %s as version %s ..." % (target, target_version))
        tar = tarfile.open(target)
        dir = tar.next().name.split("/")[0]
        tar.extractall(path=__get_dir())
        tar.close()
        target_dir = os.path.join(__get_dir(), target_version)
        if os.path.exists(target_dir):
            rmdirs(target_dir)
        shutil.move(os.path.join(__get_dir(), dir), target_dir)
    except urllib.error.URLError as e:
        msg = "Invalid version %s" % version if url is None else "Invalid url %s" % url
        msg = msg + " (underlying error is: %s)" % str(e)
        raise ArgumentError(msg)
    except tarfile.ReadError as e:
        raise ArgumentError("Unable to uncompress downloaded file: %s" % str(e))


def download_version(version, url=None, verbose=False, binary=False):
    """Download, extract, and build Cassandra tarball.

    if binary == True, download precompiled tarball, otherwise build from source tarball.
    """
    if binary:
        u = "%s/%s/apache-cassandra-%s-bin.tar.gz" % (ARCHIVE, version.split('-')[0], version) if url is None else url
    else:
        u = "%s/%s/apache-cassandra-%s-src.tar.gz" % (ARCHIVE, version.split('-')[0], version) if url is None else url
    _, target = tempfile.mkstemp(suffix=".tar.gz", prefix="ccm-")
    try:
        __download(u, target, show_progress=verbose)
        if verbose:
            print_("Extracting %s as version %s ..." % (target, version))
        tar = tarfile.open(target)
        dir = tar.next().name.split("/")[0]
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
        msg = "Invalid version %s" % version if url is None else "Invalid url %s" % url
        msg = msg + " (underlying error is: %s)" % str(e)
        raise ArgumentError(msg)
    except tarfile.ReadError as e:
        raise ArgumentError("Unable to uncompress downloaded file: %s" % str(e))
    except CCMError as e:
        # wipe out the directory if anything goes wrong. Otherwise we will assume it has been compiled the next time it runs.
        try:
            rmdirs(target_dir)
            print_("Deleted %s due to error" % target_dir)
        except:
            raise CCMError("Building C* version %s failed. Attempted to delete %s but failed. This will need to be manually deleted" % (version, target_dir))
        raise e


def compile_version(version, target_dir, verbose=False):
    # compiling cassandra and the stress tool
    logfile = lastlogfilename()
    if verbose:
        print_("Compiling Cassandra %s ..." % version)
    with open(logfile, 'w') as lf:
        lf.write("--- Cassandra Build -------------------\n")
        try:
            # Patch for pending Cassandra issue: https://issues.apache.org/jira/browse/CASSANDRA-5543
            # Similar patch seen with buildbot
            attempt = 0
            ret_val = 1
            while attempt < 3 and ret_val is not 0:
                if attempt > 0:
                    lf.write("\n\n`ant jar` failed. Retry #%s...\n\n" % attempt)
                ret_val = subprocess.call([platform_binary('ant'), 'jar'], cwd=target_dir, stdout=lf, stderr=lf)
                attempt += 1
            if ret_val is not 0:
                raise CCMError('Error compiling Cassandra. See {logfile} or run '
                               '"ccm showlastlog" for details'.format(logfile=logfile))
        except OSError as e:
            raise CCMError("Error compiling Cassandra. Is ant installed? See %s for details" % logfile)

        lf.write("\n\n--- cassandra/stress build ------------\n")
        stress_dir = os.path.join(target_dir, "tools", "stress") if (
            version >= "0.8.0") else \
            os.path.join(target_dir, "contrib", "stress")

        build_xml = os.path.join(stress_dir, 'build.xml')
        if os.path.exists(build_xml):  # building stress separately is only necessary pre-1.1
            try:
                # set permissions correctly, seems to not always be the case
                stress_bin_dir = os.path.join(stress_dir, 'bin')
                for f in os.listdir(stress_bin_dir):
                    full_path = os.path.join(stress_bin_dir, f)
                    os.chmod(full_path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)

                if subprocess.call([platform_binary('ant'), 'build'], cwd=stress_dir, stdout=lf, stderr=lf) is not 0:
                    if subprocess.call([platform_binary('ant'), 'stress-build'], cwd=target_dir, stdout=lf, stderr=lf) is not 0:
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
    return 'git@github.com:{username}/cassandra.git'.format(username=username)


def version_directory(version):
    dir = directory_name(version)
    if os.path.exists(dir):
        try:
            validate_install_dir(dir)
            return dir
        except ArgumentError as e:
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

    r = urllib.request.urlopen(GITHUB_TAGS)
    for ref in (i.get('ref', '') for i in json.loads(r.read())):
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
        urllib.request.install_opener(opener)

    u = urllib.request.urlopen(url)
    f = open(target, 'wb')
    meta = u.info()
    file_size = int(meta.get("Content-Length"))
    if show_progress:
        print_("Downloading %s to %s (%.3fMB)" % (url, target, float(file_size) / (1024 * 1024)))

    file_size_dl = 0
    block_sz = 8192
    status = None
    attempts = 0
    while file_size_dl < file_size:
        buffer = u.read(block_sz)
        if not buffer:
            attempts = attempts + 1
            if attempts >= 5:
                raise CCMError("Error downloading file (nothing read after %i attempts, downloded only %i of %i bytes)" % (attempts, file_size_dl, file_size))
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

    if show_progress:
        print_("")
    f.close()
    u.close()


def __get_dir():
    repo = os.path.join(get_default_path(), 'repository')
    if not os.path.exists(repo):
        os.mkdir(repo)
    return repo


def lastlogfilename():
    return os.path.join(__get_dir(), "last.log")
