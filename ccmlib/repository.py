# downloaded sources handling
from __future__ import with_statement

import os, shutil, urllib2, tarfile, tempfile, subprocess, stat, time
import common

ARCHIVE="http://archive.apache.org/dist/cassandra"
GIT_REPO="http://git-wip-us.apache.org/repos/asf/cassandra.git"

def setup(version, verbose=False):
    if version.startswith('git:'):
        clone_development(version, verbose=verbose)
        return (version_directory(version), None)
    else:
        cdir = version_directory(version)
        if cdir is None:
            download_version(version, verbose=verbose)
            cdir = version_directory(version)
        return (cdir, version)

def validate(path):
    if path.startswith(__get_dir()):
        _, version = os.path.split(os.path.normpath(path))
        setup(version)

def clone_development(version, verbose=False):
    local_git_cache = os.path.join(__get_dir(), '_git_cache')
    target_dir = os.path.join(__get_dir(), version.replace(':', '_')) # handle git branches like 'git:trunk'.
    git_branch = version[4:] # the part of the version after the 'git:'
    logfile = os.path.join(__get_dir(), "last.log")
    with open(logfile, 'w') as lf:
        try:
            #Checkout/fetch a local repository cache to reduce the number of
            #remote fetches we need to perform:
            if not os.path.exists(local_git_cache):
                if verbose:
                    print "Cloning Cassandra..."
                out = subprocess.call(
                    ['git', 'clone', '--mirror', GIT_REPO, local_git_cache], 
                    cwd=__get_dir(), stdout=lf, stderr=lf)
                assert out == 0, "Could not do a git clone"
            else: 
                if verbose:
                    print "Fetching Cassandra updates..."
                out = subprocess.call(
                    ['git', 'fetch', '-fup', 'origin', '+refs/*:refs/*'],
                    cwd=local_git_cache, stdout=lf, stderr=lf)

            #Checkout the version we want from the local cache:
            if not os.path.exists(target_dir):
                # development branch doesn't exist. Check it out.
                if verbose:
                    print "Cloning Cassandra (from local cache)"
                subprocess.call(['git', 'clone', local_git_cache, target_dir], cwd=__get_dir(), stdout=lf, stderr=lf)
                # now check out the right version
                if verbose:
                    print "Checking out requested branch (%s)" % git_branch
                out = subprocess.call(['git', 'checkout', git_branch], cwd=target_dir, stdout=lf, stderr=lf)
                if int(out) != 0:
                    raise common.CCMError("Could not check out git branch %s. Is this a valid branch name? (see last.log for details)" % git_branch)
                # now compile
                compile_version(git_branch, target_dir, verbose)
            else: # branch is already checked out. See if it is behind and recompile if needed.
                out = subprocess.call(['git', 'fetch', 'origin'], cwd=target_dir, stdout=lf, stderr=lf)
                assert out == 0, "Could not do a git fetch"
                status = subprocess.Popen(['git', 'status', '-sb'], cwd=target_dir, stdout=subprocess.PIPE, stderr=lf).communicate()[0]
                if status.find('[behind') > -1:
                    if verbose:
                        print "Branch is behind, recompiling"
                    out = subprocess.call(['git', 'pull'], cwd=target_dir, stdout=lf, stderr=lf)
                    assert out == 0, "Could not do a git pull"
                    out = subprocess.call(['ant', 'realclean'], cwd=target_dir, stdout=lf, stderr=lf)
                    assert out == 0, "Could not run 'ant realclean'"
                    
                    # now compile
                    compile_version(git_branch, target_dir, verbose)
        except:
            # wipe out the directory if anything goes wrong. Otherwise we will assume it has been compiled the next time it runs.
            try:
                shutil.rmtree(target_dir)
            except: pass
            raise
                

def download_version(version, url=None, verbose=False):
    u = "%s/%s/apache-cassandra-%s-src.tar.gz" % (ARCHIVE, version.split('-')[0], version) if url is None else url
    _, target = tempfile.mkstemp(suffix=".tar.gz", prefix="ccm-")
    try:
        __download(u, target, show_progress=verbose)
        if verbose:
            print "Extracting %s as version %s ..." % (target, version)
        tar = tarfile.open(target)
        dir = tar.next().name.split("/")[0]
        tar.extractall(path=__get_dir())
        tar.close()
        target_dir = os.path.join(__get_dir(), version)
        if os.path.exists(target_dir):
            shutil.rmtree(target_dir)
        shutil.move(os.path.join(__get_dir(), dir), target_dir)

        compile_version(version, target_dir, verbose=verbose)

    except urllib2.URLError as e:
        msg = "Invalid version %s" % version if url is None else "Invalid url %s" % url
        msg = msg + " (underlying error is: %s)" % str(e)
        raise common.ArgumentError(msg)
    except tarfile.ReadError as e:
        raise common.ArgumentError("Unable to uncompress downloaded file: %s" % str(e))

def compile_version(version, target_dir, verbose=False):
    # compiling cassandra and the stress tool
    logfile = os.path.join(__get_dir(), "last.log")
    if verbose:
        print "Compiling Cassandra %s ..." % version
    with open(logfile, 'w') as lf:
        lf.write("--- Cassandra build -------------------\n")
        try:
            if subprocess.call(['ant', 'jar'], cwd=target_dir, stdout=lf, stderr=lf) is not 0:
                raise common.CCMError("Error compiling Cassandra. See %s for details" % logfile)
        except OSError, e:
            raise common.CCMError("Error compiling Cassandra. Is ant installed? See %s for details" % logfile)
        
        lf.write("\n\n--- cassandra/stress build ------------\n")
        stress_dir = os.path.join(target_dir, "tools", "stress") if (
                version >= "0.8.0") else \
                os.path.join(target_dir, "contrib", "stress")

        build_xml = os.path.join(stress_dir, 'build.xml')
        if os.path.exists(build_xml): # building stress separately is only necessary pre-1.1
            try:
                # set permissions correctly, seems to not always be the case
                stress_bin_dir = os.path.join(stress_dir, 'bin')
                for f in os.listdir(stress_bin_dir):
                    full_path = os.path.join(stress_bin_dir, f)
                    os.chmod(full_path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)

                if subprocess.call(['ant', 'build'], cwd=stress_dir, stdout=lf, stderr=lf) is not 0:
                    if subprocess.call(['ant', 'stress-build'], cwd=target_dir, stdout=lf, stderr=lf) is not 0:
                        raise common.CCMError("Error compiling Cassandra stress tool.  "
                                "See %s for details (you will still be able to use ccm "
                                "but not the stress related commands)" % logfile)
            except IOError as e:
                raise common.CCMError("Error compiling Cassandra stress tool: %s (you will "
                "still be able to use ccm but not the stress related commands)" % str(e))

def version_directory(version):
    version = version.replace(':', '_') # handle git branches like 'git:trunk'.
    dir = os.path.join(__get_dir(), version)
    if os.path.exists(dir):
        try:
            common.validate_cassandra_dir(dir)
            return dir
        except common.ArgumentError as e:
            shutil.rmtree(dir)
            return None
    else:
        return None

def clean_all():
    shutil.rmtree(__get_dir())

def __download(url, target, show_progress=False):
    u = urllib2.urlopen(url)
    f = open(target, 'wb')
    meta = u.info()
    file_size = int(meta.getheaders("Content-Length")[0])
    if show_progress:
        print "Downloading %s to %s (%.3fMB)" % (url, target, float(file_size) / (1024 * 1024))

    file_size_dl = 0
    block_sz = 8192
    status = None
    attempts = 0
    while file_size_dl < file_size:
        buffer = u.read(block_sz)
        if not buffer:
            attempts = attempts + 1
            if attempts >= 5:
                raise common.CCMError("Error downloading file (nothing read after %i attemps, downloded only %i of %i bytes)" % (attempts, file_size_dl, file_size))
            time.sleep(0.5 * attempts)
            continue;
        else:
            attemps = 0

        file_size_dl += len(buffer)
        f.write(buffer)
        if show_progress:
            status = r"%10d  [%3.2f%%]" % (file_size_dl, file_size_dl * 100. / file_size)
            status = chr(8)*(len(status)+1) + status
            print status,

    if show_progress:
        print ""
    f.close()
    u.close()

def __get_dir():
    repo = os.path.join(common.get_default_path(), 'repository')
    if not os.path.exists(repo):
        os.mkdir(repo)
    return repo
