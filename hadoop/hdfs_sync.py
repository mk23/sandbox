#!/usr/bin/env python2.7

import argparse
import datetime
import errno
import fnmatch
import getpass
import itertools
import json
import multiprocessing.pool
import os
import pickle
import shutil
import socket
import stat
import subprocess
import sys
import syslog
import tempfile
import time
import traceback
import urlparse
import webhdfs


class SyncFile(object):
    _archive_suffixes = {
        '.zip':     'unzip',
        '.txz':     'tarxz',
        '.tar.xz':  'tarxz',
        '.tgz':     'targz',
        '.tar.gz':  'targz',
        '.tbz2':    'tarbz2',
        '.tar.bz2': 'tarbz2',
    }
    _archive_commands = {
        'unzip':  ['unzip', '-qq'],
        'targz':  ['tar', '-xzf'],
        'tarxz':  ['tar', '-xJf'],
        'tarbz2': ['tar', '-xjf'],
    }

    def __init__(self, remote, source, mirror, unpack):
        self.remote = remote
        self.source = source
        self.mirror = mirror
        self.unpack = unpack

    @property
    def fullname(self):
        return self.mirror + self.remote.full[len(self.source):]

    @property
    def zip_path(self):
        for e, t in self._archive_suffixes.items():
            if self.fullname.endswith(e):
                return self.unpack + self.remote.full[len(self.source):-len(e)]

    @property
    def zip_exec(self):
        for e, t in self._archive_suffixes.items():
            if self.fullname.endswith(e):
                return self._archive_commands[t]

    @property
    def filetime(self, other=None):
        return time.mktime(self.remote.date.timetuple())

    @property
    def modified(self):
        return not os.path.exists(self.fullname) or self.filetime < os.stat(self.fullname).st_mtime

    def equal(self, item):
        return self.remote.size == item.remote.size and self.remote.date == item.remote.date

    def mkdir(self, path=None):
        try:
            os.makedirs(path)
            syslog.syslog(syslog.LOG_NOTICE, 'created local directory: %s' % path)
        except OSError as e:
            if e.errno != errno.EEXIST or not os.path.exists(path):
                raise

    def rmdir(self, path):
        limit = max(os.path.commonprefix(i) for i in [[self.mirror, path], [self.unpack, path]])

        if not limit:
            syslog.syslog(syslog.LOG_ERR, 'request to purge untracked path: %s' % path)

        while True:
            path = os.path.dirname(path)
            if path == limit:
                break

            try:
                os.rmdir(path)
                syslog.syslog(syslog.LOG_NOTICE, 'purged local empty directory: %s' % path)
            except OSError as e:
                if e.errno == errno.ENOTEMPTY:
                    break
                else:
                    raise

    def purge(self, skip=False):
        if skip:
            syslog.syslog(syslog.LOG_INFO, 'purging local file: %s' % self.fullname)
            return True

        try:
            if self.zip_path:
                shutil.rmtree(self.zip_path)
                syslog.syslog(syslog.LOG_NOTICE, 'purged local unpacked directory: %s' % self.zip_path)

                self.rmdir(self.zip_path)

            os.unlink(self.fullname)
            syslog.syslog(syslog.LOG_NOTICE, 'purged local file: %s' % self.fullname)

            self.rmdir(self.fullname)
            return True
        except Exception as e:
            if isinstance(e, OSError) and e.errno != errno.ENOENT:
                log_exception(e)

    def unzip(self, temp):
        path = self.zip_path
        if not path:
            return

        data = tempfile.mkdtemp(dir=temp)
        save = '%s.__%s__' % (path, os.path.basename(data))
        try:
            syslog.syslog(syslog.LOG_DEBUG, 'created temporary unpack path: %s' % data)

            os.chdir(data)
            subprocess.check_call(self.zip_exec + [self.fullname])
            syslog.syslog(syslog.LOG_DEBUG, 'unpacked %s into %s' % (self.fullname, data))

            self.mkdir(path)

            os.rename(path, save)
            os.rename(data, path)
            os.chmod(path, 0o755)
            shutil.rmtree(save)

            syslog.syslog(syslog.LOG_NOTICE, 'moved unpacked path %s to %s' % (data, path))
        except Exception as e:
            log_exception(e)

    def fetch(self, hdfs, temp=tempfile.gettempdir(), skip=False):
        if skip:
            syslog.syslog(syslog.LOG_INFO, 'fetching hdfs file: %s' % self.remote.full)
            return True

        try:
            with tempfile.NamedTemporaryFile(dir=temp, delete=False) as data:
                syslog.syslog(syslog.LOG_DEBUG, 'created temp file: %s' % data.name)

                hdfs.get(self.remote.full, data=data)
                syslog.syslog(syslog.LOG_NOTICE, 'fetched hdfs file: %s' % self.remote.full)

            self.mkdir(os.path.dirname(self.fullname))

            os.chmod(data.name, os.stat(data.name).st_mode|stat.S_IRGRP|stat.S_IROTH)
            os.utime(data.name, (self.filetime, self.filetime))
            os.rename(data.name, self.fullname)

            self.unzip(temp)

            syslog.syslog(syslog.LOG_DEBUG, 'renamed temp file from %s to %s' % (data.name, self.fullname))
            return True
        except Exception as e:
            log_exception(e)
            if os.path.exists(data.name):
                try:
                    os.remove(data.name)
                except Exception as e:
                    log_exception(e)

    def check(self, last, skip=False):
        if skip:
            syslog.syslog(syslog.LOG_INFO, 'processing dataset manifest: %s' % self.fullname)

        try:
            data = json.load(open(self.fullname))
            syslog.syslog(syslog.LOG_INFO, 'processing %d items from manifest: %s' % (len(data['files']), self.fullname))

            for path, paths, files in os.walk(os.path.dirname(self.fullname)):
                for name in files:
                    full = '%s/%s' % (path, name)
                    part = full[len(os.path.dirname(self.fullname))+1:]
                    info = os.stat(full)

                    if full == self.fullname or stat.S_ISLNK(info.st_mode):
                        continue

                    if part not in data['files']:
                        syslog.syslog(syslog.LOG_WARNING, '  file not found in manifest, skipping: %s' % full)
                    else:
                        data['files'][part]['stat'] = os.stat(full)


            find = list(i for i, j in data['files'].items() if 'stat' not in j)
            if find:
                syslog.syslog(syslog.LOG_WARNING, '  manifest is missing %d file(s), aborting:' % len(find))
                for item in find:
                    syslog.syslog(syslog.LOG_WARNING, '    %s' % item)
                return False

            find = list(i for i, j in data['files'].items() if j['size'] != j['stat'].st_size)
            if find:
                syslog.syslog(syslog.LOG_WARNING, '  manifest has %d invalid file(s), aborting:' % len(find))
                for item in find:
                    syslog.syslog(syslog.LOG_WARNING, '    %s (expected: %d bytes, observed: %d bytes)' % (item, data['files'][item]['size'], data['files'][item]['stat'].st_size))
                return False

            find = list(i for i, j in data['files'].items() if j['stat'].st_mtime > last)
            if not find and os.stat(self.fullname).st_mtime < last:
                syslog.syslog(syslog.LOG_INFO,  '  manifest has no updates, skipping')
                return True

            os.chdir(os.path.dirname(self.fullname))
            subprocess.check_call([data['script'], self.fullname])
            # FIXME: do something when command fails to allow retries

            syslog.syslog(syslog.LOG_NOTICE, '  executed dataset manifest command: %s %s' % (data['script'], self.fullname))
            return True
        except Exception as e:
            log_exception(e)


def log_exception(ex):
    syslog.syslog(syslog.LOG_ERR, str(ex))
    for line in traceback.format_exc().split('\n'):
        syslog.syslog(syslog.LOG_DEBUG, '  %s' % line)

def setup_syslog(dest, debug=False):
    if dest == 'console':
        prio = dict((v, k[4:]) for k, v in vars(syslog).items() if type(v) == int and (v & 7) > 0 and k not in ('LOG_PID', 'LOG_CONS'))
        syslog.syslog = lambda *m: sys.stdout.write('%s %8s: %s\n' % (str(datetime.datetime.now()), prio[m[0]], m[1])) if m[0] != syslog.LOG_DEBUG or debug else True
    elif dest is not None:
        syslog.openlog(ident=os.path.basename(sys.argv[0]), logoption=syslog.LOG_PID, facility=getattr(syslog, 'LOG_%s' % dest.upper()))
    else:
        syslog.syslog = lambda *m: True

    syslog.setlogmask(syslog.LOG_UPTO(syslog.LOG_DEBUG if debug else syslog.LOG_INFO))
    syslog.syslog(syslog.LOG_DEBUG, 'logging started')

def setup_socket(port, sock=socket.socket()):
    try:
        sock.bind(('127.0.0.1', port))
        sock.listen(1)
        syslog.syslog(syslog.LOG_DEBUG, 'bound local listening socket for mutual exclusion on port: %d' % port)
    except socket.error as e:
        if e.errno == errno.EADDRINUSE:
            syslog.syslog(syslog.LOG_ERR, 'another hdfs_sync process is already running')
        else:
            log_exception(e)
        sys.exit(1)

def setup_local(index, mirror, unpack):
    local = {}

    syslog.syslog(syslog.LOG_DEBUG, 'reading local index: %s' % index)
    try:
        for key, val in pickle.load(open(index)).items():
            if val.mirror != mirror:
                syslog.syslog(syslog.LOG_WARNING, 'detected mirror directory move from %s to %s' % (val.mirror, mirror))
                val.mirror = mirror
            if val.unpack != unpack:
                syslog.syslog(syslog.LOG_WARNING, 'detected unpack directory move from %s to %s' % (val.unpack, unpack))
                val.unpack = unpack

            if not val.modified:
                local[key] = val
            else:
                syslog.syslog(syslog.LOG_WARNING, 'file changed or disappeared: %s' % key)
    except Exception as e:
        if getattr(e, 'errno', None) == errno.ENOENT:
            syslog.syslog(syslog.LOG_WARNING, 'no local index available, performing full fetch')
        else:
            log_exception(e)

    syslog.syslog(syslog.LOG_INFO, 'loaded local index containing %d items' % len(local))
    return local

def setup_items(client, source, cache, force):
    index = []

    if not force:
        try:
            syslog.syslog(syslog.LOG_DEBUG, 'fetching file index: %s/%s' % (source, cache))
            for item in pickle.loads(client.get('%s/%s' % (source, cache))):
                if isinstance(item, webhdfs.WebHDFSObject):
                    index.append(item)
                else:
                    raise TypeError('found invalid cache item type %s' % type(item))

            syslog.syslog(syslog.LOG_INFO, 'loaded cached index with %d items' % len(index))
        except (webhdfs.errors.WebHDFSFileNotFoundError, TypeError) as e:
            log_exception(e)

    if not index:
        syslog.syslog(syslog.LOG_DEBUG, 'fetching file list')
        index = list(client.ls(source, recurse=True, request=lambda x: x.full != '%s/%s' % (source, cache) if cache else True))

        syslog.syslog(syslog.LOG_INFO, 'loaded recursive index with %d items' % len(index))

    return index


def setup_avail(report, source, filter, mirror, unpack):
    avail = {}

    syslog.syslog(syslog.LOG_DEBUG, 'processing file index')
    for item in report:
        if item.name.endswith('_COPYING_'):
            syslog.syslog(syslog.LOG_DEBUG, 'skipping transferring hdfs object: %s' % item.full)
            continue
        if item.is_dir():
            syslog.syslog(syslog.LOG_DEBUG, 'skipping directory: %s' % item.full)
            continue

        try:
            for find in filter:
                if fnmatch.fnmatch(item.full[len(source) + 1:], find):
                    syslog.syslog(syslog.LOG_INFO, 'queueing hdfs object: %s' % item.full)
                    avail[item.full] = SyncFile(item, source, mirror, unpack)
                    break
            else:
                syslog.syslog(syslog.LOG_DEBUG, 'skipping excluded hdfs object: %s' % item.full)
        except Exception as e:
            log_exception(e)

    syslog.syslog(syslog.LOG_INFO, 'read remote list containing %d items' % len(avail))
    return avail

def clean_local(index, local, mirror, unpack, skip=False):
    mirrored = list(i.fullname for i in local.values())
    unpacked = list(i.zip_path for i in local.values() if i.zip_path)

    for path, paths, files in os.walk(mirror, topdown=False):
        for name in files:
            full = '%s/%s' % (path, name)
            if full not in mirrored:
                syslog.syslog(syslog.LOG_INFO, 'removing orphaned local file: %s' % full)
                if not skip:
                    os.unlink(full)

        if not skip:
            for name in paths:
                full = '%s/%s' % (path, name)
                try:
                    os.rmdir(full)
                    syslog.syslog(syslog.LOG_NOTICE, 'removed orphaned local empty directory: %s' % full)
                except OSError as e:
                    if e.errno != errno.ENOTEMPTY:
                        raise e

    for path, paths, files in os.walk(unpack, topdown=False):
        for name in paths:
            full = '%s/%s' % (path, name)
            for arch in unpacked:
                same = os.path.commonprefix([full, arch])
                if same == arch or same == full:
                    paths.remove(name)
                    break
            else:
                syslog.syslog(syslog.LOG_INFO, 'removing orphaned unpacked local directory: %s' % full)
                if not skip:
                    shutil.rmtree(full)


def begin_cache(client, source, report, args):
    if args.dry_run:
        syslog.syslog(syslog.LOG_INFO, 'uploading new %d item index to %s/%s' % (len(report), source, args.ls_cache))
        return

    try:
        client.mv('%s/%s' % (source, args.ls_cache), '%s/%s.old' % (source, args.ls_cache))
        syslog.syslog(syslog.LOG_NOTICE, 'renamed cached index %s/%s to %s/%s.old' % (source, args.ls_cache, source, args.ls_cache))

        client.put('%s/%s' % (source, args.ls_cache), pickle.dumps(report))
        syslog.syslog(syslog.LOG_NOTICE, 'uploaded new %d item index to %s/%s' % (len(report), source, args.ls_cache))
    except Exception as e:
        syslog.syslog(syslog.LOG_ERR, 'failed to save new index %s/%s' % (source, args.ls_cache))
        log_exception(e)
        try:
            client.mv('%s/%s.old' % (source, args.ls_cache), '%s/%s' % (source, args.ls_cache))
            syslog.syslog(syslog.LOG_NOTICE, 'restored cached index %s/%s.old to %s/%s' % (source, args.ls_cache, source, args.ls_cache))
        except Exception as e:
            syslog.syslog(syslog.LOG_ERR, 'failed to restore old index from %s/%s.old to %s/%s' % (source, args.ls_cache, source, args.ls_cache))
            log_exception(e)
    finally:
        try:
            client.rm('%s/%s.old' % (source, args.ls_cache))
            syslog.syslog(syslog.LOG_NOTICE, 'removed saved index %s/%s.old' % (source, args.ls_cache))
        except Exception as e:
            syslog.syslog(syslog.LOG_ERR, 'failed to remove old index %s/%s.old' % (source, args.ls_cache))
            log_exception(e)


def begin_fetch(client, source, report, args):
    dest_dir = os.path.abspath(args.dest_dir)
    temp_dir = os.path.abspath(args.temp_dir)
    sync_dir = os.path.normpath('%s/%s' % (dest_dir, args.sync_dir))
    arch_dir = os.path.normpath('%s/%s' % (dest_dir, args.arch_dir))
    includes = set(itertools.chain.from_iterable(args.includes)) or ['*']

    if os.stat(dest_dir).st_dev != os.stat(temp_dir).st_dev:
        syslog.syslog(syslog.LOG_ERR, 'destination and temp directores are cross-device')
        sys.exit(1)

    if not os.path.exists(sync_dir):
        if not args.dry_run:
            os.makedirs(sync_dir, 0o755)
            syslog.syslog(syslog.LOG_NOTICE, 'created mirror path: %s' % sync_dir)
        else:
            syslog.syslog(syslog.LOG_INFO, 'creating mirror path: %s' % sync_dir)
    if not os.path.exists(arch_dir):
        if not args.dry_run:
            os.makedirs(arch_dir, 0o755)
            syslog.syslog(syslog.LOG_NOTICE, 'created unpack path: %s' % arch_dir)
        else:
            syslog.syslog(syslog.LOG_INFO, 'creating unpack path: %s' % arch_dir)

    index = os.path.normpath('%s/.%s.idx' % (dest_dir, os.path.splitext(os.path.basename(sys.argv[0]))[0]))
    local = setup_local(index, sync_dir, arch_dir)
    avail = setup_avail(report, source, includes, sync_dir, arch_dir)
    procs = multiprocessing.pool.ThreadPool(processes=args.workers)
    xfers = {}

    for key, val in avail.items():
        if (key not in local or not val.equal(local[key])):
            xfers[key] = procs.apply_async(val.fetch, (client, temp_dir, args.dry_run))
    procs.close()
    for key, val in xfers.items():
        if val.get():
            local[key] = avail[key]
        else:
            syslog.syslog(syslog.LOG_ERR, 'failed to fetch %s' % key)
    procs.join()
    for key, val in local.items():
        if key not in avail and val.purge(args.dry_run):
            del(local[key])

    for key, val in local.items():
        if os.path.basename(val.fullname) == args.manifest:
            val.check(os.stat(index).st_mtime if os.path.exists(index) else time.mktime(datetime.datetime.min.timetuple()), args.dry_run)

    if not args.dry_run:
        pickle.dump(local, open(index, 'w'))

    clean_local(index, local, sync_dir, arch_dir, args.dry_run)


if __name__ == '__main__':
    master_parser = argparse.ArgumentParser(description='hdfs directory sync')
    master_parser.add_argument('-u', '--hdfs-url', required=True,
                               help='full hdfs url to sync')
    master_parser.add_argument('-c', '--ls-cache', default='.%s.idx' % os.path.splitext(os.path.basename(sys.argv[0]))[0],
                               help='create hdfs file index cache, and exit')
    master_parser.add_argument('-p', '--run-port', default=2311, type=int,
                               help='lock loopback port number')
    master_parser.add_argument('-l', '--log-dest', choices=['console', 'user', 'daemon'] + ['local%d' % i for i in xrange(8)],
                               help='syslog facility')
    master_parser.add_argument('-n', '--dry-run', default=False, action='store_true',
                               help='show actions to be performed')
    master_parser.add_argument('-v', '--verbose', default=False, action='store_true',
                               help='log extra debugging messages')
    master_parser.add_argument('-o', '--timeout', default=4, type=float,
                               help='request timeout in seconds')

    slave_parsers = master_parser.add_subparsers(dest='command')

    parser = slave_parsers.add_parser('fetch')
    parser.add_argument('-d', '--dest-dir',
                        help='destination directory')
    parser.add_argument('-i', '--includes', default=[], action='append', nargs='*',
                        help='explicit file globs instead of all')
    parser.add_argument('-t', '--temp-dir', default='/tmp',
                        help='where to put the temporary directory for downloads')
    parser.add_argument('-s', '--sync-dir', default='mirror',
                        help='relative directory to mirror sources into')
    parser.add_argument('-e', '--arch-dir', default='unpack',
                        help='relative directory to unpack archives into')
    parser.add_argument('-m', '--manifest',
                        help='manifest file name to watch for and process')
    parser.add_argument('-w', '--workers', type=int, default=multiprocessing.cpu_count(),
                        help='number of download threads')

    parser = slave_parsers.add_parser('cache')
    args = master_parser.parse_args()

    setup_syslog(args.log_dest, args.verbose)
    setup_socket(args.run_port)

    start_ts = datetime.datetime.now()
    hdfs_url = urlparse.urlparse(args.hdfs_url)
    hdfs_dir = hdfs_url.path
    hdfs_api = webhdfs.WebHDFSClient(hdfs_url._replace(path='').geturl(), user=getpass.getuser(), wait=args.timeout)

    try:
        ls_items = setup_items(hdfs_api, hdfs_dir, args.ls_cache, args.command=='cache')
        getattr(sys.modules['__main__'], 'begin_'+args.command)(hdfs_api, hdfs_dir, ls_items, args)

        syslog.syslog(syslog.LOG_INFO, 'execution completed in %.02fs' % (datetime.datetime.now() - start_ts).total_seconds())
        syslog.syslog(syslog.LOG_DEBUG, 'execution required %d webhdfs call%s' % (hdfs_api.calls, 's' if hdfs_api.calls != 1 else ''))
    except Exception as e:
        log_exception(e)
