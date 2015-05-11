#!/usr/bin/env python2.7

import argparse
import datetime
import errno
import getpass
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
    def modified(self):
        return not os.path.exists(self.fullname) or getattr(self, 'mtime', 0) < os.stat(self.fullname).st_mtime

    def equal(self, item):
        return self.remote.size == item.remote.size and self.remote.date == item.remote.date

    def mkdir(self, path=None):
        try:
            os.makedirs(path)
            syslog.syslog(syslog.LOG_INFO, 'created local directory: %s' % path)
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
                syslog.syslog(syslog.LOG_INFO, 'purged local empty directory: %s' % path)
            except OSError as e:
                if e.errno == errno.ENOTEMPTY:
                    break
                else:
                    raise

    def purge(self, skip=False):
        if skip:
            return True

        try:
            if self.zip_path:
                shutil.rmtree(self.zip_path)
                syslog.syslog(syslog.LOG_INFO, 'purged local unpacked directory: %s' % self.zip_path)

                self.rmdir(self.zip_path)

            os.unlink(self.fullname)
            syslog.syslog(syslog.LOG_INFO, 'purged local file: %s' % self.fullname)

            self.rmdir(self.fullname)
            return True
        except Exception as e:
            if isinstance(e, OSError) and e.errno != errno.ENOENT:
                log_exception(e)

    def unzip(self):
        try:
            if self.zip_path:
                self.mkdir(self.zip_path)

                os.chdir(self.zip_path)
                subprocess.check_call(self.zip_exec + [self.fullname])
                syslog.syslog(syslog.LOG_INFO, 'unpacked %s into %s' % (self.fullname, self.zip_path))
        except Exception as e:
            log_exception(e)

    def fetch(self, hdfs, temp=tempfile.gettempdir(), skip=False):
        if skip:
            return True

        try:
            with tempfile.NamedTemporaryFile(dir=temp, delete=False) as data:
                syslog.syslog(syslog.LOG_DEBUG, 'created temp file: %s' %  data.name)

                hdfs.get(self.remote.full, data=data)
                syslog.syslog(syslog.LOG_DEBUG, 'fetched hdfs file: %s' %  self.remote.full)

            self.mkdir(os.path.dirname(self.fullname))

            os.chmod(data.name, os.stat(data.name).st_mode|stat.S_IRGRP|stat.S_IROTH)
            os.rename(data.name, self.fullname)

            self.unzip()
            self.mtime = os.stat(self.fullname).st_mtime

            syslog.syslog(syslog.LOG_DEBUG, 'renamed temp file from %s to %s' % (data.name, self.fullname))
            return True
        except Exception as e:
            log_exception(e)
            if os.path.exists(data.name):
                try:
                    os.remove(data.name)
                except Exception as e:
                    log_exception(e)


def log_exception(ex):
    syslog.syslog(syslog.LOG_ERR, str(ex))
    for line in traceback.format_exc().split('\n'):
        syslog.syslog(syslog.LOG_DEBUG, '  %s' % line)

def setup_syslog(dest):
    if dest == 'console':
        prio = dict((v, k[4:]) for k, v in vars(syslog).items() if type(v) == int and (v & 7) > 0 and k not in ('LOG_PID', 'LOG_CONS'))
        syslog.syslog = lambda *m: sys.stdout.write('%s %8s: %s\n' % (str(datetime.datetime.now()), prio[m[0]], m[1]))
    elif dest is not None:
        syslog.openlog(ident=os.path.basename(sys.argv[0]), logoption=syslog.LOG_PID, facility=getattr(syslog, 'LOG_%s' % dest.upper()))
    else:
        syslog.syslog = lambda *m: True

    syslog.syslog(syslog.LOG_DEBUG, 'logging started')

def setup_socket(port, sock=socket.socket()):
    try:
        sock.bind(('127.0.0.1', port))
        sock.listen(1)
    except socket.error as e:
        if e.errno == errno.EADDRINUSE:
            syslog.syslog(syslog.LOG_ERR, 'another hdfs_sync process is already running')
        else:
            log_exception(e)
        sys.exit(1)

def setup_local(index, mirror):
    local = {}

    syslog.syslog(syslog.LOG_DEBUG, 'reading local index: %s' % index)
    try:
        for key, val in pickle.load(open(index)).items():
            if val.mirror != mirror:
                val.mirror = mirror
                syslog.syslog(syslog.LOG_INFO, 'detected sync directory move from %s to %s' % (val.mirror, mirror))

            if not val.modified:
                local[key] = val
            else:
                syslog.syslog(syslog.LOG_INFO, 'file changed or disappeared: %s' % key)
    except Exception as e:
        log_exception(e)

    syslog.syslog(syslog.LOG_INFO, 'loaded local index containing %d items' % len(local))
    return local

def setup_avail(client, source, mirror, unpack):
    avail = {}

    syslog.syslog(syslog.LOG_DEBUG, 'fetching file list')
    for item in client.ls(source, recurse=True):
        if item.name.endswith('_COPYING_'):
            continue

        try:
            if not item.is_dir():
                syslog.syslog(syslog.LOG_INFO, 'queueing hdfs object: %s' % item.full)
                avail[item.full] = SyncFile(item, source, mirror, unpack)
            else:
                syslog.syslog(syslog.LOG_DEBUG, 'skipping directory: %s' % item.full)
        except Exception as e:
            log_exception(e)

    syslog.syslog(syslog.LOG_INFO, 'read remote list containing %d items' % len(avail))
    return avail


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='hdfs directory sync')
    parser.add_argument('-u', '--hdfs-url', required=True,
                        help='full hdfs url to sync')
    parser.add_argument('-d', '--dest-dir', required=True,
                        help='destination directory')
    parser.add_argument('-t', '--temp-dir', default='/tmp',
                        help='where to put the temporary directory for downloads')
    parser.add_argument('-s', '--sync-dir', default='mirror',
                        help='relative directory to mirror sources into')
    parser.add_argument('-e', '--arch-dir', default='unpack',
                        help='relative directory to unpack archives into')
    parser.add_argument('-p', '--run-port', default=2311, type=int,
                        help='lock loopback port number')
    parser.add_argument('-l', '--log-dest', choices=['console', 'user', 'daemon'] + ['local%d' % i for i in xrange(8)],
                        help='syslog facility')
    parser.add_argument('-w', '--workers', type=int, default=multiprocessing.cpu_count(),
                        help='number of download threads')
    parser.add_argument('-n', '--dry-run', default=False, action='store_true',
                        help='show actions to be performed')
    args = parser.parse_args()

    setup_syslog(args.log_dest)
    setup_socket(args.run_port)

    dest_dir = args.dest_dir.rstrip('/')
    temp_dir = args.temp_dir.rstrip('/')
    sync_dir = '%s/%s' % (dest_dir, args.sync_dir.strip('/'))
    arch_dir = '%s/%s' % (dest_dir, args.arch_dir.strip('/'))

    hdfs_url = urlparse.urlparse(args.hdfs_url)
    hdfs_dir = hdfs_url.path
    hdfs_api = webhdfs.WebHDFSClient(hdfs_url._replace(path='').geturl(), user=getpass.getuser())

    try:
        if not os.path.exists(sync_dir):
            os.makedirs(sync_dir)
            syslog.syslog(syslog.LOG_INFO, 'created mirror path: %s' % sync_dir)
        if not os.path.exists(arch_dir):
            os.makedirs(arch_dir)
            syslog.syslog(syslog.LOG_INFO, 'created unpack path: %s' % arch_dir)

        index = '%s/.%s.idx' % (dest_dir, os.path.splitext(os.path.basename(sys.argv[0]))[0])
        local = setup_local(index, sync_dir)
        avail = setup_avail(hdfs_api, hdfs_dir, sync_dir, arch_dir)
        procs = multiprocessing.pool.ThreadPool(processes=args.workers)
        xfers = {}

        for key, val in avail.items():
            if (key not in local or not val.equal(local[key])):
                xfers[key] = procs.apply_async(val.fetch, (hdfs_api, temp_dir, args.dry_run))
        for key, val in xfers.items():
            if val.get():
                local[key] = avail[key]
            else:
                syslog.syslog(syslog.LOG_ERR, 'failed to fetch %s' % key)
        for key, val in local.items():
            if key not in avail and val.purge(args.dry_run):
                del(local[key])

        if not args.dry_run:
            pickle.dump(local, open(index, 'w'))
    except Exception as e:
        log_exception(e)
