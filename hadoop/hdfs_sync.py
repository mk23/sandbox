#!/usr/bin/env python

import collections
import datetime
import errno
import hashlib
import os
import pickle
import shutil
import socket
import subprocess
import sys
import syslog
import tempfile
import traceback
import urlparse

HDFS_File = collections.namedtuple('HDFS_File', 'mode repl owner group bytes date time url')
class Sync_File:
    def __init__(self, item, cmd, src, tgt):
        if type(item) == list:
            self.hdfs = HDFS_File(item)
        elif type(item) == str and item:
            self.hdfs = HDFS_File(*(item.strip().split(None, len(HDFS_File._fields))))
        else:
            raise TypeError

        url = urlparse.urlparse(src)

        self.name = self.hdfs.url[len(src) if self.hdfs.url.startswith(src) else len(url.path):]

        self.src = '%s://%s' % (url.scheme, os.path.normpath('%s/%s/%s' % (url.netloc, url.path, self.name)))
        self.tgt = os.path.normpath('%s/%s' % (tgt, self.name))
        self.cmd = cmd

    def __str__(self):
        return '%s: %s' % (self.name, ' '.join(list(self.hdfs)))

    def isdir(self):
        return self.hdfs.mode.startswith('d')

    def equal(self, item):
        return self.hdfs == item.hdfs

    def mkmd5(self):
        md5 = hashlib.md5()
        with open(self.tgt, 'rb') as f:
            for part in iter(lambda: f.read(128 * md5.block_size), b''):
                md5.update(part)

        return md5.hexdigest()

    def mkdir(self, path):
        try:
            os.makedirs(path)
        except OSError as e:
            if e.errno != errno.EEXIST or not os.path.exists(path):
                raise

    def fetch(self, temp=tempfile.gettempdir(), skip=False):
        cmd = {'old': 'hadoop fs', 'new': 'hdfs dfs'}[self.cmd].split()
        cmd.extend(['-get', self.src, temp])

        syslog.syslog(syslog.LOG_DEBUG, 'running: %s' % ' '.join(cmd))
        try:
            if not skip:
                temp = self.tgt.replace(os.path.dirname(self.tgt), temp)
                subprocess.check_output(cmd, stderr=subprocess.STDOUT)

                self.mkdir(os.path.dirname(self.tgt))
                os.rename(temp, self.tgt)

                self.md5 = self.mkmd5()

            syslog.syslog(syslog.LOG_INFO, '%s: fetched hdfs file from %s' % (self.tgt, self.hdfs.url))
            return True
        except subprocess.CalledProcessError as e:
            syslog.syslog(syslog.LOG_ERR, e.output)
        except Exception as e:
            log_exception(e)

        return False

    def purge(self, skip=False):
        try:
            if not skip:
                os.unlink(self.tgt)
            syslog.syslog(syslog.LOG_INFO, 'purged local file: %s' % self.tgt)
            return True
        except Exception as e:
            log_exception(e)

        return False

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

def setup_socket(port):
    try:
        sock = socket.socket()
        sock.bind(('127.0.0.1', port))
    except socket.error as e:
        if e.errno == errno.EADDRINUSE:
            syslog.syslog(syslog.LOG_ERR, 'another hdfs_sync process is already running')
        else:
            log_exception(e)
        sys.exit(1)

def setup_local(index):
    local = {}

    try:
        local = pickle.load(open(index))
        for key, val in local.items():
            if not os.path.exists(key) or val.md5 != val.mkmd5():
                syslog.syslog(syslog.LOG_INFO, 'file changed or disappeared: %s' % key)
                del local[key]
    except Exception as e:
        log_exception(e)

    syslog.syslog(syslog.LOG_INFO, 'loaded local index containing %d items' % len(local))
    return local

def setup_avail(cmd, url, dst):
    avail = {}
    lscmd = {'old': 'hadoop fs -lsr', 'new': 'hdfs dfs -ls -R'}[cmd].split()
    lscmd.append(url)

    syslog.syslog(syslog.LOG_DEBUG, 'running: %s' % ' '.join(lscmd))
    for line in subprocess.check_output(lscmd, stderr=subprocess.STDOUT).split('\n'):
        if not line:
            continue

        try:
            item = Sync_File(line, cmd, url, dst)
            if not item.isdir():
                syslog.syslog(syslog.LOG_INFO, 'found hdfs item: %s' % item.hdfs.url)
                avail[item.tgt] = item
            else:
                syslog.syslog(syslog.LOG_DEBUG, 'skipping directory: %s' % item.hdfs.url)
        except Exception as e:
            log_exception(e)

    syslog.syslog(syslog.LOG_INFO, 'read remote list containing %d items' % len(avail))
    return avail

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='hdfs directory sync')
    parser.add_argument('-u', '--hdfs-url', required=True,
                        help='full hdfs url to sync')
    parser.add_argument('-d', '--dest-dir', required=True,
                        help='destination directory')
    parser.add_argument('-c', '--hdfs-cmd', default='new', choices=['new', 'old'],
                        help='hdfs command type')
    parser.add_argument('-p', '--run-port', default=2311, type=int,
                        help='loopback bind port for mutually exclusive execusion')
    parser.add_argument('-l', '--log-dest', choices=['console', 'user', 'daemon'] + ['local%d' % i for i in xrange(8)],
                        help='syslog facility')
    parser.add_argument('-n', '--dry-run', default=False, action='store_true',
                        help='show actions to be performed')
    args = parser.parse_args()

    setup_syslog(args.log_dest)
    setup_socket(args.run_port)

    try:
        index = '%s/.%s.pkl' % (args.dest_dir, os.path.splitext(os.path.basename(sys.argv[0]))[0])
        syslog.syslog(syslog.LOG_DEBUG, 'reading local index: %s' % index)

        spool = tempfile.mkdtemp(dir=args.dest_dir)
        syslog.syslog(syslog.LOG_DEBUG, 'created work directory: %s' % spool)

        local = setup_local(index)
        avail = setup_avail(args.hdfs_cmd, args.hdfs_url, args.dest_dir)

        for key, val in avail.items():
            if (key not in local or not val.equal(local[key])) and val.fetch(spool, args.dry_run):
                local[key] = val
        for key, val in local.items():
            if key not in avail and val.purge(args.dry_run):
                del(local[key])

        if not args.dry_run:
            pickle.dump(local, open(index, 'w'))
    except Exception as e:
        log_exception(e)
    finally:
        try:
            syslog.syslog(syslog.LOG_INFO, 'removing work directory: %s' % spool)
            shutil.rmtree(spool)
        except:
            pass
