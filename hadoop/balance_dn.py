#!/opt/python/bin/python2.7

import argparse
import errno
import os
import re
import shutil
import socket
import subprocess
import sys
import traceback
import xml.etree.cElementTree as etree

def check_root():
    if os.geteuid() != 0:
        sys.stderr.write('must run as root\n')
        sys.exit(1)

def check_java():
    for line in subprocess.check_output('/usr/bin/jps').split('\n'):
        if not line:
            return

        proc, item = line.split()
        if item == 'DataNode':
            sys.stderr.write('cannot run while %s is up (pid %s)\n' % (item, proc))
            sys.exit(1)

def mutex_lock(port, sock=socket.socket()):
    try:
        sock.bind(('127.0.0.1', port))
        sock.listen(1)
    except socket.error as e:
        if e.errno == errno.EADDRINUSE:
            sys.stderr.write('another balance_dn process is already running\n')
        else:
            traceback.print_exc()
        sys.exit(1)

def parse_conf(path):
    conf = {}
    for part in 'core', 'hdfs':
       conf.update(dict((i.findtext('name'), i.findtext('value')) for i in etree.parse('%s/%s-site.xml' % (path, part)).findall('.//property')))

    return [re.sub('^file://', '', i) for i in re.split(r'\s*,\s*', conf.get('dfs.datanode.data.dir') or conf['dfs.data.dir'])]

def path_usage(path):
    util = os.statvfs(path)
    return {
        'util':  float(util.f_blocks - util.f_bfree) / util.f_blocks * 100,
        'used':  (util.f_blocks - util.f_bfree) * util.f_frsize,
        'total': util.f_blocks * util.f_frsize,
    }

def rebalancer(src, tgt, num, vbs=False, dry=False):
    if vbs:
        print 'moving %d bytes from %s to %s' % (num, src, ', '.join(tgt))

    dest = 0
    move = 0
    for root, dirs, files in os.walk('%s/current' % src):
        for meta in files:
            if meta.startswith('blk') and meta.endswith('.meta'):
                blck = '_'.join(meta.split('_')[:-1])

                try:
                    size = os.stat('%s/%s' % (root, meta)).st_size + os.stat('%s/%s' % (root, blck)).st_size
                    if vbs:
                        print '\tmoving %d bytes: %s/%s, %s/%s to %s' % (size, root, meta, root, blck, tgt[dest])
                    else:
                        print '\rmoving %d / %d bytes from %s to %s ... %.02f%%' % (move, num, src, ', '.join(tgt), float(move) / num * 100),

                    if not dry:
                        if not os.path.exists(tgt[dest]):
                            stat = os.stat(root)
                            os.makedirs(tgt[dest])
                            os.chown(tgt[dest], stat.st_uid, stat.st_gid)
                            shutil.copystat(root, tgt[dest])

                        meta_stat = os.stat('%s/%s' % (root, meta))
                        blck_stat = os.stat('%s/%s' % (root, blck))

                        shutil.copy2('%s/%s' % (root, meta), tgt[dest])
                        shutil.copy2('%s/%s' % (root, blck), tgt[dest])

                        os.chown('%s/%s' % (tgt[dest], meta), meta_stat.st_uid, meta_stat.st_gid)
                        os.chown('%s/%s' % (tgt[dest], blck), blck_stat.st_uid, meta_stat.st_gid)

                        os.unlink('%s/%s' % (root, meta))
                        os.unlink('%s/%s' % (root, blck))

                    dest = (dest + 1) % len(tgt)
                    move += size

                    if move >= num:
                        if vbs:
                            print '%d / %d bytes moved' % (move, num)
                        else:
                            print '\rmoved %d / %d bytes from %s to %s ... %.02f%%' % (move, num, src, ', '.join(tgt), float(move) / num * 100)
                        return
                except Exception as e:
                    print '%sfailed to move %s/%s, %s/%s: %s' % ('\t' if vbs else '\n', root, meta, root, blck, e)
    if not vbs:
        print


if __name__ == '__main__':
    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', False)

    parser = argparse.ArgumentParser(description='datanode volume balancer', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-b', '--balance', default=5, type=float,
                        help='balance percentage factor')
    parser.add_argument('-c', '--cfg-dir', default='/etc/hadoop/conf',
                        help='hadoop configuration directory')
    parser.add_argument('-p', '--mtx-prt', default=1123, type=int,
                        help='local runtime mutex port')
    parser.add_argument('-n', '--dry-run', default=False, action='store_true',
                        help='report usage without performing balance')
    parser.add_argument('-v', '--verbose', default=False, action='store_true',
                        help='report blocks being moved instead of percentage')
    args = parser.parse_args()

    if not args.dry_run:
        check_root()
        check_java()
        mutex_lock(args.mtx_prt)

    dirs = parse_conf(args.cfg_dir)

    print 'discovered block volumes:'
    print '\t', ', '.join(dirs)
    while True:
        util = dict((i, path_usage(i)) for i in dirs)
        mean = sum(i['util'] for i in util.values()) / len(util)

        print 'calculated volume usage: (%.02f%% mean)' % mean
        print '\t', ', '.join('%s: %.02f%%' % (k, v['util']) for k, v in sorted(util.items()))

        orig, used = max(((i, j) for i, j in util.items()), key=lambda k: k[1]['util'])
        dest = list('%s/current' % i for i, j in util.items() if mean - j['util'] > args.balance / (len(util) - 1))
        size = int(used['total'] * (used['util'] - mean) / 100)

        if not dest:
            print 'volumes are balanced to +/- %.02f%%' % args.balance
            break

        rebalancer(orig, dest, size, args.verbose, args.dry_run)

        if args.dry_run:
            print 'dry run is set, skipping re-check to avoid infinite loop'
            break
