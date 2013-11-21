#!/opt/python/bin/jython

import glob
import optparse
import os
import socket
import sys
import time

import java.io.File
import java.lang.Class
import java.lang.String
import java.lang.reflect.UndeclaredThrowableException
import java.net.URL
import java.net.URLClassLoader

### prepare classpath
load = java.net.URLClassLoader.getDeclaredMethod('addURL', [java.net.URL])
load.accessible = True
classpath = set(['/etc/hadoop/conf', '/etc/hbase/conf'])
for path in glob.glob('/usr/lib/hbase*') + glob.glob('/usr/lib/hadoop*'):
    for root, dirs, jars in os.walk(path):
        for name in jars:
            if name.endswith('.jar'):
                classpath.add(os.path.join(root, name))

for path in classpath:
    load.invoke(java.lang.ClassLoader.getSystemClassLoader(), [java.io.File(path).toURL()])
#####################

import org.apache.hadoop.hbase.Abortable
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HServerAddress

import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HConnectionManager
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Scan

import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter

import org.apache.hadoop.hbase.zookeeper.RootRegionTracker

import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory

from org.apache.hadoop.hbase import HConstants
from org.apache.hadoop.hbase.util import Bytes

class RegionMoverAbortable(org.apache.hadoop.hbase.Abortable):
    def abort(why, e):
        LOG.error('ABORTED! why=%s, e=%s' % (why, e))

def hosts_list(hosts):
    rval = []
    for host in hosts:
        try:
            rval.append(socket.getfqdn(host))
        except:
            pass

    return rval

def get_config(params={}, zk_hosts=[]):
    conf = org.apache.hadoop.hbase.HBaseConfiguration.create()

    conf.setInt('hbase.client.pause', 500)
    conf.setInt('hbase.client.prefetch.limit', 1)
    conf.setInt('hbase.client.retries.number', 100)
    for key, val in params.items():
        conf.set(key, val)
    if zk_hosts:
        conf.set('hbase.zookeeper.quorum', ','.join(zk_hosts))

    return conf

def get_servers(admin, src, dst):
    target = dict((s.getServerName().split(',')[0], s.getServerName()) for s in admin.getClusterStatus().getServerInfo())

    if src not in target:
        raise RuntimeError('%s is not online' % src)
    else:
        source = target.pop(src)

    return source, target.values() if not len(dst) else [s for s in target.values() if s.split(',')[0] in dst]

def get_address(server, string=False):
    host, port = server.split(',')[0:2]
    return '%s:%s' % (host, port) if string else org.apache.hadoop.hbase.HServerAddress(host, int(port))

def get_regions(config, source):
    conn = org.apache.hadoop.hbase.client.HConnectionManager.getConnection(config)
    return conn.getHRegionConnection(get_address(source)).getOnlineRegions()

def get_region_server(admin, region):
    if region.isRootRegion():
        tracker = org.apache.hadoop.hbase.zookeeper.RootRegionTracker(admin.getConnection().getZooKeeperWatcher(), RegionMoverAbortable())
        tracker.start()
        while not tracker.isLocationAvailable():
            time.sleep(0.1)

        server = tracker.getRootRegionLocation().toString() + ','
        tracker.stop()

        return server

    if region.isMetaRegion():
        table = get_table(admin.getConfiguration(), HConstants.ROOT_TABLE_NAME)
    else:
        table = get_table(admin.getConfiguration(), HConstants.META_TABLE_NAME)

    get = org.apache.hadoop.hbase.client.Get(region.getRegionName())
    get.addColumn(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER)
    get.addColumn(HConstants.CATALOG_FAMILY, HConstants.STARTCODE_QUALIFIER)

    result = table.get(get)
    server = result.getValue(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER)
    start_code = result.getValue(HConstants.CATALOG_FAMILY, HConstants.STARTCODE_QUALIFIER)

    return '%s,%d' % (java.lang.String(server).replaceFirst(':', ','), Bytes.toLong(start_code))

def get_table(config, table, cache={}):
    key = Bytes.toString(table)
    if key not in cache:
        cache[key] = org.apache.hadoop.hbase.client.HTable(config, table)

    return cache[key]

def scan_region(admin, region):
    scan = org.apache.hadoop.hbase.client.Scan(region.getStartKey())
    scan.setBatch(1)
    scan.setCaching(1)
    scan.setFilter(org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter())

    table = get_table(admin.getConfiguration(), region.tableName)
    scanner = table.getScanner(scan)
    try:
        scanner.next()
    finally:
        scanner.close()
        table.close()

def move_region(admin, region, source, target):
    admin.move(Bytes.toBytes(region.getEncodedName()), Bytes.toBytes(target))

    until = time.time() + admin.getConfiguration().getInt('hbase.move.wait.max', 60)
    while time.time() < until:
        if get_region_server(admin, region) != source:
            break

        time.sleep(0.1)
    else:
        raise RuntimeError('Region stuck on %s, target=%s' % (source, target))

    scan_region(admin, region)

def unload_regions(args, sources, targets=[]):
    # set up configuration
    params = dict(s.split('=', 1) for s in args.params if '=' in s)
    config = get_config(params, args.zk_hosts)

    # set up admin instance
    admin = org.apache.hadoop.hbase.client.HBaseAdmin(config)

    for source in sources:
        # set up list of online servers
        source, targets = get_servers(admin, source, targets)

        # verify targets available
        if not targets:
            raise RuntimeError('No target servers available to receive regions')

        # set up list of regions on the source
        regions = get_regions(config, source)

        # disable region balancer
        LOG.info('Disabling automatic region balancer')
        admin.balanceSwitch(False)

        if not args.dontmove:
            LOG.info('Moving %d region(s) from %s to %d other server(s)' % (len(regions), source, len(targets)))
            for count in xrange(len(regions)):
                region = regions[count]
                target = targets[count % len(targets)]

                LOG.info('moving region %-32s (%4d of %4d) to server=%s' % (region.getEncodedName(), count + 1, len(regions), target))
                try:
                    move_region(admin, region, source, target)
                except java.lang.reflect.UndeclaredThrowableException, e:
                    LOG.error('Exception moving %s; split/moved? %s' % (region.getEncodedName(), e))

        if args.shutdown:
            LOG.info('Shutting down region server: %s' % source)
            admin.stopRegionServer(get_address(source, admin.getClusterStatus().getVersion() >= 2))


if __name__ == '__main__':
    parser = optparse.OptionParser(usage='%prog <-s|--src-host <host>>|<-S|--src-file <file>> [options] [target1] [target2] ...')
    parser.add_option('-z', '--zk-hosts', default=[], action='append',
                      help='list of zookeeper hosts')
    parser.add_option('-s', '--src-host', default=[], action='append',
                      help='host to move regions from')
    parser.add_option('-S', '--src-file',
                      help='file containing hosts to move regions from')
    parser.add_option('-t', '--tgt-host', default=[], action='append',
                      help='host to move regions to')
    parser.add_option('-T', '--tgt-file',
                      help='file containing hosts to move regions to')
    parser.add_option('-k', '--shutdown', default=False, action='store_true',
                      help='shut down source hosts after migration')
    parser.add_option('-n', '--dontmove', default=False, action='store_true',
                      help='skip moving regions when shutting down')
    parser.add_option('-D', '--params', default=[], action='append',
                      help='extra hbase configuration key=val params')
    args, opts = parser.parse_args()

    src_hosts = set(hosts_list(args.src_host))
    if args.src_file and os.path.exists(args.src_file):
        src_hosts.update(hosts_list(open(args.src_file).read().split()))

    tgt_hosts = set(hosts_list(args.tgt_host + opts))
    if args.tgt_file and os.path.exists(args.tgt_file):
        tgt_hosts.update(hosts_list(open(args.tgt_file).read().split()))

    if not src_hosts:
        parser.error('--src-host or --src-file is required')
    if src_hosts.intersection(tgt_hosts):
        parser.error('overlapping values in source and target hosts')

    LOG = org.apache.commons.logging.LogFactory.getLog(os.path.basename(__file__))
    LOG.info('Logging started')

    try:
        unload_regions(args, src_hosts, tgt_hosts)
    except RuntimeError, e:
        LOG.error(e.message)
        sys.exit(1)
