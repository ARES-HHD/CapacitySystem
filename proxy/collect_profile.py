#!/usr/bin/env python
# encoding: utf-8
#
# This script get command string from log, it's a
# Process Pool version
#


#import daemon
import re
import threading
import Queue
import subprocess
import time
import sys
from settings import *

try:
    import json
except ImportError:
    import simplejson as json

#from log_utils import log_to_scribe

"""
_accesskey=sinaedgeahsolci14ydn&_ip=172.16.213.196_cnc.haerbin.cluster1&_port=80&_an=D11081563&_data={"%iowait": "20.25", "five_load": "2.09", "%memused": "127880", "ts_diskcache_total": "679.46Gb", "rpm_runtime": {"trafficserver": "2013-10-27 15:40:55"}, "servername": "cnc.haerbin.ts.196.nb.sinaedge.com", "ts_hit": 89.0, "ts_rawcache_used": "7.18Gb", "raid_dev_info": {"32:5": "Online, Spun Up", "32:4": "Online, Spun Up", "32:1": "Online, Spun Up", "32:0": "Online, Spun Up", "32:3": "Online, Spun Up", "32:2": "Online, Spun Up"}, "rpmversion": {"trafficserver": "3.0.4"}, "ts_bandwidth": 322.0, "reported_time": "2013-12-22 16:26:01", "iops": {"sdd": {"rps": "127.92", "rkB/s": "5884.50", "w/s": "0.00", "wps": 0, "wkB/s": "0.00", "r/s": "46.00"}, "sde": {"rps": "108.13", "rkB/s": "4433.50", "w/s": "14.00", "wps": "292.57", "wkB/s": "4096.00", "r/s": "41.00"}, "sdf": {"rps": "118.56", "rkB/s": "6995.00", "w/s": "8.00", "wps": "296.81", "wkB/s": "2374.50", "r/s": "59.00"}, "sda": {"rps": 0, "rkB/s": "0.00", "w/s": "7.00", "wps": "4.00", "wkB/s": "28.00", "r/s": "0.00"}, "sdb": {"rps": "70.54", "rkB/s": "3950.50", "w/s": "9.00", "wps": "294.50", "wkB/s": "2650.50", "r/s": "56.00"}, "sdc": {"rps": "93.55", "rkB/s": "4397.00", "w/s": "0.00", "wps": 0, "wkB/s": "0.00", "r/s": "47.00"}}, "%ts_diskcache_freq": "99.99", "%swpused": "0", "one_load": "2.31", "traffic": {"eth3": {"outerrs(packet/s)": 0, "out(packets/s)": 0, "outdrop(packet/s)": 0, "in(traffic/s)": "0b", "indrop(packet/s)": 0, "inerrs(packet/s)": 0, "out(traffic/s)": "0b", "in(packets/s)": 0}, "eth2": {"outerrs(packet/s)": 0, "out(packets/s)": 0, "outdrop(packet/s)": 0, "in(traffic/s)": "0b", "indrop(packet/s)": 0, "inerrs(packet/s)": 0, "out(traffic/s)": "0b", "in(packets/s)": 0}, "eth1": {"outerrs(packet/s)": 0, "out(packets/s)": 251808, "outdrop(packet/s)": 0, "in(traffic/s)": "10.83Mb", "indrop(packet/s)": 0, "inerrs(packet/s)": 0, "out(traffic/s)": "322.38Mb", "in(packets/s)": 129144}, "eth0": {"outerrs(packet/s)": 0, "out(packets/s)": 39704, "outdrop(packet/s)": 0, "in(traffic/s)": "49.40Mb", "indrop(packet/s)": 0, "inerrs(packet/s)": 0, "out(traffic/s)": "7.55Mb", "in(packets/s)": 37448}}, "ts_qps": 1228.0, "ts_rawcache_total": "7.30Gb", "ts_diskcache_used": "679.42Gb"}
"""

def get_data():

    # 从日志读取host数据后存储到这个dict中:
    aggracive_data = {}
    # 解析主要数据的regexp:
    p_cluster_data = re.compile(r'_ip=\d+\.\d+.\d+\.\d+_([^ ]+)&_port[^ ]+_data=({.*})')  # cluster和data数据一起解析

    last_t = 0
    while 1:
        try:                    # 读取输入,遇到文件结束则退出
            s = tailq.get()
            if s == "QUIT":
                raise Exception("quit")
        except Exception, e:
            print >> sys.stderr, '[panic] get', e
            break
        #print s
        try:
            match = p_cluster_data.search(s)
            if match:
                d_cluster, d_data = match.groups()[:2]
            if not (d_cluster and d_data):
                continue
            d_data = json.loads(d_data)
            t = d_data['reported_time'][:16]
            #print d_cluster, t
            if last_t == 0:
                last_t = t
            if last_t != t:
               print aggracive_data
               last_t = t
               aggracive_data = {}
            aggracive_key = '_'.join((d_cluster, t)) 
            if aggracive_key not in aggracive_data:
                aggracive_data[aggracive_key] = {
                'clusterbw': d_data['traffic']['eth1']['out(traffic/s)'],
                'hostdata': [d_data],
                }
            else:
                bw = float(aggracive_data[aggracive_key]['clusterbw'].replace('Mb', '')) + float(d_data['traffic']['eth1']['out(traffic/s)'].replace('Mb', ''))
                aggracive_data[aggracive_key]['clusterbw'] = ''.join((str(bw),'Mb'))
                aggracive_data[aggracive_key]['hostdata'].append(d_data)

        except Exception, e:
            print >> sys.stderr, 'skip', e

tailq = Queue.Queue(maxsize=100)

def tail_forever(fn):
    p = subprocess.Popen(["tail", "-F", fn], stdout=subprocess.PIPE)
    while 1:
        line = p.stdout.readline()
        tailq.put(line)
        if not line:
            tailq.put("QUIT")
            break

threading.Thread(target=tail_forever, args=(ACCESSLOG_FILE,)).start()

if __name__ == '__main__':
    get_data()
