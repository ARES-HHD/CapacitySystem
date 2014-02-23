#!/usr/bin/env python
# -*- coding: utf-8 -*-

import subprocess
import datetime
import os
import re
import time
import shutil
import threading
import sys

try: 
    import json
except ImportError:
    import simplejson as json
#from math import ceil

class SubProcess(object):
    """docstring for SubProcess"""
    def __init__(self, cmd, arg):
        self.cmd = cmd
        self.arg = arg
    def popen(self):
        output = subprocess.Popen(self.cmd,shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
        output_list = re.split(self.arg, output.stdout.read())
        return output_list

def format_size(size):
    if size < 1024:
        size = "%db" % size
    elif size < 1024 * 1024:
        size = "%.2fKb" % (size / 1024.0)
    elif size < 1024 * 1024 * 1024:
        size = "%.2fMb" % (size / 1024 / 1024.0)
    else:
        size = "%.2fGb" % (size / 1024 / 1024 / 1024.0)
    return size

def format_div(digit=2, arg1=1, arg2=1, arg3=1):
    """
    rewrite
    """
    try:
        digit = "0.%sf" % digit
        result = format((arg1 / arg2 / arg3), digit)
    except ZeroDivisionError:
        result = 0
    return result

class KThread(threading.Thread):
    """A subclass of threading.Thread, with a kill()
    method.
    
    Come from:
    Kill a thread in Python: 
    http://mail.python.org/pipermail/python-list/2004-May/260937.html
    """
    def __init__(self, *args, **kwargs):
        threading.Thread.__init__(self, *args, **kwargs)
        self.killed = False

    def start(self):
        """Start the thread."""
        self.__run_backup = self.run
        self.run = self.__run      # Force the Thread to install our trace.
        threading.Thread.start(self)

    def __run(self):
        """Hacked run function, which installs the
        trace."""
        sys.settrace(self.globaltrace)
        self.__run_backup()
        self.run = self.__run_backup

    def globaltrace(self, frame, why, arg):
        if why == 'call':
          return self.localtrace
        else:
          return None

    def localtrace(self, frame, why, arg):
        if self.killed:
          if why == 'line':
            raise SystemExit()
        return self.localtrace

    def kill(self):
        self.killed = True


class Timeout(Exception):
    """function run timeout"""
    
def timeout(seconds):
    """超时装饰器，指定超时时间
    若被装饰的方法在指定的时间内未返回，则抛出Timeout异常"""
    def timeout_decorator(func):
        """真正的装饰器"""
        
        def _new_func(oldfunc, result, oldfunc_args, oldfunc_kwargs):
            result.append(oldfunc(*oldfunc_args, **oldfunc_kwargs))
        
        def __deco(*args, **kwargs):
            result = []
            new_kwargs = { 
                'oldfunc': func,
                'result': result,
                'oldfunc_args': args,
                'oldfunc_kwargs': kwargs
            }# create new args for _new_func, because we want to get the func return val to result list
            thd = KThread(target=_new_func, args=(), kwargs=new_kwargs)
            thd.start()
            thd.join(seconds)
            alive = thd.isAlive()
            thd.kill() # kill the child thread
            if alive:
                raise Timeout(u'function run too long, timeout %d seconds.' % seconds)
            else:
                return result[0]
        return __deco
    return timeout_decorator


def get_mount_disk_info():

    CMD = "/bin/df -h"
    arg = "\n"
    output = SubProcess(CMD, arg)
    output_list = output.popen()
    i = 0 
    for line in output_list:
        if re.search('/data', line.strip()):
            i += 1 
    return {'ngxmount': i}

def get_iowait_info():

    CMD = "/usr/bin/iostat 1 2"
    arg = "\n\n"
    output = SubProcess(CMD, arg)
    output_list = output.popen()[3].split('\n')
    iowait_info = output_list[1].split()[3]
    return {'%iowait': iowait_info}

def get_load_info():

    with open("/proc/loadavg") as f:
        line = f.readline().split()
        one_load_info = line[0]
        five_load_info = line[1]
    return {'one_load': one_load_info, 'five_load': five_load_info}

def get_memused_info():

    with open("/etc/issue") as f:
        line = f.readline().split()
    if line[2] == '5.4':
        CMD = "/usr/bin/sar -r 1 1"
        arg = "\n\n"
        output = SubProcess(CMD, arg)
        output_list = output.popen()[1].split('\n')[1].split() 
        memused_info, swpused_info = output_list[4], output_list[9]
    else:
        CMD = "/usr/bin/sar -r -S 1 1"
        arg = "\n\n"
        output = SubProcess(CMD, arg)
        output_list = output.popen()
        memused_info, swpused_info = output_list[1].split('\n')[1].split()[4], output_list[2].split('\n')[1].split()[4]

    return {'%memused': memused_info, '%swpused': swpused_info}

def get_rpmversion_info():

    rpm_process = {'haproxy': 'haproxy.conf', 'nginx': 'nginx: worker process', 'esnv2': 'esnv2: worker process', 'bookkeeper': 'bookkeeper_http.py', 'r3v2': 'nginx: worker process', 'sinatranscenter': 'sinatranscenter.conf', 'sinatransscheduler': 'sinatransscheduler.conf'}
    rpmsta = {}
    strs = ''
    
    findAll = re.compile(r""" haproxy.*|nginx.*|esnv2.*|bookkeeper.*|r3v2.*|
                              sinatransbox.*|sinatranscenter.*|sinatransscheduler.*    #找出rpm版本
                          """, re.X)
    CMD = "/bin/rpm -qa"
    arg = ""
    output = SubProcess(CMD, arg)
    msg = output.popen()[0].strip()
    lists = findAll.findall(msg)
    if lists:
        strs = ', '.join(lists)
        rpm_list = [i.split('-')[0] for i in lists]
        CMD = "/bin/ps -ef"
        arg = ""
        output = SubProcess(CMD, arg)
        msg = output.popen()[0].strip()
        for rpm in list(set(rpm_list)):
            if rpm == 'sinatransbox' and os.path.isfile('/data0/log/sinatrans/sinatransbox.port'):
                with open("/data0/log/sinatrans/sinatransbox.port") as f:
                    line = f.readline().split()
                    transbox_port = line[0]
                    reg = transbox_port.join(['.*', '.*']) 
                    lists = re.search(reg, msg)
                    if lists:
                        info = lists.group().split()[4]
                        rpmsta.update({rpm: info})
            else:
                if rpm in rpm_process:
                    pross = rpm_process['%s' % rpm]
                    reg = pross.join(['.*', '.*']) 
                    lists = re.search(reg, msg)
                    if lists:
                        info = lists.group().split()[4]
                        rpmsta.update({rpm: info})

    return {'rpmversion': strs, 'rpm_runtime': rpmsta}

def get_iops_info():
    
    device_dict = {}
    CMD = "/usr/bin/iostat -k -x 1 2"
    arg = "\n\n"
    output = SubProcess(CMD, arg)
    output_lists = output.popen()
    server_name = re.split('[)(]', output_lists[0])[1]
    output_list = output_lists[4].split('\n')
    for line in output_list:
        if re.match('sd[a-z] |cciss/c0d[0-9] ', line.strip()):
            con = line.strip().split()
            device_dict[con[0]] = dict(
                zip(
                    ( 'r/s', 'w/s', 'rkB/s', 'wkB/s',
                      'rps',
                      'wps' ),
                    ( con[3], con[4], con[5], con[6],
                      format_div(2, float(con[5]), float(con[3])),
                      format_div(2, float(con[6]), float(con[4])))
                )
            )

    return {'iops': device_dict, 'servername': server_name}

def get_one_traffic():
    """ 获得单次累积网卡流量  
    """

    one_net_dict = {}
    with open("/proc/net/dev") as f:
        for line in f.readlines()[2:]:
            if not line.strip().startswith(('eth', 'bond')):continue
            con = re.split('[ :]+',line.strip())
            one_net_dict[con[0]] = dict(
                zip(
                    ( 'in(traffic/s)', 'in(packets/s)', 'inerrs(packet/s)',
                      'indrop(packet/s)', 'out(traffic/s)', 'out(packets/s)',
                      'outerrs(packet/s)', 'outdrop(packet/s)' ),
                    ( float(con[1]), con[2], con[3],
                      con[4], float(con[9]), con[10], 
                      con[11], con[12], ) 
                )
            )

    return one_net_dict

def get_traffic_info():
    """ 获得每秒网卡流量
    """

    net_dict = {}
    old_net = get_one_traffic()
    time.sleep(1)
    now_net = get_one_traffic()
    for dev,nets in old_net.items():
         
        dif_net = [(abs(int(t2)-int(t1)) * 8) for t1, t2 in zip(nets.values(), now_net[dev].values())]
        net_dict[dev] = dict(
            zip(
                nets.keys(),
                (dif_net[0], 
                 dif_net[1],
                 dif_net[2],
                 format_size(dif_net[3]),
                 dif_net[4], 
                 dif_net[5], 
                 format_size(dif_net[6]),
                 dif_net[7], 
                )
            )  
        ) 
    return {'traffic': net_dict}

def get_localtime():
    
    local_time = time.strftime('%Y-%m-%d %X',time.localtime())
    return {'reported_time': local_time}

def get_MegaCli_info():

    MegaCli_info = {}
    try:
        CMD = "/opt/MegaRAID/MegaCli/MegaCli64 -PDList -a0 -NoLog" 
        arg = "\n\n+"
        output = SubProcess(CMD, arg)
        output_list = output.popen()

        findAll = re.compile(r""" Enclosure\sDevice.*|Slot\sNumber.*|Firmware\sstate.* #找出物理磁盘状态
                              """, re.X)
        if output_list[1] != 'Exit Code: 0x01\n':
            for i in output_list[1:-1]:
                lists = findAll.findall(i)
                dev_info = ':'.join((lists[0].split(':')[1].strip(),lists[1].split(':')[1].strip()))
                MegaCli_info.update({dev_info: lists[2].split(':')[1].strip()})
    except:
        pass
    return MegaCli_info

def get_hpacucli_info():

    hpacucli_info = {}
    try:
        CMD = "/usr/sbin/hpacucli ctrl all show status" 
        arg = "\n"
        output = SubProcess(CMD, arg)
        output_list = output.popen()
        slot_num = output_list[1].split()[5]
        CMD = "/usr/sbin/hpacucli ctrl slot=%s show config " % slot_num 
        arg = "\n\n\n"
        output = SubProcess(CMD, arg)
        output_list = output.popen()

        findAll = re.compile(r""" physicaldrive.*|logicaldrive.*|unassigned.* #找出物理磁盘状态
                              """, re.X)
        for i in output_list:
            lists = findAll.findall(i)
            if lists:
                key, value = lists[0], lists[1]
                if len(lists) > 2:
                    two_lists = lists[:2]
                    other_lists = lists[2:]
                    hpacucli_info.update({two_lists[0]: two_lists[1]})
                    hpacucli_info.update({other_lists[0]: other_lists[1:]})
                else:
                    hpacucli_info.update({key: value})

    except:
        pass
    return hpacucli_info

def get_raid_dev_info():

    msg = 'None'
    try:
        CMD = "/usr/sbin/dmidecode -t system"
        arg = ""
        output = SubProcess(CMD, arg)
        output_list = output.popen()
        reg = 'Product\sName.*'
        lists = re.search(reg, output_list[0])
        if lists:
            machine_type = lists.group()
            if re.search('PowerEdge|IBM', machine_type):
                msg = get_MegaCli_info()
            elif re.search('ProLiant', machine_type):
                msg = get_hpacucli_info()
    except:
        msg = ''
    return {'raid_dev_info': msg}

def get_base_info():

    base_info = ['get_iowait_info', 'get_load_info', 'get_memused_info', 'get_iops_info', 'get_traffic_info', 'get_localtime', 'get_raid_dev_info']
    return base_info


def get_pdns_info():

    pdns_info = {}
    if os.path.isfile('/etc/init.d/pdns'):
        keys = ['latency', 'qsize-q', 'timedout-packets', 'udp-answers', 'udp-queries']
        for i in keys:
            CMD = "/etc/init.d/pdns show %s" % i
            arg = "="
            output = SubProcess(CMD, arg)
            output_list = output.popen()
            pdns_info.update({output_list[0]: output_list[1].strip()})
    return {'pdns_info': pdns_info}

def get_r3_qps_info():

    r3_qps = ''
    try:
        CMD = "curl 127.0.0.1/nginx_status --connect-timeout 1 -m 5"
        arg = "\n"
        output = SubProcess(CMD, arg)
        output_list = output.popen()
        r3_qps = output_list[5].split()[-1]
    except:
        pass

    return {'r3_qps': r3_qps}

def get_ts_qps_info():

    CMD = "/usr/local/bin/traffic_line -r proxy.node.user_agent_xacts_per_second"
    arg = ""
    output = SubProcess(CMD, arg)
    output_list = output.popen()
    ts_qps_info = round(float(output_list[0]))
    return {'ts_qps': ts_qps_info}

def get_ts_bandwidth_info():

    CMD = "/usr/local/bin/traffic_line -r proxy.node.client_throughput_out"
    arg = ""
    output = SubProcess(CMD, arg)
    output_list = output.popen()
    ts_bandwidth_info = round(float(output_list[0]))
    return {'ts_bandwidth': ts_bandwidth_info}

def get_ts_hit_info():

    CMD = "/usr/local/bin/traffic_line -r proxy.node.cache_hit_ratio_avg_10s"
    arg = ""
    output = SubProcess(CMD, arg)
#    ts_hit_info = '%s %s' % ('ts_hit', ceil(float(output.popen().strip()) * 100))
    ts_hit_info = round(float(output.popen()[0].strip()) * 100)
    return {'ts_hit': ts_hit_info}

def get_ts_diskcache_info():

    CMD = "/usr/local/bin/traffic_line -r proxy.process.cache.bytes_total"
    arg = ""
    output = SubProcess(CMD, arg)
    ts_diskcache_total = int(output.popen()[0].strip())
    ts_diskcache_total_info = format_size(ts_diskcache_total)

    CMD = "/usr/local/bin/traffic_line -r proxy.process.cache.bytes_used"
    arg = ""
    output = SubProcess(CMD, arg)
    ts_diskcache_used = int(output.popen()[0].strip())
    ts_diskcache_used_info = format_size(ts_diskcache_used)

    #ts_diskcache_freq = '%s' % (round(ts_diskcache_used / float(ts_diskcache_total) * 100, 2))
    ts_diskcache_freq = '%s' % (format_div(2, float(ts_diskcache_used) * 100, float(ts_diskcache_total)))

    return {'ts_diskcache_total': ts_diskcache_total_info, 'ts_diskcache_used': ts_diskcache_used_info, '%ts_diskcache_freq': ts_diskcache_freq}

def get_ts_rawcache_total_info():

    CMD = "/usr/local/bin/traffic_line -r proxy.process.cache.ram_cache.total_bytes"
    arg = ""
    output = SubProcess(CMD, arg)
    ts_rawcache_total_info = format_size(int(output.popen()[0].strip()))
    return {'ts_rawcache_total': ts_rawcache_total_info}

def get_ts_rawcache_used_info():

    CMD = "/usr/local/bin/traffic_line -r proxy.process.cache.ram_cache.bytes_used"
    arg = ""
    output = SubProcess(CMD, arg)
    ts_rawcache_used_info = format_size(int(output.popen()[0].strip()))
    return {'ts_rawcache_used': ts_rawcache_used_info}

def get_ts_rpm_ver_info():

    CMD = "/usr/local/bin/traffic_line -r proxy.node.version.manager.short"
    arg = ""
    output = SubProcess(CMD, arg)
    ts_rpm_ver_info = output.popen()[0].strip()
    rpmver = {'trafficserver': ts_rpm_ver_info}
    return {'rpmversion': rpmver}

def get_ts_start_time_info():

    CMD = "/usr/local/bin/traffic_line -r proxy.node.restarts.proxy.start_time"
    arg = ""
    output = SubProcess(CMD, arg)
    ts_start_time_info = time.strftime('%Y-%m-%d %X', time.localtime(int(output.popen()[0].strip())))
    rpmsta = {'trafficserver': ts_start_time_info}
    return {'rpm_runtime': rpmsta}

def get_ats_info():

    ats_info = ['get_ts_qps_info', 'get_ts_bandwidth_info', 'get_ts_hit_info', 'get_ts_diskcache_info', 'get_ts_rawcache_total_info', 'get_ts_rawcache_used_info', 'get_ts_rpm_ver_info', 'get_ts_start_time_info']
    return ats_info


def total_func():

    total = []
    total.append(get_base_info())

    CMD = "/bin/hostname"
    arg = ""
    output = SubProcess(CMD, arg)
    role = output.popen()[0].split('.')[2]
    if role == 'ts':
        total.append(get_ats_info())
    elif role == 'nsr3':
        total.append(['get_pdns_info', 'get_r3_qps_info', 'get_mount_disk_info', 'get_rpmversion_info',])
    else:
        total.append(['get_mount_disk_info', 'get_rpmversion_info']) 
    
    total = sum(total, [])
    return total

def worker(func, total_info):
    
    exec('total_info.update(%s())' % func)

#@timeout(4)
def master_thread():
    
    total_info = {}
    thread_list = []
    funcs = total_func()
    for func in funcs:
        t = threading.Thread(target=worker, args=(func,total_info))
        thread_list.append(t)
    for th in thread_list:
        th.start()
    for th in thread_list:
        th.join()

    info = json.dumps(total_info)
    return info

#@timeout(5)
@timeout(20)
def write_data():
    #dirpath1 = '/data0/log/server_msg/tmp/'
    dirpath2 = '/data0/log/server_msg/msg/'
    #create_time = str(time.strftime('%Y%m%d%H%M%S',time.localtime()))

    #if not os.path.exists(dirpath1):
    #    os.makedirs(dirpath1)
    if not os.path.exists(dirpath2):
        os.makedirs(dirpath2)
    #filepath = dirpath1 + create_time
    filename = 'collectmsg'
    filepath = dirpath2 + filename
    fp = open(filepath, 'a')
    try:
        fp.write(master_thread())
        fp.write('\n')
        #time.sleep(3)
    finally:
        fp.close()
        #shutil.move(filepath, dirpath2)

if __name__ == "__main__":
    #print total_func()
    #write_data()
    #master_thread()
    try:
        #print master_thread()
        write_data()
    except Timeout, e:
        print e
