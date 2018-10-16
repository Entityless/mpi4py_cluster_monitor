import os
import sys
import json
import argparse
import os
import subprocess
import io
import time

from mpi4py import MPI

comm = MPI.COMM_WORLD

my_rank = comm.Get_rank()
comm_sz = comm.Get_size()

parser = argparse.ArgumentParser()

tag_start_txt = '19260817_start'
tag_end_txt = 'that elder changed the China'

#recognized by pssh
#should be positive integer
parser.add_argument('-i', '-interval', '--interval', help='Hostfile', default='5')

args = vars(parser.parse_args())

space_strs = []
for i in range(1000):
    space_strs.append("")
    for j in range(i):
        space_strs[i] = space_strs[i] + " "

def run_bg_cmd(command):

    proc = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    proc.wait() #this works

    lns_content = []

    for line in io.TextIOWrapper(proc.stdout, encoding="utf-8"):
        lns_content.append(line[:-1])
    #     print(line[:-1])

    return lns_content

def get_logicical_core_cnt():
    ccnt_str = run_bg_cmd("cat /proc/cpuinfo | grep processor | wc -l")[0]
    return int(ccnt_str)

def get_hostname():
    # return run_bg_cmd("echo $HOSTNAME")[0] #不可行，因为mpi会继承环境变量
    return run_bg_cmd('hostname')[0]

def get_mem_swap_sz():
    ret_lns = run_bg_cmd('free -m')
    for ln in ret_lns:
        #if Swap
        if(ln[:3] == 'Swa'):
            swap = int(ln.split()[1])

        #if Mem
        if(ln[:3] == 'Mem'):
            mem = int(ln.split()[1])

    return mem, swap

def get_cur_used_mem():
    #condition1
#                  total        used        free      shared  buff/cache   available
#Mem:        1031824       18753      659891        7433      353179     1003603
#Swap:         20479          21       20458
    
    #condition2
#                 total       used       free     shared    buffers     cached
#Mem:         64183      13988      50195          0        476      10978
#-/+ buffers/cache:       2533      61649
#Swap:        20479        121      20358

    ret_lns = run_bg_cmd('free -m')

    pos_avail = ret_lns[0].find('avail')
    mem_ln_sp = ret_lns[1].split()

    if(pos_avail == -1):
        #condition2
        mem_used = int(mem_ln_sp[2]) - int(mem_ln_sp[5]) - int(mem_ln_sp[6])
    else:
        mem_used = int(mem_ln_sp[2])

    return mem_used

def get_cpu_usage_list():
    ret_lns = run_bg_cmd('mpstat -P ALL 1 1')

    #check which column %idle is in, and where the 'average' is start

    av_ln_idx = -1
    av_hit = False

    usage_list = []

    for ln in ret_lns:
        av_ln_idx += 1

        if(ln.find('Average') >= 0 and (not av_hit)):
            av_hit = True
            # break
            #do not break; parse where '%idle' is
            ln_sp = ln.split()
            idle_idx = ln_sp.index('%idle')

        elif(ln.find('Average') >= 0):
            #get the idle

            ln_sp = ln.split()
            usage = 100.0 - float(ln_sp[idle_idx])

            usage_list.append(int(usage + 0.0001))

    return usage_list

if __name__ == "__main__":
    #read the hostfile


    sleep_interval = int(args['interval'])
    if(sleep_interval <= 0):
        sleep_interval = 5
    #事实上，因为CPU的采样时间需要1秒，所以周期回避sleep_interval多一秒

    #get sys config
    ccnt = get_logicical_core_cnt()
    mem, swap = get_mem_swap_sz()
    hostname = get_hostname()

    if(my_rank == 0):
        logf_name = 'mpi4py_cluster_monitor.log'
        # logf = open(logf_name, 'a')
        # logf.write(to_print)
        # logf.close()
        print('The sleep interval: ', sleep_interval)
        sys.stdout.flush()

    my_info_dic = {}
    my_info_dic['hostname'] = hostname
    my_info_dic['ccnt'] = ccnt
    my_info_dic['mem'] = mem
    my_info_dic['swap'] = swap

    host_info = comm.gather(my_info_dic, root = 0)

    if(my_rank == 0):
        print(host_info)
        for d in host_info:
            print(d)

        #host_info is a list of info dic


    while True:

        #run the command
        used_mem = get_cur_used_mem()
        free_mem = mem - used_mem

        cpu_usage_list = get_cpu_usage_list() #this should be a integer list

        usage_txt = ""
        for usage in cpu_usage_list:
            tmpstr = str(usage)
            while(not (len(tmpstr) == 3)):
                tmpstr = " " + tmpstr
            usage_txt = usage_txt + " " + tmpstr

        #获得了这些数据之后，需要处理成字符串

        my_info_dic = {}
        my_info_dic['hn'] = hostname
        my_info_dic['cpu'] = usage_txt
        my_info_dic['mu'] = str(used_mem) + ' MB'
        my_info_dic['mf'] = str(free_mem) + ' MB'

        host_info = comm.gather(my_info_dic, root = 0)

        if my_rank == 0:
            #print the info
            #flush the stdout

            to_print = "\n\n" + time.strftime("%Y-%m-%d %H:%M:%S")

            for dics in host_info:

                to_print += '\n' + dics.pop('hn') + ': \n'
                to_print += str(dics)

            print(to_print)
            sys.stdout.flush()

            logf = open(logf_name, 'a')
            logf.write(to_print)
            logf.close()

        comm.Barrier()
        # break
        time.sleep(sleep_interval)
