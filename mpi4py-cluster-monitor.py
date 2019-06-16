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

parser.add_argument('-i', '-interval', '--interval', default='5')
parser.add_argument('-s', '-stop_signal_file_prefix', '--stop_signal_file_prefix', default='/tmp/stop-mpi4py-cluster-monitor.sgf')

args = vars(parser.parse_args())

space_strs = []
for i in range(1000):
    space_strs.append("")
    for j in range(i):
        space_strs[i] = space_strs[i] + " "

def run_bg_cmd(command):

    proc = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    proc.wait()

    lns_content = []

    for line in io.TextIOWrapper(proc.stdout, encoding="utf-8"):
        lns_content.append(line[:-1])

    return lns_content

def get_logicical_core_cnt():
    ccnt_str = run_bg_cmd("cat /proc/cpuinfo | grep processor | wc -l")[0]
    return int(ccnt_str)

def get_hostname():
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

    #get sys config
    ccnt = get_logicical_core_cnt()
    mem, swap = get_mem_swap_sz()
    hostname = get_hostname()

    if(my_rank == 0):
        logf_name = 'mpi4py_cluster_monitor.log'
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

    launch_time = time.time()
    launch_time_ts = int(launch_time * 1000)
    loop_cnt = 0
    penalty_time = 0.0

    stop_hint_filename = '{}.{}.hint'.format(args['stop_signal_file_prefix'], launch_time_ts)
    stop_signal_filename = '{}.{}.stop'.format(args['stop_signal_file_prefix'], launch_time_ts)

    os.system('touch {}'.format(stop_hint_filename))

    while True:
        loop_cnt += 1
        used_mem = get_cur_used_mem()
        free_mem = mem - used_mem

        cpu_usage_list = get_cpu_usage_list() #this should be a integer list

        usage_txt = ""
        for usage in cpu_usage_list:
            tmpstr = str(usage)
            while(not (len(tmpstr) == 3)):
                tmpstr = " " + tmpstr
            usage_txt = usage_txt + " " + tmpstr

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

        to_stop = 0
        if (os.path.isfile(stop_signal_filename)):
            to_stop = 1
        to_stop = comm.allreduce(to_stop, op=MPI.MAX)
        if (to_stop == 1):
            break

        next_wake_time = launch_time + penalty_time + loop_cnt * sleep_interval
        time_after_barrier = time.time()
        time_to_sleep = next_wake_time - time_after_barrier

        local_time_penalty = 0.0

        if (time_to_sleep < 0):
            # make time_to_sleep = 0
            local_time_penalty += 0.0 - time_to_sleep

        local_time_penalty = comm.allreduce(local_time_penalty, op=MPI.MAX)
        penalty_time += local_time_penalty

        next_wake_time = launch_time + penalty_time + loop_cnt * sleep_interval
        time_to_sleep = next_wake_time - time_after_barrier

        time.sleep(time_to_sleep)
