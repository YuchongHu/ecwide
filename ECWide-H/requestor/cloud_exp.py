import subprocess
import threading
import sys

def send_one(index, header_flag):
    if header_flag:
        #cmd = "scp /root/wide/common.hpp node@node{:0>3d}:/home/node/wide".format(index)
        cmd = "scp /root/wide/update/common.hpp node@node{:0>3d}:/home/node/wide".format(index)
        
        #cmd = "scp /root/run.sh node@node{:0>3d}:/home/node/wide".format(index)
        #cmd = "scp /root/cls.sh node@node{:0>3d}:/home/node/wide".format(index)
        #cmd = "scp /root/proxy.cpp node@node{:0>3d}:/home/node/wide".format(index)
    else:
        cmd = "scp /root/ip.ini node@node{:0>3d}:/home/node/wide".format(index)
    subprocess.call(cmd, shell=True, stdout=subprocess.DEVNULL)

#def send_one_back(index):
    #cmd = "scp /root/back_run.sh node@node-{:0>2d}:/home/node/".format(index)
    #cmd = "scp /root/nodes_ip.txt node@node-{:0>2d}:/home/node/".format(index)
#    cmd = "scp /root/back.cpp node@node-{:0>2d}:/home/node/".format(index)
#    subprocess.call(cmd, shell=True, stdout=subprocess.DEVNULL)
   # print("== update back nothing ==")


def update(n, update_type):
    thread_list = []
    if update_type == "all":
        generate_nodes_config(n)
        for i in range(n):
            t = threading.Thread(target=send_one, args=(i, True))
            t.start()
            thread_list.append(t)
            t = threading.Thread(target=send_one, args=(i, False))
            t.start()
            thread_list.append(t)
    else:
        if update_type == "config":
            generate_nodes_config(n)
        for i in range(n):
            t = threading.Thread(target=send_one, args=(i, update_type == "header"))
            thread_list.append(t)
            t.start()
    # for i in range(10):
    #     t = threading.Thread(target=send_one_back, args=(i, ))
    #     t.start()
    #     thread_list.append(t)
    for t in thread_list:
        t.join()
    print("== update ok ==")


def call_single_node(index):
    #cmd = "ssh node@node{:0>3d} \"cd /home/node/wide; bash run.sh {} {}\"".format(index, int(sys.argv[3]), int(sys.argv[4]))
    #cmd = "ssh node@node{:0>3d} \"cd /home/node/wide; rm run.sh\"".format(index)
    cmd = "ssh node@node{:0>3d} \"cd /home/node/wide/update; bash run.sh {} {}\"".format(index, int(sys.argv[3]), int(sys.argv[4]))
    subprocess.call(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

# def call_single_back(index):
#     cmd = "ssh node@node-{:0>2d} \"cd /home/node; bash back_run.sh {} {}\"".format(index, int(sys.argv[3]), int(sys.argv[4]))
#     subprocess.call(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

# def call_back():
#     thread_list = []
#     for i in range(10):
#         t = threading.Thread(target=call_single_back, args=(i,))
#         t.start()
#         thread_list.append(t)
#     for t in thread_list:
#         t.join()
#     print("== call_back ok ==")

def call_nodes(n):
    thread_list = []
    for i in range(n):
        t = threading.Thread(target=call_single_node, args=(i,))
        t.start()
        thread_list.append(t)
    for t in thread_list:
        t.join()
    print("== call nodes ok ==")


def call_master():
    subprocess.call("cd /root/wide; make all; ./requestor", shell=True, stdout=subprocess.DEVNULL)


def generate_nodes_config(n):
    lines = []
    with open("nodes_ip.txt", "r") as f:
        lines = f.readlines()
    if lines:
        with open("ip.ini", "w") as wf:
            for i in range(n):
                wf.write(lines[i].strip() + "\n")
    print("generate_config ok")

def run_exp(n):
    # back_t = threading.Thread(target=call_back)
    # back_t.start()
    master_t = threading.Thread(target=call_master)
    master_t.start()
    nodes_t = threading.Thread(target=call_nodes, args=(n,))
    nodes_t.start()
    master_t.join()
    nodes_t.join()
#    back_t.join()

if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] == "nodes_config":
        n= int(sys.argv[2])*int(sys.argv[3])
        generate_nodes_config(n)
    elif len(sys.argv) == 5 and sys.argv[1] == "update":
        n= int(sys.argv[3])*int(sys.argv[4])
        update(n, sys.argv[2])
    elif len(sys.argv) == 5 and sys.argv[1] == "exp":
        n= int(sys.argv[2])*int(sys.argv[3])
        exp_num = 1
        for i in range(exp_num):
            run_exp(n)

    else:
        print("Usage: exp GROUP RACK THREAD")
        print("   Or: update  [all/config/header]  GROUP RACK")
        print("   Or: nodes_config GROUP RACK")
