import subprocess
import threading
import sys

user_name = yao


def send_one(index, program_flag):
    if program_flag:
        cmd = "scp -r ./classes/ ./lib/ {}@node{:0>2d}:~/clc_experiment/".format(user_name,
                                                                                 index)
        # cmd = "scp ./build/bin/node_main {}@node{:0>2d}:~/clc_experiment/build/bin/".format(
        #     index)
    else:
        cmd = "scp ./config/*.ini {}@node{:0>2d}:~/clc_experiment/config/".format(user_name,
                                                                                  index)
    subprocess.call(cmd, shell=True, stdout=subprocess.DEVNULL)


def update(n, update_type):
    thread_list = []
    if update_type == "all":
        generate_nodes_config(n)
        for i in range(1, n + 1):
            t = threading.Thread(target=send_one, args=(i, True))
            t.start()
            thread_list.append(t)
            t = threading.Thread(target=send_one, args=(i, False))
            t.start()
            thread_list.append(t)
    else:
        if update_type == "config":
            generate_nodes_config(n)
        for i in range(1, n + 1):
            t = threading.Thread(target=send_one, args=(
                i, update_type == "program"))
            thread_list.append(t)
            t.start()
    for t in thread_list:
        t.join()
    print("== update ok ==")


def call_single_node(index):
    cmd = "ssh {}@node{:0>2d} \"source /etc/profile;cd ~/clc_experiment;/usr/lib/jvm/jdk-11.0.9/bin/java SocketNode {}\"".format(user_name,
                                                                                                                                 index, index)
    # cmd = "ssh {}@node{:0>2d} \"cd ~/clc_experiment;./build/bin/node_main {}\"".format(index, index)
    # get_bytes = subprocess.check_output(cmd, shell=True)
    # if index == 1:
    #     result = get_bytes.decode('utf-8')
    #     print(result)
    # print("=== node {} ===\n".format(index) + result.strip())
    if index == 1:
        subprocess.call(cmd, shell=True)
    else:
        subprocess.call(cmd, shell=True, stdout=subprocess.DEVNULL)


def call_nodes(n):
    thread_list = []
    for i in range(1, n + 1):
        t = threading.Thread(target=call_single_node, args=(i,))
        t.start()
        thread_list.append(t)
    for t in thread_list:
        t.join()
    # print("== call nodes ok ==")


def call_master():
    subprocess.call("java SocketNode 0", shell=True)
    # subprocess.call("build/bin/node_main 0", shell=True)


def generate_nodes_config(n):
    lines = []
    with open("config/nodes_ip.txt", "r") as f:
        lines = f.readlines()
    if lines:
        with open("config/nodes.ini", "w") as wf:
            for i in range(n+1):
                wf.write(lines[i].strip() + "\n")
    print("generate_config ok")


def run_exp(n):
    master_t = threading.Thread(target=call_master)
    master_t.start()
    nodes_t = threading.Thread(target=call_nodes, args=(n,))
    nodes_t.start()
    master_t.join()
    nodes_t.join()


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "nodes_config":
        generate_nodes_config(int(sys.argv[2]))
    elif len(sys.argv) == 4 and sys.argv[1] == "update":
        update(int(sys.argv[3]), sys.argv[2])
    elif len(sys.argv) == 3 and sys.argv[1] == "exp":
        n = int(sys.argv[2])
        exp_num = 10
        for i in range(exp_num):
            run_exp(n)

    else:
        print("Usage: exp  n")
        print("   Or: update  [all/config/program]  n")
        print("   Or: nodes_config n")
