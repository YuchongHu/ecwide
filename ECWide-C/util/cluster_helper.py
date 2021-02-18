import subprocess
import threading
import sys

user_name = "user"
java_path = "${JAVA_HOME}/bin/java"
work_dir = "/home/user/ecwide-c"
chunks_dir = work_dir + "/test/chunks"
data_source = "zero"

def multithread_work(n, func, *arg):
    thread_list = []
    for i in range(1, n + 1):
        t = threading.Thread(target=func, args=(i, *arg))
        t.start()
        thread_list.append(t)
    for t in thread_list:
        t.join()

def send_one(index, flag):
    if flag == "program":
        cmd = "scp -r ./classes/ ./lib/ ./util/ {}@node{:0>2d}:{}/".format(user_name, index, work_dir)
    elif flag == "config":
        cmd = "scp ./config/* {}@node{:0>2d}:{}/config/".format(user_name, index, work_dir)
    else:
        out_bytes = subprocess.check_output("ls " + chunks_dir + "/%d_*" % index, shell=True)
        name = out_bytes.decode('utf-8').strip()
        cmd = "scp {} {}@node{:0>2d}:/tmp/stdfile".format(name, user_name, index)
    subprocess.call(cmd, shell=True, stdout=subprocess.DEVNULL)

def update(n, update_type):
    if update_type == "all":
        generate_nodes_config(n)
        multithread_work(n, send_one, "config")
        multithread_work(n, send_one, "program")
    else:
        if update_type == "config":
            generate_nodes_config(n)
        multithread_work(n, send_one, update_type)
    print("update %s ok" % update_type)


def start_single_node(index):
    cmd = "ssh {0}@node{1:0>2d} \"source /etc/profile;cd {3};nohup {2} DataNode {1} > output.log 2>&1 &\"".format(user_name, index, java_path, work_dir)
    subprocess.call(cmd, shell=True)

def stop_single_node(index):
    cmd = "ssh {}@node{:0>2d} \"pkill java\"".format(user_name, index)
    subprocess.call(cmd, shell=True)

def start_cluster(n):
    subprocess.call("nohup java MasterNode > output.log 2>&1 &", shell=True)  #start_master
    multithread_work(n, start_single_node)
    print("start_cluster ok")

def stop_cluster(n):
    multithread_work(n, stop_single_node)
    subprocess.call("pkill java", shell=True)  #stop_master
    print("stop_cluster ok")

def generate_nodes_config(n):
    lines = []
    with open("config/nodes_ip.txt", "r") as f:
        lines = f.readlines()
    if lines:
        with open("config/nodes.ini", "w") as wf:
            for i in range(n+1):
                wf.write(lines[i].strip() + "\n")
    print("generate_config ok")

def get_node_chunk_info(n):
    result = [None] * (n + 1)
    out_bytes = subprocess.check_output(["ls", chunks_dir])
    names = out_bytes.decode('utf-8').split('\n')
    for name in names:
        if not name:
            continue
        s = name.split("_")
        if not s[0].isnumeric():
            continue
        node = int(s[0])
        if node > n:
            continue
        result[node] = (s[1], s[2])
    return result

def call_single_copy(index, num, chunk_type, chunk_id):
    cmd = "ssh {}@node{:0>2d} \"cd {};bash util/copy_chunks.sh {} {} {}\"".format(user_name, index, work_dir, num, chunk_type, chunk_id)
    subprocess.call(cmd, shell=True)

def call_single_clean(index):
    cmd = "ssh {0}@node{1:0>2d} \"mkdir -p {2};rm -f {2}/* \"".format(user_name, index, chunks_dir)
    subprocess.call(cmd, shell=True)

def distribute_chunks(n, num):
    cmd = "mkdir -p {0};rm -f {0}/*".format(chunks_dir)
    subprocess.call(cmd, shell=True)
    subprocess.call("java -Xmx8192m ChunkGenerator {} toy".format(data_source), shell=True)  #generate chunks
    update(n, "std_chunks")
    info = get_node_chunk_info(n)
    multithread_work(n, call_single_clean)
    # call for copying
    thread_list = []
    for i in range(1, n + 1):
        cur_info = info[i]
        t = threading.Thread(target=call_single_copy, args=(i, num, *cur_info))
        t.start()
        thread_list.append(t)
    for t in thread_list:
        t.join()
    print("distribute_chunks ok")


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "nodes_config":
        generate_nodes_config(int(sys.argv[2]))
    elif len(sys.argv) == 4 and sys.argv[1] == "update":
        update(int(sys.argv[3]), sys.argv[2])
    elif len(sys.argv) == 3 and sys.argv[1] == "start":
        start_cluster(int(sys.argv[2]))
    elif len(sys.argv) == 3 and sys.argv[1] == "stop":
        stop_cluster(int(sys.argv[2]))
    elif len(sys.argv) == 4 and sys.argv[1] == "distribute_chunks":
        distribute_chunks(int(sys.argv[2]), int(sys.argv[3]))

    else:
        print("Usage: start  n")
        print("   Or: stop  n")
        print("   Or: update  [all/config/program]  n")
        print("   Or: distribute_chunks  n  stripe_num")
        print("   Or: nodes_config  n")
