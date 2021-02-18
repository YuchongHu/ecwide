ECWide-C
=====

Here's the source code of ECWide-C described in our paper presented in USENIX FAST'21. ECWide-C is a prototype system designed for cold storage, which implemented TL, LRC and CL.

Next you will see the instructions for installing and running ECWide-C.



Installation
----

We developed the system on Ubuntu 16.04. So the following instructions on preparing development environment are also based on Ubuntu 16.04.

### Common

We can use  `apt`  to install the required tools.

 - gcc & g++
 - make
 - nasm
 - libtool & autoconf
 - git
 - ssh & openssh-server

```bash
$  sudo apt update
$  sudo apt install gcc g++ make nasm autoconf libtool git ssh openssh-server
```

### ISA-L

ECWide-C use [ISA-L](https://github.com/intel/isa-l)(Intel(R) Intelligent Storage Acceleration Library) to perform encoding operations. We need to install ISA-L manually.

```bash
$  git clone https://github.com/intel/isa-l.git
$  cd isa-l
$  ./autogen.sh
$  ./configure; make; sudo make install
```

### JDK 11

ECWide-C is based on JDK 11. We can use various implementations of JDK, such as OpenJDK and Oracle JDK. Here we take OpenJDK 11.02 as an example. We can download OpenJDK 11.02 on [available mirror](https://repo.huaweicloud.com/openjdk/11.0.2/openjdk-11.0.2_linux-x64_bin.tar.gz). Assumed that the package had been downloaded previously.

```bash
$  tar -zxvf openjdk-11.0.2_linux-x64_bin.tar.gz
$  sudo mkdir -p /usr/lib/jvm
$  sudo mv jdk-11.0.2 /usr/lib/jvm/
```

Then we need to configure the environment variables for JDK. 

```bash
$  sudo vim /etc/profile 
```

 Append following contents:

```bash
export JAVA_HOME=/usr/lib/jvm/jdk-11.0.2
export CLASSPATH=.:${JAVA_HOME}/lib:./classes
export LD_LIBRARY_PATH=./lib
export PATH=${JAVA_HOME}/bin:${PATH}
```

Activate and test:

```bash
$  source /etc/profile
$  java --version
openjdk 11.0.2 2019-01-15
OpenJDK Runtime Environment 18.9 (build 11.0.2+9)
OpenJDK 64-Bit Server VM 18.9 (build 11.0.2+9, mixed mode)
```

### Compile

After preparing the development environment above, we can compile ECWide-C via make easily.

```bash
$  make
```



## Preparation

### Configuration

There are three configuration files in the config folder.

#### nodes.ini

This file describes the IP addresses of nodes included. The first line is the IP of MasterNode, and the lines as follows are IP addresses of DataNodes. The order of IP is the same as that of node index. Each IP occupies one line.

#### scheme.ini

This file describes the coding scheme info. The keys and the values are separated by '='. Each key occupies one line.

| Key             | Description                                                  |
| --------------- | ------------------------------------------------------------ |
| codeType        | Type of coding scheme. One of TL, LRC and CL.                |
| k               | The number of data chunks.                                   |
| globalParityNum | The number of global parity chunks. f = globalParityNum in TL, f = globalParityNum + 1 in LRC and CL. |
| groupDataNum    | The number of data chunks in a group. Only valid in LRC and CL. |
| chunkSizeBits   | ChunkSize = 2 ^ chunkSizeBits  bytes                         |

For example,  scheme content bellow shows a CL scheme, with 32 data chunks, 3 global parity chunks, 11 data chunks every group, and its chunk size is 2 ^ 26 bytes = 64 MiB. Also it implicates that f = 4. 

```ini
codeType = CL
k = 32
groupDataNum = 11
globalParityNum = 3
chunkSizeBits = 26
```

#### settings.ini

This file describes the common settings in ECWide-C prototype system.

| Key             | Description                                                  |
| --------------- | ------------------------------------------------------------ |
| chunksDir       | The directory that stores chunks. Absolute path is required. |
| concurrentNum   | The number of working thread in sending thread pool and receiving thread pool. |
| multiNodeEncode | Enable multi-node encoding. Default value: false. Only valid in CL. |
| useLrs          | Enable LRS in node repair. Default value: false. Only valid in CL. |
| useLrsRequestor | Enable LRS requestor in node repair. Default value: false. Only valid in CL, when useLrs is true. |

Here's an example.

```ini
chunksDir = /home/user/ecwide-c/test/chunks
concurrentNum = 1
multiNodeEncode = true
useLrs = false
useLrsRequestor = false
```

### Chunks

When a DataNode process starts, it will scan the chunksDir given in  `config/settings.ini`  to check the chunks it owns. To keep it simple, we assume that all the chunks follow the naming pattern bellow. A chunk name consists of three parts, separated by '_'.

| Part        | Description                                                  |
| ----------- | ------------------------------------------------------------ |
| Chunk Type  | 'D' -> data chunk; 'L' -> local parity chunk; 'G' -> global parity chunk. Local parity chunk only exists in LRC and CL. |
| Stripe ID   | ID of stripe that current chunk belongs to.                  |
| Chunk Index | The index of chunk in same type of chunks, in the same stripe. |

For example, chunk "D_2_3" means data chunk 3 of stripe 2. In LRC, stripe 2 is composed of chunk "D_2_0", "D_2_1", ..., "L_2_0",  "L_2_1", ..., "G_2_0", ..., and each of them should be stored in different node.

We can use  `ChunkGnerator`  to generate some chunks. The first parameter is the source of data chunks, where "zero" means  ```/dev/zero```  and "urandom " represents  ```/dev/urandom``` . The second parameter is the number of stripes. And then we can distribute them manually.

```bash
$  java -Xmx8192m ChunkGenerator [zero/urandom] [stripes_num]
```

Note that parameter ```-Xmx8192m```  may be required when k is large, e.g. k = 64.

### Bandwidth

The gap between cross-rack bandwidth and inner-rack bandwidth is one of the key. In LRC, every DataNode is in different rack. In TL and CL, there are up to f DataNodes in a rack for each stripe.  Only when the bandwidth is set properly can the correct performance of the three schemes be measured. If your nodes are in a flat structure, you can utilize script  ```util/limit_bandwidth.sh```  to create logical racks, in order to simulate the hierarchy that fits corresponding coding scheme.

First of all, we need to check the name of network interface via  ```ifconfig``` , and fill in the variable dev_name at the head of the script. The default name is "eth0".

```shell
#!/bin/bash 
dev_name=eth0
```

For LRC, we can limit the total bandwidth directly.

```bash
$  sudo bash util/limit_bandwidth.sh all <BANDWIDTH/mbit>
```

For TL and CL, we only limit the cross-rack bandwidth. The script detect the configuration files, and group the nodes into logical racks in order. For example, when f = 4, node 1, node 2, node 3 and node 4 are considered to be in a same rack, while node 5, node 6, node 7 and node 8 are considered to be in another rack. The bandwidth between nodes in the same rack will not be limited. If the hostname of node is named "nodeXX" where “XX” is corresponding node index,  it is no need to provide node index in the parameters.

```bash
$  sudo bash util/limit_bandwidth.sh rack <BANDWIDTH/mbit> <NODE_INDEX>
$  sudo bash util/limit_bandwidth.sh rack <BANDWIDTH/mbit>
```

Note that we should run the script on every DataNodes to get proper settings.



## Run

Before running the system, we need to make sure the programs and configurations are consistent between nodes, including MasterNode and DataNodes. Chunks need to be placed in corresponding DataNodes respectively. Also, make sure the cluster hierarchy or the bandwidth settings suit coding scheme.

The operations bellow are all conducted in the root directory of ECWide-C.

### Background Process

Firstly we start the process on MasterNode,

```bash
$  java MasterNode
```

and start the process on each DataNodes separately, where the number denotes the node index, which should be consistent with the info in  `nodes.ini` . Note that the index of DataNode starts from 1 in our system.

```bash
$  java DataNode [1/2/3/...]
```

### Make Request

Run  `RequestClient`  on a DataNode to make request. Available request are repair_chunk, repair_node and multinode_encode. 

#### Repair Chunk

Provide the name of chunk that want to repair in the parameters.

```bash
$  java RequestClient repair_chunk <CHUNK_NAME>
```

#### Repair Node

Provide the index of node that want to repair in the parameters. It is equivalent to repairing all the chunks storing in given node.

```bash
$  java RequestClient repair_node <NODE_INDEX>
```

#### Multi-node Encoding

Provide the number of stripes that want to perform encoding in the parameters. Note that multinode_encode is only valid in CL, and you need to make multinode_encode request on node[ groupNum ], where groupNum = ⌈ k / groupDataNum ⌉. The multi-node encoding will conduct in a same rack, in which are nodes from node01 to node[groupNum].  We can use the script to clean previous bandwidth settings, so as to assure that there are enough nodes in a logical rack. Also make sure there are enough chunks( >=groupDataNum ) in each nodes to simulate a multi-node encoding operation.

```bash
$  sudo bash util/limit_bandwidth.sh clean
$  java RequestClient multinode_encode <STRIPES_NUM>
```



## Cluster Helper Script

It is not easy to manage so many nodes, especially when the programs or configuration files are modified. We present a python script  `util/cluster_helper.py`  to help simplifying the management of cluster.

### Preparation

#### SSH

The script is based on SSH connection without password. It is required to enable MasterNode to access all DataNodes via SSH without password.

To better identify, we set the hostname of DataNode to "nodeXX", where "XX" is its index. 

```bash
$  sudo vim /etc/hostname
```

Append to file  `/etc/hosts`  on MasterNode :

```bash
$  sudo vim /etc/hosts
192.168.1.100	master
192.168.1.101	node01
192.168.1.102	node02
192.168.1.103	node03
...				...
```

Generate a SSH key on MasterNode, and then get the permission to access each DataNode with this key. Assume that all DataNodes have a same user "user".

```bash
$  ssh-keygen
$  ssh-copy-id user@node01
$  ssh-copy-id user@node02
$  ssh-copy-id user@node03
...
```

#### IP Addresses of All Nodes

We need a file  `config/nodes_ip.txt`  that contains IP addresses of all nodes. Like  `config/nodes.ini` , the first line of the file is IP of MasterNode, followed by IP addresses of all DataNodes. The order of IP is the same as that of node index.  Each IP occupies one line. The difference to  `config/nodes.ini`  is that  `config/nodes.ini`  only includes the nodes that need to run at this time, and it can be generated by tailoring  `config/nodes_ip.txt` .

````
192.168.1.100
192.168.1.101
192.168.1.102
192.168.1.103
...
````

#### Options

Fill in the variables of options at the head of script.

```python
user_name = "user"
java_path = "${JAVA_HOME}/bin/java"
work_dir = "/home/user/ecwide-c"
chunks_dir = work_dir + "/test/chunks"
data_source = "zero"
```



### Functions

All functions  bellow are achieved by running the script on MasterNode. And the n in parameter is the number of DataNodes in this run.

#### Generate nodes.ini

Tailor  `config/nodes_ip.txt`  to generate  `config/nodes.ini` . 

```bash
$  python3 util/cluster_helper.py  nodes_config  n
```

#### Update

Send the programs or configuration files to DataNodes.  "All" means both "config "and "program". It will generate a new  `config/nodes.ini`  automatically when the parameter is "all" or "config". 

```bash
$  python3 util/cluster_helper.py  update  [all/config/program]  n
```

#### Distribute Chunks

This function is aim to generate some toy chunks in different DataNodes rapidly. The script will call  `ChunkGenerator`  to generate a stripe, distribute the chunk to diverse nodes and then call another script to generate more chunks  by copying the same chunk in that node. The source of data depends on the  `data_source`  option at the head of the script (see section [Chunks](#chunks)).

```bash
$  python3 util/cluster_helper.py  distribute_chunks  n  stripe_num
```

#### Start

Call  `MasterNode`  process and  `DataNode`  processes.

```bash
$  python3 util/cluster_helper.py  start  n
```

#### Stop

Kill related processes in MasterNode and DataNodes.

```bash
$  python3 util/cluster_helper.py  stop  n
```

### Summary

The procedure to run a cluster **on MasterNode** with the script is as follows.

1. Recompile if the source codes are modified.
2. Check the configuration files ( `config/scheme.ini` ,  `config/config.ini` ).
3. [Distribute Chunks](#distribute-chunks) with the script. (Unnecessary if run the same scheme again)
4. [Update](#update) with the script (when programs or configuration are modified).
5. Check the [bandwidth settings](#bandwidth) of DataNodes.
6. [Start](#start) with the script.
7. [Make request](#make-request).
8. [Stop](#stop) with the script.