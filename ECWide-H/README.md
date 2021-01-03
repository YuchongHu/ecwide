ECWide-H
=====

This is the source code of ECWide-H described in our paper presented in USENIX FAST'21. 
We implemet ECWide-H on Ubuntu-16.04 as follows.

Preparation for required libraries
====

Users can use `apt-get` to install required libraries.

 - make & automake-1.14
 - yasm & nasm
 - libtool
 - boost libraries (libboost-all-dev)
 - libevent (libevent-dev)
`$ sudo apt-get install gcc g++ make cmake autogen autoconf automake yasm nasm libtool libboost-all-dev libevent-dev`

Users can install **Intel®-storage-acceleration-library (ISA-l)** manually.

    $ tar -zxvf isa-l-2.14.0.tar.gz
    $ cd isa-l-2.14.0
    $ sh autogen.sh
    $ ./configure; make; sudo make install


ECWide-H Installation
====

**Memcached Servers, in */proxy***

Users can use `apt-get` to install memcached servers, or by the source code of Memcached from *http://memcached.org*.
For simplicity, we enable the physic node to generate multiple memcached servers. Run `bash cls.sh` to generate memcached servers with different ports.

    $ sudo apt-get install memcached

Users can install memcached clients within racks by modified source code of libmemcached-1.0.18.

	$ cd libmemcached-1.0.18
    $ sh configure; make; sudo make install
    $ export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH  #(if it ocuurs library path issue)

**Connect to Memcached Servers, in */requestor***
-	User should record all physic nodes' IPs and hostnames in "*node_ip.txt*";
-	User should configure that the memcached client connect to all physic nodes by SSH without password.
-	User can use `nodes_config GROUP RACK` command in "*cloud_exp.py*" to partition nodes according to "*node_ip.txt*", generate "*ip.ini*" and put into all physic nodes.

**Background traffic, in */back***
- User can deploy multiple nodes as additional clients, and run `bash back_run.sh` via "*cloud_exp.py*".

Benchmark
====
-	User can use the provided trace "*yscb_set.txt*" and "*yscb_test.txt*".
-	User can configure parameters of `(n,k,r,z)CL` in "*common.hpp*" in both */requestor* and */proxy*.
-	User can use `update` command in "*cloud_exp.py*" to spread the updated configure to all memcached clients within racks.
-	User can `exp GROUP RACK THREAD` command in "*cloud_exp.py*" to do experiments.