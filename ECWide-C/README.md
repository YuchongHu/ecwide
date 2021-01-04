ECWide-C
=====

This is the source code of ECWide-C described in our paper presented in USENIX FAST'21. 
We implemet ECWide-C on Ubuntu-16.04 as follows.

Preparation of required libraries
====

Users can use `apt` to install the required libraries.

 - gcc & g++
 - make & cmake
 - nasm
 - libtool & autoconf
 - git

```bash
$  sudo apt install gcc g++ make cmake nasm autoconf libtool git
```

Users can install the following library manually:

- **Intel®-storage-acceleration-library (ISA-L)**.

    ```bash
    $  git clone https://github.com/intel/isa-l.git
    $  cd isa-l
    $  ./autogen.sh
    $  ./configure; make; sudo make install
    ```
    
- **Sockpp**

    ```bash
    $  git clone https://github.com/fpagliughi/sockpp.git
    $  cd sockpp
    $  mkdir build ; cd build
    $  cmake ..
    $  make
    $  sudo make install
    $  sudo ldconfig
    ```

- **JDK 11**

    We can use various implementations of JDK, such as OpenJDK and Oracle JDK. Here we take Oracle JDK as an example.

    In Ubuntu 16.04, we may install JDK via  `dpkg` easily. The Linux x64Debian Package is available on https://www.oracle.com/java/technologies/javase-jdk11-downloads.html .

    ```bash
    $  sudo dpkg -i jdk-11.0.9_linux-x64_bin.deb
    ```

    The JDK 11 is installed on  `/usr/lib/jvm/jdk-11.0.9` in default. Note that the directory name may differ, which depends on the version you downloaded. Then we need to configure the environment variables for JDK. 

    ```bash
    $  vim ~/.bashrc
    ```

     Append following contents:

    ```bash
    export JAVA_HOME=/usr/lib/jvm/jdk-11.0.9
    export CLASSPATH=.:${JAVA_HOME}/lib:./classes
    export LD_LIBRARY_PATH=./lib
    export PATH=${JAVA_HOME}/bin:${PATH}
    ```

    Activate and test:

    ```bash
    $  source ~/.bashrc
    $  java --version
    java 11.0.9 2020-10-20 LTS
    Java(TM) SE Runtime Environment 18.9 (build 11.0.9+7-LTS)
    Java HotSpot(TM) 64-Bit Server VM 18.9 (build 11.0.9+7-LTS, mixed mode)
    ```

ECWide-C Installation
====

- Users can install ECWide-C via make.

	```bash
	$  make
	```

- Before running, users should prepare the `nodes_ip.text`, in which the first line is `master_node`, and the following are IPs of all `data_nodes`. Next, generate the `nodes.ini` using script `util/cloud_exp.py`, by passing the number of `data_nodes` that actually runs. 

	```bash
	$  python3 util/cloud_exp.py nodes_config n
	```

- The example configure of scheme are presented in `scheme.ini`.

- User should configure that the `master_node` connect to all `data_nodes` by SSH without password.

- Users can use  script `util/cloud_exp.py` to update the configure or program in `data_nodes` either.

  ```bash
  $  python3 util/cloud_exp.py update [all/config/program] n
  ```

- Use `dd` to generate a random block in all `data_nodes` to test.

  ```bash
  $  dd if=/dev/urandom of=test/stdfile bs=1M count=64
  ```

- To run the experiment, users should run the `master_node` ,

  ```bash
  $  java SocketNode 0
  ```

  and run the `data_nodes` respectively.

  ```bash
  $  java SocketNode [1/2/3/...]
  ```

  Or Users can run the script on  `master_node`  to conduct the experiment.

  ```bash
  $  python3 util/cloud_exp.py exp n
  ```

  