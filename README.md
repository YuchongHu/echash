# ECHash

This is the source code for ECHash described in our paper presented in ACM SoCC'19. 
ECHash is tested on Ubuntu 14.04 and Red Hat Release 6.5 with GCC (version 4.8.4 - 4.9.2), and we take the Debian and Ubuntu environment as an example.


Preparation
----

These are the required libraries that users need to download separately.
Users can use apt-get to install the required libraries.

 - make & automake-1.14
 - yasm & nasm
 - libtool
 - boost libraries (libboost-all-dev)
 - libevent (libevent-dev)
`$ sudo apt-get install gcc g++ make cmake autogen autoconf automake yasm nasm libtool libboost-all-dev libevent-dev`

Users can install the following library manually: **Intel®-storage-acceleration-library (ISA-l)**.

    $ tar -zxvf isa-l-2.14.0.tar.gz
    $ cd isa-l-2.14.0
    $ sh autogen.sh
    $ ./configure; make; sudo make install

Users can use compile and run "*ec*" to confirm that ISA-l is successfully installed.

	$ gcc ec.cpp -o ec -lisal
	$./ec 1 2

## ECHash Installation

### **Memcached Servers**

Users can use apt-get to install Memcached servers, or by the source code of Memcached from *http://memcached.org*.

    `$ sudo apt-get install memcached`

-	For standalone setup, users can start and configure the initialization parameters (e.g. `-d -m`) of Memcached servers by `$ sh cls.sh` manually. Also users can configure **Init/Scale/Repair** Memcached IP/Ports in "*config.txt*" for testing.
-	For distributed setup, users can configure distinguished IP/Ports in each node.

### **ECHash Proxy**

	$ cd libmemcached-1.0.18
	$ sh configure; make; sudo make install
	$ export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH  #(if it ocuurs library path issue)


Workloads
----

Users can use YCSB to generate workloads after executing "*ycsb_gen.sh*" and "*gen_workloads*".
For simplicity, users can also use the providing "*ycsb_set.txt*" and "*ycsb_test.txt*" as workloads (8MB for READ ONLY) to run ECHash and skip this workloads section.

1.Install *Maven-3.1.1* and configure the environment, and check the version by `mvn -version`.

	$ sudo tar -zxvf apache-maven-3.1.1-bin.tar.gz -C /usr/local
	$ cd /usr/local
	$ sudo ln -s  apache-maven-3.1.1 maven
	$ sudo vim /etc/profile.d/maven.sh
		#Add the following to maven.sh
			export M2_HOME=/usr/local/maven
			export PATH=${M2_HOME}/bin:${PATH}
	$ source /etc/profile.d/maven.sh

2.Install *java 8 update 151* and configure the environment, and check the version by `java -version`.

    $ sudo mkdir /usr/local/java
    $ sudo cp jdk-8u151-linux-x64.tar.gz /usr/local/java
    $ cd /usr/local/java
    $ sudo tar zxvf jdk-8u151-linux-x64.tar.gz
    $ sudo vim ~/.bashrc
    	#Add the following to .bashrc
    		export JAVA_HOME=/usr/local/java/jdk1.8.0_151 
    		export JRE_HOME=${JAVA_HOME}/jre  
    		export CLASSPATH=.:${JAVA_HOME}/lib:${JRE_HOME}/lib  
    		export PATH=${JAVA_HOME}/bin:$PATH
    $ source ~/.bashrc

3.Install *YCSB* (https://github.com/brianfrankcooper/YCSB.git), and build the Memcached binding.

	$ unzip YCSB-master.zip
	$ cd YCSB-master
	$ mvn -pl com.yahoo.ycsb:memcached-binding -am clean package

4.Use the **basic** parameter in "*workloads_test*" to generate the raw workloads.

-	Write/Read ratios (e.g. readproportion=1.0).
-	The rough value length (e.g. fieldlength=256)
-	Operation times (e.g. operationcount=32768)


	copy "workload_test" to YCSB-master/workloads
	copy "ycsb_gen.sh" to YCSB-master`

Configure `YCSB_HOME=` (path to YCSB-master) in "*ycsb_gen.sh*" firstly and then run the script to generate the workloads.

	$ sh ycsb_gen.sh


5.Pre-treatment workloads, execute "*gen_workloads*" to generate "*ycsb_set.txt*" and "*ycsb_test.txt*". The value length of objects can be configured in "*gen_workloads.cpp*" (`#define LENGTH 256`), and the key size of objects is configured around 20B. 

	copy "ycsb_load.load" and "ycsb_run.run" from YCSB-master to the directory of "gen_workloads.cpp"
	$ gcc gen_workloads.cpp -o gen_workloads
	$ ./gen_workloads


Benchmarks
----

-	Users can run *Basic I/O* with multiple threads (`#define NTHREADS 16` in "*io.cpp*"), but other benchmarks with single thread currently (`#define NTHREADS 1` in "*scale.cpp*" and "*repair.cpp*").
-	Function Prototype: `void MRmemcached_init_addserver(ECHash pointer, Memcached server IP, Port, Ring ID);` 
-	Users can change the directory of workloads ("*ycsb_set.txt*" and "*ycsb_test.txt*") in "*para.txt*" by configuring `Workloads Path=` (path to workloads).
-	Users can configure (N,K) coding in "*libmemcached-1.0.18/libmemcached-1.0/struct/ring.h*" (e.g. (5,3) coding as default) and keep N=RING_SIZE, where N = the number of data and parity chunks, K = numbers of data chunks. Besides, users should make sure that the number of rings of the number memcached servers initialized in "*cls.sh*" is consistent with the RING_SIZE of "*ring.h*".
-	Users can accelerate testing process by redirecting progress tracking printings to a temporary file, e.g., by "> tmp.txt".
-	Users can reset Memcached servers by `sh cls.sh` before each testing.


Compile all source files.

	$ make

1.**Basic I/O Performance** 

    $ ./io > tmp.txt

2.**Scale-out/Scale-in Performance**, users should make sure that scale-in memcached servers are already in this hash ring.
./scale [out|in] S, S indicates the number of scale out/in nodes.

	$ ./scale out S > tmp.txt # or ./scale in S > tmp.txt

3.**Degraded Read Performance During Scaling**, users can install mysqlclient for C++ firstly, create MySQL database and tables, and put the workloads into MySQL before running ECHash.

4.**Node Repair Performance**, users should make sure that the repaired memcached servers are already in this hash ring. Note that we only simulate the repair process including degraded reads and objects migration.

	$ ./repair > tmp.txt

## Publication

Liangfeng Cheng, Yuchong Hu, and Patrick P. C. Lee.
**"Coupling Decentralized Key-Value Stores with Erasure Coding."**
Proceedings of the ACM Symposium on Cloud Computing 2019 (SoCC 2019), Santa Cruz, CA, USA, November 2019.
(AR: 39/157 = 24.8%)

## Contact

Please email to Yuchong Hu ([yuchonghu@hust.edu.cn](mailto:yuchonghu@hust.edu.cn)) if you have any questions.

## Our other works

Welcome to follow our other works!

1. FAST 2021: https://github.com/YuchongHu/ecwide
2. ICDCS 2021: https://github.com/YuchongHu/stripe-merge
3. SoCC 2019: https://github.com/YuchongHu/echash
4. INFOCOM 2018: https://github.com/YuchongHu/ncscale
5. TOS: https://github.com/YuchongHu/doubler