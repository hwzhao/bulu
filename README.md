bulu
====

a MOLAP engine on HBase, built with Akka.

Usage
----

 #1. Start master node in sbt console:
```
    run-main com.cubee.CubeMaster 2551
```

 #2. Start worker node in sbt console:
```
    run-main com.cubee.CubeWorker
  ```  
  
 #3. after worker nodes initiated, enter following command in master console to begin to build the cube:
```
    build hive1g init
```
    
 #4. after building is finished, enter follwoing commands in master console to query the cube:
```
    execute hive1g query7 GEN=M&MS=S&ES=College&YEAR=2000

    execute hive1g query42 YEAR=2000&MONTH=7

    execute hive1g query52 YEAR=2000&MONTH=7

    execute hive1g query55 YEAR=2000&MONTH=7&MANAGER=21

    execute hive1g All1g YEAR=2000&MONTH=7&MANAGER=21
```

 #5. enter "stop" in master console to close all worker nodes and master node.

Configuration
----

see scr/main/resources/application.conf as an example


Test Result
----
the query time of MOLAP engine is quicker than query time with Hive in same nodes :
```
data volume 1G	     10G	100G
	        
query 7	    14X 	24X 	19X 

query 42    53X 	49X 	48X 

query 52    53X 	56X 	50X 

query 55    40X 	56X 	39X 
```

Please refer two essays for the system design and test results:

A Practice of TPC-DS Multidimensional Implementation on NoSQL Database Systems， TPCTC 2013（http://www.tpc.org/tpctc/tpctc2013/default.asp）

A Multidimensional OLAP Engine Implementation in Key-Value Database Systems， WBDB 2013 （http://clds.ucsd.edu/wbdb2013.cn/program）

