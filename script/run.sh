#!/bin/bash

# path size wtype wbuffersize rtype rbuffersize
filename=/alluxiobenchmark/testfile.t
testFileSize=1024 # test file size 1GB
wBufferSize=4096  # write buffer size 4096 byte
rBufferSize=4096  # read buffer size 4096 byte
wType=MUST_CACHE  # write data to alluxio,  storage type
rType=CACHE_PROMOTE # read data from alluxio,  storage type


alluxio fs rm $filename >/dev/null 2>&1

if [ ! -n "$HADOOP_HOME" ]; then
    echo "ERROR: You must be set \$HADOOP_HOME in enviroment!"
    exit -1
fi

CLASSPATH=.

for f in `ls $HADOOP_HOME/share/hadoop/common/*.jar`
do
    CLASSPATH=$CLASSPATH:$f
done

for f in ` ls $HADOOP_HOME/share/hadoop/hdfs/*.jar`
do
    CLASSPATH=$CLASSPATH:$f
done

for f in ` ls $HADOOP_HOME/share/hadoop/common/lib/*.jar`
do
    CLASSPATH=$CLASSPATH$f
done

for f in `ls $HADOOP_HOME/share/hadoop/yarn/*.jar`
do
    CLASSPATH=$CLASSPATH$f
done
#echo $CLASSPATH

java -classpath alluxioBenchmark-0.1-SNAPSHOT.jar:/software/servers/alluxio-1.2.0/assembly/target/alluxio-assemblies-1.2.0-jar-with-dependencies.jar   com.jd.alluxioBenchmark.AlluxioWriteTool $filename $testFileSize $wType $wBufferSize


java -classpath alluxioBenchmark-0.1-SNAPSHOT.jar:/software/servers/alluxio-1.2.0/assembly/target/alluxio-assemblies-1.2.0-jar-with-dependencies.jar   com.jd.alluxioBenchmark.AlluxioReadTool $filename $rType $rBufferSize

hadoop fs -rm $filename >/dev/null 2>&1
java -classpath $HADOOP_HOME/etc/hadoop/:$CLASSPATH:alluxioBenchmark-0.1-SNAPSHOT.jar:/software/servers/alluxio-1.2.0/assembly/target/alluxio-assemblies-1.2.0-jar-with-dependencies.jar   com.jd.alluxioBenchmark.HDFSWriteTool $filename $testFileSize $wBufferSize  2>/dev/null


java -classpath $HADOOP_HOME/etc/hadoop/:$CLASSPATH:alluxioBenchmark-0.1-SNAPSHOT.jar:/software/servers/alluxio-1.2.0/assembly/target/alluxio-assemblies-1.2.0-jar-with-dependencies.jar   com.jd.alluxioBenchmark.HDFSReadTool $filename $rBufferSize 2>/dev/null
