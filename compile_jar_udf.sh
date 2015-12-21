#!/bin/bash 

# java:  compile a java file, and if it went well run it
export HH=/opt/hadoop-2.7.1/share/hadoop
export HI=/opt/apache-hive-1.2.1-bin
export CLASSPATH=$CLASSPATH\
:$HH/common/hadoop-common-2.7.1.jar\
:$HH/hdfs/hadoop-hdfs-2.7.1.jar\
:$HH/mapreduce/lib/*\
:$HH/common/lib/*\
:$HH/tools/lib/*\
:$HI/lib/hive-common-1.2.1.jar\
:$HI/lib/lib/hive-contrib-1.2.1.jar\
:$HI/lib/hive-exec-1.2.1.jar

S=UdfRoughDistance.java
T=${S%.java}.class
E=${S%.java}

echo "."
echo "."
echo "."

# compile
javac $S

# check if class file was produced
if [ ! -e $T ] 
then
    echo "##jcr: '$T' doesn't exist, can't JAR it." 
    exit 1
fi

# if class is younger then source, then package it
S_AGE=`stat -c %Y $S`
T_AGE=`stat -c %Y $T`

if [ $S_AGE -le $T_AGE ] 
then 
    jar cvf udf.jar $T
else
    echo "## class file is older than java source!"
    exit 1
fi
