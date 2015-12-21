#!/bin/bash 

# java:  compile a java file, and run it
export HH=/opt/hadoop-2.7.1/share/hadoop
export CLASSPATH=$CLASSPATH\
:$HH/common/hadoop-common-2.7.1.jar\
:$HH/hdfs/hadoop-hdfs-2.7.1.jar\
:$HH/mapreduce/lib/*\
:$HH/common/lib/*\
:$HH/tools/lib/*\
:/opt/jedis/jedis-2.8.0.jar

#S=$1
S="Redis2hdfs.java"
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
    echo "##jcr: '$T' doesn't exist, can't run it." 
    exit 1
fi

# if class is younger then source, then execute
S_AGE=`stat -c %Y $S`
T_AGE=`stat -c %Y $T`
#echo $S_AGE
#echo $T_AGE

if [ $S_AGE -le $T_AGE ] 
then 
    java -cp .:$CLASSPATH $E $*
fi

