#!/bin/sh
mvn package
python /hadoop-2.7.2/input.py
mv input.txt /hadoop-2.7.2/input.txt
/hadoop-2.7.2/bin/hadoop dfs -rmr input
/hadoop-2.7.2/bin/hadoop dfs -rmr output
/hadoop-2.7.2/bin/hdfs dfs -put /hadoop-2.7.2/input.txt input
/hadoop-2.7.2/bin/hadoop jar /root/workspace/FriendlyNumbersMaven/target/FriendlyNumbersMaven-1.0-SNAPSHOT.jar com/rene/app/App input output
#rm /hadoop-2.7.2/output/*
#/hadoop-2.7.2/bin/hdfs dfs -get output output
/hadoop-2.7.2/bin/hdfs dfs -cat output/part-r-00000

