#!/bin/sh

sbin/stop-all.sh

build/mvn --projects core/ -Phadoop-2.3 -DskipTests install

mvn --projects assembly/ -Phadoop-2.3 -DskipTests install

sbin/start-all.sh

bin/spark-submit --class org.apache.spark.examples.KMeansTest --master spark://hadoop2:7077 lib/SparkExample2.jar file:/home/hadoop/share/udisk/MyTest/data-Kmeans 8 20 offheap
