#!/bin/bash
num_mappers=$1
num_reducers=$2
start_time=$(date "+%s") &&
size1=$( hdfs dfs -stat "%b" shakespeare_basket/shakespeare_basket1) &&
size2=$( hdfs dfs -stat "%b" shakespeare_basket/shakespeare_basket2) &&
total_size=$((size1+size2)) &&
hadoop jar hw2-2c-1.0-SNAPSHOT.jar cuhk.iems5709.SONTripletsDriver1 $num_mappers $num_reducers $total_size &&
hdfs dfs -cat output/hw2/2c-1/part-* | hdfs dfs -copyFromLocal - output/hw2/2c-1/merge &&
hadoop jar hw2-2c-1.0-SNAPSHOT.jar cuhk.iems5709.SONTripletsDriver2 $num_mappers $total_size &&
end_time=$(date "+%s") &&
elapsed_time=$((end_time-start_time)) &&
echo "$start_time ---> $end_time" "elapsed: $elapsed_time seconds"