#!/bin/bash
start_time=$(date "+%s") &&
hadoop jar hw2-2b-1.0-SNAPSHOT.jar cuhk.iems5709.SONDriver1 &&
hadoop jar hw2-2b-1.0-SNAPSHOT.jar cuhk.iems5709.SONDriver2 &&
end_time=$(date "+%s") &&
elapsed_time=$((end_time-start_time)) &&
echo "$start_time ---> $end_time" "elapsed: $elapsed_time seconds"