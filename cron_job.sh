#!/bin/bash
# Generate random message
python generate_random_message.py

#put to hdfs
hdfs dfs -put -f message_test.txt /user/root/input/

nice_date='+%Y%m%d_%H%M%S'
#run yarn job
echo "$(yarn jar /root/lab_as/map_reduce_example/target/lab1-1.0-SNAPSHOT-jar-with-dependencies.jar /user/root/input/message_test.txt /user/root/output_$(date "$nice_date"))"

