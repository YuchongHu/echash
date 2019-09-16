#!/bin/bash
# Please change this according to your ycsb installation

YCSB_HOME=~/YCSB-master

${YCSB_HOME}/bin/ycsb load basic -P ${YCSB_HOME}/workloads/workload_test  > ./ycsb_load.load

${YCSB_HOME}/bin/ycsb run basic -P ${YCSB_HOME}/workloads/workload_test   > ./ycsb_run.run

echo over
