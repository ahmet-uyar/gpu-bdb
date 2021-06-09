#!/bin/bash

if [ "$#" -gt 0 ]; then
   rfile=$1
else
   rfile="results.txt"
fi

echo "result file: $rfile"

# start the cluster
DASK_JIT_UNSPILL=True CLUSTER_MODE=NVLINK bash cluster_configuration/cluster-startup.sh SCHEDULER

# sllep 3 seconds for the cluster to startup
echo "waiting the cluster to startup ......"
sleep 20

# run the query
python queries/q06/gpu_bdb_query_06.py

# compute the result and append it to results.xtx
python compute_time.py $rfile

# tear down the cluster
pkill -f dask

# wait for the shutdown
sleep 5
