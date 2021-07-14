#!/bin/bash

SCHEDULER_PORT=8786
INTERFACE=ib0
GPU_BDB_HOME=${PWD}

LOCAL_DIRECTORY=$GPU_BDB_HOME/dask-local-directory
export SCHEDULER_FILE=$LOCAL_DIRECTORY/scheduler.json
LOGDIR=$LOCAL_DIRECTORY/logs
WORKER_DIR=$GPU_BDB_HOME/gpu-bdb-dask-workers/

# Cluster memory configuration
#MAX_SYSTEM_MEMORY=$(free -m | awk '/^Mem:/{print $2}')M
MAX_SYSTEM_MEMORY=64663M
DEVICE_MEMORY_LIMIT=16GB
POOL_SIZE=16GB

# Dask-cuda optional configuration
export DASK_JIT_UNSPILL=True
export DASK_EXPLICIT_COMMS=False

# Dask/distributed configuration
export DASK_DISTRIBUTED__COMM__TIMEOUTS__CONNECT=${DASK_DISTRIBUTED__COMM__TIMEOUTS__CONNECT:-100s}
export DASK_DISTRIBUTED__COMM__TIMEOUTS__TCP=${DASK_DISTRIBUTED__COMM__TIMEOUTS__TCP:-600s}
export DASK_DISTRIBUTED__COMM__RETRY__DELAY__MIN=${DASK_DISTRIBUTED__COMM__RETRY__DELAY__MIN:-1s}
export DASK_DISTRIBUTED__COMM__RETRY__DELAY__MAX=${DASK_DISTRIBUTED__COMM__RETRY__DELAY__MAX:-60s}

rm -rf $LOGDIR/*
mkdir -p $LOGDIR
rm -rf $WORKER_DIR/*
mkdir -p $WORKER_DIR

# Purge Dask config directories
rm -rf ~/.config/dask

echo "Starting UCX scheduler ..."
CUDA_VISIBLE_DEVICES='0' \
DASK_UCX__CUDA_COPY=True \
DASK_UCX__TCP=True \
DASK_UCX__NVLINK=True \
DASK_UCX__INFINIBAND=True \
DASK_UCX__RDMACM=False \
nohup dask-scheduler \
                    --port $SCHEDULER_PORT \
                    --interface $INTERFACE \
                    --protocol ucx \
                    --scheduler-file $SCHEDULER_FILE \
                    > $LOGDIR/scheduler.log 2>&1 &


# Setup workers
echo "Starting workers with NVLINK ..."
echo "device memory limit: " $DEVICE_MEMORY_LIMIT
echo "RMM POOL_SIZE: " $POOL_SIZE
echo "memory limit: " $MAX_SYSTEM_MEMORY
CUDA_VISIBLE_DEVICES=1,2,3,4 \
dask-cuda-worker --device-memory-limit $DEVICE_MEMORY_LIMIT \
                 --local-directory $WORKER_DIR \
                 --rmm-pool-size $POOL_SIZE \
                 --memory-limit $MAX_SYSTEM_MEMORY \
                 --enable-nvlink  \
                 --enable-infiniband \
                 --enable-tcp-over-ucx \
                 --scheduler-file $SCHEDULER_FILE \
                 >> $LOGDIR/worker.log 2>&1 &

