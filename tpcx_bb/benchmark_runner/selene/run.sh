#!/bin/bash

ACCOUNT=rapids
PARTITION=luna
WORKERS=1

IMAGE="/path/to/container/"
DATA_PATH="/lustre/fsw/rapids"
MOUNT_PATH="/tpcx-bb-data/"
TPCX_BB_HOME="$HOME/tpcx-bb"

rm *.out
rm -rf $HOME/dask-local-directory/*

srun \
    --account $ACCOUNT \
    --partition $PARTITION \
    --nodes 1 \
    --container-mounts $DATA_PATH:$MOUNT_PATH,$HOME:$HOME \
    --container-image=$IMAGE \
    bash -c "$TPCX_BB_HOME/tpcx_bb/benchmark_runner/selene/scheduler-client.sh"


if [ "$WORKERS" -gt "0" ]; then
    srun \
        --account $ACCOUNT \
        --partition $PARTITION \
        --nodes $WORKERS \
        --container-mounts $DATA_PATH:$MOUNT_PATH,$HOME:$HOME \
        --container-image $IMAGE \
        bash -c "$TPCX_BB_HOME/tpcx_bb/cluster_configuration/cluster-startup-selene.sh"
fi
