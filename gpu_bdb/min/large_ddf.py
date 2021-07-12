#
# Copyright (c) 2019-2020, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import sys
import dask.dataframe as dd
from dask.distributed import Client
from dask.distributed import wait
import dask_cudf
import pandas as pd
import numpy as np
import cudf
import cupy


def create_pandas_df(nrows, ncols):
    df = pd.DataFrame()
    for i in range(ncols):
        df["col-" + str(i)] = np.arange(0, nrows, dtype="int64")
    print("pandas df constructed.")
    print("rows in pandas df:", len(df.index))
    return df


def create_dask_df(nrows, ncols, npartitions):
    df = create_pandas_df(nrows=nrows, ncols=ncols)
    ddf = dd.from_pandas(df, npartitions=npartitions)
    ddf = ddf.persist()
    wait(ddf)
    print("dask ddf constructed.")
    print("rows in ddf:", len(ddf.index))
    return ddf


def create_cudf_df(nrows, ncols):
    df = cudf.DataFrame()
    for i in range(ncols):
        df["col-" + str(i)] = cupy.arange(0, nrows, dtype="int64")
    print("cudf df constructed.")
    print("rows in cudf df:", len(df.index))
    return df


def create_dask_cudf_df(nrows, ncols, npartitions):
    df = create_cudf_df(nrows=nrows, ncols=ncols)
    ddf = dask_cudf.from_cudf(df, npartitions=npartitions)
    ddf = ddf.persist()
    wait(ddf)
    print("dask_cudf ddf constructed.")
    print("rows in ddf:", len(ddf.index))
    return ddf


if __name__ == "__main__":

    scheduler_file = "dask-local-directory/scheduler.json"
    try:
        with open(scheduler_file) as fp:
            print(fp.read())
        client = Client(scheduler_file=scheduler_file)
        print('Connected!')
    except OSError as e:
        sys.exit(f"Unable to create a Dask Client connection: {e}")

    print("client:", client)
    import cupy as cp
    import rmm
    cp.cuda.set_allocator(rmm.rmm_cupy_allocator)
    client.run(cp.cuda.set_allocator, rmm.rmm_cupy_allocator)

    ncols = 4
    nrows = 125000000
    npartitions = 4
    print("nrows, ncols, npartitions:", nrows, ncols, npartitions)
    ddf = create_dask_cudf_df(nrows=nrows, ncols=ncols, npartitions=npartitions)
    #ddf = create_dask_df(nrows=nrows, ncols=ncols, npartitions=npartitions)
