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

import sys
import dask
import dask.dataframe as dd
from dask.distributed import Client
from dask.distributed import wait
import dask_cudf
import pandas as pd
import numpy as np
import cudf
import cupy


def create_pandas_df(nrows, ncols, start=0):
    df = pd.DataFrame()
    for i in range(ncols):
        df["col-" + str(i)] = np.arange(start, nrows+start, dtype="int64")
    print("pandas df constructed.")
    print("rows in pandas df:", len(df.index))
    return df


def create_cudf_df(nrows, ncols, start=0):
    df = cudf.DataFrame()
    for i in range(ncols):
        df["col-" + str(i)] = cupy.arange(start, nrows + start, dtype="int64")
    print("cudf df constructed.")
    print("rows in cudf df:", len(df.index))
    return df


# this first creates the df on the client gpu,
# then df is transferred from client gpu to worker gpus by partitioning
# persist/wait mechanism makes sure that the df on the client gpu is transferred to worker gpus
# this method does not scale over 2GB
def create_dask_cudf_df1(nrows, ncols, npartitions):
    df = create_cudf_df(nrows=nrows, ncols=ncols)
    ddf = dask_cudf.from_cudf(df, npartitions=npartitions)
    ddf = ddf.persist()
    wait(ddf)
    print("dask_cudf ddf constructed.")
    print("rows in ddf:", len(ddf.index))
    return ddf


# this first creates a single large pandas dataframe on the client cpu,
# then dask.df is constructed from it with partitions
# then the dask.df is transferred from client cpu to worker gpus as a dask_cudf.df
# this method does not scale and it does not work for 4GB and higher
def create_dask_cudf_df2(nrows, ncols, npartitions):
    df = create_pandas_df(nrows=nrows, ncols=ncols)
    ddf = dd.from_pandas(df, npartitions=npartitions)
    ddf = dask_cudf.from_dask_dataframe(ddf)
    ddf = ddf.persist()
    wait(ddf)
    print("dask_cudf ddf constructed.")
    print("rows in ddf:", len(ddf.index))
    print("npartitions in ddf:", ddf.npartitions)
    return ddf


# this method first creates a partition as a cudf df on the client gpu,
# then df is transferred from client gpu to a worker gpu as a single df
# when all partitions are transferred to worker gpus, all concatted to make a single dask_cudf df
# this method works when each partition size is 1GB or smaller, but it is very slow.
# since all partitions are first created on client gpu and transferred to worker gpus sequentially
def create_dask_cudf_df3(nrows, ncols, npartitions):
    nrows = int(nrows/npartitions)
    df_list = []
    for i in range(npartitions):
        df = create_cudf_df(nrows=nrows, ncols=ncols, start=nrows * i)
        ddf = dask_cudf.from_cudf(df, npartitions=1)
        ddf = ddf.persist()
        wait(ddf)
        df_list.append(ddf)
        print(i, "dask_cudf ddf constructed.")
        print(i, "rows in ddf:", len(ddf.index))

    ddf = dask_cudf.concat(df_list)
    ddf = ddf.persist()
    wait(ddf)
    print("concated dask_cudf ddf.")
    print("rows in ddf:", len(ddf.index))
    print("npartitions in ddf:", ddf.npartitions)
    return ddf


# this method first creates a partition as a pandas df on the client cpu,
# this pandas df is converted to a dask.df
# then the dask.df is transferred from client cpu to a worker gpu as a single dask_cudf.df
# when all partitions are transferred to worker gpus, all dask_cudf.df's are concatted to make a single dask_cudf.df
# this method works for 16GB, or 32GB when each partition size is 1GB or smaller
# but it is very slow since all data is created on the client cpu and transferred to gpus sequentially
def create_dask_cudf_df4(nrows, ncols, npartitions):
    nrows = int(nrows/npartitions)
    df_list = []
    for i in range(npartitions):
        df = create_pandas_df(nrows=nrows, ncols=ncols, start=nrows * i)
        ddf = dd.from_pandas(df, npartitions=1)
        ddf = dask_cudf.from_dask_dataframe(ddf)
        ddf = ddf.persist()
        wait(ddf)
        df_list.append(ddf)
        print(i, "dask_cudf ddf constructed.")
        print(i, "rows in ddf:", len(ddf.index))

    ddf = dask_cudf.concat(df_list)
    ddf = ddf.persist()
    wait(ddf)
    print("concated dask_cudf ddf.")
    print("rows in ddf:", len(ddf.index))
    print("npartitions in ddf:", ddf.npartitions)
    return ddf

df_list = []

# this should be created with dask.delayed
@dask.delayed
def create_single_dask_cudf_df(nrows, ncols, start=0):
    df = create_cudf_df(nrows=nrows, ncols=ncols, start=start)
    ddf = dask_cudf.from_cudf(df, npartitions=1)
#    df_list.append(ddf)
    return len(ddf.index)


# create each dask_cudf.df in parallel in worker gpus with dask.delayed
# concat them all, when all are created
def create_dask_cudf_df5(nrows, ncols, npartitions):
    nrows = int(nrows/npartitions)
    len_list = []
    for i in range(npartitions):
        length = dask.delayed(create_single_dask_cudf_df)(nrows=nrows, ncols=ncols, start=nrows * i)
        len_list.append(length)
        print(i, "dask_cudf ddf constructed.")

    len_list = list(dask.compute(*len_list))
    print("length of first partition: ", len_list[0])
#    print("dask.compute completed:")
#    df_list = list(dask.compute(*df_list))
#    print("dask compute done.")
#    print("type of dask compute result:", type(df_list))

#    ddf = dd.from_delayed(df_list)
    ddf = dask_cudf.concat(df_list)
    ddf = ddf.persist()
    wait(ddf)
    print("concated dask_cudf ddf.")
    print("rows in ddf:", len(ddf.index))
    print("npartitions in ddf:", ddf.npartitions)
    print("sum of first column in ddf:", ddf["col-0"].sum().compute())
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
#    nrows = 125000000 * 4
    nrows = 125000000 # 125M*8=1GB, each row 1GB
#    nrows = 125000 * 4
    npartitions = 4 * 4
    print("nrows, ncols, npartitions:", nrows, ncols, npartitions)
    ddf = create_dask_cudf_df5(nrows=nrows, ncols=ncols, npartitions=npartitions)
