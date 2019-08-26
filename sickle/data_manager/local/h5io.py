# -*- coding: utf-8 -*-
import sqlalchemy as sa
import numpy as np
import pandas as pd
import tables
import datetime as dt
from sqlalchemy.pool import NullPool
from tqdm import tqdm
import os
import pickle

import multiprocessing
# 数据库连接池，每个数据库仅允许创建一个连接
_db_engine_pools = []


def create_engine(db_url: str):
    for engine in _db_engine_pools:
        if db_url == engine.url: # 如果连接池内有连接，则直接 return
            return engine
    engine = sa.create_engine(db_url, poolclass=NullPool)
    _db_engine_pools.append(engine)
    return engine


class H5Base(object):
    def __init__(self, complib):
        # see tables.parameters.DRIVER
        self._driver = 'H5FD_CORE'
        self._filters = tables.Filters(complevel=9, complib=complib)
        self._h5_handle = None
        self._path = ''
        # HDF5 元数据缓存大小 20 MB
        self._cache_size = 20 * 1024 * 1024

    def _open(self, mode):
        self._h5_handle = tables.open_file(self._path, mode=mode,
                                           filters=self._filters,
                                           CHUNK_CACHE_SIZE=self._cache_size,
                                           DRIVER=self._driver)

    def close(self):
        if self._h5_handle is not None:
            self._h5_handle.close()


class H5Reader(H5Base):
    def __init__(self, path, complib='lzo'):
        super(H5Reader, self).__init__(complib)
        self._path = path
        self._all_col = []

    def open(self):
        self._open(mode='r')

    def get_data(self, stock_code, columns=None):
        """Remark : if handle closed, return cache invalid.
        cache_cls : class base at CacheBase """

        if columns is None:
            columns = [i.name for i in self._h5_handle.list_nodes(self._h5_handle.root)]
        cache = None
        f_root = self._h5_handle.root
        if stock_code != '*':
            ix = (f_root['windcode'][:] == stock_code.encode('utf-8')).nonzero()[0]
            if len(ix) > 0:
                cache = {k: f_root[k][ix[0]:ix[-1] + 1] for k in columns}
        else:
            cache = {k: f_root[k][:] for k in columns}
        return cache


class H5Writer(H5Base):
    def __init__(self, path, complib='lzo', ignore_col=[]):
        super(H5Writer, self).__init__(complib)
        self._path = path
        self._atom_map = {np.dtype('object'): tables.StringAtom(40),
                          np.dtype('float64'): tables.Float64Atom(),
                          np.dtype('int64'): tables.Int64Atom()}
        self._ignore_col = ignore_col
        self._db_engine = None

    def open(self, db_url=None, mode='a'):
        self._open(mode=mode)
        if db_url != None:
            self._db_engine = create_engine(db_url)

    def df_to_hdf5(self, data):
        """
        data : pandas.DataFrame, save data
        data_type : str , it maybe be one of the list:
                ['tick', 'kline', 'factor',...] or other
        """
        if len(data) <= 0:
            return

        lower_columns = map(lambda x: x.lower(), data.columns)
        data.columns = lower_columns
        all_columns = data.columns.values
        df_dtypes = data.dtypes

        for col in all_columns:
            if col in self._ignore_col:
                continue
            try:
                arr = self._h5_handle.get_node(self._h5_handle.root, col, classname='EArray')
            except (tables.NoSuchNodeError, ValueError):
                atom = self._atom_map.get(df_dtypes[col], None)
                arr = self._h5_handle.create_earray(self._h5_handle.root, col,
                                                    atom=atom, shape=(0,),
                                                    filters=self._filters)
            arr.append(data[col].values)

    def sql_to_hdf5(self, sql):
        """ save kline data to hdf5 file
        data_type : str , it can be one of the list:
                ['tick', 'kline', 'factor',...] or other
        """
        with self._db_engine.connect() as conn:
            data = pd.read_sql(sql, conn)
        self.df_to_hdf5(data)
        return len(data)

    def get_max_datetime(self):
        try:
            arr = self._h5_handle.get_node(self._h5_handle.root, 'datetime', classname='EArray')
            max_dt = dt.datetime.fromtimestamp(arr[-1])
        except (tables.NoSuchNodeError, ValueError, IndexError):
            max_dt = dt.datetime(year=2000, month=1, day=1)
        return max_dt


def sql_to_hdf5(db_url: str, sql: str, hdf5_path: str):
    h5writer = H5Writer(hdf5_path)
    try:
        h5writer.open(db_url)
        h5writer.sql_to_hdf5(sql)
    finally:
        h5writer.close()
    return hdf5_path


def dbtable_to_hdf5(db_url: str, table_name: str, hdf5_dir: str):
    hdf5_path = hdf5_dir + '/' + table_name
    sql = "SELECT * FROM \"{}\" ORDER BY \"datetime\" ASC".format(table_name)
    return sql_to_hdf5(db_url, sql, hdf5_path)


def df_to_hdf5(df, hdf5_path):
    h5writer = H5Writer(hdf5_path)
    try:
        h5writer.open()
        h5writer.df_to_hdf5(df)
    finally:
        h5writer.close()
    return hdf5_path


def hdf5_to_nparray(hdf5_path: str, symbol: str, columns=None):
    # h5reader = H5Reader(hdf5_path + '/' + symbol)
    h5reader = H5Reader(hdf5_path)
    try:
        h5reader.open()
        data = h5reader.get_data(symbol, columns)
    finally:
        h5reader.close()
    return data

