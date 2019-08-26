import pandas as pd
import pandas.io.sql as sql
import psycopg2 as pg
from sqlalchemy import create_engine
import os
import json
user_home = os.path.expanduser('~')
config_path = os.path.join(user_home, '.pg_config')
CONFIG_FILE = os.path.join(config_path, 'pg_connections.json')


def set_config_file(path):
    if not os.path.exists(path):
        raise RuntimeError("Config file not exist")

    global CONFIG_FILE
    CONFIG_FILE = path


def read_config():
    global CONFIG_FILE
    with open(CONFIG_FILE) as f:
        return json.loads(f.read())


class PostgreSql:

    def __init__(self, pg_name=None):
        self._pg_con = None
        self._pg_raw_con = None
        self._pg_cur = None
        if pg_name is not None:
            cfg = read_config()
            db_cfg = cfg[pg_name]
            self._host = db_cfg['host']
            self._port = db_cfg['port']
            self._user_name = db_cfg['user']
            self._password = db_cfg['password']
            self._db_name = db_cfg['database']
            self._pg_con = create_engine('postgresql+psycopg2://%s:%s@%s:%s/%s' % (
                self._user_name, self._password, self._host, self._port, self._db_name), pool_recycle=3600)
            self._pg_raw_con = pg.connect(host=self._host, port=self._port, dbname=self._db_name, user=self._user_name,
                                          password=self._password)
            self._pg_cur = self._pg_raw_con.cursor()
        else:
            self._host = None
            self._port = None
            self._user_name = None
            self._password = None
            self._db_name = None

    def connect(self, host, port, user_name, password, db_name):
        self._host = host
        self._port = port
        self._user_name = user_name
        self._password = password
        self._db_name = db_name

        self._pg_con = create_engine('postgresql+psycopg2://%s:%s@%s:%s/%s' % (
            self._user_name, self._password, self._host, self._port, self._db_name), pool_recycle=3600)
        self._pg_raw_con = pg.connect(host=self._host, port=self._port, dbname=self._db_name, user=self._user_name,
                                      password=self._password)
        self._pg_cur = self._pg_raw_con.cursor()

    def reconnect(self):
        self._pg_con = create_engine('postgresql+psycopg2://%s:%s@%s:%s/%s' % (
            self._user_name, self._password, self._host, self._port, self._db_name), pool_recycle=3600)
        self._pg_raw_con = pg.connect(host=self._host, port=self._port, dbname=self._db_name, user=self._user_name,
                                      password=self._password)
        self._pg_cur = self._pg_raw_con.cursor()

    def close(self):
        if self._pg_raw_con is not None:
            self._pg_raw_con.close()
            self._pg_raw_con = None
        if self._pg_cur is not None:
            self._pg_cur.close()
            self._pg_cur = None
        if not self._pg_con:
            self._pg_con.dispose()

    def do_execute(self, sql):
        try:
            self._pg_cur.execute(sql)
            self._pg_raw_con.commit()
        except Exception as e:
            if "duplicate key" not in e.__str__(): print(sql)
            print(e.__str__())
            self.close()
            self.reconnect()
            return -1
        return 0

    def do_execute_with_return(self, sql):
        result = None
        try:
            self._pg_cur.execute(sql)
            self._pg_raw_con.commit()
            column_name = [col.name for col in self._pg_cur.description]
            result = pd.DataFrame(data=self._pg_cur.fetchall(), columns=column_name)
        except Exception as e:
            print(e.__str__())
            result = None
            self.close()
            self.reconnect()
        return result

    def do_save_df(self, data_df, tb_name):
        try:
            data_df.to_sql(name=tb_name, con=self._pg_con, if_exists='append', index=False, chunksize=50000)
            ret = 0
        except sql.DatabaseError as e:
            if "duplicate key" in e.__str__():
                ret = -3
            else:
                print(e.__str__())
                ret = -1
        except Exception as e2:
            if "duplicate key" in e2.__str__():
                ret = -3
            else:
                print(e2.__str__())
                ret = -2
        return ret

    def copy_to(self, file, table_name, col=None):
        try:
            self._pg_cur.copy_to(file, table_name, columns=col)
            self._pg_raw_con.commit()
        except Exception as e:
            print(e.__str__())
            self.close()
            self.reconnect()
            return -1
        return 0

    def copy_from(self, file, table_name, col=None):
        try:
            self._pg_cur.copy_from(file, table_name, columns=col)
            self._pg_raw_con.commit()
        except Exception as e:
            print(e.__str__())
            if "duplicate key" not in e.__str__():
                print(sql)
            self.close()
            self.reconnect()
            return -1
        return 0

    def raw_con(self):
        return self._pg_raw_con

    def get_engine(self):
        return self._pg_con

    def get_all_tb_names(self):
        return self._pg_con.table_names()
