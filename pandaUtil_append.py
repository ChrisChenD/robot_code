#!/usr/bin/env python3 

# from tkinter import E
import pymysql
import sqlalchemy
import pandas

class Db:
    "user:password@127.0.0.1:3306/database_name"
    def __init__(self, info_string):
        self.info_string = info_string
    @property
    def config(self):
        info = self.info_string
        config = dict()
        config['user'], info = info.split(':', 1)
        config['password'], info = info.split('@', 1)
        config['host'], info  = info.split(':')
        config['port'], config['database']  = info.split('/')
        config['port'] = int(config['port'])
        return config
    @property
    def client_cmd(self):
        config = self.config
        return [
            'mysql -h {host} -P {port} -u {user} -D {database} -p'.format(**config),
            config['password']
        ]

class ReadMysql:
    'Read_mysql'
    def __init__(self, config_string, chunksize=1000, 
            sqlprint=False, timeout=None, id_range=[0, 10**20],
            id_order=True):
        self.id_order = id_order
        self.timeout = timeout
        self.sqlprint = sqlprint
        self.chunksize = chunksize
        self.chunknum = -1
        self.config = config_string
        self.cursor, self.db = None, None
        self.id_range = id_range
        self.connect()
    def full_data(self, fulldata):
        self.id_order = not fulldata;return self
    def print_sql(self, sqlprint):
        self.sqlprint = sqlprint;return self
    def set_chunk(self, chunksize, chunknum):
        self.chunksize = chunksize
        self.chunknum = chunknum
        return self
    def __del__(self): self.disconnect()
    def connect(self):
        self.db = pymysql.connect(**Db(self.config).config, read_timeout=self.timeout)
        self.cursor = self.db.cursor()
    def disconnect(self):
        if hasattr(self, 'cursor') and self.cursor:
            self.cursor.close()
        if self.db:
            self.db.close()
    
    def __call__(self, sql):
        #sql = "INSERT INTO userinfo(username,passwd) VALUES('jack','123')"
        try:
            self.cursor.execute(sql)
            r = self.cursor.fetchall()
            return r
        except:
            print('Mysql Query#:', sql)
            import traceback;print(traceback.format_exc())

    def show(self, sql):
        r = self(sql)
        print(r)
    @property
    def table_list(self):
        'list all tables in database'
        r = self('show tables;')
        return [record[0] for record in r]
    
    def create_info(self, table_name):
        'list all fields infomation'
        # print('debug', 'table_name', table_name)
        r = self(f'show create table {table_name}')
        # print('debug', 'r', r)
        name, create_sql, *rest = r[0]
        # cols = create_sql.split('(', 1)[1].rsplit(')', 1)[0]
        # cols = cols.split(',')
        # cols = [col_info.strip() for col_info in cols]
        # return cols
        return create_sql

    def demo_1():
        # mysql = ReadMysql("test:test@127.0.0.1:3306/test")
        mysql = ReadMysql('kf2_poc:shiye1805A@127.0.0.1:30152/seeyii_assets_database')
        for table_name in mysql.table_list:
            print(table_name, mysql.create_info(table_name))
        r = mysql('select * from test1')
        print(r)
        print(list(zip(*r)))
        
    @property
    def df(self):
        # engine = sqlalchemy.create_engine(f'mysql+pymysql://{self.config}')
        if self.sqlprint:
            print(f'sql [{self.sql}]')
        # df = pandas.read_sql_query(self.sql, engine)
        df = pandas.read_sql_query(self.sql, self.db)
        return df

    @property
    def df_chunks(self):
        # print('sql', self.sql)
        # df_chunk_iter = pandas.read_sql_query(self.sql, self.db, chunksize=self.chunksize)
        # return df_chunk_iter
        # import pandas.io.sql as psql
        # chunk_size = 10000
        # offset = 0
        id_base, id_top = self.id_range
        # dfs = []
        for i in range(1000000000000000000000):
            if i == self.chunknum: 
                break

            # print('id_base', id_base)
            sql = f"{self.sql} and id between {id_base} and {id_top} order by id limit {self.chunksize} "#  % (self.chunk_size, offset) 
            if self.id_order is False:
                sql = self.sql#  % (self.chunk_size, offset) 
                
            # sql = f"{self.sql} and id >= {id_base} order by id limit {self.chunksize} "#  % (self.chunk_size, offset) 
            # sql = f"{self.sql} order by id limit {self.chunksize} "#  % (self.chunk_size, offset) 
            if 'id' not in self.fields and '*' not in self.fields:
                old_fields = self.fields
                self.fields = old_fields + ',id'
                sql = f"{self.sql} and id between {id_base} and {id_top} order by id limit {self.chunksize} "#  % (self.chunk_size, offset) 
                if self.id_order is False:
                    sql = self.sql#  % (self.chunk_size, offset) 

                # sql = f"{self.sql} and id >= {id_base} order by id limit {self.chunksize} "#  % (self.chunk_size, offset) 
                # sql = f"{self.sql} order by id limit {self.chunksize} "#  % (self.chunk_size, offset) 
                self.fields = old_fields
            # if self.sqlprint:
            if 1:
                print(f'pandas.read_sql_query [{sql}]')
            # print(f'sql [{sql}]')
            new_df = pandas.read_sql_query(sql, self.db)
            if len(new_df) > 0:
                id_base = new_df.iloc[-1]['id']
            if 'id' not in self.fields and '*' not in self.fields:
                new_df.drop('id', axis=1, inplace=True)
                # del new_df['id']
            yield new_df
            # print('chunksize-len-base', self.chunksize, len(new_df), id_base)
            if len(new_df) < self.chunksize:
                break
            if id_base >= id_top:
                break
            if self.id_order is False:
                break
        # full_df = pd.concat(dfs)

    @property
    def sql(self):
        return f'select {self.fields} from {self.table_name} where {self.cond}'
    def __getitem__(self, _slice):
        "[table_name, fields, where/cond]"
        self.table_name, self.fields, self.cond = _slice
        return self
    def demo_2():
        mysql = ReadMysql("test:test@127.0.0.1:3306/test")
        df = mysql['test1', 'id,name', 'id > 1'].df
        # show df
        df.info()
        print(df)
    @property
    def dict(self):
        '把mysql查询结果, 制作成dict: {field1:field2}'
        # r =  dict()
        # for idx, row in self.df.iterrows():
        #     r[row[0]] = row[1]
        # return r
        return dict(
            (row[0], row[1])
            for idx, row in self.df.iterrows()
        )
    @property
    def set(self):
        return set(
            row[0]
            for idx, row in self.df.iterrows()
        )
    @property
    def series(self):
        # first col
        return self.df
    
    # def table_info(self, table_name):
    #     create_sql = self.create_info(table_name)
    #     return Table_info(create_sql)


def Encode(s, pair=('(', ')'), mid_func=lambda mid:mid.replace(',', '?')):
    "把(,) 转化成 (?)"
    lc, rc = pair
    new_s = ''
    while len(s) > 0:
        if lc not in s:
            return new_s + s
        a, s = s.split(lc, 1)
        mid, b = s.split(rc, 1)
        mid = mid.replace(',', '?')
        s = b
        new_s += f"{a}{lc}{mid}{rc}"
    return new_s


# def df_append(self, chunk, key, _type=str):
def chunk_append(root_chunk, append_info, key, key_type=str):
    'key:  [on_key, [l_key, r_key]]'
    '与一个 df_chunk merge '
    assert len(root_chunk) != 0
    append_db, append_tb, append_select, append_cond = append_info
    root_key, append_key = key
    e_set = set(root_chunk.loc[:,root_key])# 去重
    e_map = lambda e:e
    if key_type==str:
        e_map = lambda e:str(e)# 增加字符串的单引号
    e_tuple = tuple(e_map(e) for e in e_set if e is not None)# 去空值
    in_str = str(e_tuple) if len(e_tuple) > 1 else str(e_tuple)[:-2]+')'
    append_cond = f"{append_cond} and {append_key} in {in_str}"
    append_chunk = ReadMysql(append_db)[
        append_tb,
        append_select, append_cond
    ].df
    if root_key != append_key:
        append_chunk.rename(columns={append_key:root_key}, inplace=True)

    merged_chunk = None
    try:
        merged_chunk = pandas.merge(
            root_chunk, append_chunk, 
            on=root_key, 
            suffixes=['',''], 
            how='inner'
        )
    except:
        import traceback;print(traceback.format_exc())
        print('key:', root_key)
        print('common key:', set(root_chunk.columns)&set(append_chunk.columns))
        exit()
    return merged_chunk
    # # table = self
    # # on_key, (table_key, chunk_key) = key
    # # c = table.copy()
    # # name_list = chunk.loc[:,'eventSubject']
    # name_list = chunk.loc[:,chunk_key]
    # name_tuple = list(filter(None,set(name_list)))
    # name_tuple_str = str(tuple(_type(name) for name in name_tuple))
    # if len(name_tuple) == 1:
    #     name_tuple_str = name_tuple_str[:-2]+name_tuple_str[-1:]
    # c.kwargs['cond'] += f' and {table_key} in {name_tuple_str}'
    # df = c._df
    # if on_key != table_key:
    #     df.rename(columns={table_key:on_key}, inplace=True)
    # if on_key != chunk_key:
    #     chunk.rename(columns={chunk_key:on_key}, inplace=True)
    
    # row_debug = [None]
    # try:
    #     # print('len:chunk', len(chunk))
    #     # print('len:df', len(df))
    #     chunk = pandas.merge(df, chunk, on=on_key, suffixes=['',''], how='inner')
    



if __name__ == '__main__':
    # ReadMysql.demo_1()
    # ReadMysql.demo_2()
    # Table_info.demo3()
    pass

