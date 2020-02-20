#!/usr/bin/env python

# -*- coding: utf-8 -*- 

"""
@File: Mydb.py

Created on 02 20 15:32 2020

@Authr: zhf12341 from Mr.Zhao

"""

import pymysql
import pandas as pd

class Mydb():

    """创建数据库和表"""
    """传入参数"""
    def __init__(self,host,user,password,db):

        self.host = host
        self.user = user
        self.password = password
        self.db = db
        self.database = pymysql.connect(host = self.host,
                             user = self.user,
                             password = self.password,
                             database = self.db,
                             )


    """创建表"""
    def get_table(self,table,df):

        cursor = self.database.cursor()
        # 使用 execute() 方法执行 SQL，如果表存在则删除
        cursor.execute("""DROP TABLE IF EXISTS {}""".format(table))
        cursor.execute("""CREATE TABLE {}({})""".format(table, self.make_table_sql(df)[0]))
        print("表单 {} 创建完毕！".format(table))


    """删除表"""
    def del_table(self,table):

        cursor = self.database.cursor()
        # 使用 execute() 方法执行 SQL，如果表存在则删除
        cursor.execute("DROP TABLE IF EXISTS {}".format(table))
        print("表单 {} 删除完毕！".format(table))


    """获取dataframe表头及相应sql语句"""

    def make_table_sql(self,df):

        columns = df.columns.tolist()
        types = df.dtypes.to_dict()
        make_table,columns_name = [],[]
        for item in columns:
            itype = str(types[item])
            item = '`' + item + '`'
            columns_name.append(item)
            if 'int' in itype:
                char = item + ' INT'
            elif 'float' in itype:
                char = item + ' FLOAT'
            elif 'object' in itype:
                char = item + ' VARCHAR(255)'
            elif 'datetime' in itype:
                char = item + ' DATETIME'
            make_table.append(char)
        return ', '.join(make_table), ', '.join(columns_name)


    """将dataframe导入数据库"""

    def csv2mysql(self,table,df):

        cursor = self.database.cursor()
        # 处理数据防止报错1064
        [df[i].astype(float) if df[i].dtype != object else df[i].astype(str) for i in df]
        values = df.values.tolist()
        # 根据columns个数
        s = ','.join(["%s" for _ in range(len(df.columns))])
        # executemany批量操作 插入数据 批量操作比逐个操作速度快很多
        sql = """INSERT INTO {}({})
                VALUES ({});""".format(table, self.make_table_sql(df)[1], s)
        cursor.executemany(sql, values)
        # 提交并关闭
        self.database.commit()
        print("表单 {} 数据传输完毕！".format(table))


if __name__ == '__main__':

    db = Mydb('localhost','root','Zhao@123','test')
    print(db)
    df = pd.read_csv('sincell.csv')
    db.get_table('sincell',df)
    db.csv2mysql('sincell',df)