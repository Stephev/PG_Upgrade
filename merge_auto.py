#!/usr/bin/python
# -*- coding:utf-8 -*-
# @Time    : 2018/12/28 0028 
# @Author  : Stephev
# @Site    : 
# @File    : merge_auto.py
# @Software:

import psycopg2
import ConfigParser
import string


db = ConfigParser.ConfigParser()
db.read('database.conf')
host_PG8 = db.get("PG8_db","host")
user_PG8 = db.get("PG8_db","user")
pwd_PG8 = db.get("PG8_db","passwd")
db_PG8 = db.get("PG8_db","database")
port_PG8 = db.get("PG8_db","port")

host_PG10 = db.get("PG10_db","host")
user_PG10 = db.get("PG10_db","user")
pwd_PG10 = db.get("PG10_db","passwd")
db_PG10 = db.get("PG10_db","database")
port_PG10 = db.get("PG10_db","port")
foreign_name = db.get("PG10_db","foreign_servername")

table_schema = db.get("tb","table_schema")
table_name = db.get("tb","tablename")
#table_name = table_schema+"."+table_name1


class PGINFO:

    def __init__(self,host, user, pwd, db, port):
        self.host = host
        self.user = user
        self.pwd = pwd
        self.db = db
        self.port = port

    def __GetConnect(self):
        """
        得到连接信息
        返回: conn.cursor()
        """
        if not self.db:
            raise(NameError, "没有设置数据库信息")
        self.conn = psycopg2.connect(database=self.db, user=self.user, password=self.pwd, host=self.host, port=self.port)
        cur = self.conn.cursor()
        if not cur:
            raise (NameError, "连接数据库失败")
        else:
            return  cur

    def ExecQuery(self, sql):
        """
        执行查询语句
        """
        if sql == 'close':
            self.conn.close()
        else:
            cur = self.__GetConnect()
            cur.execute(sql)
            #resList = cur.fetchall()
            return cur

    def Execute(self,sql):
        """
        执行操作语句
        """
        if sql == 'close':
            self.conn.close()
        else:
            cur = self.__GetConnect()
            cur.execute(sql)
            self.conn.commit()

def found_key(table_name1):
    pg = PGINFO(host=host_PG8, user=user_PG8, pwd=pwd_PG8, db=db_PG8, port=port_PG8)
    print "正在查询给定表的主键"
    found_sql = "SELECT pg_attribute.attname AS colname " \
                "FROM pg_constraint " \
                "INNER JOIN pg_class ON pg_constraint.conrelid = pg_class.oid 	" \
                "INNER JOIN pg_attribute " \
                "ON pg_attribute.attrelid = pg_class.oid " \
                "AND pg_attribute.attnum = pg_constraint.conkey[1] " \
                "INNER JOIN pg_type ON pg_type.oid = pg_attribute.atttypid " \
                "WHERE pg_class.relname = '"+table_name1+"'AND " \
                " pg_constraint.contype = 'p';"
    cur = pg.ExecQuery(found_sql)
    a = cur.fetchone()
    #print a
    b = a[0]
    prim_key = str(b)
    print  "该表的主键是 "+prim_key
    pg.Execute('close')
    return prim_key

prim_key = found_key(table_name)
print prim_key


def Merge_auto():
    print "开始进行MERGE数据..."
    pg = PGINFO(host=host_PG10, user=user_PG10, pwd=pwd_PG10, db=db_PG10, port=port_PG10)
    get_colu = "select column_name from information_schema.columns " \
               "where table_schema='"+table_schema+"' and table_name='"+table_name+"';"
    cur = pg.ExecQuery(get_colu)
    colu  = cur.fetchall()
    select_name = prim_key
    set_name = "SET "
    for i in colu:
        a_colname = i[0]
        print a_colname
        a = str(a_colname)
        if a == prim_key:
            continue
        else:
            select_name = select_name+","+a
            set_name = set_name+a+" = z."+a+","
    set_name = set_name.strip(string.punctuation)
    #print select_name
    #print set_name
    merge_sql = "WITH upsert AS (" \
                            "UPDATE "+table_name+" m "+set_name+" " \
                            "FROM fdw_vw_"+table_name+" z " \
                            "WHERE z.x___action = 1 " \
                                "AND m."+prim_key+" = z."+prim_key+" " \
                            "RETURNING m.*" \
                            ") " \
                "INSERT INTO "+table_name+" SELECT "+select_name+" FROM fdw_vw_"+table_name+" a " \
                "WHERE a.x___action = 1 " \
                    "AND NOT EXISTS (" \
                        "SELECT 1 " \
                        "FROM upsert b " \
                        "WHERE a."+prim_key+" = b."+prim_key+" );"
    pg.Execute(merge_sql)
    delete_sql = "DELETE FROM "+table_name+" AS a WHERE EXISTS (SELECT 1 FROM fdw_vw_"+table_name+" b " \
                 "WHERE x___action = 2 AND a."+prim_key+" = b."+prim_key+" );"
    pg.Execute(delete_sql)
    pg.Execute('close')
    print "数据同步成功！！"
    return


def main():
    Merge_auto()
    return


if __name__ == '__main__':
    main()

