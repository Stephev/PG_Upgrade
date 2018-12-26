#!/usr/bin/python
# -*- coding:utf-8 -*-
# @Time    : 2018/12/26 0026 
# @Author  : Stephev
# @Site    : 
# @File    : create_view.py
# @Software:

import os


import psycopg2
import re
import ConfigParser


db = ConfigParser.ConfigParser()
db.read('database.conf')
host_cus = db.get("db","host")
user_cus = db.get("db","user")
pwd_cus = db.get("db","passwd")
db_cus = db.get("db","database")
port_cus = db.get("db","port")
table_name = db.get("tb","tablename")
prim_key = db.get("tb","primarykey")


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



def create():
    pg = PGINFO(host=host_cus, user=user_cus, pwd=pwd_cus, db=db_cus, port=port_cus)
    print "正在创建记录主键变化表"
    create_ta = "create table c_"+table_name+"("+prim_key+" int);"
    pg.Execute(create_ta)

    print "正在创建触发器函数"
    create_fun = "create or replace function f_tri_"+table_name+"()" \
                 " returns trigger as $$" \
                 " BEGIN" \
                    " IF TG_OP = 'INSERT' THEN" \
                        " insert into c_"+table_name+"("+prim_key+") values(new."+prim_key+"); " \
                    "END IF; " \
                    "IF TG_OP = 'UPDATE' THEN " \
                        "insert into c_"+table_name+"("+prim_key+") values(new."+prim_key+"); " \
                        "IF new."+prim_key+" <> old."+prim_key+" THEN " \
                            "insert into c_test01("+prim_key+") values(old."+prim_key+"); " \
                        "END IF; " \
                    "END IF; " \
                    "IF TG_OP = 'DELETE' THEN " \
                        "insert into c_"+table_name+"("+prim_key+") values(old."+prim_key+"); " \
                    "END IF; " \
                    "RETURN NULL; " \
                    "END $$ language plpgsql;"
    pg.Execute(create_fun)
    print "创建触发器"
    create_trigger = "create trigger tri_c_"+table_name+" after delete or update or insert on "+table_name+" for each row execute procedure f_tri_"+table_name+"();"
    pg.Execute(create_trigger)
    pg.Execute('close')
    return


def Create_vw():
    pg = PGINFO(host=host_cus, user=user_cus, pwd=pwd_cus, db=db_cus, port=port_cus)
    print "正在创建动态视图"
    get_columns = "select column_name from information_schema.columns where table_schema='public' and table_name='"+table_name+"';"
    cur = pg.ExecQuery(get_columns)
    colu  = cur.fetchall()
    col_1 = "1 AS x___action, NULL AS x_ctid"
    col_2 = "2 AS x___action, NULL AS x_ctid"
    col_3 = "3 AS x___action,ctid::varchar AS x_ctid"
    for i in colu:
        a = str(i)
        b = a.split("'")
        col_1 = col_1+","+b[1]+"::text"
        col_2 = col_2+",null as "+b[1]
        col_3 = col_3+",null as "+b[1]
    print "检测col是否正常"
    print col_1
    print col_2
    print col_3

    create_veiw = "CREATE VIEW vw_"+table_name+" " \
                    "AS " \
                    "SELECT "+col_1+" " \
                    "FROM "+table_name+" a " \
                    "WHERE EXISTS ( " \
                    "SELECT 1 FROM c_"+table_name+" b WHERE a."+prim_key+" = b."+prim_key+" ) " \
                    "UNION ALL " \
                    "SELECT DISTINCT "+col_2+" FROM c_"+table_name+" a  " \
                    "WHERE NOT EXISTS ( SELECT 1 FROM test01 b  WHERE a."+prim_key+" = b."+prim_key+" )" \
                    " UNION ALL " \
                    "SELECT "+col_3+" FROM c_"+table_name+";"

    pg.Execute(create_veiw)
    pg.Execute('close')
    print "视图创建完毕"
    return


def main():
    create()
    Create_vw()
    return


if __name__ == '__main__':
    main()
