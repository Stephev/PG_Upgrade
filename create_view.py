#!/usr/bin/python
# -*- coding:utf-8 -*-
# @Time    : 2018/12/26 0026 
# @Author  : Stephev
# @Site    : 
# @File    : create_view.py
# @Software:

import psycopg2
import ConfigParser


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
#print prim_key


def Create_TRI():
    pg = PGINFO(host=host_PG8, user=user_PG8, pwd=pwd_PG8, db=db_PG8, port=port_PG8)
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
    create_trigger = "create trigger tri_c_"+table_name+" after delete or update or insert " \
                     "on "+table_name+" for each row execute procedure f_tri_"+table_name+"();"
    pg.Execute(create_trigger)
    pg.Execute('close')
    return


def Create_vw():
    pg = PGINFO(host=host_PG8, user=user_PG8, pwd=pwd_PG8, db=db_PG8, port=port_PG8)
    print "正在创建动态视图"
    get_columns = "select column_name,data_type from information_schema.columns " \
                  "where table_schema='public' and table_name='"+table_name+"';"
    cur = pg.ExecQuery(get_columns)
    colu  = cur.fetchall()
    col_1 = "1 AS x___action, NULL AS x_ctid"
    col_2 = "2 AS x___action, NULL AS x_ctid"
    col_3 = "3 AS x___action,ctid::varchar AS x_ctid"
    for i in colu:
        a_colname = i[0]
        print a_colname
        b_coltype = i[1]
        print b_coltype
        a = str(a_colname)
        b = str(b_coltype)
        col_1 = col_1+","+a+"::"+b
        col_2 = col_2+",null ::"+b+" as "+a
        col_3 = col_3+",null ::"+b+" as "+a
    #print "检测col是否正常"
    #print col_1
    #print col_2
    #print col_3
    create_veiw = "CREATE VIEW vw_"+table_name+" " \
                    "AS " \
                    "SELECT "+col_1+" " \
                    "FROM "+table_name+" a " \
                    "WHERE EXISTS ( " \
                    "SELECT 1 FROM c_"+table_name+" b WHERE a."+prim_key+" = b."+prim_key+" ) " \
                    "UNION ALL " \
                    "SELECT DISTINCT "+col_2+" FROM c_"+table_name+" a  " \
                    "WHERE NOT EXISTS ( SELECT 1 FROM "+table_name+" b  WHERE a."+prim_key+" = b."+prim_key+" )" \
                    " UNION ALL " \
                    "SELECT "+col_3+" FROM c_"+table_name+";"
    pg.Execute(create_veiw)
    pg.Execute('close')
    print "视图创建完毕"
    return


def Create_FORE():
    print "正在为Pg10 库创建视图外部表"
    pg = PGINFO(host=host_PG8, user=user_PG8, pwd=pwd_PG8, db=db_PG8, port=port_PG8)
    get_vw = "select column_name,data_type from information_schema.columns " \
             "where table_schema='"+table_schema+"' and table_name='"+table_name+"';"
    cur = pg.ExecQuery(get_vw)
    col = cur.fetchall()
    colun_name = "x___action smallint NOT NULL,x_ctid text"
    for i in col:
        a_colname = i[0]
        #print a_colname
        b_coltype = i[1]
        #print b_coltype
        a = str(a_colname)
        b = str(b_coltype)
        colun_name = colun_name+","+a+" "+b
    pg.Execute('close')
    create_foreign = "create foreign table fdw_vw_"+table_name+"("+colun_name+") " \
                    "server "+foreign_name+" options (table_name 'vw_"+table_name+"');"
    pg10 = PGINFO(host=host_PG10, user=user_PG10, pwd=pwd_PG10, db=db_PG10, port=port_PG10)
    pg10.Execute(create_foreign)
    pg10.Execute('close')
    print "创建外部表成功"


def main():
    Create_TRI()
    Create_vw()
    Create_FORE()
    return


if __name__ == '__main__':
    main()
