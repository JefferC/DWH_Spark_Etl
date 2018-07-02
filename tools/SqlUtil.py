# encoding: utf8

# Author: Cai, Jiefei
# Date  : 2018/07/02 16:09:28

import re

class SqlUtil:

    @staticmethod
    def ProcessSql(sql,var):
        sql = sql.strip().upper()
        sql = SqlUtil.ReplaceVars(sql,SqlUtil.ProcessVars(var))
        sql = SqlUtil.SplitSql(sql)
        return sql

    @classmethod
    def ReplaceVars(cls,sql,var):
        for i in var:
            sql = sql.replace("$" + i + "$", var[i])
        return sql

    @classmethod
    def ProcessVars(cls,var):
        if not isinstance(var,dict):
            return False
        r = {}
        for i in var:
            if isinstance(var[i],str):
                r[i] = var[i]
        return r

    @classmethod
    def SplitSql(cls,sql):
        reg = re.compile(r"\'(.*?)\'")
        cont = reg.findall(sql)
        Done = False
        st = 0
        mach = 0
        while not Done:
            rg = reg.search(sql, st)
            if not rg:
                Done = True
            else:
                st = rg.end()
                ss = rg.start()
                st_str = sql[0:ss]
                ed_str = sql[st:]
                k = "$-%s-$" % str(mach)
                sql = k.join([st_str, ed_str])
                mach = mach + 1
        sql = sql.split(";")
        sql2 = []
        for i in sql:
            if i.strip() == "":
                continue
            for j in xrange(len(cont)):
                k = "$-%s-$" % str(j)
                i = i.replace(k, "'" + cont[j] + "'")
            sql2.append(i)
        return sql2