# encoding: utf8

# Author: Cai, Jiefei
# Date  : 2018/07/02 16:09:28

import re




def ProcessSql(sql,var):
    logger = var['self'].logger
    # 以下的代码会出事情。注释并引以为戒
    # sql = sql.strip().upper()
    sql = ReplaceVars(sql,ProcessVars(var))
    if not sql:
        logger.wtLog("Error","Not A Sql")
        exit(0)
    sql = SplitSql(sql)
    return sql


def ReplaceVars(sql,var):
    # 处理SQL，将SQL中的$var$变量替换成值
    if not var:
        print "Error Script: SqlUtil.ReplaceVars"
        exit(12)
    # 把sql中的变量都替换成值
    for i in var:
        sql = sql.replace("$" + i + "$", var[i])
    return sql


def ProcessVars(var):
    # 有毒就返回False
    if not isinstance(var,dict):
        return False
    r = {}
    # 只处理value是string的变量
    for i in var:
        if isinstance(var[i],str):
            r[i] = var[i]
    return r


def SplitSql(sql):
    # 匹配引号内内容
    reg = re.compile(r"\'(.*?)\'")
    # 引号里内容中的分号替换成不可见字符\1
    sql = reg.sub(RegSubMethod,sql)
    # 处理后的Sql拆分
    sql = sql.split(";")
    Result = []
    # 循环sql列表将引号替换回去
    for i in sql:
        # 干掉注释内的内容。
        res = RemoveComment(i.replace('\1',';').strip())
        # 跳过空Sql
        if res.strip() == "":
            continue
        Result.append(res)
    return Result


def RegSubMethod(match):
    # 替换为不可见字符。
    return match.group(0).replace(";","\1")


def RemoveComment(sql):
    reg1 = re.compile(r"\/\*[\s\S]*?\*\/")
    reg2 = re.compile(r"--(.*)?")
    sql = reg1.sub("",sql)
    sql = reg2.sub("",sql)
    return sql


def SplitSql_Old(sql):
    # 这个方法比较糟糕，废弃了
    # 基本思路是先把sql中引号内的部分拿出来。然后对Sql进行Split（担心引号里会有分号影响split）
    # 随后再按顺序将引号里的内容再塞回去
    # 当时就觉得特别糟糕。翌日想了个简单又好用的方式
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