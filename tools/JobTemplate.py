# encoding: utf8

# Author: Cai, Jiefei
# Date  : 2018/06/25 15:23:01

import SparkObject

ss = SparkObject.SparkObject()


sql = '''
    use tmp;
    select addv1v2(ver1,ver2),ver1,ver2 from udftest
'''

err = ss.execSql(sql,var=vars())

if err != 0:
    exit(12)
