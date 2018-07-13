# encoding: utf8

# Author: Cai, Jiefei
# Date  : 2018/06/25 15:23:01

import SparkObject

ss = SparkObject.SparkObject()


sql = '''
    show databases
'''

err = ss.execSql(sql,var=vars())

if err != 0:
    exit(12)

while True:
    a = raw_input("sql:")
    if a == 'quit':
        exit()
    else:
        ss.execSql(a,var=vars())
