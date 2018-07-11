# encoding: utf8

# Author: Cai, Jiefei
# Date  : 2018/06/25 15:23:01

import SparkObject

ss = SparkObject.SparkObject()

sql = '''
    show databases;
    use tmp;
    show tables;
    /* select 99 from firsttb
    */
    
    select * from firsttb --where memo not like '%;%'
'''

err = ss.execSql(sql)

if  err != 0:
    exit(12)
