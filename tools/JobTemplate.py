# encoding: utf8

# Author: Cai, Jiefei
# Date  : 2018/06/25 15:23:01

import SparkObject

ss = SparkObject.SparkObject()

sql = '''
    show databases;
    use tmp;
    show tables;
    drop table if exists firsttb;
    create table firsttb(id int,name varchar(20),memo varchar(200)) stored as parquet;
    insert into firsttb values(1,'Me','A ; B ; C . E');
    select * from firsttb where memo like '%;%'
'''

err = ss.execSql(sql)

if  err != 0:
    exit(12)
