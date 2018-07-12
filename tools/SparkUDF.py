# encoding: utf8

# Author: Cai, Jiefei
# Date  : 2018/07/12 13:45:51


import SparkObject


def udf1(v1,v2):
    return v1 + v2


so = SparkObject.SparkObject()


so.SpkSess.udf.register("addv1v2",udf1)


