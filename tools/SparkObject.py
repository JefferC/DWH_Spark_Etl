# encoding: utf8

# Author: Cai, Jiefei
# Date  : 2018/07/02 14:48:51


from pyspark.sql import SparkSession
from pyspark import SparkConf
import Config,SqlUtil


class SparkObject:

    def __init__(self):
        self.setSparkSession()

    def setSparkSession(self,ss=None,ifinit=True):
        if not ifinit:
            if isinstance(ss,SparkSession):
                self.SpkSess = ss
                return True
            else:
                return False
        conf = SparkConf().setAppName(Config.SPARK_APPNAME).setMaster(Config.HADOOP_MASTER)
        for i in Config.SPARKCONFIG:
            conf.set(i,Config.SPARKCONFIG[i])
        self.SpkSess = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()

    def getSparkSession(self):
        return self.SpkSess

    def execSql(self,sql):
        if self.SpkSess == None:
            self.setSparkSession()
        StatsCode = 0
        try:
            for i in SqlUtil.SqlUtil.ProcessSql(sql,vars()):
                r = self.SpkSess.sql(i)
                if i.upper().strip().startswith("SELECT") or i.upper().strip().startswith("SHOW"):
                    r.show(truncate=False)
        except Exception as e:
            StatsCode = 1
            print e
        return StatsCode