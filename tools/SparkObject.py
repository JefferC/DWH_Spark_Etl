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
        # 如果不需要初始化，则无需创建SparkSession实例
        if not ifinit:
            if isinstance(ss,SparkSession):
                self.SpkSess = ss
                return True
            else:
                return False
        # 创建SparkConf实例
        conf = SparkConf().setAppName(Config.SPARK_APPNAME).setMaster(Config.HADOOP_MASTER)
        # 添加所有配置信息，可在Config.py中配置
        for i in Config.SPARKCONFIG:
            conf.set(i,Config.SPARKCONFIG[i])
        # 创建SparkSession实例
        self.SpkSess = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()

    def getSparkSession(self):
        return self.SpkSess

    def execSql(self,sql):
        # 判断SparkSession是否存在，不存在则创建
        if self.SpkSess == None:
            self.setSparkSession()
        # 状态代码： 0 成功 否则失败
        StatsCode = 0
        try:
            # 调用Sql工具箱处理分割SQL
            for i in SqlUtil.SqlUtil.ProcessSql(sql,vars()):
                r = self.SpkSess.sql(i)
                # 查询操作需要打印到控制台
                if i.upper().strip().startswith("SELECT") or i.upper().strip().startswith("SHOW"):
                    r.show(truncate=False)
        except Exception as e:
            StatsCode = 1
            print e
        return StatsCode