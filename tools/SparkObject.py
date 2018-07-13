# encoding: utf8

# Author: Cai, Jiefei
# Date  : 2018/07/02 14:48:51


from pyspark.sql import SparkSession
from pyspark import SparkConf,HiveContext
import Config,SqlUtil,LogUtil,SparkUDF


class SparkObject:

    def __init__(self):
        self.logger = LogUtil.LogUtil()
        self.setSparkSession()

    def setSparkSession(self,ss=None,ifinit=True):
        # 如果不需要初始化，则无需创建SparkSession实例
        if not ifinit:
            if isinstance(ss,SparkSession):
                self.SpkSess = ss
                return True
            else:
                self.logger.wtLog("Error","ss is not a SparkSession instance. exit 12")
                exit(12)
        # 创建SparkConf实例
        conf = SparkConf().setAppName(Config.SPARK_APPNAME).setMaster(Config.HADOOP_MASTER)
        # 添加所有配置信息，可在Config.py中配置
        for i in Config.SPARKCONFIG:
            conf.set(i,Config.SPARKCONFIG[i])
            #self.logger.wtLog("INFO","Add Config: %s To: %s" %(i,Config.SPARKCONFIG[i]))
        # 创建SparkSession实例
        self.SpkSess = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
        self.SpkCont = self.SpkSess.sparkContext
        self.SpkCont.setLogLevel(Config.WARNING_LEVEL)
        # 获取HiveContext
        self.HiveCont = HiveContext(self.SpkCont)
        # 元数据
        self.catalog = self.SpkSess.catalog
        # 版本判断
        self.SpkVersion = self.SpkSess.version
        if not self.SpkVersion.startswith("2."):
            self.logger.wtLog("ERROR","Only Spark2.0 Supported! Current Version: %s " %self.SpkVersion)
            exit(12)

    def getSparkSession(self):
        return self.SpkSess

    def execSql(self,sql,ifhive = False,var=vars()):
        # 判断SparkSession是否存在，不存在则创建
        if self.SpkSess is None:
            self.setSparkSession()
        # 状态代码： 0 成功 否则失败
        StatsCode = 0
        try:
            # 调用Sql工具箱处理分割SQL
            for i in SqlUtil.ProcessSql(sql,var):
                # 是否对Hive表进行操作。使用HiveContext.sql进行执行
                if ifhive:
                    r = self.HiveCont.sql(i)
                else:
                    r = self.SpkSess.sql(i)
                # 查询操作需要打印到控制台
                if i.upper().strip().startswith("SELECT") or i.upper().strip().startswith("SHOW"):
                    r.show(truncate=False)
        except Exception as e:
            StatsCode = 1
            self.logger.wtLog("Error", "Exec SQL Failed")
            self.logger.wtLog("Error", str(e))
        return StatsCode

    def execFile(self):
        # ToDo
        return True


    # 注册Spark UDF
    def register(self,func='ALL'):
        if func == "ALL":
            for v in vars(SparkUDF):
                f = vars(SparkUDF)[v]
                if callable(f):
                    self.SpkSess.udf.register(v,f)
        else:
            self.SpkSess.udf.register(func,vars(SparkUDF)[func])


    def destroy(self):
        self.SpkSess.stop()
        self.logger.wtLog("Error","Job Destroy")
        exit(12)