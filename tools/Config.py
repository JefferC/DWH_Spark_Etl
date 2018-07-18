# encoding: utf8

# Author: Cai, Jiefei
# Date  : 2018/06/25 15:25:01

# ---------- 1.Directions ---------- #
HOME_DIR = r'D:\Profile\PyScript\pj2'
KETTLE_LOG_DIR = r'D:\Profile\PyScript\pj2\data\kettle_log'

# 本地模式不起作用(如果metadata已经存在就没啥用)，用spark-submit则会用hive-site.xml。综上所述，这个配置没有作用
SPARK_WAREHOUSE_DIR = r"D:\Profile\PyScript\pj2\data\warehouse"


# ---------- 2.Names ---------- #
SPARK_APPNAME = 'Kettle_Log'
HADOOP_MASTER = 'local[*]'


# ---------- 3.Config ---------- #
SPARKCONFIG = {
    "spark.sql.warehouse.dir": SPARK_WAREHOUSE_DIR,
    "spark.task.maxFailures" : 4
}

# ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
WARNING_LEVEL = 'OFF'



# Spark Stream 批次时间
BATCHDUR = 1

# Kafka IP
KAFKA_SERVER = "localhost"

# ---------- 4.Log ---------- #
# 日志保留时间
LOG_KEEP_DAYS = 3

JOBLOG_DIR = r"D:\Profile\PyScript\pj2\Log"

LOGGING_DIR = r"D:\Profile\PyScript\pj2\LOG"