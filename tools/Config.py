# encoding: utf8

# Author: Cai, Jiefei
# Date  : 2018/06/25 15:25:01

# ---------- 1.Directions ---------- #
HOME_DIR = r'D:\Profile\PyScript\pj2'
KETTLE_LOG_DIR = r'D:\Profile\PyScript\pj2\data\kettle_log'

SPARK_WAREHOUSE_DIR = r"D:\Profile\PyScript\pj2\data\warehouse"
#SPARK_WAREHOUSE_DIR = r"D:\Profile\PyScript\pj2\tools\spark-warehouse"

JOBLOG_DIR = r"D:\Profile\PyScript\pj2\Log"

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