# encoding: utf8

# Author: Cai, Jiefei
# Date  : 2018/07/11 10:47:29

import logging,sys,os,datetime
import Config

class LogUtil:

    def __init__(self):
        self.log = logging.getLogger("Kettle_Log")
        # 日志格式
        formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
        # 日志文件
        file_handler = logging.FileHandler(Config.LOGGING_DIR + os.sep + datetime.datetime.strftime(datetime.datetime.now(),"%Y-%m-%d") + ".log")
        file_handler.setFormatter(formatter)
        # 控制台 --> 标准输出
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.formatter = formatter
        # 添加处理
        self.log.addHandler(file_handler)
        self.log.addHandler(console_handler)
        self.log.setLevel(logging.INFO)
        # 清理日志 ToDo：这里不是特别好。每次实例化日志工具箱就会清理日志。考虑定时处理（例如利用定时器）
        self.ClearLog()

    def wtLog(self,lv,msg):
        if self.log == None:
            self.__init__()
        lv = lv.lower()
        if lv == "debug":
            self.log.debug(msg)
        elif lv == "info":
            self.log.info(msg)
        elif lv == "warn":
            self.log.warn(msg)
        else:
            self.log.error(msg)
        return True

    def ClearLog(self):
        self.wtLog("INFO","Start Clear Logs")
        # 保留一定时间的日志
        moth = lambda x:datetime.datetime.strftime(datetime.datetime.today() - datetime.timedelta(days=x),'%Y-%m-%d')
        days = xrange(Config.LOG_KEEP_DAYS)
        keeps = map(moth,days)
        # 获取日志目录下所有文件或路径
        d = os.listdir(Config.LOGGING_DIR)
        for i in d:
            # 只关注log文件 ToDo: 这里也可以优化为准确定位到YYYY-MM-DD.log文件 后期考虑
            if os.path.isfile(Config.LOGGING_DIR + os.sep + i) and i[-3:] == "log":
                # 非保留项目则删除
                if i.split(".")[0] not in keeps:
                    os.remove(Config.LOGGING_DIR + os.sep + i)
                    self.wtLog("INFO","Remove Log: %s" %(Config.LOGGING_DIR + os.sep + i))