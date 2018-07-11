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
        file_handler = logging.FileHandler(Config.LOGGING_DIR + os.sep + datetime.datetime.strftime(datetime.datetime.now(),"%Y-%m-%S") + ".log")
        file_handler.setFormatter(formatter)
        # 控制台
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.formatter = formatter
        # 添加处理
        self.log.addHandler(file_handler)
        self.log.addHandler(console_handler)
        self.log.setLevel(logging.INFO)

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
            self.log.critical(msg)
        return True