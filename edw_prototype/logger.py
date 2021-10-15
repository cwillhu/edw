import logging, os
from logging.handlers import RotatingFileHandler
import os.path as p

# Set up root logger

logger = logging.getLogger()  #name of this logger is root
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

sh = logging.StreamHandler()
sh.setLevel(logging.DEBUG)
sh.setFormatter(formatter)
logger.addHandler(sh)

def getRotatingFileHandler(logFile):
    fh = RotatingFileHandler(logFile, mode='w', backupCount=10, delay=True)
    fh.setFormatter(formatter)
    fh.setLevel(logging.DEBUG)
    if logFile.exists():
        fh.doRollover()    
    return fh
