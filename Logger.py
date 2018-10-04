# -*- coding: utf-8 -*-
import logging

def Logger(mod_name):
    logger = logging.getLogger(mod_name)
    handler = logging.FileHandler('log/app.log')
    formatter = logging.Formatter('%(levelname)s: %(asctime)s %(filename)s %(name)s %(funcName)s (linha:%(lineno)d) -- %(message)s',datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    return logger
