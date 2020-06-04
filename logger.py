#!/usr/bin/python2.7
# -*- coding: utf-8 -*-

import logging

class BasicLog(object):
    
    def __init__(self, logger_path, logger_name, logger_level = logging.DEBUG):
        '''
        '''
        self.logger = logging.getLogger(logger_path + '/' + logger_name)
        file_handler = logging.FileHandler(logger_path + '/' + logger_name, mode='a')
        file_handler.setFormatter(
        logging.Formatter('%(asctime)s - [line:%(lineno)d] - %(levelname)s: %(message)s'))
        self.logger.addHandler(file_handler)
        self.logger.setLevel(logger_level)

    def get_log(self):
        return self.logger