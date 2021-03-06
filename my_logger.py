import os
import logging
import logging.handlers


def debug_logging(log_file_name):
    """Init for logging"""
    path = os.path.split(log_file_name)
    if not os.path.isdir(path[0]):
        os.makedirs(path[0])
    
    logger = logging.getLogger('root')

    rthandler = logging.handlers.RotatingFileHandler(
        log_file_name, maxBytes=20 * 1024 * 1024, backupCount=20, encoding='utf-8')
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '%(asctime)s %(filename)s[line:%(lineno)d] \
        [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    rthandler.setFormatter(formatter)
    logger.addHandler(rthandler)


def online_logging(log_file_name):
    """Init for logging"""
    path = os.path.split(log_file_name)
    if not os.path.isdir(path[0]):
        os.makedirs(path[0])
    
    logger = logging.getLogger('root')

    rthandler = logging.handlers.RotatingFileHandler(
        log_file_name, maxBytes=20 * 1024 * 1024, backupCount=20, encoding='utf-8')
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    rthandler.setFormatter(formatter)
    logger.addHandler(rthandler)


def access_logging(log_file_name):
    """Init for logging"""
    path = os.path.split(log_file_name)
    if not os.path.isdir(path[0]):
        os.makedirs(path[0])
    
    access_logger = logging.getLogger('access')

    rthandler = logging.handlers.RotatingFileHandler(
        log_file_name, maxBytes=100 * 1024 * 1024, backupCount=10, encoding='utf-8')
    access_logger.setLevel(logging.INFO)
    access_logger.addHandler(rthandler)
