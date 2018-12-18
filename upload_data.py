﻿# -*- coding: utf-8 -*-
import time
import json
import socket

import arrow

import helper
from helper_consul import ConsulAPI
from helper_kafka_consumer import KafkaConsumer
from helper_kafka_producer import KafkaProducer
from my_yaml import MyYAML
from my_logger import *


debug_logging('/home/logs/error.log')
logger = logging.getLogger('root')


class UploadData(object):
    def __init__(self):
        # 配置文件
        self.my_ini = MyYAML('/home/my.yaml').get_ini()

        # request方法类
        self.kc = None
        self.kp = KafkaProducer(**dict(self.my_ini['kafka_producer']))
        self.con = ConsulAPI()
        self.con.path = dict(self.my_ini['consul'])['path']

        self.uuid = None                    # session id
        self.session_time = time.time()     # session生成时间戳
        self.ttl = dict(self.my_ini['consul'])['ttl']               # 生存周期
        self.lock_name = dict(self.my_ini['consul'])['lock_name']   # 锁名

        self.local_ip = socket.gethostbyname(socket.gethostname())  # 本地IP

        self.partitions = (12, 2)       # 分区数
        self.item = None
        self.part_list = list(range(60))

    def get_lock(self):
        """获取锁"""
        p = False
        if self.uuid is None:
            self.uuid = self.con.put_session(self.ttl, self.lock_name)['ID']
            self.session_time = time.time()
            p = True
        # 大于一定时间间隔则更新session
        t = time.time() - self.session_time
        if t > (self.ttl - 5):
            self.con.renew_session(self.uuid)
            self.session_time = time.time()
            p = True
        if self.item is None:
            for i in range(self.partitions[1]):
                l = self.con.get_lock(self.uuid, self.local_ip, i)
                if l == None:
                    self.uuid = None
                    return False
                if l:           # l是True
                    self.item = i
                    self.part_list = list(range(self.partitions[0]))[i::self.partitions[1]]
                    break
        else:
            l = self.con.get_lock(self.uuid, self.local_ip, self.item)
        if p:
            lock_msg = '{0} {1} {2} {3}'.format(self.uuid, l, self.item, self.part_list)
            print(lock_msg)
            logger.info(lock_msg)
        # session过期
        if l == None:
            self.uuid = None
            return False
        return l

    def handling_data(self):
        info = []
        offsets = {}
        for i in range(1000):
            msg = self.kc.c.poll(0.001)
        
            if msg is None:
                continue
            if msg.error():
                continue
            else:
                try:
                    i = json.loads(msg.value().decode('utf-8'))
                    if i.get('KKBH', None) is None:
                        logger.info(i)
                    else:
                        item = {
                            'kkdd_id': i['KKBH'],
                            'kkdd': helper.KKBH.get(i['KKBH'], ''),
                            'fxbh_id': int(i['FXBH']),
                            'jgsj': arrow.get(i['JGSJ'], 'YYYY/MM/DD/HH/mm/ss').format('YYYY-MM-DD HH:mm:ss'),
                            'cdbh': int(i['CDBH']),
                            'hphm': i['HPHM'].encode('utf-8').decode('unicode_escape'),
                            'hpys_id': int(i['HPYS']),
                            'clsd': i['CLSD'],
                            'clxs': i['CLXS'],
                            'txsl': i['TXSL'],
                            'imgurl': i['TX1'].encode('utf-8').decode('unicode_escape'),
                            'imgurl6': i['TX6'].encode('utf-8').decode('unicode_escape'),
                            'csys': i['CSYS'],
                            'cllx': i['CLLX'],
                            'hpzl': i['HPZL'],
                            'gcxh': i.get('GCXH', '')
                        }
                        info.append(item)
                except Exception as e:
                   logger.error(e)
                   logger.error(msg.value())
            par = msg.partition()
            off = msg.offset()
            offsets[par] = off
        if offsets == {}:
            return 0
        else:
            lost_msg = []             # 未上传数据列表
            def acked(err, msg):
                if err is not None:
                    lost_msg.append(msg.value().decode('utf-8'))
                    logger.error(msg.value())
                    logger.error(err)
            t = arrow.now('PRC').format('YYYY-MM-DD HH:mm:ss')
            for i in info:
                value = {'timestamp': t, 'message': i}
                self.kp.produce_info(key=None, value=json.dumps(value), cb=acked)
            self.kp.flush()
            if len(lost_msg) > 0:
                return 0
            self.kc.c.commit(async=False)
            info_msg = 'info={0}, lost_msg={1}, offset={2}'.format(len(info), len(lost_msg), offsets)
            print('info={0}, lost_msg={1}'.format(len(info), len(lost_msg)))
            logger.info(info_msg)
            return 1

    def main_loop(self):
        count = 0
        while 1:
            try:
                #if not self.get_lock():
                #    if self.kc is not None:
                #        del self.kc
                #        self.kc = None
                #    self.item = None
                #    self.part_list = []
                #    time.sleep(2)
                #    continue
                #logger.info('loop')
                if count > 5:
                    logger.info('exit')
                    exit()
                if self.kc is None:
                    self.kc = KafkaConsumer(**dict(self.my_ini['kafka_consumer']))
                    self.kc.assign(self.part_list)
                n = self.handling_data()
                if n > 0:
                    count = 0
                else:
                    count += 1
                    logger.info('count={0}'.format(count))
                    time.sleep(1)
            except Exception as e:
                logger.exception(e)
                count += 2
                logger.info('count={0}'.format(count))
                time.sleep(15)

        
