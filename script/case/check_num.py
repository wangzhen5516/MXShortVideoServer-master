#!/usr/bin/env python
#coding:utf-8
#测试 请求数与结果数是否一致
import os,sys
import datetime
sys.path.append('./config/')
from config import *
sys.path.append(THIRFT_TEST)
import random
import logging
from recommend import RecommendService
from time import sleep
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TCompactProtocol
from random import choice
def run() :
    try:
        transport = TSocket.TSocket('localhost',19889)
        transport = TTransport.TFramedTransport(transport)
        protocol = TCompactProtocol.TCompactProtocol(transport)
        client = RecommendService.Client(protocol)
        transport.open()
        i=0
        while i < 10:
            req = RecommendService.Request()
            req.userId = 'test'
            req.channel='all'
            country=['IN']
            c=choice(country)
            req.country= str(c)
            req.yuyan  ="*"
            req.interfaceName = "youtube_version_1_0"
            num=[50,30,10,80]
            e=choice(num)
            req.num = e
            res = client.recommend(req)
            #print len(res.resultList)
        
            if None == res or res.resultList==None or len(res.resultList)<req.num:
                print 'check_num.py -> resultList结果为空或请求数小于结果数, req:' + str(req)
                return 1
            #print end.strftime("%Y-%m-%d %H:%M:%S") + ":" + str(datetime.datetime.now().microsecond)
            i+=1
        transport.close()
        return 0
    
    except Thrift.TException,ex:
        print "%s" % (ex.message)
        return 1

if __name__ == '__main__':
    exit(run())
