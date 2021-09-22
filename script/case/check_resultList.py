#!/usr/bin/env python
#coding:utf-8
#测试 请求数与结果数是否一致
import sys
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
import json

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
            num=[1]
            e=choice(num)
            req.num = e
            res = client.recommend(req)
            if None == res or None == res.resultList :
                print 'check_resultList.py -> no result'
                return 1
            #print res.resultList
            #print type(res.resultList)
            for result in res.resultList:
                #print result
                for j in FIELD_LIST:
                    #print j, getattr(result,j)
                    try:
    
                        if getattr (result,j)==None :
                            print 'check_resultList.py -> 脚本Check_resultist字段：'+j+' 为None'
                            return 1
                        if j not in str(result):
                            print 'check_resultList.py -> 脚本Check_resultList字段：'+j+' not found'
                            return 1
                    except Exception as e:
                        print 'check_resultList.py -> 脚本Check_resultList没有字段: '+j
                        return 1
            if res.resultList==None or len(res.resultList)<req.num:
                print 'check_resultList.py -> 结果为空或结果数小于请求数'
                return 1
            
            i+=1
            transport.close()
            return 0
    
    except Thrift.TException,ex:
        print "%s" % (ex.message)
        return 1
if __name__ == '__main__':
    exit(run())
