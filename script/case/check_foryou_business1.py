#!/usr/bin/env python
#coding=utf-8
import json
import requests
import sys
import time
import datetime
sys.path.append('./config/')
from config import *
sys.path.append(THIRFT_TEST)
reload(sys)
sys.setdefaultencoding("utf-8")

from recommend import RecommendService

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TCompactProtocol
import os

typeSet=set()
channelSet=set()
def run() :
    try:
        transport = TSocket.TSocket('localhost', 19889)
        transport = TTransport.TFramedTransport(transport)
        protocol = TCompactProtocol.TCompactProtocol(transport)
        client = RecommendService.Client(protocol)
        transport.open()
        req_time = datetime.datetime.now()+datetime.timedelta(days=-3)
        #print req_time.strftime("%Y-%m-%d %H:%M:%S")
        i=0
        while i < 10 :
            req = RecommendService.Request()
            req.userId = req_time.strftime("%Y-%m-%d %H:%M:%S")+"_test"
            req.id = req.userId
            req.channel= "all"
            req.country= "IN"
            req.yuyan  ="*"
            req.interfaceName = "youtube_version_1_0"
            req.num = 10
            res = client.recommend(req)
            index=0
            if None == res or None == res.resultList :
                print 'check_foryou_business1.py -> no result'
                return 1
            for result in res.resultList:
                #print result
                #print result.channel
                if result.channel not in channelSet:
                    channelSet.add(result.channel)
                if result.type not in typeSet:
                    typeSet.add(result.type)
                #三天前数据召回
                #if index==0 and result.type==1 and  result.publishTime<req_time.strftime("%Y-%m-%d %H:%M:%S"):
                index=index+1
            if len(channelSet)<=1:
                print 'check_foryou_business1.py -> 脚本Check_foryou chanel频道小于1个'
                return 1
            i+=1
            transport.close()
            return 0
    
    except Thrift.TException, ex:
        print "%s" % (ex.message)
        return 1
if __name__ == '__main__':
    exit(run())
