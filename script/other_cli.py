#!/usr/bin/env python
# coding: utf-8

import os
import sys
import datetime
sys.path.append('./gen-py')

from recommend import RecommendService

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TCompactProtocol

from mzy_conf import *


def create_req_list(req_type, req_list):
  result = list()
  for r in req_list:
    req = req_type()
    for item in r.items():
      if hasattr(req, item[0]):
        setattr(req, item[0], item[1])
    result.append(req)
  return result


def send_req(get_data_method, req_list):
  for req in req_list:
    start = datetime.datetime.now()
    res = get_data_method(req)
    print req
    print "********"
    print res
    end = datetime.datetime.now()
    spend = (end - start).microseconds / 1000
    print req
    if res is None:
      print "res is none!"
      return
    if isinstance(res, RecommendService.Response):
      if res.resultList is not None:
        print "res.resultList:"
        for r in res.resultList:
          print r
          print r.channelIds
      if res.detailList is not None:
        print "res.detailList:"
        for detail in res.detailList:
          print detail
      print "responseType: " + str( res.responseType)
      print "spend [" + str(spend) + "]ms\n"
    else:
      if len(res) == 0:
        print "no result!"
        return
      if isinstance(res[0], RecommendService.Tabs):
        print "fetchTabs:"
        for res_i in res:
          print res_i.tabName
      elif isinstance(res[0], RecommendService.Banner):
        print "fetchBannerData:"
        for res_i in res:
          print res_i
      elif isinstance(res[0], RecommendService.DetailInfo):
        print "getDetails:"
        for res_i in res:
          print res_i
      else:
        print "unknown interface~~~~~"
      print "spend [" + str(spend) + "]ms\n"

try:
  #transport = TSocket.TSocket('localhost', 19889)
  transport = TSocket.TSocket('ec2-13-126-189-162.ap-south-1.compute.amazonaws.com', 19889)
  transport = TTransport.TFramedTransport(transport)
  protocol = TCompactProtocol.TCompactProtocol(transport)
  client = RecommendService.Client(protocol)
  transport.open()
  msg = client.test("jack")
  print "server reponse: " + msg
  ii=0
  start = datetime.datetime.now()
  while 1 :
    for interface in interface_for_test:
      if interface == 'getDetails':
        direq_list = create_req_list(RecommendService.DIRequest, getDetails_request_list)
        send_req(client.getDetails, direq_list)
      elif interface == 'recommend':
        req_list = create_req_list(RecommendService.Request, recommend_request_list)
        send_req(client.recommend, req_list)
      elif interface == 'fetchTabs':
        req_list = create_req_list(RecommendService.Request, fetchTabs_request_list)
        send_req(client.fetchTabs, req_list)
      elif interface == 'fetchBannerData':
        req_list = create_req_list(RecommendService.Request, fetchBannerData_request_list)
        send_req(client.fetchBannerData, req_list)
      elif interface == 'getGenresList':
        req_list = create_req_list(RecommendService.Request,getGenresList_request_list)
        send_req(client.getGenresList, req_list)
      else:
        print 'unknown interface, please check your config!\n'
    if os.path.exists('./request_conf.pyc'):
      os.remove('./request_conf.pyc')

    break

except Thrift.TException, ex:
  print "%s" % (ex.message)
finally:
  transport.close()