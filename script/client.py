#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import datetime

sys.path.append('./gen-py')

from recommend import RecommendService

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TCompactProtocol
from request_conf import *


def create_req_list(req_type, req_list):
    result = list()
    for r in req_list:
        req = req_type()
        for item in r.items():
            if hasattr(req, item[0]):
                setattr(req, item[0], item[1])
        result.append(req)
    return result


def send_req(get_data_method, req_list, field_to_print):
    for req in req_list:
        start = datetime.datetime.now()
        res = get_data_method(req)
        end = datetime.datetime.now()
        spend = (end - start).microseconds / 1000
        print get_data_method.func_name + ':'
        print req
        if res is None:
            print "res is none!"
            print "spend [" + str(spend) + "]ms\n"
            return
        if isinstance(res, RecommendService.Response):
            # 未指定打印字段，则打印detailList或resultList
            if field_to_print is None or len(field_to_print) == 0:
                if res.detailList is not None:
                    print 'detailList size: %d' % len(res.detailList)
                    for res_i in res.detailList:
                        print_info(res_i, [])
                elif res.resultList is not None:
                    print 'resultList size: %d' % len(res.resultList)
                    for res_i in res.resultList:
                        print_info(res_i, [])
                else:
                    print 'there is no resultList and detailList in the response!'
            else:
                result_field_list = [x.split('.')[1] for x in field_to_print if
                                     '.' in x and x.split('.')[0] == 'result']
                detail_field_list = [x.split('.')[1] for x in field_to_print if
                                     '.' in x and x.split('.')[0] == 'detailInfo']

                # 打印resultList
                if res.resultList is not None and len(res.resultList) > 0:
                    if len(result_field_list) > 0:
                        print 'resultList size: %d' % len(res.resultList)
                        for res_i in res.resultList:
                            print_info(res_i, result_field_list)

                # 打印detailList
                elif res.detailList is not None and len(res.detailList) > 0:
                    print 'detailList size: %d' % len(res.detailList)
                    if len(detail_field_list) > 0:
                        for res_i in res.detailList:
                            print_info(res_i, detail_field_list)
                else:
                    print 'there is no resultList and detailList in the response!'

                # 打印response中其他数据
                response_str = 'Response('
                for field in field_to_print:
                    if '.' not in field:
                        if hasattr(res, field):
                            response_str += field + ":"
                            if getattr(res, field) is not None:
                                response_str += getattr(res, field) + ', '
                            else:
                                response_str += 'None, '
                if response_str.endswith(', '):
                    response_str = response_str[:-2]
                print response_str + ')'

                # 指定了result字段而未指定result的子字段，则打印全部字段
                if 'result' in field_to_print:
                    if res.resultList is not None and len(res.resultList) > 0:
                        print 'resultList size: %d' % len(res.resultList)
                        for res_i in res.resultList:
                            print_info(res_i, [])
                # 指定了detailinfo字段而未指定detailinfo的子字段，则打印全部字段
                if 'detailInfo' in field_to_print:
                    if res.detailList is not None and len(res.detailList) > 0:
                        print 'detailList size: %d' % len(res.detailList)
                        for res_i in res.detailList:
                            print_info(res_i, [])
        else:
            if len(res) == 0:
                print "no result!\n"
                return
            else:
                print 'res size: %d' % len(res)
                for res_i in res:
                    print_info(res_i, field_to_print)
        print "spend [" + str(spend) + "]ms\n"


def print_info(o, field_to_print):
    if field_to_print is None or len(field_to_print) == 0:
        print o
    else:
        info_dict = dict()
        for field in field_to_print:
            if hasattr(o, field):
                info_dict[field] = getattr(o, field)

        L = ['%s=%r' % (key, value)
             for key, value in info_dict.iteritems()]
        print '%s(%s)' % (o.__class__.__name__, ', '.join(L))


try:
    transport = TSocket.TSocket('localhost', 19889)
    # transport = TSocket.TSocket('localhost', 19959)
    transport = TTransport.TFramedTransport(transport)
    protocol = TCompactProtocol.TCompactProtocol(transport)
    client = RecommendService.Client(protocol)
    transport.open()
    msg = client.test("jack")
    print "server reponse: " + msg

    start = datetime.datetime.now()
    while 1:
        for interface in interface_for_test.items():
            if interface[0] == 'getDetails':
                req_list = create_req_list(RecommendService.DIRequest, interface[1][1])
            else:
                req_list = create_req_list(RecommendService.Request, interface[1][1])
            func = getattr(client, interface[0], None)
            if func is not None and callable(func):
                send_req(func, req_list, interface[1][0])
            else:
                print 'unknown interface, please check your config!\n'
        break

except Thrift.TException, ex:
    print "%s" % (ex.message)
finally:
    transport.close()

