#! encoding: utf8
import logging
import random
import threading
import time
import sys
import fire
from timeit import default_timer
sys.path.append('./gen-py')
from recommend import RecommendService
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol


class PressureTest:
    def __init__(self, log_path=None, log_level=logging.DEBUG, req_path=None, thread_num=1, duration=60, qps=1000, is_stat=False):
        logging.basicConfig(level=log_level,
                            filename=log_path,
                            filemode='a',
                            format='%(asctime)s[%(levelname)s][%(filename)s:%(lineno)d:%(funcName)s]%(message)s')
        self.req_path = req_path
        self.thread_num = thread_num
        self.duration = duration
        self.qps = qps
        self.is_stat = is_stat

        self.req_file = open(self.req_path, 'r')
        self.req_list = [self.parse_req(x.strip()) for x in self.req_file]
        self.req_list = [x for x in self.req_list if x]
        self.req_list_len = len(self.req_list)
        #print self.req_list_len

    def parse_req(self, line):
        req = RecommendService.Request()
        try :
            line = line.split(', ')
            info = {}
            for card in line :
                card_array = card.split(':')
                if 2 != len(card_array) :
                    req.deviceInfo = card.replace('deviceInfo:', '')
                    continue
                info[card_array[0]] = card_array[1]
            if info.get('userId'):
                req.userId = info.get('userId')
            else :
                req.userId = ''
            if info.get('uuid'):
                req.uuid = info.get('uuid')
            if info.get('country'):
                req.country = info.get('country')
            if info.get('channel'):
                req.channel = info.get('channel')
            if info.get('yuyan'):
                req.yuyan = info.get('yuyan')
            if info.get('num'):
                req.num = int(info.get('num'))
            if info.get('finalId'):
                req.finalId = info.get('finalId')
            #if info.get('type'):
            #    req.type = info.get('type')
            if info.get('interfaceName'):
                req.interfaceName = info.get('interfaceName')
            #if info.get('hasWiFi'):
            #    req.hasWiFi = info.get('hasWiFi')
            if info.get('metaType'):
                req.metaType = info.get('metaType')
            if info.get('id'):
                req.id = info.get('id')
            else :
                req.id = ''
        except Exception,e:
            print e
            return None
        return req

    def start(self):
        logging.info('start threads.')
        thread_list = []
        self.thread_data_list = []
        for i in xrange(0, self.thread_num):
            thread_data = {}
            thread_data['index'] = i
            t = threading.Thread(target=self._run, name='thread-%d' % i, args=(thread_data,))
            thread_list.append(t)
            self.thread_data_list.append(thread_data)
            logging.info('thread_name=%s, index=%s starting...' % (t.name, i))
            t.start()

        for t in thread_list:
            t.join()
        logging.info('all thread stopped.')

        if self.is_stat == True:
            self.make_stat()
            logging.info('make stat success.')

    def make_stat(self):
        total_log_list = []
        for thread_data in self.thread_data_list:
            log_list = thread_data['log_list']
            total_log_list += log_list

        # print total_log_list
        total_log_list = sorted(total_log_list, key=lambda a: a['exTime'])
        #for x in total_log_list:
        #    print x
        log_len = len(total_log_list)
        logging.info('req_path=%s, thread_num=%s, duration=%s, qps=%s', self.req_path, self.thread_num, self.duration, self.qps)
        logging.info('error ratio=%s%%', len([x for x in total_log_list if x['errno']!=0])/log_len*100)
        logging.info('min exTime=%sms', round(total_log_list[0]['exTime']*1000))
        logging.info('90 exTime=%sms', round(total_log_list[int(0.90 * log_len) - 1]['exTime']*1000))
        logging.info('95 exTime=%sms', round(total_log_list[int(0.95 * log_len) - 1]['exTime']*1000))
        logging.info('99 exTime=%sms', round(total_log_list[int(0.99 * log_len) - 1]['exTime']*1000))
        logging.info('max exTime=%sms', round(total_log_list[int(log_len - 1)]['exTime']*1000))
        logging.info('avg exTime=%sms', round(reduce(lambda a, b: a + b, [x['exTime'] for x in total_log_list]) / log_len*1000))
        logging.info('real_qps=%s', log_len/self.duration)


    def _run(self, thread_data):
        index = thread_data['index']
        log_list = []
        qps_per_thread = self.qps / self.thread_num
        second_per_req = 1.0 / qps_per_thread
        client = self.get_client()
        if not client :
            logging.error("get client failed...")
            return
        logging.info('qps_per_thread=%s, second_per_req=%s', qps_per_thread, second_per_req)
        start_time = time.time()
        t = threading.current_thread()
        logging.debug('thread_name=%s, index=%s running...' % (t.name, index))
        req_num = 0
        while time.time() - start_time < self.duration:
            req_num += 1
            # req_start_time = time.time()
            req_start_time = default_timer()
            ran_index = random.randint(0, self.req_list_len - 1)
            logging.debug('thread_name=%s, ran_index=%s', t.name, ran_index)
            req = self.req_list[ran_index]
            if req :
                res_dict = self.execute_req(client, req)
            # req_ex_time = time.time() - req_start_time
            req_ex_time = default_timer() - req_start_time
            logging.debug('thread_name=%s, req_ex_time=%s', t.name, req_ex_time)
            if self.is_stat:
                log_obj = {}
                log_obj['logId'] = '%s-%s' % (index, req_num)
                log_obj['exTime'] = req_ex_time
                log_obj['errno'] = res_dict['errno']
                # print log_obj
                log_list.append(log_obj)
            if req_ex_time < second_per_req:
                sleep_time = (second_per_req - req_ex_time) * 0.85
                time.sleep(sleep_time)
                logging.debug('thread_name=%s, sleep_time=%s', t.name, sleep_time) 
        thread_data['log_list'] = log_list
        logging.debug('thread_name=%s, index=%s stopped.' % (t.name, index))

    def get_client(self): 
        try:
            transport = TSocket.TSocket('localhost', 19887)
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            client = RecommendService.Client(protocol)
            transport.open()
            return client
        except Thrift.TException, ex:
            print "%s" % (ex.message)
            return None

    def execute_req(self, client, req):
        res_dict = {}
        try:
            #print req
            res = client.recommend(req)
            #print res.resultList
            if res :
                res_dict['errno'] = res.flag
                if res.resultList :
                    res_dict['resultSize'] = res.resultList.__len__()
                else :
                    res_dict['resultSize'] = 0
            else : 
                res_dict['errno'] = -100
                res_dict['resultSize'] = 0
        except Thrift.TException, ex:
            res_dict['errno'] = -100
            res_dict['resultSize'] = 0
            print "%s" % (ex.message)
            logging.error("%s" % (ex.message))

        t = threading.current_thread()
        logging.info('thread_name=%s, execute_req[req=%s]...' % (t.name, req))
        return res_dict

if __name__ == '__main__':
    fire.Fire(PressureTest)
