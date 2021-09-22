#!/usr/bin/env python
#coding: utf8
import sys
import datetime
sys.path.append('./gen-py')
reload(sys)
sys.setdefaultencoding('utf-8')
from recommend import RecommendService

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TCompactProtocol

try:
  transport = TSocket.TSocket('localhost', 19889)
  transport = TTransport.TFramedTransport(transport)
  protocol = TCompactProtocol.TCompactProtocol(transport)
  client = RecommendService.Client(protocol)
  transport.open()
  msg = client.test("jack")
  print "server reponse: " + msg

  start = datetime.datetime.now()
  while 1 :
    req = RecommendService.Request()
    #req = fetchBannerDataService.Request()
    req.userId = "0eeba555b3-7303-485a-ac62-dcbe11d0e602102"

    req.cardId="nullCard"
    #req.cardId="4da67b58d24a4d904661d85fba88c899"
    req.platformId="1"
    #req.nextToken = "-2"
    #req.tabId="2"

    #req.entranceType="tv_show_video"
    #req.resourceId="4da67b58d24a4d904661d85fba88c899"
    #req.resourceType="short_video"
    #req.genresList = ["54fffed987dffd200ed48df992c979c7", "f00b79213e8109a06a401e0c8d61b01b"]
    #req.filterId="de44cf3fb1edd1ab82999f00065887a3"
    #req.entranceType="song"
    #req.cardId="4bac6bae95227199772ec05914d1c923"
    #req.interfaceName = "mxbeta_main_version_1_0"
    #req.interfaceName = "similar_playlist_version_1_0"
    req.interfaceName = "mxbeta_main_nullcard_version_1_0"
    #req.nextToken = "-5"
    #req.interfaceName = "banner_version_1_0"
    #req.interfaceName = "mxbeta_main_version_1_0"
    #req.interfaceName = "popular_short_videos_from_the_publisher_version_1_0"
    req.languageList = ["hi"]
    #req.resourceType="publisher"
    #req.resourceId="72e758a54e0587683a60f6075689933a"

    #req.finalId = "817d09ffcdd46082ecd1ff221f76843e"
    #req.interfaceName = "episodes_one_season_version_4_0"
    req.type = 0
    req.num = 20
    print req
    res = client.recommend(req)
    #res = client.fetchBannerData(req)
    #res = client.fetchTabs(req)
    #print res
    #for res_i in res :
    #    print res_i.tabName + "\t" + str([x.cardName for x in res_i.cardList])
    #end = datetime.datetime.now()
    #print res.resultList
    #print "listStyle: " + str(res.listStyle)
    #print "moreStyle: " + str(res.moreStyle)
    #print res
    print res
    if res.resultList is not None :
        for result in res.resultList:
            print str(result.id) + "\t" + str(result.recallSign) + "\t" + str(result.resultType)
    end = datetime.datetime.now()

    print end.strftime("%Y-%m-%d %H:%M:%S") + ":" + str(datetime.datetime.now().microsecond)
    spend = (end - start).microseconds / 1000
    print "spend [" + str(spend) + "]ms"
    break



except Thrift.TException, ex:
  print "%s" % (ex.message)
finally:
  transport.close()