package main

import (
	"time"
	"log"
	"os"
	"fmt"
	".."
	"github.com/pinterest/bender"
	bthrift "github.com/pinterest/bender/thrift"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/pinterest/bender/hist"
	//"io/ioutil"
	//"github.com/widuu/gojson"
	//"math/rand"
	"bufio"
	//"io"
	"strings"
	"strconv"
	"io"
)

func SyntheticRecommendRequests() chan interface{}{
	var num = 50000
	c := make(chan interface{}, num)

	//f, err := os.Open("output.log_output")
	//f, err := os.Open("22")
	//f, err := os.Open("./data/short_video_5_0.raw")
	//f, err := os.Open("./data/mxbeta_version_short_video_5_0.raw")
	f, err := os.Open("111")
	if err != nil {
		fmt.Print(err)
	}

	buf := bufio.NewReader(f)
	for ; len(c)<num-1; {
		line, error := buf.ReadString('\n')
		line = strings.TrimSpace(line)
                
                if error != nil || io.EOF == error {
		        break
		}
		lineArray := strings.Split(line, ", ")
		var infoMap map[string]string
		/* 创建集合 */
		infoMap = make(map[string]string)
		for _, info := range lineArray {
			everyInfo := strings.Split(info, ":")
			if 1 == len(everyInfo) {
				infoMap[everyInfo[0]] = ""
                                continue
			}

			if 3 == len(everyInfo) {
				infoMap["channel"] = everyInfo[1] + everyInfo[2]
                                continue
			}
			infoMap[everyInfo[0]] = everyInfo[1]
		}

		var request recommend.Request

		_, ok := infoMap["userId"]
		if ok {
			request.UserId = infoMap["userId"]
		}else {
			request.UserId = ""
		}

		_, ok = infoMap["cardId"]
		if ok {
			request.CardId = infoMap["cardId"]
		}else {
			request.CardId = ""
		}

                _, ok = infoMap["country"]
                if ok {
                        s := infoMap["country"]
                        request.Country = &s
                }

		_, ok = infoMap["num"]
		if ok {
			b,_ := strconv.Atoi(infoMap["num"])
			request.Num = int32(b)
		}

		_, ok = infoMap["finalId"]
		if ok {
			request.FinalId = infoMap["finalId"]
		}


		_, ok = infoMap["type"]
		if ok {
			b,_ := strconv.Atoi(infoMap["type"])
			request.Type = int8(b)
		}

		_, ok = infoMap["interfaceName"]
		if ok {
			request.InterfaceName = infoMap["interfaceName"]
		}

		_, ok = infoMap["resourceId"]
		if ok {
			request.ResourceId = infoMap["resourceId"]
		}else{
			request.ResourceId = ""
		}

		_, ok = infoMap["platformId"]
		if ok {
			request.PlatformId = infoMap["platformId"]
		}else{
			request.PlatformId = ""
		}

		_, ok = infoMap["tabId"]
		if ok {
			request.TabId = infoMap["tabId"]
		}else{
			request.TabId = ""
		}

                _, ok = infoMap["networkStatus"]
                if ok {
                        s := infoMap["networkStatus"]
                        request.NetworkStatus = &s
                }

                _, ok = infoMap["language"]
                if ok {
                        s := infoMap["language"]
                        request.Language = &s
                }

                _, ok = infoMap["area"]
                if ok {
                        s := infoMap["area"]
                        request.Area = &s
                }

                _, ok = infoMap["entranceType"]
                if ok {
                        s := infoMap["entranceType"]
                        request.EntranceType = &s
                }

                _, ok = infoMap["filterId"]
                if ok {
                        s := infoMap["filterId"]
                        request.FilterId= &s
                }

		_, ok = infoMap["resourceType"]
		if ok {
			request.ResourceType = infoMap["resourceType"]
		}else{
			request.ResourceType = ""
		}

		c <- &request
	}

	close(c)
	return c

}



//重要:需要跟服务端所用协议一致
func ASExecutor(request interface{}, transport thrift.TTransport) (interface{}, error) {
	pFac := thrift.NewTCompactProtocolFactory()
	client := recommend.NewRecommendServiceClientFactory(transport, pFac)
	return client.Recommend(nil, request.(*recommend.Request))

	//client := di.NewDIServiceClientFactory(transport, pFac)
	//return client.GetDetail(nil, request.(*di.DIRequest))
}

// 主要是用bender的接口，完成压测
func main() {
	//var qpsTarget = 575.0
	qpsTarget , err := strconv.ParseFloat(os.Args[1], 64)
	fmt.Println(err)
	intervals := bender.ExponentialIntervalGenerator(qpsTarget) // qps
	requests := SyntheticRecommendRequests()           //总请求数
	//host := "ec2-35-154-68-167.ap-south-1.compute.amazonaws.com:19955"
	//host := "ec2-52-66-188-192.ap-south-1.compute.amazonaws.com:19889" //press机器
	host := "ec2-13-232-94-32.ap-south-1.compute.amazonaws.com:19889" //press机器
	//host := "ec2-13-126-189-161.ap-south-1.compute.amazonaws.com:19889" //press机器
	//host := "ec2-13-127-65-186.ap-south-1.compute.amazonaws.com:19889" //beta-0-new
	//host := "ec2-13-127-145-119.ap-south-1.compute.amazonaws.com:19889" //press-1
	//host := "ec2-13-232-35-96.ap-south-1.compute.amazonaws.com:19889" //press-3
	//host := "127.0.0.1:19959"
	//buffer字节数, clientExec, 超时时间, hosts--写server所在的ip和端口,如非同一机器,保证端口可访问
	//exec := bthrift.NewThriftRequestExec(thrift.NewTBufferedTransportFactory(125), ASExecutor, 10 * time.Second, host)
	//exec := bthrift.NewThriftRequestExec(thrift.NewTCompactProtocolFactory(), ASExecutor, 10 * time.Second, host)
	exec := bthrift.NewThriftRequestExec(thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory()), ASExecutor, 10 * time.Second, host)
	recorder := make(chan interface{}, 128)
	bender.LoadTestThroughput(intervals, requests, exec, recorder)
	l := log.New(os.Stdout, "", log.LstdFlags)
	h := hist.NewHistogram(60000, 1000000)
	bender.Record(recorder, bender.NewLoggingRecorder(l), bender.NewHistogramRecorder(h))
	fmt.Println(h)
}
