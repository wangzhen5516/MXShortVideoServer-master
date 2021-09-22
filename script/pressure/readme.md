更改bender.go文件
    1、找到当前机器GOPATH路径，更改路径下的/src/github.com/pinterest/bender/bender.go文件
    2、在LoadTestThroughput方法中time.Sleep(time.Duration(wait))这一行后添加
            nowTime := int64(time.Now().UnixNano()/1000000)
    	    timeStr := strconv.FormatInt(nowTime,10)
    		time1 := timeStr
    		reflect.ValueOf(request).Elem().FieldByName("TimeSign").Set(reflect.ValueOf(&time1))
    3、具体可直接复制当前目录下benderTest.txt文件内容