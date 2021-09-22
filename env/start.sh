ps aux|grep recommend-0.0.1 |grep -v "grep" | awk '{print $2}' | xargs -i sudo kill -9 {}
rm -rf log/*
nohup java -jar ./bin/recommend-0.0.1-SNAPSHOT.jar ./conf/conf.properties > ./log/nohup.out &
#tail -f log/nohup.out
