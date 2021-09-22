ps aux|grep recommend-0.0.1 |grep -v "grep" | awk '{print $2}' | xargs -i kill -9 {}
