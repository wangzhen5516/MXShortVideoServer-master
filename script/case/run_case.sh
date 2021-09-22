#! /bin/bash
#用于跑各类case


python check_num.py
if [ $? -eq 0 ];then
    echo "run case[check_num] pass"
else
    echo "run case[check_num] fail"
    exit 1
fi

python check_foryou_business1.py
if [ $? -eq 0 ];then
    echo "run case[check_foryou_business1] pass"
else
    echo "run case[check_foryou_business1] fail"
    exit 1
fi

python check_resultList.py
if [ $? -eq 0 ];then
    echo "run case[check_resultList] pass"
else
    echo "run case[check_resultList] fail"
    exit 1
fi

exit 0
