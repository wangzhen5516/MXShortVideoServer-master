#! /bin/bash
if [ ! -d "./bak" ]; then
    mkdir ./bak
fi
if [ -d "./mx-beta" ]; then
    filename="./mx-beta.`date +%Y%m%d%H%M%S`"
    mv ./mx-beta ./bak/$filename
fi
mkdir ./mx-beta