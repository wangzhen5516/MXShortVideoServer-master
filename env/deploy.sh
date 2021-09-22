#! /bin/bash
cd ../
/usr/local/maven/bin/mvn clean install
cd ./env
cp -r ../conf .
mkdir bin
mkdir log
cp ../target/recommend-0.0.1-SNAPSHOT.jar ./bin/
sh start.sh