#!/bin/sh
set -e
function ensure_success {
        return_status="$1"
        error_message="$2"
        if [ "$return_status" -ne 0  ];then
                echo "$error_message"
                exit
        fi  
}

npm install
mvn clean install -DskipTests
ensure_success "$?" "error building jar"
wget -O ./target/newrelic.jar https://s3.ap-south-1.amazonaws.com/mx-search-corpus/search_server/newrelic.jar
ensure_success "$?" "error downloading jar"
