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
./node_modules/wait-port/bin/wait-port.js -t 60000 4000
sleep 5
consul maint -disable
ensure_success "$?" "maint disable"
sleep 5
