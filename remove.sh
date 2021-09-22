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

consul maint -enable
ensure_success "$?" "maint enable"
sleep 17
