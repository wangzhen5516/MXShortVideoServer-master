if [ "$DEPLOY_ENV" == "not_defined" ] || [ "$SERVICE_NAME" == "not_defined" ] || [ "$CONFIG_URL" == "not_defined" ] || [ "$PORT" == "not_defined" ]; then
  echo "Container failed to start, pls check env variables DEPLOY_ENV SERVICE_NAME CONFIG_URL PORT"
  exit 1
fi

ensure_success() {
  return_status="$1"
  error_message="$2"
  if [ "$return_status" -ne 0  ];then
    echo "$error_message"
    exit
  fi
}

check_url_return_code() {
  export url=$1
  export mcode=`curl -o /dev/null -s -w "%{http_code}\n" $url`
  echo "$mcode"
  if [ "$mcode" == "200" ]; then
    return 1
  else
    echo "Failed to download $url"
    exit 1
  fi
}

echo "Downloading resources for $DEPLOY_ENV..."

check_url_return_code "$CONFIG_URL/newrelic.yml"
curl -sLo /srv/repo/conf/newrelic.yml $CONFIG_URL/newrelic.yml
ensure_success "$?" "Failed to download $CONFIG_URL/newrelic.yml"

if [ $DEPLOY_ENV == "mxdev" ]; then
  check_url_return_code "$CONFIG_URL/conf.properties"
  curl -sLo /srv/repo/conf/conf.properties $CONFIG_URL/conf.properties
  ensure_success "$?" "Failed to download $CONFIG_URL/conf.properties"
else
  check_url_return_code "$CONFIG_URL/conf-$DEPLOY_ENV.properties"
  curl -sLo /srv/repo/conf/conf.properties $CONFIG_URL/conf-$DEPLOY_ENV.properties
  ensure_success "$?" "Failed to download $CONFIG_URL/conf-$DEPLOY_ENV.properties"
fi

export MEM_NUM="$(($(($(($(cat /sys/fs/cgroup/memory/memory.limit_in_bytes) * 70 / 100 )) / 1024 )) / 1024 ))"

export JAVA_OPTIONS="-Xmx${MEM_NUM}m -Xms${MEM_NUM}m"

echo "Setting java opts $JAVA_OPTIONS"

export RECOMMENDATION_PORT=$PORT

exec "$@"
