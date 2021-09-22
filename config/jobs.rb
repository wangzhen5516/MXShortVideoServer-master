if File.exist?('/etc/profile.d/java_opts.sh')
  ENV["JAVA_OPTS"] = File.read("/etc/profile.d/java_opts.sh").gsub!(/[!@%&"]/,'').chomp.partition('=').last
end

job "mxshortserver" do
  command "java #{ENV['JAVA_OPTS']} -Dnewrelic.config.file=./conf/newrelic.yml -javaagent:./target/newrelic.jar -jar target/recommend-0.0.1-SNAPSHOT.jar ./conf/conf.properties"
  stdout_logfile "log/production.log"
  env %(RECOMMENDATION_PORT="40%(process_num)02d")
  startsecs 50
end
