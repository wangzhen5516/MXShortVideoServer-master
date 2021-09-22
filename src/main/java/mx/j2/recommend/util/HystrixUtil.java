package mx.j2.recommend.util;

import com.netflix.hystrix.*;
import com.netflix.hystrix.strategy.properties.HystrixPropertiesCommandDefault;
import com.newrelic.api.agent.NewRelic;
import mx.j2.recommend.conf.Conf;

import java.util.List;

/**
 * @author ：zhongrenli
 * @date ：Created in 1:50 下午 2020/7/16
 */
public class HystrixUtil {

    public static final HystrixCommand.Setter httpSetter = HystrixCommand.Setter
            .withGroupKey(HystrixCommandGroupKey.Factory.asKey("HttpServerGroup"))
            .andCommandKey(HystrixCommandKey.Factory.asKey("HttpServerCommand"))
            .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey("http-server-pool"))
            .andCommandPropertiesDefaults(
                    HystrixPropertiesCommandDefault.Setter()
                            .withCircuitBreakerEnabled(true)
                            .withCircuitBreakerErrorThresholdPercentage(10)
                            .withCircuitBreakerRequestVolumeThreshold(100)
                            .withCircuitBreakerSleepWindowInMilliseconds(8000)
                            .withExecutionTimeoutInMilliseconds(300)
                            .withFallbackIsolationSemaphoreMaxConcurrentRequests(5000)
                            .withExecutionIsolationStrategy(HystrixCommandProperties.ExecutionIsolationStrategy.THREAD))
            .andThreadPoolPropertiesDefaults(
                    HystrixThreadPoolProperties.Setter()
                            .withQueueSizeRejectionThreshold(40)
                            .withCoreSize(20)
                            .withMaxQueueSize(80));

    public final static HystrixCommand.Setter ES_SETTER = HystrixCommand.Setter
            .withGroupKey(HystrixCommandGroupKey.Factory.asKey("ElasticSearchGroup"))
            .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey("elasticsearch-pool"))
            .andCommandPropertiesDefaults(
                    HystrixPropertiesCommandDefault.Setter()
                            .withCircuitBreakerEnabled(true)
                            .withCircuitBreakerErrorThresholdPercentage(20)
                            .withCircuitBreakerRequestVolumeThreshold(200)
                            .withCircuitBreakerSleepWindowInMilliseconds(10000)
                            .withExecutionTimeoutInMilliseconds(Conf.getRestClientRequestConfigSocketTimeout())
                            .withExecutionIsolationStrategy(HystrixCommandProperties.ExecutionIsolationStrategy.THREAD))
            .andThreadPoolPropertiesDefaults(
                    HystrixThreadPoolProperties.Setter()
                            .withCoreSize(20)
                            .withMaxQueueSize(80)
                            .withQueueSizeRejectionThreshold(40));

    public final static HystrixCommand.Setter videoEsSetter = HystrixCommand.Setter
            .withGroupKey(HystrixCommandGroupKey.Factory.asKey("VideoElasticSearchGroup"))
            .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey("video-elasticsearch-pool"))
            .andCommandPropertiesDefaults(
                    HystrixPropertiesCommandDefault.Setter()
                            .withCircuitBreakerEnabled(true)
                            .withCircuitBreakerErrorThresholdPercentage(10)
                            .withCircuitBreakerRequestVolumeThreshold(200)
                            .withCircuitBreakerSleepWindowInMilliseconds(10000)
                            .withExecutionTimeoutInMilliseconds(Conf.getRestClientRequestConfigSocketTimeout())
                            .withExecutionIsolationStrategy(HystrixCommandProperties.ExecutionIsolationStrategy.THREAD))
            .andThreadPoolPropertiesDefaults(
                    HystrixThreadPoolProperties.Setter()
                            .withCoreSize(20)
                            .withMaxQueueSize(80)
                            .withQueueSizeRejectionThreshold(40));

    public final static HystrixCommand.Setter video7EsSetter = HystrixCommand.Setter
            .withGroupKey(HystrixCommandGroupKey.Factory.asKey("Video7ElasticSearchGroup"))
            .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey("video7-elasticsearch-pool"))
            .andCommandPropertiesDefaults(
                    HystrixPropertiesCommandDefault.Setter()
                            .withCircuitBreakerEnabled(true)
                            .withCircuitBreakerErrorThresholdPercentage(10)
                            .withCircuitBreakerRequestVolumeThreshold(200)
                            .withCircuitBreakerSleepWindowInMilliseconds(10000)
                            .withExecutionTimeoutInMilliseconds(Conf.getRestClientRequestConfigSocketTimeout())
                            .withExecutionIsolationStrategy(HystrixCommandProperties.ExecutionIsolationStrategy.THREAD))
            .andThreadPoolPropertiesDefaults(
                    HystrixThreadPoolProperties.Setter()
                            .withCoreSize(20)
                            .withMaxQueueSize(80)
                            .withQueueSizeRejectionThreshold(40));

    public final static HystrixCommand.Setter strategyEsSetter = HystrixCommand.Setter
            .withGroupKey(HystrixCommandGroupKey.Factory.asKey("StrategyElasticSearchGroup"))
            .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey("strategy-elasticsearch-pool"))
            .andCommandPropertiesDefaults(
                    HystrixPropertiesCommandDefault.Setter()
                            .withCircuitBreakerEnabled(true)
                            .withCircuitBreakerErrorThresholdPercentage(10)
                            .withCircuitBreakerRequestVolumeThreshold(150)
                            .withCircuitBreakerSleepWindowInMilliseconds(10000)
                            .withExecutionTimeoutInMilliseconds(Conf.getRestClientRequestConfigSocketTimeout())
                            .withExecutionIsolationStrategy(HystrixCommandProperties.ExecutionIsolationStrategy.THREAD))
            .andThreadPoolPropertiesDefaults(
                    HystrixThreadPoolProperties.Setter()
                            .withCoreSize(20)
                            .withMaxQueueSize(80)
                            .withQueueSizeRejectionThreshold(40));

    public final static HystrixCommand.Setter strategyCassandraSetter = HystrixCommand.Setter
            .withGroupKey(HystrixCommandGroupKey.Factory.asKey("StrategyCassandraGroup"))
            .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey("strategy-cassandra-pool"))
            .andCommandPropertiesDefaults(
                    HystrixPropertiesCommandDefault.Setter()
                            .withCircuitBreakerEnabled(true)
                            .withCircuitBreakerErrorThresholdPercentage(20)
                            .withCircuitBreakerRequestVolumeThreshold(10)
                            .withCircuitBreakerSleepWindowInMilliseconds(5000)
                            .withExecutionTimeoutInMilliseconds(300)
                            .withExecutionIsolationStrategy(HystrixCommandProperties.ExecutionIsolationStrategy.THREAD))
            .andThreadPoolPropertiesDefaults(
                    HystrixThreadPoolProperties.Setter()
                            .withCoreSize(20)
                            .withMaxQueueSize(40)
                            .withQueueSizeRejectionThreshold(40));

    public final static HystrixCommand.Setter cassandraSetter = HystrixCommand.Setter
            .withGroupKey(HystrixCommandGroupKey.Factory.asKey("CassandraGroup"))
            .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey("cassandra-pool"))
            .andCommandPropertiesDefaults(
                    HystrixPropertiesCommandDefault.Setter()
                            .withCircuitBreakerEnabled(true)
                            .withCircuitBreakerErrorThresholdPercentage(20)
                            .withCircuitBreakerRequestVolumeThreshold(10)
                            .withCircuitBreakerSleepWindowInMilliseconds(5000)
                            .withExecutionTimeoutInMilliseconds(500)
                            .withExecutionIsolationStrategy(HystrixCommandProperties.ExecutionIsolationStrategy.THREAD))
            .andThreadPoolPropertiesDefaults(
                    HystrixThreadPoolProperties.Setter()
                            .withCoreSize(20)
                            .withMaxQueueSize(40)
                            .withQueueSizeRejectionThreshold(40));

    public final static HystrixCommand.Setter historyCassandraSetter = HystrixCommand.Setter
            .withGroupKey(HystrixCommandGroupKey.Factory.asKey("HistoryCassandraGroup"))
            .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey("history-cassandra-pool"))
            .andCommandPropertiesDefaults(
                    HystrixPropertiesCommandDefault.Setter()
                            .withCircuitBreakerEnabled(true)
                            .withCircuitBreakerErrorThresholdPercentage(20)
                            .withCircuitBreakerRequestVolumeThreshold(10)
                            .withCircuitBreakerSleepWindowInMilliseconds(5000)
                            .withExecutionTimeoutInMilliseconds(600)
                            .withExecutionIsolationStrategy(HystrixCommandProperties.ExecutionIsolationStrategy.THREAD))
            .andThreadPoolPropertiesDefaults(
                    HystrixThreadPoolProperties.Setter()
                            .withCoreSize(20)
                            .withMaxQueueSize(80)
                            .withQueueSizeRejectionThreshold(40));

    public final static HystrixCommand.Setter followCassandraSetter = HystrixCommand.Setter
            .withGroupKey(HystrixCommandGroupKey.Factory.asKey("FollowCassandraGroup"))
            .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey("follow-cassandra-pool"))
            .andCommandPropertiesDefaults(
                    HystrixPropertiesCommandDefault.Setter()
                            .withCircuitBreakerEnabled(true)
                            .withCircuitBreakerErrorThresholdPercentage(20)
                            .withCircuitBreakerRequestVolumeThreshold(10)
                            .withCircuitBreakerSleepWindowInMilliseconds(5000)
                            .withExecutionTimeoutInMilliseconds(300)
                            .withExecutionIsolationStrategy(HystrixCommandProperties.ExecutionIsolationStrategy.THREAD))
            .andThreadPoolPropertiesDefaults(
                    HystrixThreadPoolProperties.Setter()
                            .withCoreSize(20)
                            .withMaxQueueSize(80)
                            .withQueueSizeRejectionThreshold(40));

    public final static HystrixCommand.Setter pubFeatureRedisSetter = HystrixCommand.Setter
            .withGroupKey(HystrixCommandGroupKey.Factory.asKey("PubFeatureRedisSetterGroup"))
            .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey("pubfeature-redis-pool"))
            .andCommandPropertiesDefaults(
                    HystrixPropertiesCommandDefault.Setter()
                            .withCircuitBreakerEnabled(true)
                            .withCircuitBreakerErrorThresholdPercentage(20)
                            .withCircuitBreakerRequestVolumeThreshold(200)
                            .withCircuitBreakerSleepWindowInMilliseconds(1000)
                            .withExecutionTimeoutInMilliseconds(Conf.getRestClientRequestConfigSocketTimeout())
                            .withExecutionIsolationStrategy(HystrixCommandProperties.ExecutionIsolationStrategy.THREAD))
            .andThreadPoolPropertiesDefaults(
                    HystrixThreadPoolProperties.Setter()
                            .withCoreSize(20)
                            .withMaxQueueSize(80)
                            .withQueueSizeRejectionThreshold(40));

    public static final HystrixCommand.Setter segaMakerSetter = HystrixCommand.Setter
            .withGroupKey(HystrixCommandGroupKey.Factory.asKey("SegaMakerGroup"))
            .andCommandKey(HystrixCommandKey.Factory.asKey("SegaMakerCommand"))
            .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey("sega-maker-pool"))
            .andCommandPropertiesDefaults(
                    HystrixPropertiesCommandDefault.Setter()
                            .withCircuitBreakerEnabled(true)
                            .withCircuitBreakerErrorThresholdPercentage(10)
                            .withCircuitBreakerRequestVolumeThreshold(200)
                            .withCircuitBreakerSleepWindowInMilliseconds(8000)
                            .withExecutionTimeoutInMilliseconds(500)
                            .withFallbackIsolationSemaphoreMaxConcurrentRequests(3000)
                            .withExecutionIsolationStrategy(HystrixCommandProperties.ExecutionIsolationStrategy.THREAD))
            .andThreadPoolPropertiesDefaults(
                    HystrixThreadPoolProperties.Setter()
                            .withQueueSizeRejectionThreshold(40)
                            .withCoreSize(20)
                            .withMaxQueueSize(80));

    public final static HystrixCommand.Setter publisherCassandraSetter = HystrixCommand.Setter
            .withGroupKey(HystrixCommandGroupKey.Factory.asKey("PublisherCassandraGroup"))
            .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey("publisher-cassandra-pool"))
            .andCommandPropertiesDefaults(
                    HystrixPropertiesCommandDefault.Setter()
                            .withCircuitBreakerEnabled(true)
                            .withCircuitBreakerErrorThresholdPercentage(20)
                            .withCircuitBreakerRequestVolumeThreshold(10)
                            .withCircuitBreakerSleepWindowInMilliseconds(5000)
                            .withExecutionTimeoutInMilliseconds(300)
                            .withExecutionIsolationStrategy(HystrixCommandProperties.ExecutionIsolationStrategy.THREAD))
            .andThreadPoolPropertiesDefaults(
                    HystrixThreadPoolProperties.Setter()
                            .withCoreSize(20)
                            .withMaxQueueSize(80)
                            .withQueueSizeRejectionThreshold(40));

    public final static HystrixCommand.Setter PUBLISHER_PAGE_CASSANDRA_SETTER = HystrixCommand.Setter
            .withGroupKey(HystrixCommandGroupKey.Factory.asKey("PublisherPageCassandraGroup"))
            .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey("publisher-page-cassandra-pool"))
            .andCommandPropertiesDefaults(
                    HystrixPropertiesCommandDefault.Setter()
                            .withCircuitBreakerEnabled(true)
                            .withCircuitBreakerErrorThresholdPercentage(20)
                            .withCircuitBreakerRequestVolumeThreshold(10)
                            .withCircuitBreakerSleepWindowInMilliseconds(5000)
                            .withExecutionTimeoutInMilliseconds(600)
                            .withExecutionIsolationStrategy(HystrixCommandProperties.ExecutionIsolationStrategy.THREAD))
            .andThreadPoolPropertiesDefaults(
                    HystrixThreadPoolProperties.Setter()
                            .withCoreSize(20)
                            .withMaxQueueSize(80)
                            .withQueueSizeRejectionThreshold(40));


    public final static HystrixCommand.Setter fieldConfSetter = HystrixCommand.Setter
            .withGroupKey(HystrixCommandGroupKey.Factory.asKey("RedisGroup"))
            .andCommandKey(HystrixCommandKey.Factory.asKey("FieldConfSetter"))
            .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey("field-redis-pool"))
            .andCommandPropertiesDefaults(
                    HystrixPropertiesCommandDefault.Setter()
                            .withCircuitBreakerEnabled(true)
                            .withCircuitBreakerErrorThresholdPercentage(20)
                            .withCircuitBreakerRequestVolumeThreshold(10)
                            .withCircuitBreakerSleepWindowInMilliseconds(5000)
                            .withExecutionTimeoutInMilliseconds(500)
                            .withFallbackIsolationSemaphoreMaxConcurrentRequests(2000)
                            .withExecutionIsolationStrategy(HystrixCommandProperties.ExecutionIsolationStrategy.THREAD))
            .andThreadPoolPropertiesDefaults(
                    HystrixThreadPoolProperties.Setter()
                            .withQueueSizeRejectionThreshold(40)
                            .withCoreSize(20)
                            .withMaxQueueSize(80));

    public static <T> void logFallback(HystrixCommand<T> cmd) {
        Throwable e = cmd.getExecutionException();
        List<HystrixEventType> es = cmd.getExecutionEvents();

        String msg = String.format("Command key: %s, ExecutionEvents: %s, ExecutionMessage: %s, Group: %s",
                cmd.getClass().getSimpleName(), es, e, cmd.getCommandGroup());
        System.out.println(msg);

        if (e != null) {
            e.printStackTrace();
        } else {
            NewRelic.noticeError(msg);
        }
    }
}
