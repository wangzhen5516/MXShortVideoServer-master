package mx.j2.recommend.hystrix;

import com.netflix.hystrix.*;
import com.netflix.hystrix.strategy.properties.HystrixPropertiesCommandDefault;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.LogTool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author zhongrenli
 */
public class GuavaBloomSetCapacityCommand extends HystrixCommand<Boolean> {
    private static Logger logger = LogManager.getLogger(GuavaBloomSetCapacityCommand.class);
    private String key;
    private String value;

    private final static long EXPIRE_TIME = 60 * 60 * 1;

    public GuavaBloomSetCapacityCommand() {
        super(Setter
                .withGroupKey(HystrixCommandGroupKey.Factory.asKey("RedisGroup"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("GuavaBloomSetCommand"))
                .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey("gbs-redis-poll"))
                .andCommandPropertiesDefaults(
                        HystrixPropertiesCommandDefault.Setter()
                                .withCircuitBreakerEnabled(true)
                                .withCircuitBreakerErrorThresholdPercentage(10)
                                .withCircuitBreakerRequestVolumeThreshold(10)
                                .withCircuitBreakerSleepWindowInMilliseconds(5000)
                                .withExecutionTimeoutInMilliseconds(300)
                                .withFallbackIsolationSemaphoreMaxConcurrentRequests(3000)
                                .withExecutionIsolationStrategy(HystrixCommandProperties.ExecutionIsolationStrategy.THREAD))
                .andThreadPoolPropertiesDefaults(
                        HystrixThreadPoolProperties.Setter()
                                .withQueueSizeRejectionThreshold(40)
                                .withCoreSize(20)
                                .withMaxQueueSize(80))
        );
    }

    public GuavaBloomSetCapacityCommand(String key, String value) {
        this();
        this.key = key;
        this.value = value;
    }

    @Override
    protected Boolean run() throws Exception {
        StatefulRedisClusterConnection<String, String> connection = MXDataSource.guavaBloom().getRedisConn();
        connection.async().setex(key, EXPIRE_TIME, value);
        return true;
    }

    @Override
    protected Boolean getFallback() {
        LogTool.printJsonStatusLog(logger, "GuavaBloomSetCommand redis abnormal. logId", "nullId");
        return false;
    }
}
