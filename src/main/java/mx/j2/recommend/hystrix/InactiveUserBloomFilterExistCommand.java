package mx.j2.recommend.hystrix;

import com.netflix.hystrix.*;
import com.netflix.hystrix.strategy.properties.HystrixPropertiesCommandDefault;
import io.lettuce.core.api.StatefulRedisConnection;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.LogTool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author zhongrenli
 */
public class InactiveUserBloomFilterExistCommand extends HystrixCommand<Boolean> {
    private static Logger logger = LogManager.getLogger(InactiveUserBloomFilterExistCommand.class);
    private String key;
    private String value;

    public InactiveUserBloomFilterExistCommand() {
        super(Setter
                .withGroupKey(HystrixCommandGroupKey.Factory.asKey("RedisGroup"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("BloomFilterExistCommand"))
                .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey("bfe-redis-poll"))
                .andCommandPropertiesDefaults(
                        HystrixPropertiesCommandDefault.Setter()
                                .withCircuitBreakerEnabled(true)
                                .withCircuitBreakerErrorThresholdPercentage(20)
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

    public InactiveUserBloomFilterExistCommand(String key, String value) {
        this();
        this.key = key;
        this.value = value;
    }

    @Override
    protected Boolean run() throws Exception {
        StatefulRedisConnection<String, String> connection = MXDataSource.inactiveHistoryBloom().getRedisConnection();
        return connection.sync().exists(key) >= 0;

    }

    @Override
    protected Boolean getFallback() {
        LogTool.printJsonStatusLog(logger, "InactiveUserBloomFilterExistCommand redis abnormal. logId", "nullId");
        return false;
    }
}
