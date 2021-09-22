package mx.j2.recommend.hystrix;

import com.netflix.hystrix.*;
import com.netflix.hystrix.strategy.properties.HystrixPropertiesCommandDefault;
import io.lettuce.core.api.StatefulRedisConnection;
import io.rebloom.client.Client;
import mx.j2.recommend.util.LogTool;
import mx.j2.recommend.util.MXJudgeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class BloomFilterExistSingleCommand extends HystrixCommand<Boolean> {
    private static Logger logger = LogManager.getLogger(BloomFilterExistSingleCommand.class);
    private String key;
    private String value;
    private StatefulRedisConnection<String, String> connection;
    private Client client;

    private BloomFilterExistSingleCommand() {
        super(Setter
                .withGroupKey(HystrixCommandGroupKey.Factory.asKey("RedisGroup"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("BloomFilterExistSingleCommand"))
                .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey("bfe-redis-poll"))
                .andCommandPropertiesDefaults(
                        HystrixPropertiesCommandDefault.Setter()
                                .withCircuitBreakerEnabled(true)
                                .withCircuitBreakerErrorThresholdPercentage(20)
                                .withCircuitBreakerRequestVolumeThreshold(10)
                                .withCircuitBreakerSleepWindowInMilliseconds(5000)
                                .withExecutionTimeoutInMilliseconds(800)
                                .withFallbackIsolationSemaphoreMaxConcurrentRequests(2000)
                                .withExecutionIsolationStrategy(HystrixCommandProperties.ExecutionIsolationStrategy.THREAD))
                .andThreadPoolPropertiesDefaults(
                        HystrixThreadPoolProperties.Setter()
                                .withQueueSizeRejectionThreshold(40)
                                .withCoreSize(20)
                                .withMaxQueueSize(80))
        );
    }

    public BloomFilterExistSingleCommand(String key,
                                         String value,
                                         StatefulRedisConnection<String, String> connection,
                                         Client client) {
        this();
        this.key = key;
        this.value = value;
        this.connection = connection;
        this.client = client;
    }

    private boolean exists(String key) {
        return connection.sync().exists(key) >= 0;
    }

    @Override
    protected Boolean run() throws Exception {
        if (MXJudgeUtils.isEmpty(key)
                || MXJudgeUtils.isEmpty(value)
                || !exists(key)
                || connection == null
                || client == null) {
            return false;
        }

        return client.exists(key, value);

    }

    @Override
    protected Boolean getFallback() {
        LogTool.printJsonStatusLog(logger, "BloomFilterExistSingleCommand redis abnormal. logId", "nullId");
        return false;
    }
}
