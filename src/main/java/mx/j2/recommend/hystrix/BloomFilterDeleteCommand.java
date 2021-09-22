package mx.j2.recommend.hystrix;

import com.netflix.hystrix.*;
import com.netflix.hystrix.strategy.properties.HystrixPropertiesCommandDefault;
import io.lettuce.core.api.StatefulRedisConnection;
import mx.j2.recommend.manager.MXDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class BloomFilterDeleteCommand extends HystrixCommand<Long> {
    private static Logger logger = LogManager.getLogger(BloomFilterDeleteCommand.class);
    private String key;
    public BloomFilterDeleteCommand(String key) {
        this();
        this.key = key;
    }

    private BloomFilterDeleteCommand() {
        super(Setter
                .withGroupKey(HystrixCommandGroupKey.Factory.asKey("RedisGroup"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("BloomFilterDelCommand"))
                .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey("bfa-redis-poll"))
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

    @Override
    protected Long run() throws Exception {
        StatefulRedisConnection<String, String> connection = MXDataSource.rebloom().getRedisConnection();
        return connection.sync().del(this.key);
    }

    @Override
    protected Long getFallback() {
        logger.error("Bloom Filter Delete Failed.");
        return -1L;
    }
}
