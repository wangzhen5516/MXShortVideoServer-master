package mx.j2.recommend.hystrix;

import com.netflix.hystrix.*;
import com.netflix.hystrix.strategy.properties.HystrixPropertiesCommandDefault;
import io.lettuce.core.api.StatefulRedisConnection;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.LogTool;
import mx.j2.recommend.util.MXJudgeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class BloomFilterAddCommand extends HystrixCommand<boolean[]> {
    private static Logger logger = LogManager.getLogger(BloomFilterAddCommand.class);
    private final static long EXPIRE_TIME = 60 * 60 * 24 * 7;
    private final static long BLOOM_SIZE = 100000;
    private final static double ERROR_RATE = 0.001;
    private String key;
    private String[] values;

    public BloomFilterAddCommand() {
        super(Setter
                .withGroupKey(HystrixCommandGroupKey.Factory.asKey("RedisGroup"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("BloomFilterAddCommand"))
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

    public BloomFilterAddCommand(String key, String[] values) {
        this();
        this.key = key;
        this.values = values;
    }

    private void expire(String key) {
        StatefulRedisConnection<String, String> connection = MXDataSource.rebloom().getRedisConnection();
        connection.sync().expire(key, EXPIRE_TIME);
    }

    private boolean exists(String key) {
        StatefulRedisConnection<String, String> connection = MXDataSource.rebloom().getRedisConnection();
        return connection.sync().exists(key) >= 0;
    }

    @Override
    protected boolean[] run() throws Exception {
        if (MXJudgeUtils.isEmpty(values)) {
            return null;
        }
        // create if don't exist
        if (!exists(key)) {
            MXDataSource.rebloom().getMasterClient().createFilter(key, BLOOM_SIZE, ERROR_RATE);
        }
        // expire
        expire(key);
        // add to bloom
        return MXDataSource.rebloom().getMasterClient().addMulti(key, values);
    }

    @Override
    protected boolean[] getFallback() {
        LogTool.printJsonStatusLog(logger, "BloomFilterAddCommand redis abnormal. logId", "nullId");
        return new boolean[values.length];
    }
}
