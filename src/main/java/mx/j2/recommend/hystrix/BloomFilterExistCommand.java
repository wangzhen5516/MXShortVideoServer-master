package mx.j2.recommend.hystrix;

import com.netflix.hystrix.*;
import com.netflix.hystrix.strategy.properties.HystrixPropertiesCommandDefault;
import io.lettuce.core.api.StatefulRedisConnection;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.LogTool;
import mx.j2.recommend.util.MXJudgeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class BloomFilterExistCommand extends HystrixCommand<boolean[]> {
    private static Logger logger = LogManager.getLogger(BloomFilterExistCommand.class);
    private String key;
    private String[] values;

    public BloomFilterExistCommand() {
        super(HystrixCommand.Setter
                .withGroupKey(HystrixCommandGroupKey.Factory.asKey("RedisGroup"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("BloomFilterExistCommand"))
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

    public BloomFilterExistCommand(String key, String[] values) {
        this();
        this.key = key;
        this.values = values;
    }

    private boolean exists(String key) {
        StatefulRedisConnection<String, String> connection = MXDataSource.rebloom().getRedisConnection();
        return connection.sync().exists(key) >= 0;
    }

    @Override
    protected boolean[] run() throws Exception {
        if (MXJudgeUtils.isEmpty(key) || !exists(key)) {
            return new boolean[values.length];
        }
        if (MXJudgeUtils.isEmpty(values)) {
            return null;
        }
        return MXDataSource.rebloom().getMasterClient().existsMulti(key, values);

    }

    @Override
    protected boolean[] getFallback() {
        LogTool.printJsonStatusLog(logger, "BloomFilterExistCommand redis abnormal. logId", "nullId");
        return new boolean[values.length];
    }
}
