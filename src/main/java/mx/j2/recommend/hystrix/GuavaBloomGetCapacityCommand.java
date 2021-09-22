package mx.j2.recommend.hystrix;

import com.netflix.hystrix.*;
import com.netflix.hystrix.strategy.properties.HystrixPropertiesCommandDefault;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.LogTool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;

/**
 * @author zhongrenli
 */
public class GuavaBloomGetCapacityCommand extends HystrixCommand<Integer> {
    private static Logger logger = LogManager.getLogger(GuavaBloomGetCapacityCommand.class);
    private String key;

    public GuavaBloomGetCapacityCommand() {
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

    public GuavaBloomGetCapacityCommand(String key) {
        this();
        this.key = key;
    }

    @Override
    protected Integer run() throws Exception {
        StatefulRedisClusterConnection<String, String> connection = MXDataSource.guavaBloom().getRedisConn();
        RedisFuture<String> future = connection.async().get(key);
        future.await(200, TimeUnit.MILLISECONDS);
        if (future.isDone()) {
            return Integer.valueOf(future.get());
        }
        return 0;
    }

    @Override
    protected Integer getFallback() {
        LogTool.printJsonStatusLog(logger, "GuavaBloomSetCommand redis abnormal. logId", "nullId");
        return 0;
    }
}
