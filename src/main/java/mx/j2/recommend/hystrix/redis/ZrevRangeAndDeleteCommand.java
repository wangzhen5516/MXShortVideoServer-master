package mx.j2.recommend.hystrix.redis;

import com.netflix.hystrix.*;
import com.netflix.hystrix.strategy.properties.HystrixPropertiesCommandDefault;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.LogTool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class ZrevRangeAndDeleteCommand extends HystrixCommand<List<String>> {
    private static Logger logger = LogManager.getLogger(ZrevRangeAndDeleteCommand.class);
    private String key;

    public ZrevRangeAndDeleteCommand() {
        super(Setter
                .withGroupKey(HystrixCommandGroupKey.Factory.asKey("RedisGroup"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("ZrevRangeAndDeleteCommand"))
                .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey("zr-redis-poll"))
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
                                .withMaxQueueSize(80))
        );
    }

    public ZrevRangeAndDeleteCommand(String key) {
        this();
        this.key = key;
    }

    @Override
    protected List<String> run() throws Exception {
        List<String> videoIds = new ArrayList<>();

        StatefulRedisClusterConnection<String, String> connection = MXDataSource.redis().getPrivateAccountClusterConnection();
        RedisAdvancedClusterAsyncCommands<String, String> cacheJedis = connection.async();

        try {
            if (null == cacheJedis) {
                return videoIds;
            }

            // 按分数从大到小召回
            RedisFuture<List<String>> future = cacheJedis.zrevrange(key, 0, -1);
            future.await(Conf.getJedisClusterSocketTimeout(), TimeUnit.MILLISECONDS);
            if (future.isDone()) {
                videoIds = future.get(Conf.getJedisClusterSocketTimeout(), TimeUnit.MILLISECONDS);
            }
            RedisFuture<Long> delFuture = cacheJedis.del(key);
            delFuture.await(Conf.getJedisClusterSocketTimeout(), TimeUnit.MILLISECONDS);
            return videoIds;
        } catch (Exception e) {
            String message = String.format("redis happen exception: %s", e.fillInStackTrace());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
            return videoIds;
        }
    }

    @Override
    protected List<String> getFallback() {
        LogTool.printJsonStatusLog(logger, "ZrevRangeAndDeleteCommand redis abnormal. logId", "nullId");
        return Collections.emptyList();
    }
}
