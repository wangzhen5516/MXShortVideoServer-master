package mx.j2.recommend.hystrix.redis;

import com.netflix.hystrix.*;
import com.netflix.hystrix.strategy.properties.HystrixPropertiesCommandDefault;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.LogTool;
import mx.j2.recommend.util.MXJudgeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ZrevRangeWithScoresStragegyCommand extends HystrixCommand<Map<String, Double>> {
    private static Logger logger = LogManager.getLogger(ZrevRangeWithScoresStragegyCommand.class);
    private final static long EXPIRE_TIME = 60 * 60 * 24 * 7;
    private final static long BLOOM_SIZE = 100000;
    private final static double ERROR_RATE = 0.001;
    private String key;
    private long length;

    public ZrevRangeWithScoresStragegyCommand() {
        super(Setter
                .withGroupKey(HystrixCommandGroupKey.Factory.asKey("RedisGroup"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("ZrevRangeWithScoresStragegyCommand"))
                .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey("zrws-redis-poll"))
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

    public ZrevRangeWithScoresStragegyCommand(String key, long length) {
        this();
        this.key = key;
        this.length = length;
    }

    @Override
    protected Map<String, Double> run() throws Exception {
        Map<String, Double> resultMap = new HashMap<>();

        StatefulRedisClusterConnection<String, String> connection = MXDataSource.redis().getStrategyConnection();
        RedisAdvancedClusterAsyncCommands<String, String> cacheJedis = connection.async();

        try {
            if (null == cacheJedis) {
                return resultMap;
            }

            // 按分数从大到小召回
            RedisFuture<List<ScoredValue<String>>> future = cacheJedis.zrevrangeWithScores(key, 0, length);
            future.await(Conf.getJedisClusterSocketTimeout(), TimeUnit.MILLISECONDS);
            if (future.isDone()) {
                List<ScoredValue<String>> tmp = future.get(Conf.getJedisClusterSocketTimeout(), TimeUnit.MILLISECONDS);
                if (MXJudgeUtils.isNotEmpty(tmp)) {
                    tmp.forEach(o -> {
                        resultMap.put(o.getValue(), o.getScore());
                    });
                }
            }
            return resultMap;
        } catch (Exception e) {
            String message = String.format("redis happen exception: %s", e.fillInStackTrace());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
            return resultMap;
        }

    }

    @Override
    protected Map<String, Double> getFallback() {
        LogTool.printJsonStatusLog(logger, "ZrevRangeWithScoresStragegyCommand redis abnormal. logId", "nullId");
        return Collections.emptyMap();
    }
}
