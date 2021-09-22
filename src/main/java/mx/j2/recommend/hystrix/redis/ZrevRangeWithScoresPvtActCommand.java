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

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @Author DuoZhao
 * @Date 2021-04-26
 */

public class ZrevRangeWithScoresPvtActCommand extends HystrixCommand<Map<String, Double>> {
    private static final Logger log = LogManager.getLogger(ZrevRangeWithScoresPvtActCommand.class);
    private String key;
    private long start;
    private long end;

    private ZrevRangeWithScoresPvtActCommand() {
        super(Setter
                .withGroupKey(HystrixCommandGroupKey.Factory.asKey("PvRedisGroup"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("ZrevRangeWithScoresPvtActCommand"))
                .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey("pv-redis-poll"))
                .andCommandPropertiesDefaults(
                        HystrixPropertiesCommandDefault.Setter()
                                .withCircuitBreakerEnabled(true)
                                .withCircuitBreakerErrorThresholdPercentage(10)
                                .withCircuitBreakerRequestVolumeThreshold(10)
                                .withCircuitBreakerSleepWindowInMilliseconds(5000)
                                .withExecutionTimeoutInMilliseconds(100)
                                .withFallbackIsolationSemaphoreMaxConcurrentRequests(3000)
                                .withExecutionIsolationStrategy(HystrixCommandProperties.ExecutionIsolationStrategy.THREAD))
                .andThreadPoolPropertiesDefaults(
                        HystrixThreadPoolProperties.Setter()
                                .withQueueSizeRejectionThreshold(40)
                                .withCoreSize(20)
                                .withMaxQueueSize(80))
        );
    }

    public ZrevRangeWithScoresPvtActCommand(String key, long start, long end) {
        this();
        this.key = key;
        this.start = start;
        this.end = end;
    }

    @Override
    protected Map<String, Double> run() throws Exception {
        Map<String, Double> resultMap = new HashMap<>();

        StatefulRedisClusterConnection<String, String> connection = MXDataSource.redis().getPrivateAccountClusterConnection();
        RedisAdvancedClusterAsyncCommands<String, String> cacheJedis = connection.async();

        try {
            if (null == cacheJedis) {
                return resultMap;
            }

            // 按分数从大到小召回
            RedisFuture<List<ScoredValue<String>>> future = cacheJedis.zrevrangeWithScores(key, start, end);
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
            LogTool.printErrorLog(log, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
            return resultMap;
        }
    }

    @Override
    protected Map<String, Double> getFallback() {
        LogTool.printJsonStatusLog(log, "ZrevRangeWithScoresPvtActCommand redis abnormal. logId ", this.key);
        return Collections.emptyMap();
    }
}
