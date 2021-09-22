package mx.j2.recommend.hystrix;

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ZrevRangeStragegyCommand extends HystrixCommand<List<String>> {
    private static Logger logger = LogManager.getLogger(ZrevRangeStragegyCommand.class);
    private final static long EXPIRE_TIME = 60 * 60 * 24 * 7;
    private final static long BLOOM_SIZE = 100000;
    private final static double ERROR_RATE = 0.001;
    private String key;
    private long start;
    private long end;

    private ZrevRangeStragegyCommand() {
        super(Setter
                .withGroupKey(HystrixCommandGroupKey.Factory.asKey("RedisGroup"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("ZrevRangeStragegyCommand"))
                .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey("zrs-redis-poll"))
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

    public ZrevRangeStragegyCommand(String key, long start, long end) {
        this();
        this.key = key;
        this.start = start;
        this.end=end;
    }

    @Override
    protected List<String> run() throws Exception {
        List<String> resultList = new ArrayList<>();

        StatefulRedisClusterConnection<String, String> connection = MXDataSource.redis().getStrategyConnection();
        RedisAdvancedClusterAsyncCommands<String, String> cacheJedis = connection.async();

        if (null == cacheJedis) {
            return resultList;
        }

        // 按分数从大到小召回
        RedisFuture<List<ScoredValue<String>>> future = cacheJedis.zrevrangeWithScores(key, start, end);
        future.await(Conf.getJedisClusterSocketTimeout(), TimeUnit.MILLISECONDS);
        if (future.isDone()) {
            List<ScoredValue<String>> tmp = future.get(Conf.getJedisClusterSocketTimeout(), TimeUnit.MILLISECONDS);
            if (MXJudgeUtils.isNotEmpty(tmp)) {
                tmp.forEach(o -> {
                    resultList.add(o.getValue());
                });
            }
        }
        return resultList;
    }

    @Override
    protected List<String> getFallback() {
        LogTool.printJsonStatusLog(logger, "ZrevRangeStragegyCommand redis abnormal. logId", "nullId");

        Throwable e = this.getExecutionException();
        String message = String.format("redis happen exception: %s", e.fillInStackTrace());
        LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        return Collections.emptyList();
    }
}
