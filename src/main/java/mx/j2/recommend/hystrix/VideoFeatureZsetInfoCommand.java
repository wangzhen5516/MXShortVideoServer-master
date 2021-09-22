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
import mx.j2.recommend.util.MXStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author Qi Mao
 * @date 1/20/2021
 */
public class
VideoFeatureZsetInfoCommand extends HystrixCommand<List<String>> {
    private static Logger logger = LogManager.getLogger(VideoFeatureZsetInfoCommand.class);
    private String key;

    private VideoFeatureZsetInfoCommand() {
        super(Setter
                .withGroupKey(HystrixCommandGroupKey.Factory.asKey("RedisGroup"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("VideoFeatureZsetInfoCommand"))
                .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey("vfzuc-redis-poll"))
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

    public VideoFeatureZsetInfoCommand(String key) {
        this();
        this.key = key;
    }

    @Override
    protected List<String> run() throws Exception {
        List<String> resultList = new ArrayList<>();
        if (MXStringUtils.isEmpty(key)) {
            return resultList;
        }

        StatefulRedisClusterConnection<String, String> connection = MXDataSource.redis().getPubFeatureClusterConnection();
        RedisAdvancedClusterAsyncCommands<String, String> cacheJedis = connection.async();

        if (null == cacheJedis) {
            return resultList;
        }

        // 按分数从大到小召回
        RedisFuture<List<ScoredValue<String>>> future = cacheJedis.zrevrangeWithScores(key, 0, -1);
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
        LogTool.printJsonStatusLog(logger, "VideoFeatureZsetInfoCommand redis abnormal. logId", "nullId");

        Throwable e = this.getExecutionException();
        String message = String.format("redis happen exception: %s", e.fillInStackTrace());
        LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        return Collections.emptyList();
    }
}
