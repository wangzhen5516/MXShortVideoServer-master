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

public class HGetAllBindAudioCommand extends HystrixCommand<Map<String, String>> {
    private static final Logger logger = LogManager.getLogger(mx.j2.recommend.hystrix.redis.HGetAllBindAudioCommand.class);
    private String key;

    private HGetAllBindAudioCommand() {
        super(Setter
                .withGroupKey(HystrixCommandGroupKey.Factory.asKey("RedisGroup"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("HGetAllBindAudioCommand"))
                .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey("hgetall-redis-poll"))
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

    public HGetAllBindAudioCommand(String key) {
        this();
        this.key = key;
    }

    @Override
    protected Map<String, String> run() throws Exception {
        Map<String, String> bindAudioMap = new HashMap<>();

        StatefulRedisClusterConnection<String, String> connection = MXDataSource.redis().getPubFeatureClusterConnection();
        RedisAdvancedClusterAsyncCommands<String, String> cacheJedis = connection.async();

        try {
            if (null == cacheJedis) {
                return bindAudioMap;
            }
            if (null == key) {
                return bindAudioMap;
            }
            RedisFuture<Map<String, String>> future = cacheJedis.hgetall(key);
            future.await(Conf.getJedisClusterSocketTimeout(), TimeUnit.MILLISECONDS);
            if(future.isDone()) {
                Map<String, String> tmp = future.get(Conf.getJedisClusterSocketTimeout(), TimeUnit.MILLISECONDS);
                if (tmp != null && !tmp.isEmpty()) {
                    bindAudioMap.putAll(tmp);
                }
            }
            return bindAudioMap;
        } catch (Exception e) {
            String message = String.format("HGetAllBindAudioCommand redis happen exception: %s", e.fillInStackTrace());
            LogTool.printJsonStatusLog(logger, message, "nullId");
            return bindAudioMap;
        }
    }

    @Override
    protected Map<String, String> getFallback() {
        LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), "HGetAllBindAudioCommand happen exception", null, (Object) null);
        return Collections.emptyMap();
    }
}
