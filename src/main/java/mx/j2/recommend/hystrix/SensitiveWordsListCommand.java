package mx.j2.recommend.hystrix;

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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class SensitiveWordsListCommand extends HystrixCommand<Set<String>> {
    private static final Logger logger = LogManager.getLogger(SensitiveWordsListCommand.class);
    private String key;

    private SensitiveWordsListCommand() {
        super(Setter
                .withGroupKey(HystrixCommandGroupKey.Factory.asKey("RedisGroup"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("SensitiveWordsListCommand"))
                .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey("sm-redis-poll"))
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

    public SensitiveWordsListCommand(String key) {
        this();
        this.key = key;
    }

    @Override
    protected Set<String> run() throws Exception {
        Set<String> sensitiveWordsList = new HashSet<>();

        StatefulRedisClusterConnection<String, String> connection = MXDataSource.redis().getPrivateAccountClusterConnection();
        RedisAdvancedClusterAsyncCommands<String, String> cacheJedis = connection.async();

        try {
            if (null == cacheJedis) {
                return sensitiveWordsList;
            }
            if (null == key) {
                return sensitiveWordsList;
            }
            RedisFuture<Set<String>> future = cacheJedis.smembers(key);
            future.await(Conf.getJedisClusterSocketTimeout(), TimeUnit.MILLISECONDS);
            if(future.isDone()) {
                Set<String> tmp = future.get(Conf.getJedisClusterSocketTimeout(), TimeUnit.MILLISECONDS);
                if (tmp != null && !tmp.isEmpty()) {
                    sensitiveWordsList.addAll(tmp);
                }
            }
            return sensitiveWordsList;
        } catch (Exception e) {
            String message = String.format("SensitiveWordsListCommand redis happen exception: %s", e.fillInStackTrace());
            LogTool.printJsonStatusLog(logger, message, "nullId");
            return sensitiveWordsList;
        }
    }

    @Override
    protected Set<String> getFallback() {
        LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), "SensitiveWordsListCommand happen exception", null, (Object) null);
        return Collections.emptySet();
    }
}
