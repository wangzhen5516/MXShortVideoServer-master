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
import mx.j2.recommend.util.MXJudgeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class PrivateAccountSetCommand extends HystrixCommand<Set<String>> {
    private static final Logger logger = LogManager.getLogger(PrivateAccountSetCommand.class);
    private String key;

    private PrivateAccountSetCommand() {
        super(Setter
                .withGroupKey(HystrixCommandGroupKey.Factory.asKey("RedisGroup"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("PrivateAccountSetCommand"))
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

    public PrivateAccountSetCommand(String key) {
        this();
        this.key = key;
    }

    @Override
    protected Set<String> run() throws Exception {
        Set<String> privateAccountSet = new HashSet<>();

        StatefulRedisClusterConnection<String, String> connection = MXDataSource.redis().getPrivateAccountClusterConnection();
        RedisAdvancedClusterAsyncCommands<String, String> cacheJedis = connection.async();

        if (null == cacheJedis) {
            return privateAccountSet;
        }
        if (null == key) {
            return privateAccountSet;
        }
        RedisFuture<Set<String>> future = cacheJedis.smembers(key);
        future.await(Conf.getJedisClusterSocketTimeout(), TimeUnit.MILLISECONDS);
        if(future.isDone()) {
            Set<String> tmp = future.get(Conf.getJedisClusterSocketTimeout(), TimeUnit.MILLISECONDS);
            if (MXJudgeUtils.isNotEmpty(tmp)) {
                privateAccountSet.addAll(tmp);
            }
        }
        return privateAccountSet;
    }

    @Override
    protected Set<String> getFallback() {
        LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), "PrivateAccountSetCommand happen exception", null, (Object) null);

        Throwable e = this.getExecutionException();
        String message = String.format("PrivateAccountSetCommand redis happen exception: %s", e.fillInStackTrace());
        LogTool.printJsonStatusLog(logger, message, "nullId");
        return Collections.emptySet();
    }
}
