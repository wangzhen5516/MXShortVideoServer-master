package mx.j2.recommend.hystrix.redis;

import com.netflix.hystrix.*;
import com.netflix.hystrix.strategy.properties.HystrixPropertiesCommandDefault;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.HystrixUtil;
import mx.j2.recommend.util.LogTool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;

/**
 * @author qiqi
 * @date 2021-02-04 13:51
 */
public class GetStringStgCommand extends HystrixCommand<String> {
    Logger logger = LogManager.getLogger(GetStringStgCommand.class);

    private String key;

    private GetStringStgCommand() {
        super(Setter
                .withGroupKey(HystrixCommandGroupKey.Factory.asKey("RedisGroup"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("StgStringCommand"))
                .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey("string-redis-poll"))
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

    public GetStringStgCommand(String key) {
        this();
        this.key = key;
    }

    @Override
    protected String run() {
        StatefulRedisClusterConnection<String, String> connection = MXDataSource.redis().getStrategyConnection();
        RedisAdvancedClusterAsyncCommands<String, String> commands = connection.async();

        if (null == commands) {
            return "";
        }

        RedisFuture<String> future = commands.get(key);

        try {
            return future.get(Conf.getJedisClusterSocketTimeout(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            String message = String.format("redis happen exception: %s", e.fillInStackTrace());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
            return "";
        }
    }

    @Override
    protected String getFallback() {
        HystrixUtil.logFallback(this);
        return "";
    }
}
