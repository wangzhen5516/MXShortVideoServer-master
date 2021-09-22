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

import java.util.concurrent.TimeUnit;

/**
 * @author qiqi
 * @date 2020-11-28 17:20
 */
public class GetVideoNumOfPubCommand extends HystrixCommand<String> {

    private static Logger logger = LogManager.getLogger(GetVideoNumOfPubCommand.class);

    /**
     * 缓存key
     */
    private String cacheKey;

    private GetVideoNumOfPubCommand() {
        super(Setter
                .withGroupKey(HystrixCommandGroupKey.Factory.asKey("RedisGroup"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("GetVideoNumOfPubCommand"))
                .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey("surl-redis-poll"))
                .andCommandPropertiesDefaults(
                        HystrixPropertiesCommandDefault.Setter()
                                .withCircuitBreakerEnabled(true)
                                .withCircuitBreakerErrorThresholdPercentage(10)
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

    public GetVideoNumOfPubCommand(String cacheKey) {
        this();
        this.cacheKey = cacheKey;
    }


    @Override
    public String run() throws Exception {
        try {
            StatefulRedisClusterConnection<String, String> connection = MXDataSource.redis().getCacheConnection();
            RedisAdvancedClusterAsyncCommands<String, String> cacheJedis = connection.async();
            if (cacheJedis == null) {
                return null;
            }
            RedisFuture<String> future = cacheJedis.get(cacheKey);
            future.await(Conf.getJedisClusterSocketTimeout(), TimeUnit.MILLISECONDS);
            if (future.isDone()) {
                return future.get();
            }
        } catch (Exception e) {
            e.printStackTrace();
            String message = String.format("[%s] redis happen exception: %s", this.getClass().getSimpleName(), e.fillInStackTrace());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        }
        return "";
    }
}
