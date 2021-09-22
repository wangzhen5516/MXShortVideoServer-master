package mx.j2.recommend.hystrix.redis;

import com.netflix.hystrix.*;
import com.netflix.hystrix.strategy.properties.HystrixPropertiesCommandDefault;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.LogTool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author qiqi
 * @date 2020-11-28 16:43
 */
public class SetVideoNumOfPubCommand extends HystrixCommand<Void> {

    private static Logger logger = LogManager.getLogger(SetVideoNumOfPubCommand.class);
    /**
     * 缓存过期时间
     */
    private static final long EXPRIE_TIME = Conf.getRecommendVideoNumExpireTime();

    /**
     * 请求的key
     */
    private String cacheKey;
    /**
     * 缓存的结果
     */
    private String cacheValue;


    private SetVideoNumOfPubCommand() {
        super(Setter
                .withGroupKey(HystrixCommandGroupKey.Factory.asKey("RedisGroup"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("SetVideoNumOfPubCommand"))
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

    public SetVideoNumOfPubCommand(String cacheKey, String cacheValue) {
        this();
        this.cacheKey = cacheKey;
        this.cacheValue = cacheValue;
    }

    @Override
    public Void run() throws Exception {
        try {
            StatefulRedisClusterConnection<String, String> connection = MXDataSource.redis().getCacheConnection();
            RedisAdvancedClusterAsyncCommands<String, String> cacheJedis = connection.async();
            if (cacheJedis == null) {
                return null;
            }
            cacheJedis.setex(cacheKey, EXPRIE_TIME, cacheValue);
        } catch (Exception e) {
            e.printStackTrace();
            String message = String.format("[%s] redis happen exception: %s", this.getClass().getSimpleName(), e.fillInStackTrace());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        }
        return null;
    }

}
