package mx.j2.recommend.hystrix;

import com.netflix.hystrix.*;
import com.netflix.hystrix.strategy.properties.HystrixPropertiesCommandDefault;
import com.newrelic.api.agent.NewRelic;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.LogTool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Set;

public class PrivateAccountZAddCommand extends HystrixCommand<Void> {
    private static Logger logger = LogManager.getLogger(PrivateAccountZAddCommand.class);
    private String redisKey;
    private Set<String> resIdList;
    private int size;
    private long expireTime;

    public PrivateAccountZAddCommand() {
        super(Setter
                .withGroupKey(HystrixCommandGroupKey.Factory.asKey("PvRedisGroup"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("PrivateAccountZAddCommand"))
                .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey("paz-redis-poll"))
                .andCommandPropertiesDefaults(
                        HystrixPropertiesCommandDefault.Setter()
                                .withCircuitBreakerEnabled(true)
                                .withCircuitBreakerErrorThresholdPercentage(20)
                                .withCircuitBreakerRequestVolumeThreshold(10)
                                .withCircuitBreakerSleepWindowInMilliseconds(5000)
                                .withExecutionTimeoutInMilliseconds(600)
                                .withFallbackIsolationSemaphoreMaxConcurrentRequests(2000)
                                .withExecutionIsolationStrategy(HystrixCommandProperties.ExecutionIsolationStrategy.THREAD))
                .andThreadPoolPropertiesDefaults(
                        HystrixThreadPoolProperties.Setter()
                                .withQueueSizeRejectionThreshold(40)
                                .withCoreSize(20)
                                .withMaxQueueSize(80))
        );
    }

    public PrivateAccountZAddCommand(String redisKey, Set<String> resIdList, int size, long expireTime) {
        this();
        this.redisKey = redisKey;
        this.resIdList = resIdList;
        this.size = size;
        this.expireTime = expireTime;
    }

    @Override
    protected Void run() throws Exception {

        StatefulRedisClusterConnection<String, String> connection = MXDataSource.redis().getPrivateAccountClusterConnection();
        RedisAdvancedClusterAsyncCommands<String, String> cacheJedis = connection.async();
        long timpStamp = System.currentTimeMillis();
        Object[] scoredValues = new Object[resIdList.size() << 1];
        int i = 0;
        for (String id : resIdList) {
            scoredValues[i] = -(double) timpStamp;
            scoredValues[i + 1] = id;
            i += 2;
        }

        if (cacheJedis != null) {
            cacheJedis.zadd(redisKey, scoredValues);
            cacheJedis.zremrangebyrank(redisKey, size, -1);
            cacheJedis.expire(redisKey, expireTime);
        }
        return null;
    }

    @Override
    protected Void getFallback() {
        LogTool.printJsonStatusLog(logger, "PrivateAccountZAddCommand redis abnormal. logId", "nullId");

        Throwable e = this.getExecutionException();
        String message = String.format("PrivateAccountZAddCommand : %s", e.fillInStackTrace());
        LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        NewRelic.noticeError("PrivateAccountZAddCommand Exception");
        return null;
    }
}
