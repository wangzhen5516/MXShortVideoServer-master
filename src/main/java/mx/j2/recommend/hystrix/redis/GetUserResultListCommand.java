package mx.j2.recommend.hystrix.redis;

import com.netflix.hystrix.*;
import com.netflix.hystrix.strategy.properties.HystrixPropertiesCommandDefault;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.thrift.Result;
import mx.j2.recommend.util.LogTool;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.StringSerializationUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class GetUserResultListCommand extends HystrixCommand<Boolean>{
    private static Logger logger = LogManager.getLogger(GetUserResultListCommand.class);
    private BaseDataCollection dc;

    public GetUserResultListCommand() {
        super(Setter
                .withGroupKey(HystrixCommandGroupKey.Factory.asKey("RedisGroup"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("GetUserResultListCommand"))
                .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey("gurl-redis-poll"))
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

    public GetUserResultListCommand(BaseDataCollection dc) {
        this();
        this.dc=dc;
    }

    @Override
    protected Boolean run() throws Exception {

        StatefulRedisClusterConnection<String, String> connection = MXDataSource.redis().getCacheConnection();
        RedisAdvancedClusterAsyncCommands<String, String> cacheJedis = connection.async();

        String keyStr = dc.client.user.uuId + Conf.getRecommendResultCacheSuffix();

        try {
            if (null == cacheJedis) {
                return true;
            }

            RedisFuture<String> future = cacheJedis.get(keyStr);
            future.await(Conf.getJedisClusterSocketTimeout(), TimeUnit.MILLISECONDS);
            if (future.isDone()) {
                String res = future.get();
                if (MXJudgeUtils.isEmpty(res)) {
                    return true;
                }
                dc.cachedResultList = (List<Result>) StringSerializationUtil.deserialize(res);

                if (null == dc.cachedResultList) {
                    dc.cachedResultList = new ArrayList<>();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            String message = String.format("[%s] redis happen exception: %s", this.getClass().getSimpleName(), e.fillInStackTrace());
            LogTool.printErrorLog(logger, "", message, null, null);
        }
        return true;
    }

    @Override
    protected Boolean getFallback() {
        LogTool.printJsonStatusLog(logger, "GetUserResultListCommand redis abnormal. logId", "nullId");
        return false;
    }
}
