package mx.j2.recommend.hystrix;

import com.netflix.hystrix.*;
import com.netflix.hystrix.strategy.properties.HystrixPropertiesCommandDefault;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.LogTool;
import mx.j2.recommend.util.MXJudgeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class GetUserLastHistoryIdCommand extends HystrixCommand<Boolean>{
    private static Logger logger = LogManager.getLogger(GetUserLastHistoryIdCommand.class);
    private BaseDataCollection dc;

    public GetUserLastHistoryIdCommand() {
        super(Setter
                .withGroupKey(HystrixCommandGroupKey.Factory.asKey("RedisGroup"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("GetUserHistoryCommand"))
                .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey("guh-redis-poll"))
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

    public GetUserLastHistoryIdCommand(BaseDataCollection dc) {
        this();
        this.dc=dc;
    }

    @Override
    protected Boolean run() throws Exception {
        StatefulRedisClusterConnection<String, String> connection = MXDataSource.redis().getCacheNewConnection();
        RedisAdvancedClusterAsyncCommands<String, String> cacheJedis = connection.async();

        if (null == cacheJedis) {
            return false;
        }

        RedisFuture<List<ScoredValue<String>>> future = cacheJedis.zrangeWithScores(dc.client.user.userId + Conf.getHistoryIdsSuffix(), 0, 0);
        future.await(Conf.getJedisClusterSocketTimeout(), TimeUnit.MILLISECONDS);
        List<ScoredValue> scoredValues = new ArrayList<>();
        if(future.isDone()) {
            scoredValues.addAll(future.get(Conf.getJedisClusterSocketTimeout(), TimeUnit.MILLISECONDS));
        }

        if (MXJudgeUtils.isNotEmpty(scoredValues)) {
            dc.lastHistoryIdTimeStamp = -(long)(scoredValues.get(0).getScore());
        }
        return true;
    }

    @Override
    protected Boolean getFallback() {
        LogTool.printJsonStatusLog(logger, "GetUserHistoryCommand redis abnormal. logId", "nullId");

        Throwable e = this.getExecutionException();
        String message = String.format("redis happen exception: %s", e.fillInStackTrace());
        LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        return false;
    }
}
