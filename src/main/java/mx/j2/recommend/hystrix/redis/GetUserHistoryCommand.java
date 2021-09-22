package mx.j2.recommend.hystrix.redis;

import com.netflix.hystrix.*;
import com.netflix.hystrix.strategy.properties.HystrixPropertiesCommandDefault;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.LogTool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class GetUserHistoryCommand extends HystrixCommand<Boolean>{
    private static Logger logger = LogManager.getLogger(GetUserHistoryCommand.class);
    //TODO: move to CONF and redis, this is the size of small List
    private static final int WATERMARK_SMALL_LIST = 3000;
    private BaseDataCollection dc;

    public GetUserHistoryCommand() {
        super(HystrixCommand.Setter
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

    public GetUserHistoryCommand(BaseDataCollection dc) {
        this();
        this.dc=dc;
    }

    @Override
    protected Boolean run() throws Exception {
        StatefulRedisClusterConnection<String, String> connectionNew = MXDataSource.redis().getCacheNewConnection();
        RedisAdvancedClusterAsyncCommands<String, String> cacheNewJedis = connectionNew.async();

        String historyOrderSetKey;
        historyOrderSetKey = dc.client.user.uuId + Conf.getHistoryIdsSuffix();

        if (null == cacheNewJedis) {
            return false;
        }
        if (null == dc.client.user.uuId) {
            return false;
        }
        int param = 1;
//        if (dc.isDebugModeOpen) {
//            param = 2;
//        }

        //从userId中读取
        if(dc.client.user.isLogined) {
            RedisFuture<List<String>> future3 = cacheNewJedis.zrange(dc.client.user.userId + Conf.getHistoryIdsSuffix(), 0, -1);
            future3.await(Conf.getJedisClusterSocketTimeout() * param, TimeUnit.MILLISECONDS);
            if(future3.isDone()) {
                dc.historyIdList.addAll(future3.get(Conf.getJedisClusterSocketTimeout(), TimeUnit.MILLISECONDS));
            }
        }

        if(dc.client.user.isHaveMachineID) {
            RedisFuture<List<String>> future4 = cacheNewJedis.zrange(dc.client.user.adId + Conf.getHistoryIdsSuffix(), 0, -1);
            future4.await(Conf.getJedisClusterSocketTimeout(), TimeUnit.MILLISECONDS);
            if(future4.isDone()) {
                dc.historyIdList.addAll(future4.get(Conf.getJedisClusterSocketTimeout(), TimeUnit.MILLISECONDS));
            }
        }

        //TODO: remove this UUID logic, after 2020.12.15
        RedisFuture<List<String>> future2;
        future2 = cacheNewJedis.zrange(historyOrderSetKey, 0, -1);

        future2.await(Conf.getJedisClusterSocketTimeout(), TimeUnit.MILLISECONDS);
        if (future2.isDone()) {
            dc.historyIdList.addAll(future2.get(Conf.getJedisClusterSocketTimeout(), TimeUnit.MILLISECONDS));
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
