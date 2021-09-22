package mx.j2.recommend.hystrix;

import com.alibaba.fastjson.JSON;
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

/**
 * @author zhongrenli
 */
public class GetUserHistoryNotTopHotCommand extends HystrixCommand<Boolean>{
    private static Logger logger = LogManager.getLogger(GetUserHistoryNotTopHotCommand.class);
    //TODO: move to CONF and redis, this is the size of small List
    private static final int WATERMARK_SMALL_LIST = 3000;
    private static final int HISTORY_SIZE = 3000;
    private BaseDataCollection dc;
    private static final String my_suffix = "_his_not_top_hot";

    public GetUserHistoryNotTopHotCommand() {
        super(Setter
                .withGroupKey(HystrixCommandGroupKey.Factory.asKey("RedisTophotGroup"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("GetUserHistoryNewCommand"))
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

    public GetUserHistoryNotTopHotCommand(BaseDataCollection dc) {
        this();
        this.dc=dc;
    }

    @Override
    protected Boolean run() throws Exception {
        StatefulRedisClusterConnection<String, String> connection = MXDataSource.redis().getCacheTopHotConnection();
        RedisAdvancedClusterAsyncCommands<String, String> jedis = connection.async();

        if (null == jedis) {
            return false;
        }
        if (null == dc.client.user.uuId) {
            return false;
        }
        int param = 1;
//        if (dc.isDebugModeOpen) {
//            param = 2;
//        }

        RedisFuture<List<String>> future = jedis.zrange(dc.client.user.uuId + my_suffix, 0, HISTORY_SIZE);
        future.await(Conf.getJedisClusterSocketTimeout() * param, TimeUnit.MILLISECONDS);
        if(future.isDone()) {
            List<String> hisNewList = future.get(Conf.getJedisClusterSocketTimeout(), TimeUnit.MILLISECONDS);
            dc.historyIdList.addAll(hisNewList);
        }

        return true;
    }

    @Override
    protected Boolean getFallback() {
        logger.error(JSON.toJSONString("GetUserHistoryNotTopHotCommand redis abnormal. fallback"));
        Throwable e = this.getExecutionException();
        String message = String.format("redis happen exception: %s", e.fillInStackTrace());
        LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        return false;
    }
}
