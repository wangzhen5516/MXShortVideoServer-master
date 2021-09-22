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
import mx.j2.recommend.util.MXJudgeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class GetUserHistoryNewCommand extends HystrixCommand<Boolean>{
    private static Logger logger = LogManager.getLogger(GetUserHistoryNewCommand.class);
    //TODO: move to CONF and redis, this is the size of small List
    private static final int WATERMARK_SMALL_LIST = 3000;
    private BaseDataCollection dc;
    private static final String my_suffix = "_his_new";

    public GetUserHistoryNewCommand() {
        super(Setter
                .withGroupKey(HystrixCommandGroupKey.Factory.asKey("RedisGuavaGroup"))
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

    public GetUserHistoryNewCommand(BaseDataCollection dc) {
        this();
        this.dc=dc;
    }

    @Override
    protected Boolean run() throws Exception {
        StatefulRedisClusterConnection<String, String> connection = MXDataSource.guavaBloom().getRedisConn();
        RedisAdvancedClusterAsyncCommands<String, String> guavaJedis = connection.async();

        if (null == guavaJedis) {
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
            RedisFuture<List<String>> future3 = guavaJedis.zrange(dc.client.user.userId + my_suffix, 0, Conf.getMaxHistoryListSize() * param);
            future3.await(Conf.getJedisClusterSocketTimeout() * param, TimeUnit.MILLISECONDS);
            if(future3.isDone()) {
                List<String> hisNewList = future3.get(Conf.getJedisClusterSocketTimeout(), TimeUnit.MILLISECONDS);
                dc.historyIdList.addAll(hisNewList);
                if (MXJudgeUtils.isNotEmpty(hisNewList) && hisNewList.size() > WATERMARK_SMALL_LIST) {
                    //update at 2020.12.5, don't need
                    //DataSourceManager.INSTANCE.getaWSSnsDataSource().send(dc.userId);
                }
            }
        }
        return true;
    }

    @Override
    protected Boolean getFallback() {
        logger.error(JSON.toJSONString("GetUserHistoryNewCommand redis abnormal. fallback"));

        Throwable e = this.getExecutionException();
        String message = String.format("redis happen exception: %s", e.fillInStackTrace());
        LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        return false;
    }
}
