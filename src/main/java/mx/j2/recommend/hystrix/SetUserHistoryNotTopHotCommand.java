package mx.j2.recommend.hystrix;

import com.netflix.hystrix.*;
import com.netflix.hystrix.strategy.properties.HystrixPropertiesCommandDefault;
import com.newrelic.api.agent.NewRelic;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.LogTool;
import mx.j2.recommend.util.MXJudgeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Set;

/**
 * @author zhongrenli
 */
public class SetUserHistoryNotTopHotCommand extends HystrixCommand<Void> {
    private static Logger logger = LogManager.getLogger(SetUserHistoryNotTopHotCommand.class);
    private String userId;
    private Set<String> idSet;
    private boolean isVipUser;
    private static final String my_suffix = "_his_not_top_hot";
    private static final int TOP_HOT_HISTORY_SIZE = 3000;

    public SetUserHistoryNotTopHotCommand() {
        super(Setter
                .withGroupKey(HystrixCommandGroupKey.Factory.asKey("RedisTophotGroup"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("SetUserHistoryOnlyTopHotCommand"))
                .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey("suh-redis-poll"))
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

    public SetUserHistoryNotTopHotCommand(String userId, Set<String> resIdList, boolean isVipUser) {
        this();
        this.isVipUser = isVipUser;
        this.userId = userId;
        this.idSet = resIdList;
    }

    @Override
    protected Void run() throws Exception {
        if(MXJudgeUtils.isEmpty(idSet)) {
            return null;
        }

        StatefulRedisClusterConnection<String, String> connection = MXDataSource.redis().getCacheTopHotConnection();
        RedisAdvancedClusterAsyncCommands<String, String> guavaJedis = connection.async();
        long timpStamp = System.currentTimeMillis();
        Object[] scoredValues = new Object[idSet.size() << 1];
        int i = 0;
        for (String id : idSet) {
            scoredValues[i] = -(double) timpStamp;
            scoredValues[i + 1] = id;
            i += 2;
        }

        int param = 1;
        if (isVipUser) {
            param = 2;
        }

        String redisKey = userId + my_suffix;
        if (guavaJedis != null) {
            guavaJedis.zadd(redisKey, scoredValues);
            guavaJedis.zremrangebyrank(redisKey, TOP_HOT_HISTORY_SIZE * param, -1);
            guavaJedis.expire(redisKey, Conf.getHistoryIdsExpireTime() * param);
        }

        return null;
    }

    @Override
    protected Void getFallback() {
        LogTool.printJsonStatusLog(logger, "SetUserHistoryNotTopHotCommand redis abnormal. logId", "nullId");

        Throwable e = this.getExecutionException();
        String message = String.format("SetUserHistoryNotTopHotCommand: %s", e.fillInStackTrace());
        LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        NewRelic.noticeError("SetUserHistoryNotTopHotCommand Exception");
        return null;
    }
}
