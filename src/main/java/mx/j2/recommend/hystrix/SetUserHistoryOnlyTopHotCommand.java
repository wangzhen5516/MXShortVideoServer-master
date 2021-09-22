package mx.j2.recommend.hystrix;

import com.netflix.hystrix.*;
import com.netflix.hystrix.strategy.properties.HystrixPropertiesCommandDefault;
import com.newrelic.api.agent.NewRelic;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.LogTool;
import mx.j2.recommend.util.MXJudgeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Set;

public class SetUserHistoryOnlyTopHotCommand extends HystrixCommand<Integer> {
    private static Logger logger = LogManager.getLogger(SetUserHistoryOnlyTopHotCommand.class);
    private String userId;
    private Set<String> resIdList;
    private boolean isVipUser;
    private static final String my_suffix = "_his_new";
    private static final int TOPHOT_HISTORY_SIZE = 5000;
    private static final int EXPIRE_TIME = 10*24*60*60;//10d

    public SetUserHistoryOnlyTopHotCommand() {
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

    public SetUserHistoryOnlyTopHotCommand(String userId, Set<String> resIdList, boolean isVipUser) {
        this();
        this.isVipUser = isVipUser;
        this.userId = userId;
        this.resIdList = resIdList;
    }

    @Override
    protected Integer run() throws Exception {
        if (MXJudgeUtils.isEmpty(resIdList)) {
            return 0;
        }

        int param = 1;
        if (isVipUser) {
            param = 2;
        }

        StatefulRedisClusterConnection<String, String> connection = MXDataSource.redis().getCacheTopHotConnection();
        RedisAdvancedClusterAsyncCommands<String, String> guavaJedis = connection.async();
        long timpStamp = System.currentTimeMillis();
        Object[] scoredValues = new Object[resIdList.size() << 1];
        int i = 0;
        for (String id : resIdList) {
            scoredValues[i] = -(double) timpStamp;
            scoredValues[i + 1] = id;
            i += 2;
        }

        String redisKey = userId + my_suffix;

        if (guavaJedis != null) {
            guavaJedis.zadd(redisKey, scoredValues);
            guavaJedis.zremrangebyrank(redisKey, TOPHOT_HISTORY_SIZE * param, -1);
            guavaJedis.expire(redisKey, EXPIRE_TIME * param);
        }

        return 1;
    }

    @Override
    protected Integer getFallback() {
        LogTool.printJsonStatusLog(logger, "SetUserHistoryOnlyTopHotCommand redis abnormal. logId", "nullId");

        Throwable e = this.getExecutionException();
        String message = String.format("SetUserHistoryOnlyTopHotCommand: %s", e.fillInStackTrace());
        LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        NewRelic.noticeError("SetUserHistoryOnlyTopHotCommand Exception");
        return -1;
    }
}
