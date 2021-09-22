package mx.j2.recommend.hystrix;

import com.netflix.hystrix.*;
import com.netflix.hystrix.strategy.properties.HystrixPropertiesCommandDefault;
import com.newrelic.api.agent.NewRelic;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.LogTool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Set;

public class SetUserHistoryNewCommand extends HystrixCommand<Integer> {
    private static Logger logger = LogManager.getLogger(SetUserHistoryNewCommand.class);
    private String userId;
    private Set<String> resIdList;
    private boolean isVipUser;
    private static final String my_suffix = "_his_new";
    private static final int EXPIRE_TIME = 60*60*24*2;

    public SetUserHistoryNewCommand() {
        super(Setter
                .withGroupKey(HystrixCommandGroupKey.Factory.asKey("RedisGuavaGroup"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("SetUserHistoryNewCommand"))
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

    public SetUserHistoryNewCommand(String userId, Set<String> resIdList, boolean isVipUser) {
        this();
        this.isVipUser = isVipUser;
        this.userId = userId;
        this.resIdList = resIdList;
    }

    @Override
    protected Integer run() throws Exception {
        StatefulRedisClusterConnection<String, String> connection = MXDataSource.guavaBloom().getRedisConn();
        RedisAdvancedClusterAsyncCommands<String, String> guavaJedis = connection.async();
        long timpStamp = System.currentTimeMillis();
        Object[] scoredValues = new Object[resIdList.size() << 1];
        int i = 0;
        for (String id : resIdList) {
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
            RedisFuture<Long> result = guavaJedis.zadd(redisKey, scoredValues);
            Long resultSize = result.get();
            if(result.get() < resIdList.size()) {
                logger.error("SetUserHistoryNewCommand find the history may lost, the size of success is " + result.get());
            }
            guavaJedis.zremrangebyrank(redisKey, Conf.getMaxHistoryListSize() * param, -1);
            guavaJedis.expire(redisKey, EXPIRE_TIME * param);
            return resultSize.intValue();
        }
        return -1;
    }

    @Override
    protected Integer getFallback() {
        LogTool.printJsonStatusLog(logger, "SetUserHistoryNewCommand redis abnormal. logId", "nullId");

        Throwable e = this.getExecutionException();
        String message = String.format("SetUserHistoryNewCommand : %s", e.fillInStackTrace());
        LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        NewRelic.noticeError("SetUserHistoryNewCommand Exception");
        return -1;
    }
}
