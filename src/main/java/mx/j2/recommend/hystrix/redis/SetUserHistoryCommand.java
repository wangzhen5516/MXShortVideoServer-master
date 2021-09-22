package mx.j2.recommend.hystrix.redis;

import com.netflix.hystrix.*;
import com.netflix.hystrix.strategy.properties.HystrixPropertiesCommandDefault;
import com.newrelic.api.agent.NewRelic;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.LogTool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Set;

public class SetUserHistoryCommand extends HystrixCommand<Void> {
    private static Logger logger = LogManager.getLogger(SetUserHistoryCommand.class);
    private String historyOrderSetKey;
    private Set<String> resIdList;
    private long historySize;
    private boolean isVipUser;

    public SetUserHistoryCommand() {
        super(Setter
                .withGroupKey(HystrixCommandGroupKey.Factory.asKey("RedisGroup"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("SetUserHistoryCommand"))
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

    public SetUserHistoryCommand(String historyOrderSetKey, Set<String> resIdList, long historySize, boolean isVipUser) {
        this();
        this.isVipUser = isVipUser;
        this.historyOrderSetKey = historyOrderSetKey;
        this.resIdList = resIdList;
        this.historySize = historySize;
    }

    @Override
    protected Void run() throws Exception {
        StatefulRedisClusterConnection<String, String> connection = MXDataSource.redis().getCacheNewConnection();
        RedisAdvancedClusterAsyncCommands<String, String> cacheJedis = connection.async();
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

        try {
            if (cacheJedis != null) {
                cacheJedis.zadd(historyOrderSetKey, scoredValues);
                /**
                 *  TODO 2020.8.1加入的新逻辑, 如果历史超上限, 不是从最头上抹掉, 而是从距离头部3000的位置上抹掉
                 *  TODO: FIXME historySize的数量不准确，会导致多次delete
                 *  这里用15%来计算, 原因是为了防止后续20000的上限调整, 引发bug
                 *
                if (Conf.getMaxHistoryListSize() < historySize + resIdList.size()) {
                    long delta = historySize + resIdList.size() - Conf.getMaxHistoryListSize();
                    int start = (int) (Conf.getMaxHistoryListSize() * 0.85);
                    cacheJedis.zremrangebyrank(historyOrderSetKey, start, start + delta);
                }*/

                cacheJedis.zremrangebyrank(historyOrderSetKey, Conf.getMaxHistoryListSize() * param, -1);
                cacheJedis.expire(historyOrderSetKey, Conf.getHistoryIdsExpireTime() * param);
            }
        } catch (Exception e) {
            String message = String.format("SetUserHistoryCommand %s", e.fillInStackTrace());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
            NewRelic.noticeError("SetUserHistoryCommand Exception");
        }
        return null;
    }

    @Override
    protected Void getFallback() {
        LogTool.printJsonStatusLog(logger, "SetUserHistoryCommand redis abnormal. logId", "nullId");
        return null;
    }
}
