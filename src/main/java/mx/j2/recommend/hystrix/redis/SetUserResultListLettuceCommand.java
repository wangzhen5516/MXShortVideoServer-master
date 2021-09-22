package mx.j2.recommend.hystrix.redis;

import com.netflix.hystrix.*;
import com.netflix.hystrix.strategy.properties.HystrixPropertiesCommandDefault;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.LogTool;
import mx.j2.recommend.util.MXJudgeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;

public class SetUserResultListLettuceCommand extends HystrixCommand<Void> {
    private static Logger logger = LogManager.getLogger(SetUserResultListLettuceCommand.class);
    private BaseDataCollection dc;
    private static final long expireTime = Conf.getRecommendResultCacheExpireTime();

    public SetUserResultListLettuceCommand() {
        super(Setter
                .withGroupKey(HystrixCommandGroupKey.Factory.asKey("RedisGroup"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("SetUserResultListCommand"))
                .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey("surl-redis-poll"))
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

    public SetUserResultListLettuceCommand(BaseDataCollection dc) {
        this();
        this.dc = dc;
    }

    @Override
    protected Void run() throws Exception {
        String langKey = "";
        if (null != dc.req && dc.req.isSetLanguageList() && MXJudgeUtils.isNotEmpty(dc.req.languageList)) {
            langKey = dc.req.languageList.get(0);
        }
        String keyStr = dc.client.user.uuId + Conf.getRecommendResultCacheSuffix() + langKey;

        try {
            MXDataSource.redis().getUserResultCache().put(
                    keyStr,
                    dc.cachedResultList,
                    Conf.getRecommendResultCacheExpireTime(),
                    TimeUnit.SECONDS
            );
        } catch (Exception e) {
            e.printStackTrace();
            String message = String.format("[%s] redis happen exception: %s", this.getClass().getSimpleName(), e.fillInStackTrace());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        }
        return null;
    }

    @Override
    protected Void getFallback() {
        LogTool.printJsonStatusLog(logger, "SetUserResultListLettuceCommand redis abnormal. logId", "nullId");
        return null;
    }
}
