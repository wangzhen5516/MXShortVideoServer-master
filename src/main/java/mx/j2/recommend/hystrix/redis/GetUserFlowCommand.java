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
import mx.j2.recommend.util.MXStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;

/**
 * @author qiqi
 * @date 2020-09-18 15:43
 */
public class GetUserFlowCommand extends HystrixCommand<String> {

    private final static String KEY_PRE = "_debug_code";

    private static Logger logger = LogManager.getLogger(GetUserFlowCommand.class);
    private BaseDataCollection dc;

    public GetUserFlowCommand(){
        super(HystrixCommand.Setter
                .withGroupKey(HystrixCommandGroupKey.Factory.asKey("RedisGroup"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("GetUserFlowCommand"))
                .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey("guh-redis-poll"))
                .andCommandPropertiesDefaults(
                        HystrixPropertiesCommandDefault.Setter()
                                .withCircuitBreakerEnabled(true)
                                .withCircuitBreakerErrorThresholdPercentage(10)
                                .withCircuitBreakerRequestVolumeThreshold(10)
                                .withCircuitBreakerSleepWindowInMilliseconds(5000)
                                .withExecutionTimeoutInMilliseconds(500)
                                .withFallbackIsolationSemaphoreMaxConcurrentRequests(3000)
                                .withExecutionIsolationStrategy(HystrixCommandProperties.ExecutionIsolationStrategy.THREAD))
                .andThreadPoolPropertiesDefaults(
                        HystrixThreadPoolProperties.Setter()
                                .withQueueSizeRejectionThreshold(40)
                                .withCoreSize(20)
                                .withMaxQueueSize(80))
        );
    }
    public GetUserFlowCommand(BaseDataCollection dc){
        this();
        this.dc=dc;
    }

    @Override
    protected String run() throws Exception {
        StatefulRedisClusterConnection<String, String> connection = MXDataSource.redis().getStrategyConnection();
        RedisAdvancedClusterAsyncCommands<String, String> cacheJedis = connection.async();
        String interfaceName = dc.req.getInterfaceName();
        String uuId = dc.req.getUserInfo().getUuid();
        if (MXStringUtils.isBlank(interfaceName) || MXStringUtils.isBlank(uuId)) {
            return null;
        }
        try{
            RedisFuture<String> future = cacheJedis.hget(interfaceName + KEY_PRE, uuId);
            future.await(Conf.getJedisClusterSocketTimeout(), TimeUnit.MILLISECONDS);
            if (future.isDone()) {
                String flowCode = future.get();
                return flowCode;
            }
        }catch (Exception e){
            String message = String.format("redis happen exception: %s", e.fillInStackTrace());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        }
        return null;
    }
}
