package mx.j2.recommend.hystrix;

import com.netflix.hystrix.*;
import com.netflix.hystrix.strategy.properties.HystrixPropertiesCommandDefault;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.LogTool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;

public class GetStringFromPrivateAccountRedisCommand extends HystrixCommand<String> {
    private static final Logger logger = LogManager.getLogger(PrivateAccountSetCommand.class);
    private String key;

    private GetStringFromPrivateAccountRedisCommand(){
        super(Setter
                .withGroupKey(HystrixCommandGroupKey.Factory.asKey("RedisGroup"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("GetStringFromPrivateAccountRedisCommand"))
                .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey("gsfparc-redis-poll"))
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

    public GetStringFromPrivateAccountRedisCommand(String key){
        this();
        this.key = key;
    }

    @Override
    protected String run() throws Exception {
        String result = null;
        StatefulRedisClusterConnection<String, String> connection = MXDataSource.redis().getPrivateAccountClusterConnection();
        RedisAdvancedClusterAsyncCommands<String, String> cacheJedis = connection.async();

        try{
            if (null == cacheJedis) {
                return result;
            }
            if (null == key) {
                return result;
            }
            RedisFuture<String> future = cacheJedis.get(key);
            future.await(Conf.getJedisClusterSocketTimeout(), TimeUnit.MILLISECONDS);
            if(future.isDone()){
                result = future.get(Conf.getJedisClusterSocketTimeout(), TimeUnit.MILLISECONDS);
            }
            return result;
        }catch (Exception e){
            String message = String.format("GetStringFromPrivateAccountRedisCommand redis happen exception: %s", e.fillInStackTrace());
            LogTool.printJsonStatusLog(logger, message, "nullId");
            return result;
        }
    }
    @Override
    protected String getFallback() {
        LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), "GetStringFromPrivateAccountRedisCommand happen exception", null, (Object) null);
        return null;
    }
}
