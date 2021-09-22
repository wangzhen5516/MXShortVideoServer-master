package mx.j2.recommend.hystrix;

import com.netflix.hystrix.*;
import com.netflix.hystrix.strategy.properties.HystrixPropertiesCommandDefault;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.LogTool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class GuavaBloomGetCommand extends HystrixCommand<byte[]> {
    private static Logger logger = LogManager.getLogger(GuavaBloomGetCommand.class);
    public final static byte[] ERROR_RESULT = new byte[0];
    private byte[] key;
    private String userId;

    public GuavaBloomGetCommand() {
        super(Setter
                .withGroupKey(HystrixCommandGroupKey.Factory.asKey("RedisGroup"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("GuavaBloomGetCommand"))
                .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey("gbg-redis-poll"))
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

    public GuavaBloomGetCommand(byte[] key, String userId) {
        this();
        this.key = key;
        this.userId = userId;
    }

    @Override
    protected byte[] run() throws Exception {
        byte[] result = null;
//        DataSourceManager.INSTANCE.getLocalCacheDataSource().getBloomFilter(userId);
//        if (result != null)
//            return result;

        StatefulRedisClusterConnection<byte[], byte[]> connection = MXDataSource.guavaBloom().getGuavaBloomRedisConn();
        try {
            if (null == connection) {
                return ERROR_RESULT;
            }
            Future<byte[]> future = connection.async().get(key);
            result = future.get(500, TimeUnit.MILLISECONDS);
//            if(result != null) {
//                DataSourceManager.INSTANCE.getLocalCacheDataSource().setBloomFilter(userId, result);
//            }
            return result;
        } catch (Exception e) {
            e.printStackTrace();
            String message = String.format("redis happen exception: %s", e.fillInStackTrace());
            logger.error(message);
            return ERROR_RESULT;
        }
    }

    @Override
    protected byte[] getFallback() {
        LogTool.printJsonStatusLog(logger, "GuavaBloomGetCommand redis abnormal. logId", "nullId");
        return ERROR_RESULT;
    }
}
