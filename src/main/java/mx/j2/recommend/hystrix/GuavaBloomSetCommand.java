package mx.j2.recommend.hystrix;

import com.netflix.hystrix.*;
import com.netflix.hystrix.strategy.properties.HystrixPropertiesCommandDefault;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.LogTool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class GuavaBloomSetCommand extends HystrixCommand<Boolean> {
    private static Logger logger = LogManager.getLogger(GuavaBloomSetCommand.class);
    public final static byte[] ERROR_RESULT = new byte[0];
    private byte[] key;
    private byte[] bloomBytes;
    private boolean isNx;

    private final static long EXPIRE_TIME = 60 * 60 * 1;// modify the redis bloom's expire time to 1H

    public GuavaBloomSetCommand() {
        super(Setter
                .withGroupKey(HystrixCommandGroupKey.Factory.asKey("RedisGroup"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("GuavaBloomSetCommand"))
                .andThreadPoolKey(HystrixThreadPoolKey.Factory.asKey("gbs-redis-poll"))
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

    public GuavaBloomSetCommand(byte[] key, byte[] bloomBytes, boolean isNx) {
        this();
        this.key = key;
        this.bloomBytes = bloomBytes;
        this.isNx = isNx;
    }

    private void setex(StatefulRedisClusterConnection<byte[], byte[]> connection, byte[] key, byte[] value, long seconds) {
        if (connection == null) {
            return;
        }
        if(isNx) {
            connection.async().setnx(key, value);
        } else {
            connection.async().set(key, value);
        }
        connection.async().expire(key, seconds);
    }

    @Override
    protected Boolean run() throws Exception {
        StatefulRedisClusterConnection<byte[], byte[]> connection = MXDataSource.guavaBloom().getGuavaBloomRedisConn();
        setex(connection, key, bloomBytes, EXPIRE_TIME);
        return true;
    }

    @Override
    protected Boolean getFallback() {
        LogTool.printJsonStatusLog(logger, "GuavaBloomSetCommand redis abnormal. logId", "nullId");
        return false;
    }
}
