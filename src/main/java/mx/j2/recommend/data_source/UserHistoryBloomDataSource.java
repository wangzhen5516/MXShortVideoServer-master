package mx.j2.recommend.data_source;

import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.netflix.hystrix.HystrixCommand;
import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Trace;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.hystrix.GuavaBloomGetCapacityCommand;
import mx.j2.recommend.hystrix.GuavaBloomGetCommand;
import mx.j2.recommend.hystrix.GuavaBloomSetCapacityCommand;
import mx.j2.recommend.hystrix.GuavaBloomSetCommand;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * @author zhongren.li
 * @date 2020-10.28
 */
public class UserHistoryBloomDataSource extends BaseDataSource {
    public static final Logger logger = LogManager.getLogger(UserHistoryBloomDataSource.class);

    private RedisURI redisUrl = null;
    private RedisClusterClient redisClusterClient = null;
    private StatefulRedisClusterConnection<String, String> redisClusterConnection = null;

    private final static String BLOOM_SUFFIX = "_history";

    private final static String BLOOM_CAPACITY_SUFFIX = "_capacity";

    /**
     * 构造函数
     */
    public UserHistoryBloomDataSource() {
        init();
    }

    /**
     * 初始化
     */
    public void init() {
        redisUrl = RedisURI.Builder.redis(Conf.getRedisGuavaHost(), 6379)
                .withTimeout(Duration.ofMillis(Conf.getJedisClusterSocketTimeout()))
                .build();
        redisClusterClient = RedisClusterClient.create(redisUrl);
        redisClusterClient.setOptions(ClusterClientOptions.builder()
                .validateClusterNodeMembership(false)
                .autoReconnect(true)
                .build());
        redisClusterConnection = redisClusterClient.connect(StringCodec.UTF8);
    }

    public StatefulRedisClusterConnection<String, String> getRedisConn() {
        return getConn(redisUrl, redisClusterClient, redisClusterConnection, StringCodec.UTF8);
    }

    private <T> StatefulRedisClusterConnection<T, T> getConn(RedisURI redisURI, RedisClusterClient redisClient, StatefulRedisClusterConnection<T, T> redisConnection, RedisCodec<T, T> redisCodec) {
        if (null == redisConnection || !redisConnection.isOpen()) {
            if (null == redisClient) {
                redisClient = RedisClusterClient.create(redisURI);
            }
            redisConnection = redisClient.connect(redisCodec);
        }
        return redisConnection;
    }

    private byte[] getBloomFilterFromRedis(byte[] key, String userId) {
        try {
            HystrixCommand<byte[]> command = new GuavaBloomGetCommand(key, userId);
            if (command.isCircuitBreakerOpen()) {
                NewRelic.noticeError("getBloomFilterFromRedis circuit breaker open!");
                System.out.println("getBloomFilterFromRedis circuit breaker open!");
            }
            return command.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return GuavaBloomGetCommand.ERROR_RESULT;
    }

    private void setBloomFilterToRedis(byte[] key, byte[] bloomBytes) {
        try {
            HystrixCommand<Boolean> command = new GuavaBloomSetCommand(key, bloomBytes, false);
            if (command.isCircuitBreakerOpen()) {
                NewRelic.noticeError(command.getCommandKey().name() + " circuit breaker open!");
                System.out.println(command.getCommandKey().name() + " circuit breaker open!");
            }
            command.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void set(StatefulRedisClusterConnection<byte[], byte[]> connection, byte[] key, byte[] value) {
        if (connection == null) {
            return;
        }
        connection.async().set(key, value);
    }

    @Trace(dispatcher = true)
    public BloomFilter<String> getBloomFilter(String id) {
        BloomFilter<String> bloomFilter = null;
        byte[] key = String.format("%s%s", id, BLOOM_SUFFIX).getBytes(StandardCharsets.UTF_8);
        byte[] bloom = getBloomFilterFromRedis(key, id);
        if (bloom == null) {
            return null;
        }

        try {
            bloomFilter = BloomFilter.readFrom(new ByteArrayInputStream(bloom), (Funnel<String>) (s, primitiveSink) -> primitiveSink.putString(s, Charsets.UTF_8));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return bloomFilter;
    }

    @Trace(dispatcher = true)
    public void setBloomFilter(String id, BloomFilter<String> bloomFilter) {
        byte[] key = String.format("%s%s", id, BLOOM_SUFFIX).getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            bloomFilter.writeTo(out);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        byte[] bloomBytes = out.toByteArray();
        setBloomFilterToRedis(key, bloomBytes);
    }

    public void setBloomCapacity(String id, long capacity) {
        String key = String.format("%s%s", id, BLOOM_CAPACITY_SUFFIX);
        GuavaBloomSetCapacityCommand command = new GuavaBloomSetCapacityCommand(key, Long.toString(capacity));
        command.execute();
    }

    public long getBloomCapacity(String id) {
        String key = String.format("%s%s", id, BLOOM_CAPACITY_SUFFIX);
        GuavaBloomGetCapacityCommand command = new GuavaBloomGetCapacityCommand(key);
        return command.execute();
    }
}
