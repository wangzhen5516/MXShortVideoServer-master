package mx.j2.recommend.data_source;

import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
import com.netflix.hystrix.HystrixCommand;
import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Trace;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.hystrix.GuavaBloomGetCommand;
import mx.j2.recommend.hystrix.GuavaBloomSetCommand;
import mx.j2.recommend.util.BloomUtil;
import mx.j2.recommend.util.DefineTool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;

/**
 * @author xiang.zhou
 * @date 2020-07-17
 */
public class GuavaBloomDataSource extends BaseDataSource {
    public static final Logger logger = LogManager.getLogger(GuavaBloomDataSource.class);

    private RedisURI guavaRedisUri = null;

    private RedisClusterClient guavaByteArrayRedisClient = null;
    private StatefulRedisClusterConnection<byte[], byte[]> guavaByteArrayRedisConn = null;

    private RedisClusterClient guavaStringRedisClient = null;
    private StatefulRedisClusterConnection<String, String> guavaStringRedisConn = null;

    private final static String SUFFIX = "_guava";
    private final static String BIG_SUFFIX = "_big_guava";

    private final static int SMALL_BLOOM_FILTER_SIZE = 3000;
    private final static double ERROR_RATE = 0.001;


    /**
     * 构造函数
     */
    public GuavaBloomDataSource() {
        init();
    }

    /**
     * 初始化
     */
    public void init() {
        guavaRedisUri = RedisURI.Builder.redis(Conf.getRedisGuavaHost(), 6379)
                .withTimeout(Duration.ofMillis(Conf.getJedisClusterSocketTimeout()))
                .build();
        guavaByteArrayRedisClient = RedisClusterClient.create(guavaRedisUri);
        guavaByteArrayRedisClient.setOptions(ClusterClientOptions.builder()
                .validateClusterNodeMembership(false)
                .autoReconnect(true)
                .build());
        guavaByteArrayRedisConn = guavaByteArrayRedisClient.connect(newBytesBytesCodec());

        guavaStringRedisClient = RedisClusterClient.create(guavaRedisUri);
        guavaStringRedisClient.setOptions(ClusterClientOptions.builder()
                .validateClusterNodeMembership(false)
                .autoReconnect(true)
                .build());
        guavaStringRedisConn = guavaStringRedisClient.connect(StringCodec.UTF8);
    }

    private RedisCodec<byte[], byte[]> newBytesBytesCodec() {
        return ByteArrayCodec.INSTANCE;
    }

    @Trace(dispatcher = true)
    public StatefulRedisClusterConnection<byte[], byte[]> getGuavaBloomRedisConn() {
        return getConn(guavaRedisUri, guavaByteArrayRedisClient, guavaByteArrayRedisConn, newBytesBytesCodec());
    }

    public StatefulRedisClusterConnection<String, String> getRedisConn() {
        return getConn(guavaRedisUri, guavaStringRedisClient, guavaStringRedisConn, StringCodec.UTF8);
    }

    public <T> StatefulRedisClusterConnection<T, T> getConn(RedisURI redisURI, RedisClusterClient redisClient, StatefulRedisClusterConnection<T, T> redisConnection, RedisCodec<T, T> redisCodec) {
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
                NewRelic.noticeError(command.getCommandKey().name() + " circuit breaker open!");
                System.out.println(command.getCommandKey().name() + " circuit breaker open!");
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

    private void setBloomFilterToRedisNX(byte[] key, byte[] bloomBytes) {
        try {
            HystrixCommand<Boolean> command = new GuavaBloomSetCommand(key, bloomBytes, true);
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

    private BloomFilter<String> createBloomFilter(int insertions, double fpp) {
        return BloomFilter.create(Funnels.stringFunnel(Charsets.UTF_8), insertions, fpp);
    }

    public BloomFilter<String> createDefaultBloomFilter() {
        return createBloomFilter(SMALL_BLOOM_FILTER_SIZE, ERROR_RATE);
    }

    /**
     * 通过UserId获得guava的bloom filter.并写入到DC中
     *
     * @param userId 用户的ID
     * @param dc
     */
    @Trace(dispatcher = true)
    public BloomFilter<String> getBloomFilter(String userId, BaseDataCollection dc) {
        byte[] key = (userId + SUFFIX).getBytes(StandardCharsets.UTF_8);
        byte[] bloom = getBloomFilterFromRedis(key, userId);

        if (bloom == null) {
            dc.guavaBloomFilter = createDefaultBloomFilter();// 新建一个，返回
            dc.isBloomNew = true;
            return dc.guavaBloomFilter;
        } else if (GuavaBloomGetCommand.ERROR_RESULT.equals(bloom)) {
            return null;
        }

        try {
            dc.guavaBloomFilter = BloomFilter.readFrom(new ByteArrayInputStream(bloom), (Funnel<String>) (s, primitiveSink) -> primitiveSink.putString(s, Charsets.UTF_8));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return dc.guavaBloomFilter;
    }


    public BloomFilter<String> getBigBloomFilter(String userId) {
        byte[] bigKey = (userId + BIG_SUFFIX).getBytes(StandardCharsets.UTF_8);
        byte[] bloom = getBloomFilterFromRedis(bigKey, userId);
        if (bloom == null || GuavaBloomGetCommand.ERROR_RESULT.equals(bloom)) {
            return null;
        }
        try {
            return BloomFilter.readFrom(new ByteArrayInputStream(bloom), (Funnel<String>) (s, primitiveSink) -> primitiveSink.putString(s, Charsets.UTF_8));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public void setHistoryToGuava(BloomFilter<String> guavaBloomFilter, String userId, Collection<String> ids) {
        for (String id : ids) {
            guavaBloomFilter.put(id);
        }
    }

    public void setBloomFilter(BaseDataCollection baseDc) {
        if (baseDc.resIdList.isEmpty()) {
            return;
        }
        if (!DefineTool.TabInfoEnum.HOT.getId().equals(baseDc.req.tabId) && !DefineTool.TabInfoEnum.STATUS.getId().equals(baseDc.req.tabId)) {
            return;
        }
        if (baseDc.guavaBloomFilter == null) {
            if (baseDc.isHaveBloom) {
                logger.error(baseDc.req.interfaceName);
                logger.error("get null dc.guavaBloomFilter");
            }
            return;
        }
        String userId = BloomUtil.getUserId(baseDc);
        if (userId == null) {
            logger.error(" null uuid and userId for this request " + baseDc.req);
            return;
        }
        // write to guava
        setHistoryToGuava(baseDc.guavaBloomFilter, userId, baseDc.resIdList);

        // write to redis
        byte[] key = (userId + SUFFIX).getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            baseDc.guavaBloomFilter.writeTo(out);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        byte[] bloomBytes = out.toByteArray();
        //stop local cache, becauseof the LB
//        DataSourceManager.INSTANCE.getLocalCacheDataSource().setBloomFilter(userId, bloomBytes);
        if (baseDc.isBloomNew) {
            // TODO: 多次写，多次读，看redis可能得到 一个空吗？
            setBloomFilterToRedis(key, bloomBytes);
        } else {
            setBloomFilterToRedisNX(key, bloomBytes);
        }
    }

}
