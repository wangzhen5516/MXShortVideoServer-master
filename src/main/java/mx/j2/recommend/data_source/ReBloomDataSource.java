package mx.j2.recommend.data_source;

import com.netflix.hystrix.HystrixCommand;
import com.newrelic.api.agent.NewRelic;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.rebloom.client.Client;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.hystrix.BloomFilterAddCommand;
import mx.j2.recommend.hystrix.BloomFilterDeleteCommand;
import mx.j2.recommend.hystrix.BloomFilterExistCommand;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.BloomUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;

/**
 * @author xiang.zhou
 * @date 2020-07-04
 */
public class ReBloomDataSource extends BaseDataSource {
    private static Logger logger = LogManager.getLogger(ReBloomDataSource.class);

    private Client clientMaster;
    private Client clientSlave;
    private static final int POOL_SIZE_MASTER = 100;
    private static final int POOL_SIZE_SLAVE = 300;
    private RedisURI bloomRedisUri = null;
    private RedisClient bloomClient = null;
    private StatefulRedisConnection<String, String> bloomConnection = null;

    //构造器
    public ReBloomDataSource() {
        this(Conf.getRedisBloomMasterHost(), Conf.getRedisBloomMasterPort(), Conf.getRedisBloomSlaveHost(), Conf.getRedisBloomSlavePort(), Conf.getJedisClusterSocketTimeout());
    }

    private ReBloomDataSource(String host, int port, String hostSlave, int portSlave, int timeout) {
        clientMaster = new Client(
                host,
                port,
                timeout, POOL_SIZE_MASTER);

        clientSlave = new Client(
                hostSlave,
                portSlave,
                timeout, POOL_SIZE_SLAVE);

        bloomRedisUri = RedisURI.Builder.redis(host, port)
                .withTimeout(Duration.ofMillis(timeout))
                .build();
        bloomClient = RedisClient.create(bloomRedisUri);
        bloomClient.setOptions(ClientOptions.builder().build());
        bloomConnection = bloomClient.connect();
    }

    //获取链接
    public StatefulRedisConnection<String, String> getRedisConnection() {
        if (null == bloomConnection || !bloomConnection.isOpen()) {
            if (null == bloomClient) {
                bloomClient = RedisClient.create(bloomRedisUri);
                bloomConnection = bloomClient.connect();
            } else {
                bloomConnection = bloomClient.connect();
            }
        }
        return bloomConnection;
    }

    public Client getMasterClient() {
        if (null == clientMaster) {
            clientMaster = new Client(Conf.getRedisBloomMasterHost(), Conf.getRedisBloomMasterPort(), Conf.getJedisClusterSocketTimeout(), POOL_SIZE_MASTER);
        }
        return clientMaster;
    }

    @Deprecated
    public Client getSlaveClient() {
        if (null == clientSlave) {
            clientSlave = new Client(Conf.getRedisBloomSlaveHost(), Conf.getRedisBloomSlavePort(), Conf.getJedisClusterSocketTimeout(), POOL_SIZE_SLAVE);
        }
        return clientSlave;
    }

    //对外接口
    public void setBloomHistoryList(BaseDataCollection dc) {
        String bloomKey = BloomUtil.getUserIdBloomKey(dc);
        if (bloomKey == null) {
            logger.error("get a null key");
            return;
        }
        MXDataSource.rebloom().add(bloomKey, dc.resIdList.toArray(new String[0]));
    }

    public boolean[] add(String key, String[] values) {
        try {
            HystrixCommand<boolean[]> command = new BloomFilterAddCommand(key, values);
            if (command.isCircuitBreakerOpen()) {
                NewRelic.noticeError(command.getCommandKey().name() + " circuit breaker open!");
            }
            return command.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new boolean[values.length];
    }

    public boolean[] exists(String key, String[] values) {
        try {
            HystrixCommand<boolean[]> command = new BloomFilterExistCommand(key, values);
            if (command.isCircuitBreakerOpen()) {
                NewRelic.noticeError(command.getCommandKey().name() + " circuit breaker open!");
            }
            return command.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new boolean[values.length];
    }

    public void deleteBloom(String key) {
        try {
            BloomFilterDeleteCommand deleteCommand = new BloomFilterDeleteCommand(key);
            deleteCommand.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void setFetchFollersBloomHistoryList(BaseDataCollection dc) {
        String bloomKey = BloomUtil.getUserFetchFollowersContentBloomKey(dc);
        if (bloomKey == null) {
            logger.error("get a null key for fetch followers");
            return;
        }
        MXDataSource.rebloom().add(bloomKey, dc.resIdList.toArray(new String[0]));
    }

}