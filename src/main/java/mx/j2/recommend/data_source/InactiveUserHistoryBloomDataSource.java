package mx.j2.recommend.data_source;

import com.netflix.hystrix.HystrixCommand;
import com.newrelic.api.agent.NewRelic;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.rebloom.client.Client;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.hystrix.BloomFilterExistSingleCommand;
import mx.j2.recommend.hystrix.InactiveUserBloomFilterExistCommand;

import java.time.Duration;

/**
 * @author zhongren.li
 * @date 2020-09-24
 */
public class InactiveUserHistoryBloomDataSource extends BaseDataSource {

    private static final int POOL_SIZE_MASTER = 100;
    private RedisURI bloomRedisUri = null;
    private RedisClient bloomClient = null;
    private StatefulRedisConnection<String, String> bloomConnection = null;
    private Client client;

    public InactiveUserHistoryBloomDataSource() {
        this(
                Conf.getInactiveUsersHistoryBloomRedisEndPointURL(),
                Conf.getInactiveUsersHistoryBloomRedisPort(),
                Conf.getJedisClusterSocketTimeout()
        );
    }

    private InactiveUserHistoryBloomDataSource(String host, int port, int timeout) {
        client = new Client(
                host,
                port,
                timeout, POOL_SIZE_MASTER);

        bloomRedisUri = RedisURI.Builder.redis(host, port)
                .withTimeout(Duration.ofMillis(timeout))
                .build();
        bloomClient = RedisClient.create(bloomRedisUri);
        bloomClient.setOptions(ClientOptions.builder().build());
        bloomConnection = bloomClient.connect();
    }

    public StatefulRedisConnection<String, String> getRedisConnection() {
        if (null == bloomConnection || !bloomConnection.isOpen()) {
            if (null == bloomClient) {
                bloomClient = RedisClient.create(bloomRedisUri);
            }
            bloomConnection = bloomClient.connect();
        }
        return bloomConnection;
    }

    public Client getClient() {
        if (null == client) {
            client = new Client(
                    Conf.getInactiveUsersHistoryBloomRedisEndPointURL(),
                    Conf.getInactiveUsersHistoryBloomRedisPort(),
                    Conf.getJedisClusterSocketTimeout(),
                    POOL_SIZE_MASTER);
        }
        return client;
    }

    public boolean exists(String key, String value) {
        try {
            HystrixCommand<Boolean> command = new InactiveUserBloomFilterExistCommand(key, value);
            if (command.isCircuitBreakerOpen()) {
                NewRelic.noticeError(command.getCommandKey().name() + " circuit breaker open!");
            }
            return command.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public boolean existsSingle(String key, String value) {
        try {
            HystrixCommand<Boolean> command = new BloomFilterExistSingleCommand(
                    key,
                    value,
                    getRedisConnection(),
                    getClient());
            if (command.isCircuitBreakerOpen()) {
                NewRelic.noticeError(command.getCommandKey().name() + " circuit breaker open!");
            }
            return command.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
}