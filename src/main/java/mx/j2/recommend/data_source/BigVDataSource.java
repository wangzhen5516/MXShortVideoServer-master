package mx.j2.recommend.data_source;

import io.lettuce.core.RedisURI;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.util.MXJudgeUtils;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class BigVDataSource extends BaseDataSource {
    private static Map<String, Double> bigV;

    private RedisClusterClient strategyClient;
    private StatefulRedisClusterConnection<String, String> strategyConnection;

    public BigVDataSource() {
        init();
    }

    public StatefulRedisClusterConnection<String, String> getStrategyConnection() {
        if (!strategyConnection.isOpen()) {
            strategyConnection = strategyClient.connect();
        }
        return strategyConnection;
    }

    private void init() {
        bigV = new ConcurrentHashMap<>(1024);

        RedisURI strategyRedisUri = RedisURI.Builder.redis(Conf.getRedisStrategyHost(), Conf.getRedisStrategyPort())
                .withTimeout(Duration.ofMillis(Conf.getJedisClusterSocketTimeout()))
                .build();

        strategyClient = RedisClusterClient.create(strategyRedisUri);
        strategyClient.setOptions(ClusterClientOptions.builder()
                .validateClusterNodeMembership(false)
                .build());
        strategyConnection = strategyClient.connect();

        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutorService.scheduleAtFixedRate(this::getBigVFromStrategyRedis, 0, 10, TimeUnit.MINUTES);
    }

    private void getBigVFromStrategyRedis() {
        List<ScoredValue<String>> scoredValueList = getStrategyConnection().sync().zrangeWithScores("vip_publisher_list", 0, -1);
        Map<String, Double> tempBigV = new ConcurrentHashMap<>(1024);
        if (MXJudgeUtils.isNotEmpty(scoredValueList)) {
            for (int i = 0; i < scoredValueList.size(); i++) {
                if (scoredValueList.get(i) != null) {
                    tempBigV.put(scoredValueList.get(i).getValue(), scoredValueList.get(i).getScore());
                }
            }
        }
        bigV = tempBigV;
    }

    public boolean isBigV(String publisherId) {

        return bigV.keySet().contains(publisherId);
    }
}
