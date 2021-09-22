package mx.j2.recommend.data_source;

import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.util.MXJudgeUtils;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static mx.j2.recommend.util.BaseMagicValueEnum.*;

/**
 * @author liangjie.feng
 * @version 1.0
 * @date 2021/3/25 下午4:42
 * @description
 */
public class PublisherPageWhiteDataSource extends BaseDataSource {
    private static final int INIT_SIZE = 2048;
    private static Map<String, String> whiteMap;

    private RedisClusterClient strategyClient;
    private StatefulRedisClusterConnection<String, String> strategyConnection;

    public PublisherPageWhiteDataSource() {
        init();
    }

    public StatefulRedisClusterConnection<String, String> getStrategyConnection() {
        if (!strategyConnection.isOpen()) {
            strategyConnection = strategyClient.connect();
        }
        return strategyConnection;
    }

    private void init() {
        whiteMap = new HashMap<>(INIT_SIZE);

        RedisURI strategyRedisUri = RedisURI.Builder.redis(Conf.getRedisStrategyHost(), Conf.getRedisStrategyPort())
                .withTimeout(Duration.ofMillis(Conf.getJedisClusterSocketTimeout()))
                .build();

        strategyClient = RedisClusterClient.create(strategyRedisUri);
        strategyClient.setOptions(ClusterClientOptions.builder()
                .validateClusterNodeMembership(false)
                .build());
        strategyConnection = strategyClient.connect();

        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutorService.scheduleAtFixedRate(this::getWhiteSetFromStrategyRedis, 0, 10, TimeUnit.MINUTES);
    }

    private void getWhiteSetFromStrategyRedis() {
        Map<String, String> tempMap = new HashMap<>(INIT_SIZE);

        addPublishers(tempMap, FP_PUBLISHER_TRAFFIC_SUPPORT_LV2);
        addPublishers(tempMap, FP_PUBLISHER_TRAFFIC_SUPPORT_LV3);
        addPublishers(tempMap, FP_PUBLISHER_TRAFFIC_SUPPORT_LV4);

        whiteMap = tempMap;
    }

    /**
     * 添加某扶持级别下的所有账号
     */
    private void addPublishers(Map<String, String> map, String level) {
        Set<String> idSet = getStrategyConnection().sync().smembers(level);

        if (MXJudgeUtils.isNotEmpty(idSet)) {
            idSet.forEach(id -> map.put(id, level));
        }
    }

    public boolean isInWhiteList(String publisherId) {
        return whiteMap.containsKey(publisherId);
    }

    /**
     * 返回扶持级别数字简写
     */
    public String getSupportLevel(String publisherId) {
        String level = whiteMap.get(publisherId);

        if (MXJudgeUtils.isNotEmpty(level)) {
            return level.substring(level.length() - 1);
        }

        return "null";
    }
}
