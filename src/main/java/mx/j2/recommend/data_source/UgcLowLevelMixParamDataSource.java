package mx.j2.recommend.data_source;

import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.util.MXJudgeUtils;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author liangjie.feng
 * @version 1.0
 * @date 2021/6/1 下午3:54
 * @description
 */
public class UgcLowLevelMixParamDataSource extends BaseDataSource {
    private RedisClusterClient strategyClient;
    private StatefulRedisClusterConnection<String, String> strategyConnection;

    private static double mixParam = 100.0;

    public UgcLowLevelMixParamDataSource() {
        RedisURI strategyRedisUri = RedisURI.Builder.redis(Conf.getRedisStrategyHost(), Conf.getRedisStrategyPort())
                .withTimeout(Duration.ofMillis(Conf.getJedisClusterSocketTimeout()))
                .build();
        strategyClient = RedisClusterClient.create(strategyRedisUri);
        strategyClient.setOptions(ClusterClientOptions.builder()
                .validateClusterNodeMembership(false)
                .build());
        strategyConnection = strategyClient.connect();

        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.scheduleAtFixedRate(this::getMixParamFromRedis, 0, 10, TimeUnit.MINUTES);
    }

    private void getMixParamFromRedis() {
        String mixParamTemp = getStrategyConnection().sync().get("mix_param");
        if (MXJudgeUtils.isNotEmpty(mixParamTemp)) {
            mixParam = Double.parseDouble(mixParamTemp);
        }
    }

    private StatefulRedisClusterConnection<String, String> getStrategyConnection() {
        if (!strategyConnection.isOpen()) {
            strategyConnection = strategyClient.connect();
        }
        return strategyConnection;
    }

    public static double getMixParam() {
        return mixParam;
    }
}
