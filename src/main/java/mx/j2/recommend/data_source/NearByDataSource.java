package mx.j2.recommend.data_source;

import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class NearByDataSource extends BaseDataSource {

    public static double HEAT_SCORE_CONF = 10000;
    public static double ONLINE_TIME_CONF = 2.0;
    public static int START_RADIUS = 10;
    public static int STEP_SIZE = 20;
    public static int MAX_DISTANCE = 50;

    private RedisClusterClient strategyClient;
    private StatefulRedisClusterConnection<String, String> strategyConnection;

    public NearByDataSource() {init();}

    public StatefulRedisClusterConnection<String, String> getStrategyConnection() {
        if (!strategyConnection.isOpen()) {
            strategyConnection = strategyClient.connect();
        }
        return strategyConnection;
    }

    private void init() {
        RedisURI strategyRedisUri = RedisURI.Builder.redis(Conf.getRedisStrategyHost(), Conf.getRedisStrategyPort())
                .withTimeout(Duration.ofMillis(Conf.getJedisClusterSocketTimeout()))
                .build();

        strategyClient = RedisClusterClient.create(strategyRedisUri);
        strategyClient.setOptions(ClusterClientOptions.builder()
                .validateClusterNodeMembership(false)
                .build());
        strategyConnection = strategyClient.connect();

        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutorService.scheduleAtFixedRate(this::refresh, 0, 10, TimeUnit.MINUTES);
    }

    private void refresh() {
        try {
            Map<String, String> nearByConf = getStrategyConnection().sync().hgetall("nearby_conf");
            if (MXJudgeUtils.isEmpty(nearByConf)) {
                return;
            }

            if (MXStringUtils.isNotEmpty(nearByConf.get("heat_score"))) {
                HEAT_SCORE_CONF = Double.parseDouble(nearByConf.get("heat_score"));
            }
            if (MXStringUtils.isNotEmpty(nearByConf.get("online_time"))) {
                ONLINE_TIME_CONF = Double.parseDouble(nearByConf.get("online_time"));
            }
            if (MXStringUtils.isNotEmpty(nearByConf.get("start_radius"))) {
                START_RADIUS = Integer.parseInt(nearByConf.get("start_radius"));
            }
            if (MXStringUtils.isNotEmpty(nearByConf.get("step_size"))) {
                STEP_SIZE = Integer.parseInt(nearByConf.get("step_size"));
            }
            if (MXStringUtils.isNotEmpty(nearByConf.get("max_distance"))) {
                MAX_DISTANCE = Integer.parseInt(nearByConf.get("max_distance"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
