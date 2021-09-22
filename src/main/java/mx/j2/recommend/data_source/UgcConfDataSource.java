package mx.j2.recommend.data_source;

import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.util.MXJudgeUtils;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class UgcConfDataSource extends BaseDataSource {
    /**
     * 区分用户是新用户还是老用户, 区分处理
     * 为了性能考虑, 位置选择时只自旋一次, 对于老用户, 极端情况下, 大概有4%-7%的概率位置选择冲突, ugc3不够数.
     */
    public static int HISTORY_NUMBER_MIDDLE = 60;
    public static int HISTORY_NUMBER_OLD = 120;
    public static int HISTORY_NUMBER_OLD_OLD = 300;

    public static double PERCENTAGE_NEWUSER_POOL1 = 0.0;
    public static double PERCENTAGE_NEWUSER_POOL2 = 0.0;
    public static double PERCENTAGE_NEWUSER_POOL3 = 1.0;
    public static double PERCENTAGE_NEWUSER_POOL4 = 1.0;
    public static double PERCENTAGE_NEWUSER_POOL5 = 1.0;
    public static double PERCENTAGE_NEWUSER_VIP1 = 2.0;
    public static double PERCENTAGE_NEWUSER_VIP2 = 1.0;
    public static double PERCENTAGE_NEWUSER_RANDOM_VIP1 = 2.0;
    public static double PERCENTAGE_NEWUSER_RANDOM_VIP2 = 1.0;
    public static double PERCENTAGE_NEWUSER_BEAUTY = 1.0;
    public static double PERCENTAGE_NEWUSER_AGEOLDV1 = 1.0;
    public static double PERCENTAGE_NEWUSER_AGEOLDV2 = 0.5;

    public static double PERCENTAGE_MIDDLE_POOL1 = 0.0;
    public static double PERCENTAGE_MIDDLE_POOL2 = 0.0;
    public static double PERCENTAGE_MIDDLE_POOL3 = 1.0;
    public static double PERCENTAGE_MIDDLE_POOL4 = 1.0;
    public static double PERCENTAGE_MIDDLE_POOL5 = 1.0;
    public static double PERCENTAGE_MIDDLE_VIP1 = 2.0;
    public static double PERCENTAGE_MIDDLE_VIP2 = 1.0;
    public static double PERCENTAGE_MIDDLE_RANDOM_VIP1 = 2.0;
    public static double PERCENTAGE_MIDDLE_RANDOM_VIP2 = 1.0;
    public static double PERCENTAGE_MIDDLE_BEAUTY = 1.0;
    public static double PERCENTAGE_MIDDLE_AGEOLDV1 = 1.0;
    public static double PERCENTAGE_MIDDLE_AGEOLDV2 = 0.5;

    public static double PERCENTAGE_OLD_POOL1 = 0.0;
    public static double PERCENTAGE_OLD_POOL2 = 0.0;
    public static double PERCENTAGE_OLD_POOL3 = 1.0;
    public static double PERCENTAGE_OLD_POOL4 = 1.0;
    public static double PERCENTAGE_OLD_POOL5 = 1.0;
    public static double PERCENTAGE_OLD_VIP1 = 2.0;
    public static double PERCENTAGE_OLD_VIP2 = 1.0;
    public static double PERCENTAGE_OLD_RANDOM_VIP1 = 2.0;
    public static double PERCENTAGE_OLD_RANDOM_VIP2 = 1.0;
    public static double PERCENTAGE_OLD_BEAUTY = 1.0;
    public static double PERCENTAGE_OLD_AGEOLDV1 = 1.0;
    public static double PERCENTAGE_OLD_AGEOLDV2 = 0.5;

    public static double PERCENTAGE_OLD_OLD_POOL1 = 0.0;
    public static double PERCENTAGE_OLD_OLD_POOL2 = 0.0;
    public static double PERCENTAGE_OLD_OLD_POOL3 = 1.0;
    public static double PERCENTAGE_OLD_OLD_POOL4 = 2.0;
    public static double PERCENTAGE_OLD_OLD_POOL5 = 2.0;
    public static double PERCENTAGE_OLD_OLD_VIP1 = 3.0;
    public static double PERCENTAGE_OLD_OLD_VIP2 = 3.0;
    public static double PERCENTAGE_OLD_OLD_RANDOM_VIP1 = 3.0;
    public static double PERCENTAGE_OLD_OLD_RANDOM_VIP2 = 3.0;
    public static double PERCENTAGE_OLD_OLD_BEAUTY = 1.0;
    public static double PERCENTAGE_OLD_OLD_AGEOLDV1 = 1.0;
    public static double PERCENTAGE_OLD_OLD_AGEOLDV2 = 0.5;

    public static int TAG_POOL_A = 6;
    public static int TAG_POOL_B = 1;

    public static int USER_PROFILE_NUM = 1;
    public static int LANGUAGE_NUM = 10;

    private String ugcConfigurationKey = "ugc_conf";

    private RedisClusterClient strategyClient = null;
    private StatefulRedisClusterConnection<String, String> strategyConnection = null;


    public UgcConfDataSource() {
        init();
    }

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

        ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(1);
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                refresh();
            }
        };
        scheduledExecutorService.scheduleAtFixedRate(runnable, 0, 1, TimeUnit.MINUTES);
    }

    private void refresh() {
        try {
            TAG_POOL_A = Integer.parseInt(getStrategyConnection().sync().get("TAG_POOL_A"));
            TAG_POOL_B = Integer.parseInt(getStrategyConnection().sync().get("TAG_POOL_B"));
            Map<String, String> confMap = getStrategyConnection().sync().hgetall(ugcConfigurationKey);
            if (confMap == null) {
                return;
            }
            if (confMap.get("HISTORY_NUMBER_MIDDLE") != null) {
                HISTORY_NUMBER_MIDDLE = Integer.parseInt(confMap.get("HISTORY_NUMBER_MIDDLE"));
            }
            if (confMap.get("HISTORY_NUMBER_OLD") != null) {
                HISTORY_NUMBER_OLD = Integer.parseInt(confMap.get("HISTORY_NUMBER_OLD"));
            }
            if (confMap.get("HISTORY_NUMBER_OLD_OLD") != null) {
                HISTORY_NUMBER_OLD_OLD = Integer.parseInt(confMap.get("HISTORY_NUMBER_OLD_OLD"));
            }
            if (confMap.get("USER_PROFILE_NUM") != null) {
                USER_PROFILE_NUM = Integer.parseInt(confMap.get("USER_PROFILE_NUM"));
            }
            if (confMap.get("LANGUAGE_NUM") != null) {
                LANGUAGE_NUM = Integer.parseInt(confMap.get("LANGUAGE_NUM"));
            }
            // get the detail conf
            Map<String, String> detailConfLv1 = getStrategyConnection().sync().hgetall("ugc_conf_lv1");
            if (MXJudgeUtils.isEmpty(detailConfLv1)) {
                return;
            }
            PERCENTAGE_NEWUSER_POOL1 = Double.valueOf(detailConfLv1.get("PERCENTAGE_NEWUSER"));
            PERCENTAGE_MIDDLE_POOL1 = Double.valueOf(detailConfLv1.get("PERCENTAGE_MIDDLE"));
            PERCENTAGE_OLD_POOL1 = Double.valueOf(detailConfLv1.get("PERCENTAGE_OLD"));
            PERCENTAGE_OLD_OLD_POOL1 = Double.valueOf(detailConfLv1.get("PERCENTAGE_OLD_OLD"));

            Map<String, String> detailConfLv2 = getStrategyConnection().sync().hgetall("ugc_conf_lv2");
            if (MXJudgeUtils.isEmpty(detailConfLv2)) {
                return;
            }
            PERCENTAGE_NEWUSER_POOL2 = Double.valueOf(detailConfLv2.get("PERCENTAGE_NEWUSER"));
            PERCENTAGE_MIDDLE_POOL2 = Double.valueOf(detailConfLv2.get("PERCENTAGE_MIDDLE"));
            PERCENTAGE_OLD_POOL2 = Double.valueOf(detailConfLv2.get("PERCENTAGE_OLD"));
            PERCENTAGE_OLD_OLD_POOL2 = Double.valueOf(detailConfLv2.get("PERCENTAGE_OLD_OLD"));

            Map<String, String> detailConfLv3 = getStrategyConnection().sync().hgetall("ugc_conf_lv3");
            if (MXJudgeUtils.isEmpty(detailConfLv3)) {
                return;
            }
            PERCENTAGE_NEWUSER_POOL3 = Double.valueOf(detailConfLv3.get("PERCENTAGE_NEWUSER"));
            PERCENTAGE_MIDDLE_POOL3 = Double.valueOf(detailConfLv3.get("PERCENTAGE_MIDDLE"));
            PERCENTAGE_OLD_POOL3 = Double.valueOf(detailConfLv3.get("PERCENTAGE_OLD"));
            PERCENTAGE_OLD_OLD_POOL3 = Double.valueOf(detailConfLv3.get("PERCENTAGE_OLD_OLD"));

            Map<String, String> detailConfLv4 = getStrategyConnection().sync().hgetall("ugc_conf_lv4");
            if (MXJudgeUtils.isEmpty(detailConfLv4)) {
                return;
            }
            PERCENTAGE_NEWUSER_POOL4 = Double.valueOf(detailConfLv4.get("PERCENTAGE_NEWUSER"));
            PERCENTAGE_MIDDLE_POOL4 = Double.valueOf(detailConfLv4.get("PERCENTAGE_MIDDLE"));
            PERCENTAGE_OLD_POOL4 = Double.valueOf(detailConfLv4.get("PERCENTAGE_OLD"));
            PERCENTAGE_OLD_OLD_POOL4 = Double.valueOf(detailConfLv4.get("PERCENTAGE_OLD_OLD"));

            Map<String, String> detailConfLv5 = getStrategyConnection().sync().hgetall("ugc_conf_lv5");
            if (MXJudgeUtils.isEmpty(detailConfLv5)) {
                return;
            }
            PERCENTAGE_NEWUSER_POOL5 = Double.valueOf(detailConfLv5.get("PERCENTAGE_NEWUSER"));
            PERCENTAGE_MIDDLE_POOL5 = Double.valueOf(detailConfLv5.get("PERCENTAGE_MIDDLE"));
            PERCENTAGE_OLD_POOL5 = Double.valueOf(detailConfLv5.get("PERCENTAGE_OLD"));
            PERCENTAGE_OLD_OLD_POOL5 = Double.valueOf(detailConfLv5.get("PERCENTAGE_OLD_OLD"));
            // vip1
            Map<String, String> detailConfVip1 = getStrategyConnection().sync().hgetall("ugc_conf_vip1");
            if (MXJudgeUtils.isEmpty(detailConfVip1)) {
                return;
            }
            PERCENTAGE_NEWUSER_VIP1 = Double.valueOf(detailConfVip1.get("PERCENTAGE_NEWUSER"));
            PERCENTAGE_MIDDLE_VIP1 = Double.valueOf(detailConfVip1.get("PERCENTAGE_MIDDLE"));
            PERCENTAGE_OLD_VIP1 = Double.valueOf(detailConfVip1.get("PERCENTAGE_OLD"));
            PERCENTAGE_OLD_OLD_VIP1 = Double.valueOf(detailConfVip1.get("PERCENTAGE_OLD_OLD"));
            PERCENTAGE_NEWUSER_RANDOM_VIP1 = Double.valueOf(detailConfVip1.get("PERCENTAGE_NEWUSER_RANDOM"));
            PERCENTAGE_MIDDLE_RANDOM_VIP1 = Double.valueOf(detailConfVip1.get("PERCENTAGE_MIDDLE_RANDOM"));
            PERCENTAGE_OLD_RANDOM_VIP1 = Double.valueOf(detailConfVip1.get("PERCENTAGE_OLD_RANDOM"));
            PERCENTAGE_OLD_OLD_RANDOM_VIP1 = Double.valueOf(detailConfVip1.get("PERCENTAGE_OLD_OLD_RANDOM"));

            // vip2
            Map<String, String> detailConfVip2 = getStrategyConnection().sync().hgetall("ugc_conf_vip2");
            if (MXJudgeUtils.isEmpty(detailConfVip2)) {
                return;
            }
            PERCENTAGE_NEWUSER_VIP2 = Double.valueOf(detailConfVip2.get("PERCENTAGE_NEWUSER"));
            PERCENTAGE_MIDDLE_VIP2 = Double.valueOf(detailConfVip2.get("PERCENTAGE_MIDDLE"));
            PERCENTAGE_OLD_VIP2 = Double.valueOf(detailConfVip2.get("PERCENTAGE_OLD"));
            PERCENTAGE_OLD_OLD_VIP2 = Double.valueOf(detailConfVip2.get("PERCENTAGE_OLD_OLD"));
            PERCENTAGE_NEWUSER_RANDOM_VIP2 = Double.valueOf(detailConfVip1.get("PERCENTAGE_NEWUSER_RANDOM"));
            PERCENTAGE_MIDDLE_RANDOM_VIP2 = Double.valueOf(detailConfVip1.get("PERCENTAGE_MIDDLE_RANDOM"));
            PERCENTAGE_OLD_RANDOM_VIP2 = Double.valueOf(detailConfVip1.get("PERCENTAGE_OLD_RANDOM"));
            PERCENTAGE_OLD_OLD_RANDOM_VIP2 = Double.valueOf(detailConfVip1.get("PERCENTAGE_OLD_OLD_RANDOM"));

            // beaty
            List<String> detailConfBeauty = getStrategyConnection().sync().lrange("ugc_conf_beauty", 0, -1);
            if (MXJudgeUtils.isEmpty(detailConfBeauty)|| detailConfBeauty.size() < 4) {
                return;
            }
            PERCENTAGE_NEWUSER_BEAUTY = Double.valueOf(detailConfBeauty.get(0));
            PERCENTAGE_MIDDLE_BEAUTY = Double.valueOf(detailConfBeauty.get(1));
            PERCENTAGE_OLD_BEAUTY = Double.valueOf(detailConfBeauty.get(2));
            PERCENTAGE_OLD_OLD_BEAUTY = Double.valueOf(detailConfBeauty.get(3));

            /*养老池*/
            List<String> detailConfOld1 = getStrategyConnection().sync().lrange("ugc_conf_old1",0,-1);
            if(MXJudgeUtils.isEmpty(detailConfOld1)|| detailConfOld1.size() < 4){
                return;
            }
            PERCENTAGE_NEWUSER_AGEOLDV1 = Double.valueOf(detailConfOld1.get(0));
            PERCENTAGE_MIDDLE_AGEOLDV1 = Double.valueOf(detailConfOld1.get(1));
            PERCENTAGE_OLD_AGEOLDV1 = Double.valueOf(detailConfOld1.get(2));
            PERCENTAGE_OLD_OLD_AGEOLDV1 = Double.valueOf(detailConfOld1.get(3));

            List<String> detailConfOld2 = getStrategyConnection().sync().lrange("ugc_conf_old2",0,-1);
            if(MXJudgeUtils.isEmpty(detailConfOld2)|| detailConfOld2.size() < 4){
                return;
            }
            PERCENTAGE_NEWUSER_AGEOLDV2 = Double.valueOf(detailConfOld2.get(0));
            PERCENTAGE_MIDDLE_AGEOLDV2 = Double.valueOf(detailConfOld2.get(1));
            PERCENTAGE_OLD_AGEOLDV2 = Double.valueOf(detailConfOld2.get(2));
            PERCENTAGE_OLD_OLD_AGEOLDV2 = Double.valueOf(detailConfOld2.get(3));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
