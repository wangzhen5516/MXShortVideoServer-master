package mx.j2.recommend.data_source;

import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.recall.impl.TopKOLVideoIn30DaysRecallNew;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.lang.Thread.sleep;

public enum EsayStartTopKOLDataSource {
    INSTANCE();
    private static final String REDIS_KEY = "vip_30d_top_videos_v1";
    private static final String CACHE_KEY = "topKOLvideoin30days";
    private final DetailDataSource detailDataSource = MXDataSource.details();
    private final ElasticCacheSource elasticCacheSource = MXDataSource.redis();
    private final LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
    private final static int RANDOM_FACTOR = new Random().nextInt(10);
    private static final int REDIS_INTERVEL = 100;

    EsayStartTopKOLDataSource() {
        startScheduledTasks();
    }

    public boolean registerRedisRecall() {
        List<String> videoIdList = elasticCacheSource.getVideoFeatureZsetInfoFromRedis(REDIS_KEY);
        if (MXJudgeUtils.isNotEmpty(videoIdList)) {
            List<BaseDocument> resultList = detailDataSource.get(videoIdList, TopKOLVideoIn30DaysRecallNew.class.getName());
            if (MXJudgeUtils.isNotEmpty(resultList)) {
                localCacheDataSource.setTopKolVideo30DaysRedisCache(CACHE_KEY, resultList);

                return true;
            }
        }
        return false;
    }

    private void startScheduledTasks() {
        ScheduledExecutorService serviceNormal = Executors.newSingleThreadScheduledExecutor();

        int setScheduleSuccesfull = -3;
        while (setScheduleSuccesfull < 0) {
            try {
                serviceNormal.scheduleAtFixedRate(this::scheduledTaskNormal, 1, 1, TimeUnit.MINUTES);
                setScheduleSuccesfull = 0;

            } catch (Exception e) {
                setScheduleSuccesfull++;
                e.printStackTrace();

            }
        }
    }

    private void scheduledTaskNormal() {
        try {
            long time = System.currentTimeMillis() / 60000;

            int recallPeriod = 300 / 60;
            if (1 > recallPeriod) {
                return;
            }
            long randomFactor = time + RANDOM_FACTOR;
            if (randomFactor % recallPeriod == 0) {
                loadRecall();
                // 稍微休息50ms
                try {
                    sleep(REDIS_INTERVEL);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("run scheduled urgent task failed : " + e.toString());
        }
    }

    public void loadRecall(){
        List<String> videoIdList = elasticCacheSource.getVideoFeatureZsetInfoFromRedis(REDIS_KEY);
        if (MXJudgeUtils.isNotEmpty(videoIdList)) {
            List<BaseDocument> resultList = detailDataSource.get(videoIdList, TopKOLVideoIn30DaysRecallNew.class.getName());
            if (MXJudgeUtils.isNotEmpty(resultList)) {
                localCacheDataSource.setTopKolVideo30DaysRedisCache(CACHE_KEY, resultList);

                return;
            }
        }
    }
}
