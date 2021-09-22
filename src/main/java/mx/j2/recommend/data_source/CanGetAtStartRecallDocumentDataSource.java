package mx.j2.recommend.data_source;

import com.alibaba.fastjson.JSONObject;
import com.alicp.jetcache.Cache;
import com.alicp.jetcache.MultiLevelCacheBuilder;
import com.alicp.jetcache.RefreshPolicy;
import com.alicp.jetcache.embedded.CaffeineCacheBuilder;
import com.alicp.jetcache.redis.lettuce.RedisLettuceCacheBuilder;
import com.alicp.jetcache.support.FastjsonKeyConvertor;
import com.alicp.jetcache.support.JavaValueDecoder;
import com.alicp.jetcache.support.JavaValueEncoder;
import com.newrelic.api.agent.Trace;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.document.ShortDocument;
import mx.j2.recommend.data_model.interfaces.IDocumentProcessor;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.recall_data_in_mem.ESCanGetStartRecall;
import mx.j2.recommend.recall_data_in_mem.KeyDocumentListDataStruct;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static java.lang.Thread.sleep;

public enum CanGetAtStartRecallDocumentDataSource {
    /**
     * 注释
     */
    INSTANCE();

    private static Logger log = LogManager.getLogger(CanGetAtStartRecallDocumentDataSource.class);

    private final Cache<String, KeyDocumentListDataStruct> multiRecallToDocumentListMap;
    private final List<ESCanGetStartRecall> esRecallList;
    private static final int CACHE_VOLUME = 200;
    private static final int ES_INTERVEL = 100;

    private static final int CACHE_ALIVE_TIME_SECONDS = 20;

    private final Map<String, ESCanGetStartRecall> recallNameToRecallInstanceMap;

    CanGetAtStartRecallDocumentDataSource() {
        RedisURI redisUri = RedisURI.Builder.redis(Conf.getRedisCacheHost(), Conf.getRedisCachePort())
                .withTimeout(Duration.ofMillis(Conf.getJedisClusterSocketTimeout()))
                .build();
        RedisClusterClient redisClient = RedisClusterClient.create(redisUri);
        redisClient.setOptions(ClusterClientOptions.builder()
                .autoReconnect(true)
                .build());

        Cache<String, KeyDocumentListDataStruct> recallLocalCache = CaffeineCacheBuilder.createCaffeineCacheBuilder()
                .limit(CACHE_VOLUME)
                .cacheNullValue(true)
                .buildCache();

        Cache<String, KeyDocumentListDataStruct> recallRemoteCache = RedisLettuceCacheBuilder.createRedisLettuceCacheBuilder()
                .keyConvertor(FastjsonKeyConvertor.INSTANCE)
                .valueDecoder(JavaValueDecoder.INSTANCE)
                .valueEncoder(JavaValueEncoder.INSTANCE)
                .redisClient(redisClient)
                .keyPrefix("recallCache") //TODO 在预发布测试时需要更新这个key，上线以后可以改回来
                .buildCache();

        RefreshPolicy policy = RefreshPolicy.newPolicy(CACHE_ALIVE_TIME_SECONDS - 5, TimeUnit.SECONDS);
        multiRecallToDocumentListMap = MultiLevelCacheBuilder.createMultiLevelCacheBuilder()
                .addCache(recallLocalCache, recallRemoteCache)
                .expireAfterWrite(CACHE_ALIVE_TIME_SECONDS, TimeUnit.SECONDS)
//                .loader(this::load)
//                .refreshPolicy(policy)
                .buildCache();

        esRecallList = new CopyOnWriteArrayList<>();
        recallNameToRecallInstanceMap = new ConcurrentHashMap<>();
//        startScheduledTasks();
    }

    public boolean registerESSearch(ESCanGetStartRecall recall) {
        String recallName = recall.getRecallName();
        //TODO temp logic. remove the used RECALL
        if(recallName.contains("StatePoolRecall") || recallName.contains("PoolRecall")) {
            return true;
        }
        if (MXJudgeUtils.isNotEmpty(recallName)) {
            Map<String, BaseDataCollection.ESRequest> esRequestMap = recall.getESRequestMap();
            if (MXJudgeUtils.isNotEmpty(esRequestMap)) {
                esRecallList.add(recall);
                recallNameToRecallInstanceMap.put(recallName, recall);
                if (!needLoad(recallName)) {
                    return false;
                }
                loadESRecallDocument(recall);
                // 稍微休息50ms
                try {
                    sleep(ES_INTERVEL);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        return false;
    }

    private boolean needLoad(String recallName) {
        return null == multiRecallToDocumentListMap.get(recallName);
    }

    private void loadESRecallDocument(ESCanGetStartRecall recall) {
        String recallName = recall.getRecallName();
        log.info("it's time to run " + recallName);
        KeyDocumentListDataStruct recallDocumetListDataStruct = multiRecallToDocumentListMap.get(recallName);
        if (null == multiRecallToDocumentListMap.get(recallName)) {
            recallDocumetListDataStruct = new KeyDocumentListDataStruct(recallName);
            //TODO need to set back to {multiRecallToDocumentListMap}
        }

        Map<String, BaseDataCollection.ESRequest> esRequestMap = recall.getESRequestMap();

        int size = esRequestMap.size();
        for (Map.Entry<String, BaseDataCollection.ESRequest> entry : esRequestMap.entrySet()) {
            BaseDataCollection.ESRequest esRequest = entry.getValue();
            if (esRequest == null) {
                log.info(recallName + entry.getKey() + " recall failed, no request is prepared");
                continue;
            }
            List<BaseDocument> documents = new ArrayList<>();

            if (esRequest.isOnlyNeedFetchDetails()) {
                IDocumentProcessor processor = document -> document.scoreDocument.recallWeightScore = recall.getRecallDocumentWeight();
                documents = MXDataSource.details().get(esRequest.getIds(), esRequest.recallName, processor);
            } else {
                if ("pool".equals(esRequest.esItr)) {
                    documents = MXDataSource.ES().searchForDocuments(esRequest);
                } else if ("video".equals(esRequest.esItr)) {
                    documents = MXDataSource.videoES().searchForDocuments(esRequest);
                }
                for (BaseDocument document : documents) {
                    document.scoreDocument.recallWeightScore = recall.getRecallDocumentWeight();
                }
            }
            recallDocumetListDataStruct.setDocumentListByKey(entry.getKey(), documents);
            try {
                size--;
                if (size > 0) {
                    TimeUnit.MICROSECONDS.sleep(ES_INTERVEL);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        recall.doSomethingAfterLoad();
    }

    /**
     * TODO 这里改成了1分钟一把, 后续可以根据业务需要来调整频率
     */
    private void startScheduledTasks() {
        ScheduledExecutorService serviceNormal = Executors.newSingleThreadScheduledExecutor();
        ScheduledExecutorService serviceUrgent = Executors.newSingleThreadScheduledExecutor();

        int setScheduleSuccesfull = -3;
        while (setScheduleSuccesfull < 0) {
            try {
                serviceNormal.scheduleAtFixedRate(this::scheduledTaskNormal, 1, 1, TimeUnit.MINUTES);
                setScheduleSuccesfull = 0;
            } catch (Exception e) {
                setScheduleSuccesfull++;
                e.printStackTrace();
                log.error("set normal schedule job failed " + e.getMessage());
            }
        }

        setScheduleSuccesfull = -3;
        while (setScheduleSuccesfull < 0) {
            try {
                serviceUrgent.scheduleAtFixedRate(this::scheduledTaskUrgent, 3, 1, TimeUnit.SECONDS);
                setScheduleSuccesfull = 0;
            } catch (Exception e) {
                setScheduleSuccesfull++;
                e.printStackTrace();
                log.error("set urgent schedule job failed " + e.getMessage());
            }
        }
    }

    private KeyDocumentListDataStruct load(Object key) {
        if (!recallNameToRecallInstanceMap.containsKey(key)) {
            System.out.println("don't have this recall, please check!");
            return null;
        }

        ESCanGetStartRecall recall = recallNameToRecallInstanceMap.get(key);
        String recallName = recall.getRecallName();

        KeyDocumentListDataStruct struct = new KeyDocumentListDataStruct(recallName);

        Map<String, BaseDataCollection.ESRequest> esRequestMap = recall.getESRequestMap();
        int size = esRequestMap.size();
        for (Map.Entry<String, BaseDataCollection.ESRequest> entry : esRequestMap.entrySet()) {
            BaseDataCollection.ESRequest esRequest = entry.getValue();
            if (esRequest == null) {
                log.info(recallName + entry.getKey() + " recall failed, no request is prepared");
                continue;
            }
            List<BaseDocument> documents = new ArrayList<>();

            if (esRequest.isOnlyNeedFetchDetails()) {
                IDocumentProcessor processor = document -> document.scoreDocument.recallWeightScore = recall.getRecallDocumentWeight();
                documents = MXDataSource.details().get(esRequest.getIds(), esRequest.recallName, processor);
            } else {
                if ("pool".equals(esRequest.esItr)) {
                    documents = MXDataSource.ES().searchForDocuments(esRequest);
                } else if ("video".equals(esRequest.esItr)) {
                    documents = MXDataSource.videoES().searchForDocuments(esRequest);
                }
                for (BaseDocument document : documents) {
                    document.scoreDocument.recallWeightScore = recall.getRecallDocumentWeight();
                }
            }
            struct.setDocumentListByKey(entry.getKey(), documents);
            try {
                size--;
                if (size > 0) {
                    TimeUnit.MICROSECONDS.sleep(ES_INTERVEL);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        recall.doSomethingAfterLoad();
        return struct;
    }

    private void scheduledTaskNormal() {
        try {
            long time = System.currentTimeMillis() / 60000;
            for (ESCanGetStartRecall recall : esRecallList) {
                int recallPeriod = recall.scheduledPeriodSeconds() / 60;
                if (1 > recallPeriod) {
                    continue;
                }
                long randomFactor = time + recall.getRandomFactor();
                if (randomFactor % recallPeriod == 0) {
                    loadESRecallDocument(recall);
                    // 稍微休息50ms
                    try {
                        sleep(ES_INTERVEL);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("run scheduled urgent task failed : " + e.toString());
        }
    }

    private void scheduledTaskUrgent() {
        // 统计换算成秒
        try {
            long time = System.currentTimeMillis() / 1000;
            for (ESCanGetStartRecall recall : esRecallList) {
                int recallPeriod = recall.scheduledPeriodSeconds();
                // 大于等于1分钟的, 都走normal
                if (1 > recallPeriod || 60 < recallPeriod) {
                    continue;
                }
                long randomFactor = time + recall.getRandomFactor();
                if (randomFactor % recallPeriod == 0) {
                    loadESRecallDocument(recall);
                    // 稍微休息50ms
                    try {
                        sleep(ES_INTERVEL);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("run scheduled urgent task failed : " + e.toString());
        }
    }

    @Trace(dispatcher = true)
    public List<BaseDocument> getDocumentsForESRecall(ESCanGetStartRecall recall, String esQueryKey) {
//        KeyDocumentListDataStruct keyDocumentListDataStruct = multiRecallToDocumentListMap.get(recall.getRecallName());
//        if (keyDocumentListDataStruct != null) {
//            return keyDocumentListDataStruct.getDocumentListByKey(esQueryKey);
//        }
        return Collections.emptyList();
    }

}
