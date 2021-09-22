package mx.j2.recommend.data_source;

import com.alicp.jetcache.Cache;
import com.alicp.jetcache.MultiLevelCacheBuilder;
import com.alicp.jetcache.RefreshPolicy;
import com.alicp.jetcache.embedded.CaffeineCacheBuilder;
import com.alicp.jetcache.redis.lettuce.RedisLettuceCacheBuilder;
import com.alicp.jetcache.support.FastjsonKeyConvertor;
import com.alicp.jetcache.support.JavaValueDecoder;
import com.alicp.jetcache.support.JavaValueEncoder;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.interfaces.IDocumentProcessor;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.manager.MXManager;
import mx.j2.recommend.recall_data_in_mem.ESCanGetStartRecall;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
/**
 * @author xiang.zhou
 * @description
 * @date 2021/2/9
 */
public class TempLanguagePoolDataSource {

    private static final String POOL_RECALL = "CountryPoolRecall";

    private static final List<BaseDocument> EMPTY_LIST = new ArrayList<>();

    private final Cache<String, List<BaseDocument>> poolToDocumentList;

    private static final int CACHE_VOLUME = 10;

    private static final int ES_INTERVEL = 200;

    private static final int CACHE_ALIVE_TIME_SECONDS = 20;

    public TempLanguagePoolDataSource() {

        RedisURI redisUri = RedisURI.Builder.redis(Conf.getRedisCacheHost(), Conf.getRedisCachePort())
                .withTimeout(Duration.ofMillis(Conf.getJedisClusterSocketTimeout()))
                .build();
        RedisClusterClient redisClient = RedisClusterClient.create(redisUri);
        redisClient.setOptions(ClusterClientOptions.builder()
                .autoReconnect(true)
                .build());

        Cache<String, List<BaseDocument>> recallLocalCache = CaffeineCacheBuilder.createCaffeineCacheBuilder()
                .limit(CACHE_VOLUME)
                .cacheNullValue(true)
                .buildCache();

        Cache<String, List<BaseDocument>> recallRemoteCache = RedisLettuceCacheBuilder.createRedisLettuceCacheBuilder()
                .keyConvertor(FastjsonKeyConvertor.INSTANCE)
                .valueDecoder(JavaValueDecoder.INSTANCE)
                .valueEncoder(JavaValueEncoder.INSTANCE)
                .redisClient(redisClient)
                .keyPrefix("CountryRecallCache")
                .buildCache();

        RefreshPolicy policy = RefreshPolicy.newPolicy(CACHE_ALIVE_TIME_SECONDS, TimeUnit.SECONDS);
        poolToDocumentList = MultiLevelCacheBuilder.createMultiLevelCacheBuilder()
                .addCache(recallLocalCache, recallRemoteCache)
                .expireAfterWrite(CACHE_ALIVE_TIME_SECONDS+4, TimeUnit.SECONDS)
                .loader(this::load)
                .refreshPolicy(policy)
                .buildCache();

        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.schedule(this::preLoad, 60, TimeUnit.SECONDS);
    }

    private void preLoad() {
        assert MXManager.recall().getComponentInstance(POOL_RECALL) != null;

        ESCanGetStartRecall recall = (ESCanGetStartRecall) MXManager.recall().getComponentInstance(POOL_RECALL);
        Map<String, BaseDataCollection.ESRequest> esRequestMap = recall.getESRequestMap();

        int size = esRequestMap.size();
        for (Map.Entry<String, BaseDataCollection.ESRequest> entry : esRequestMap.entrySet()) {

            BaseDataCollection.ESRequest esRequest = entry.getValue();
            if (esRequest == null) {
                continue;
            }
            List<BaseDocument> documents = search(esRequest, recall);
            poolToDocumentList.put(entry.getKey(), documents);
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

    private List<BaseDocument> load(Object key) {
        if (MXManager.recall().getComponentInstance(POOL_RECALL) == null) {
            return EMPTY_LIST;
        }

        ESCanGetStartRecall recall = (ESCanGetStartRecall) MXManager.recall().getComponentInstance(POOL_RECALL);
        Map<String, BaseDataCollection.ESRequest> esRequestMap = recall.getESRequestMap();
        if (!esRequestMap.containsKey(key)) {
            return EMPTY_LIST;
        }

        BaseDataCollection.ESRequest esRequest = esRequestMap.get(key);
        List<BaseDocument> documents = search(esRequest, recall);
        return new ArrayList<>(documents);
    }

    private List<BaseDocument> search(BaseDataCollection.ESRequest esRequest, ESCanGetStartRecall recall) {
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
        return new ArrayList<>(documents);
    }

    public List<BaseDocument> getDocumentList(String key) {
        return new ArrayList<>(poolToDocumentList.get(key));
    }
}
