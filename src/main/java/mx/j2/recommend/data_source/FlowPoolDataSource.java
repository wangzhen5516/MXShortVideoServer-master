package mx.j2.recommend.data_source;

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
import mx.j2.recommend.data_model.interfaces.IDocumentProcessor;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.manager.MXManager;
import mx.j2.recommend.recall_data_in_mem.ESCanGetStartRecall;
import mx.j2.recommend.util.MXCollectionUtils;
import mx.j2.recommend.util.MXJudgeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author ：zhongrenli
 * @date ：Created in 11:23 上午 2021/2/4
 */
public class FlowPoolDataSource {

    Logger log = LogManager.getLogger(FlowPoolDataSource.class);

    private static final String POOL_RECALL = "PoolRecall";

    private static final List<BaseDocument> EMPTY_LIST = new ArrayList<>();

    private final Cache<String, List<BaseDocument>> poolFollowContent;

    private static volatile Map<String, List<BaseDocument>> documentsMap;

    private static final int CACHE_VOLUME = 256;

    private static final int CACHE_ALIVE_TIME_SECONDS = 40;

    private static final int ES_INTERVEL = 200;

    private static final long FLOW_CONTENT_SIZE = 500;

    private volatile static Map<String, AtomicInteger> counter;

    private static final int CACHE_LIMIT = 2;

    private final ExecutorService loadDocumentsExecutorService = Executors.newFixedThreadPool(3);

    public FlowPoolDataSource() {

        RedisURI redisUri = RedisURI.Builder.redis(Conf.getRedisCacheHost(), Conf.getRedisCachePort())
                .withTimeout(Duration.ofMillis(Conf.getJedisClusterSocketTimeout()))
                .build();
        RedisClusterClient redisClient = RedisClusterClient.create(redisUri);
        redisClient.setOptions(ClusterClientOptions.builder()
                .autoReconnect(true)
                .build());

        documentsMap = new ConcurrentHashMap<>(CACHE_VOLUME/2);
        counter = new ConcurrentHashMap<>(CACHE_VOLUME/2);

        Cache<String, List<BaseDocument>> recallLocalCache = CaffeineCacheBuilder.createCaffeineCacheBuilder()
                .limit(CACHE_VOLUME)
                .cacheNullValue(true)
                .buildCache();

        Cache<String, List<BaseDocument>> recallRemoteCache = RedisLettuceCacheBuilder.createRedisLettuceCacheBuilder()
                .keyConvertor(FastjsonKeyConvertor.INSTANCE)
                .valueDecoder(JavaValueDecoder.INSTANCE)
                .valueEncoder(JavaValueEncoder.INSTANCE)
                .redisClient(redisClient)
                .keyPrefix("FollowContentPool")
                .buildCache();
        RefreshPolicy policy = RefreshPolicy.newPolicy(CACHE_ALIVE_TIME_SECONDS - 5, TimeUnit.SECONDS);
        poolFollowContent = MultiLevelCacheBuilder.createMultiLevelCacheBuilder()
                .addCache(recallLocalCache, recallRemoteCache)
                .loader(this::load)
                .refreshPolicy(policy)
                .buildCache();

        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.schedule(this::preLoadFollowContent, 30, TimeUnit.SECONDS);

        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        service.scheduleAtFixedRate(this::fill, 10, CACHE_ALIVE_TIME_SECONDS, TimeUnit.SECONDS);
    }

    private void preLoadFollowContent() {
        Set<String> poolSet = MXDataSource.pools().all().keySet();
        assert !poolSet.isEmpty();
        assert MXManager.recall().getComponentInstance(POOL_RECALL) != null;

        ESCanGetStartRecall recall = (ESCanGetStartRecall) MXManager.recall().getComponentInstance(POOL_RECALL);
        Map<String, BaseDataCollection.ESRequest> esRequestMap = recall.getESRequestMap();
        //Map<String, SearchSourceBuilder> builderMap = recall.getSearchSourceBuilderMap();

        List<String> idList;
        int size = esRequestMap.size();
        if (MXJudgeUtils.isNotEmpty(esRequestMap)) {
            for (Map.Entry<String, BaseDataCollection.ESRequest> entry : esRequestMap.entrySet()) {
                if (entry != null && MXJudgeUtils.isNotEmpty(entry.getKey())) {
                    try {
                        idList = search(entry.getValue());
                        List<String> finalIdList = idList;
                        IDocumentProcessor processor = document -> document.scoreDocument.recallWeightScore = recall.getRecallDocumentWeight();
                        loadDocumentsExecutorService.execute(() -> {
                            List<BaseDocument> documents = MXDataSource.details().get(finalIdList, recall.getRecallName(), processor);
                            poolFollowContent.put(entry.getKey(), documents);
                        });
                        try {
                            size--;
                            if (size > 0) {
                                TimeUnit.MICROSECONDS.sleep(ES_INTERVEL);
                            }
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        poolFollowContent.put(entry.getKey(), new ArrayList<>());
                    }
                }
            }
        }
        try {
            loadDocumentsExecutorService.awaitTermination(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            e.printStackTrace();
        }
        loadDocumentsExecutorService.shutdown();
        fill(esRequestMap.keySet());
    }

    private List<BaseDocument> load(Object key) {
        if (MXManager.recall().getComponentInstance(POOL_RECALL) == null) {
            return EMPTY_LIST;
        }
        String s = String.valueOf(key);
        ESCanGetStartRecall recall = (ESCanGetStartRecall) MXManager.recall().getComponentInstance(POOL_RECALL);
        Map<String, BaseDataCollection.ESRequest> esRequestMap = recall.getESRequestMap();
        //Map<String, SearchSourceBuilder> builderMap = recall.getSearchSourceBuilderMap();

        if (!esRequestMap.containsKey(s)) {
            return EMPTY_LIST;
        }
        List<String> idList = search(esRequestMap.get(s));
        IDocumentProcessor processor = document -> document.scoreDocument.recallWeightScore = recall.getRecallDocumentWeight();
        List<BaseDocument> documents = MXDataSource.details().get(idList, recall.getRecallName(), processor);

        if (MXCollectionUtils.isEmpty(documents)) {
            if (counter.containsKey(s)) {
                int value = counter.get(s).getAndIncrement();
                if (value >= CACHE_LIMIT) {
                    documentsMap.put(s, documents);
                }
            } else {
                counter.put(s, new AtomicInteger(1));
            }
        } else {
            documentsMap.put(s, documents);
        }

        return new ArrayList<>(documents);
    }

    /*private List<String> getDocumentByScroll(BaseDataCollection.ESRequest esRequest, SearchSourceBuilder builder) {
        List<String> ids = new ArrayList<>();
        if (esRequest != null && "pool".equals(esRequest.esItr) && MXJudgeUtils.isNotEmpty(esRequest.poolIndex)) {
            ids = MXDataSource.ES().searchForDocumentsScroll(esRequest.poolIndex, builder, esRequest.recallName, FLOW_CONTENT_SIZE);
        }

        return ids;
    }*/

    private List<String> search(BaseDataCollection.ESRequest esRequest) {
        List<String> ids = new ArrayList<>();
        if (esRequest.isOnlyNeedFetchDetails()) {
            return esRequest.getIds();
        } else {
            if ("pool".equals(esRequest.esItr)) {
                ids = MXDataSource.ES().searchForIds(esRequest);
            } else if ("video".equals(esRequest.esItr)) {
                ids = MXDataSource.videoES().searchForIds(esRequest);
            }
        }
        return new ArrayList<>(ids);
    }

    private void fill() {
        Set<String> set = documentsMap.keySet();
        set.forEach(s -> {
            List<BaseDocument> value = poolFollowContent.get(s);
            if (null != value){
                documentsMap.put(s, value);
            }
        });
    }

    private void fill(Set<String> keys) {
        assert MXCollectionUtils.isNotEmpty(keys);

        keys.forEach(key -> {
            log.info("fill key " + key + " size: " + poolFollowContent.get(key).size());
            documentsMap.put(key, poolFollowContent.get(key));
        });
    }

    @Trace(dispatcher = true)
    public List<BaseDocument> getFollowContent(String key) {
        if (null == documentsMap.get(key)) {
            ESCanGetStartRecall recall = (ESCanGetStartRecall) MXManager.recall().getComponentInstance(POOL_RECALL);
            if (null != recall) {
                Map<String, BaseDataCollection.ESRequest> esRequestMap = recall.getESRequestMap();
                if (esRequestMap.containsKey(key)) {
                    poolFollowContent.put(key, null);
                    poolFollowContent.get(key);
                }
            }
            documentsMap.computeIfAbsent(key, k -> new ArrayList<>());
        }

        return new ArrayList<>(documentsMap.get(key));
    }
}
