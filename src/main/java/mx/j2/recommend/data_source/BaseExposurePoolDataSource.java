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
 * 曝光池数据源基类
 */
public abstract class BaseExposurePoolDataSource extends BaseDataSource {
    private Logger logger = LogManager.getLogger(BaseExposurePoolDataSource.class);
    private static final int CACHE_VOLUME = 256;
    private static final int CACHE_ALIVE_TIME_SECONDS = 40;
    private static final int ES_INTERVAL = 200;
    private static final int CACHE_LIMIT = 2;
    private final Cache<String, List<BaseDocument>> cache;// 多级缓存
    private final ExecutorService loadDocumentsExecutorService = Executors.newFixedThreadPool(3);
    private volatile Map<String, List<BaseDocument>> documentsMap;// 内存缓存
    private volatile Map<String, AtomicInteger> counter;

    public BaseExposurePoolDataSource() {
        RedisURI redisUri = RedisURI.Builder.redis(Conf.getRedisCacheHost(), Conf.getRedisCachePort())
                .withTimeout(Duration.ofMillis(Conf.getJedisClusterSocketTimeout()))
                .build();
        RedisClusterClient redisClient = RedisClusterClient.create(redisUri);
        redisClient.setOptions(ClusterClientOptions.builder()
                .autoReconnect(true)
                .build());

        documentsMap = new ConcurrentHashMap<>(CACHE_VOLUME / 2);
        counter = new ConcurrentHashMap<>(CACHE_VOLUME / 2);

        Cache<String, List<BaseDocument>> recallLocalCache = CaffeineCacheBuilder.createCaffeineCacheBuilder()
                .limit(CACHE_VOLUME)
                .cacheNullValue(true)
                .buildCache();

        Cache<String, List<BaseDocument>> recallRemoteCache = RedisLettuceCacheBuilder.createRedisLettuceCacheBuilder()
                .keyConvertor(FastjsonKeyConvertor.INSTANCE)
                .valueDecoder(JavaValueDecoder.INSTANCE)
                .valueEncoder(JavaValueEncoder.INSTANCE)
                .redisClient(redisClient)
                .keyPrefix(getRedisKeyPrefix())
                .buildCache();

        RefreshPolicy policy = RefreshPolicy.newPolicy(CACHE_ALIVE_TIME_SECONDS - 5, TimeUnit.SECONDS);
        cache = MultiLevelCacheBuilder.createMultiLevelCacheBuilder()
                .addCache(recallLocalCache, recallRemoteCache)
                .loader(this::load)
                .refreshPolicy(policy)
                .buildCache();

        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.schedule(this::preload, 35, TimeUnit.SECONDS);

        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        service.scheduleAtFixedRate(this::fill, 15, CACHE_ALIVE_TIME_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * 启动时预加载
     */
    private void preload() {
        ESCanGetStartRecall recall = getRecall();
        if (recall == null) {
            return;
        }

        Map<String, BaseDataCollection.ESRequest> esRequestMap = recall.getESRequestMap();
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
                            cache.put(entry.getKey(), documents);
                        });
                        try {
                            size--;
                            if (size > 0) {
                                TimeUnit.MICROSECONDS.sleep(ES_INTERVAL);
                            }
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        cache.put(entry.getKey(), new ArrayList<>());
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
        ESCanGetStartRecall recall = getRecall();
        if (recall == null) {
            return MXCollectionUtils.EMPTY_LIST;
        }

        String s = String.valueOf(key);
        Map<String, BaseDataCollection.ESRequest> esRequestMap = recall.getESRequestMap();

        if (!esRequestMap.containsKey(s)) {
            return MXCollectionUtils.EMPTY_LIST;
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

    /**
     * 先拿到 id 列表
     */
    private List<String> search(BaseDataCollection.ESRequest esRequest) {
        List<String> ids = MXDataSource.ES().searchForIds(esRequest);
        return new ArrayList<>(ids);
    }

    private void fill() {
        Set<String> set = documentsMap.keySet();
        set.forEach(s -> {
            List<BaseDocument> value = cache.get(s);
            if (null != value) {
                documentsMap.put(s, value);
            }
        });
    }

    private void fill(Set<String> keys) {
        assert MXJudgeUtils.isNotEmpty(keys);

        keys.forEach(key -> {
            logger.info("fill key " + key + " size: " + cache.get(key).size());
            documentsMap.put(key, cache.get(key));
        });
    }

    @Trace(dispatcher = true)
    public List<BaseDocument> get(String key) {
        if (null == documentsMap.get(key)) {
            ESCanGetStartRecall recall = getRecall();
            if (null != recall) {
                Map<String, BaseDataCollection.ESRequest> esRequestMap = recall.getESRequestMap();
                if (esRequestMap.containsKey(key)) {
                    cache.put(key, null);
                    cache.get(key);
                }
            }
            documentsMap.computeIfAbsent(key, k -> new ArrayList<>());
        }
        return new ArrayList<>(documentsMap.get(key));
    }

    private String getRedisKeyPrefix() {
        return getPoolID() + "ExposurePool";
    }

    /**
     * 池子唯一标识
     */
    abstract String getPoolID();

    /**
     * 对应的召回器
     */
    abstract ESCanGetStartRecall getRecall();
}
