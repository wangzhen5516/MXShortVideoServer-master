package mx.j2.recommend.data_source;

import com.alibaba.fastjson.JSONObject;
import com.alicp.jetcache.*;
import com.alicp.jetcache.embedded.CaffeineCacheBuilder;
import com.alicp.jetcache.embedded.LinkedHashMapCacheBuilder;
import com.alicp.jetcache.redis.lettuce.RedisLettuceCacheBuilder;
import com.alicp.jetcache.support.FastjsonKeyConvertor;
import com.alicp.jetcache.support.JavaValueDecoder;
import com.alicp.jetcache.support.JavaValueEncoder;
import com.google.common.collect.Lists;
import com.newrelic.api.agent.Trace;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.document.ShortDocument;
import mx.j2.recommend.data_model.interfaces.IDocumentProcessor;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author ：zhongrenli
 */
public class DetailDataSource extends BaseDataSource {
    private static final Logger logger = LogManager.getLogger(ElasticCacheSource.class);

    private static final String LOCAL_CACHE_SUFFIX = "objects";

    private Cache<String, BaseDocument> globalCache;

    /**
     * cache 数量
     */
    private static final int CACHE_VOLUME = 200000;

    /**
     * cache 时间, 单位（分钟）
     */
    private static final int ALIVE_TIME_IN_CACHE = 2;

    /**
     * 统计缓存成功率
     */
    private static final AtomicInteger i = new AtomicInteger(0);
    private static final AtomicInteger sum = new AtomicInteger(0);

    /**
     * 二级缓存
     */
    private Cache<String, BaseDocument> multiLevelCache;

    public DetailDataSource() {
        init();
    }

    /**
     * 初始化
     */
    private void init() {
        RedisURI redisUri = RedisURI.Builder.redis(Conf.getRedisCacheHost(), Conf.getRedisCachePort())
                .withTimeout(Duration.ofMillis(Conf.getJedisClusterSocketTimeout()))
                .build();
        RedisClusterClient redisClient = RedisClusterClient.create(redisUri);
        redisClient.setOptions(ClusterClientOptions.builder()
                .autoReconnect(true)
                .build());

        globalCache = LinkedHashMapCacheBuilder.createLinkedHashMapCacheBuilder()
                .limit(CACHE_VOLUME)
                .expireAfterWrite(1, TimeUnit.MINUTES)
                .cacheNullValue(true)
                .buildCache();

        Cache<String, JSONObject> docDetailLocalCache = CaffeineCacheBuilder.createCaffeineCacheBuilder()
                .limit(CACHE_VOLUME)
                .cacheNullValue(true)
                .buildCache();

        Cache<String, JSONObject> docDetailRemoteCache = RedisLettuceCacheBuilder.createRedisLettuceCacheBuilder()
                .keyConvertor(FastjsonKeyConvertor.INSTANCE)
                .valueDecoder(JavaValueDecoder.INSTANCE)
                .valueEncoder(JavaValueEncoder.INSTANCE)
                .keyPrefix("docCache")
                .redisClient(redisClient)
                .buildCache();

        RefreshPolicy policy = RefreshPolicy.newPolicy(100, TimeUnit.SECONDS)
                .stopRefreshAfterLastAccess(10, TimeUnit.MINUTES);
        multiLevelCache = MultiLevelCacheBuilder.createMultiLevelCacheBuilder()
                .addCache(docDetailLocalCache, docDetailRemoteCache)
                .expireAfterWrite(ALIVE_TIME_IN_CACHE, TimeUnit.MINUTES)
                .refreshPolicy(policy)
                .loader(this::load)
                .cacheNullValue(true)
                .buildCache();

        logger.info("{\"dataSourceInfo\":\"[DetailDataSource init successfully]\"}");
    }


    public List<BaseDocument> get(Set<String> inputIds, String recallName) {
        List<String> ids = new ArrayList<>(inputIds);
        return get(ids, recallName);
    }

    public List<BaseDocument> get(List<String> inputIds, String recallName) {
        return get(inputIds, recallName, null);
    }

    public List<BaseDocument> get(Set<String> inputIds, String recallName, IDocumentProcessor processor) {
        List<String> ids = new ArrayList<>(inputIds);
        return get(ids, recallName, processor);
    }

    public List<BaseDocument> get(List<String> inputIds, String recallName, IDocumentProcessor processor) {
        List<BaseDocument> objects = get(inputIds);
        toProcessor(objects, recallName, processor);
        return objects;
    }

    public List<BaseDocument> get(List<String> inputIds, IDocumentProcessor processor) {
        List<BaseDocument> objects = get(inputIds);
        toProcessor(objects, processor);
        return objects;
    }

    @Trace(dispatcher = true)
    public List<BaseDocument> get(List<String> inputIds) {
        if (MXJudgeUtils.isEmpty(inputIds)) {
            return new ArrayList<>();
        }
        List<String> ids = new ArrayList<>(inputIds);
        List<BaseDocument> cloneList = new ArrayList<>();

        // 写死就是短视频
        DefineTool.CategoryEnum category = DefineTool.CategoryEnum.SHORT_VIDEO;

        Set<String> keys = new HashSet<>();
        ids.forEach(id -> keys.add(String.format("%s:%s:%s", id, category.getName(), LOCAL_CACHE_SUFFIX)));

        // 此处保证，返回的列表顺序与传入的一致
        Map<String, Integer> indexMap = new HashMap<>(ids.size());
        for (int i = 0; i < ids.size(); i++) {
            indexMap.put(ids.get(i), i);
        }

        List<BaseDocument> detailList = new ArrayList<>();
        List<String> fromCacheIdList = new ArrayList<>();

        MultiGetResult<String, BaseDocument> getResult = globalCache.GET_ALL(keys);
        if (getResult.isSuccess()) {
            fillCacheFromLocal(getResult, detailList, fromCacheIdList);
        }

        if (MXJudgeUtils.isNotEmpty(fromCacheIdList)) {
            deepClone(detailList, cloneList);
            ids.removeAll(fromCacheIdList);
            detailList.clear();

            if (ids.isEmpty()) {
                cloneList.sort(Comparator.comparingInt(o -> indexMap.get(o.getId())));
                return cloneList;
            }
        }

        Map<String, JSONObject> objectsFromCA = MXDataSource.cassandra().fetchDetails(ids);
        List<BaseDocument> documentFromCA = new ArrayList<>();
        if (MXJudgeUtils.isNotEmpty(objectsFromCA) && MXJudgeUtils.isNotEmpty(objectsFromCA.values())) {
            documentFromCA = toDocuments(new ArrayList<>(objectsFromCA.values()));
        }

        if (MXJudgeUtils.isNotEmpty(documentFromCA)) {
            Map<String, BaseDocument> storeMap = new HashMap<>(objectsFromCA.size() * 2);
            documentFromCA.forEach(object -> {
                String key = String.format("%s:%s:%s", object.getId(), category.getName(), LOCAL_CACHE_SUFFIX);
                storeMap.put(key, object);
            });

            multiLevelCache.PUT_ALL(storeMap);
            globalCache.PUT_ALL(storeMap);

            detailList.addAll(documentFromCA);
            deepClone(detailList, cloneList);
        }

        cloneList.removeIf(doc -> null == doc.getId());

        if (MXJudgeUtils.isNotEmpty(cloneList)) {
            cloneList.sort(Comparator.comparingInt(o -> indexMap.get(o.getId())));
        }
        return cloneList;
    }

    private BaseDocument load(Object key) {
        Map<String, JSONObject> map = MXDataSource.cassandra().fetchDetails(Lists.newArrayList(String.valueOf(key)));
        JSONObject value = map.getOrDefault(String.valueOf(key), null);
        BaseDocument shortDocument = new ShortDocument().loadJsonObj(value, DefineTool.CategoryEnum.SHORT_VIDEO, "");
        if (shortDocument != null) {
            globalCache.PUT(String.valueOf(key), shortDocument);
        }
        return shortDocument;
    }

    private List<BaseDocument> toDocuments(List<JSONObject> objects) {
        List<BaseDocument> resultDocumentList = new ArrayList<>();
        for (JSONObject object : objects) {
            // 写死就是短视频，别整太麻烦，以后扩展也没啥 W 的
            BaseDocument shortDocument = new ShortDocument().loadJsonObj(object, DefineTool.CategoryEnum.SHORT_VIDEO, "");
            if (null != shortDocument) {
                resultDocumentList.add(shortDocument);
            }
        }
        return resultDocumentList;
    }

    private void toProcessor(List<BaseDocument> resultDocumentList, String recallName, IDocumentProcessor processor) {
        if (MXJudgeUtils.isNotEmpty(resultDocumentList)) {
            for (BaseDocument shortDocument : resultDocumentList) {
                if (shortDocument != null) {
                    shortDocument.setRecallName(recallName);
                    if (processor != null) {
                        processor.process(shortDocument);
                    }
                }
            }
        }
    }

    private void toProcessor(List<BaseDocument> resultDocumentList, IDocumentProcessor processor) {
        if (MXJudgeUtils.isNotEmpty(resultDocumentList) && processor != null) {
            for (BaseDocument shortDocument : resultDocumentList) {
                if (shortDocument != null) {
                    processor.process(shortDocument);
                }
            }
        }
    }

    @Trace(dispatcher = true)
    private void fillCacheFromLocal(MultiGetResult<String, BaseDocument> getResult, List<BaseDocument> detailList, List<String> idList) {
        Map<String, CacheGetResult<BaseDocument>> resultMap = getResult.getValues();
        for (Map.Entry<String, CacheGetResult<BaseDocument>> entry : resultMap.entrySet()) {
            CacheGetResult<BaseDocument> cacheGetResult = entry.getValue();
            sum.getAndIncrement();
            if (cacheGetResult.isSuccess()) {
                i.getAndIncrement();
                BaseDocument shortDocument = cacheGetResult.getValue();
                if (null != shortDocument && MXJudgeUtils.isNotEmpty(shortDocument.getId())) {
                    detailList.add(shortDocument);
                    idList.add(shortDocument.getId());
                }
            }
            if (sum.intValue() == 10000000) {// 统计的区间
                int sumIntern = sum.intValue();
                int iIntern = i.intValue();
                sum.set(0);
                i.set(0);
                logger.info(String.format("CacheHit:%d-%d ", iIntern, sumIntern) + iIntern * 1.0 / sumIntern);
            }
        }
    }

    private void deepClone(List<BaseDocument> source, List<BaseDocument> target) {
        target.addAll(source);
    }
}
