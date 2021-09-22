package mx.j2.recommend.data_source;

import com.alicp.jetcache.Cache;
import com.alicp.jetcache.CacheGetResult;
import com.alicp.jetcache.MultiGetResult;
import com.alicp.jetcache.embedded.CaffeineCacheBuilder;
import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author ：zhongrenli
 * @date ：Created in 3:10 下午 2020/7/10
 */
public class DetailPoolDataSource extends BaseDataSource {

    /**
     * cache 数量
     */
    private static final int CACHE_VOLUME = 100000;

    /**
     * cache 时间, 单位（秒）
     */
    private static final int ALIVE_TIME = 15;

    /**
     * doc 本地缓存
     */
    private final Cache<String, BaseDocument> docDetailLocalCache = CaffeineCacheBuilder.createCaffeineCacheBuilder()
            .limit(CACHE_VOLUME)
            .expireAfterWrite(ALIVE_TIME, TimeUnit.MINUTES)
            .buildCache();

    public DetailPoolDataSource() {
        init();
    }

    /**
     * 初始化
     */
    private void init() {
    }

    /**
     * 根据id列表，返回详情
     *
     * @param inputIds 输入publisherIds
     * @return list<BaseDocument>
     */
    @Trace(dispatcher = true)
    public List<BaseDocument> getVideoDetailByPublisherIds(BaseDataCollection dc, int recallId, List<String> inputIds, DefineTool.CategoryEnum category) {
        if (MXJudgeUtils.isEmpty(inputIds)) {
            return Collections.emptyList();
        }
        List<String> ids = new ArrayList<>(inputIds);
        List<BaseDocument> cloneList = new ArrayList<>();

        if (null == category || DefineTool.CategoryEnum.DEFAULT.equals(category)) {
            return Collections.emptyList();
        }

        Set<String> keys = new HashSet<>();
        ids.forEach(id -> {
            keys.add(String.format("%s:%s", id, "_videos"));
        });

        Map<String, Integer> indexMap = new HashMap<>(ids.size());
        for (int i = 0; i < ids.size(); i++) {
            indexMap.put(ids.get(i), i);
        }

        List<BaseDocument> detailList = new ArrayList<>();
        List<String> fromCacheIdList = new ArrayList<>();
        MultiGetResult<String, BaseDocument> getResult = docDetailLocalCache.GET_ALL(keys);
        if (getResult.isSuccess()) {
            fillCacheFromLocal(getResult, detailList, fromCacheIdList);
        }

        return cloneList;
    }

    private void fillCacheFromLocal(MultiGetResult<String, BaseDocument> getResult, List<BaseDocument> detailList, List<String> idList) {
        Map<String, CacheGetResult<BaseDocument>> resultMap = getResult.getValues();
        for (Map.Entry<String, CacheGetResult<BaseDocument>> entry : resultMap.entrySet()) {
            CacheGetResult<BaseDocument> cacheGetResult = entry.getValue();
            if (cacheGetResult.isSuccess()) {
                BaseDocument bdoc = cacheGetResult.getValue();
                if (null != bdoc) {
                    detailList.add(bdoc);
                    idList.add(bdoc.id);
                }
            }
        }
    }
}
