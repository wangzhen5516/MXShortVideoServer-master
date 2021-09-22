package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alicp.jetcache.Cache;
import com.alicp.jetcache.embedded.CaffeineCacheBuilder;
import com.newrelic.api.agent.Trace;
import mx.j2.recommend.component.configurable.ComponentConfig;
import mx.j2.recommend.component.configurable.config.RecallConfig;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXCollectionUtils;
import mx.j2.recommend.util.MXJudgeUtils;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 简单支持索引和排序的 ES 召回基类
 */
public abstract class BaseElasticSearchRecall extends BaseRecall<BaseDataCollection> {
    private static final String KEY_SORT = "sort";// 排序字段
    private static final String KEY_CACHE = "cache";// 是否缓存
    private static final int CACHE_TIME_IN_SECONDS = 30;// 缓存时间
    private static final JSONArray DEFAULT_SORT_JSON;// 默认排序方式

    // 缓存
    private final Cache<String, List<BaseDocument>> localCache = CaffeineCacheBuilder.createCaffeineCacheBuilder()
            .expireAfterWrite(CACHE_TIME_IN_SECONDS, TimeUnit.SECONDS)
            .buildCache();

    static {
        DEFAULT_SORT_JSON = new JSONArray();
        JSONObject sortCore = new JSONObject();
        JSONObject script = new JSONObject();
        script.put("script", "Math.random()");
        script.put("type", "number");
        script.put("order", "asc");
        sortCore.put("_script", script);
        DEFAULT_SORT_JSON.add(sortCore);
    }

    @Override
    public void registerConfig(Map<String, Class> outConfMap) {
        super.registerConfig(outConfMap);
        outConfMap.put(RecallConfig.KEY_ES_INDEX, String.class);
        outConfMap.put(RecallConfig.KEY_SIZE, Integer.class);
        outConfMap.put(KEY_SORT, ComponentConfig.Items.StringList.class);
        outConfMap.put(KEY_CACHE, Boolean.class);
    }

    @Override
    @Trace(dispatcher = true)
    public void recall(BaseDataCollection dc) {
        boolean isCacheEnabled = isCacheEnabled();
        List<BaseDocument> result = null;
        DefineTool.RecallFrom from = DefineTool.RecallFrom.LOCAL;

        // 有缓存先从缓存拿
        if (isCacheEnabled) {
            result = getCache();
        }

        // 缓存没开或没有，去 ES 拿
        if (MXJudgeUtils.isEmpty(result)) {
            result = getES();
            from = DefineTool.RecallFrom.ES;
        }

        // 都没拿到，空手回
        if (MXJudgeUtils.isEmpty(result)) {
            return;
        }

        setResult(dc, result);
        setResultInfo(dc, result.size(), from);

        // 写缓存（缓存开启且缓存为空）
        if (isCacheEnabled && DefineTool.RecallFrom.ES.equals(from)) {
            setCache(result);
        }
    }

    /**
     * 从 ES 拿
     */
    @Nullable
    private List<BaseDocument> getES() {
        List<String> ids = getIdsFromES();
        if (MXJudgeUtils.isEmpty(ids)) {
            return null;
        }

        return MXDataSource.details().get(ids);
    }

    /**
     * 从 ES 拿所有 ID
     */
    private List<String> getIdsFromES() {
        String index = String.format(DefineTool.ES.SEARCH_FORMAT, config.getEsIndex());
        JSONObject content = constructQueryContent();
        return MXDataSource.ES().sendSyncSearch(index, content.toJSONString());
    }

    /**
     * 构建查询体
     */
    private JSONObject constructQueryContent() {
        JSONObject content = new JSONObject();

        // 数量
        content.put("size", getSize());

        // 排序
        JSONArray sort = parseSortConfig();
        if (MXJudgeUtils.isEmpty(sort)) {
            content.put("sort", DEFAULT_SORT_JSON);
        } else {
            content.put("sort", sort);
        }

        // 不需要返回详情
        content.put("_source", false);

        return content;
    }

    /**
     * 解析排序字段
     */
    private JSONArray parseSortConfig() {
        JSONArray sort = new JSONArray();

        List<String> sortList = getSortList();
        if (MXJudgeUtils.isEmpty(sortList)) {
            return sort;
        }

        String format = "{'%s':{'order': '%s'}}";
        for (String sortIt : sortList) {
            String[] temp = sortIt.split(ComponentConfig.Format.VALUE_INTERNAL_SEPARATOR);

            // 合法性检查
            if (temp.length < 2) {
                continue;
            }

            sort.add(JSON.parseObject(String.format(format, temp[0], temp[1])));
        }

        return sort;
    }

    /**
     * 缓存 key
     */
    private String getCacheKey() {
        return String.format("%s_%s", getEsIndex(), getResultKey());
    }

    /**
     * 读缓存
     */
    @Nullable
    private List<BaseDocument> getCache() {
        List<BaseDocument> cache = localCache.get(getCacheKey());

        if (MXJudgeUtils.isNotEmpty(cache)) {
            return MXCollectionUtils.shallowCopy(cache);
        }

        return null;
    }

    /**
     * 写缓存
     */
    private void setCache(List<BaseDocument> documents) {
        localCache.put(getCacheKey(), MXCollectionUtils.shallowCopy(documents));
    }

    /**
     * 是否开启缓存
     */
    private boolean isCacheEnabled() {
        return config.getBoolean(KEY_CACHE);
    }

    /**
     * 排序字段配置
     */
    private List<String> getSortList() {
        return config.getStringList(KEY_SORT);
    }
}
