package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alicp.jetcache.Cache;
import com.alicp.jetcache.RefreshPolicy;
import com.alicp.jetcache.embedded.CaffeineCacheBuilder;
import com.newrelic.api.agent.Trace;
import mx.j2.recommend.component.configurable.config.RecallConfig;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author ：zhongrenli
 * @date ：Created in 4:03 下午 2021/6/21
 */
public class SpecialPoolRecall extends BaseRecall<BaseDataCollection>{

    private static final String KEY_SORT = "sort";
    private static final String KEY_CACHED = "cached";
    private static final String KEY_CACHED_TIME = "cached_time";
    private static final int DEFAULT_CACHE_TIME = 30;
    private static final int CACHE_VOLUME = 5000;
    private static final JSONArray DEFAULT_SORT_JSON;
    private static final String REQUEST_URL_FORMAT = "/%s/_search?pretty=false";

    RefreshPolicy policy = RefreshPolicy.newPolicy(DEFAULT_CACHE_TIME - 5, TimeUnit.SECONDS);
    private final Cache<String, List<BaseDocument>> localCache = CaffeineCacheBuilder.createCaffeineCacheBuilder()
            .limit(CACHE_VOLUME)
            .loader(this::load)
            .refreshPolicy(policy)
            .cacheNullValue(true)
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
    @Trace(dispatcher = true)
    public void recall(BaseDataCollection dc) {
        if (!check()) {
            return;
        }

        if (config.getBoolean(KEY_CACHED)) {
            List<BaseDocument> cachedDocuments = getCache();
            if (MXJudgeUtils.isNotEmpty(cachedDocuments)) {
                addResult(dc, cachedDocuments);
                record(dc, cachedDocuments.size(), DefineTool.RecallFrom.LOCAL.getName());
                return;
            }
        }

        List<String> result = getResultFromEs();
        if (MXJudgeUtils.isEmpty(result)) {
            return;
        }

        List<BaseDocument> documents = MXDataSource.details().get(result);
        addResult(dc, documents);
        record(dc, documents.size(), DefineTool.RecallFrom.ES.getName());

        if (config.getBoolean(KEY_CACHED)) {
            setCache(documents);
        }
    }

    @Override
    public void registerConfig(Map<String, Class> outConfMap) {
        super.registerConfig(outConfMap);
        outConfMap.put(RecallConfig.KEY_ES_INDEX, String.class);
        outConfMap.put(RecallConfig.KEY_SIZE, Integer.class);
        outConfMap.put(KEY_SORT, String.class);
        outConfMap.put(KEY_CACHED, Boolean.class);
        outConfMap.put(KEY_CACHED_TIME, Integer.class);
    }

    private List<String> getResultFromEs() {
        String index = String.format(REQUEST_URL_FORMAT, config.getEsIndex());
        String content = construct();
        return MXDataSource.ES().sendSyncSearch(index, content);
    }

    private boolean check() {
        return MXJudgeUtils.isNotEmpty(config.getEsIndex());
    }

    private String construct() {
        JSONObject content = new JSONObject();

        content.put("size", config.getSize()==0?200:config.getSize());
        JSONArray sort = parseSort();
        if (MXJudgeUtils.isEmpty(sort)) {
            content.put("sort", DEFAULT_SORT_JSON);
        } else {
            content.put("sort", sort);
        }

        return content.toJSONString();
    }

    private JSONArray parseSort() {
        JSONArray sort = new JSONArray();

        String s = config.getString(KEY_SORT);
        if (MXJudgeUtils.isEmpty(s)) {
            return sort;
        }
        String format = "{'%s':{'order': '%s'}}";
        String[] sa = s.replace("[", "").replace("]", "").split("\\|");
        for (String c : sa) {
            String[] temp = c.split("-");
            if (temp.length < 2) {
                continue;
            }
            sort.add(JSON.parseObject(String.format(format, temp[0], temp[1])));
        }
        return sort;
    }

    private void record(BaseDataCollection dc, int size, String from) {
        dc.syncSearchResultSizeMap.put(this.getName()+"sss"+this.getResultKey(), size);
        dc.resultFromMap.put(this.getName(), from);

      //  String key = String.format("%s_%s", this.getName(), "category");
     //   dc.syncSearchResultSizeMap.put(key,size);
      //  dc.resultFromMap.put(key,from);
    }

    private String getCacheKey () {
        return String.format("%s_%s", config.getEsIndex(), config.getResultKey());
    }

    private List<BaseDocument> getCache() {
        return localCache.get(getCacheKey());
    }

    private void setCache(List<BaseDocument> documents) {
        localCache.putIfAbsent(getCacheKey(), documents);
    }

    private List<BaseDocument> load(Object key) {
        List<BaseDocument> documents = new ArrayList<>();

        int count = 2;
        while (count > 0) {
            List<String> result = getResultFromEs();
            if (MXJudgeUtils.isNotEmpty(result)) {
                documents = MXDataSource.details().get(result);
                break;
            }
            count--;
        }
        return documents;
    }
}
