package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.pool_conf.ExposurePoolConf;
import mx.j2.recommend.recall_data_in_mem.ESCanGetStartRecall;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 曝光池召回
 */
@SuppressWarnings("unused")
public class ExposurePoolRecall extends BaseRecall<BaseDataCollection> implements ESCanGetStartRecall {
    private static final Logger log = LogManager.getLogger(ExposurePoolRecall.class);
    private static final JSONArray DEFAULT_SORT_JSON;
    private static final String SORT_STRING_PATTERN = "[{ \"_uid\": { \"missing\": \"0\", \"order\": \"desc\" } }]";

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
        ExposurePoolConf poolConf = getPoolConf(dc);
        if (poolConf == null) {
            return;
        }

        String key = getDataSourceKey(poolConf);
        List<BaseDocument> result = getPoolData(key);

        if (MXJudgeUtils.isNotEmpty(result)) {
            setResult(dc, result);
            dc.syncSearchResultSizeMap.put(this.getName(), result.size());
            dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.ES.getName());
        }
    }

    /**
     * 获取池子配置
     */
    ExposurePoolConf getPoolConf(BaseDataCollection dc) {
        return MXDataSource.exposurePoolConf().get(dc.recommendFlow.name);
    }

    /**
     * 获取全池子配置
     */
    Map<String, ExposurePoolConf> getAllPoolConf() {
        return MXDataSource.exposurePoolConf().all();
    }

    /**
     * 获取池子数据
     */
    List<BaseDocument> getPoolData(String key) {
        return MXDataSource.exposurePool().get(key);
    }

    @Override
    public float getRecallWeightScore() {
        return RECALL_WEIGHT;
    }

    @Override
    public String getRecallName() {
        return this.getName();
    }

    /**
     * 没啥用
     */
    @Override
    public float getRecallDocumentWeight() {
        return 0;
    }

    @Override
    public Map<String, BaseDataCollection.ESRequest> getESRequestMap() {
        Map<String, BaseDataCollection.ESRequest> esRequestMap = new HashMap<>();
        Map<String, ExposurePoolConf> allPoolConf = getAllPoolConf();

        if (MXJudgeUtils.isNotEmpty(allPoolConf)) {
            for (ExposurePoolConf poolConf : allPoolConf.values()) {
                BaseDataCollection.ESRequest esRequest = constructRequest(poolConf);
                esRequestMap.put(getDataSourceKey(poolConf), esRequest);
            }
        }

        return esRequestMap;
    }

    /**
     * 去数据源拿数据的 key
     */
    private String getDataSourceKey(ExposurePoolConf poolConf) {
        return DefineTool.toKey(poolConf.esIndex, poolConf.sortField.toString());
    }

    private BaseDataCollection.ESRequest constructRequest(ExposurePoolConf pc) {
        String request = String.format(DefineTool.ES.SEARCH_FORMAT, pc.esIndex);
        String content = "";

        // 添加排序字段
        if (MXJudgeUtils.isNotEmpty(pc.sortField)) {
            try {
                JSONArray sortArray = JSONArray.parseArray(SORT_STRING_PATTERN);
                sortArray.addAll(0, pc.sortField);
                JSONObject contentTemp = constructContent(null, 0, pc.recallSize, null, sortArray);
                contentTemp.put("_source", false);
                content = contentTemp.toString();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        if (MXStringUtils.isEmpty(content)) {
            content = constructContent(null, 0, pc.recallSize, null, DEFAULT_SORT_JSON).toString();
        }

        if (log.isDebugEnabled()) {
            log.debug(String.format(getName() + " request: %s", request));
            log.debug(String.format(getName() + " content: %s", content));
        }

        return new BaseDataCollection.ESRequest(request, content, this.getRecallName(), "", "pool", pc.esIndex);
    }

    /**
     * 没啥用
     */
    @Nullable
    @Override
    public Map<String, SearchSourceBuilder> getSearchSourceBuilderMap() {
        return null;
    }

    /**
     * 更没啥用
     */
    @Override
    public void doSomethingAfterLoad() {
    }

    /**
     * 没啥用
     */
    @Override
    public int scheduledPeriodSeconds() {
        return DefineTool.ScheduledPeriodSeconds.TenSeconds.getSeconds();
    }

    /**
     * 没啥用
     */
    @Override
    public int getRandomFactor() {
        return 0;
    }
}
