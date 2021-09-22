package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.newrelic.api.agent.Trace;
import mx.j2.recommend.component.configurable.config.RecallConfig;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.List;
import java.util.Map;

public class SoundOutRecall extends BaseRecall<BaseDataCollection> {

    private static final String REQUEST_URL_FORMAT = "/%s/_search?pretty=false";
    private static final float RECALL_WEIGHT_SCORE = 1200;
    private static final JSONArray SORT_JSON;

    static {
        SORT_JSON = new JSONArray();
        JSONObject sortCore = new JSONObject();
        JSONObject script = new JSONObject();
        script.put("script", "Math.random()");
        script.put("type", "number");
        script.put("order", "asc");
        sortCore.put("_script", script);
        SORT_JSON.add(sortCore);
    }

    @Override
    public boolean skip(BaseDataCollection data) {
        return false;
    }

    @Override
    @Trace(dispatcher = true)
    public void recall(BaseDataCollection baseDc) {

        String localCacheKey = String.format("%s_%s", this.getName(), baseDc.client.user.uuId);
        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();

        List<BaseDocument> resultList = localCacheDataSource.getSoundOutRecallCache(localCacheKey);
        if (MXJudgeUtils.isNotEmpty(resultList)) {
            addResult(baseDc, resultList);
            baseDc.syncSearchResultSizeMap.put(this.getName(), resultList.size());
            baseDc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.LOCAL.getName());
            return;
        }

        resultList = MXDataSource.ES().searchForDocuments(getEsRequest());
        if (MXJudgeUtils.isEmpty(resultList)) {
            return;
        }

        localCacheDataSource.setSoundOutRecallCache(localCacheKey, resultList);
        addResult(baseDc, resultList);
        baseDc.syncSearchResultSizeMap.put(this.getName(), resultList.size());
        baseDc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.ES.getName());
    }

    private BaseDataCollection.ESRequest getEsRequest() {
        String EsRequest = String.format(REQUEST_URL_FORMAT, config.getEsIndex());
        String content = constructContent(null, 0, config.getSize(), null, SORT_JSON).toJSONString();

        return new BaseDataCollection.ESRequest(EsRequest, content, this.getName(), "", "");
    }

    @Override
    public float getRecallWeightScore() {
        return RECALL_WEIGHT_SCORE;
    }

    @Override
    public void registerConfig(Map<String, Class> outConfMap) {
        outConfMap.put(RecallConfig.KEY_RESULT, String.class);
        outConfMap.put(RecallConfig.KEY_SIZE, Integer.class);
        outConfMap.put(RecallConfig.KEY_ES_INDEX, String.class);
    }
}
