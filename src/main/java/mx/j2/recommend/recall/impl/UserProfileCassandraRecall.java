package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.newrelic.api.agent.Trace;
import mx.j2.recommend.component.configurable.config.RecallConfig;
import mx.j2.recommend.component.list.skip.ISkip;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.interfaces.IDocumentProcessor;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.BloomUtil;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXStringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UserProfileCassandraRecall extends BaseRecall<BaseDataCollection> {
    private static final String KEY_TABLE = "table";
    private final String DEFAULT_GUARANTEE_KEY = "default_uuid_%s";
    private final String DEFAULT_GUARANTEE_CACHE_KEY = "default_uuid_%s_%s";

    @Override
    @Trace(dispatcher = true)
    public void recall(BaseDataCollection baseDc) {
        //从cassandra中取出视频id和score
        //TODO
        long startTime = System.nanoTime();
        String result = getResultFromCa(baseDc);
        baseDc.appendToTimeRecord(System.nanoTime() - startTime, this.getName() + "_StrategyCassandra");
        String cacheKey = String.format(DEFAULT_GUARANTEE_CACHE_KEY, baseDc.recommendFlow.name, config.getString(KEY_TABLE));

        if (MXStringUtils.isEmpty(result)) {
            result = MXDataSource.cache().getUserProfileGuaranteeCache(cacheKey);
            if (MXStringUtils.isEmpty(result)) {
                result = getResultFromCaGuarantee(baseDc);
                if (MXStringUtils.isEmpty(result)) {
                    return;
                }
                MXDataSource.cache().setUserProfileGuaranteeCache(cacheKey, result);
            }
        }

        JSONArray jsonArray = JSON.parseArray(result);

        Map<String, Double> scoreMap = new HashMap<>();
        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject jsonObject = (JSONObject) jsonArray.get(i);
            if (jsonObject != null && MXStringUtils.isNotEmpty(jsonObject.getString("id")) && jsonObject.getDouble("score") != null) {
                scoreMap.put(jsonObject.getString("id"), jsonObject.getDouble("score"));
            }
        }

        IDocumentProcessor processor = document -> document.scoreDocument.manualTopScore = scoreMap.get(document.id);

        //TODO
        startTime = System.nanoTime();
        //根据视频id获取视频详情
        List<BaseDocument> mergedList = MXDataSource.details().get(scoreMap.keySet(), getDocRecallName(), processor);
        baseDc.appendToTimeRecord(System.nanoTime() - startTime, this.getName() + "_Detail");

        //根据score排序
        mergedList.sort((doc0, doc1) -> Double.compare(doc1.scoreDocument.manualTopScore, doc0.scoreDocument.manualTopScore));

        addResult(baseDc, mergedList);
        baseDc.syncSearchResultSizeMap.put(this.getName(), mergedList.size());
        baseDc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.CASSANDRA.getName());
    }

    @Override
    public void registerConfig(Map<String, Class> outConfMap) {
        outConfMap.put(RecallConfig.KEY_SKIP, ISkip.class);
        outConfMap.put(RecallConfig.KEY_RESULT, String.class);
        outConfMap.put(KEY_TABLE, String.class);
    }

    private String getDocRecallName() {
        return DefineTool.toKey(getName(), config.getString(KEY_TABLE));
    }


    public String getResultFromCa(BaseDataCollection baseDc) {
        String table = config.getString(KEY_TABLE);
        return MXDataSource.strategyCA().getStrategyOutPutFromCassandraById(BloomUtil.getUuid(baseDc), table);
    }

    public String getResultFromCaGuarantee(BaseDataCollection dc) {
        String table = config.getString(KEY_TABLE);
        return MXDataSource.strategyCA().getStrategyOutPutFromCassandraById(String.format(DEFAULT_GUARANTEE_KEY, dc.recommendFlow.name), table);
    }
}
