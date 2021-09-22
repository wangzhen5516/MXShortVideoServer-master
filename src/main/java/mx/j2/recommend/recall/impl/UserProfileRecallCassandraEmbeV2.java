package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.interfaces.IDocumentProcessor;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.data_source.StrategyCassandraDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.BloomUtil;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UserProfileRecallCassandraEmbeV2 extends BaseRecall<BaseDataCollection> {

    private final static int CACHE_TIME_SECONDS = 1800;

    private final static String TABLE_NAME = "personal_reco_embedding_02";

    @Override
    public boolean skip(BaseDataCollection baseDc) {
        if (!DefineTool.TabInfoEnum.HOT.getId().equals(baseDc.req.tabId)) {
            return true;
        }
        return false;
    }

    @Override
    public void recall(BaseDataCollection baseDc) {
        //本地缓存key
        String cacheKey = construcCacheKey(baseDc);

        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
        List<BaseDocument> cacheDocumentList = localCacheDataSource.getScoreWeightRecallCache(cacheKey);

        //如果本地缓存有数据，直接取出返回
        if (MXJudgeUtils.isNotEmpty(cacheDocumentList)) {
            baseDc.userProfileOfflineRecommendList.addAll(cacheDocumentList);
            baseDc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.LOCAL.getName());
            baseDc.syncSearchResultSizeMap.put(this.getName(), cacheDocumentList.size());
            return;
        }

        //从cassandra中取出视频id和score
        StrategyCassandraDataSource strategyCassandraDataSource = MXDataSource.strategyCA();
        String result = strategyCassandraDataSource.getStrategyOutPutFromCassandraById(BloomUtil.getUuid(baseDc), TABLE_NAME);
        if (MXStringUtils.isEmpty(result)) {
            return;
        }
        JSONArray jsonArray = JSON.parseArray(result);
        Map<String, Double> scoreMap = new HashMap<>();
        for (Object o : jsonArray) {
            JSONObject jsonObject = (JSONObject) o;
            if (jsonObject != null && MXStringUtils.isNotEmpty(jsonObject.getString("id")) && jsonObject.getDouble("score") != null) {
                scoreMap.put(jsonObject.getString("id"), jsonObject.getDouble("score"));
            }
        }

        IDocumentProcessor processor = document -> document.scoreDocument.manualTopScore = scoreMap.get(document.id);

        //根据视频id获取视频详情
        List<BaseDocument> mergedList = MXDataSource.details().get(scoreMap.keySet(), getName(), processor);

        //根据score排序
        mergedList.sort((doc0, doc1) -> Double.compare(doc1.scoreDocument.manualTopScore, doc0.scoreDocument.manualTopScore));
        localCacheDataSource.setScoreWeightRecallCache(cacheKey, mergedList, CACHE_TIME_SECONDS);

        baseDc.userProfileOfflineRecommendList.addAll(mergedList);
        baseDc.syncSearchResultSizeMap.put(this.getName(), mergedList.size());
        baseDc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.ES.getName());

    }

    private String construcCacheKey(BaseDataCollection baseDc) {
        return String.format("%s_%s_%s", this.getName(), baseDc.req.getInterfaceName(), BloomUtil.getUuid(baseDc));
    }
}
