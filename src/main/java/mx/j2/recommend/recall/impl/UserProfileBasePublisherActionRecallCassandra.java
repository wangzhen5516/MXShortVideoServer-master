package mx.j2.recommend.recall.impl;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.data_source.StrategyCassandraDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.BloomUtil;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zhongrenli
 */
@Deprecated
public class UserProfileBasePublisherActionRecallCassandra extends BaseRecall<BaseDataCollection> {

    private final static int CACHE_TIME_SECONDS = 1800;

    private static final String TABLE_NAME = "taka_personal_reco_base_pub_act_v1 ";

    @Override
    public boolean skip(BaseDataCollection data) {
        return false;
    }

    @Override
    public void recall(BaseDataCollection baseDc) {
        if (!DefineTool.TabInfoEnum.HOT.getId().equals(baseDc.req.tabId)) {
            return;
        }

        //本地缓存key
        String cacheKey  = construcCacheKey(baseDc);

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
        result = StrUtil.unWrap(result, '"', '"');
        result = result.replaceAll("\\\\", "");

        JSONArray jsonArray = JSON.parseArray(result);
        List<BaseDocument> mergedList = new ArrayList<>();
        Map<String, Double> scoreMap = new HashMap<>();
        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject jsonObject = (JSONObject) jsonArray.get(i);
            if (jsonObject != null && MXStringUtils.isNotEmpty(jsonObject.getString("id")) && jsonObject.getDouble("score") != null) {
                scoreMap.put(jsonObject.getString("id"), jsonObject.getDouble("score"));
            }
        }

        //根据视频id获取视频详情
        List<BaseDocument> resultList = MXDataSource.details().get(new ArrayList<>(scoreMap.keySet()), this.getName());
        if(MXJudgeUtils.isNotEmpty(resultList)){
            mergedList.addAll(resultList);
        }

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
