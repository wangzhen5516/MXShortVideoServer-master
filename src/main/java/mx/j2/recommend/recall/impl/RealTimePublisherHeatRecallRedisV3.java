package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.hystrix.GetStringFromPrivateAccountRedisCommand;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXCollectionUtils;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;

import java.util.*;

/**
 * @author liangjie.feng
 * @version 1.0
 * @date 2021/2/6 下午12:01
 * @description
 */
public class RealTimePublisherHeatRecallRedisV3 extends BaseRecall<BaseDataCollection> {

    protected String REDIS_KEY_FORMAT;
    // [
    //    {
    //        "id":"200002zoAZ",
    //        "score":1615197985043,
    //        "source":"publisher"
    //    },类似这样

    @Override
    public boolean skip(BaseDataCollection baseDC) {
        String videoId = null;
        if (baseDC.req.extraClientInfo != null) {
            videoId = baseDC.req.extraClientInfo.lastInteractiveId;
        }
        if (MXStringUtils.isEmpty(videoId)) {
            return true;
        }
        List<String> type = baseDC.recommendFlow.realType;
        if (MXCollectionUtils.isEmpty(type)) {
            return true;
        }

        // TODO zxj 临时逻辑, 下掉该召回器中的对应R组的逻辑
        if (8500 <= baseDC.client.user.userSmallFlowCode && 8999 >= baseDC.client.user.userSmallFlowCode) {
            return true;
        }

        return !type.contains(baseDC.req.extraClientInfo.lastInteractiveType);
    }

    @Override
    public void recall(BaseDataCollection dc) {
        String videoId = dc.req.extraClientInfo.lastInteractiveId;
        setRedisFormat();

        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
        String localCacheKey = String.format("%s_%s", this.getName(), videoId);
        List<BaseDocument> docList = localCacheDataSource.getRealtimePublisherCache(localCacheKey);
        if (null == docList) {
            Map<String, Double> videoIdToScore = getPubIdScoreMap(String.format(REDIS_KEY_FORMAT, videoId), dc);
            List<String> idList = new ArrayList<>();
            if(videoIdToScore != null) {
                // 排序
                List<Map.Entry<String, Double>> list = new ArrayList<Map.Entry<String, Double>>(videoIdToScore.entrySet());
                Collections.sort(list, (o1, o2) -> o2.getValue().compareTo(o1.getValue()));
                for (Map.Entry<String, Double> mapping : list) {
                    idList.add(mapping.getKey());
                }
            }
            docList = MXDataSource.details().get(idList, this.getName());
            localCacheDataSource.setRealtimePublisherCache(localCacheKey, docList);
        }
        if (MXJudgeUtils.isNotEmpty(docList)) {
            if (null != dc.req.extraClientInfo && MXStringUtils.isNotEmpty(dc.req.extraClientInfo.lastInteractiveId)) {
                docList.removeIf(item -> dc.req.extraClientInfo.lastInteractiveId.equals(item.id));
            }
            dc.realTimeClickDocList.addAll(docList);
            dc.syncSearchResultSizeMap.put(this.getName(), docList.size());
            dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.REDIS.getName());
        }
    }

    private Map<String, Double> getPubIdScoreMap(String key, BaseDataCollection baseDc) {
        GetStringFromPrivateAccountRedisCommand command = new GetStringFromPrivateAccountRedisCommand(key);
        String jsonString = command.execute();
        if (MXStringUtils.isEmpty(jsonString)) {
            return null;
        }
        Map<String, Double> publisherScoreMap = loadJsonString(jsonString, baseDc);
        return publisherScoreMap;
    }

    private Map<String, Double> loadJsonString(String jsonString, BaseDataCollection baseDc) {
        JSONArray jsonArray = JSONArray.parseArray(jsonString);
        if (MXJudgeUtils.isEmpty(jsonArray)) {
            return null;
        }

        Map<String, Double> publisherIdScoreMap = new HashMap<>();
        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject obj = (JSONObject) jsonArray.get(i);
            if (obj.containsKey("id") && obj.containsKey("score")) {
                String id = obj.getString("id");
                Double score = obj.getDouble("score");
                String source = obj.getString("source");
                if (MXStringUtils.isNotEmpty(id) && score != null && score > 0) {
                    publisherIdScoreMap.put(id, score);//TODO 后续把这个和下面合并，都放到DC中吧。把排序逻辑摘到ranker里。
                    baseDc.publisherIdSourceMap.put(id, source);
                }
            }
        }
        return publisherIdScoreMap;
    }

    protected void setRedisFormat() {
        REDIS_KEY_FORMAT = "item_reco_cf_03-%s";
    }
}
