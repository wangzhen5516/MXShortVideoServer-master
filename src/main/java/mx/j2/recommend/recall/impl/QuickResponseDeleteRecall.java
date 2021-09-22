package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.component.configurable.config.RecallConfig;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.hystrix.GetStringFromPrivateAccountRedisCommand;
import mx.j2.recommend.hystrix.redis.ZrevRangeAndDeleteCommand;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QuickResponseDeleteRecall extends BaseRecall<BaseDataCollection> {
    private final String RESPONSE_REDIS_KEY = "%s:latest_action_video_v2";
    private final String REACTION_REDIS_KEY = "item_reco_cf_03-%s";
    private static final String KEY_DATA_ID = "id";
    private static final String KEY_DATA_SCORE = "score";
    private static final String KEY_DATA_SOURCE = "source";

    @Override
    public void recall(BaseDataCollection dc) {
        ZrevRangeAndDeleteCommand command = new ZrevRangeAndDeleteCommand(String.format(RESPONSE_REDIS_KEY, dc.client.user.uuId));
        List<String> videoIds = command.execute();
        if (MXJudgeUtils.isEmpty(videoIds)) {
            return;
        }

        Map<String, Double> videoIdToScore = getPubIdScoreMap(String.format(REACTION_REDIS_KEY, videoIds.get(videoIds.size() - 1)), dc);
        if (MXJudgeUtils.isEmpty(videoIdToScore)) {
            return;
        }

        // 排序
        List<String> idList = new ArrayList<>();
        List<Map.Entry<String, Double>> list = new ArrayList<>(videoIdToScore.entrySet());
        list.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));
        for (Map.Entry<String, Double> mapping : list) {
            idList.add(mapping.getKey());
        }

        List<BaseDocument> docList = MXDataSource.details().get(idList);
        addResult(dc, docList);
        dc.syncSearchResultSizeMap.put(this.getName(), docList.size());
        dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.REDIS.getName());
    }

    @Override
    public void registerConfig(Map<String, Class> outConfMap) {
        outConfMap.put(RecallConfig.KEY_RESULT, String.class);
    }

    private Map<String, Double> getPubIdScoreMap(String key, BaseDataCollection baseDc) {
        GetStringFromPrivateAccountRedisCommand command = new GetStringFromPrivateAccountRedisCommand(key);
        String jsonString = command.execute();

        if (MXStringUtils.isEmpty(jsonString)) {
            return null;
        }

        return loadJsonString(jsonString, baseDc);
    }

    private Map<String, Double> loadJsonString(String jsonString, BaseDataCollection baseDc) {
        JSONArray jsonArray = JSONArray.parseArray(jsonString);
        if (MXJudgeUtils.isEmpty(jsonArray)) {
            return null;
        }

        Map<String, Double> publisherIdScoreMap = new HashMap<>();
        for (Object o : jsonArray) {
            JSONObject obj = (JSONObject) o;

            if (obj.containsKey(KEY_DATA_ID) && obj.containsKey(KEY_DATA_SCORE)) {
                String id = obj.getString(KEY_DATA_ID);
                Double score = obj.getDouble(KEY_DATA_SCORE);
                String source = obj.getString(KEY_DATA_SOURCE);

                if (MXStringUtils.isNotEmpty(id) && score != null && score > 0) {
                    publisherIdScoreMap.put(id, score);//TODO 后续把这个和下面合并，都放到DC中吧。把排序逻辑摘到ranker里。
                    baseDc.publisherIdSourceMap.put(id, source);
                }
            }
        }
        return publisherIdScoreMap;
    }
}
