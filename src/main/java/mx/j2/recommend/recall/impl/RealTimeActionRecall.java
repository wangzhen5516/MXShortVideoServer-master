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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * [
 * {
 * "id":"200002zoAZ",
 * "score":1615197985043,
 * "source":"publisher"
 * },类似这样
 */
@SuppressWarnings("unused")
public class RealTimeActionRecall extends BaseRecall<BaseDataCollection> {
    private static final String KEY_FORMAT = "format";
    private static final String KEY_DATA_ID = "id";
    private static final String KEY_DATA_SCORE = "score";
    private static final String KEY_DATA_SOURCE = "source";

    @Override
    public void registerConfig(Map<String, Class> outConfMap) {
        super.registerConfig(outConfMap);
        outConfMap.put(KEY_FORMAT, String.class);
    }

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

        if (!type.contains(baseDC.req.extraClientInfo.lastInteractiveType)) {
            return true;
        }

        return super.skip(baseDC);
    }

    @Override
    public void recall(BaseDataCollection dc) {
        String videoId = dc.req.extraClientInfo.lastInteractiveId;

        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
        String localCacheKey = DefineTool.toKey(getId(), videoId);
        List<BaseDocument> docList = localCacheDataSource.getRealtimePublisherCache(localCacheKey);

        if (null == docList) {
            Map<String, Double> videoIdToScore = getPubIdScoreMap(String.format(getFormatConfig(), videoId), dc);
            List<String> idList = new ArrayList<>();

            if (videoIdToScore != null) {
                // 排序
                List<Map.Entry<String, Double>> list = new ArrayList<>(videoIdToScore.entrySet());
                list.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));

                for (Map.Entry<String, Double> mapping : list) {
                    idList.add(mapping.getKey());
                }
            }

            docList = MXDataSource.details().get(idList);
            localCacheDataSource.setRealtimePublisherCache(localCacheKey, docList);
        }

        if (MXJudgeUtils.isNotEmpty(docList)) {
            if (null != dc.req.extraClientInfo && MXStringUtils.isNotEmpty(dc.req.extraClientInfo.lastInteractiveId)) {
                docList.removeIf(item -> dc.req.extraClientInfo.lastInteractiveId.equals(item.id));
                dc.debug.sourceId = dc.req.extraClientInfo.lastInteractiveId;
            }

            setResult(dc, docList);
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

    /**
     * 拿 redis key 格式的配置
     */
    private String getFormatConfig() {
        return config.getString(KEY_FORMAT);
    }

    /**
     * 由 format 决定数据来源
     */
    @Override
    protected String getDataId() {
        return getFormatConfig();
    }
}
