package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.component.configurable.config.RecallConfig;
import mx.j2.recommend.component.list.skip.ISkip;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UserProfileTagESRecall extends UserProfileTagRecall {
    // ES Config
    private final JSONArray sortJson;
    private final String SORT_FIELD = "heat_score2";
    private final String SEARCH_KEY_FIELD = "publisher_id";
    private final int RECALL_SIZE = 200;
    private String requestUrlFormat;

    // Recall Config
    private final String NUM = "num";
    private final String TABLE = "table";
    private final int MAX_RECALL_NUM = 3;

    private final String LOCAL_CACHE_KEY = "RealTimePublisherRecallAll_%s";

    public UserProfileTagESRecall() {
        requestUrlFormat = "/%s/_search?pretty=false";

        sortJson = new JSONArray();
        JSONObject sortCore = new JSONObject();
        sortCore.put("order", "desc");
        sortCore.put("missing", "_last");
        JSONObject sortObj = new JSONObject();
        sortObj.put(SORT_FIELD, sortCore);
        sortJson.add(sortObj);
    }

    @Override
    public void doRecall(BaseDataCollection dc, List<String> userTags) {
        if (MXCollectionUtils.isEmpty(userTags)) {
            return;
        }
        // 目前规定最多只召回3个publisher_tag的视频，过多容易给ES造成压力，以后酌情调整
        if (userTags.size() > MAX_RECALL_NUM) {
            userTags = userTags.subList(0, MAX_RECALL_NUM);
        }
        Map<String, List<BaseDocument>> resMap = new HashMap<>(userTags.size());
        for (String tagIt : userTags) {
            if (MXStringUtils.isBlank(tagIt)) {
                continue;
            }
            LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
            String localCacheKey = String.format(LOCAL_CACHE_KEY, tagIt);
            List<BaseDocument> docList = localCacheDataSource.getRealtimePublisherCache(localCacheKey);
            if (MXJudgeUtils.isEmpty(docList)) {
                String queryBody = constructQuery(tagIt);
                String elasticSearchRequest = String.format(requestUrlFormat, DefineTool.CategoryEnum.SHORT_VIDEO.getIndexAndType());
                List<JSONObject> resultJsonList = MXDataSource.videoES().sendSyncSearchPure(elasticSearchRequest, queryBody);
                if (MXJudgeUtils.isEmpty(resultJsonList)) {
                    continue;
                }
                List<String> idList = ESJsonTool.loadOnlyIdList(resultJsonList);
                if (MXJudgeUtils.isEmpty(idList)) {
                    continue;
                }
                docList = MXDataSource.details().get(idList, this.getName());
                if (MXJudgeUtils.isEmpty(docList)) {
                    continue;
                }
                localCacheDataSource.setRealtimePublisherCache(localCacheKey, docList);
            }
            List<BaseDocument> copyList = new ArrayList<>(docList.size());
            DefineTool.deepClone(docList, copyList);
            resMap.put(tagIt, copyList);
        }
        if (MXCollectionUtils.isNotEmpty(resMap)) {
            resMap.forEach((k, v) -> dc.syncSearchResultSizeMap.put(k, v.size()));
            setResult(dc, resMap);
        }
    }

    public String constructQuery(String id) {
        JSONObject content = new JSONObject();
        JSONArray mustArray = new JSONArray();
        JSONObject mustFather = new JSONObject();
        JSONObject boolFather = new JSONObject();
        JSONObject matchIdFieldFather = new JSONObject();
        JSONObject matchIdField = new JSONObject();
        JSONObject matchFather = new JSONObject();
        JSONObject matchField = new JSONObject();

        matchIdField.put(SEARCH_KEY_FIELD, id);
        matchIdFieldFather.put("match", matchIdField);

        matchField.put("status", DefineTool.OnlineStatusesEnum.ONLINE.getIndex());
        matchFather.put("match", matchField);

        mustArray.add(matchIdFieldFather);
        mustArray.add(matchFather);

        mustFather.put("must", mustArray);
        boolFather.put("bool", mustFather);
        content.put("query", boolFather);
        content.put("size", RECALL_SIZE);
        content.put("sort", sortJson);

        return content.toString();
    }

    @Override
    public void registerConfig(Map<String, Class> outConfMap) {
        outConfMap.put(RecallConfig.KEY_SKIP, ISkip.class);
        outConfMap.put(RecallConfig.KEY_RESULT, String.class);
        outConfMap.put(NUM, Integer.class);
        outConfMap.put(TABLE, String.class);
    }

    /**
     * 获取配置的 tag 数量，使用几个 tag 召回
     */
    @Override
    public int getNum() {
        return config.getInt(NUM);
    }

    /**
     * 获取 tag CA table 配置，从哪个表召回 tag
     */
    @Override
    public String getTable() {
        return config.getString(TABLE);
    }
}
