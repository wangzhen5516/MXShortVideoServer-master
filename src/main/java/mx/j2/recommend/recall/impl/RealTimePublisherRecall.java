package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.document.ShortDocument;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.hystrix.redis.ZrevRangePvCommand;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;

import java.util.ArrayList;
import java.util.List;

import static mx.j2.recommend.util.DefineTool.CategoryEnum.SHORT_VIDEO;

/**
 * @Author: xiaoling.zhu
 * @Date: 2020-11-02
 */

public class RealTimePublisherRecall extends BaseRecall<BaseDataCollection> {
    private static final int length = 5;
    private static final int RECALL_SIZE = 200;
    private static final String SEARCH_KEY_FIELD = "publisher_id";

    private String requestUrlFormat;
    private JSONArray sortJson;
    private final static String SORT_FIELD = "heat_score";

    private final String REDIS_KEY_FORMAT = "%s:prefer_publisher";

    public RealTimePublisherRecall() {
        requestUrlFormat = "/%s/_search?pretty=false";

        sortJson = new JSONArray();
        JSONObject sortCore = new JSONObject();
        sortCore.put("order", "desc");
        sortCore.put("missing", "_last");
        JSONObject sortObj = new JSONObject();
        sortObj.put(SORT_FIELD, sortCore);
        sortJson.add(sortObj);

        JSONObject sortCore2 = new JSONObject();
        sortCore2.put("order", "desc");
        JSONObject sortObj2 = new JSONObject();
        sortObj2.put("_uid", sortCore2);
        sortJson.add(sortObj2);
    }

    @Override
    public boolean skip(BaseDataCollection dc) {
        return MXStringUtils.isEmpty(dc.client.user.uuId);
    }

    @Override
    @Trace(dispatcher = true)
    public void recall(BaseDataCollection dc) {
        String userId = dc.client.user.uuId;
        ZrevRangePvCommand userPubHystrixCommand = new ZrevRangePvCommand(String.format(REDIS_KEY_FORMAT, userId), 0, length);
        List<String> pubIds = userPubHystrixCommand.execute();
        List<BaseDocument> docList = new ArrayList<>();
        if (MXJudgeUtils.isNotEmpty(pubIds)) {
                String queryBody;
                String elasticSearchRequest;
                String localCacheKey;
                LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
                for (String pubId : pubIds) {
                    localCacheKey = String.format("%s_%s", this.getName(), pubId);
                    List<BaseDocument> temp = localCacheDataSource.getRealtimePublisherCache(localCacheKey);
                    if (null == temp) {
                        temp = new ArrayList<>();
                        queryBody = constructQuery(SEARCH_KEY_FIELD, RECALL_SIZE, pubId, null);
                        elasticSearchRequest = String.format(requestUrlFormat, DefineTool.CategoryEnum.SHORT_VIDEO.getIndexAndType());
                        List<JSONObject> resultJsonList = MXDataSource.videoES().sendSyncSearchPure(elasticSearchRequest, queryBody);
                        if (MXJudgeUtils.isEmpty(resultJsonList)) {
                            continue;
                        }

                        for (JSONObject resultJson : resultJsonList) {
                            BaseDocument doc = new ShortDocument().loadJsonObj(resultJson, SHORT_VIDEO, this.getName());
                            if (doc != null) {
                                temp.add(doc);
                            }
                        }

                        if (null == temp) {
                            continue;
                        }
                        localCacheDataSource.setRealtimePublisherCache(localCacheKey, temp);
                    }
                    docList.addAll(temp);
                }

                if (MXJudgeUtils.isNotEmpty(docList)) {
                    dc.userPrePubDocList.addAll(docList);
                }
            }
    }

    public String constructQuery(String searchKey, int size, String pubId, List<String> sourceList) {
        JSONObject content = new JSONObject();
        JSONArray mustArray = new JSONArray();
        JSONObject mustFather = new JSONObject();
        JSONObject boolFather = new JSONObject();
        JSONObject matchIdFieldFather = new JSONObject();
        JSONObject matchIdField = new JSONObject();
        JSONObject matchFather = new JSONObject();
        JSONObject matchField = new JSONObject();

        matchIdField.put(searchKey, pubId);
        matchIdFieldFather.put("match", matchIdField);

        matchField.put("status", DefineTool.OnlineStatusesEnum.ONLINE.getIndex());
        matchFather.put("match", matchField);

        mustArray.add(matchIdFieldFather);
        mustArray.add(matchFather);

        mustFather.put("must", mustArray);
        boolFather.put("bool", mustFather);
        content.put("query", boolFather);
        content.put("size", size);
        content.put("sort", sortJson);

        if (MXJudgeUtils.isNotEmpty(sourceList)) {
            JSONArray sourceArray = new JSONArray();
            sourceArray.addAll(sourceList);
            content.put("_source", sourceArray);
        }

        return content.toString();
    }
}
