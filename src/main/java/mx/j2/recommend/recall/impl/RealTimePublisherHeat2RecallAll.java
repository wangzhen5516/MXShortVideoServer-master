package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.hystrix.redis.ZrevRangePvCommand;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.ESJsonTool;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @Author: DuoZhao
 * @Date: 2020/11/30
 * 获取所有在redis中的publisherId，shuffle后再取9个
 */
@Deprecated
public class RealTimePublisherHeat2RecallAll extends BaseRecall<BaseDataCollection> {
    private static final int length = -1;
    private static final int RECALL_SIZE = 200;
    private static final String SEARCH_KEY_FIELD = "publisher_id";

    private String requestUrlFormat;
    private JSONArray sortJson;
    private final static String SORT_FIELD = "heat_score2";

    private final String REDIS_KEY_FORMAT = "%s:prefer_publisher";

    int MAX_COUNT = 6;

    public RealTimePublisherHeat2RecallAll() {
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
    public boolean skip(BaseDataCollection data) {
        return false;
    }

    @Override
    @Trace(dispatcher = true)
    public void recall(BaseDataCollection dc) {
        String userId = dc.client.user.uuId;
        if (MXStringUtils.isNotEmpty(userId)) {
            StringBuilder logExtra = new StringBuilder();

            ZrevRangePvCommand userPubHystrixCommand = new ZrevRangePvCommand(String.format(REDIS_KEY_FORMAT, userId), 0, length);
            List<String> pubIds = userPubHystrixCommand.execute();

            // 记录用户的 publisher list 长度
            logExtra.append("Publishers:").append(pubIds != null ? pubIds.size() : 0).append(",");

            List<BaseDocument> docList = new ArrayList<>();
            if (MXJudgeUtils.isNotEmpty(pubIds)) {
                Collections.shuffle(pubIds);
                String queryBody;
                String elasticSearchRequest;
                String localCacheKey;
                LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
                if (pubIds.size() <= 3) {
                    dc.ratioForRealTimeMixer = pubIds.size();
                } else if (pubIds.size() > 3 && pubIds.size() <= MAX_COUNT) {
                    dc.ratioForRealTimeMixer = 3;
                } else if (pubIds.size() > MAX_COUNT && pubIds.size() <= MAX_COUNT * 2) {
                    dc.ratioForRealTimeMixer = pubIds.size() / 2;
                } else if (pubIds.size() > MAX_COUNT * 2) {
                    dc.ratioForRealTimeMixer = MAX_COUNT;
                }

                // 记录召回的 publisher id 和数量
                logExtra.append("Used:{");

                for (int i = 0; i < dc.ratioForRealTimeMixer; i++) {
                    // 记录使用的 publisher id
                    logExtra.append(pubIds.get(i)).append(":");

                    localCacheKey = String.format("%s_%s", this.getName(), pubIds.get(i));
                    List<BaseDocument> temp = localCacheDataSource.getRealtimePublisherCache(localCacheKey);
                    if (null == temp) {
                        queryBody = constructQuery(SEARCH_KEY_FIELD, RECALL_SIZE, pubIds.get(i), null);
                        elasticSearchRequest = String.format(requestUrlFormat, DefineTool.CategoryEnum.SHORT_VIDEO.getIndexAndType());
                        List<JSONObject> resultJsonList = MXDataSource.videoES().sendSyncSearchPure(elasticSearchRequest, queryBody);
                        if (MXJudgeUtils.isEmpty(resultJsonList)) {
                            continue;
                        }

                        List<String> idList = ESJsonTool.loadOnlyIdList(resultJsonList);
                        if (MXJudgeUtils.isEmpty(idList)) {
                            continue;
                        }

                        temp = MXDataSource.details().get(idList, this.getName());
                        if (MXJudgeUtils.isEmpty(temp)) {
                            continue;
                        }

                        localCacheDataSource.setRealtimePublisherCache(localCacheKey, temp);
                    }

                    // 记录召回的数量
                    logExtra.append(temp.size()).append(",");

                    docList.addAll(temp);
                }

                logExtra.append("}");

                if (MXJudgeUtils.isNotEmpty(docList)) {
                    dc.userPrePubDocList.addAll(docList);
                }

                // 记录日志
                dc.logComponentExtra.put(getName(), logExtra.toString());
            }
        }
    }

    public String constructQuery(String searchKey, int size, String id, List<String> sourceList) {
        JSONObject content = new JSONObject();
        JSONArray mustArray = new JSONArray();
        JSONObject mustFather = new JSONObject();
        JSONObject boolFather = new JSONObject();
        JSONObject matchIdFieldFather = new JSONObject();
        JSONObject matchIdField = new JSONObject();
        JSONObject matchFather = new JSONObject();
        JSONObject matchField = new JSONObject();

        matchIdField.put(searchKey, id);
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
