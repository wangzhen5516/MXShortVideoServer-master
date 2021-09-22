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
import java.util.List;

/**
 * @Author: xiaoling.zhu
 * @Date: 2021-01-06
 */

public class RealTimePublisherHeatScoreRecall extends RealTimePublisherRecallAllChosenV {
    private final int length = -1;
    private final int RECALL_SIZE = 200;
    private final String SEARCH_KEY_FIELD = "publisher_id";

    private String requestUrlFormat;
    private JSONArray sortJson;
    private final String SORT_FIELD = "heat_score";
    private final String RANGE_FIELD = "heat_score";
    private final double MIN_RANGE_SCORE = 0.4;

    private final String REDIS_KEY_FORMAT = "%s:prefer_publisher";

    private int MAX_COUNT = 6;
    private int RECALL_MAX_COUNT = 12;
    private int SUFFICENT_SIZE = 6;

    public RealTimePublisherHeatScoreRecall() {
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
                shufflePublisher(pubIds);
                String queryBody;
                String elasticSearchRequest;
                String localCacheKey;
                LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
                if (pubIds.size() <= 3) {
                    dc.ratioForRealTimeMixer = pubIds.size();
                } else if (pubIds.size() <= this.MAX_COUNT) {
                    dc.ratioForRealTimeMixer = 3;
                } else if (pubIds.size() <= this.MAX_COUNT * 2) {
                    dc.ratioForRealTimeMixer = pubIds.size() / 2;
                } else {
                    dc.ratioForRealTimeMixer = MAX_COUNT;
                }

                // 记录召回的 publisher id 和数量
                logExtra.append("Used:{");

                int pubSize = Math.min(this.RECALL_MAX_COUNT,pubIds.size());
                int alreadyRecallSize = 0;

                for (int i = 0; i < pubSize; i++) {
                    // 记录使用的 publisher id
                    logExtra.append(pubIds.get(i)).append(":");

                    localCacheKey = String.format("%s_%s", this.getName(), pubIds.get(i));
                    List<BaseDocument> temp = localCacheDataSource.getRealtimePublisherCache(localCacheKey);
                    if (null == temp) {
                        queryBody = constructQuery(this.SEARCH_KEY_FIELD, RECALL_SIZE, pubIds.get(i), null);
                        elasticSearchRequest = String.format(this.requestUrlFormat, DefineTool.CategoryEnum.SHORT_VIDEO.getIndexAndType());
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

                    alreadyRecallSize++;
                    if(alreadyRecallSize>=this.SUFFICENT_SIZE){
                        break;
                    }
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

    @Override
    public String constructQuery(String searchKey, int size, String id, List<String> sourceList) {
        JSONObject content = new JSONObject();
        JSONArray mustArray = new JSONArray();
        JSONObject mustFather = new JSONObject();
        JSONObject boolFather = new JSONObject();
        JSONObject matchIdFieldFather = new JSONObject();
        JSONObject matchIdField = new JSONObject();
        JSONObject matchFather = new JSONObject();
        JSONObject matchField = new JSONObject();
        JSONObject rangeOrder = new JSONObject();
        JSONObject rangeField = new JSONObject();
        JSONObject rangeFather = new JSONObject();

        matchIdField.put(searchKey, id);
        matchIdFieldFather.put("match", matchIdField);

        matchField.put("status", DefineTool.OnlineStatusesEnum.ONLINE.getIndex());
        matchFather.put("match", matchField);

        rangeOrder.put("gte", this.MIN_RANGE_SCORE);
        rangeField.put(this.RANGE_FIELD, rangeOrder);
        rangeFather.put("range", rangeField);

        mustArray.add(matchIdFieldFather);
        mustArray.add(matchFather);
        mustArray.add(rangeFather);

        mustFather.put("must", mustArray);
        boolFather.put("bool", mustFather);
        content.put("query", boolFather);
        content.put("size", size);
        content.put("sort", this.sortJson);

        if (MXJudgeUtils.isNotEmpty(sourceList)) {
            JSONArray sourceArray = new JSONArray();
            sourceArray.addAll(sourceList);
            content.put("_source", sourceArray);
        }

        return content.toString();
    }

}
