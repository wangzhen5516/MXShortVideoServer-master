package mx.j2.recommend.recall.impl;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.document.ShortDocument;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.hystrix.GetStringFromPrivateAccountRedisCommand;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;

import java.util.*;

import static mx.j2.recommend.util.DefineTool.CategoryEnum.SHORT_VIDEO;

/**
 * @Author: QiMao
 * @Date: 12/5/2020
 * 加权随机获得实时publisherId
 */
@Deprecated
public class RealTimePublisherWeightRandomRecall extends BaseRecall<BaseDataCollection> {
    private static final int RECALL_SIZE = 200;
    private static final String SEARCH_KEY_FIELD = "publisher_id";

    private String requestUrlFormat;
    private JSONArray sortJson;
    private final static String SORT_FIELD = "heat_score";

    private final String REDIS_KEY_FORMAT = "%s:prefer_publisher_v1";

    private static final Random RANDOM = new Random();

    public RealTimePublisherWeightRandomRecall() {
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
            String key = String.format(REDIS_KEY_FORMAT, userId);

            //TODO:用新的redis然后得到json，再解析出来publist

            Map<String, Integer> map = getPubIdScoreMap(key);

            if (MXJudgeUtils.isEmpty(map)) {
                return;
            }

            List<String> pubIds = new ArrayList<>(map.keySet());
            List<BaseDocument> docList = new ArrayList<>();
            if (MXJudgeUtils.isNotEmpty(pubIds)) {
                Collections.shuffle(pubIds);
                String queryBody;
                String elasticSearchRequest;
                String localCacheKey;
                boolean needToRandom = true;
                LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
                if (pubIds.size() <= 3) {
                    dc.ratioForRealTimeMixer = pubIds.size();
                    needToRandom = false;
                } else if (pubIds.size() > 3 && pubIds.size() <= 6) {
                    dc.ratioForRealTimeMixer = 3;
                } else if (pubIds.size() > 6 && pubIds.size() <= 12) {
                    dc.ratioForRealTimeMixer = pubIds.size() / 2;
                } else if (pubIds.size() > 12) {
                    dc.ratioForRealTimeMixer = 6;
                }


                for (int i = 0; i < dc.ratioForRealTimeMixer; i++) {
                    if (map.isEmpty()) {
                        break;
                    }

                    //如果rationForRealTimeMixer和pubIds大小相等，就不random，直接拿
                    String pubId;
                    if (needToRandom) {
                        pubId = getOnePubId(map);
                    } else {
                        pubId = pubIds.get(i);
                    }

                    if (MXStringUtils.isEmpty(pubId)) {
                        continue;
                    }
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

                    map.remove(pubId);
                }

                if (MXJudgeUtils.isNotEmpty(docList)) {
                    dc.userPrePubDocList.addAll(docList);
                    dc.syncSearchResultSizeMap.put(this.getName(), docList.size());
                    dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.ES.getName());
                }
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

    /**
     * 从redis返回publisherIdList
     *
     * @return
     */
    private Map<String, Integer> getPubIdScoreMap(String key) {
        GetStringFromPrivateAccountRedisCommand command = new GetStringFromPrivateAccountRedisCommand(key);
        String jsonString = command.execute();
        if (MXStringUtils.isEmpty(jsonString)) {
            return null;
        }
        Map<String, Integer> publisherScoreMap = loadJsonString(jsonString);
        return publisherScoreMap;
    }

    private Map<String, Integer> loadJsonString(String jsonString) {
        JSONArray jsonArray = JSONArray.parseArray(jsonString);
        if (MXJudgeUtils.isEmpty(jsonArray)) {
            return null;
        }

        Map<String, Integer> publisherIdScoreMap = new HashMap<>();
        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject obj = (JSONObject) jsonArray.get(i);
            if (obj.containsKey("publisher_id") && obj.containsKey("score")) {
                String publisherId = obj.getString("publisher_id");
                Integer score = obj.getInteger("score");
                if (MXStringUtils.isNotEmpty(publisherId) && score != null && score > 0) {
                    publisherIdScoreMap.put(publisherId, score);
                }
            }
        }
        return publisherIdScoreMap;
    }

    /**
     * 得到随机权重的publisherId
     *
     * @param map
     * @return
     */
    private String getOnePubId(Map<String, Integer> map) {
        int[] scores = new int[map.size()];
        String[] ids = new String[map.size()];
        int index = 0;

        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            scores[index] = entry.getValue();
            ids[index] = entry.getKey();
            index++;
        }

        for (int i = 1; i < scores.length; i++) {
            scores[i] += scores[i - 1];
        }

        index = getOneIndex(scores);
        String pubId = ids[index];

        if (MXStringUtils.isEmpty(pubId)) {
            return null;
        }
        return pubId;
    }

    /**
     * 返回随机权重的index
     *
     * @param scores
     * @return
     */
    private int getOneIndex(int[] scores) {
        int len = scores.length;
        int idx = RANDOM.nextInt(scores[len - 1]) + 1;
        int left = 0, right = len - 1;
        // search position
        while (left < right) {
            int mid = left + (right - left) / 2;
            if (scores[mid] == idx)
                return mid;
            else if (scores[mid] < idx)
                left = mid + 1;
            else
                right = mid;
        }
        return left;
    }

}
