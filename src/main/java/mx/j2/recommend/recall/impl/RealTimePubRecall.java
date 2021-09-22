package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.component.configurable.config.RecallConfig;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.manager.DataSourceManager;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.stream.Collectors;

public class RealTimePubRecall extends BaseRecall<BaseDataCollection> {
    private static final String KEY_TABLE = "table";
    private static final String KEY_COLUMN = "column";
    private static final String KEY_PUBLISHER_NUM = "publisher_num";
    private static final int MAX_PUB_NUM = 10;
    private static final String SEARCH_KEY_FIELD = "publisher_id";
    private final static String SORT_FIELD = "heat_score2";
    private final static double GTE = 7.0;
    private static final String requestFormat = "/%s/_search?pretty=false";
    private static final int RECALL_SIZE = 200;
    public static String QUERY_FORMAT = "select %s from %s where uuid = '%s';";
    public static char flag = 'r';
    private static String[] CHOSEN_V_ARRAY = {};
    /**
     * 精选大 V 集合
     */
    private static Set<String> CHOSEN_V_SET;

    /*
     * 精选大 V 集合初始化
     */
    static {
        if (MXCollectionUtils.isNotEmpty(CHOSEN_V_ARRAY)) {
            CHOSEN_V_SET = new HashSet<>(Arrays.asList(CHOSEN_V_ARRAY));
        }
    }

    private JSONArray sortJson;

    @Override
    public boolean skip(BaseDataCollection baseDC) {
        return MXJudgeUtils.isNotEmpty(baseDC.req.nextToken);
    }

    @Override
    public void recall(BaseDataCollection dc) {
        try {
            doRecall(dc, dc.userPrePubDocList);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected void doRecall(BaseDataCollection dc, List<BaseDocument> targetList) {
        String columnName = config.getString(KEY_COLUMN);
        String table = config.getString(KEY_TABLE);
        int pubNum = config.getInt(KEY_PUBLISHER_NUM);

        String userid = dc.client.user.uuId;
        if (MXStringUtils.isEmpty(userid)) {
            return;
        }
        String query = String.format(QUERY_FORMAT, columnName, table, userid);
        List<String> pubIds = DataSourceManager.INSTANCE.getUserStrategyTagDataSource().getRealPubListFromCA(query, columnName, MAX_PUB_NUM);
        List<BaseDocument> docList = new ArrayList<>();
        if (MXCollectionUtils.isEmpty(pubIds)) {
            return;
        }
        shuffle(pubIds);
        String localCacheKey;
        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
        for (int i = 0; i < Math.min(pubNum, pubIds.size()); i++) {
            localCacheKey = String.format("%s_%s_%s", this.getName(), table, pubIds.get(i));
            List<BaseDocument> res = localCacheDataSource.getRealtimePublisherCache(localCacheKey);
            if (null == res) {
                constructSort();
                String queryBody = constractQuery(SEARCH_KEY_FIELD, RECALL_SIZE, pubIds.get(i), null);
                String esReq = String.format(requestFormat, DefineTool.CategoryEnum.SHORT_VIDEO.getIndexAndType());
                List<JSONObject> resultJsonList = MXDataSource.videoES().sendSyncSearchPure(esReq, queryBody);
                if (MXCollectionUtils.isEmpty(resultJsonList)) {
                    if (resultJsonList.isEmpty()) {
                        localCacheDataSource.setRealtimePublisherCache(localCacheKey, new ArrayList<>());
                    }
                    continue;
                }
                List<String> idList = ESJsonTool.loadOnlyIdList(resultJsonList);
                if (MXJudgeUtils.isEmpty(idList)) {
                    continue;
                }
                res = MXDataSource.details().get(idList, this.getName());
                if (MXJudgeUtils.isEmpty(res)) {
                    continue;
                }
                localCacheDataSource.setRealtimePublisherCache(localCacheKey, res);
            }
            docList.addAll(res);
        }
        if (MXCollectionUtils.isNotEmpty(docList)) {
            targetList.addAll(docList);
            dc.syncSearchResultSizeMap.put(this.getName(), docList.size());
        }
    }

    private void shuffle(List<String> pubIds) {

        if (MXCollectionUtils.isEmpty(pubIds) || MXCollectionUtils.isEmpty(CHOSEN_V_SET)) {
            Collections.shuffle(pubIds);
            return;
        }

        // 精选大 V 数量变为 3 倍
        List<String> repeatIdList = new ArrayList<>();

        for (String id : pubIds) {
            if (CHOSEN_V_SET.contains(id)) {
                repeatIdList.add(id);
                repeatIdList.add(id);
            }
        }

        // 如果没有精选大 V，使用父类版本，并及时返回止损
        if (MXJudgeUtils.isEmpty(repeatIdList)) {
            Collections.shuffle(pubIds);
            return;
        }

        // 重复 id 加入到原数组里
        pubIds.addAll(repeatIdList);

        // 打散
        Collections.shuffle(pubIds);

        // 去重
        List<String> distinctList = pubIds.stream().distinct().collect(Collectors.toList());

        // 写回原数组
        pubIds.clear();
        pubIds.addAll(distinctList);
    }

    private String constractQuery(String searchKey, int size, String id, List<String> sourceList) {
        JSONObject content = new JSONObject();
        JSONArray mustArray = new JSONArray();
        JSONObject mustFather = new JSONObject();
        JSONObject boolFather = new JSONObject();
        JSONObject matchIdFieldFather = new JSONObject();
        JSONObject rangeMatchFather = new JSONObject();
        JSONObject rangeMatch = new JSONObject();
        JSONObject rangeMatchSon = new JSONObject();
        JSONObject matchIdField = new JSONObject();
        JSONObject matchFather = new JSONObject();
        JSONObject matchField = new JSONObject();

        matchIdField.put(searchKey, id);
        matchIdFieldFather.put("match", matchIdField);

        rangeMatchSon.put("gte", GTE);
        rangeMatch.put(SORT_FIELD, rangeMatchSon);
        rangeMatchFather.put("range", rangeMatch);

        matchField.put("status", DefineTool.OnlineStatusesEnum.ONLINE.getIndex());
        matchFather.put("match", matchField);

        mustArray.add(matchIdFieldFather);
        mustArray.add(rangeMatchFather);
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

    private void constructSort() {
        sortJson = new JSONArray();
        JSONObject sortCore = new JSONObject();
        sortCore.put("order", "desc");
        sortCore.put("missing", "_last");
        JSONObject sortObj = new JSONObject();
        sortObj.put(SORT_FIELD, sortCore);
        sortJson.add(sortObj);
    }

    @Override
    public void registerConfig(Map<String, Class> outConfMap) {
        outConfMap.put(KEY_TABLE, String.class);
        outConfMap.put(KEY_COLUMN, String.class);
        outConfMap.put(KEY_PUBLISHER_NUM, Integer.class);
    }
}
