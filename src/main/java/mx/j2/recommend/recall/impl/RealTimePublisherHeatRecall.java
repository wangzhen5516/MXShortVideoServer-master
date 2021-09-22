package mx.j2.recommend.recall.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.newrelic.api.agent.NewRelic;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.data_source.StrategyPoolConfDataSource;
import mx.j2.recommend.manager.DataSourceManager;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.pool_conf.StrategyPoolConf;
import mx.j2.recommend.task.StrategyPoolExecutor;
import mx.j2.recommend.util.*;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @Author: yixin.guo
 * @Date: 2020-11-28
 */

public class RealTimePublisherHeatRecall extends BaseRecall<BaseDataCollection> {
    private static Logger logger = LogManager.getLogger(RealTimePublisherHeatRecall.class);
    private static final int RECALL_SIZE = 200;
    private static final String SEARCH_KEY_FIELD = "publisher_id";
    private final JSONArray sortJson;
    private final static String SORT_FIELD = "heat_score";
    private final static String RANGE_FIELD = "heat_score";
    private final static double min = 0.4;


    public RealTimePublisherHeatRecall() {
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
        String videoId = null;
        if (dc.req.extraClientInfo != null) {
            videoId = dc.req.extraClientInfo.lastInteractiveId;
        }
        if (MXStringUtils.isEmpty(videoId)) {
            return true;
        }
        List<String> type = dc.recommendFlow.realType;
        if (MXCollectionUtils.isEmpty(type)) {
            return true;
        }
        return !type.contains(dc.req.extraClientInfo.lastInteractiveType);
    }

    @Override
    public void recall(BaseDataCollection dc) {
        String videoId = null;
        if (dc.req.extraClientInfo != null) {
            videoId = dc.req.extraClientInfo.lastInteractiveId;
        }
        List<BaseDocument> objects = MXDataSource.details().get(Collections.singletonList(videoId), this.getName());
        if (MXJudgeUtils.isEmpty(objects)) {
            return;
        }
        BaseDocument doc = objects.get(0);
        if (useHashTag()) {
            String tagName = getTagName(doc);
            if (MXStringUtils.isNotBlank(tagName)) {
                doRecall(dc, tagName);
                if (MXCollectionUtils.isNotEmpty(dc.realTimeClickDocList)) {
                    return;
                }
            }
        }
        String publisherId = doc.publisher_id;
        if (MXStringUtils.isEmpty(publisherId)) {
            return;
        }

        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
        String localCacheKey = String.format("%s_%s", this.getName(), publisherId);
        List<BaseDocument> docList = localCacheDataSource.getRealtimePublisherCache(localCacheKey);
        if (null == docList) {
            docList = new ArrayList<>();
            String queryBody = constructQuery(publisherId, null);
            String requestUrlFormat = "/%s/_search?pretty=false";
            String elasticSearchRequest = String.format(requestUrlFormat, DefineTool.CategoryEnum.SHORT_VIDEO.getIndexAndType());
            List<JSONObject> resultJsonList = MXDataSource.videoES().sendSyncSearchPure(elasticSearchRequest, queryBody);
            if (MXJudgeUtils.isEmpty(resultJsonList)) {
                return;
            }

            List<String> idList = ESJsonTool.loadOnlyIdList(resultJsonList);
            if (MXJudgeUtils.isEmpty(idList)) {
                return;
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
            dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.ES.getName());
        }
    }

    public String constructQuery(String id, List<String> sourceList) {
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

        JSONObject rangeFather = new JSONObject();
        JSONObject rangeJson = new JSONObject();
        JSONObject gteJson = new JSONObject();
        gteJson.put("gte", min);
        rangeJson.put(RANGE_FIELD, gteJson);
        rangeFather.put("range", rangeJson);

        mustArray.add(rangeFather);
        mustArray.add(matchIdFieldFather);
        mustArray.add(matchFather);

        mustFather.put("must", mustArray);
        boolFather.put("bool", mustFather);
        content.put("query", boolFather);
        content.put("size", RECALL_SIZE);
        content.put("sort", sortJson);

        if (MXJudgeUtils.isNotEmpty(sourceList)) {
            JSONArray sourceArray = new JSONArray();
            sourceArray.addAll(sourceList);
            content.put("_source", sourceArray);
        }

        return content.toString();
    }

    private String getTagName(BaseDocument doc) {
        Set<String> mlTags = doc.mlTags;
        JSONArray pTags = doc.primaryTags;
        JSONArray sTags = doc.secondaryTags;
        if (MXCollectionUtils.isNotEmpty(mlTags)) {
            return mlTags.iterator().next();
        } else if (MXCollectionUtils.isNotEmpty(pTags)) {
            return pTags.getString(0);
        } else if (MXCollectionUtils.isNotEmpty(sTags)) {
            return sTags.getString(0);
        } else {
            return null;
        }
    }

    private void doRecall(BaseDataCollection dc, String videoTag) {
        StrategyPoolConfDataSource strategyPoolConfDataSource = MXDataSource.strategyPool();
        Set<String> poolSet = strategyPoolConfDataSource.getPoolSet();
        if (CollectionUtils.isEmpty(poolSet)) {
            return;
        }

        Map<String, StrategyPoolConf> needMap = new HashMap<>();
        for (String pool : poolSet) {
            StrategyPoolConf conf = strategyPoolConfDataSource.getStrategyPoolConf(pool);
            if (null == conf) {
                continue;
            }
            if (!conf.excludeSmallFlowList.isEmpty() && conf.excludeSmallFlowList.contains(dc.recommendFlow.name)) {
                continue;
            }
            String key = String.format("%s_%s", conf.poolIndexPrefix, videoTag);
            needMap.put(key, conf);

        }
        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
        StrategyPoolExecutor executor = DataSourceManager.INSTANCE.getStrategyPoolExecutor();
        CountDownLatch cd = new CountDownLatch(needMap.size());
        needMap.forEach((k, v) -> {
            List<BaseDocument> documents = localCacheDataSource.getTopHotTagDocumentCache(k);
            if (CollectionUtils.isNotEmpty(documents)) {
                dc.realTimeClickDocList.addAll(documents);
                dc.syncSearchResultSizeMap.put(this.getName(), documents.size());
                dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.LOCAL.getName());
                cd.countDown();
                return;
            }
            executor.execute(k, dc.realTimeStrategyPoolToDocumentListMap, v.poolRecallSize, this.getName(), cd);
        });
        try {
            cd.await(300, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            NewRelic.noticeError("new error in counter await about maxWaitTimeMs" + e.getMessage());
            e.printStackTrace();
            logger.error("new error in counter await about maxWaitTimeMs", e);
        }
        if (MXCollectionUtils.isNotEmpty(dc.realTimeStrategyPoolToDocumentListMap)) {
            needMap.forEach((k, v) -> {
                if (MXCollectionUtils.isNotEmpty(dc.realTimeStrategyPoolToDocumentListMap.get(k))) {
                    dc.realTimeClickDocList.addAll(dc.realTimeStrategyPoolToDocumentListMap.get(k));
                    dc.syncSearchResultSizeMap.put(this.getName(), dc.realTimeStrategyPoolToDocumentListMap.get(k).size());
                    dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.REDIS.getName());
                }
            });
        }

    }

    public boolean useHashTag() {
        return false;
    }
}
