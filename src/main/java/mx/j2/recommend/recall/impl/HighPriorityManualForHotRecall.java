package mx.j2.recommend.recall.impl;

import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.ElasticCacheSource;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.List;
import java.util.Map;

/**
 * @author zhongrenli
 * @ Author     ：zhongrenli
 * @ Date       ：Created in 下午4:05 2018/12/5
 * @ Description：${description}
 */
public class HighPriorityManualForHotRecall extends BaseRecall<BaseDataCollection> {

    private final static int RECALL_SIZE = 5;

    private final static String REDIS_KEY = "manual_top_trending_list";


    /**
     * 构造函数
     */
    public HighPriorityManualForHotRecall() {
        init();
    }

    /**
     * 初始化
     */
    @Override
    public void init() {
        requestUrlFormat = "/%s/_search?pretty=false";
    }

    @Override
    public boolean skip(BaseDataCollection baseDc) {
        if (!DefineTool.TabInfoEnum.HOT.getId().equals(baseDc.req.tabId)) {
            return true;
        }
        return false;
    }

    @Override
    @Trace(dispatcher = true)
    public void recall(BaseDataCollection baseDc) {
        String localDocumentCacheKey = String.format("%s_%s_%s", baseDc.req.getInterfaceName(), baseDc.req.getTabId(), REDIS_KEY);
        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();

        List<BaseDocument> cacheDocumentList = localCacheDataSource.getScoreWeightRecallCache(localDocumentCacheKey);
        if (MXJudgeUtils.isNotEmpty(cacheDocumentList)) {
            baseDc.highPriorityManualList.addAll(cacheDocumentList);
            baseDc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.LOCAL.getName());
            baseDc.syncSearchResultSizeMap.put(this.getName(), cacheDocumentList.size());
            return;
        }

        String localRedisListCacheKey = String.format("%s_%s_%s_%s", baseDc.req.getInterfaceName(), baseDc.req.getTabId(), REDIS_KEY, "_manual_redis_list");
        Map<String, Double> manualControlMap = localCacheDataSource.getManualControlRedisListCache(localRedisListCacheKey);

        List<BaseDocument> resultDocumentList;
        if (MXJudgeUtils.isNotEmpty(manualControlMap)) {
            baseDc.highPriorityManualIdSet.addAll(manualControlMap.keySet());
            resultDocumentList = MXDataSource.details().get(manualControlMap.keySet(), getName());
        } else {
            ElasticCacheSource elasticCacheSource = MXDataSource.redis();
            manualControlMap = elasticCacheSource.getManualControltCache(REDIS_KEY, RECALL_SIZE - 1);

            if (MXJudgeUtils.isEmpty(manualControlMap)) {
                return;
            } else {
                localCacheDataSource.setManualControlRedisListCache(localRedisListCacheKey, manualControlMap);
            }
            baseDc.highPriorityManualIdSet.addAll(manualControlMap.keySet());
            resultDocumentList = MXDataSource.details().get(manualControlMap.keySet(), getName());
        }

        for (BaseDocument doc : resultDocumentList) {
            if (manualControlMap.containsKey(doc.id)) {
                doc.scoreDocument.manualTopScore = manualControlMap.get(doc.id);
            }
        }

        resultDocumentList.sort((doc0, doc1) -> Double.compare(doc1.scoreDocument.manualTopScore, doc0.scoreDocument.manualTopScore));

        localCacheDataSource.setScoreWeightRecallCache(localDocumentCacheKey, resultDocumentList, 120);

        baseDc.syncSearchResultSizeMap.put(this.getName(), resultDocumentList.size());
        baseDc.highPriorityManualList.addAll(resultDocumentList);
        baseDc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.ES.getName());
        baseDc.syncSearchResultSizeMap.put(this.getName() + "_LocalCache_Push", resultDocumentList.size());
    }
}