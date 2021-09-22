package mx.j2.recommend.recall.impl;

import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.data_collection.info.MXEntityDebugInfo;
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
public class ManualTopRecall extends BaseRecall<BaseDataCollection> {

    private final static int RECALL_SIZE = 200;

    private final static String REDIS_KEY = "manual_top_%s";


    /**
     * 构造函数
     */
    public ManualTopRecall() {
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
    @Trace(dispatcher = true)
    public void recall(BaseDataCollection baseDc) {
        String redisKey = getRedisKey(baseDc);
        String localDocumentCacheKey = String.format("%s_%s_%s", baseDc.req.getInterfaceName(), baseDc.req.getTabId(), redisKey);
        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();

        List<BaseDocument> cacheDocumentList = localCacheDataSource.getScoreWeightRecallCache(localDocumentCacheKey);
        if (MXJudgeUtils.isNotEmpty(cacheDocumentList)) {
            getList(baseDc).addAll(cacheDocumentList);
            baseDc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.LOCAL.getName());
            baseDc.syncSearchResultSizeMap.put(this.getName(), cacheDocumentList.size());
            return;
        }

        String localRedisListCacheKey = String.format("%s_%s_%s_%s", baseDc.req.getInterfaceName(), baseDc.req.getTabId(), redisKey, "_manual_top");
        Map<String, Double> manualControlMap = localCacheDataSource.getManualControlRedisListCache(localRedisListCacheKey);

        List<BaseDocument> resultDocumentList;
        if (MXJudgeUtils.isNotEmpty(manualControlMap)) {
            resultDocumentList = MXDataSource.details().get(manualControlMap.keySet(), getName());
        } else {
            ElasticCacheSource elasticCacheSource = MXDataSource.redis();
            manualControlMap = elasticCacheSource.getManualControltCache(redisKey, getRecallSize() - 1);

            if (MXJudgeUtils.isEmpty(manualControlMap)) {
                return;
            } else {
                localCacheDataSource.setManualControlRedisListCache(localRedisListCacheKey, manualControlMap);
            }
            resultDocumentList = MXDataSource.details().get(manualControlMap.keySet(), getName());
        }

        for (BaseDocument doc : resultDocumentList) {
            if (manualControlMap.containsKey(doc.id)) {
                doc.scoreDocument.manualTopScore = manualControlMap.get(doc.id);
            }

            // 召回信息
            MXEntityDebugInfo debugInfo = baseDc.debug.getDebugInfoByEntityId(doc.id);
            debugInfo.recall.name = getName();
        }

        resultDocumentList.sort((doc0, doc1) -> Double.compare(doc1.scoreDocument.manualTopScore, doc0.scoreDocument.manualTopScore));

        localCacheDataSource.setScoreWeightRecallCache(localDocumentCacheKey, resultDocumentList, 120);
        baseDc.syncSearchResultSizeMap.put(this.getName(), resultDocumentList.size());
        getList(baseDc).addAll(resultDocumentList);
        baseDc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.ES.getName());
        baseDc.syncSearchResultSizeMap.put(this.getName() + "_LocalCache_Push", resultDocumentList.size());
    }

    public String getRedisKey(BaseDataCollection dc) {
        return String.format(REDIS_KEY, getRedisKeyPattern(dc));
    }

    public String getRedisKeyPattern(BaseDataCollection dc) {
        return dc.recommendFlow.name.replace("mx_hot_tab_internal_version_2_0_", "");
    }

    public List<BaseDocument> getList(BaseDataCollection dc) {
        return dc.highPriorityVideoForNewUserList;
    }

    public int getRecallSize() {
        return RECALL_SIZE;
    }
}