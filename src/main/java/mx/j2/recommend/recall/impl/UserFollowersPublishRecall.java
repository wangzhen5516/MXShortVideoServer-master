package mx.j2.recommend.recall.impl;

import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.interfaces.IDocumentProcessor;
import mx.j2.recommend.data_source.ElasticCacheSource;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.BloomUtil;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * 用户个性化
 * @author zhongren.li
 */
public class UserFollowersPublishRecall extends BaseRecall<BaseDataCollection> {
    private static Logger log = LogManager.getLogger(UserFollowersPublishRecall.class);

    private final static int CACHE_TIME_SECONDS = 600;

    private static final int RECALL_WEIGHT = 1200;
    private static final int RECALL_SIZE = 200;

    // 子类可能重写覆盖
    protected String REDIS_KEY = "offline_reco_%s";

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
        String cacheKey  = construcCacheKey(baseDc);
        List<BaseDocument> toList = getList(baseDc);
        if(toList == null) {
            log.error("Errorlist:"+this.getName()+" Request: "+baseDc.req);
            return;
        }

        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
        List<BaseDocument> cacheDocumentList = localCacheDataSource.getScoreWeightRecallCache(cacheKey);
        if (MXJudgeUtils.isNotEmpty(cacheDocumentList)) {
            toList.addAll(cacheDocumentList);
            baseDc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.LOCAL.getName());
            baseDc.syncSearchResultSizeMap.put(this.getName(), cacheDocumentList.size());
            return;
        }

        ElasticCacheSource elasticCacheSource = MXDataSource.redis();
        String redisKey = String.format(REDIS_KEY, BloomUtil.getUserId(baseDc));
        Map<String, Double> manualControlMap = elasticCacheSource.getManualControltCache(redisKey, RECALL_SIZE);
        if (MXJudgeUtils.isEmpty(manualControlMap)) {
            return;
        }

        IDocumentProcessor processor = document -> document.scoreDocument.manualTopScore = manualControlMap.get(document.id);
        List<BaseDocument> mergedList = MXDataSource.details().get(manualControlMap.keySet(), getName(), processor);

        mergedList.sort((doc0, doc1) -> Double.compare(doc1.scoreDocument.manualTopScore, doc0.scoreDocument.manualTopScore));
        localCacheDataSource.setScoreWeightRecallCache(cacheKey, mergedList, CACHE_TIME_SECONDS);
        toList.addAll(mergedList);
        baseDc.syncSearchResultSizeMap.put(this.getName(), mergedList.size());
        baseDc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.ES.getName());
        doSomethingAfterRecall(baseDc);
    }

    protected void doSomethingAfterRecall(BaseDataCollection baseDc) {
    }

    private String construcCacheKey(BaseDataCollection baseDc){
        return String.format("%s_%s_%s", this.getName(), baseDc.req.getInterfaceName(), BloomUtil.getUserId(baseDc));
    }

    @Override
    public float getRecallWeightScore() {
        return RECALL_WEIGHT;
    }

    /**
     * 子类可能复写
     */
    protected List<BaseDocument> getList(BaseDataCollection baseDc) {
        return baseDc.userFollowPublishList;
    }

    public static void main(String[] args) {
    }
}
