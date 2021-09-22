package mx.j2.recommend.recall.impl;

import com.newrelic.api.agent.Trace;
import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.interfaces.IDocumentProcessor;
import mx.j2.recommend.data_source.ElasticCacheSource;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.pool_conf.PoolConf;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * 用户个性化
 * @author zhongren.li
 */
public class SingleToPool11Recall extends BaseRecall<BaseDataCollection> {
    private static Logger log = LogManager.getLogger(SingleToPool11Recall.class);

    private final static int CACHE_TIME_SECONDS = 600;

    private static final int RECALL_SIZE = 500;

    protected String REDIS_KEY = "tophot_kol_ge_lv11";

    @Override
    public boolean skip(BaseDataCollection baseDc) {
        if (!DefineTool.TabInfoEnum.HOT.getId().equals(baseDc.req.tabId)) {
            return true;
        }
        // logined filter
        if (baseDc.client.user.isLogined) {
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
        Map<String, Double> manualControlMap = elasticCacheSource.getManualControltCache(REDIS_KEY, RECALL_SIZE);
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
        if (getList(baseDc) != null) {
            Collections.shuffle(getList(baseDc));
        }
    }

    private String construcCacheKey(BaseDataCollection baseDc){
        return "SingleRecall_only";
    }

    /**
     * 子类可能复写
     */
    protected List<BaseDocument> getList(BaseDataCollection baseDc) {
        PoolConf conf = MXDataSource.pools().getPoolConfByLevel(11, baseDc);
        if(conf == null) {
            return null;
        }
        return baseDc.poolToDocumentListMap.get(conf.poolIndex);
    }

}
