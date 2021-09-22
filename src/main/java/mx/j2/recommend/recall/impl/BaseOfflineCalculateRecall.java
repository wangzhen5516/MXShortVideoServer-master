package mx.j2.recommend.recall.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.CanGetAtStartRecallDocumentDataSource;
import mx.j2.recommend.data_source.ElasticCacheSource;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.recall_data_in_mem.ESCanGetStartRecall;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.List;
import java.util.Map;

/**
 * @ Author     ：zhongrenli
 * @ Date       ：Created in 下午4:05 2020/08/05
 * @ Description：策略类基础类，避免重复编码
 * @author zhongrenli
 */
public abstract class BaseOfflineCalculateRecall extends BaseRecall<BaseDataCollection> implements ESCanGetStartRecall{

    /**
     * 构造函数
     */
    public BaseOfflineCalculateRecall() {
        init();
    }

    /**
     * 初始化
     */
    @Override
    public void init() {}

    @Override
    public void recall(BaseDataCollection baseDc) {
        if (!DefineTool.TabInfoEnum.HOT.getId().equals(baseDc.req.tabId)) {
            return;
        }

        List<BaseDocument> resultDocumentList = CanGetAtStartRecallDocumentDataSource
                .INSTANCE.getDocumentsForESRecall(this, getESRequestKey());

        if (MXJudgeUtils.isEmpty(resultDocumentList)) {
            return;
        }

        Map<String, Double> offlineCalculateMap = getOfflineCalculateMap();
        if (MXJudgeUtils.isEmpty(offlineCalculateMap)) {
            return;
        }

        score(resultDocumentList, offlineCalculateMap);
        resultDocumentList.sort((doc0, doc1) -> Double.compare(doc1.scoreDocument.offlineCalculateScore, doc0.scoreDocument.offlineCalculateScore));

        baseDc.syncSearchResultSizeMap.put(this.getName(), resultDocumentList.size());

        List<BaseDocument> resultList = getRecallResultDocumentList(baseDc);
        if (null != resultList) {
            resultList.addAll(resultDocumentList);
        }
    }

    public Map<String, Double> getOfflineCalculateFromRedisMap() {
        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
        String localRedisListCacheKey = getLocalCacheKey();

        ElasticCacheSource elasticCacheSource = MXDataSource.redis();
        Map<String, Double> offlineCalculateMap = elasticCacheSource.getManualControltCache(getRedisKey(), getRecallSize());

        if (MXJudgeUtils.isEmpty(offlineCalculateMap)) {
            return offlineCalculateMap;
        } else {
            localCacheDataSource.setManualControlRedisListCache(localRedisListCacheKey, offlineCalculateMap);
        }
        return offlineCalculateMap;
    }

    public Map<String, Double> getOfflineCalculateMap() {
        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
        String localRedisListCacheKey = getLocalCacheKey();
        Map<String, Double> offlineCalculateMap = localCacheDataSource.getManualControlRedisListCache(localRedisListCacheKey);

        if (MXJudgeUtils.isEmpty(offlineCalculateMap)) {
            ElasticCacheSource elasticCacheSource = MXDataSource.redis();
            offlineCalculateMap = elasticCacheSource.getManualControltCache(getRedisKey(), getRecallSize());

            if (MXJudgeUtils.isEmpty(offlineCalculateMap)) {
                return offlineCalculateMap;
            } else {
                localCacheDataSource.setManualControlRedisListCache(localRedisListCacheKey, offlineCalculateMap);
            }
        }
        return offlineCalculateMap;
    }

    /**
     * 返回召回数量，默认 200
     * @return recall size
     */
    public int getRecallSize() {
        return 200;
    }

    /**
     * 获取 redis 流量池的 key
     * @return key
     */
    public abstract String getRedisKey();

    /**
     * 如果做本地缓存，获取本地缓存的 key
     * @return local cache key
     */
    public abstract String getLocalCacheKey();

    /**
     * 获取承载本次召回结果的 list
     * @return list
     */
    public abstract List<BaseDocument> getRecallResultDocumentList(BaseDataCollection dc);

    /**
     * 获取随机因子，可以提供默认实现
     * @return 随机因子
     */
    @Override
    public abstract int getRandomFactor();

    /**
     * 给每个 doc 打分，提供默认实现
     * @param resultDocumentList 结果 list
     * @param offlineCalculateMap 打分表，可以自定义
     */
    public void score(List<BaseDocument> resultDocumentList, Map<String, Double> offlineCalculateMap) {
        for (BaseDocument doc : resultDocumentList) {
            if (offlineCalculateMap.containsKey(doc.id)) {
                doc.scoreDocument.offlineCalculateScore = offlineCalculateMap.get(doc.id);
            }
        }
    }

    public String getRequestUrlFormat(){
        return "/%s/_search?pretty=false";
    }

    public String getESRequestKey(){
        return "DEFAULT";
    }

    @Override
    public String getRecallName() {
        return this.getName();
    }

    @Override
    public float getRecallDocumentWeight() {
        return 0;
    }

    @Override
    public abstract Map<String, BaseDataCollection.ESRequest> getESRequestMap();

    /**
     * TODO Hook，do what you should do, after refresh
     */
    @Override
    public abstract void doSomethingAfterLoad();

}