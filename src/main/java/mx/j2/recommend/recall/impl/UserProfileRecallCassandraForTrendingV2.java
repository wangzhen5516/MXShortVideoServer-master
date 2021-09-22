package mx.j2.recommend.recall.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.data_source.StrategyCassandraDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.BloomUtil;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;

import java.util.List;

public class UserProfileRecallCassandraForTrendingV2 extends BaseRecall<BaseDataCollection> {

    private final static int CACHE_TIME_SECONDS = 1800;

    private final static String TABLE_NAME = "taka_xgboost_v2";

    @Override
    public boolean skip(BaseDataCollection baseDc) {
        if (!DefineTool.TabInfoEnum.HOT.getId().equals(baseDc.req.tabId)) {
            return true;
        }
        return false;
    }

    @Override
    public void recall(BaseDataCollection baseDc) {
        //本地缓存key
        String cacheKey = constructCacheKey(baseDc);

        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
        List<BaseDocument> cacheDocumentList = localCacheDataSource.getScoreWeightRecallCache(cacheKey);

        //如果本地缓存有数据，直接取出返回
        if (MXJudgeUtils.isNotEmpty(cacheDocumentList)) {
            baseDc.userProfileTrendingOfflineRecommendList.addAll(cacheDocumentList);
            baseDc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.LOCAL.getName());
            baseDc.syncSearchResultSizeMap.put(this.getName(), cacheDocumentList.size());
            return;
        }

        //从cassandra中取出视频id和score
        StrategyCassandraDataSource strategyCassandraDataSource = MXDataSource.strategyCA();
        String result = strategyCassandraDataSource.getStrategyOutPutFromCassandraById(BloomUtil.getUuid(baseDc), TABLE_NAME);
        if (MXStringUtils.isEmpty(result)) {
            //用defaultuuid搜
            strategyCassandraDataSource.getStrategyOutputforTrendingByDefaultUuid(this, baseDc, TABLE_NAME);
            return;
        }
        strategyCassandraDataSource.processResultFromCassandra(result, this, cacheKey, CACHE_TIME_SECONDS, baseDc);
    }

    private String constructCacheKey(BaseDataCollection baseDc) {
        return String.format("%s_%s_%s", this.getName(), baseDc.req.getInterfaceName(), BloomUtil.getUuid(baseDc));
    }
}