package mx.j2.recommend.recall.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.List;

/**
 * @author Qi Mao
 * @date 1/20/2021
 * @description 30天内大V的视频数据的召回，按finish_retention_sum_10s字段排序...
 */
public class TopKOLVideoIn30DaysRecall extends BaseRecall<BaseDataCollection> {
    private static final String REDIS_KEY = "vip_30d_top_videos_v1";
    private static String CACHE_KEY = "topKOLvideoin30days";

    @Override
    public boolean skip(BaseDataCollection dc) {
        if (!DefineTool.TabInfoEnum.HOT.getId().equals(dc.req.tabId)) {
            return true;
        }
        return false;
    }

    @Override
    public void recall(BaseDataCollection dc) {
        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
        List<BaseDocument> resultList = localCacheDataSource.getTopKolVideo30DaysRedisCache(CACHE_KEY);
        if (null != resultList){
            dc.topKOLVideoIn30DaysList.addAll(resultList);
            dc.syncSearchResultSizeMap.put(this.getName(),resultList.size());
            dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.LOCAL.getName());
            return;
        }

        List<String> videoIdList = MXDataSource.redis().getVideoFeatureZsetInfoFromRedis(REDIS_KEY);
        if(MXJudgeUtils.isEmpty(videoIdList)){
            return;
        }

        List<BaseDocument> videoList = MXDataSource.details().get(videoIdList, this.getName());
        if(MXJudgeUtils.isEmpty(videoList)){
            return;
        }
        localCacheDataSource.setTopKolVideo30DaysRedisCache(CACHE_KEY, videoList);
        dc.topKOLVideoIn30DaysList.addAll(videoList);
        dc.syncSearchResultSizeMap.put(this.getName(),videoList.size());
        dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.REDIS.getName());
    }
}
