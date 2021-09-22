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
 */
public class TopKOLVideoIn30DaysRecallNew extends BaseRecall<BaseDataCollection> {
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
        if(MXJudgeUtils.isEmpty(resultList)){
            return;
        }
        dc.topKOLVideoIn30DaysList.addAll(resultList);
        dc.syncSearchResultSizeMap.put(this.getName(),resultList.size());
        dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.LOCAL.getName());
    }
}
