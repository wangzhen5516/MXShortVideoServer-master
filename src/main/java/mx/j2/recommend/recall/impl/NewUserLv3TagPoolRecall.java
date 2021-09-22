package mx.j2.recommend.recall.impl;


import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;

import mx.j2.recommend.data_source.LocalCacheDataSource;

import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXCollectionUtils;

import java.util.List;


public class NewUserLv3TagPoolRecall extends BaseRecall<BaseDataCollection> {

    private static String LOCAL_CACHE_KEY = "tag_pool_lv3_video_list";

    @Override
    public boolean skip(BaseDataCollection dc) {
        return false;
    }

    @Override
    public void recall(BaseDataCollection dc) {
        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
        List<BaseDocument> cacheList = localCacheDataSource.getTagPoolLv3VideoListCache(LOCAL_CACHE_KEY);
        if (!MXCollectionUtils.isEmpty(cacheList)) {
            dc.tagPoolLv3List.addAll(cacheList);
            dc.syncSearchResultSizeMap.put(this.getName(), cacheList.size());
            dc.resultFromMap.put(this.getName(), DefineTool.RecallFrom.LOCAL.getName());
        }
    }
}
