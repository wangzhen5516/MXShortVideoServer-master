package mx.j2.recommend.manager.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.util.DefineTool;

import java.util.List;

import static mx.j2.recommend.util.DefineTool.Cache.CacheStatus.IgnoreRecall;

public class PushCacheManager extends BaseCacheManager {

    @Override
    public boolean skip(BaseDataCollection dc) {
        return super.skip(dc) || IgnoreRecall == dc.util.cacheStatus;
    }

    @Override
    DefineTool.Cache.CacheOperationEnum getOperation() {
        return DefineTool.Cache.CacheOperationEnum.WRITE;
    }

    @Override
    public List<String> list(BaseDataCollection dc) {
        return dc.recommendFlow.cacheList;
    }
}