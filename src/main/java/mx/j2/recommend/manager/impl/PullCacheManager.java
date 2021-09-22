package mx.j2.recommend.manager.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.data_collection.FeedDataCollection;
import mx.j2.recommend.data_model.data_collection.OtherDataCollection;
import mx.j2.recommend.manager.MXManager;
import mx.j2.recommend.util.DefineTool;

import java.util.List;

import static mx.j2.recommend.util.DefineTool.Cache.CacheStatus.IgnoreAll;
import static mx.j2.recommend.util.DefineTool.Cache.CacheStatus.IgnoreNothing;

public class PullCacheManager extends BaseCacheManager {

    @Override
    public boolean skip(BaseDataCollection data) {
        return false;
    }

    @Override
    DefineTool.Cache.CacheOperationEnum getOperation() {
        return DefineTool.Cache.CacheOperationEnum.READ;
    }

    @Override
    public List<String> list(BaseDataCollection dc) {
        return dc.recommendFlow.cacheList;
    }

    @Override
    public void postProcess(BaseDataCollection dc) {
        if (dc instanceof OtherDataCollection) {
            return;
        }

        if (!checkCacheQuality(dc)) {
            dc.cachedResultList.clear();
            dc.util.cacheStatus = IgnoreNothing;
        } else {
            dc.util.cacheStatus = IgnoreAll;
        }
    }

    private boolean checkCacheQuality(BaseDataCollection dc) {
        if (dc instanceof FeedDataCollection) {
            MXManager.filter().getComponentInstance("HistoryFilterForResultCache").filter(dc);
            MXManager.filter().getComponentInstance("SpecialFilterForResultCache").filter(dc);
            // 确保数量够多, 才走缓存, 否则走正常逻辑
            return (dc.cachedResultList.size() > dc.req.getNum() + 5) ? true : false;
        }

        return false;
    }
}