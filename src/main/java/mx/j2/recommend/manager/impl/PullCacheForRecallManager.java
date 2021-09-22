package mx.j2.recommend.manager.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.data_collection.OtherDataCollection;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;

import java.util.List;

public class PullCacheForRecallManager extends BaseCacheManager {

    @Override
    public boolean skip(BaseDataCollection data) {
        return false;
    }

    @Override
    DefineTool.Cache.CacheOperationEnum getOperation() {
        return DefineTool.Cache.CacheOperationEnum.READ;
    }

    @Override
    public void postProcess(BaseDataCollection dc) {
        if (dc instanceof OtherDataCollection) {
            return;
        }

        if (!checkCacheQuality(dc)) {
            dc.mergedList.clear();
        }
    }

    @Override
    public List<String> list(BaseDataCollection dc) {
        return dc.recommendFlow.cacheForRecallList;
    }

    private boolean checkCacheQuality(BaseDataCollection dc) {
        return MXJudgeUtils.isNotEmpty(dc.data.result.resultList);
    }
}