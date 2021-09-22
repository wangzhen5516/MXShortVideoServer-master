package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;

public class PoolDuration7Filter extends BaseFilter {
    private static final int MIN_DURATION = 7000;
    private static final String POOL_RECALL = "PoolRecall";

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection dc) {
        // 临时放开对池子数据的限制
        if (POOL_RECALL.equals(doc.recallName)) {
            return false;
        }
        long duration = doc.duration == 0 ? doc.innerDuration : doc.duration;
        return duration < MIN_DURATION;
    }
}
