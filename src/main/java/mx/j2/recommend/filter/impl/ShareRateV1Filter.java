package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.util.BaseMagicValueEnum;

public class ShareRateV1Filter extends ShareRateFilter{
    private final double shareRatio = 0.0004d;

    @Override
    public String getCacheKey() {
        return this.getName();
    }

    // 不走redis所以复写为空
    @Override
    public boolean prepare(BaseDataCollection dc) {
        return true;
    }

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection dc) {
        if (doc == null) {
            return true;
        }

        if (!doc.statisticsDocument.isLoadSuccess()) {
            return false;
        }

        if (!(doc.recallName.contains("PoolRecall") && BaseMagicValueEnum.HIGH_LEVEL.equals(doc.poolLevel))
                && !doc.recallName.contains("StrategyTagPoolRecallWeightedShuffleNumber2")) {
            return false;
        }

        /*view小于1000也不过滤*/
        if (Double.compare(doc.statisticsDocument.getViewAll3d(), VIEW_TH_LOW) < 0) {
            return false;
        }

        return doc.statisticsDocument.shareRate3d < shareRatio;
    }
}
