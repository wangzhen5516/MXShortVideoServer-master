package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.util.BaseMagicValueEnum;

public class ShareRateV7Filter extends ShareRateFilter {
    public static double LIKERATE0D_LIMIT = 0.04d;

    @Override
    public String getCacheKey() {
        return this.getName();
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

        /*LikeRate0d大于%2也不过滤, LIKERATE0D_LIMIT = 0.02d*/
        if (Double.compare(doc.statisticsDocument.getLikeRate0d(), LIKERATE0D_LIMIT) > 0) {
            return false;
        }

        return doc.statisticsDocument.shareRate3d < ratio;
    }
}
