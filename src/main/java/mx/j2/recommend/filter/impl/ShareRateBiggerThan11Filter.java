package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;

import static mx.j2.recommend.util.BaseMagicValueEnum.FEATURE30D;

/**
 * @author liangjie.feng
 * @version 1.0
 * @date 2021/3/30 下午6:56
 * @description
 */
public class ShareRateBiggerThan11Filter extends ShareRateFilter {
    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection dc) {
        if (doc == null) {
            return true;
        }

        if (!doc.statisticsDocument.isLoadSuccess()) {
            return false;
        }

        if (!(doc.recallName.contains("PoolRecall") && doc.getPoolPriority() >= 11)
                && !doc.recallName.contains("StrategyTagPoolRecallWeightedShuffleNumber2")) {
            return false;
        }

        /*view小于1000也不过滤*/
        if (!doc.statisticsDocument.exist(FEATURE30D)) {
            return false;
        }

        if (Double.compare(doc.statisticsDocument.get(FEATURE30D).getViewAll(), VIEW_TH_LOW) < 0) {
            return false;
        }

        if ("mx_hot_tab_internal_version_2_0_B2".equals(dc.recommendFlow.name) &&
                doc.statisticsDocument.get(FEATURE30D).getFinishRetentionSum10s() >= 7.8) {
            return false;
        }

        return doc.statisticsDocument.get(FEATURE30D).getShareRate() < ratio;
    }
}
