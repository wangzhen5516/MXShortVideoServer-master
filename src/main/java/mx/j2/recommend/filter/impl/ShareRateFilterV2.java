package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.util.BaseMagicValueEnum;

import java.util.Map;

/**
 * 可配置过滤器模板类
 * shareRate配置船新版本
 */
@SuppressWarnings("unused")
public class ShareRateFilterV2 extends BaseFilter<BaseDataCollection> {
    protected double ratio = 0.0007d;
    protected static final int VIEW_TH_LOW = 1000;
    String configFiled = "ratio";

    @Override
    public boolean prepare(BaseDataCollection dc) {
        ratio = config.getDouble(configFiled);
        return true;
    }

    @Override
    public void registerConfig(Map outConfMap) {
        outConfMap.put(configFiled, Double.class);
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
        if (Double.compare(doc.statisticsDocument.getViewAll30d(), VIEW_TH_LOW) < 0) {
            return false;
        }
        return doc.statisticsDocument.shareRate30d < ratio;
    }
}
