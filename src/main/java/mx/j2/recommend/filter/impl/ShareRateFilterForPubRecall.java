package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import org.apache.commons.lang.StringUtils;

import java.util.Map;

import static mx.j2.recommend.util.BaseMagicValueEnum.FEATURE30D;

/**
 * 可配置过滤器模板类
 * shareRate配置船新版本
 *
 * @author Qi Qi
 * @sup Qi Mao
 */

public class ShareRateFilterForPubRecall extends BaseFilter<BaseDataCollection> {
    protected double ratio = 0.0004d;
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
        if (StringUtils.isNotEmpty(doc.recallName) && ("RealTimePubRecall".equals(doc.recallName) || "LongTermPubRecall".equals(doc.recallName))) {

            if (!doc.statisticsDocument.isLoadSuccess()) {
                return false;
            }

            if (!doc.statisticsDocument.exist(FEATURE30D)) {
                return false;
            }

            /*view小于1000也不过滤*/
            if (Double.compare(doc.statisticsDocument.get(FEATURE30D).getViewAll(), VIEW_TH_LOW) < 0) {
                return false;
            }
            return doc.statisticsDocument.get(FEATURE30D).getShareRate() < ratio;
        }
        return false;
    }
}
