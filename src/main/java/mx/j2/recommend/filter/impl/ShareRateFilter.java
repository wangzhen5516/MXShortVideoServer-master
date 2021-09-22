package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.ElasticCacheSource;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.BaseMagicValueEnum;
import mx.j2.recommend.util.MXStringUtils;

/**
 * @author qiqi
 * @date 2021-02-04 14:38
 */
public class ShareRateFilter extends BaseFilter {
    protected double ratio = 0.0006d;
    protected static final int VIEW_TH_LOW = 1000;

    @Override
    public boolean prepare(BaseDataCollection dc) {
        String cachekey = getCacheKey();
        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
        String local = localCacheDataSource.getShareRateCache(cachekey);
        if (MXStringUtils.isNotBlank(local)) {
            ratio = Double.valueOf(local);
        } else {
            ElasticCacheSource cacheSource = MXDataSource.redis();
            String redis = cacheSource.getStringFromStg(cachekey);
            if (MXStringUtils.isNotBlank(redis)) {
                ratio = Double.valueOf(redis);
                localCacheDataSource.setShareRateCache(cachekey, redis);
            }
        }

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
        if (Double.compare(doc.statisticsDocument.getViewAll30d(), VIEW_TH_LOW) < 0) {
            return false;
        }

        return doc.statisticsDocument.shareRate30d < ratio;
    }

    public String getCacheKey() {
        return this.getName();
    }
}
