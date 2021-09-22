package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.ElasticCacheSource;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.data_source.UserProfileDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.BaseMagicValueEnum;
import mx.j2.recommend.util.MXStringUtils;

import static mx.j2.recommend.util.BaseMagicValueEnum.FEATURE30D;

/**
 * @author xuejian.zhang
 * @date 2021-03-20 14:38
 */
public class ShareRateForNewUserFilter extends BaseFilter<BaseDataCollection> {
    protected double shareRate = 0.0008d;
    protected static final int VIEW_TH_LOW = 1000;
    protected int HISTORY_TH = 300;

    @Override
    public boolean skip(BaseDataCollection baseDC) {
        return UserProfileDataSource.isUserOlderThan(baseDC, HISTORY_TH);
    }

    @Override
    public boolean prepare(BaseDataCollection dc) {
        String cachekey = getCacheKey();
        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
        String local = localCacheDataSource.getShareRateCache(cachekey);
        if (MXStringUtils.isNotBlank(local)) {
            shareRate = Double.valueOf(local);
        } else {
            ElasticCacheSource cacheSource = MXDataSource.redis();
            String redis = cacheSource.getStringFromStg(cachekey);
            if (MXStringUtils.isNotBlank(redis)) {
                shareRate = Double.valueOf(redis);
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

        if (!(doc.recallName.contains("PoolRecall") && BaseMagicValueEnum.HIGH_LEVEL.equals(doc.poolLevel))) {
            return false;
        }

        if (!doc.statisticsDocument.exist(FEATURE30D)) {
            return false;
        }

        /*view小于1000也不过滤*/
        if (Double.compare(doc.statisticsDocument.get(FEATURE30D).getViewAll(), VIEW_TH_LOW) < 0) {
            return false;
        }
        return doc.statisticsDocument.get(FEATURE30D).getShareRate() < shareRate;
    }

    public String getCacheKey() {
        return this.getName();
    }
}
