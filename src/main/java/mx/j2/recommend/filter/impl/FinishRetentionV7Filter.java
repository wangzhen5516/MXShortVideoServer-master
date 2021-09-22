package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_source.ElasticCacheSource;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.MXStringUtils;

import static mx.j2.recommend.util.BaseMagicValueEnum.FEATURE30D;

@SuppressWarnings("unused")
public class FinishRetentionV7Filter extends BaseFilter {
    protected static double ratio = 6.5d;
    private static final int VIEW_TH_LOW = 1000;

    @Override
    public boolean prepare(BaseDataCollection dc) {
        String cachekey = getCacheKey();
        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
        String local = localCacheDataSource.getFinishRetentionCache(cachekey);
        if (MXStringUtils.isNotBlank(local)) {
            ratio = Double.parseDouble(local);
        } else {
            ElasticCacheSource cacheSource = MXDataSource.redis();
            String redis = cacheSource.getStringFromStg(cachekey);
            if (MXStringUtils.isNotBlank(redis)) {
                ratio = Double.parseDouble(redis);
                localCacheDataSource.setFinishRetentionCache(cachekey, redis);
            }
        }

        return true;
    }

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection dc) {
        if (doc == null) {
            return true;
        }

        if (doc.getPriority() < 7) {
            return false;
        }

        /*只针对高级池子和非池子召回数据过滤*/
        if (!doc.statisticsDocument.isLoadSuccess()) {
            return false;
        }
        /*view小于1000也不过滤*/
        if (!doc.statisticsDocument.exist(FEATURE30D)) {
            return false;
        }

        if (Double.compare(doc.statisticsDocument.get(FEATURE30D).getViewAll(), VIEW_TH_LOW) < 0) {
            return false;
        }

        return doc.statisticsDocument.get(FEATURE30D).getFinishRetentionSum10s() < ratio;
    }

    public String getCacheKey() {
        return this.getName();
    }
}
