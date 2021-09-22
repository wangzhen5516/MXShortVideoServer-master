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
 * @date 2021-02-04 11:10
 */
public class LikeDivideShareFilter extends BaseFilter {


    /**
     * 初始化
     */
    private static int ratio = 15;
    private static final int VIEW_TH_LOW = 1000;

    @Override
    public boolean prepare(BaseDataCollection dc) {
        String localKey = getCacheKey();
        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
        String local = localCacheDataSource.getLikeDivideShareCache(localKey);
        if (MXStringUtils.isNotBlank(local) && MXStringUtils.isNumeric(local)) {
            ratio = Integer.valueOf(local);
        } else {
            ElasticCacheSource cacheSource = MXDataSource.redis();
            String redis = cacheSource.getStringFromStg(localKey);
            if (MXStringUtils.isNotBlank(redis) && MXStringUtils.isNumeric(redis)) {
                ratio = Integer.valueOf(redis);
                localCacheDataSource.setLikeDivideShareCache(localKey, redis);
            }
        }

        return true;
    }

    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection dc) {
        if (doc == null) {
            return true;
        }
        if (MXStringUtils.isNotBlank(doc.poolLevel) && !BaseMagicValueEnum.HIGH_LEVEL.equals(doc.poolLevel)) {
            return false;
        }
        /*只针对高级池子和非池子召回数据过滤*/
        double shareData = doc.statisticsDocument.shareRate30d;
        double likeData = doc.statisticsDocument.likeRate30d;
        if (shareData == 0) {
            return false;
        }
        /*view小于1000也不过滤*/
        if (Double.compare(doc.statisticsDocument.getViewAll30d(), VIEW_TH_LOW) < 0) {
            return false;
        }
        return likeData / shareData > ratio;
    }

    public String getCacheKey() {
        return this.getName();
    }
}
