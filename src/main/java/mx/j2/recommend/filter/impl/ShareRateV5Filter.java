package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.document.StatisticsDocument;
import mx.j2.recommend.data_source.ElasticCacheSource;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.util.BaseMagicValueEnum;
import mx.j2.recommend.util.MXStringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ShareRateV5Filter extends BaseFilter{
    protected static double ratio = 0.0008;
    protected static double fr5cRatio = 0.5;
    protected static double frsRatio = 6.0;

    private static String fr5cFormat = "fr5c_%s";
    private static String frsFormat = "frs_%s";

    protected static final int VIEW_TH_LOW = 1000;
    private static final List<String> POOL_INDEX = Arrays.asList("lv7", "lv8", "lv9", "lv10", "lv11");


    @Override
    public boolean prepare(BaseDataCollection dc) {
        String cachekey = getCacheKey();
        String cachekeyfr5c = getCacheKeyfr5c();
        String cachekeyfrs = getCacheKeyfrs();

        LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
        String local = localCacheDataSource.getShareRateCache(cachekey);
        String localfr5c = localCacheDataSource.getShareRateCache(cachekeyfr5c);
        String localfrs = localCacheDataSource.getShareRateCache(cachekeyfrs);

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

        if (MXStringUtils.isNotBlank(localfr5c)) {
            fr5cRatio = Double.valueOf(localfr5c);
        } else {
            ElasticCacheSource cacheSource = MXDataSource.redis();
            String redis = cacheSource.getStringFromStg(cachekeyfr5c);
            if (MXStringUtils.isNotBlank(redis)) {
                fr5cRatio = Double.valueOf(redis);
                localCacheDataSource.setShareRateCache(cachekeyfr5c, redis);
            }
        }

        if (MXStringUtils.isNotBlank(localfrs)) {
            frsRatio = Double.valueOf(localfrs);
        } else {
            ElasticCacheSource cacheSource = MXDataSource.redis();
            String redis = cacheSource.getStringFromStg(cachekeyfrs);
            if (MXStringUtils.isNotBlank(redis)) {
                frsRatio = Double.valueOf(redis);
                localCacheDataSource.setShareRateCache(cachekeyfrs, redis);
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
        String poolIndex = doc.poolIndex;
        if (skipPoolIndex(poolIndex) && MXStringUtils.isNotBlank(poolIndex)) {
            String[] index = poolIndex.split("_");
            List<String> indexList = Arrays.asList(index);
            List<String> res = new ArrayList<>(indexList);
            res.retainAll(POOL_INDEX);
            /*表示有交集,7-11级池子不过滤*/
            if (res.size() > 0) {
                return false;
            }
        }
        /*只针对高级池子和非池子召回数据过滤*/
        if (!doc.statisticsDocument.isLoadSuccess()) {
            return false;
        }
        /*view小于1000也不过滤*/
        if (Double.compare(doc.statisticsDocument.getViewAll30d(), VIEW_TH_LOW) < 0) {
            return false;
        }
        StatisticsDocument statDoc = doc.statisticsDocument;
        if(statDoc.shareRate30d < ratio || statDoc.getFinishRate5sCut30d() < fr5cRatio || statDoc.getFinishRetentionSum10s30d() < frsRatio){
            return true;
        }
        return false;
    }

    public String getCacheKey() {
        return this.getName();
    }
    public String getCacheKeyfr5c() {
        return String.format(fr5cFormat,this.getName());
    }
    public String getCacheKeyfrs() {
        return String.format(frsFormat,this.getName());
    }

    public boolean skipPoolIndex(String poolIndex) {
        return false;
    }
}
