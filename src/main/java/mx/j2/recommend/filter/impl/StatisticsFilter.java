package mx.j2.recommend.filter.impl;

import mx.j2.recommend.data_model.data_collection.BaseDataCollection;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.document.StatisticsDocument;
import mx.j2.recommend.util.OptionalUtil;

/**
 * @author zhongrenli
 * modified by xuejian.zhang, @2021.02.21
 */
public class StatisticsFilter extends BaseFilter {
    private static final int VIEW_TH_LOW = 2000;
    private static final int VIEW_TH_HIGH = 10000;
    private static final double DOWNLOAD_RATE_TH = 0.0003;
    private static final double LIKE_RATE_TH = 0.01;
    private static final double SHARE_RATE_TH = 0.000001;
    private static final int POOL_LEVEL_TH = 4;


    @Override
    public boolean isFilted(BaseDocument doc, BaseDataCollection dc) {
        StatisticsDocument statistics = doc.statisticsDocument;
        if (!OptionalUtil.ofNullable(statistics).isPresent()) {
            return false;
        }
        
        if (!statistics.isLoadSuccess()) {
            return false;
        }

        boolean isFilt = false;

        // 曝光不足, 则不过滤
        if (Double.compare(statistics.getViewAll30d(), VIEW_TH_LOW) < 0) {
            return isFilt;
        }

        // 对于1-4级池子数据, 曝光暂不稳定, 暂时先不过滤
        if (POOL_LEVEL_TH >= doc.getPoolPriority()) {
            return isFilt;
        }

        boolean noDownloadMeet = Double.compare(statistics.getDownloadRate30d(), DOWNLOAD_RATE_TH) < 0;
        boolean noLikeMeet = Double.compare(statistics.getLikeRate30d(), LIKE_RATE_TH) < 0;
        boolean noShareMeet = Double.compare(statistics.getShareRate30d(), SHARE_RATE_TH) < 0;
        isFilt = noDownloadMeet || noLikeMeet || noShareMeet;

        if (isFilt) {
            return true;
        }

        // 优先试验下大于10000view的指标过滤
        if (Double.compare(statistics.getViewAll30d(), VIEW_TH_HIGH) > 0) {
            boolean isFinishLessThan30 = Double.compare(statistics.getFinishedRate30d(), 0.3) < 0;
            boolean isFinishLessThan19 = Double.compare(statistics.getFinishedRate30d(), 0.19) < 0;
            boolean isShareLessThandd2 = Double.compare(statistics.getShareRate30d(), 0.0002) < 0;

            isFilt = isFinishLessThan19 || (isFinishLessThan30 && isShareLessThandd2);
        }

        return isFilt;
    }
}
