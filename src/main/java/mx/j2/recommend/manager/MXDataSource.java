package mx.j2.recommend.manager;

import mx.j2.recommend.data_source.*;
import mx.j2.recommend.data_source.userprofile.tag.UserProfileStrategyTagDS;

/**
 * 数据源接口，从此处便捷的拿到数据源
 */
public abstract class MXDataSource {

    /**
     * 详情数据源，即详情池
     */
    public static DetailDataSource details() {
        return DataSourceManager.INSTANCE.getDetailDataSource();
    }

    /**
     * 池子配置数据源
     */
    public static PoolConfDataSource pools() {
        return DataSourceManager.INSTANCE.getPoolConfDataSource();
    }

    /**
     * 通用 CA 数据库
     */
    public static CassandraDataSource cassandra() {
        return DataSourceManager.INSTANCE.getCassandraDataSource();
    }

    public static UserProfileDataSource profile() {
        return DataSourceManager.INSTANCE.getUserProfileDataSource();
    }

    public static UserProfileRealtimeDataSource profileRealtime() {
        return DataSourceManager.INSTANCE.getUserProfileRealtimeDataSource();
    }

    public static ElasticCacheSource redis() {
        return DataSourceManager.INSTANCE.getElasticCacheSource();
    }

    public static BigVDataSource verify() {
        return DataSourceManager.INSTANCE.getBigVDataSource();
    }

    public static PublisherPageWhiteDataSource white() {
        return DataSourceManager.INSTANCE.getPublisherPageWhiteDataSource();
    }

    public static LocalCacheDataSource cache() {
        return DataSourceManager.INSTANCE.getLocalCacheDataSource();
    }

    public static RecommendFlowDataSource flow() {
        return DataSourceManager.INSTANCE.getRecommendFlowDataSource();
    }

    public static UserProfileTagDataSource profileTag() {
        return DataSourceManager.INSTANCE.getUserProfileTagDataSource();
    }

    public static UserStrategyTagDataSource profileTagV2() {
        return DataSourceManager.INSTANCE.getUserStrategyTagDataSource();
    }

    public static UserProfileStrategyTagDS profileStgTag() {
        return DataSourceManager.INSTANCE.getUserProfileStrategyTagDS();
    }

    public static StrategyPoolConfDataSource strategyPool() {
        return DataSourceManager.INSTANCE.getStrategyPoolConfDataSource();
    }

    public static VideoElasticSearchDataSource videoES() {
        return DataSourceManager.INSTANCE.getVideoElasticSearchDataSource();
    }

    public static HttpDataSource http() {
        return DataSourceManager.INSTANCE.getHttpDataSource();
    }

    public static StrategyCassandraDataSource strategyCA() {
        return DataSourceManager.INSTANCE.getStrategyCassandraDataSource();
    }

    public static ElasticSearchDataSource ES() {
        return DataSourceManager.INSTANCE.getElasticSearchDataSource();
    }

    public static GuavaBloomDataSource guavaBloom() {
        return DataSourceManager.INSTANCE.getGuavaBloomDataSource();
    }

    public static TagTopVideoDataSource tagTop() {
        return DataSourceManager.INSTANCE.getTagTopVideoDataSource();
    }

    public static UserHistoryBloomDataSource historyBloom() {
        return DataSourceManager.INSTANCE.getUserHistoryBloomDataSource();
    }

    public static NewHistoryBloomCaDataSource historyBloomV2() {
        return DataSourceManager.INSTANCE.getNewHistoryBloomCaDataSource();
    }

    public static InactiveUserHistoryBloomDataSource inactiveHistoryBloom() {
        return DataSourceManager.INSTANCE.getInactiveUserHistoryBloomDataSource();
    }

    public static StrategyElasticSearchDataSource strategyES() {
        return DataSourceManager.INSTANCE.getStrategyElasticSearchDataSource();
    }

    public static HotestHashTag15DataSource hottestHashTag() {
        return DataSourceManager.INSTANCE.getHotestHashTag15DataSource();
    }

    public static ReBloomDataSource rebloom() {
        return DataSourceManager.INSTANCE.getReBloomDataSource();
    }

    public static PublisherCassandraDataSource publisherCA() {
        return DataSourceManager.INSTANCE.getPublisherCassandraDataSource();
    }

    public static PublisherBadgeCassandraDataSource badgeCA() {
        return DataSourceManager.INSTANCE.getPublisherBadgeCassandraDataSource();
    }

    public static VideoElasticSearchVersion7DataSource videoESV7() {
        return DataSourceManager.INSTANCE.getVideoElasticSearchVersion7DataSource();
    }

    public static AWSSqsDataSource SQS() {
        return DataSourceManager.INSTANCE.getAwsSqsDataSource();
    }

    public static AmazonSageDataSource sage() {
        return DataSourceManager.INSTANCE.getAmazonSageDataSource();
    }

//    public static RecallScoreWeightDataSource score() {
//        return DataSourceManager.INSTANCE.getRecallScoreWeightDataSource();
//    }

    public static DateTimeDataSource date() {
        return DataSourceManager.INSTANCE.getDateTimeDataSource();
    }

    public static AWSSnsDataSource SNS() {
        return DataSourceManager.INSTANCE.getaWSSnsDataSource();
    }

    public static FlowPoolDataSource flowPool() {
        return DataSourceManager.INSTANCE.getFlowPoolDataSource();
    }

    public static TempLanguagePoolDataSource tempLanguagePool() {
        return DataSourceManager.INSTANCE.getTempLanguagePoolDataSource();
    }

    public static UserFollowBloomCaDataSource userFollowBloom() {
        return DataSourceManager.INSTANCE.getUserFollowBloomCaDataSource();
    }

    public static PublisherPageCassandraDataSource publisherPage() {
        return DataSourceManager.INSTANCE.getPublisherPageCassandraDataSource();
    }

    public static InterestTagDataSource interestTag() {
        return DataSourceManager.INSTANCE.getInterestTagDataSource();
    }

    public static UgcConfDataSource tempConf() {
        return DataSourceManager.INSTANCE.getUgcConfDataSource();
    }

    public static CreatorDataSource creator() {
        return DataSourceManager.INSTANCE.getCreatorDataSource();
    }

    public static ExposurePoolConfDataSource exposurePoolConf() {
        return DataSourceManager.INSTANCE.getExposurePoolConfDataSource();
    }

    public static ExposurePoolDataSource exposurePool() {
        return DataSourceManager.INSTANCE.getExposurePoolDataSource();
    }

    public static ProfilePoolDataSource profilePool() {
        return DataSourceManager.INSTANCE.getProfilePoolDataSource();
    }

    public static ProfilePoolConfDataSource profilePoolConf() {
        return DataSourceManager.INSTANCE.getProfilePoolConfDataSource();
    }
}
