package mx.j2.recommend.manager;

import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.data_source.*;
import mx.j2.recommend.data_source.userprofile.tag.UserProfileStrategyTagDS;
import mx.j2.recommend.task.CassandraExecutor;
import mx.j2.recommend.task.StrategyPoolExecutor;
import mx.j2.recommend.task.TaskExecutor;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Created by zhangxuejian on 2018/1/11.
 */
@NotThreadSafe
public class DataSourceManager {
    public final static DataSourceManager INSTANCE = new DataSourceManager();
    private final ElasticCacheSource elasticCacheSource;
    private final ElasticSearchDataSource elasticSearchDataSource;
    private final VideoElasticSearchDataSource videoElasticSearchDataSource;
    private final StrategyElasticSearchDataSource strategyElasticSearchDataSource;
    private final GuavaBloomDataSource guavaBloomDataSource;
    private final RecommendFlowDataSource recommendFlowDataSource;
    private final LocalCacheDataSource localCacheDataSource;
    //private final RecallScoreWeightDataSource recallScoreWeightDataSource;
    private final HttpDataSource httpDataSource;
    private final ReBloomDataSource reBloomDataSource;
    private final DetailDataSource detailDataSource;
    private final UgcConfDataSource ugcConfDataSource;
    private final CassandraDataSource cassandraDataSource;
    private final PoolConfDataSource poolConfDataSource;
    private final AWSSnsDataSource aWSSnsDataSource;
    private final StrategyCassandraDataSource strategyCassandraDataSource;
    private final BigVDataSource bigVDataSource;
    private final InactiveUserHistoryBloomDataSource inactiveUserHistoryBloomDataSource;
    private final UserProfileDataSource userProfileDataSource;
    private final UserProfileTagDataSource userProfileTagDataSource;
    private final VideoElasticSearchVersion7DataSource videoElasticSearchVersion7DataSource;
    private final NearByDataSource nearByDataSource;
    private final UserHistoryBloomDataSource userHistoryBloomDataSource;
    //private final HistoryBloomCassandraDataSource historyBloomCassandraDataSource;
    private final TaskExecutor taskExecutor;
    private final TagTopVideoDataSource tagTopVideoDataSource;
    private final DateTimeDataSource dateTimeDataSource;
    private final AmazonSageDataSource amazonSageDataSource;
    private final AWSSqsDataSource awsSqsDataSource;
    private final NewHistoryBloomCaDataSource newHistoryBloomCaDataSource;
    private final CassandraExecutor cassandraExecutor;
    private final HotestHashTag15DataSource hotestHashTag15DataSource;
    private final PublisherCassandraDataSource publisherCassandraDataSource;
    private final PublisherBadgeCassandraDataSource publisherBadgeCassandraDataSource;
    private final StrategyPoolConfDataSource strategyPoolConfDataSource;
    private final FlowPoolDataSource flowPoolDataSource;
    private final UserStrategyTagDataSource userStrategyTagDataSource;
    private final StrategyPoolExecutor strategyPoolExecutor;
    private final TempLanguagePoolDataSource tempLanguagePoolDataSource;
    private final RealTimeStrategyDataSource realTimeStrategyDataSource;
    private final UserProfileRealtimeDataSource userProfileRealtimeDataSource;
    private final UserFollowBloomCaDataSource userFollowBloomCaDataSource;
    private final FilterFieldDataSource filterFieldDataSource;
    private final NewUserTagPoolDataSource newUserTagPoolDataSource;
    private final UserPrePubDataSource userPrePubDataSource;
    private final PublisherPageCassandraDataSource publisherPageCassandraDataSource;
    private final PublisherPageWhiteDataSource publisherPageWhiteDataSource;
    private final InterestTagDataSource interestTagDataSource;
    private final StatisticDataSource statisticDataSource;
    private final UgcLowLevelMixParamDataSource ugcLowLevelMixParamDataSource;
    private final CreatorDataSource creatorDataSource;
    private final ExposurePoolConfDataSource exposurePoolConfDataSource;
    private final ExposurePoolDataSource exposurePoolDataSource;
    private final ProfilePoolConfDataSource profilePoolConfDataSource;
    private final ProfilePoolDataSource profilePoolDataSource;
    private final UserProfileStrategyTagDS userProfileStrategyTagDS;

    public DataSourceManager() {
        elasticCacheSource = new ElasticCacheSource();
        elasticSearchDataSource = new ElasticSearchDataSource();
        videoElasticSearchDataSource = new VideoElasticSearchDataSource();
        strategyElasticSearchDataSource = new StrategyElasticSearchDataSource();
        recommendFlowDataSource = new RecommendFlowDataSource();
        localCacheDataSource = new LocalCacheDataSource();
        //recallScoreWeightDataSource = new RecallScoreWeightDataSource();
        httpDataSource = new HttpDataSource();
        reBloomDataSource = new ReBloomDataSource();
        guavaBloomDataSource = new GuavaBloomDataSource();
        detailDataSource = new DetailDataSource();
        ugcConfDataSource = new UgcConfDataSource();
        cassandraDataSource = new CassandraDataSource();
        poolConfDataSource = new PoolConfDataSource();
        aWSSnsDataSource = new AWSSnsDataSource();
        strategyCassandraDataSource = new StrategyCassandraDataSource();
        bigVDataSource = new BigVDataSource();
        inactiveUserHistoryBloomDataSource = new InactiveUserHistoryBloomDataSource();
        userProfileDataSource = new UserProfileDataSource();
        userProfileTagDataSource = new UserProfileTagDataSource();
        videoElasticSearchVersion7DataSource = new VideoElasticSearchVersion7DataSource();
        nearByDataSource = new NearByDataSource();
        userHistoryBloomDataSource = new UserHistoryBloomDataSource();
        //historyBloomCassandraDataSource = new HistoryBloomCassandraDataSource();
        taskExecutor = new TaskExecutor(Conf.getWorkThreadNum());
        tagTopVideoDataSource = new TagTopVideoDataSource();
        dateTimeDataSource = new DateTimeDataSource();
        amazonSageDataSource = new AmazonSageDataSource();
        dateTimeDataSource.scheduleUpdateTime();
        awsSqsDataSource = new AWSSqsDataSource();
        newHistoryBloomCaDataSource = new NewHistoryBloomCaDataSource();
        cassandraExecutor = new CassandraExecutor(Conf.getWorkThreadNum());
        hotestHashTag15DataSource = new HotestHashTag15DataSource();
        publisherCassandraDataSource = new PublisherCassandraDataSource();
        publisherBadgeCassandraDataSource = new PublisherBadgeCassandraDataSource();
        strategyPoolConfDataSource = new StrategyPoolConfDataSource();
        flowPoolDataSource = new FlowPoolDataSource();
        userStrategyTagDataSource = new UserStrategyTagDataSource();
        strategyPoolExecutor = new StrategyPoolExecutor();
        tempLanguagePoolDataSource = new TempLanguagePoolDataSource();
        realTimeStrategyDataSource = new RealTimeStrategyDataSource();
        userProfileRealtimeDataSource = new UserProfileRealtimeDataSource();
        userFollowBloomCaDataSource = new UserFollowBloomCaDataSource();
        filterFieldDataSource = new FilterFieldDataSource();
        newUserTagPoolDataSource = new NewUserTagPoolDataSource();
        publisherPageCassandraDataSource = new PublisherPageCassandraDataSource();
        userPrePubDataSource = new UserPrePubDataSource();
        publisherPageWhiteDataSource = new PublisherPageWhiteDataSource();
        interestTagDataSource = new InterestTagDataSource();
        statisticDataSource = new StatisticDataSource();
        ugcLowLevelMixParamDataSource = new UgcLowLevelMixParamDataSource();
        creatorDataSource = new CreatorDataSource();
        exposurePoolConfDataSource = new ExposurePoolConfDataSource();
        exposurePoolDataSource = new ExposurePoolDataSource();
        profilePoolConfDataSource = new ProfilePoolConfDataSource();
        profilePoolDataSource = new ProfilePoolDataSource();
        userProfileStrategyTagDS = new UserProfileStrategyTagDS();
    }

    ElasticCacheSource getElasticCacheSource() {
        return elasticCacheSource;
    }

    ElasticSearchDataSource getElasticSearchDataSource() {
        return elasticSearchDataSource;
    }

    VideoElasticSearchDataSource getVideoElasticSearchDataSource() {
        return videoElasticSearchDataSource;
    }

    RecommendFlowDataSource getRecommendFlowDataSource() {
        return recommendFlowDataSource;
    }

    LocalCacheDataSource getLocalCacheDataSource() {
        return localCacheDataSource;
    }

//    RecallScoreWeightDataSource getRecallScoreWeightDataSource() {
//        return recallScoreWeightDataSource;
//    }

    HttpDataSource getHttpDataSource() {
        return httpDataSource;
    }

    ReBloomDataSource getReBloomDataSource() {
        return reBloomDataSource;
    }

    GuavaBloomDataSource getGuavaBloomDataSource() {
        return guavaBloomDataSource;
    }

    DetailDataSource getDetailDataSource() {
        return detailDataSource;
    }

    UgcConfDataSource getUgcConfDataSource() {
        return ugcConfDataSource;
    }

    CassandraDataSource getCassandraDataSource() {
        return cassandraDataSource;
    }

    PoolConfDataSource getPoolConfDataSource() {
        return poolConfDataSource;
    }

    PublisherPageCassandraDataSource getPublisherPageCassandraDataSource() {
        return publisherPageCassandraDataSource;
    }

    /**
     * 所有对此类有循环依赖的，可以在此处（本类初始化已完成）初始化
     */
    public void init() {
        // 要依赖 redis 数据源，所以放到这里
        FallbackDataSource.INSTANCE.init();

        // 要依赖 redis 数据源，所以放到这里
        if (poolConfDataSource != null) {
            poolConfDataSource.init();
        }

        // 依赖其他数据源(CA)或者说本 Manager，所以放到这里初始化
        if (recommendFlowDataSource != null) {
            recommendFlowDataSource.init();
        }
    }

    AWSSnsDataSource getaWSSnsDataSource() {
        return aWSSnsDataSource;
    }

    StrategyElasticSearchDataSource getStrategyElasticSearchDataSource() {
        return strategyElasticSearchDataSource;
    }

    StrategyCassandraDataSource getStrategyCassandraDataSource() {
        return strategyCassandraDataSource;
    }

    BigVDataSource getBigVDataSource() {
        return bigVDataSource;
    }

    InactiveUserHistoryBloomDataSource getInactiveUserHistoryBloomDataSource() {
        return inactiveUserHistoryBloomDataSource;
    }

    UserProfileDataSource getUserProfileDataSource() {
        return userProfileDataSource;
    }

    UserProfileTagDataSource getUserProfileTagDataSource() {
        return userProfileTagDataSource;
    }

    VideoElasticSearchVersion7DataSource getVideoElasticSearchVersion7DataSource() {
        return videoElasticSearchVersion7DataSource;
    }

    NearByDataSource getNearByDataSource() {
        return nearByDataSource;
    }

    UserHistoryBloomDataSource getUserHistoryBloomDataSource() {
        return userHistoryBloomDataSource;
    }

//    public HistoryBloomCassandraDataSource getHistoryBloomCassandraDataSource() {
//        return historyBloomCassandraDataSource;
//    }

    public TaskExecutor getTaskExecutor() {
        return taskExecutor;
    }

    TagTopVideoDataSource getTagTopVideoDataSource() {
        return tagTopVideoDataSource;
    }

    DateTimeDataSource getDateTimeDataSource() {
        return dateTimeDataSource;
    }

    AmazonSageDataSource getAmazonSageDataSource() {
        return amazonSageDataSource;
    }

    AWSSqsDataSource getAwsSqsDataSource() {
        return awsSqsDataSource;
    }

    NewHistoryBloomCaDataSource getNewHistoryBloomCaDataSource() {
        return newHistoryBloomCaDataSource;
    }

    public CassandraExecutor getCassandraExecutor() {
        return cassandraExecutor;
    }

    HotestHashTag15DataSource getHotestHashTag15DataSource() {
        return hotestHashTag15DataSource;
    }

    PublisherCassandraDataSource getPublisherCassandraDataSource() {
        return publisherCassandraDataSource;
    }

    PublisherBadgeCassandraDataSource getPublisherBadgeCassandraDataSource() {
        return publisherBadgeCassandraDataSource;
    }

    StrategyPoolConfDataSource getStrategyPoolConfDataSource() {
        return strategyPoolConfDataSource;
    }

    FlowPoolDataSource getFlowPoolDataSource() {
        return flowPoolDataSource;
    }

    public UserStrategyTagDataSource getUserStrategyTagDataSource() {
        return userStrategyTagDataSource;
    }

    public StrategyPoolExecutor getStrategyPoolExecutor() {
        return strategyPoolExecutor;
    }

    public TempLanguagePoolDataSource getTempLanguagePoolDataSource() {
        return tempLanguagePoolDataSource;
    }

    public RealTimeStrategyDataSource getRealTimeStrategyDataSource() {
        return realTimeStrategyDataSource;
    }

    public UserProfileRealtimeDataSource getUserProfileRealtimeDataSource() {
        return userProfileRealtimeDataSource;
    }

    public UserFollowBloomCaDataSource getUserFollowBloomCaDataSource() {
        return userFollowBloomCaDataSource;
    }

    public FilterFieldDataSource getFilterFieldDataSource() {
        return filterFieldDataSource;
    }

    public NewUserTagPoolDataSource getNewUserTagPoolDataSource() {
        return newUserTagPoolDataSource;
    }

    public UserPrePubDataSource getUserPrePubDataSource() {
        return userPrePubDataSource;
    }

    public PublisherPageWhiteDataSource getPublisherPageWhiteDataSource() {
        return publisherPageWhiteDataSource;
    }

    public InterestTagDataSource getInterestTagDataSource() {
        return interestTagDataSource;
    }

    public StatisticDataSource getStatisticDataSource() {
        return statisticDataSource;
    }

    public CreatorDataSource getCreatorDataSource() {
        return creatorDataSource;
    }

    ExposurePoolConfDataSource getExposurePoolConfDataSource() {
        return exposurePoolConfDataSource;
    }

    ExposurePoolDataSource getExposurePoolDataSource() {
        return exposurePoolDataSource;
    }

    ProfilePoolConfDataSource getProfilePoolConfDataSource() {
        return profilePoolConfDataSource;
    }

    ProfilePoolDataSource getProfilePoolDataSource() {
        return profilePoolDataSource;
    }

    UserProfileStrategyTagDS getUserProfileStrategyTagDS() {
        return userProfileStrategyTagDS;
    }
}