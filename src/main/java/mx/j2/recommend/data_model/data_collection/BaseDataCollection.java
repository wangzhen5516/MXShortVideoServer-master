package mx.j2.recommend.data_model.data_collection;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.hash.BloomFilter;
import mx.j2.recommend.data_model.DataCollection;
import mx.j2.recommend.data_model.Document;
import mx.j2.recommend.data_model.UserProfile;
import mx.j2.recommend.data_model.data_collection.info.*;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.document.LiveDocument;
import mx.j2.recommend.data_model.flow.RecommendFlow;
import mx.j2.recommend.data_source.LocalCacheDataSource;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.pool_conf.PoolConf;
import mx.j2.recommend.pool_conf.StrategyPoolConf;
import mx.j2.recommend.thrift.PublisherInfo;
import mx.j2.recommend.thrift.Request;
import mx.j2.recommend.thrift.Result;
import mx.j2.recommend.util.BaseMagicValueEnum;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;
import mx.j2.recommend.util.annotation.CollectionList;
import mx.j2.recommend.util.bean.BloomFilterCollections;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static mx.j2.recommend.util.DefineTool.EsType;
import static mx.j2.recommend.util.DefineTool.FlowInterface.*;

/**
 * 数据集合
 *
 * @author zhongren.li
 */
public abstract class BaseDataCollection implements DataCollection {
    private static final String INTERNAL_SORTED_VIDEO_LIST_LOCAL_CACHE_KEY = "intenal_sorted_publisher_%s";

    /**
     * 请使用新结构归档
     */
    public MXClientInfo client;
    public MXDataInfo data;
    public MXDebugInfo debug;
    public MXUtilInfo util;

    public BloomFilter<String> guavaBloomFilter;
    public BloomFilter<String> bigBloomFilter;
    public int bigBloomFilterCount;
    public boolean isBloomNew;
    public boolean isHaveBloom;

    public BloomFilterCollections bloomFilterCollections;

    public long userHistorySize;

    //real_time接口使用
    public Map<String, String> publisherIdSourceMap;

    /**
     * 请求.
     */
    public Request req;


    /**
     * 用户个性化数据信息
     */
    public UserProfile longTermUserProfile;

    /**
     * 记录服务一次开始结束时间.
     */
    public long startTime;
    public long endTime;

    /**
     * 记录某个模块开始结束时间。
     */
    public long moduleStartTime;
    public long moduleEndTime;

    /**
     * 归并到一起的列表，同步结果直接存到这里面。
     */
    @CollectionList(type = CollectionList.Type.DEFAULT)
    public List<BaseDocument> mergedList;

    /**
     * 根据userProfilePublisherList召回的列表
     */
    @CollectionList(type = CollectionList.Type.DEFAULT, suffix = "follow_suggestions")
    public List<BaseDocument> preferredPublisherVideoList;

    /**
     * follow页保底召回
     */
    @CollectionList(type = CollectionList.Type.DEFAULT, suffix = "follow_guarantee")
    public List<BaseDocument> followGuaranteeList;

    @CollectionList(type = CollectionList.Type.DEFAULT, suffix = "_userPrePub")
    public List<BaseDocument> userPrePubDocList;

    @CollectionList(type = CollectionList.Type.DEFAULT, suffix = "_userPrePubNew")
    public List<BaseDocument> userPrePubDocLongTermList;

    /**
     * 需要掺入的 tophot state 数据
     */
    @CollectionList(type = CollectionList.Type.POOL, suffix = "_tophot_state")
    public List<BaseDocument> topHotStateList;

    @CollectionList(type = CollectionList.Type.POOL, suffix = "_new_language")
    public List<BaseDocument> newLanguageDocumentList;

    @CollectionList(type = CollectionList.Type.POOL, suffix = "_country")
    public List<BaseDocument> countryDocumentList;

    /**
     * 异步召回结果列表。
     */
    public List<BaseDocument> asynRecallList;

    /**
     * 此次召回的外环半径
     */
    public int radius;

    /**
     * 小语言召回文件列表
     */
    @CollectionList(type = CollectionList.Type.DEFAULT, suffix = "_minorLanguage")
    public List<BaseDocument> minorLanguageRecallList;

    /**
     * 大脑袋召回结果
     */
    @CollectionList(type = CollectionList.Type.DEFAULT, suffix = "_bigHead")
    public List<BaseDocument> bigHeadList;

    /**
     * 推荐流。
     */
    public RecommendFlow recommendFlow;

    /**
     * 记录同步召回中的召回器对应的召回数量。
     */
    public Map<String, Integer> syncSearchResultSizeMap;

    /**
     * 推荐历史列表。
     */
    public Set<String> historyIdList;

    /**
     * 当次推荐列表。
     */
    public Set<String> resIdList;
    public Set<String> topHotIdList;
    public Set<String> notTopHotIdList;

    /**
     * 空请求。
     */
    public final static Request EMPTY_REQUEST = new Request();

    /**
     * 记录结果来自哪里。
     */
    public Map<String, String> resultFromMap;

    /**
     * 记录结果来自哪里。
     */
    public Map<String, String> logMap;

    /**
     * 各组件想要记录的额外信息，不固定，注意及时删除无用 log
     * 组件名称（如 recall） -> 记录内容
     */
    public Map<String, String> logComponentExtra;

    /**
     * 记录搜索引擎召回时用到了哪些召回器。
     */
    public Set<String> searchEngineRecallerSet;

    /**
     * 异步ES查询语句列表 - 各种池子。
     */
    public List<ESRequest> searchRequestList;

    /**
     * 异步ES查询语句列表。
     */
    public List<ESRequest> videoSearchRequestList;

    /**
     * 异步ES查询语句列表。
     */
    public List<ESRequest> videoNewSearchRequestList;

    /**
     * 异步ES查询语句列表 - 适用于策略 ES。
     */
    public List<ESRequest> strategySearchRequestList;

    /**
     * 记录哪些召回器需要本地缓存。
     */
    public Map<String, String> localCacheRecallKeyMap;

    /**
     * 记录特殊召回器需要本地缓存。
     */
    public Map<String, String> localCacheMatchScoreRecallKeyMap;

    /**
     * 记录特殊召回器记录缓存时间。
     */
    public Map<String, Integer> localCacheMatchScoreRecallCacheTimeMap;

    /**
     * 是否开启debug 模式
     */
    public boolean isDebugModeOpen;

    /**
     * 存储来自于 Redis 的置顶 list
     */
    @CollectionList(type = CollectionList.Type.TOPHOT, suffix = "_manual")
    public List<BaseDocument> highPriorityManualList;

    /**
     * 存储来自于 Redis 的置顶 list
     */
    public List<Result> highPriorityManualResultList;

    /**
     * 存储来自于 Redis 的置顶 id set
     */
    public Set<String> highPriorityManualIdSet;

    /**
     * 存储来自于 Redis 的置顶 list
     */
    @CollectionList(type = CollectionList.Type.TOPHOT, suffix = "_HighPriorityForNewUser")
    public List<BaseDocument> highPriorityVideoForNewUserList;

    /**
     * 存储来自于 Redis 的置顶 list
     */
    @CollectionList(type = CollectionList.Type.TOPHOT, suffix = "_manualList")
    public List<BaseDocument> manualList;

    /**
     * 存储来自于 Redis 的置顶 list
     */
    public List<Result> highPriorityVideoForNewUserResultList;

    public Map<String, String> resulIdLanMap;

    /**
     * 将每个分数的视频进行分堆，供后续打散使用
     */
    public TreeMap<Float, List<Result>> scoreToResultListMap;

    /**
     * video id 对应的 tag list
     */
    public Map<String, List<String>> videoIdToTagListMap;

    /**
     * 用来存 个性化 数据
     */
    @CollectionList(type = CollectionList.Type.TOPHOT, suffix = "_user_calculate")
    public List<BaseDocument> userProfileOfflineRecommendList;

    /**
     * 用来存 个性化 数据
     */
    @CollectionList(type = CollectionList.Type.TOPHOT, suffix = "_user_profile_trending_calculate")
    public List<BaseDocument> userProfileTrendingOfflineRecommendList;

    /**
     * 用来存 个性化 数据
     * 基于用户喜爱标签的召回数据
     * 标签 -> 视频列表
     */
    public Map<UserProfile.Tag, List<BaseDocument>> userProfileTagMap;

    public Map<UserProfile.Tag, List<BaseDocument>> userProfileTagMapNew;

    /**
     * 用来存每个用户 follow 的其他人的数据
     */
    @CollectionList(type = CollectionList.Type.TOPHOT, suffix = "_offline_user_follow")
    public List<BaseDocument> userFollowPublishList;

    /**
     * 用来存每个用户 follow 的其他人的数据
     */
    @CollectionList(type = CollectionList.Type.TOPHOT, suffix = "_offline_user_follow_V")
    public List<BaseDocument> userFollowVPublishList;

    /**
     * 用户 follow 的publisher们更新的新视频的数量
     */
    public int userFollowPublishListSize;

    /**
     * 用来存准实时数据
     */
    @CollectionList(type = CollectionList.Type.TOPHOT, suffix = "_offline_similar")
    public List<BaseDocument> similarRealList;

    @CollectionList(type = CollectionList.Type.TOPHOT, suffix = "_offline_related")
    public List<BaseDocument> relatedList;


    public List<String> userDislikePubIds;

    public List<String> longTermuserDislikePubIds;

    @CollectionList(type = CollectionList.Type.POOL, suffix = "_interest_tag")
    public List<BaseDocument> interestTagDocList;

    public List<String> userUploadVideoIDList;

    /**
     * 大V发布的视频id
     */
    public List<String> bigVPublishVideoIdList;

    /**
     * 从redis缓存中召回的结果列表, 只需要过上下线状态过滤, 免去其他逻辑
     */
    public List<Result> cachedResultList;

    /**
     * 用户最后一次访问的时间戳
     */
    public long lastHistoryIdTimeStamp;

    /**
     * 一级保底数据
     */
    public List<BaseDocument> guaranteeFirstLevelDocList;

    /**
     * 二级保底数据 - 来自保底池子
     */
    @CollectionList(type = CollectionList.Type.POOL, suffix = "_guarantee_pool")
    public List<BaseDocument> guaranteeSecondLevelDocList;

    /**
     * 给新用户保存的保底的3级tag池的数据
     */
    @CollectionList(type = CollectionList.Type.POOL, suffix = "_guarantee_pool")
    public List<BaseDocument> tagPoolLv3List;

    /**
     * 保底数据，用于打包
     */
    public List<BaseDocument> fallbackList;

    /**
     * 保存 mix 之后的 id
     */
    public Set<String> mixDocumentIdList;

    /**
     * 记录一次请求中, 是否已经处理过保底数据列表
     */
    public boolean firstRoundDone;

    public Map<String, Map<String, String>> nextTokenMap;
    public int totalNumber;

    /**
     * 记录一次请求中, 是否已经处理过保底数据列表
     */
    @CollectionList(type = CollectionList.Type.DEFAULT, suffix = "_top_KOL_Video_In_30_Days")
    public List<BaseDocument> topKOLVideoIn30DaysList;

    /**
     * pool 数据
     */
    public Map<String, List<BaseDocument>> poolToDocumentListMap;

    /**
     * pool conf.现在在PoolRecall中初始化
     *
     * @see mx.j2.recommend.recall.impl.PoolRecall#recall
     */
    public Map<String, PoolConf> poolConfMap;

    /**
     * strategy tag pool 数据
     */
    public Map<String, List<BaseDocument>> strategyPoolToDocumentListMap;

    /**
     * 实时strategy tag pool 数据
     */
    public Map<String, List<BaseDocument>> realTimeStrategyPoolToDocumentListMap;

    /**
     * strategy tag pool 数据
     */
    public Map<String, StrategyPoolConf> strategyPoolConfMap;

    /**
     * strategy tag pool 数据
     */
    public Map<String, StrategyPoolConf> realTimeStrategyPoolConfMap;

    /**
     * 实时点击 Id List
     */
    public List<String> realTimeClickIdList;

    /**
     * 实时点击
     */
    @CollectionList(type = CollectionList.Type.DEFAULT, suffix = "_real_time_like")
    public List<BaseDocument> realTimeClickDocList;

    /**
     * 将realTimeClickDocList筛选，每个publisher_id最多出现两次
     */
    public List<BaseDocument> realTimeClickDocListPub;

    /**
     * HashTag中需要置顶的视频
     */
    @CollectionList(type = CollectionList.Type.DEFAULT, suffix = "hash_tag_top_video")
    public List<BaseDocument> hashTagTopVideoList;

    /**
     * effect中需要置顶的视频
     */
    @CollectionList(type = CollectionList.Type.DEFAULT, suffix = "effect_top_video")
    public List<BaseDocument> effectTopVideoList;

    /**
     * 实时召回
     */
    @CollectionList(type = CollectionList.Type.DEFAULT, suffix = "realTime")
    public List<BaseDocument> realTimeRecallList;

    /**
     * 根据最近观看视频匹配的数据
     */
    @CollectionList(type = CollectionList.Type.DEFAULT, suffix = "_latest_view_related")
    public List<BaseDocument> latestViewRelatedDocList;

    /**
     * 用来给召回器过滤计数
     */
    public Multiset<String> recallFilterCount;

    /**
     * 本次请求实际召回的数量
     * 翻页需要
     */
    int recallSize;

    /**
     * 本次请求最后一个召回结果
     * 翻页需要
     */
    Result lastRecallResult;

    /**
     * 不喜欢的publisher列表
     */
    public List<String> disLikePublisherIdList;

    /**
     * 不喜欢的publisher列表
     */
    public List<String> dislikeTagIdList;

    /**
     * 是否超时
     */
    public boolean isTimeout;

    public boolean isError;

    /**
     * RealTimePublisherRecallAll所需的ratio
     */
    public int ratioForRealTimeMixer;

    /**
     * RealTimePublisherRecallNEW所需的ratio
     */
    public int ratioForRealTimeMixerNew;

    /**
     * 是否从三个videos的缓存召回
     */
    public boolean isFromPublisherCache;
    public boolean isFromTagCache;
    public boolean isFromSameAudioCache;

    /**
     * 拉黑用户列表
     */
    public List<String> blockList;

    /**
     * 账户迁移状态
     */
    public Integer status;

    /**
     * 单独存放预告片的字段
     */
    @CollectionList(type = CollectionList.Type.POOL, suffix = "_trailer_video")
    public List<BaseDocument> trailerVideo;

    @CollectionList(type = CollectionList.Type.TOPHOT, suffix = "_funny_list")
    public List<BaseDocument> funnyList;

    public List<BaseDocument> predictDocumentList;

    public String featureString;

    public Multiset<String> userProfileCount;

    public Map<String, String> idToRecallNameMap;

    public String redDotInfo;

    public String tagTableName;

    public Map<String, List<BaseDocument>> strategyTagDocumentListMap;

    public Set<String> tagSet;

    public Set<UserProfile.Tag> userLongTagSet;

    public Set<UserProfile.Tag> userLongCategorySet;

    /**
     * 成人偏好视频
     */
    @CollectionList(type = CollectionList.Type.DEFAULT, suffix = "_adult_preference")
    public List<BaseDocument> adultPreferenceDocumentList;

    /**
     * 成人偏好 uuid
     */
    public boolean isAdultUuid;

    /**
     * follow card推荐的KOL Publisher INFO
     */
    public List<PublisherInfo> followCardKOLIds;

    @CollectionList(type = CollectionList.Type.DEFAULT, suffix = "_ipl_video")
    public List<BaseDocument> IPLDocumentList;

    /**
     * 该list包含普通和白名单的数据
     */
    @CollectionList(type = CollectionList.Type.DEFAULT, suffix = "_all_live")
    public List<LiveDocument> liveDocumentList;

    /**
     * 需要置顶的直播间数据
     */
    @CollectionList(type = CollectionList.Type.DEFAULT, suffix = "_live_lock")
    public List<LiveDocument> liveLockDocumentList;

    public List<PublisherInfo> cmsPubCardPubIds;

    public int uploadDaysRecent;

    /**
     * 构造函数
     */
    public BaseDataCollection() {
        init();
    }

    /**
     * 初始化函数
     */
    private void init() {
        data = new MXDataInfo();
        debug = new MXDebugInfo();
        util = new MXUtilInfo();
        client = new MXClientInfo();

        syncSearchResultSizeMap = new HashMap<>();
        resultFromMap = new HashMap<>();
        logMap = new HashMap<>(64);
        logComponentExtra = new HashMap<>();
        mergedList = new ArrayList<>();
        searchRequestList = new ArrayList<>();
        historyIdList = new HashSet<>();
        asynRecallList = new ArrayList<>();
        resIdList = new HashSet<>();
        topHotIdList = new HashSet<>();
        searchEngineRecallerSet = new HashSet<>();
        startTime = BaseMagicValueEnum.LONG_INITIAL_VALUE;
        endTime = BaseMagicValueEnum.LONG_INITIAL_VALUE;
        moduleStartTime = BaseMagicValueEnum.LONG_INITIAL_VALUE;
        moduleEndTime = BaseMagicValueEnum.LONG_INITIAL_VALUE;
        recommendFlow = new RecommendFlow();
        req = null;
        localCacheRecallKeyMap = new HashMap<>();
        localCacheMatchScoreRecallKeyMap = new HashMap<>();
        localCacheMatchScoreRecallCacheTimeMap = new HashMap<>();
        isDebugModeOpen = false;
        resulIdLanMap = new HashMap<>();
        highPriorityManualList = new ArrayList<>();
        highPriorityVideoForNewUserList = new ArrayList<>();
        userUploadVideoIDList = new ArrayList<>();
        scoreToResultListMap = new TreeMap<>();
        videoIdToTagListMap = new HashMap<>();
        highPriorityManualIdSet = new HashSet<>();
        userProfileOfflineRecommendList = new ArrayList<>();
        minorLanguageRecallList = new ArrayList<>();
        userFollowPublishList = new ArrayList<>();
        userFollowVPublishList = new ArrayList<>();
        cachedResultList = new ArrayList<>();
        lastHistoryIdTimeStamp = -1;
        tagPoolLv3List = new ArrayList<>();
        fallbackList = new ArrayList<>();
        mixDocumentIdList = new HashSet<>();
        guaranteeFirstLevelDocList = new ArrayList<>();
        highPriorityManualResultList = new ArrayList<>();
        highPriorityVideoForNewUserResultList = new ArrayList<>();
        isBloomNew = false;
        isHaveBloom = false;
        guavaBloomFilter = null;
        bigBloomFilter = null;
        bigBloomFilterCount = 0;
        poolToDocumentListMap = new HashMap<>();
        poolConfMap = new HashMap<>();
        strategyPoolToDocumentListMap = new ConcurrentHashMap<>();
        realTimeStrategyPoolToDocumentListMap = new HashMap<>();
        firstRoundDone = false;
        realTimeClickDocList = new ArrayList<>();
        realTimeClickIdList = new ArrayList<>();
        bigVPublishVideoIdList = new ArrayList<>();
        followGuaranteeList = new ArrayList<>();
        hashTagTopVideoList = new ArrayList<>();
        effectTopVideoList = new ArrayList<>();
        preferredPublisherVideoList = new ArrayList<>();
        recallFilterCount = HashMultiset.create();
        bloomFilterCollections = new BloomFilterCollections();
        videoSearchRequestList = new ArrayList<>();
        strategySearchRequestList = new ArrayList<>();
        videoNewSearchRequestList = new ArrayList<>();
        userProfileTrendingOfflineRecommendList = new ArrayList<>();
        userPrePubDocList = new ArrayList<>();
        userHistorySize = 0;
        disLikePublisherIdList = new ArrayList<>();
        dislikeTagIdList = new ArrayList<>();
        userProfileTagMap = new HashMap<>();
        guaranteeSecondLevelDocList = new ArrayList<>();
        isTimeout = false;
        isError = false;
        ratioForRealTimeMixer = 0;
        userFollowPublishListSize = 0;
        realTimeRecallList = new ArrayList<>();
        userPrePubDocLongTermList = new ArrayList<>();
        isFromPublisherCache = false;
        isFromSameAudioCache = false;
        isFromTagCache = false;
        blockList = new ArrayList<>();
        status = Integer.MAX_VALUE;
        trailerVideo = new ArrayList<>();
        predictDocumentList = new ArrayList<>();
        latestViewRelatedDocList = new ArrayList<>();
        topHotStateList = new ArrayList<>();
        featureString = BaseMagicValueEnum.STRING_INITIAL_VALUE;
        userProfileCount = HashMultiset.create();
        notTopHotIdList = new HashSet<>();
        idToRecallNameMap = new HashMap<>();
        redDotInfo = BaseMagicValueEnum.STRING_INITIAL_VALUE;
        topKOLVideoIn30DaysList = new ArrayList<>();
        bigHeadList = new ArrayList<>();
        similarRealList = new ArrayList<>();
        funnyList = new ArrayList<>();
        newLanguageDocumentList = new ArrayList<>();
        countryDocumentList = new ArrayList<>();
        strategyPoolConfMap = new LinkedHashMap<>();
        realTimeStrategyPoolConfMap = new HashMap<>();
        tagTableName = BaseMagicValueEnum.STRING_INITIAL_VALUE;
        userProfileTagMapNew = new HashMap<>();
        strategyTagDocumentListMap = new HashMap<>();
        tagSet = new HashSet<>();
        adultPreferenceDocumentList = new ArrayList<>();
        isAdultUuid = false;
        userLongTagSet = new HashSet<>();
        manualList = new ArrayList<>();
        liveDocumentList = new ArrayList<>();
        realTimeClickDocListPub = new ArrayList<>();
        interestTagDocList = new ArrayList<>();
        followCardKOLIds = new ArrayList<>();
        IPLDocumentList = new ArrayList<>();
        liveLockDocumentList = new ArrayList<>();
        publisherIdSourceMap = new HashMap<>();
        relatedList = new ArrayList<>();
        cmsPubCardPubIds = new ArrayList<>();
        totalNumber = BaseMagicValueEnum.INT_INITIAL_VALUE;
        nextTokenMap = new LinkedHashMap<>();
        uploadDaysRecent = BaseMagicValueEnum.INT_INITIAL_VALUE;
        longTermUserProfile = new UserProfile();
        userLongCategorySet = new HashSet<>();
    }

    /**
     * 由于采用了对象池，所以这里用完以后要清理
     */
    public void baseClean() {
        data.clean();
        util.clean();
        debug.clean();
        client.clean();

        guavaBloomFilter = null;
        bigBloomFilter = null;
        mergedList = null;
        mergedList = new ArrayList<>();
        req = null;
        recommendFlow = null;
        startTime = BaseMagicValueEnum.LONG_INITIAL_VALUE;
        endTime = BaseMagicValueEnum.LONG_INITIAL_VALUE;
        moduleStartTime = BaseMagicValueEnum.LONG_INITIAL_VALUE;
        moduleEndTime = BaseMagicValueEnum.LONG_INITIAL_VALUE;
        syncSearchResultSizeMap.clear();
        searchRequestList.clear();
        historyIdList.clear();
        resIdList.clear();
        topHotIdList.clear();
        resultFromMap.clear();
        logMap.clear();
        logComponentExtra.clear();
        asynRecallList.clear();
        searchEngineRecallerSet.clear();
        localCacheRecallKeyMap.clear();
        localCacheMatchScoreRecallKeyMap.clear();
        localCacheMatchScoreRecallCacheTimeMap.clear();
        isDebugModeOpen = false;
        resulIdLanMap.clear();
        highPriorityManualList.clear();
        highPriorityVideoForNewUserList.clear();
        userUploadVideoIDList.clear();
        scoreToResultListMap.clear();
        videoIdToTagListMap.clear();
        highPriorityManualIdSet.clear();
        minorLanguageRecallList.clear();
        userProfileOfflineRecommendList = new ArrayList<>();
        userFollowPublishListSize = 0;
        userFollowPublishList.clear();
        userFollowVPublishList.clear();
        cachedResultList.clear();
        lastHistoryIdTimeStamp = -1;
        tagPoolLv3List.clear();
        fallbackList.clear();
        mixDocumentIdList.clear();
        guaranteeFirstLevelDocList.clear();
        highPriorityManualResultList.clear();
        highPriorityVideoForNewUserResultList.clear();
        isBloomNew = false;
        isHaveBloom = false;
        bigBloomFilterCount = 0;
        poolToDocumentListMap.clear();
        poolConfMap.clear();
        strategyPoolToDocumentListMap.clear();
        realTimeStrategyPoolToDocumentListMap.clear();
        firstRoundDone = false;
        realTimeClickDocList.clear();
        realTimeClickIdList.clear();
        bigVPublishVideoIdList.clear();
        followGuaranteeList.clear();
        hashTagTopVideoList.clear();
        effectTopVideoList.clear();
        preferredPublisherVideoList.clear();
        recallFilterCount.clear();
        videoSearchRequestList.clear();
        strategySearchRequestList.clear();
        videoNewSearchRequestList.clear();
        userProfileTrendingOfflineRecommendList.clear();
        bloomFilterCollections.clear();
        userPrePubDocList.clear();
        userHistorySize = 0;
        disLikePublisherIdList.clear();
        dislikeTagIdList.clear();
        userProfileTagMap.clear();
        guaranteeSecondLevelDocList.clear();
        isTimeout = false;
        isError = false;
        ratioForRealTimeMixer = 0;
        realTimeRecallList.clear();
        userPrePubDocLongTermList.clear();
        isFromPublisherCache = false;
        isFromSameAudioCache = false;
        isFromTagCache = false;
        blockList.clear();
        status = Integer.MAX_VALUE;
        trailerVideo.clear();
        predictDocumentList.clear();
        latestViewRelatedDocList.clear();
        topHotStateList.clear();
        featureString = BaseMagicValueEnum.STRING_INITIAL_VALUE;
        userProfileCount.clear();
        notTopHotIdList.clear();
        idToRecallNameMap.clear();
        redDotInfo = BaseMagicValueEnum.STRING_INITIAL_VALUE;
        topKOLVideoIn30DaysList.clear();
        similarRealList.clear();
        bigHeadList.clear();
        funnyList.clear();
        newLanguageDocumentList.clear();
        countryDocumentList = null;
        countryDocumentList = new ArrayList<>();
        strategyPoolConfMap.clear();
        realTimeStrategyPoolConfMap.clear();
        tagTableName = BaseMagicValueEnum.STRING_INITIAL_VALUE;
        userProfileTagMapNew.clear();
        strategyTagDocumentListMap.clear();
        tagSet.clear();
        adultPreferenceDocumentList.clear();
        isAdultUuid = true;
        userLongTagSet.clear();
        manualList.clear();
        realTimeClickDocListPub.clear();
        this.interestTagDocList.clear();
        followCardKOLIds.clear();
        IPLDocumentList.clear();
        publisherIdSourceMap.clear();
        relatedList.clear();
        liveDocumentList.clear();
        liveLockDocumentList.clear();
        cmsPubCardPubIds.clear();
        totalNumber = BaseMagicValueEnum.INT_INITIAL_VALUE;
        nextTokenMap.clear();
        uploadDaysRecent = BaseMagicValueEnum.INT_INITIAL_VALUE;
        longTermUserProfile.clean();
        userLongCategorySet.clear();
    }

    @Override
    public String getName() {
        return getClass().getSimpleName();
    }

    /**
     * 召回队列merge到一个队列中
     */
    public void merge() {
        Map<String, BaseDocument> docMap = new LinkedHashMap<>();
        for (Document doc : asynRecallList) {
            BaseDocument d = (BaseDocument) doc;
            if (!docMap.containsKey(d.id)) {
                docMap.put(d.id, d);
            }
        }
        if (MXJudgeUtils.isNotEmpty(docMap)) {
            mergedList.addAll(docMap.values());
        }


        /**
         * nearby接口不打散, sticker_group, sticker接口不打散
         */
        if (NEARBY_PEOPLE_VERSION_1_0.getName().equals(req.interfaceName)
                || STICKER_GROUP_VERSION_1_0.getName().equals(req.interfaceName)
                || STICKER_VERSION_1_0.getName().equals(req.interfaceName)) {
            return;
        }
        //        shuffle为了打散
        if (DefineTool.TabInfoEnum.HOT.getId().equals(req.tabId) || DefineTool.TabInfoEnum.STATUS.getId().equals(req.tabId)) {
            Collections.shuffle(mergedList);
        }
    }

    /**
     * 召回队列merge到一个队列中
     */
    public void mergeForInternalInterface(InternalDataCollection dc) {
        Map<String, BaseDocument> docMap = new LinkedHashMap<>();
        for (Document doc : asynRecallList) {
            BaseDocument d = (BaseDocument) doc;
            if (!docMap.containsKey(d.id)) {
                docMap.put(d.id, d);
            }
        }
        if (MXJudgeUtils.isNotEmpty(docMap)) {
            mergedList.addAll(docMap.values());
        }

        if (dc.internalReq.interfaceName.equals(INTERNAL_SORTED_VIDEO_LIST_OF_PUBLISHER_1_0.getName())) {
            String cacheKey = String.format(INTERNAL_SORTED_VIDEO_LIST_LOCAL_CACHE_KEY, dc.internalReq.resourceIdList.get(0));
            LocalCacheDataSource localCacheDataSource = MXDataSource.cache();
            if (MXJudgeUtils.isEmpty(localCacheDataSource.getInternalSortedVideoListCache(cacheKey))) {
                localCacheDataSource.setInternalSortedVideoListCache(cacheKey, mergedList);
            } else {
                mergedList.addAll(dc.mergedList);
            }
        } else if (dc.internalReq.interfaceName.equals(INTERNAL_GENERAL_FILTER_1_0.getName())) {
            mergedList.addAll(dc.mergedList);
        } else if (dc.internalReq.interfaceName.equals(INTERNAL_POOL_VIDEO_FILTER_1_0.getName())) {
            mergedList.addAll(dc.mergedList);
        } else if (dc.internalReq.interfaceName.equals(INTERNAL_VIDEOS_OF_THE_TAG_VERSION_1_0.getName())) {
            //InternalDataCollection的结果放在前面， otherDataCollection的结果放在后面
            mergedList.addAll(0, dc.mergedList);
            totalNumber += dc.totalNumber;
        }
    }

    /**
     * 存放ES请求的类
     *
     * @author zhangxuejian
     */
    public static class ESRequest {
        public String searchRequest;
        public String searchContent;
        public String recallName;
        public String smallFlowFlag;
        private boolean onlyNeedFetchDetails;
        private List<String> ids;
        public String esItr;
        public String poolIndex;

        public ESRequest(String searchRequest, String searchContent, String recallName, String smallFlowFlag, String esItr) {
            this.searchRequest = searchRequest;
            this.searchContent = searchContent;
            this.recallName = recallName;
            this.smallFlowFlag = smallFlowFlag;
            this.onlyNeedFetchDetails = false;
            this.esItr = esItr;
        }

        public ESRequest(String searchRequest, String searchContent, String recallName, String smallFlowFlag, String esItr, String poolIndex) {
            this.searchRequest = searchRequest;
            this.searchContent = searchContent;
            this.recallName = recallName;
            this.smallFlowFlag = smallFlowFlag;
            this.onlyNeedFetchDetails = false;
            this.esItr = esItr;
            this.poolIndex = poolIndex;
        }

        public ESRequest(List<String> ids, String recallName, String smallFlowFlag) {
            this.recallName = recallName;
            this.smallFlowFlag = smallFlowFlag;
            this.onlyNeedFetchDetails = true;
            this.ids = new ArrayList<>(ids);
            this.esItr = "video";
        }

        public boolean isOnlyNeedFetchDetails() {
            return this.onlyNeedFetchDetails;
        }

        public List<String> getIds() {
            return this.ids;
        }

        @Override
        public String toString() {
            return "ESRequest{" +
                    "searchRequest='" + searchRequest + '\'' +
                    ", searchContent='" + searchContent + '\'' +
                    ", recallName='" + recallName + '\'' +
                    ", smallFlowFlag='" + smallFlowFlag + '\'' +
                    ", onlyNeedFetchDetails=" + onlyNeedFetchDetails +
                    ", ids=" + ids +
                    ", esItr='" + esItr + '\'' +
                    '}';
        }
    }

    /**
     * 添加到异步ES请求列表中, 只允许走video的ES
     */
    public void addToESRequestList(String searchRequest,
                                   String searchContent,
                                   String recallName,
                                   String smallFlowFlag,
                                   String esFlag) {
        ESRequest esRequest = new ESRequest(searchRequest, searchContent, recallName, smallFlowFlag, "video");

        if (MXStringUtils.isEmpty(esFlag) || EsType.ES_POOL.getTypeName().equals(esFlag)) {
            this.searchRequestList.add(esRequest);
        } else if (EsType.STRATEGY.getTypeName().equals(esFlag)) {
            this.strategySearchRequestList.add(esRequest);
        } else if (EsType.VIDEO.getTypeName().equals(esFlag)) {
            this.videoSearchRequestList.add(esRequest);
        } else if (EsType.VIDEO_NEW.getTypeName().equals(esFlag)) {
            this.videoNewSearchRequestList.add(esRequest);
        } else {
            System.out.println("please choice a es type!!");
        }
    }

    /**
     * timeRecordMap取代timeRecord来记录时间
     */
    public void appendToTimeRecord(long spendTime, String moduleName) {
        debug.timeRecordMap.put(moduleName, (int) (spendTime / 1000000));
    }

    public void appendToDeletedRecord(int deletedSize, String moduleName) {
        if (0 == deletedSize) {
            return;
        }
        if (debug.deletedRecordMap.get(moduleName) == null) {
            debug.deletedRecordMap.putIfAbsent(moduleName, deletedSize);
        } else {
            debug.deletedRecordMap.put(moduleName, debug.deletedRecordMap.get(moduleName) + deletedSize);
        }
    }

    /**
     * 各组件,自己打自己的日志
     */
    public String dcLog() {
        logMap.put("request", this.req.toString());
        extractRequestInfoToLog(req, logMap);

        logMap.put("mergedListSize", String.valueOf(mergedList.size()));
        logMap.put("resultSize", String.valueOf(data.result.resultList.size()));
        logMap.put("bloomHistorySize", String.valueOf(userHistorySize));
        logMap.put("historyIdListSize", String.valueOf(historyIdList.size()));
        if (null != data.response && data.response.resultList != null) {
            logMap.put("responseSize", String.valueOf(data.response.resultList.size()));
        } else {
            logMap.put("responseSize", "0");
        }
        logMap.put("isDebugModeOpen", String.valueOf(isDebugModeOpen));
        logMap.put("totalTime", String.valueOf(this.endTime - this.startTime));
        logMap.put("resultFromMap", resultFromMap.toString());
        logMap.put("smallFlowName", recommendFlow.name);
        // logMap.put("deletedIdRecordMap", deletedIdRecordMap.toString());
        logMap.put("userId", client.user.userId);
        logMap.put("uuid", client.user.uuId);
        logMap.put("bigBloomFilterCount", String.valueOf(bigBloomFilterCount));
        logMap.put("nextToken", data.response.nextToken);
        logMap.put("cacheStatus", util.cacheStatus.toString());
        logMap.put("isFromPublisherCache", String.valueOf(isFromPublisherCache));
        logMap.put("isFromTagCache", String.valueOf(isFromTagCache));
        logMap.put("isFromSameAudioCache", String.valueOf(isFromSameAudioCache));
        logMap.put("userFollowPublishListSize", String.valueOf(userFollowPublishListSize));
        logMap.put("redDotInfo", redDotInfo);
        logMap.put("isRobotRequest", String.valueOf(req.isRobotRequest));
        if (req.location != null) {
            logMap.put("country", req.location.country);
            logMap.put("state", req.location.state);
            logMap.put("city", req.location.city);
        }
        logMap.put("platformId", req.platformId);
        logMap.put("tabId", req.tabId);
        logMap.put("logId", req.logId);
        logMap.put("resourceId", req.resourceId);
        logMap.put("resourceType", req.resourceType);
        if (MXJudgeUtils.isNotEmpty(req.languageList)) {
            logMap.put("languageList", req.languageList.toString());
        }
        logMap.put("nextToken", req.nextToken);
        logMap.put("execTimeDelay", req.execTimeDelay);
        logMap.put("isRetryRequest", String.valueOf(req.isRetryRequest));
        logMap.put("isRobotRequest", String.valueOf(req.isRobotRequest));
        if (req.extraClientInfo != null) {
            logMap.put("lastInteractiveId", req.extraClientInfo.lastInteractiveId);
            logMap.put("lastInteractiveTimestamp", req.extraClientInfo.lastInteractiveTimestamp);
            logMap.put("lastInteractiveType", req.extraClientInfo.lastInteractiveType);
        }

        Map<String, Integer> map = new HashMap<>();
        if (null != recallFilterCount) {
            for (String key : recallFilterCount.elementSet()) {
                map.put(key, recallFilterCount.count(key));
            }
        }
        Map<String, Integer> map2 = new HashMap<>();
        if (null != userProfileCount) {
            for (String key : userProfileCount.elementSet()) {
                map2.put(key, userProfileCount.count(key));
            }
        }

        if (MXJudgeUtils.isNotEmpty(data.response.resultList)) {
            if (DefineTool.CategoryEnum.SHORT_VIDEO.getName().equals(data.response.resultList.get(0).getResultType())) {
                packVideoInResultListInfoToLog();
            }
        }
        debug.timeRecordMap.put("totalTime", (int) (this.endTime - this.startTime));

        debug.logCollectorMap.put("resultSize", data.result.resultListSize);
        debug.logCollectorMap.put("totalRecallDeleted", map.values().stream().reduce(Integer::sum).orElse(0));

        String log = JSON.toJSONString(logMap);

        log = mergeInLog(log, "filterDeleted", simplifyDeletedRecordMap(debug.deletedRecordMap));
        log = mergeInLog(log, "recallDeleted", map);
        log = mergeInLog(log, "userProfileCount", map2);
        log = mergeInLog(log, "timeSpendMs", debug.timeRecordMap);
        log = mergeInLog(log, "syncSearchRecallListSize", syncSearchResultSizeMap);
        log = mergeInLog(log, "logComponentExtra", logComponentExtra);
        log = mergeInLog(log, "logCollector", debug.logCollectorMap);

        return log;
    }

    private void packVideoInResultListInfoToLog() {
        List<JSONObject> list = new ArrayList<>();
        for (int i = 0; i < data.response.resultList.size(); i++) {
            Result result = data.response.resultList.get(i);
            if (DefineTool.CategoryEnum.SHORT_VIDEO.getName().equals(result.getResultType())) {
                String id = result.shortVideo.getId();
                JSONObject object = new JSONObject();
                object.put("id", id);
                object.put("resultOrder", i);
                object.put("type", result.shortVideo.getType());
                object.put("title", result.shortVideo.getName());
                if (debug.debugInfoMap.containsKey(id)) {
                    object.put("debugInfo", debug.debugInfoMap.get(id));
                }
                if (debug.attachInfoMap.containsKey(id)) {
                    object.put("attachInfo", debug.attachInfoMap.get(id));
                }
                list.add(object);
            }

        }
        logMap.put("resultListDetail", list.toString());
    }

    public String mergeInLog(String fatherString, String sonName, Map sonMap) {
        if (MXJudgeUtils.isEmpty(sonMap)) {
            return fatherString;
        }
        fatherString = fatherString.substring(0, fatherString.length() - 1);
        fatherString += String.format(",\"%s\":%s}", sonName, JSON.toJSONString(sonMap));
        return fatherString;
    }

    public String mergeInLog(String fatherString, String sonName, String sonString) {
        if (MXJudgeUtils.isEmpty(sonString)) {
            return fatherString;
        }
        fatherString = fatherString.substring(0, fatherString.length() - 1);
        fatherString += String.format(",\"%s\":%s}", sonName, sonString);
        return fatherString;
    }

    public void extractRequestInfoToLog(Request request, Map logMap) {
        if (null == request || null == logMap) {
            return;
        }
        if (request.isSetUserInfo()) {
            if (request.userInfo.isSetUserId())
                logMap.put("userId", request.userInfo.getUserId());
            if (request.userInfo.isSetUuid())
                logMap.put("uuid", request.userInfo.getUuid());
        }
        if (request.isSetInterfaceName()) {
            logMap.put("interfaceName", request.getInterfaceName());
        }
        if (request.isSetLogId()) {
            logMap.put("logId", request.getLogId());
        }
        if (request.isSetPlatformId()) {
            logMap.put("platformId", request.getPlatformId());
        }
        if (request.isSetTabId()) {
            logMap.put("tabId", request.getTabId());
        }
        if (request.isSetType()) {
            logMap.put("type", request.getType());
        }
        if (request.isSetNum()) {
            logMap.put("num", request.getNum());
        }
    }

    /**
     * 记录召回信息，目前用于翻页填写 next token
     * 要在 merge 方法之前执行，防止顺序有变化
     */
    public void recordRecallInfo() {
        // 发布者视频接口
        if (MX_VIDEOS_OF_THE_PUBLISHER_VERSION_1_0.getName().equals(this.req.interfaceName)
                || MX_VIDEOS_OF_THE_PUBLISHER_ME_VERSION_1_0.getName().equals(this.req.interfaceName)
                || NEARBY_PEOPLE_VERSION_1_0.getName().equals(this.req.interfaceName)) {
            // 记录本次所有召回的数量（此处跟 asynRecallList 有耦合，比较烦人，先这样吧）
            recallSize = asynRecallList.size();

            // 记录最后一个召回文档并打包，稍后填 next token 要用
            if (recallSize > 0) {
                BaseDocument lastDocument = asynRecallList.get(recallSize - 1);
                lastRecallResult = packResult(lastDocument);
            }
        }
    }

    /**
     * 打包一个文档结果，子类实现具体方法
     *
     * @param document 要打包的文档
     */
    Result packResult(BaseDocument document) {
        return new Result();
    }

    /**
     * 最终的打包结果列表
     */
    public List<Result> getResultList() {
        return data.result.resultList;
    }

    /**
     * 返回结果列表
     */
    public List<BaseDocument> getRecallResult() {
        return mergedList;
    }

    /**
     * 添加结果列表
     */
    public void addRecallResult(List<BaseDocument> result) {
        mergedList.addAll(result);
    }

    /**
     * 设置结果列表
     */
    public void setRecallResult(List<BaseDocument> result) {
        mergedList.clear();
        mergedList.addAll(result);
    }

    /**
     * 返回结果数据
     *
     * @param key key
     * @return List or Map
     */
    public Object getResult(String key) {
        return data.recall.resultMap.get(key);
    }

    /**
     * 添加一个结果列表
     */
    public void addResult(String key, List<BaseDocument> result) {
        List<BaseDocument> list = (List<BaseDocument>) data.recall.resultMap.computeIfAbsent(key, s -> new ArrayList<>());
        list.addAll(result);
    }

    /**
     * 设置（覆盖）一个结果数据
     *
     * @param key    key
     * @param result List or Map
     */
    public void setResult(String key, Object result) {
        data.recall.resultMap.put(key, result);
    }

    /**
     * 当前请求的resultMap是否为空
     *
     * @return
     */
    public boolean isRecallResultEmpty() {
        return data.recall.isEmpty();
    }

    /**
     * 设置 Prepare 组件的结果
     */
    public void setPrepareResult(String prepareName, Object result) {
        data.temp.prepareResultMap.put(prepareName, result);
    }

    /**
     * 获取 Prepare 组件的结果
     */
    public Object getPrepareResult(String prepareName) {
        return data.temp.prepareResultMap.get(prepareName);
    }

    /**
     * 请求用户信息
     */
    public MXUserInfo getUserInfo() {
        return client.user;
    }

    /**
     * 简化deletedRecordMap日志信息
     *
     * @param oldMap
     * @return
     */
    public Map<String, Map<String, Integer>> simplifyDeletedRecordMap(Map<String, Integer> oldMap) {
        if (oldMap == null) {
            return null;
        }
        Map<String, Map<String, Integer>> outerMap = new HashMap<>();
        for (Map.Entry<String, Integer> entry : oldMap.entrySet()) {
            String key = entry.getKey();

            String outer = "SINGLE";
            String inner = key;
            if (key != null && key.matches("[\\w]*_lv.*")) {
                outer = key.replaceFirst("_lv.*", "");
                inner = key.replace(outer + "_", "");

            }

            if (outerMap.get(outer) == null) {
                Map<String, Integer> innerMap = new HashMap<>();
                innerMap.put(inner, entry.getValue());
                outerMap.put(outer, innerMap);
            } else {
                outerMap.get(outer).put(inner, entry.getValue());
            }
        }
        return outerMap;
    }

    public DefineTool.Cache.CacheOperationEnum getCacheOperation() {
        return util.cacheOperation;
    }

    public void setCacheOperation(DefineTool.Cache.CacheOperationEnum cacheOperation) {
        this.util.cacheOperation = cacheOperation;
    }

    /**
     * 结果访问接口
     */
    public interface IResult {
        String KEY_RESULT = "result";

        /**
         * 召回列表集合，维护和解决冲突用
         * 各可配置召回器按此内容配置值
         */
        enum ListEnum {
            // 注意，DEFAULT 这种配置指明存 mergedList，不往 resultMap 里存
            DEFAULT("default"),
            USER_PROFILE_OFFLINE("user_profile_offline"),
            NEW_LANGUAGE("new_language"),
            SOUND_OUT("sound_out"),
            USER_PROFILE_REDIS_ACTION_V1("user_profile_redis_action_v1"),// 贺哥实验
            USER_PROFILE_TAG("user_profile_tag"),// 林哥实验
            REAL_TIME_ACTION("real_time_action"),
            REAL_TIME_ACTION_STORAGE("real_time_action_storage"),
            EXPOSURE_POOL("exposure_pool"),// 曝光池
            PROFILE_POOL("profile_pool")// 个性化池
            ;

            public final String value;

            ListEnum(String value) {
                this.value = value;
            }
        }

        /**
         * 返回访问数据的键
         */
        String getResultKey();
    }

    public static void main(String[] args) {
        Map<String, Integer> oldMap = new HashMap<>();
        Map<String, Map<String, Integer>> outerMap = new HashMap<>();
        oldMap.put("simple", 1);
        oldMap.put("simple_lv2", 1);
        oldMap.put("simple_lv2_v2", 1);

        for (Map.Entry<String, Integer> entry : oldMap.entrySet()) {
            String key = entry.getKey();

            String outer = "SINGLE";
            String inner = key;
            if (key != null && key.matches("[\\w]*_lv.*")) {
                outer = key.replaceFirst("_lv.*", "");
                inner = key.replace(outer + "_", "");

            }

            if (outerMap.get(outer) == null) {
                Map<String, Integer> innerMap = new HashMap<>();
                innerMap.put(inner, entry.getValue());
                outerMap.put(outer, innerMap);
            } else {
                outerMap.get(outer).put(inner, entry.getValue());
            }
        }
        System.out.println(oldMap);
        System.out.println(outerMap);
    }
}
