package mx.j2.recommend.data_source;

import com.alibaba.fastjson.JSONObject;
import com.alicp.jetcache.Cache;
import com.alicp.jetcache.MultiLevelCacheBuilder;
import com.alicp.jetcache.embedded.CaffeineCacheBuilder;
import com.alicp.jetcache.redis.lettuce.RedisLettuceCacheBuilder;
import com.alicp.jetcache.support.FastjsonKeyConvertor;
import com.alicp.jetcache.support.JavaValueDecoder;
import com.alicp.jetcache.support.JavaValueEncoder;
import com.google.common.collect.Lists;
import com.newrelic.api.agent.Trace;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.document.LiveDocument;
import mx.j2.recommend.data_model.document.SageMakerPublisherFeatureDocument;
import mx.j2.recommend.thrift.Result;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.LogTool;
import mx.j2.recommend.util.MXJudgeUtils;
import mx.j2.recommend.util.MXStringUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.concurrent.NotThreadSafe;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * LocalCache 服务接口
 *
 * @author zhangxuejian
 */
@NotThreadSafe
public class LocalCacheDataSource extends BaseDataSource {

    private static Logger logger = LogManager.getLogger(LocalCacheDataSource.class);
    private final static List<BaseDocument> DEFAULT_RESULT_LIST = new ArrayList<>();

    /**
     * 保底召回-缓存文档数量上限，缓存时间（分钟）
     */
    private static final int GUARANTEE_CACHE_VOLUME = 10000;
    private static final int GUARANTEE_CACHE_ALIVE_TIME = 10;

    /**
     * 指定id的特殊召回文档数量上线，缓存时间（分钟）
     */
    private static final int SPECIAL_CACHE_VOLUME = 10000;
    private static final int SPECIAL_CACHE_ALIVE_TIME = 10;
    private static final int SCORE_WEIGHT_CACHE_VOLUME = 10000;
    private static final int PUBLIDHER_CACHE_VOLUME = 10000;
    private static final int AUDIO_NUM_CACHE_VOLUME = 10000;

    /**
     * 用来缓存运营在 Redis 中配置的置顶视频 list，缓存时间（分钟）
     */
    private static final int MANUAL_CONTROL_REDIS_CACHE_VOLUME = 300;
    private static final int TOP_KOL_VIDEO_30_DAYS_REDIS_CACHE_VOLUME = 500;
    private static final int MANUAL_CONTROL_REDIS_CACHE_ALIVE_TIME = 1;
    private static final int TOP_KOL_VIDEO_30_DAYS_REDIS_CACHE_ALIVE_TIME = 30;

    /**
     * 用户follower的缓存配置参数
     */
    private static final int USER_FOLLOWER_CACHE_VOLUME = 10000;
    private static final int USER_FOLLOWER_CACHE_ALIVE_TIME = 10;

    /**
     * userProfile publisher列表缓存配置参数
     */
    private static final int USER_PROFILE_PUBLISHER_CACHE_VOLUME = 10000;
    private static final int USER_PROFILE_PUBLISHER_ALIVE_TIME = 30;

    /**
     * 试探池召回缓存
     */
    private static final int SOUND_OUT_CACHE_VOLUME = 10000;
    private static final int SOUND_OUT_CACHE_ALIVE_TIME = 20;

    /**
     * 贴纸分组召回缓存参数
     */
    private static final int STICKER_GROUP_CACHE_VOLUME = 1;
    private static final int STICKER_GROUP_CACHE_ALIVE_TIME = 1;

    /**
     * 贴纸召回缓存参数
     */
    private static final int STICKER_CACHE_VOLUME = 100;
    private static final int STICKER_CACHE_ALIVE_TIME = 30;

    /**
     * cms publisher 卡publisher缓存
     */
    private static final int CMS_PUB_CARD_VOLUME = 5;
    private static final int CMS_PUB_CARD_ALIVE_TIME = 10;
    private final Cache<String, List<String>> cmsPubCardPubIdsCache = CaffeineCacheBuilder.createCaffeineCacheBuilder()
            .limit(CMS_PUB_CARD_VOLUME).expireAfterWrite(CMS_PUB_CARD_ALIVE_TIME, TimeUnit.SECONDS).buildCache();

    /**
     * 贴纸召回缓存
     */
    private final Cache<String, List<BaseDocument>> stickerRecallCache = CaffeineCacheBuilder.createCaffeineCacheBuilder()
            .limit(STICKER_CACHE_VOLUME).expireAfterWrite(STICKER_CACHE_ALIVE_TIME, TimeUnit.SECONDS).buildCache();

    /**
     * 贴纸分组召回缓存
     */
    private final Cache<String, List<BaseDocument>> stickerGroupRecallCache = CaffeineCacheBuilder.createCaffeineCacheBuilder()
            .limit(STICKER_GROUP_CACHE_VOLUME).expireAfterWrite(STICKER_GROUP_CACHE_ALIVE_TIME, TimeUnit.MINUTES).buildCache();

    /**
     * 试探池召回缓存
     */
    private final Cache<String, List<BaseDocument>> soundOutRecallCache = CaffeineCacheBuilder.createCaffeineCacheBuilder()
            .limit(SOUND_OUT_CACHE_VOLUME).expireAfterWrite(SOUND_OUT_CACHE_ALIVE_TIME, TimeUnit.SECONDS).buildCache();

    /**
     * userProfile publisher列表缓存
     */
    private final Cache<String, List<String>> userProfilePublisherCache = CaffeineCacheBuilder.createCaffeineCacheBuilder()
            .limit(USER_PROFILE_PUBLISHER_CACHE_VOLUME).expireAfterWrite(USER_PROFILE_PUBLISHER_ALIVE_TIME, TimeUnit.SECONDS).buildCache();

    /**
     * 保底召回
     */
    private final Cache<String, List<BaseDocument>> recallCache = CaffeineCacheBuilder.createCaffeineCacheBuilder()
            .limit(GUARANTEE_CACHE_VOLUME).expireAfterWrite(GUARANTEE_CACHE_ALIVE_TIME, TimeUnit.MINUTES).buildCache();

    /**
     * 视频类指定id的特殊召回
     */
    private final Cache<String, List<BaseDocument>> specialRecallCache = CaffeineCacheBuilder.createCaffeineCacheBuilder()
            .limit(SPECIAL_CACHE_VOLUME).expireAfterWrite(SPECIAL_CACHE_ALIVE_TIME, TimeUnit.MINUTES).buildCache();

    /**
     * 卡片召回条件相关的
     */
    private final Cache<String, String> cardRecallConditionCache = CaffeineCacheBuilder.createCaffeineCacheBuilder()
            .limit(GUARANTEE_CACHE_VOLUME).expireAfterWrite(GUARANTEE_CACHE_ALIVE_TIME, TimeUnit.MINUTES).buildCache();

    /**
     * 运营控制置顶
     */
    private final Cache<String, Map<String, Double>> manualControlRedisListCache = CaffeineCacheBuilder.createCaffeineCacheBuilder()
            .limit(MANUAL_CONTROL_REDIS_CACHE_VOLUME).expireAfterWrite(MANUAL_CONTROL_REDIS_CACHE_ALIVE_TIME, TimeUnit.MINUTES).buildCache();
    /**
     * 大V视频
     */
    private final Cache<String, List<BaseDocument>> topKOLVideo30DaysRedisCache = CaffeineCacheBuilder.createCaffeineCacheBuilder()
            .limit(TOP_KOL_VIDEO_30_DAYS_REDIS_CACHE_VOLUME).expireAfterWrite(TOP_KOL_VIDEO_30_DAYS_REDIS_CACHE_ALIVE_TIME, TimeUnit.MINUTES).buildCache();
    /**
     * UGC recall结果的状态
     */
    private final Cache<String, Boolean> ugcRecallCacheStatus = CaffeineCacheBuilder.createCaffeineCacheBuilder()
            .limit(100).expireAfterWrite(1, TimeUnit.MINUTES).buildCache();

    /**
     * guava bloom filter's Local cache
     */
    private final Cache<String, byte[]> guavaBloomFilterCache = CaffeineCacheBuilder.createCaffeineCacheBuilder()
            .limit(50000).expireAfterWrite(1, TimeUnit.MINUTES).buildCache();

    /**
     * 召回对应分数相关数据的缓存
     */
    private final Cache<String, List<BaseDocument>> scoreWeightRecallCache = CaffeineCacheBuilder.createCaffeineCacheBuilder()
            .limit(SCORE_WEIGHT_CACHE_VOLUME).buildCache();

    private final Cache<String, List<String>> userFollowesCache = CaffeineCacheBuilder.createCaffeineCacheBuilder()
            .limit(USER_FOLLOWER_CACHE_VOLUME).expireAfterWrite(USER_FOLLOWER_CACHE_ALIVE_TIME, TimeUnit.SECONDS).buildCache();

    /**
     * 召回对应分数相关数据的缓存
     */
    private final Cache<String, List<BaseDocument>> publisherPageCache = CaffeineCacheBuilder.createCaffeineCacheBuilder()
            .limit(3000).expireAfterWrite(1, TimeUnit.MINUTES).buildCache();

    /**
     * 召回对应分数相关数据的缓存
     */
    private final Cache<String, Integer> numOfPublisherVideoCache = CaffeineCacheBuilder.createCaffeineCacheBuilder()
            .limit(PUBLIDHER_CACHE_VOLUME).expireAfterWrite(5, TimeUnit.SECONDS).buildCache();

    private final Cache<String, Integer> numOfAudioCache = CaffeineCacheBuilder.createCaffeineCacheBuilder()
            .limit(AUDIO_NUM_CACHE_VOLUME).expireAfterWrite(5, TimeUnit.SECONDS).buildCache();

    /**
     * tag对应的document的列表
     */
    private final Cache<String, List<BaseDocument>> topHotTagDocumentCache = CaffeineCacheBuilder.createCaffeineCacheBuilder()
            .limit(10000).expireAfterWrite(10, TimeUnit.MINUTES).buildCache();

    /**
     * publisher 是否有置顶数据
     */
    private final Cache<String, Boolean> publisherCache = CaffeineCacheBuilder.createCaffeineCacheBuilder()
            .limit(30000).expireAfterWrite(10, TimeUnit.MINUTES).buildCache();

    /**
     * publisher heatScore的置顶数据
     */
    private final Cache<String, List<BaseDocument>> publisherDocumentCache = CaffeineCacheBuilder.createCaffeineCacheBuilder()
            .limit(20000).expireAfterWrite(2, TimeUnit.MINUTES).buildCache();

    /**
     * hashtag置顶视频缓存
     */
    private final Cache<String, List<JSONObject>> videosStickOnTopCache = CaffeineCacheBuilder.createCaffeineCacheBuilder().
            limit(10000).expireAfterWrite(3, TimeUnit.MINUTES).buildCache();

    /**
     * 好友关系缓存
     */
    private final Cache<String, Integer> isFriendCache = CaffeineCacheBuilder.createCaffeineCacheBuilder().
            limit(50000).expireAfterAccess(30, TimeUnit.SECONDS).buildCache();

    /**
     * 不喜欢的publisher缓存
     */
    private final Cache<String, List<String>> disLikePublisherCache = CaffeineCacheBuilder.createCaffeineCacheBuilder().
            limit(10000).expireAfterAccess(5, TimeUnit.SECONDS).buildCache();

    /**
     * 实时publisher视频缓存
     */
    private final Cache<String, List<BaseDocument>> realtimePublisherCache;

    /**
     * videos_of_same_audio 接口缓存
     */
    private final Cache<String, Map<String, Object>> videosOfAudioCache = CaffeineCacheBuilder.createCaffeineCacheBuilder().
            limit(1000).expireAfterWrite(3, TimeUnit.MINUTES).buildCache();

    private final Cache<String, List<BaseDocument>> videosOfTagCache = CaffeineCacheBuilder.createCaffeineCacheBuilder().
            limit(200).expireAfterWrite(30, TimeUnit.SECONDS).buildCache();


    private final Cache<String, List<BaseDocument>> videosOfEffectCache = CaffeineCacheBuilder.createCaffeineCacheBuilder().
            limit(1000).expireAfterWrite(30, TimeUnit.SECONDS).buildCache();


    private final Cache<String, List<BaseDocument>> videosOfPublisherCache = CaffeineCacheBuilder.createCaffeineCacheBuilder().
            limit(5000).expireAfterWrite(10, TimeUnit.SECONDS).buildCache();

    /**
     * 拉黑关系缓存
     */
    private final Cache<String, List<String>> blockListCache = CaffeineCacheBuilder.createCaffeineCacheBuilder().
            limit(1000).expireAfterWrite(30, TimeUnit.SECONDS).buildCache();

    /**
     * publisher feature
     */
    private final Cache<String, SageMakerPublisherFeatureDocument> sageMakerPublisherFeatureDocCache = CaffeineCacheBuilder.createCaffeineCacheBuilder().
            limit(2000).expireAfterWrite(60, TimeUnit.SECONDS).buildCache();

    /**
     * sameAudioManualTopCache
     */
    private final Cache<String, List<BaseDocument>> sameAudioManualTopCache = CaffeineCacheBuilder.createCaffeineCacheBuilder().
            limit(2000).expireAfterWrite(60, TimeUnit.SECONDS).buildCache();

    /**
     * 特殊tag下video id list缓存
     */
    private final Cache<String, List<String>> specialPinTagVideoListCache = CaffeineCacheBuilder.createCaffeineCacheBuilder().
            limit(500).expireAfterWrite(30, TimeUnit.SECONDS).buildCache();

    /**
     * publisher关注相关推荐
     */
    private final Cache<String, List<String>> similarPublisherIdsCache = CaffeineCacheBuilder.createCaffeineCacheBuilder().
            limit(500).expireAfterWrite(30, TimeUnit.SECONDS).buildCache();

    /**
     * 准实时召回
     */
    private final Cache<String, List<String>> similarRealVideoIdsCache = CaffeineCacheBuilder.createCaffeineCacheBuilder().
            limit(500).expireAfterWrite(10, TimeUnit.SECONDS).buildCache();

    /**
     * 点赞/分享
     */
    private final Cache<String, String> likeDivideShareCache = CaffeineCacheBuilder.createCaffeineCacheBuilder().
            limit(10).expireAfterWrite(1, TimeUnit.MINUTES).buildCache();
    /**
     * 分享
     */
    private final Cache<String, String> shareRateCache = CaffeineCacheBuilder.createCaffeineCacheBuilder().
            limit(10).expireAfterWrite(1, TimeUnit.MINUTES).buildCache();

    /**
     * 混入比例
     */
    private final Cache<String, String> mixRatioCache = CaffeineCacheBuilder.createCaffeineCacheBuilder().
            limit(10).expireAfterWrite(1, TimeUnit.MINUTES).buildCache();

    /**
     * 完成
     */
    private final Cache<String, String> finishRetentionCache = CaffeineCacheBuilder.createCaffeineCacheBuilder().
            limit(10).expireAfterWrite(1, TimeUnit.MINUTES).buildCache();

    /**
     * 敏感词缓存
     */
    private final Cache<String, Set<String>> sensitiveWordCache = CaffeineCacheBuilder.createCaffeineCacheBuilder().
            limit(10).expireAfterWrite(30, TimeUnit.SECONDS).buildCache();

    private final Cache<String, List<String>> userTagsCache = CaffeineCacheBuilder.createCaffeineCacheBuilder()
            .limit(USER_FOLLOWER_CACHE_VOLUME).expireAfterWrite(60, TimeUnit.MINUTES).buildCache();

    /**
     * 内部publisher视频排序接口缓存
     */
    private final Cache<String, List<BaseDocument>> internalSortedVideoListCache = CaffeineCacheBuilder.createCaffeineCacheBuilder().
            limit(100000).expireAfterWrite(10, TimeUnit.MINUTES).buildCache();

    /**
     *
     */
    private final Cache<String, List<BaseDocument>> tagPoolLv3VideoListCache = CaffeineCacheBuilder.createCaffeineCacheBuilder().
            limit(100000).expireAfterWrite(10, TimeUnit.MINUTES).buildCache();

    /**
     * 成人偏好数据缓存
     */
    private final Cache<String, List<BaseDocument>> adultPreferenceVideoListCache = CaffeineCacheBuilder.createCaffeineCacheBuilder().
            limit(10).expireAfterWrite(1, TimeUnit.MINUTES).buildCache();

    /**
     * 音乐三个接口的结果缓存
     * key: 接口名
     * value: 结果列表
     */
    private final Cache<String, List<Result>> musicCache = CaffeineCacheBuilder.createCaffeineCacheBuilder().
            limit(3).expireAfterWrite(1, TimeUnit.MINUTES).buildCache();

    /**
     * IPL视频的缓存
     * key: 召回名+redis key
     * value: document list
     */
    private final Cache<String, List<BaseDocument>> IPLDocListCache = CaffeineCacheBuilder.createCaffeineCacheBuilder().
            limit(3).expireAfterWrite(1, TimeUnit.MINUTES).buildCache();

    /**
     * tiki视频缓存
     */
    private final Cache<String, List<BaseDocument>> tikiDocListCache = CaffeineCacheBuilder.createCaffeineCacheBuilder().
            limit(2).expireAfterWrite(30, TimeUnit.SECONDS).buildCache();

    /**
     * 直播间结果缓存
     * key:userId_interface
     */
    private final Cache<String, List<LiveDocument>> liveCache = CaffeineCacheBuilder.createCaffeineCacheBuilder().
            limit(100).expireAfterWrite(30, TimeUnit.SECONDS).buildCache();

    private final Cache<String, List<BaseDocument>> pubgDocCache = CaffeineCacheBuilder.createCaffeineCacheBuilder().
            limit(100).expireAfterWrite(10, TimeUnit.MINUTES).buildCache();

    /**
     * 个性化路保底数据缓存
     */
    private final Cache<String, String> userProfileGuaranteeCache = CaffeineCacheBuilder.createCaffeineCacheBuilder().
            limit(100).expireAfterWrite(5, TimeUnit.MINUTES).buildCache();

    /**
     * 直播用户以及时间信息缓存
     */
    private final Cache<String, Map<String, Long>> liveUserCache = CaffeineCacheBuilder.createCaffeineCacheBuilder().
            limit(10).expireAfterWrite(15, TimeUnit.SECONDS).buildCache();

    /**
     * bind audio信息缓存
     */
    private final Cache<String, Map<String, String>> bindAudioMapCache = CaffeineCacheBuilder.createCaffeineCacheBuilder().
            limit(100).expireAfterWrite(1, TimeUnit.MINUTES).buildCache();


    /**
     * 构造函数
     */
    public LocalCacheDataSource() {
        RedisURI redisUri = RedisURI.Builder.redis(Conf.getRedisCacheHost(), Conf.getRedisCachePort())
                .withTimeout(Duration.ofMillis(Conf.getJedisClusterSocketTimeout()))
                .build();
        RedisClusterClient redisClient = RedisClusterClient.create(redisUri);
        redisClient.setOptions(ClusterClientOptions.builder()
                .autoReconnect(true)
                .build());

        Cache<String, List<BaseDocument>> realtimePublisherLocalCache = CaffeineCacheBuilder.createCaffeineCacheBuilder()
                .limit(10000)
                .cacheNullValue(true)
                .buildCache();

        Cache<String, List<BaseDocument>> realtimePublisherRemoteCache = RedisLettuceCacheBuilder.createRedisLettuceCacheBuilder()
                .keyConvertor(FastjsonKeyConvertor.INSTANCE)
                .valueDecoder(JavaValueDecoder.INSTANCE)
                .valueEncoder(JavaValueEncoder.INSTANCE)
                .redisClient(redisClient)
                .keyPrefix("new-realtimePublisherRemoteCache")
                .buildCache();

        realtimePublisherCache = MultiLevelCacheBuilder.createMultiLevelCacheBuilder()
                .addCache(realtimePublisherLocalCache, realtimePublisherRemoteCache)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .buildCache();

        init();
    }

    /**
     * 初始化函数
     */
    public void init() {
        logger.info("{\"dataSourceInfo\":\"[LocalCacheSource init successfully]\"}");
    }

    /**
     * 设置试探池召回缓存
     */
    @Trace(dispatcher = true)
    public void setSoundOutRecallCache(String key, List<BaseDocument> soundOutList) {
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }
        if (soundOutList == null) {
            return;
        }
        try {
            if (soundOutRecallCache.get(key) == null) {
                soundOutRecallCache.put(key, soundOutList);
            }
        } catch (Exception e) {
            String message = String.format("put local soundOut cache for %s failed, info: %s", key, e.toString());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        }
    }

    /**
     * 获取试探池召回缓存
     */
    @Trace(dispatcher = true)
    public List<BaseDocument> getSoundOutRecallCache(String key) {
        if (!Conf.getLocalCacheSwitch()) {
            return DEFAULT_RESULT_LIST;
        }
        return soundOutRecallCache.get(key);
    }

    /**
     * 设置stickerGroup缓存
     *
     * @param key
     * @param stickerGroupList
     */
    @Trace(dispatcher = true)
    public void setStickerGroupRecallCache(String key, List<BaseDocument> stickerGroupList) {
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }
        if (stickerGroupList == null) {
            return;
        }
        try {
            if (stickerGroupRecallCache.get(key) == null) {
                stickerGroupRecallCache.put(key, stickerGroupList);
            }
        } catch (Exception e) {
            String message = String.format("put local stickerGroup cache for %s failed, info: %s", key, e.toString());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        }
    }

    /**
     * 获取stickerGroup缓存
     */
    @Trace(dispatcher = true)
    public List<BaseDocument> getStickerGroupRecallCache(String key) {
        if (!Conf.getLocalCacheSwitch()) {
            return DEFAULT_RESULT_LIST;
        }
        return stickerGroupRecallCache.get(key);
    }

    /**
     * 设置sticker缓存
     */
    @Trace(dispatcher = true)
    public void setStickerRecallCache(String key, List<BaseDocument> stickerList) {
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }
        if (stickerList == null) {
            return;
        }
        try {
            if (stickerRecallCache.get(key) == null) {
                stickerRecallCache.put(key, stickerList);
            }
        } catch (Exception e) {
            String message = String.format("put local sticker cache for %s failed, info: %s", key, e.toString());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        }
    }

    /**
     * 获取sticker缓存
     */
    @Trace(dispatcher = true)
    public List<BaseDocument> getStickerRecallCache(String key) {
        if (!Conf.getLocalCacheSwitch()) {
            return DEFAULT_RESULT_LIST;
        }
        return stickerRecallCache.get(key);
    }

    /**
     * 设置召回器缓存，为保底召回器
     */
    @Trace(dispatcher = true)
    public void setFeedRecallCache(String key, List<BaseDocument> recallList) {
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }
        if (null == recallList) {
            return;
        }
        try {
            if (null == recallCache.get(key)) {
                recallCache.put(key, Lists.newArrayList(recallList));
            }
        } catch (Exception e) {
            String message = String.format("put local recall cache for %s failed, info: %s", key, e.toString());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        }
    }

    /**
     * 获取召回器缓存，为保底召回器
     */
    @Trace(dispatcher = true)
    public List<BaseDocument> getFeedRecallCache(String key) {
        if (!Conf.getLocalCacheSwitch()) {
            return DEFAULT_RESULT_LIST;
        }
        return recallCache.get(key);
    }

    /**
     * 设置召回器缓存，为特殊召回器
     */
    @Trace(dispatcher = true)
    public void setSpecialRecallCache(String key, List<BaseDocument> recallList) {
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }
        if (null == recallList) {
            return;
        }
        try {
            if (null == specialRecallCache.get(key)) {
                specialRecallCache.put(key, Lists.newArrayList(recallList));
            }
        } catch (Exception e) {
            String message = String.format("put local recall cache for %s failed, info: %s", key, e.toString());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        }
    }

    /**
     * 获取召回器缓存，为特殊召回器
     */
    @Trace(dispatcher = true)
    public List<BaseDocument> getSpecialRecallCache(String key) {
        if (!Conf.getLocalCacheSwitch()) {
            return DEFAULT_RESULT_LIST;
        }
        return specialRecallCache.get(key);
    }

    /**
     * 设置召回器缓存，为特殊召回器
     * 单位设置为 秒
     */
    @Trace(dispatcher = true)
    public void setScoreWeightRecallCache(String key, List<BaseDocument> recallList, long expireAfterWrite) {
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }
        if (null == recallList) {
            return;
        }
        try {
            if (null == scoreWeightRecallCache.get(key)) {
                scoreWeightRecallCache.put(key, Lists.newArrayList(recallList), expireAfterWrite, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            String message = String.format("put local recall cache for %s failed, info: %s", key, e.toString());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        }
    }

    /**
     * 获取召回器缓存，为特殊召回器
     */
    @Trace(dispatcher = true)
    public List<BaseDocument> getScoreWeightRecallCache(String key) {
        if (!Conf.getLocalCacheSwitch()) {
            return DEFAULT_RESULT_LIST;
        }
        return scoreWeightRecallCache.get(key);
    }

    @Trace(dispatcher = true)
    public void setPublisherPageCache(String key, List<BaseDocument> recallList) {
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }
        if (null == recallList) {
            return;
        }
        try {
            if (null == publisherPageCache.get(key)) {
                publisherPageCache.put(key, Lists.newArrayList(recallList));
            }
        } catch (Exception e) {
            String message = String.format("put local recall cache for %s failed, info: %s", key, e.toString());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        }
    }

    @Trace(dispatcher = true)
    public List<BaseDocument> getPublisherPageCache(String key) {
        if (!Conf.getLocalCacheSwitch()) {
            return DEFAULT_RESULT_LIST;
        }
        List<BaseDocument> documents = publisherPageCache.get(key);
        if (CollectionUtils.isEmpty(documents)) {
            return DEFAULT_RESULT_LIST;
        }
        return new ArrayList<>(documents);
    }

    /**
     * 设置用户偏好publisher
     */
    public void setUserProfilePublisherCache(String key, List<String> userProfilePublisherList) {
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }
        if (userProfilePublisherList == null) {
            return;
        }
        try {
            if (userProfilePublisherCache.get(key) == null) {
                userProfilePublisherCache.put(key, userProfilePublisherList);
            }
        } catch (Exception e) {
            String message = String.format("put local userProfile publisher cache for %s failed, info: %s", key, e.toString());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        }
    }

    /**
     * 获取用户偏好publisher
     */
    public List<String> getUserProfilePublisherCache(String key) {
        if (!Conf.getLocalCacheSwitch()) {
            return new ArrayList<>();
        }
        return userProfilePublisherCache.get(key);
    }

    /**
     * 设置运营置顶数据
     * 单位设置为分钟
     */
    @Trace(dispatcher = true)
    public void setManualControlRedisListCache(String key, Map<String, Double> topNVideosIdsMap) {
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }
        if (null == topNVideosIdsMap) {
            return;
        }
        try {
            if (null == manualControlRedisListCache.get(key)) {
                manualControlRedisListCache.put(key, topNVideosIdsMap);
            }
        } catch (Exception e) {
            String message = String.format("put local recall cache for %s failed, info: %s", key, e.toString());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        }
    }

    /**
     * 设置30天大V视频缓存
     * 单位设置为分钟
     */
    @Trace(dispatcher = true)
    public void setTopKolVideo30DaysRedisCache(String key, List<BaseDocument> topKOLVideoIn30DaysList) {
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }
        if (null == topKOLVideoIn30DaysList) {
            return;
        }
        try {
            if (null == topKOLVideo30DaysRedisCache.get(key)) {
                topKOLVideo30DaysRedisCache.put(key, topKOLVideoIn30DaysList);
            }
        } catch (Exception e) {
            String message = String.format("put local recall cache for %s failed, info: %s", key, e.toString());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        }
    }

    /**
     * 运营手动控制视频列表
     */
    @Trace(dispatcher = true)
    public Map<String, Double> getManualControlRedisListCache(String key) {
        if (!Conf.getLocalCacheSwitch()) {
            return new HashMap<>();
        }
        return manualControlRedisListCache.get(key);
    }

    /**
     * 得到30天大V视频缓存
     */
    @Trace(dispatcher = true)
    public List<BaseDocument> getTopKolVideo30DaysRedisCache(String key) {
        if (!Conf.getLocalCacheSwitch()) {
            return null;
        }
        return topKOLVideo30DaysRedisCache.get(key);
    }

    /**
     * 清理一把缓存
     */
    @Trace(dispatcher = true)
    public boolean clearManualControlRedisListCache(String key) {
        if (!Conf.getLocalCacheSwitch()) {
            return false;
        }
        return manualControlRedisListCache.remove(key);
    }


    /**
     * 获取召回器条件缓存
     */
    @Trace(dispatcher = true)
    public String getRecallConditionCache(String key) {
        if (!Conf.getLocalCacheSwitch()) {
            return null;
        }
        return cardRecallConditionCache.get(key);
    }

    /**
     * 缓存状态 的缓存。在UGC recall中用来标识
     *
     * @param s
     */
    @Trace(dispatcher = true)
    public void setCacheCanUsed(String s) {
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }
        ugcRecallCacheStatus.put(s, true);
    }

    @Trace(dispatcher = true)
    public boolean getCacheCanUsed(String s) {
        if (!Conf.getLocalCacheSwitch()) {
            return false;
        }
        Boolean b = ugcRecallCacheStatus.get(s);
        return b == null ? false : b;
    }

    @Trace(dispatcher = true)
    public void setNumOfVideoPublisher(String key, int num) {
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }
        numOfPublisherVideoCache.put(key, num);
    }

    @Trace(dispatcher = true)
    public Integer getNumOfVideoPublisher(String key) {
        if (!Conf.getLocalCacheSwitch()) {
            return null;//null for miss
        }
        return numOfPublisherVideoCache.get(key);
    }

    @Trace(dispatcher = true)
    public void setNumOfAudio(String key, int num) {
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }
        numOfAudioCache.put(key, num);
    }

    @Trace(dispatcher = true)
    public Integer getNumOfAudio(String key) {
        if (!Conf.getLocalCacheSwitch()) {
            return null;
        }
        return numOfAudioCache.get(key);
    }

    @Trace(dispatcher = true)
    public List<String> getUserFollowersFromCache(String key) {
        if (!Conf.getLocalCacheSwitch()) {
            return null;
        }
        return userFollowesCache.get(key);
    }

    @Trace(dispatcher = true)
    public void setUserFollowesCache(String key, List<String> followers) {
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }
        userFollowesCache.put(key, followers);
    }

    /**
     * guava bloom 的本地缓存
     *
     * @param userId
     * @return
     */
    @Trace(dispatcher = true)
    public byte[] getBloomFilter(String userId) {
        if (!Conf.getLocalCacheSwitch()) {
            return null;
        }
        return guavaBloomFilterCache.get(userId);
    }

    @Trace(dispatcher = true)
    public void setBloomFilter(String userId, byte[] guavaBloomFilter) {
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }
        guavaBloomFilterCache.put(userId, guavaBloomFilter);
    }

    @Trace(dispatcher = true)
    public void setTopHotTagDocumentCache(String key, List<BaseDocument> docList) {
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }
        if (null == docList) {
            return;
        }
        try {
            if (null == topHotTagDocumentCache.get(key)) {
                topHotTagDocumentCache.put(key, Lists.newArrayList(docList));
            }
        } catch (Exception e) {
            String message = String.format("put top hot tag document cache for %s failed, info: %s", key, e.toString());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        }
    }

    /**
     * 获取召回器缓存，为特殊召回器
     */
    @Trace(dispatcher = true)
    public List<BaseDocument> getTopHotTagDocumentCache(String key) {
        if (!Conf.getLocalCacheSwitch()) {
            return Collections.emptyList();
        }
        return topHotTagDocumentCache.get(key);
    }

    @Trace(dispatcher = true)
    public Boolean getCacheIsHaveTopHotInPublisher(String publisherId) {
        if (!Conf.getLocalCacheSwitch()) {
            return null;
        }
        return publisherCache.get(publisherId);
    }

    @Trace(dispatcher = true)
    public void setCacheIsHaveTopHotInPublisher(String publisherId, boolean isHaveTop) {
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }
        publisherCache.put(publisherId, isHaveTop);
    }

    @Trace(dispatcher = true)
    public List<BaseDocument> getCacheHaveTopHeatScoreInPublisher(String publisherId) {
        if (!Conf.getLocalCacheSwitch()) {
            return null;
        }
        return publisherDocumentCache.get(publisherId);
    }

    @Trace(dispatcher = true)
    public void setCacheHaveTopHeatScoreInPublisher(String publisherId, List<BaseDocument> documents) {
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }
        if (documents == null) {
            return;
        }
        publisherDocumentCache.put(publisherId, documents);
    }

    @Trace(dispatcher = true)
    public void setVideosStickOnTopCache(String key, List<JSONObject> jsonList) {
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }
        if (null == jsonList) {
            return;
        }
        try {
            if (null == videosStickOnTopCache.get(key)) {
                videosStickOnTopCache.put(key, Lists.newArrayList(jsonList));
            }
        } catch (Exception e) {
            String message = String.format("put videos stick on top of the hashTag for %s failed, info: %s", key, e.toString());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        }
    }

    @Trace(dispatcher = true)
    public List<JSONObject> getVideosStickOnTopCache(String key) {
        if (!Conf.getLocalCacheSwitch()) {
            return Collections.emptyList();
        }
        return videosStickOnTopCache.get(key);
    }

    @Trace(dispatcher = true)
    public void setIsFriendCache(String key, Integer status) {
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }
        if (null == status) {
            return;
        }
        try {
            if (null == isFriendCache.get(key)) {
                isFriendCache.put(key, status);
            }
        } catch (Exception e) {
            String message = String.format("put isFriend for %s failed, info: %s", key, e.toString());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        }
    }

    @Trace(dispatcher = true)
    public Integer getIsFriendCache(String key) {
        if (!Conf.getLocalCacheSwitch()) {
            return null;
        }
        return isFriendCache.get(key);
    }

    @Trace(dispatcher = true)
    public List<String> getDisLikePublisherCache(String key) {
        if (!Conf.getLocalCacheSwitch()) {
            return Collections.emptyList();
        }
        return disLikePublisherCache.get(key);
    }

    @Trace(dispatcher = true)
    public void setDisLikePublisherCache(String key, List<String> disLikeList) {
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }
        if (null == disLikeList) {
            return;
        }
        try {
            if (null == disLikePublisherCache.get(key)) {
                disLikePublisherCache.put(key, Lists.newArrayList(disLikeList));
            }
        } catch (Exception e) {
            String message = String.format("put dislike publisher for %s failed, info: %s", key, e.toString());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        }
    }

    @Trace(dispatcher = true)
    public List<BaseDocument> getRealtimePublisherCache(String key) {
        if (!Conf.getLocalCacheSwitch()) {
            return Collections.emptyList();
        }
        return realtimePublisherCache.get(key);
    }

    @Trace(dispatcher = true)
    public void setRealtimePublisherCache(String key, List<BaseDocument> documents) {
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }
        if (null == documents) {
            return;
        }
        try {
            if (null == realtimePublisherCache.get(key)) {
                realtimePublisherCache.put(key, Lists.newArrayList(documents));
            }
        } catch (Exception e) {
            String message = String.format("set videos of real time publisher document cache for %s failed, info: %s", key, e.toString());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        }
    }

    @Trace(dispatcher = true)
    public Map<String, Object> getVideosOfAudioCache(String key) {
        if (!Conf.getLocalCacheSwitch()) {
            return Collections.emptyMap();
        }
        return videosOfAudioCache.get(key);
    }

    @Trace(dispatcher = true)
    public void setVideosOfAudioCache(String key, Map<String, Object> map) {
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }
        if (null == map) {
            return;
        }
        try {
            if (null == videosOfAudioCache.get(key)) {
                videosOfAudioCache.put(key, new HashMap<>(map));
            }
        } catch (Exception e) {
            String message = String.format("put dislike publisher for %s failed, info: %s", key, e.toString());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        }
    }

    @Trace(dispatcher = true)
    public List<BaseDocument> getVideosOfTagCache(String key) {
        if (!Conf.getLocalCacheSwitch()) {
            return Collections.emptyList();
        }
        return videosOfTagCache.get(key);
    }

    @Trace(dispatcher = true)
    public void setVideosOfTagCache(String key, List<BaseDocument> docList) {
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }
        if (null == docList) {
            return;
        }
        try {
            if (null == videosOfTagCache.get(key)) {
                videosOfTagCache.put(key, Lists.newArrayList(docList));
            }
        } catch (Exception e) {
            String message = String.format("put dislike publisher for %s failed, info: %s", key, e.toString());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        }
    }

    @Trace(dispatcher = true)
    public List<BaseDocument> getVideosOfEffectCache(String key) {
        if (!Conf.getLocalCacheSwitch()) {
            return Collections.emptyList();
        }
        return videosOfEffectCache.get(key);
    }

    @Trace(dispatcher = true)
    public void setVideosOfEffectCache(String key, List<BaseDocument> docList) {
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }
        if (null == docList) {
            return;
        }
        try {
            if (null == videosOfEffectCache.get(key)) {
                videosOfEffectCache.put(key, Lists.newArrayList(docList));
            }
        } catch (Exception e) {
            String message = String.format("put effect for %s failed, info: %s", key, e.toString());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        }
    }

    @Trace(dispatcher = true)
    public List<BaseDocument> getVideosOfPublisherCache(String key) {

        if (!Conf.getLocalCacheSwitch()) {
            return Collections.emptyList();
        }
        return videosOfPublisherCache.get(key);
    }

    @Trace(dispatcher = true)
    public void setVideosOfPublisherCache(String key, List<BaseDocument> docList) {
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }
        if (null == docList) {
            return;
        }
        try {
            if (null == videosOfPublisherCache.get(key)) {
                videosOfPublisherCache.put(key, Lists.newArrayList(docList));
            }
        } catch (Exception e) {
            String message = String.format("put dislike publisher for %s failed, info: %s", key, e.toString());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        }
    }

    @Trace(dispatcher = true)
    public void setBlockListCache(String key, List<String> pubIds) {
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }
        if (null == pubIds) {
            return;
        }
        try {
            if (null == blockListCache.get(key)) {
                blockListCache.put(key, pubIds);
            }
        } catch (Exception e) {
            String message = String.format("setBlockListCache for %s failed, info: %s", key, e.toString());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        }
    }

    @Trace(dispatcher = true)
    public List<String> getBlockListCache(String key) {
        if (!Conf.getLocalCacheSwitch()) {
            return Collections.emptyList();
        }
        return blockListCache.get(key);
    }

    @Trace(dispatcher = true)
    public void setSageMakerPublisherFeatureToCache(Map<String, SageMakerPublisherFeatureDocument> needPutToCacheMap) {
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }
        sageMakerPublisherFeatureDocCache.putAll(needPutToCacheMap);
    }

    @Trace(dispatcher = true)
    public Map<String, SageMakerPublisherFeatureDocument> getSageMakerPubFeatureFromCache(Set<String> pubIds) {
        if (!Conf.getLocalCacheSwitch()) {
            return null;
        }
        return sageMakerPublisherFeatureDocCache.getAll(pubIds);
    }

    @Trace(dispatcher = true)
    public void setSameAudioManualTopCache(String audioID, List<BaseDocument> list) {
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }
        if(StringUtils.isEmpty(audioID)){
            return;
        }
        if(CollectionUtils.isEmpty(list)){
            return;
        }
        sameAudioManualTopCache.put(audioID,list);
    }

    @Trace(dispatcher = true)
    public List<BaseDocument> getSameAudioManualTopCache(String audioId) {
        if (!Conf.getLocalCacheSwitch()) {
            return null;
        }
        if(StringUtils.isEmpty(audioId)){
            return null;
        }
        return sameAudioManualTopCache.get(audioId);
    }

    public void setSpecialPinTagVideoListCache(String key, List<String> videoIds) {
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }
        if (null == videoIds) {
            return;
        }
        try {
            if (null == specialPinTagVideoListCache.get(key)) {
                specialPinTagVideoListCache.put(key, videoIds);
            }
        } catch (Exception e) {
            String message = String.format("setBlockListCache for %s failed, info: %s", key, e.toString());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        }
    }

    @Trace(dispatcher = true)
    public List<String> getSpecialPinTagVideoListCache(String key) {
        if (!Conf.getLocalCacheSwitch()) {
            return Collections.emptyList();
        }
        return specialPinTagVideoListCache.get(key);
    }

    @Trace(dispatcher = true)
    public List<String> getSimilarPublisherIdsCache(String key) {
        if (!Conf.getLocalCacheSwitch()) {
            return null;
        }
        return similarPublisherIdsCache.get(key);
    }

    @Trace(dispatcher = true)
    public void setSimilarPublisherIdsCache(String key, List<String> publisherIds) {
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }
        if (MXJudgeUtils.isEmpty(publisherIds)) {
            return;
        }
        try {
            if (null == similarPublisherIdsCache.get(key)) {
                similarPublisherIdsCache.put(key, publisherIds);
            }
        } catch (Exception e) {
            String message = String.format("similarPublisherIdsCache for %s failed, info: %s", key, e.toString());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        }
    }

    @Trace(dispatcher = true)
    public List<String> getUserTagsCache(String key) {
        if (!Conf.getLocalCacheSwitch()) {
            return null;
        }
        return userTagsCache.get(key);
    }

    @Trace(dispatcher = true)
    public void setUserTagsCache(String key, List<String> publisherIds) {
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }
        if (MXJudgeUtils.isEmpty(publisherIds)) {
            return;
        }
        try {
            if (null == userTagsCache.get(key)) {
                userTagsCache.put(key, publisherIds);
            }
        } catch (Exception e) {
            String message = String.format("userTagsCache for %s failed, info: %s", key, e.toString());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        }
    }

    @Trace(dispatcher = true)
    public List<String> getSimilarRealVideoIdsCache(String key) {
        if (!Conf.getLocalCacheSwitch()) {
            return null;
        }
        return similarRealVideoIdsCache.get(key);
    }

    @Trace(dispatcher = true)
    public void setSimilarRealVideoIdsCache(String key, List<String> videoIds) {
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }
        if (MXJudgeUtils.isEmpty(videoIds)) {
            return;
        }
        try {
            if (null == similarRealVideoIdsCache.get(key)) {
                similarRealVideoIdsCache.put(key, videoIds);
            }
        } catch (Exception e) {
            String message = String.format("similarRealVideoIdsCache for %s failed, info: %s", key, e.toString());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        }
    }

    @Trace(dispatcher = true)
    public Set<String> getSensitiveWordCache(String key) {
        if (!Conf.getLocalCacheSwitch()) {
            return null;
        }
        return sensitiveWordCache.get(key);
    }

    @Trace(dispatcher = true)
    public void setSensitiveWordCache(String key, Set<String> words) {
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }
        if (MXJudgeUtils.isEmpty(words)) {
            return;
        }
        try {
            if (null == sensitiveWordCache.get(key)) {
                sensitiveWordCache.put(key, words);
            }
        } catch (Exception e) {
            String message = String.format("sensitiveWordCache for %s failed, info: %s", key, e.toString());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        }
    }

    @Trace(dispatcher = true)
    public String getLikeDivideShareCache(String key) {
        if (!Conf.getLocalCacheSwitch()) {
            return null;
        }
        return likeDivideShareCache.get(key);
    }

    @Trace(dispatcher = true)
    public void setLikeDivideShareCache(String key, String ratio) {
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }
        if (MXStringUtils.isEmpty(ratio)) {
            return;
        }
        try {
            if (null == likeDivideShareCache.get(key)) {
                likeDivideShareCache.put(key, ratio);
            }
        } catch (Exception e) {
            String message = String.format("setLikeDivideShareCache for %s failed, info: %s", key, e.toString());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        }
    }

    @Trace(dispatcher = true)
    public String getShareRateCache(String key) {
        if (!Conf.getLocalCacheSwitch()) {
            return null;
        }
        return shareRateCache.get(key);
    }

    @Trace(dispatcher = true)
    public void setShareRateCache(String key, String ratio) {
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }
        if (MXStringUtils.isEmpty(ratio)) {
            return;
        }
        try {
            if (null == shareRateCache.get(key)) {
                shareRateCache.put(key, ratio);
            }
        } catch (Exception e) {
            String message = String.format("setShareRateCache for %s failed, info: %s", key, e.toString());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        }
    }

    @Trace(dispatcher = true)
    public String getMixRatioCache(String key) {
        if (!Conf.getLocalCacheSwitch()) {
            return null;
        }
        return mixRatioCache.get(key);
    }

    @Trace(dispatcher = true)
    public void setMixRatioCache(String key, String ratio) {
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }
        if (MXStringUtils.isEmpty(ratio)) {
            return;
        }
        try {
            if (null == mixRatioCache.get(key)) {
                mixRatioCache.put(key, ratio);
            }
        } catch (Exception e) {
            String message = String.format("setMixRatioCache for %s failed, info: %s", key, e.toString());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        }
    }

    @Trace(dispatcher = true)
    public String getFinishRetentionCache(String key) {
        if (!Conf.getLocalCacheSwitch()) {
            return null;
        }
        return finishRetentionCache.get(key);
    }

    @Trace(dispatcher = true)
    public void setFinishRetentionCache(String key, String ratio) {
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }
        if (MXStringUtils.isEmpty(ratio)) {
            return;
        }
        try {
            if (null == finishRetentionCache.get(key)) {
                finishRetentionCache.put(key, ratio);
            }
        } catch (Exception e) {
            String message = String.format("setFinishRetentionCache for %s failed, info: %s", key, e.toString());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        }
    }

    @Trace(dispatcher = true)
    public List<BaseDocument> getInternalSortedVideoListCache(String key) {
        if (!Conf.getLocalCacheSwitch()) {
            return null;
        }
        return internalSortedVideoListCache.get(key);
    }

    @Trace(dispatcher = true)
    public void setInternalSortedVideoListCache(String key, List<BaseDocument> docList) {
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }
        if (MXJudgeUtils.isEmpty(docList)) {
            return;
        }
        try {
            if (null == internalSortedVideoListCache.get(key)) {
                internalSortedVideoListCache.put(key, docList);
            }
        } catch (Exception e) {
            String message = String.format("setInternalSortedVideoListCache for %s failed, info: %s", key, e.toString());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        }
    }

    @Trace(dispatcher = true)
    public List<BaseDocument> getTagPoolLv3VideoListCache(String key) {
        if (!Conf.getLocalCacheSwitch()) {
            return null;
        }
        return tagPoolLv3VideoListCache.get(key);
    }

    @Trace(dispatcher = true)
    public void setTagPoolLv3VideoListCache(String key, List<BaseDocument> docList) {
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }
        if (MXJudgeUtils.isEmpty(docList)) {
            return;
        }
        try {
            if (null == tagPoolLv3VideoListCache.get(key)) {
                tagPoolLv3VideoListCache.put(key, docList);
            }
        } catch (Exception e) {
            String message = String.format("setTagPoolLv3VideoListCache for %s failed, info: %s\", key, e.toString()");
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        }
    }


    @Trace(dispatcher = true)
    public List<BaseDocument> getAdultPreferenceVideoListCache(String key) {
        if (!Conf.getLocalCacheSwitch()) {
            return null;
        }
        return adultPreferenceVideoListCache.get(key);
    }

    @Trace(dispatcher = true)
    public void setAdultPreferenceVideoListCache(String key, List<BaseDocument> docList) {
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }
        if (MXJudgeUtils.isEmpty(docList)) {
            return;
        }
        try {
            if (null == adultPreferenceVideoListCache.get(key)) {
                adultPreferenceVideoListCache.put(key, docList);
            }
        } catch (Exception e) {
            String message = String.format("setAdultPreferenceVideoListCache for %s failed, info: %s", key, e.toString());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        }
    }

    @Trace(dispatcher = true)
    public List<Result> getMusicCache(String key) {
        if (!Conf.getLocalCacheSwitch()) {
            return null;
        }
        return musicCache.get(key);
    }

    @Trace(dispatcher = true)
    public void setMusicCache(String key, List<Result> resultList) {
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }

        if (MXJudgeUtils.isEmpty(resultList)) {
            return;
        }

        try {
            if (null == musicCache.get(key)) {
                musicCache.put(key, resultList);
            }
        } catch (Exception e) {
            String message = String.format("setMusicCache for %s failed, info: %s", key, e.toString());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        }
    }

    @Trace(dispatcher = true)
    public List<BaseDocument> getIPLCache(String key) {
        if (!Conf.getLocalCacheSwitch()) {
            return null;
        }
        return IPLDocListCache.get(key);
    }

    @Trace(dispatcher = true)
    public void setIPLCache(String key, List<BaseDocument> docList) {
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }
        if (MXJudgeUtils.isEmpty(docList)) {
            return;
        }
        try {
            if (null == IPLDocListCache.get(key)) {
                IPLDocListCache.put(key, docList);
            }
        } catch (Exception e) {
            String message = String.format("setIPLCache for %s failed, info: %s", key, e.toString());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        }
    }

    @Trace(dispatcher = true)
    public List<BaseDocument> getTikiCache(String key) {
        if (!Conf.getLocalCacheSwitch()) {
            return null;
        }
        return tikiDocListCache.get(key);
    }

    @Trace(dispatcher = true)
    public void setTikiCache(String key, List<BaseDocument> docList) {
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }
        if (MXJudgeUtils.isEmpty(docList)) {
            return;
        }
        try {
            if (null == tikiDocListCache.get(key)) {
                tikiDocListCache.put(key, docList);
            }
        } catch (Exception e) {
            String message = String.format("setTikiCache for %s failed, info: %s", key, e.toString());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        }
    }

    @Trace(dispatcher = true)
    public List<LiveDocument> getLiveCache(String key) {
        if (!Conf.getLocalCacheSwitch()) {
            return null;
        }
        return liveCache.get(key);
    }

    @Trace(dispatcher = true)
    public void setLiveCache(String key, List<LiveDocument> resList) {
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }
        if (MXJudgeUtils.isEmpty(resList)) {
            return;
        }
        try {
            if (null == liveCache.get(key)) {
                liveCache.put(key, resList);
            }
        } catch (Exception e) {
            String message = String.format("setLiveCache for %s failed, info: %s", key, e.toString());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        }
    }

    @Trace(dispatcher = true)
    public List<BaseDocument> getPubgDocCache(String key) {
        if (!Conf.getLocalCacheSwitch()) {
            return null;
        }
        return pubgDocCache.get(key);
    }

    @Trace(dispatcher = true)
    public void setPubgDocCache(String key, List<BaseDocument> resList) {
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }
        if (MXJudgeUtils.isEmpty(resList)) {
            return;
        }
        try {
            if (null == pubgDocCache.get(key)) {
                pubgDocCache.put(key, resList);
            }
        } catch (Exception e) {
            String message = String.format("setPubgDocCache for %s failed, info: %s", key, e.toString());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null);
        }
    }

    public void setCmsPubCardPubIdsCache(String key, List<String> pubIds){
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }
        if (MXJudgeUtils.isEmpty(pubIds)) {
            return;
        }
        try {
            if (null == cmsPubCardPubIdsCache.get(key)) {
                cmsPubCardPubIdsCache.put(key, pubIds);
            }
        } catch (Exception e) {
            String message = String.format("setPubgDocCache for %s failed, info: %s", key, e.toString());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null);
        }
    }

    public List<String> getcmsPubCardPubIdsCache(String key){
        if (!Conf.getLocalCacheSwitch()) {
            return null;
        }
        return cmsPubCardPubIdsCache.get(key);
    }

    @Trace(dispatcher = true)
    public void setUserProfileGuaranteeCache(String key, String result) {
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }
        if (MXJudgeUtils.isEmpty(result)) {
            return;
        }
        try {
            if (null == userProfileGuaranteeCache.get(key)) {
                userProfileGuaranteeCache.put(key, result);
            }
        } catch (Exception e) {
            String message = String.format("setUserProfileGuaranteeCache for %s failed, info: %s", key, e.toString());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null);
        }
    }

    @Trace(dispatcher = true)
    public String getUserProfileGuaranteeCache(String key){
        if (!Conf.getLocalCacheSwitch()) {
            return null;
        }
        return userProfileGuaranteeCache.get(key);
    }

    @Trace(dispatcher = true)
    public void setLiveUserCache(String key, Map<String, Long> map) {
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }
        if (MXJudgeUtils.isEmpty(map)) {
            return;
        }
        try {
            if (null == liveUserCache.get(key)) {
                liveUserCache.put(key, map);
            }
        } catch (Exception e) {
            String message = String.format("setLiveUserCache for %s failed, info: %s", key, e.toString());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null);
        }
    }

    @Trace(dispatcher = true)
    public Map<String, Long> getLiveUserCache(String key){
        if (!Conf.getLocalCacheSwitch()) {
            return Collections.emptyMap();
        }
        return liveUserCache.get(key);
    }


    @Trace(dispatcher = true)
    public Map<String, String> getBindAudioMapCache(String key) {
        if (!Conf.getLocalCacheSwitch()) {
            return null;
        }
        return bindAudioMapCache.get(key);
    }

    @Trace(dispatcher = true)
    public void setBindAudioMapCache(String key, Map<String, String> map) {
        if (!Conf.getLocalCacheSwitch()) {
            return;
        }
        if (MXJudgeUtils.isEmpty(map)) {
            return;
        }
        try {
            if (null == bindAudioMapCache.get(key)) {
                bindAudioMapCache.put(key, map);
            }
        } catch (Exception e) {
            String message = String.format("setBindAudioMapCache for %s failed, info: %s", key, e.toString());
            LogTool.printErrorLog(logger, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null);
        }
    }
}