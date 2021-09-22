package mx.j2.recommend.data_model.document;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import mx.j2.recommend.data_model.ClientVersionInfo;
import mx.j2.recommend.data_model.Document;
import mx.j2.recommend.data_model.DocumentTag;
import mx.j2.recommend.data_model.statistics_document.BaseStatisticsDocument;
import mx.j2.recommend.data_source.StatisticDataSource;
import mx.j2.recommend.manager.DataSourceManager;
import mx.j2.recommend.manager.MXDataSource;
import mx.j2.recommend.statistic_conf.StatisticConf;
import mx.j2.recommend.util.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static mx.j2.recommend.util.BaseMagicValueEnum.*;

/**
 * 文档类，这里可以是新闻、视频、图片等
 *
 * @author zhuowei
 */
public abstract class BaseDocument implements Document, Serializable {
    private static final long serialVersionUID = 1010720206045713984L;
    private static Logger log = LogManager.getLogger(BaseDocument.class);

    /**
     * 资源id
     */
    public String id;

    /**
     * 观看数
     */
    public long viewCount;

    /**
     * 分享数
     */
    public long shareCount;

    /**
     * 喜欢数
     */
    public long likeCount;

    /**
     * 下载数
     */
    public long downloadCount;

    /**
     * 下载数
     */
    public long commentCount;

    /**
     * 视频时长
     */
    public long duration;
    /**
     * 音频时长
     */
    public float audioDuration;
    /**
     * 通过QQ上传方式上传视频的宽度
     */
    public int qqMetaWidth;
    /**
     * 通过QQ上传方式上传视频的高度
     */
    public int qqMetaHeight;
    /**
     * 被包在内层的视频时长
     */
    public long innerDuration;
    /**
     * 资源状态
     */
    public int status;

    /**
     * 是否被删除
     */
    public boolean isDelete;

    /**
     * 资源描述
     */
    public String description;

    /**
     * 资源状态
     */
    public String itemType;

    /**
     * 头图链接
     */
    public String thumbnailUrl;

    /**
     * 资源链接
     */
    public String contentUrl;

    /**
     * 头图宽度
     */
    public int thumbnailWidth;

    /**
     * 头图高度
     */
    public int thumbnailHeight;

    /**
     * 召回器标签
     */
    public String recallName;

    /**
     * 召回器拉链标签
     */
    public String recallResultID;

    /**
     * 视频的来源
     */
    public String postFrom;

    /**
     * 此召回的个性化标签
     */
    private String recallTag;

    /**
     * 此召回的个性化数据库表
     */
    private String recallTable;

    /**
     * 资源类别
     */
    public DefineTool.CategoryEnum category;

    /**
     * 视频名称
     */
    public String title;

    /**
     * 视频来源APP Name
     */
    public String appName;

    /**
     * 推荐流名字
     */
    public String recommendFlowName;

    /**
     * 是否是运营标记过
     */
    public int specialSign;

    /**
     * 解密视频标记
     */
    public int decryptSign;

    /**
     * 上传视频标记
     */
    public int uploadSign;

    /**
     * 下载链接
     */
    public String downloadUrl;

    /**
     * 是否是成人内容
     */
    public int isAdult;

    /**
     * (viewCount + shareCount + downloadCount)*10/viewCount
     */
    public float totalScore;

    /**
     * 分数 Document
     */
    public ScoreDocument scoreDocument;

    /**
     * 是否要经过长宽过滤（部分特殊视频不需要长宽过滤）
     */
    public boolean isNoNeedWidthAndHeightFilt;

    /**
     * 视频中性别
     */
    public int gender;

    /**
     * 语言标注
     */
    public String languageId;
    public List<String> languageIdList;

    /**
     * 用户上传
     */
    public boolean isUgc;

    /**
     * 该视频是否是大V发布的
     */
    public boolean isBigV;

    /**
     * 视频是否被举报
     */
    public boolean isReported;

    /**
     * 是否有水印
     */
    public int waterMark;
    /**
     * 每个document的人工标注的tag list
     */
    public List<String> humanTagList;

    /**
     * 文档类别，如美食，体育等等
     */
    public List<String> categories;

    /**
     * 黄反、孩子等标签集合
     */
    public List<DocumentTag> tags;

    /**
     * 上面标签 的String标签
     */
    public Set<String> tagString;

    /**
     * 区分来源, 爬虫还是真实数据
     */
    public String videoSource;

    /**
     * 每个document的人工标注的tag list
     */
    public String publisher_id;

    /**
     * 国家
     */
    public List<String> countries;

    /**
     * 是否是黄色的 {0,1}
     */
    private long isPorn;

    /**
     * 是否是暴力的 {0,1}
     */
    private long isTerrorism;

    /**
     * 是否是政治的 {0,1}
     */
    private long isPolitical;

    /**
     * 是否是广告视频
     */
    public int isAdVideo;

    /**
     * 是否是机器人发布的视频
     */
    public int isRobot;

    /**
     * 人工打分
     */
    private float humanScore;

    /**
     * 是否是 original audio video
     */
    public int isOriginalAudio;

    /**
     * 上线时间
     */
    public long onlineTime;

    /**
     * 在publisher页的顺序
     */
    public int order;

    /**
     * 有些召回器会召回多个池子，这里做个标记
     */
    public String recallPoolName;

    /**
     * 根据地理位置召回的距离
     */
    public double distance;

    /**
     * 需要的onlineTime粒度
     */
    public long onlineTimeNeed;

    /**
     * 热度和online_time的综合分数
     */
    public double multipleScore;

    /**
     * heatScore 热度分数，用于autio页排序
     */
    public float heatScore;


    /**
     * heatScore2 热度分数，用于autio页排序
     */
    public double heatScore2;

    /**
     * hashtagHeat热度分数
     */
    public double hashtagHeat;

    /**
     * 置顶单独历史
     */
    public boolean isTopHotHistory;

    /**
     * 视频观看权限 0: Public; 1: Friends; 2: Private
     */
    public int viewPrivacy;

    /**
     * 账户是否是隐私账户 0：普通账户; 1：隐私账户
     */
    public int privateAccount;

    /**
     * 该视频是不是重复
     */
    public int duplicated;
    /**
     * 当前视频清晰度的分数（小于20算清晰）
     */
    public float niqeScore;

    /**
     * 是否是原创，由extraInfo的publisher_id推断
     */
    public boolean isNotOriginal;

    /**
     * Description 中是否含有敏感词
     */
    public int badDescription;

    /**
     * 是否是用户follow的，TODO 不能缓存这个字段
     */
    public boolean isFollowed;

    /**
     * 来自腾讯sdk视频
     */
    public int fromTecent;
    public StatisticsDocument statisticsDocument;
    public int likeInfoStatus;

    /**
     * 字段的存在性判断
     */
    public static final long FIELD_EXISTS_FLAG_IS_PORN = 1;
    public static final long FIELD_EXISTS_FLAG_IS_TERRORISM = 1 << 1;
    public static final long FIELD_EXISTS_FLAG_IS_POLITICAL = 1 << 2;
    public static final long FIELD_EXISTS_FLAG_HUMAN_SCORE = 1 << 3;
    public static final long FIELD_EXISTS_FLAG_APPEAL_STATUS = 1 << 4;
    public static final long FIELD_EXISTS_FLAG_IS_DISABLED = 1 << 5;
    public static final long FIELD_EXISTS_FLAG_IS_REPORTED = 1 << 6;
    private long fieldExistsFlags;

    /**
     * 支持的客户端最小版本
     */
    public ClientVersionInfo clientVersionInfo;

    /**
     * ml_tag标签
     */
    public Set<String> mlTags;

    // 目前只有 debug 使用，简单存储一下（现在real_action接口也用了）
    public JSONArray primaryTags;
    public JSONArray secondaryTags;

    public String durationLevel;

    public Set<String> descTag;

    public SageMakerVideoFeatureDocument sageMakerVideoFeatureDocument;

    public String feature;

    private int appealStatus;

    private boolean isDisabled;

    private int priority;

    private int tagScore;

    public int universal;

    public String poolLevel;

    public String poolIndex;

    private int poolPriority;

    private int strategyPoolPriority;

    private List<String> retainTags;

    /**
     * 直播间数据
     */
    private LiveDocument LiveInfo;

    /**
     * 内部接口所需原始数据，务必不要再正常逻辑中使用
     */
    public String mlTagsForInternal;

    /**
     * 内部接口所需原始数据，务必不要再正常逻辑中使用
     */
    public String bigHeadForInternal;

    /**
     * 内部接口所需原始数据，务必不要再正常逻辑中使用
     */
    public String likeInfoForInternal;

    /**
     * 内部接口所需原始数据，务必不要再正常逻辑中使用
     */
    public String featStat30dForInternal;

    /**
     * 内部接口所需原始数据，务必不要再正常逻辑中使用
     */
    public String featStat0dForInternal;

    /**
     * 是否是ipl视频
     */
    public boolean isIpl;

    /**
     * ipl视频人工审核状态
     */
    public int humanReviewStatus;

    /**
     * 经过计算后得到的排序权重值
     */
    public double realTimeSortedWeighted;

    /**
     * Key: 接口名， Value: 已经拼好的nextToken。考虑到缓存没有使用深拷贝，所有用不同接口名字区分nextToken
     */
    public Map<String, String> nextTokenMap;

    /**
     * 视频的原始宽度
     */
    public int originalWidth;

    /**
     * 视频的原始高度
     */
    public int originalHeight;

    /**
     * 构造函数
     */
    public BaseDocument() {
        init();
    }

    /**
     * 初始化函数
     */
    private void init() {
        id = STRING_INITIAL_VALUE;
        viewCount = LONG_INITIAL_VALUE;
        shareCount = LONG_INITIAL_VALUE;
        likeCount = LONG_INITIAL_VALUE;
        downloadCount = LONG_INITIAL_VALUE;
        commentCount = LONG_INITIAL_VALUE;
        duration = LONG_INITIAL_VALUE;
        innerDuration = LONG_INITIAL_VALUE;
        status = INT_INITIAL_VALUE;
        description = STRING_INITIAL_VALUE;
        itemType = STRING_INITIAL_VALUE;
        thumbnailUrl = STRING_INITIAL_VALUE;
        contentUrl = STRING_INITIAL_VALUE;
        recallName = STRING_INITIAL_VALUE;
        recallResultID = STRING_INITIAL_VALUE;
        postFrom = STRING_INITIAL_VALUE;
        recallTag = STRING_INITIAL_VALUE;
        recallTable = STRING_INITIAL_VALUE;
        recommendFlowName = STRING_INITIAL_VALUE;
        category = DefineTool.CategoryEnum.DEFAULT;
        title = STRING_INITIAL_VALUE;
        totalScore = FLOAT_INITIAL_VALUE;
        thumbnailWidth = INT_INITIAL_VALUE;
        thumbnailHeight = INT_INITIAL_VALUE;
        specialSign = INT_INITIAL_VALUE;
        decryptSign = INT_INITIAL_VALUE;
        uploadSign = INT_INITIAL_VALUE;
        downloadUrl = STRING_INITIAL_VALUE;
        isAdult = INT_INITIAL_VALUE;
        gender = INT_INITIAL_VALUE;
        appName = STRING_INITIAL_VALUE;
        languageId = STRING_INITIAL_VALUE;
        isNoNeedWidthAndHeightFilt = false;
        scoreDocument = new ScoreDocument();
        isUgc = false;
        humanTagList = new ArrayList<>();
        categories = new ArrayList<>();
        tags = new ArrayList<>();
        videoSource = STRING_INITIAL_VALUE;
        publisher_id = STRING_INITIAL_VALUE;
        countries = new ArrayList<>();
        recallPoolName = STRING_INITIAL_VALUE;
        heatScore = FLOAT_INITIAL_VALUE;
        hashtagHeat = DOUBLE_INITIAL_VALUE;
        isTopHotHistory = false;
        isBigV = false;
        isReported = false;
        distance = DOUBLE_INITIAL_VALUE;
        isAdVideo = INT_INITIAL_VALUE;
        viewPrivacy = INT_INITIAL_VALUE;
        isRobot = INT_INITIAL_VALUE;
        multipleScore = DOUBLE_INITIAL_VALUE;
        privateAccount = INT_INITIAL_VALUE;
        mlTags = new HashSet<>();
        primaryTags = new JSONArray();
        secondaryTags = new JSONArray();
        descTag = new HashSet<>();
        tagString = new HashSet<>();
        statisticsDocument = new StatisticsDocument();
        durationLevel = STRING_INITIAL_VALUE;
        waterMark = INT_INITIAL_VALUE;
        audioDuration = FLOAT_INITIAL_VALUE;
        qqMetaWidth = INT_INITIAL_VALUE;
        qqMetaHeight = INT_INITIAL_VALUE;
        duplicated = INT_INITIAL_VALUE;
        niqeScore = FLOAT_INITIAL_VALUE;
        fromTecent = INT_INITIAL_VALUE;
        onlineTimeNeed = LONG_INITIAL_VALUE;
        this.sageMakerVideoFeatureDocument = new SageMakerVideoFeatureDocument();
        feature = STRING_INITIAL_VALUE;
        appealStatus = INT_INITIAL_VALUE;
        universal = INT_INITIAL_VALUE;
        isDisabled = false;
        isNotOriginal = false;
        priority = INT_INITIAL_VALUE;
        tagScore = INT_INITIAL_VALUE;
        poolLevel = STRING_INITIAL_VALUE;
        badDescription = INT_INITIAL_VALUE;
        languageIdList = new ArrayList<>();
        poolPriority = INT_INITIAL_VALUE;
        likeInfoStatus = INT_INITIAL_VALUE;
        strategyPoolPriority = INT_INITIAL_VALUE;
        isIpl = false;
        humanReviewStatus = INT_INITIAL_VALUE;
        realTimeSortedWeighted = DOUBLE_INITIAL_VALUE;
        isDelete = false;
        nextTokenMap = new ConcurrentHashMap<>();
        retainTags = new ArrayList<>();
        originalWidth = INT_INITIAL_VALUE;
        originalHeight = INT_INITIAL_VALUE;
    }

    /**
     * 判断字段是否存在
     */
    public boolean exists(long flag) {
        return (fieldExistsFlags & flag) != 0;
    }

    /**
     * 解析召回结构为Document形式
     */
    public void loadJsonObj(JSONObject source, DefineTool.CategoryEnum category, String recallName, BaseDocument baseDocument) {
        if (!source.containsKey(METADATA_ID) && !source.containsKey(VIDEO_ID) && DefineTool.CategoryEnum.SHORT_VIDEO.equals(category)) {
            return;
        }

        baseDocument.recallName = recallName;
        baseDocument.category = category;
        try {
            if (source.containsKey(METADATA_ID)) {
                baseDocument.id = source.getString(METADATA_ID);
            }

            if (source.containsKey(VIDEO_ID)) {
                baseDocument.id = source.getString(VIDEO_ID);
            }

            if (source.containsKey(VIEW_COUNT)) {
                baseDocument.viewCount = source.getLongValue(VIEW_COUNT);
            }

            if (source.containsKey(SHARE_COUNT)) {
                baseDocument.shareCount = source.getLongValue(SHARE_COUNT);
            }

            if (source.containsKey(LIKE_COUNT)) {
                baseDocument.likeCount = source.getLongValue(LIKE_COUNT);
            }

            if (source.containsKey(DOWNLOAD_COUNT)) {
                baseDocument.downloadCount = source.getLongValue(DOWNLOAD_COUNT);
            }

            if (source.containsKey(STATUS)) {
                baseDocument.status = source.getIntValue(STATUS);
            }

            if (source.containsKey(DESCRIPTION)) {
                baseDocument.description = source.getString(DESCRIPTION);
            }

            if (source.containsKey(THUMB_URL)) {
                baseDocument.thumbnailUrl = source.getString(THUMB_URL);
            }

            if (source.containsKey(DOWNLOAD_URL)) {
                baseDocument.downloadUrl = source.getString(DOWNLOAD_URL);
            }

            if (source.containsKey(TITLE)) {
                baseDocument.title = source.getString(TITLE);
            }

            if (source.containsKey(SPECIAL_SIGN)) {
                baseDocument.specialSign = source.getIntValue(SPECIAL_SIGN);
            }

            if (source.containsKey(DECRYPT_SIGN)) {
                baseDocument.decryptSign = source.getIntValue(DECRYPT_SIGN);
            }

            if (source.containsKey(UPLOADS_SIGN)) {
                baseDocument.uploadSign = source.getIntValue(UPLOADS_SIGN);
            }

            if (source.containsKey(IS_ADULT)) {
                baseDocument.isAdult = source.getIntValue(IS_ADULT);
            }

            if (source.containsKey(COMMENT_COUNT)) {
                baseDocument.commentCount = source.getLongValue(COMMENT_COUNT);
            }

            if (source.containsKey(GENDER)) {
                baseDocument.gender = source.getIntValue(GENDER);
            }

            if (source.containsKey(APP_NAME)) {
                baseDocument.appName = source.getString(APP_NAME);
            }

            if (source.containsKey(ITEM_TYPE)) {
                baseDocument.itemType = source.getString(ITEM_TYPE);
            }

            if (source.containsKey(LANGUAGE_ID)) {
                baseDocument.languageId = source.getString(LANGUAGE_ID);
            }
            if (source.containsKey(LANGUAGE_ID_LIST)) {
                String languageIds = source.getString(LANGUAGE_ID_LIST);
                if (MXStringUtils.isNotBlank(languageIds)) {
                    try {
                        baseDocument.languageIdList = JSONArray.parseArray(languageIds).toJavaList(String.class);
                    } catch (Exception e) {
                        log.error("baseDocument.languageIdList error", e);
                    }
                }
            }

            if (source.containsKey(PUBLISHER_ID)) {
                baseDocument.publisher_id = source.getString(PUBLISHER_ID);
                if (MXDataSource.verify().isBigV(source.getString(PUBLISHER_ID))) {
                    baseDocument.isBigV = true;
                }
            }

            if (source.containsKey(ML_TAGS)) {
                JSONObject mlTagsObj = source.getJSONObject(ML_TAGS);
                if (MXJudgeUtils.isNotEmpty(mlTagsObj)) {
                    baseDocument.mlTags.addAll(mlTagsObj.keySet());
                }
            }
            if (source.containsKey(PRIMARY_TAGS)) {
                JSONArray jsonArray = source.getJSONArray(PRIMARY_TAGS);
                if (MXJudgeUtils.isNotEmpty(jsonArray)) {
                    baseDocument.primaryTags = jsonArray;
                }
            }

            if (source.containsKey(SECONDARY_TAGS)) {
                JSONArray jsonArray = source.getJSONArray(SECONDARY_TAGS);
                if (MXJudgeUtils.isNotEmpty(jsonArray)) {
                    baseDocument.secondaryTags = jsonArray;
                }
            }

            if (source.containsKey(FEATURE30D)) {
                JSONObject feature30d = source.getJSONObject(FEATURE30D);
                OptionalUtil.ofNullable(feature30d)
                        .ifPresent(obj -> {
                            if (obj.containsKey("like_rate_30d")) {
                                this.statisticsDocument.setLikeRate30d(obj.getDoubleValue("like_rate_30d"));
                            }
                            if (obj.containsKey("download_rate_30d")) {
                                this.statisticsDocument.setDownloadRate30d(obj.getDoubleValue("download_rate_30d"));
                            }
                            if (obj.containsKey("share_rate_30d")) {
                                this.statisticsDocument.setShareRate30d(obj.getDoubleValue("share_rate_30d"));
                            }
                            if (obj.containsKey("finish_rate_30d")) {
                                this.statisticsDocument.setFinishedRate30d(obj.getDoubleValue("finish_rate_30d"));
                            }
                            if (obj.containsKey("play_rate_30d")) {
                                this.statisticsDocument.setPlayRate30d(obj.getDoubleValue("play_rate_30d"));
                            }
                            if (obj.containsKey(FINISH_RETENTION_SUM_10S_30D)) {
                                this.statisticsDocument.setFinishRetentionSum10s30d(obj.getDoubleValue(FINISH_RETENTION_SUM_10S_30D));
                            }
                            if (obj.containsKey("view_all_30d")) {
                                this.statisticsDocument.setViewAll30d(obj.getDoubleValue("view_all_30d"));
                            }
                            if (obj.containsKey("avg_playtime_30d")) {
                                this.statisticsDocument.setAvgPlaytime30d(obj.getDoubleValue("avg_playtime_30d"));
                            }
                            if (obj.containsKey("finish_rate_5s_cut_30d")) {
                                this.statisticsDocument.setFinishRate5sCut30d(obj.getDoubleValue("finish_rate_5s_cut_30d"));
                            }
                        });
            }

            if (source.containsKey(FEATURE3D)) {
                JSONObject feature3d = source.getJSONObject(FEATURE3D);
                OptionalUtil.ofNullable(feature3d).ifPresent(obj -> {
                    if (obj.containsKey("share_rate_3d")) {
                        this.statisticsDocument.setShareRate3d(obj.getDoubleValue("share_rate_3d"));
                    }
                    if (obj.containsKey("view_all_3d")) {
                        this.statisticsDocument.setViewAll3d(obj.getIntValue("view_all_3d"));
                    }
                    if (obj.containsKey("download_rate_3d")) {
                        this.statisticsDocument.setDownloadRate3d(obj.getDoubleValue("download_rate_3d"));
                    }
                    this.statisticsDocument.setLoadSuccess(true);
                });
            }

            if (source.containsKey(FEATURE0D)) {
                JSONObject feature0d = source.getJSONObject(FEATURE0D);
                OptionalUtil.ofNullable(feature0d).ifPresent(obj -> {
                    if (obj.containsKey("like_rate_0d")) {
                        this.statisticsDocument.setLikeRate0d(obj.getDoubleValue("like_rate_0d"));
                    }
                });
            }

            fillStatistic(source);

            if (source.containsKey(COUNTRYS_NAME)) {
                String countries = source.getString(COUNTRYS_NAME);
                if (MXStringUtils.isNotBlank(countries)) {
                    try {
                        baseDocument.countries = JSONArray.parseArray(countries).toJavaList(String.class);
                    } catch (Exception e) {
                        log.error("baseDocument.countries is error", e);
                    }
                }
            }
            try {
                if (source.containsKey(THUMBNAIL_INFO)) {
                    JSONObject thumbnailInfo = source.getJSONObject(THUMBNAIL_INFO);

                    if (thumbnailInfo.containsKey(THUMBNAIL_WIDTH)) {
                        this.thumbnailWidth = thumbnailInfo.getIntValue(THUMBNAIL_WIDTH);
                    }

                    if (thumbnailInfo.containsKey(THUMBNAIL_HEIGHT)) {
                        this.thumbnailHeight = thumbnailInfo.getIntValue(THUMBNAIL_HEIGHT);
                    }
                }
            } catch (Exception e) {
                log.error(THUMBNAIL_INFO + " error, message->" + e);
            }

            if (source.containsKey(HUMAN_TAGS_STRING)) {
                String humanTagString = source.getString(HUMAN_TAGS_STRING);
                if (MXJudgeUtils.isNotEmpty(humanTagString)) {
                    try {
                        baseDocument.humanTagList = JSONArray.parseArray(humanTagString).toJavaList(String.class);
                    } catch (Exception e) {
                        log.error("Doc id is " + baseDocument.id + " Human Tags Error Message is " + e.fillInStackTrace().toString());
                    }
                }
            }

            try {
                categories = MXJsonUtils.getStringListValue(source, CATEGORIES);
            } catch (Exception e) {
                log.error("Doc id is " + baseDocument.id + " Categories Error Message is " + e.fillInStackTrace().toString());
            }

            if (source.containsKey(TAGS)) {
                try {
                    JSONArray array = source.getJSONArray(TAGS);
                    baseDocument.tags = array.toJavaList(DocumentTag.class);
                } catch (Exception e) {
                    log.error("Doc id is " + baseDocument.id + " Tags Error Message is " + e.fillInStackTrace().toString());
                }
            }

            if (source.containsKey(HASH_TAG_STRING)) {
                JSONArray mlTagsObj = source.getJSONArray(HASH_TAG_STRING);
                if (MXJudgeUtils.isNotEmpty(mlTagsObj)) {
                    baseDocument.descTag.addAll(mlTagsObj.toJavaList(String.class));
                }
            }

            if (source.containsKey(IS_UGC_CONTENT)) {
                baseDocument.isUgc = source.getBooleanValue(IS_UGC_CONTENT);
            }
            if (source.containsKey(HUMAN_SCORE)) {
                baseDocument.humanScore = source.getFloatValue(HUMAN_SCORE);
                fieldExistsFlags |= FIELD_EXISTS_FLAG_HUMAN_SCORE;
            }

            if (source.containsKey(IS_PORN)) {
                baseDocument.isPorn = source.getLongValue(IS_PORN);
                fieldExistsFlags |= FIELD_EXISTS_FLAG_IS_PORN;
            }

            if (source.containsKey(IS_TERRORISM)) {
                baseDocument.isTerrorism = source.getLongValue(IS_TERRORISM);
                fieldExistsFlags |= FIELD_EXISTS_FLAG_IS_TERRORISM;
            }

            if (source.containsKey(IS_POLITICAL)) {
                baseDocument.isPolitical = source.getLongValue(IS_POLITICAL);
                fieldExistsFlags |= FIELD_EXISTS_FLAG_IS_POLITICAL;
            }

            if (source.containsKey(IS_REPORTED)) {
                baseDocument.isReported = source.getBooleanValue(IS_REPORTED);
                fieldExistsFlags |= FIELD_EXISTS_FLAG_IS_REPORTED;
            }

            if (source.containsKey(ONLINE_TIME)) {
                baseDocument.onlineTime = source.getLong(ONLINE_TIME);
            }

            if (source.containsKey(IS_AD_VIDEO)) {
                baseDocument.isAdVideo = source.getIntValue(IS_AD_VIDEO);
            }

            if (source.containsKey(IS_ROBOT)) {
                baseDocument.isRobot = source.getIntValue(IS_ROBOT);
            }

            if (source.containsKey(CLIENT_VERSION_INFO)) {
                JSONObject clientVersionInfoObject = source.getJSONObject("client_version_info");
                if (clientVersionInfoObject != null) {
                    clientVersionInfo = new ClientVersionInfo();

                    if (clientVersionInfoObject.containsKey("android")) {
                        int androidVersion = -1;
                        try {
                            androidVersion = clientVersionInfoObject.getIntValue("android");
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        clientVersionInfo.setAndroid(androidVersion);
                    }

                    if (clientVersionInfoObject.containsKey("ios")) {
                        String iosVersion = "";
                        try {
                            iosVersion = clientVersionInfoObject.getString("ios");
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        clientVersionInfo.setIos(iosVersion);
                    }
                }
            }
            if (source.containsKey(VIEW_PRIVACY)) {
                baseDocument.viewPrivacy = source.getIntValue(VIEW_PRIVACY);
            }
            if (source.containsKey(PERMISSION)) {
                JSONObject object = source.getJSONObject(PERMISSION);
                if (MXCollectionUtils.isNotEmpty(object)) {
                    if (object.containsKey("view_privacy")) {
                        baseDocument.viewPrivacy = object.getIntValue("view_privacy");
                    }
                }
            }

            if (source.containsKey(PRIVATE_ACCOUNT)) {
                baseDocument.privateAccount = source.getIntValue(PRIVATE_ACCOUNT);
            }
            if (source.containsKey("upload_message_from")) {
                if ("QQ".equals(source.getString("upload_message_from"))) {
                    baseDocument.fromTecent = 1;
                }
            }
            if (source.containsKey(QQ_METADATA)) {
                JSONObject metadataObject = source.getJSONObject(QQ_METADATA);
                try {
                    if (metadataObject.containsKey("AudioDuration")) {
                        baseDocument.audioDuration = metadataObject.getFloatValue("AudioDuration");
                    }
                    if (metadataObject.containsKey("Height")) {
                        baseDocument.qqMetaHeight = metadataObject.getIntValue("Height");
                    }
                    if (metadataObject.containsKey("Width")) {
                        baseDocument.qqMetaWidth = metadataObject.getIntValue("Width");
                    }
                } catch (Exception e) {
                    log.error("AudioDuration is error", e);
                }
            }
            if (source.containsKey(WATERMARK)) {
                try {
                    baseDocument.waterMark = source.getIntValue(WATERMARK);
                } catch (Exception e) {
                    log.error("watermark is error", e);
                }
            }
            if (source.containsKey(IS_DUPLICATED)) {
                try {
                    baseDocument.duplicated = source.getIntValue(IS_DUPLICATED);
                } catch (Exception e) {
                    log.error("duplicated is error", e);
                }
            }
            if (source.containsKey(NIQE_SCORE)) {
                try {
                    baseDocument.niqeScore = source.getFloatValue(NIQE_SCORE);
                } catch (Exception e) {
                    log.error("niqeScore is error", e);
                }
            }
            // tran tags to Set<String>
            if (MXJudgeUtils.isNotEmpty(baseDocument.tags)) {
                for (DocumentTag docTag : baseDocument.tags) {
                    tagString.add(docTag.name.toLowerCase());
                }
            }

            if (source.containsKey(APPEAL_STATUS)) {
                try {
                    baseDocument.appealStatus = source.getIntValue(APPEAL_STATUS);
                    fieldExistsFlags |= FIELD_EXISTS_FLAG_APPEAL_STATUS;
                } catch (Exception e) {
                    LogTool.printErrorLog(log, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), e.getMessage(), null, source);
                }
            }
            if (source.containsKey(IS_DISABLED)) {
                try {
                    baseDocument.isDisabled = source.getBooleanValue(IS_DISABLED);
                    fieldExistsFlags |= FIELD_EXISTS_FLAG_IS_DISABLED;
                } catch (Exception e) {
                    LogTool.printErrorLog(log, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), e.getMessage(), null, source);
                }
            }

            this.likeCount = calculateLike();

            if (source.containsKey(BAD_DESCRIPTION)) {
                try {
                    baseDocument.badDescription = source.getIntValue(BAD_DESCRIPTION);
                } catch (Exception e) {
                    LogTool.printErrorLog(log, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), e.getMessage(), null, source);
                }
            }
            if (source.containsKey(LIKE_INFO)) {
                try {
                    JSONObject object = source.getJSONObject(LIKE_INFO);
                    if (object.containsKey("status")) {
                        this.likeInfoStatus = object.getIntValue("status");
                    }
                } catch (Exception e) {
                    LogTool.printErrorLog(log, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), e.getMessage(), null, source);
                }
            }

            if (source.containsKey(ML_TAGS)) {
                baseDocument.mlTagsForInternal = source.getString(ML_TAGS);
            }
            if (source.containsKey(BIG_HEAD)) {
                baseDocument.bigHeadForInternal = source.getString(BIG_HEAD);
            }
            if (source.containsKey(LIKE_INFO)) {
                JSONObject likeInfoObject = source.getJSONObject(LIKE_INFO);
                if (MXJudgeUtils.isNotEmpty(likeInfoObject)) {
                    baseDocument.likeInfoForInternal = likeInfoObject.toJSONString();
                }
            }
            if (source.containsKey(FEATURE30D)) {
                baseDocument.featStat30dForInternal = source.getString(FEATURE30D);
            }
            if (source.containsKey(FEATURE0D)) {
                baseDocument.featStat0dForInternal = source.getString(FEATURE0D);
            }

            if (source.containsKey(IS_IPL)) {
                baseDocument.isIpl = source.getBooleanValue(IS_IPL);
            }
            if (source.containsKey(HUMAN_REVIEW_STATUS)) {
                baseDocument.humanReviewStatus = source.getIntValue(HUMAN_REVIEW_STATUS);
            }

            if (source.containsKey(IS_DELETE)) {
                baseDocument.isDelete = source.getBooleanValue(IS_DELETE);
            }
        } catch (Exception x) {
            log.error("Document detail " + baseDocument);
            x.printStackTrace();
            String message = String.format("load document failed -> %s", source.toString());
            LogTool.printErrorLog(log, DefineTool.ErrorNoEnum.NORMAL_ERROR.getErrorNo(), message, null, null);
        }
    }

    public void fillStatistic(JSONObject source) {
        StatisticDataSource dataSource = DataSourceManager.INSTANCE.getStatisticDataSource();
        Map<String, StatisticConf> map = dataSource.getStatisticConfMap();
        if (null == map || map.isEmpty()) {
            return;
        }

        for (Map.Entry<String, StatisticConf> entry : map.entrySet()) {
            if (!source.containsKey(entry.getKey())) {
                continue;
            }

            JSONObject feature = source.getJSONObject(entry.getKey());
            BaseStatisticsDocument doc = new BaseStatisticsDocument();
            OptionalUtil.ofNullable(feature)
                    .ifPresent(obj -> {
                        StatisticConf conf = entry.getValue();
                        if (null == conf || conf.getIndexStringList().isEmpty()) {
                            return;
                        }

                        boolean isSetSuccess = false;
                        for (String index : conf.getIndexStringList()) {
                            if (!feature.containsKey(index)) {
                                continue;
                            }

                            String o = index.replace(String.format("_%s", conf.getSuffix()), "");
                            String fieldName = dataSource.getFieldName(o);
                            if (null == fieldName) {
                                continue;
                            }
                            try {
                                Field field = BaseStatisticsDocument.class.getDeclaredField(fieldName);
                                field.setAccessible(true);
                                field.setDouble(doc, feature.getDoubleValue(index));
                                isSetSuccess = true;
                            } catch (NoSuchFieldException | IllegalAccessException e) {
                                e.printStackTrace();
                            }
                        }

                        if (isSetSuccess) {
                            this.statisticsDocument.put(entry.getKey(), doc);
                            this.statisticsDocument.setLoadSuccess(true);
                        }
                    });
        }
    }

    static String getStringValue(JSONObject source, String key) {
        return getValue(source, key, String.class, "");
    }

    public static List<String> getStrListValue(JSONObject source, String key) {
        String strList = getStringValue(source, key);

        if (MXStringUtils.isNotBlank(strList)) {
            try {
                return JSONArray.parseArray(strList).toJavaList(String.class);
            } catch (Exception e) {
                log.error("Error document field " + key, e);
            }
        }

        return null;
    }

    /**
     * 获取字段值的通用模板方法
     */
    private static <T> T getValue(JSONObject source, String key, Class<T> clazz, T defaultValue) {
        if (source.containsKey(key)) {
            return source.getObject(key, clazz);
        }
        return defaultValue;
    }

    public String getTag() {
        if (MXJudgeUtils.isNotEmpty(humanTagList)) {
            return humanTagList.get(0);
        } else {
            return "None";
        }
    }

    public String mlTagToString() {
        if (MXJudgeUtils.isNotEmpty(mlTags)) {
            return mlTags.toString();
        } else {
            return "None";
        }
    }

    private int calculateLike() {
        if (!this.statisticsDocument.exist(FEATURE30D)) {
            return 0;
        }
        BaseStatisticsDocument doc = this.statisticsDocument.get(FEATURE30D);
        return (int) (doc.getLikeRate() * doc.getViewAll());
    }

    public void setDescTag(Set<String> descTag) {
        this.descTag = descTag;
    }

    public Set<String> getDescTag() {
        return descTag;
    }

    public void setTagString(Set<String> tagString) {
        this.tagString = tagString;
    }

    public Set<String> getTagString() {
        return tagString;
    }

    public void setMlTags(Set<String> tags) {
        mlTags = new HashSet<>(tags);
    }

    public Set<String> getMlTags() {
        return mlTags;
    }

    public JSONArray getPrimaryTags() {
        return primaryTags;
    }

    public void setPrimaryTags(JSONArray primaryTags) {
        this.primaryTags = primaryTags;
    }

    public JSONArray getSecondaryTags() {
        return secondaryTags;
    }

    public void setSecondaryTags(JSONArray secondaryTags) {
        this.secondaryTags = secondaryTags;
    }

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public static Logger getLog() {
        return log;
    }

    public String getId() {
        return id;
    }

    public long getViewCount() {
        return viewCount;
    }

    public long getShareCount() {
        return shareCount;
    }

    public long getLikeCount() {
        return likeCount;
    }

    public long getDownloadCount() {
        return downloadCount;
    }

    public long getCommentCount() {
        return commentCount;
    }

    public long getDuration() {
        return duration;
    }

    public long getInnerDuration() {
        return innerDuration;
    }

    public int getStatus() {
        return status;
    }

    public String getDescription() {
        return description;
    }

    public String getItemType() {
        return itemType;
    }

    public String getThumbnailUrl() {
        return thumbnailUrl;
    }

    public String getContentUrl() {
        return contentUrl;
    }

    public int getThumbnailWidth() {
        return thumbnailWidth;
    }

    public int getThumbnailHeight() {
        return thumbnailHeight;
    }

    public String getRecallName() {
        return recallName;
    }

    public String getRecallResultID() {
        return recallResultID;
    }

    public String getPostFrom() {
        return postFrom;
    }

    public String getRecallTag() {
        return recallTag;
    }

    public String getRecallTable() {
        return recallTable;
    }

    public DefineTool.CategoryEnum getCategory() {
        return category;
    }

    public String getTitle() {
        return title;
    }

    public String getAppName() {
        return appName;
    }

    public String getRecommendFlowName() {
        return recommendFlowName;
    }

    public int getSpecialSign() {
        return specialSign;
    }

    public int getDecryptSign() {
        return decryptSign;
    }

    public int getUploadSign() {
        return uploadSign;
    }

    public String getDownloadUrl() {
        return downloadUrl;
    }

    public int getIsAdult() {
        return isAdult;
    }

    public float getTotalScore() {
        return totalScore;
    }

    public ScoreDocument getScoreDocument() {
        return scoreDocument;
    }

    public boolean isNoNeedWidthAndHeightFilt() {
        return isNoNeedWidthAndHeightFilt;
    }

    public int getGender() {
        return gender;
    }

    public String getLanguageId() {
        return languageId;
    }

    public boolean isUgc() {
        return isUgc;
    }

    public List<String> getHumanTagList() {
        return humanTagList;
    }

    public List<String> getCategories() {
        return categories;
    }

    public String getVideoSource() {
        return videoSource;
    }

    public String getPublisher_id() {
        return publisher_id;
    }

    public List<String> getCountries() {
        return countries;
    }

    public static void setLog(Logger log) {
        BaseDocument.log = log;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setViewCount(long viewCount) {
        this.viewCount = viewCount;
    }

    public void setShareCount(long shareCount) {
        this.shareCount = shareCount;
    }

    public void setLikeCount(long likeCount) {
        this.likeCount = likeCount;
    }

    public void setDownloadCount(long downloadCount) {
        this.downloadCount = downloadCount;
    }

    public void setCommentCount(long commentCount) {
        this.commentCount = commentCount;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }

    public void setInnerDuration(long innerDuration) {
        this.innerDuration = innerDuration;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void setItemType(String itemType) {
        this.itemType = itemType;
    }

    public void setThumbnailUrl(String thumbnailUrl) {
        this.thumbnailUrl = thumbnailUrl;
    }

    public void setContentUrl(String contentUrl) {
        this.contentUrl = contentUrl;
    }

    public void setThumbnailWidth(int thumbnailWidth) {
        this.thumbnailWidth = thumbnailWidth;
    }

    public void setThumbnailHeight(int thumbnailHeight) {
        this.thumbnailHeight = thumbnailHeight;
    }

    public void setRecallName(String recallName) {
        this.recallName = recallName;
    }

    public void setRecallResultID(String recallResultID) {
        this.recallResultID = recallResultID;
    }

    public void setPostFrom(String postFrom) {
        this.postFrom = postFrom;
    }

    public void setRecallTag(String recallTag) {
        this.recallTag = recallTag;
    }

    public void setRecallTable(String recallTable) {
        this.recallTable = recallTable;
    }

    public void setCategory(DefineTool.CategoryEnum category) {
        this.category = category;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public void setRecommendFlowName(String recommendFlowName) {
        this.recommendFlowName = recommendFlowName;
    }

    public void setSpecialSign(int specialSign) {
        this.specialSign = specialSign;
    }

    public void setDecryptSign(int decryptSign) {
        this.decryptSign = decryptSign;
    }

    public void setUploadSign(int uploadSign) {
        this.uploadSign = uploadSign;
    }

    public void setDownloadUrl(String downloadUrl) {
        this.downloadUrl = downloadUrl;
    }

    public void setIsAdult(int isAdult) {
        this.isAdult = isAdult;
    }

    public void setTotalScore(float totalScore) {
        this.totalScore = totalScore;
    }

    public void setScoreDocument(ScoreDocument scoreDocument) {
        this.scoreDocument = scoreDocument;
    }

    public void setNoNeedWidthAndHeightFilt(boolean noNeedWidthAndHeightFilt) {
        isNoNeedWidthAndHeightFilt = noNeedWidthAndHeightFilt;
    }

    public void setGender(int gender) {
        this.gender = gender;
    }

    public void setLanguageId(String languageId) {
        this.languageId = languageId;
    }

    public void setUgc(boolean ugc) {
        isUgc = ugc;
    }

    public void setHumanTagList(List<String> humanTagList) {
        this.humanTagList = humanTagList;
    }

    public void setCategories(List<String> categories) {
        this.categories = categories;
    }

    public void setVideoSource(String videoSource) {
        this.videoSource = videoSource;
    }

    public void setPublisher_id(String publisher_id) {
        this.publisher_id = publisher_id;
    }

    public void setCountries(List<String> countries) {
        this.countries = countries;
    }

    public long getIsPorn() {
        return isPorn;
    }

    public void setIsPorn(long isPorn) {
        this.isPorn = isPorn;
    }

    public long getIsTerrorism() {
        return isTerrorism;
    }

    public void setIsTerrorism(long isTerrorism) {
        this.isTerrorism = isTerrorism;
    }

    public long getIsPolitical() {
        return isPolitical;
    }

    public void setIsPolitical(long isPolitical) {
        this.isPolitical = isPolitical;
    }

    public float getHumanScore() {
        return humanScore;
    }

    public void setHumanScore(float humanScore) {
        this.humanScore = humanScore;
    }

    public long getFieldExistsFlags() {
        return fieldExistsFlags;
    }

    public void setFieldExistsFlags(long fieldExistsFlags) {
        this.fieldExistsFlags = fieldExistsFlags;
    }

    public List<DocumentTag> getTags() {
        return tags;
    }

    public void setTags(List<DocumentTag> tags) {
        this.tags = tags;
    }

    public ClientVersionInfo getClientVersionInfo() {
        return clientVersionInfo;
    }

    public void setClientVersionInfo(ClientVersionInfo clientVersionInfo) {
        this.clientVersionInfo = clientVersionInfo;
    }

    public String getRecallPoolName() {
        return recallPoolName;
    }

    public void setRecallPoolName(String recallPoolName) {
        this.recallPoolName = recallPoolName;
    }

    public boolean isTopHotHistory() {
        return isTopHotHistory;
    }

    public void setTopHotHistory(boolean topHotHistory) {
        isTopHotHistory = topHotHistory;
    }

    public boolean isBigV() {
        return isBigV;
    }

    public void setBigV(boolean bigV) {
        isBigV = bigV;
    }

    public Long getOnlineTime() {
        return onlineTime;
    }

    public void setOnlineTime(Long onlineTime) {
        this.onlineTime = onlineTime;
    }

    public boolean isReported() {
        return isReported;
    }

    public void setReported(boolean reported) {
        isReported = reported;
    }

    public int getIsAdVideo() {
        return isAdVideo;
    }

    public void setIsAdVideo(int isAdVideo) {
        this.isAdVideo = isAdVideo;
    }

    public void setViewPrivacy(int viewPrivacy) {
        this.viewPrivacy = viewPrivacy;
    }

    public int getViewPrivacy() {
        return viewPrivacy;
    }

    public int getIsRobot() {
        return isRobot;
    }

    public void setIsRobot(int isRobot) {
        this.isRobot = isRobot;
    }


    public float getHeatScore() {
        return heatScore;
    }

    public void setHeatScore(float heatScore) {
        this.heatScore = heatScore;
    }

    public StatisticsDocument getStatisticsDocument() {
        return statisticsDocument;
    }

    public void setStatisticsDocument(StatisticsDocument statisticsDocument) {
        this.statisticsDocument = statisticsDocument;
    }

    public float getAudioDuration() {
        return audioDuration;
    }

    public void setAudioDuration(float audioDuration) {
        this.audioDuration = audioDuration;
    }

    public int getWaterMark() {
        return waterMark;
    }

    public void setWaterMark(int waterMark) {
        this.waterMark = waterMark;
    }

    public int getDuplicated() {
        return duplicated;
    }

    public void setDuplicated(int duplicated) {
        this.duplicated = duplicated;
    }

    public float getNiqeScore() {
        return niqeScore;
    }

    public void setNiqeScore(float niqeScore) {
        this.niqeScore = niqeScore;
    }

    public int getFromTecent() {
        return fromTecent;
    }

    public void setFromTecent(int fromTecent) {
        this.fromTecent = fromTecent;
    }

    public String getDurationLevel() {
        return durationLevel;
    }

    public void setDurationLevel(String durationLevel) {
        this.durationLevel = durationLevel;
    }

    public double getHeatScore2() {
        return heatScore2;
    }

    public void setHeatScore2(double heatScore2) {
        this.heatScore2 = heatScore2;
    }

    public int getAppealStatus() {
        return appealStatus;
    }

    public boolean isDisabled() {
        return isDisabled;
    }

    public void setAppealStatus(int appealStatus) {
        this.appealStatus = appealStatus;
    }

    public void setDisabled(boolean disabled) {
        isDisabled = disabled;
    }

    public boolean isNotOriginal() {
        return isNotOriginal;
    }

    public void setNotOriginal(boolean notOriginal) {
        isNotOriginal = notOriginal;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public int getTagScore() {
        return tagScore;
    }

    public void setTagScore(int tagScore) {
        this.tagScore = tagScore;
    }

    public void setPoolLevel(String poolLevel) {
        this.poolLevel = poolLevel;
    }

    public String getPoolLevel() {
        return this.poolLevel;
    }

    public String getPoolIndex() {
        return poolIndex;
    }

    public void setPoolIndex(String poolIndex) {
        this.poolIndex = poolIndex;
    }

    public int getBadDescription() {
        return badDescription;
    }

    public void setBadDescription(int badDescription) {
        this.badDescription = badDescription;
    }

    public int getPoolPriority() {
        return poolPriority;
    }

    public void setPoolPriority(int poolPriority) {
        this.poolPriority = poolPriority;
    }

    public int getStrategyPoolPriority() {
        return strategyPoolPriority;
    }

    public void setStrategyPoolPriority(int strategyPoolPriority) {
        this.strategyPoolPriority = strategyPoolPriority;
    }

    public double getRealTimeSortedWeighted() {
        return realTimeSortedWeighted;
    }

    public void setRealTimeSortedWeighted(double realTimeSortedWeighted) {
        this.realTimeSortedWeighted = realTimeSortedWeighted;
    }

    public double getHashtagHeat() {
        return hashtagHeat;
    }

    public void setHashtagHeat(double hashtagHeat) {
        this.hashtagHeat = hashtagHeat;
    }

    public List<String> getRetainTags() {
        return retainTags;
    }

    public void setRetainTags(List<String> retainTags) {
        this.retainTags = retainTags;
    }

    public int getOriginalWidth() {
        return originalWidth;
    }

    public void setOriginalWidth(int originalWidth) {
        this.originalWidth = originalWidth;
    }

    public int getOriginalHeight() {
        return originalHeight;
    }

    public void setOriginalHeight(int originalHeight) {
        this.originalHeight = originalHeight;
    }
}
