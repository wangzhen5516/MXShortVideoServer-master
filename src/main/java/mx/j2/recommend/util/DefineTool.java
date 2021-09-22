package mx.j2.recommend.util;

import mx.j2.recommend.conf.Conf;
import mx.j2.recommend.data_model.document.BaseDocument;
import mx.j2.recommend.data_model.document.ShortDocument;
import mx.j2.recommend.util.helper.EnumFindHelper;
import mx.j2.recommend.util.helper.EnumKeyGetter;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.BeanUtils;

import java.util.List;

/**
 * @author zhangxuejian 定义用的工具，定义了Type的类别
 */

public class DefineTool {

    private final static String MX_INDEX = Conf.getMxIndex();
    private final static String CMS_INDEX = Conf.getCmsIndex();
    private final static String UGC_LV1_INDEX = Conf.getUgcLv1Index();
    private final static String MUSIC_INDEX = Conf.getMusicIndex();

    /**
     * 作为 key 的字符串内部单词分割线
     */
    private final static String KEY_INTERNAL_SEPARATOR = "_";

    public static final String MAIN_PACKAGE_PREFIX = "mx.j2.recommend.";

    /**
     * Cassandra 小流量配置 语句
     */
    private static String QUERY_FORMAT_BASE = "SELECT " + Recall.Config.Pools.Table.COLUMN_CONTENT + " FROM %s WHERE "
            + Recall.Config.Pools.Table.COLUMN_STATUS + "='" + Recall.Config.Pools.Status.ONLINE.name + "' and " + Recall.Config.Pools.Table.COLUMN_ENV + "='";
    private static String QUERY_ORDER_CLAUSE = "ORDER BY " + Recall.Config.Pools.Table.COLUMN_TIME + " DESC LIMIT 1 ALLOW FILTERING;";

    /**
     * 将关键词组合成 key
     */
    public static String toKey(String... keywords) {
        StringBuilder key = new StringBuilder();

        for (String keyword : keywords) {
            key.append(keyword);
            key.append(KEY_INTERNAL_SEPARATOR);
        }

        if (key.length() > 0) {
            key.deleteCharAt(key.length() - 1);
        }

        return key.toString();
    }

    public static void deepClone(List<BaseDocument> source, List<BaseDocument> target) {
        source.forEach(doc -> {
            BaseDocument bdoc = new ShortDocument();
            BeanUtils.copyProperties(doc, bdoc);
            bdoc.recallName = doc.recallName;
            target.add(bdoc);
        });
    }

    /**
     * 推荐接口相关信息
     */
    public enum FlowInterface {
        // 1_0版本主接口
        MX_MAIN_VERSION_1_0("mx_main_version_1_0", "feed", "list"),
        MX_VIDEOS_OF_THE_PUBLISHER_VERSION_1_0("mx_videos_of_the_publisher_version_1_0", "other", "list"),
        MX_VIDEOS_OF_THE_PUBLISHER_ME_VERSION_1_0("mx_videos_of_the_publisher_me_version_1_0", "other", "list"),
        MX_FETCH_FOLLOWERS_CONTENT_VERSION_1_0("mx_fetch_followers_content_version_1_0", "feed", "list"),
        MX_VIDEOS_OF_THE_SAME_AUDIO_VERSION_1_0("mx_videos_of_the_same_audio_version_1_0", "other", "list"),

        // 1_0版本内部接口
        MX_HOT_TAB_INTERNAL_VERSION_1_0("mx_hot_tab_internal_version_1_0", "feed", "list"),
        MX_STATUS_TAB_INTERNAL_VERSION_1_0("mx_status_tab_internal_version_1_0", "feed", "list"),
        MX_HOT_TAB_INTERNAL_FOR_OPERATE_VERSION_1_0("mx_hot_tab_internal_for_operate_version_1_0", "feed", "list"),
        MX_STATUS_TAB_INTERNAL_FOR_OPERATE_VERSION_1_0("mx_status_tab_internal_for_operate_version_1_0", "feed", "list"),
        REAL_TIME_ACTION_VERSION_1_0("real_time_action_version_1_0", "feed", "list"),

        // 2_0版本主接口
        MX_MAIN_VERSION_2_0("mx_main_version_2_0", "feed", "list"),

        // 2_0版本内部接口
        MX_HOT_TAB_INTERNAL_VERSION_2_0("mx_hot_tab_internal_version_2_0", "feed", "list"),
        MX_HOT_TAB_INTERNAL_FOR_MAIN_VERSION_2_0("mx_hot_tab_internal_for_main_version_2_0", "feed", "list"),

        //获取publisher的作品数
        MX_VIDEONUM_OF_THE_PUBLISHER_VERSION_1_0("mx_videonum_of_the_publisher_version_1_0", "other", "list"),

        //获取publisher的private作品数
        MX_VIDEONUM_OF_THE_PRIVATE_PUBLISHER_VERSION_1_0("mx_videonum_of_the_private_publisher_version_1_0", "other", "list"),
        //获取publisher的private作品
        MX_VIDEOS_OF_THE_PUBLISHER_PRIVATE_VERSION_1_0("mx_videos_of_the_publisher_private_version_1_0", "other", "list"),

        MX_VIDEOS_OF_THE_TAG_VERSION_1_0("mx_videos_of_the_tag_version_1_0", "other", "list"),
        MX_VIDEOS_OF_THE_EFFECT_VERSION_1_0("mx_videos_of_the_effect_version_1_0", "other", "list"),
        MX_FOLLOWERS_BOOL_REFRESH_VERSION_1_0("mx_followers_bool_refresh_version_1_0", "feed", "list"),
        MX_FOLLOWERS_LIVE_BOOL_REFRESH_VERSION_1_0("mx_followers_live_bool_refresh_version_1_0", "feed", "list"),

        MX_VIDEOS_OF_THE_TAG_SPECIAL_PIN_VERSION_1_0("mx_videos_of_the_tag_special_pin_version_1_0", "other", "list"),

        FETCH_BANNER_VERSION_1_0("fetch_banner_version_1_0", "banner", "list"),
        FETCH_TABS_VERSION_1_0("fetch_tabs_version_1_0", "fetch_tabs", "list"),

        MX_HOT_TAB_INTERNAL_TEST_VERSION_2_0("mx_hot_tab_internal_test_version_2_0", "feed", "list"),

        NEARBY_PEOPLE_VERSION_1_0("nearby_people_version_1_0", "other", "list"),

        FETCH_ACCOUNT_STATUS_1_0("fetch_account_status_1_0", "other", "list"),

        FOLLOW_SUGGESTIONS_VERSION_1_0("follow_suggestions_version_1_0", "feed", "list"),

        SIMILAR_FOLLOWERS_VERSION_1_0("similar_followers_version_1_0", "other", "list"),

        STICKER_GROUP_VERSION_1_0("sticker_group_version_1_0", "other", "list"),
        STICKER_VERSION_1_0("sticker_version_1_0", "other", "list"),
        BADGE_VERSION_1_0("badge_version_1_0", "other", "list"),
        TIME_OF_BADGE_VERSION_1_0("time_of_badge_version_1_0", "other", "list"),

        // 创作者平台资质判断接口
        MX_CREATOR_CHECK_VERSION_1_0("mx_creator_check_version_1_0", "other", "list"),

        /*
         * 音乐库 Discovery Tab 下的接口
         */
        // 拉"音乐卡"内容（内容是若干音乐）
        MX_MUSIC_TRACK_CARD_CONTENT_VERSION_1_0("mx_music_track_card_content_version_1_0", "other", "list"),
        // 拉"播放列表卡"内容（内容是若干播放列表名称）
        MX_MUSIC_PLAYLIST_CARD_CONTENT_VERSION_1_0("mx_music_playlist_card_content_version_1_0", "other", "list"),
        // 拉"播放列表"内容（内容是若干音乐）
        MX_MUSIC_PLAYLIST_CONTENT_VERSION_1_0("mx_music_playlist_content_version_1_0", "other", "list"),
        // 年度上传的第一个视频
        MX_FIRST_VIDEO_OF_YEAR_VERSION_1_0("mx_first_video_of_year_version_1_0", "other", "list"),

        FETCH_FOLLOW_CARD_1_0("fetch_follow_card_1_0", "other", "list"),

        MX_LIVE_STREAM_VERSION_1_0("mx_live_stream_version_1_0", "other", "list"),
        MX_LIVE_CARD_VERSION_1_0("mx_live_card_version_1_0", "other", "list"),
        MX_LIVE_FEED_VERSION_1_0("mx_live_feed_version_1_0", "other", "list"),
        MX_LIVE_FOLLOW_VERSION_1_0("mx_live_follow_version_1_0", "other", "list"),
        MX_FETCH_CMS_PUB_CARD_1_0("fetch_cms_pub_card_1_0", "other", "list"),

        // 内部接口
        INTERNAL_VIDEONUM_OF_THE_PUBLISHER_1_0("internal_videonum_of_the_publisher_1_0", "internal", "list"),
        INTERNAL_GENERAL_FILTER_1_0("internal_general_filter_1_0", "internal", "list"),
        INTERNAL_SORTED_VIDEO_LIST_OF_PUBLISHER_1_0("internal_sorted_video_list_of_publisher_1_0", "internal", "list"),
        INTERNAL_POOL_VIDEO_FILTER_1_0("internal_pool_video_filter_1_0", "internal", "list"),
        INTERNAL_VIDEOS_OF_THE_TAG_VERSION_1_0("internal_videos_of_the_tag_version_1_0", "internal", "list"),
        DEFAULT("", "", "");

        private String name;
        private String stream;
        private String type;

        FlowInterface(String name, String stream, String type) {
            this.name = name;
            this.stream = stream;
            this.type = type;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getStream() {
            return stream;
        }

        public void setStream(String stream) {
            this.stream = stream;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        static final EnumFindHelper<FlowInterface, String> stringHelper = new EnumFindHelper<FlowInterface, String>(
                FlowInterface.class, new StringGetter());

        static class StringGetter implements EnumKeyGetter<FlowInterface, String> {
            @Override
            public String getKey(FlowInterface enumValue) {
                return enumValue.name;
            }
        }

        public static FlowInterface findFlowInterfaceByName(String name, FlowInterface defaultValue) {
            return stringHelper.find(name, defaultValue);
        }
    }

    /**
     * Recommend Flow 配置相关
     */
    public interface RecommendFlow {
        // CA 表
        interface ConfigTable {
            // 配置表
            interface FlowConf {
                String NAME = "flow_conf";

                String COLUMN_INTERFACE = "interface";
                String COLUMN_FLOWS = "flows";
                String COLUMN_TIME = "time";
                String COLUMN_ENV = "env";

                String QUERY_FORMAT_BASE = "SELECT * FROM %s";
                String QUERY_ORDER_CLAUSE = "ORDER BY " + COLUMN_TIME + " DESC LIMIT 1";

                static String queryFormat(String env, String interfaceName) {
                    return QUERY_FORMAT_BASE
                            + " WHERE " + COLUMN_ENV + "='" + env.toLowerCase() + "' AND " + COLUMN_INTERFACE + "='" + interfaceName + "' "
                            + QUERY_ORDER_CLAUSE;
                }
            }

            // 状态表，记录配置有变化的接口
            interface InterfaceStatus {
                String NAME = "interface_status";

                String COLUMN_INTERFACE = "interface";
                String COLUMN_ENV = "env";

                String QUERY_FORMAT_BASE = "SELECT " + COLUMN_INTERFACE + " FROM %s";

                static String queryFormat(String env) {
                    return QUERY_FORMAT_BASE + " WHERE " + COLUMN_ENV + "='" + env.toLowerCase() + "'";
                }
            }
        }
    }

    /**
     * 数据类别信息
     */
    public enum CategoryEnum {
        GIF("gif", MX_INDEX, "gif", "1"),
        SHORT_VIDEO("short", MX_INDEX, "video", "2"),
        PICTURE("picture", MX_INDEX, "pic", "3"),
        TEXT("text", MX_INDEX, "text", "5"),
        BOTTLE("bottle", MX_INDEX, "", "10"),
        BANNER("banner", CMS_INDEX, "Banner", "11"),
        CARD("card", CMS_INDEX, "", ""),
        CARDLISTITEM("CARDLISTITEM", CMS_INDEX, "CardlistItem", ""),
        CARDNEW("cardnew", CMS_INDEX, "Card", ""),
        DENSE_VECTOR_VIDEO("video_vector", "dense_vector_video", "", ""),
        HASHTAG_INFO("hashtag_info", CMS_INDEX, "Hashtag", ""),
        EFFECT_INFO("effect_info", CMS_INDEX, "effect", ""),
        STICKER_GROUP("sticker_group", CMS_INDEX, "StickerGroup", ""),
        STICKER("sticker", CMS_INDEX, "Sticker", ""),
        LIVE_STREAM("live_stream", CMS_INDEX, "Live", ""),
        // 音乐类型
        MUSIC_TRACK("music_track", "", "music_track", ""),
        // 音乐播放列表类型
        MUSIC_PLAYLIST("music_playlist", MUSIC_INDEX, "music_playlist", ""),
        // publisher的勋章数据
        BADGE_MAX_WEEKS("BADGE_MAX_WEEKS", null, null, null),
        BADGE_MAX_DAYS("BADGE_MAX_DAYS", null, null, null),
        BADGE_TOTAL_DAYS("BADGE_TOTAL_DAYS", null, null, null),
        FOLLOW_RED("FOLLOW_RED", "takatak_follow_red", "_doc", null),
        DEFAULT("", MX_INDEX, "", "");

        private String name;
        private String index;
        private String type;
        private String itemType;

        CategoryEnum(String name, String index, String type, String itemType) {
            this.name = name;
            this.index = index;
            this.type = type;
            this.itemType = itemType;
        }

        public String getName() {
            return name;
        }

        public String getIndex() {
            return index;
        }

        public String getType() {
            return type;
        }

        public String getItemType() {
            return itemType;
        }

        public String getIndexAndType() {
            if (MXJudgeUtils.isNotEmpty(type)) {
                return index + "/" + type;
            }
            return index;
        }

        static final EnumFindHelper<CategoryEnum, String> stringHelper = new EnumFindHelper<CategoryEnum, String>(
                CategoryEnum.class, new StringGetter());

        static class StringGetter implements EnumKeyGetter<CategoryEnum, String> {
            @Override
            public String getKey(CategoryEnum enumValue) {
                return enumValue.name;
            }
        }

        public static CategoryEnum findCategoryByName(String name, CategoryEnum defaultValue) {
            return stringHelper.find(name, defaultValue);
        }

        static final EnumFindHelper<CategoryEnum, String> stringItemTypeHelper = new EnumFindHelper<CategoryEnum, String>(
                CategoryEnum.class, new StringItemTypeGetter());

        static class StringItemTypeGetter implements EnumKeyGetter<CategoryEnum, String> {
            @Override
            public String getKey(CategoryEnum enumValue) {
                return enumValue.itemType;
            }
        }

        public static CategoryEnum findCategoryByItemType(String itemType, CategoryEnum defaultValue) {
            return stringItemTypeHelper.find(itemType, defaultValue);
        }

        public static boolean isInCategory(String categoryName) {
            if (MXJudgeUtils.isEmpty(categoryName)) {
                return false;
            }

            if (!CategoryEnum.DEFAULT.equals(findCategoryByName(categoryName, CategoryEnum.DEFAULT))) {
                return true;
            }

            return false;
        }

        public static String getIndexAndType(String categoryName) {
            CategoryEnum category = findCategoryByName(categoryName, CategoryEnum.DEFAULT);

            if (!CategoryEnum.DEFAULT.equals(category)) {
                return category.getIndex() + "/" + category.getType();
            } else {
                return null;
            }
        }

        public static CategoryEnum getCategoryByName(String categoryName) {
            CategoryEnum category = findCategoryByName(categoryName, CategoryEnum.DEFAULT);

            if (!CategoryEnum.DEFAULT.equals(category)) {
                return category;
            } else {
                return null;
            }
        }
    }

    /**
     * 不同数据的不同流
     */
    public enum StreamEnum {
        FEED("Feed"),
        BOTTLE("Bottle"),
        OTHER("other"),
        BANNER("banner"),
        FETCH_TABS("fetch_tabs"),
        INTERNAL("Internal"),
        NULL("Null");

        private String name;

        StreamEnum(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        static final EnumFindHelper<StreamEnum, String> stringHelper = new EnumFindHelper<StreamEnum, String>(
                StreamEnum.class, new StreamEnum.StringGetter());

        static class StringGetter implements EnumKeyGetter<StreamEnum, String> {
            @Override
            public String getKey(StreamEnum enumValue) {
                return enumValue.name;
            }
        }

        public static StreamEnum findStreamByName(String name, StreamEnum defaultValue) {
            return stringHelper.find(name, defaultValue);
        }

        public static StreamEnum getStream(String streamName) {
            return findStreamByName(streamName, StreamEnum.NULL);
        }
    }

    /**
     * 错误信息
     */
    public enum ErrorNoEnum {
        NORMAL_ERROR("1"),
        RETRY_REQUEST("2");

        private String errorNo;

        ErrorNoEnum(String errorNo) {
            this.errorNo = errorNo;
        }

        public String getErrorNo() {
            return this.errorNo;
        }
    }

    /**
     * 是否通过预判断
     */
    public enum PassPreJudgeEnum {
        PASS("pass", true),
        NO_PASS("no_pass", false);

        private String name;

        private boolean flag;

        PassPreJudgeEnum(String name, boolean flag) {
            this.name = name;
            this.flag = flag;
        }

        public String getName() {
            return this.name;
        }

        public boolean getFlag() {
            return flag;
        }
    }

    /**
     * 平台信息
     */
    public enum PlatformsEnum {
        //        ANDROID_OPERATE("android_operate", "0"),
        ANDROID("android", "1"),
        IOS("iOS", "2"),
        WEB("web", "3"),
        ANDROID_BETA("android_beta", "4"),
        DESKTOP_WEB("desktop_web", "5");

        private String name;
        private String index;

        PlatformsEnum(String name, String index) {
            this.name = name;
            this.index = index;
        }

        public String getName() {
            return name;
        }

        public String getIndex() {
            return index;
        }

    }

    /**
     * 资源上下线信息
     */
    public enum OnlineStatusesEnum {
        ONLINE("online", 1),
        OFFLINE("offline", 0);

        private String name;
        private int index;

        OnlineStatusesEnum(String name, int index) {
            this.index = index;
            this.name = name;
        }

        public int getIndex() {
            return index;
        }

        public String getName() {
            return name;
        }
    }

    /**
     * 性别信息
     */
    public enum GenderEnum {
        UNKNOWN("unknown"),// 无性别信息的业务实现
        MALE("male"),
        FEMALE("female");

        private String name;

        GenderEnum(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    /**
     * 召回结果信息来自哪里
     */
    public enum RecallFrom {
        ES("ElasticSearch"),
        REDIS("Redis"),
        LOCAL("LOCAL"),
        CASSANDRA("Cassandra");
        private String name;

        RecallFrom(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

    }

    /**
     * tab与id对应
     */
    public enum TabInfoEnum {
        GIF("1", "gif"),
        HOT("2", "hot"),
        STATUS("3", "status"),
        PIC("4", "pic"),
        //TODO: 临时的adId
        DISCOVERY("5", "bannerAndCard", "5bf68b89b8dc9fce0bfb567750f69100", "5bf68b89b8dc9fce0bfb567750f69100"),
        NEARBY("6", "nearby"),
        MUSIC("7", "music", "5bf68b89b8dc9fce0bfb567750f69123", "5bf68b89b8dc9fce0bfb567750f69123"),
        DEFAULT("", "");

        private String id;
        private String name;
        private String tabIdDev;
        private String tabIdProd;

        TabInfoEnum(String id, String name) {
            this.id = id;
            this.name = name;
        }

        TabInfoEnum(String id, String name, String tabIdDev, String tabIdProd) {
            this.id = id;
            this.name = name;
            this.tabIdDev = tabIdDev;
            this.tabIdProd = tabIdProd;
        }

        public String getId() {
            return this.id;
        }

        public String getName() {
            return this.name;
        }

        public String getTabIdDev() {
            return tabIdDev;
        }

        public String getTabIdProd() {
            return tabIdProd;
        }

        static final EnumFindHelper<TabInfoEnum, String> stringIdHelper = new EnumFindHelper<TabInfoEnum, String>(
                TabInfoEnum.class, new TabInfoEnum.StringIdGetter());

        static class StringIdGetter implements EnumKeyGetter<TabInfoEnum, String> {
            @Override
            public String getKey(TabInfoEnum enumValue) {
                return enumValue.id;
            }
        }

        public static TabInfoEnum findTabInfoEnumById(String id, TabInfoEnum defaultValue) {
            return stringIdHelper.find(id, defaultValue);
        }
    }

    public enum ScheduledPeriodSeconds {
        TenHours(36000),
        OneHour(3600),
        ThirtyMinutes(1800),
        TenMinutes(600),
        FiveMinutes(300),
        OneMinute(60),
        ThirtySeconds(30),
        TenSeconds(10);

        private int seconds;

        ScheduledPeriodSeconds(int seconds) {
            this.seconds = seconds;
        }

        public int getSeconds() {
            return this.seconds;
        }

    }

    public enum MixType {
        /**
         * 混合类型
         */
        MIX("mix"),
        GUARANTEE("guarantee");

        private String type;

        MixType(String type) {
            this.type = type;
        }

        public String getType() {
            return this.type;
        }
    }

    public interface Cache {
        enum CacheStatus {
            NullStatus,
            IgnoreNothing,
            IgnoreAll,
            IgnoreRecall,
            OnlyNeedRuler
        }

        /**
         * 缓存操作
         */
        enum CacheOperationEnum {
            NONE,// 啥也不干
            READ,// 读缓存
            WRITE// 写缓存
        }
    }

    public enum Language {
        Telugu("te", "te", "Telugu"),
        Tamil("ta", "ta", "Tamil"),
        Bhojpuri("bho", "bho", "Bhojpuri"),
        Punjabi("pa", "pa", "Punjabi"),
        Marathi("mr", "mr", "Marathi"),
        Kannada("kn", "kn", "Kannada"),
        Bengali("bn", "bn", "Bengali"),
        Gujarati("gu", "gu", "Gujarati"),
        Malayalam("ml", "ml", "Malayalam"),
        English("en", "en", "English"),
        Hindi("hi", "hi", "Hindi"),
        NoLanguage("no_language", "no_language", "NoLanguage");

        public String abbreviation;
        public String id;
        public String name;

        Language(String abbreviation, String id, String name) {
            this.abbreviation = abbreviation;
            this.id = id;
            this.name = name;
        }

        static final EnumFindHelper<Language, String> stringIdHelper = new EnumFindHelper<Language, String>(
                Language.class, new Language.StringIdGetter());

        static class StringIdGetter implements EnumKeyGetter<Language, String> {
            @Override
            public String getKey(Language enumValue) {
                return enumValue.name;
            }
        }

        public static Language findLanaguage(String name, Language defaultValue) {
            return stringIdHelper.find(name, defaultValue);
        }
    }

    public enum EsType {
        STRATEGY("strategy"),
        VIDEO("video"),
        VIDEO_NEW("videoNew"),
        ES_POOL("pool");

        private String typeName;

        EsType(String typeName) {
            this.typeName = typeName;
        }

        public String getTypeName() {
            return typeName;
        }
    }

    public enum UserHistoryBloomInfoEnum {
        USER_ID("userId", "user_id_history_in_bloom"),
        UUID("uuid", "uuid_history_in_bloom"),
        AD_ID("adId", "ad_id_history_in_bloom");

        private final String name;
        private final String redisBloomKey;

        UserHistoryBloomInfoEnum(String name, String redisBloomKey) {
            this.name = name;
            this.redisBloomKey = redisBloomKey;
        }

        public String getName() {
            return name;
        }

        public String getRedisBloomKey() {
            return redisBloomKey;
        }
    }

    public enum EsPoolLevel {
        HIGH("high"),
        LOW("low");

        private String level;

        EsPoolLevel(String level) {
            this.level = level;
        }

        public String getLevel() {
            return level;
        }
    }

    public enum PoolConfUserLevel {
        NEW("new"),
        OLD("old");

        private String level;

        PoolConfUserLevel(String level) {
            this.level = level;
        }

        public String getLevel() {
            return level;
        }
    }

    public enum TimeZoneEnum {
        INDIA("GMT+5:30"),
        ;

        public final String timeZone;

        TimeZoneEnum(String timeZone) {
            this.timeZone = timeZone;
        }
    }

    /**
     * 配置环境
     */
    public enum Env {
        PROD("prod", "PROD"),
        PRE("pre", "PRE"),
        DEV("dev", "DEV");

        public String name;
        public String confValue;

        Env(String name) {
            this.name = name;
        }

        Env(String name, String confValue) {
            this.name = name;
            this.confValue = confValue;
        }

        public String query() {
            return QUERY_FORMAT_BASE + name + "' " + QUERY_ORDER_CLAUSE;
        }
    }

    /**
     * 召回相关
     */
    public interface Recall {

        /**
         * 配置相关
         */
        class Config {
            /**
             * 数量相关
             */
            public enum SizeEnum {
                /**
                 * 发布者视频召回器
                 */
                VIDEOS_OF_PUBLISHER(40),

                ;

                public int configValue;

                SizeEnum(int configValue) {
                    this.configValue = configValue;
                }
            }

            /**
             * 池子配置相关
             */
            public static abstract class Pools {

                public static String query(String env) {
                    return QUERY_FORMAT_BASE + env.toLowerCase() + "' " + QUERY_ORDER_CLAUSE;
                }

                /**
                 * 配置状态
                 */
                enum Status {
                    ONLINE("online"),// 上线
                    OFFLINE("offline");// 下线

                    public String name;

                    Status(String name) {
                        this.name = name;
                    }
                }


                public interface Table {
                    String NAME = "pool_conf";
                    String COLUMN_CONTENT = "content";
                    String COLUMN_STATUS = "status";
                    String COLUMN_TIME = "time";
                    String COLUMN_ENV = "env";
                }
            }
        }
    }

    /**
     * ES 相关
     */
    public interface ES {
        String SEARCH_FORMAT = "/%s/_search?pretty=false";
        String TAKATAK_SIMPLE_TRIGGER = "takatak_simple_trigger_v%s";
        String IS_UGC_CONTENT = "is_ugc_content";
        String ONLINE_TIME = "online_time";

        static String endpointSearch(CategoryEnum category) {
            return String.format(SEARCH_FORMAT, category.getIndexAndType());
        }
    }

    public interface EsKey {
        String ORDER = "order";
        String ASC = "asc";
        String MISSING = "missing";
    }

    /**
     * 数据库相关
     */
    public interface DB {
        /**
         * 结构化查询语言相关
         */
        interface SQL {
            String QUERY_BY_ID_FORMAT = "select * from %s where id='%s';";
        }
    }

    /**
     * 错误事件发生时的处理
     */
    public enum ErrorEnum {
        /**
         * 一般的错误，必须报告错误
         */
        GENERAL {
            @Override
            void report(Logger logger, Exception e) {
                if (e != null) {
                    if (logger != null) {
                        logger.error("ERROR: " + e.toString());
                    }
                    e.printStackTrace();
                }
            }
        },

        /**
         * 致命错误发生，必须停止服务，及早发现，扼杀在摇篮里
         */
        FATAL {
            @Override
            void report(Logger logger, Exception e) {
                if (e != null) {
                    if (logger != null) {
                        logger.error("FATAL ERROR: " + e.toString());
                    }
                    e.printStackTrace();
                }

                // 结束进程
                System.exit(1);
            }
        };

        abstract void report(Logger logger, Exception e);
    }

    /**
     * 个性化相关（用户画像）
     */
    public interface UserProfile {
        /**
         * 标签相关
         */
        class Tags {
            /**
             * 个性化标签里语言标签的前缀
             */
            private static String ML_TAGS_LANGUAGE_TAG_PREFIX = "language_";

            public static String KEY_SPACE = "takatak";

            /**
             * CA 数据库表相关
             */
            public interface Table {
                String NAME = "realtime_tag_prefer";
                String COLUMN_TAG = "ml_tag";
            }

            /**
             * 如果是语言标签，剥离出语言
             *
             * @return Language
             */
            public static Language unwrapLanguage(String tag) {
                if (tag.contains(ML_TAGS_LANGUAGE_TAG_PREFIX)) {
                    String languageName = tag.replace(ML_TAGS_LANGUAGE_TAG_PREFIX, "");
                    return Language.findLanaguage(languageName, Language.NoLanguage);
                }
                return Language.NoLanguage;
            }
        }
    }

    /**
     * 爬虫账户
     */
    public enum CrawlerAccountEnum {
        CRAWLER_ACCOUNT("13");

        private String prefix;

        CrawlerAccountEnum(String prefix) {
            this.prefix = prefix;
        }

        public String getPrefix() {
            return prefix;
        }
    }

    /**
     * 实时行为接口——行为归档
     */
    public enum RealTimeActionEnum {
        LIKE("like"),
        PLAYED("played"),
        SHARE("share"),
        DOWN("down"),
        COMMENT("comment");

        public final String value;

        RealTimeActionEnum(String value) {
            this.value = value;
        }
    }

    public enum InternalInterface {

        INTERNAL_VIDEONUM_OF_THE_PUBLISHER_1_0("internal_videonum_of_the_publisher", 100, "publisherId", 10),
        INTERNAL_GENERAL_FILTER_1_0("internal_general_filter_1_0", 2000, "videoId", 5),
        INTERNAL_SORTED_VIDEO_LIST_OF_PUBLISHER_1_0("internal_sorted_video_list_of_publisher_1_0", 1, "publisherId", 3),
        INTERNAL_POOL_VIDEO_FILTER_1_0("internal_pool_video_filter_1_0", 1, "videoId", 10),
        INTERNAL_VIDEOS_OF_THE_TAG_VERSION_1_0("internal_videos_of_the_tag_version_1_0", 3, "videoId", 10);
        private String internalInterfaceName;
        private int maxRequestNumber;
        private String resourceIdType;
        private int rateLimiter;

        InternalInterface(String internalInterfaceName, int maxRequestNumber, String resourceIdType, int rateLimiter) {
            this.internalInterfaceName = internalInterfaceName;
            this.maxRequestNumber = maxRequestNumber;
            this.resourceIdType = resourceIdType;
            this.rateLimiter = rateLimiter;
        }

        public String getInternalInterfaceName() {
            return internalInterfaceName;
        }

        public int getRateLimiter() {
            return rateLimiter;
        }

        public void setRateLimiter(int rateLimiter) {
            this.rateLimiter = rateLimiter;
        }

        public void setInternalInterfaceName(String internalInterfaceName) {
            this.internalInterfaceName = internalInterfaceName;
        }

        public int getMaxRequestNumber() {
            return maxRequestNumber;
        }

        public void setMaxRequestNumber(int maxRequestNumber) {
            this.maxRequestNumber = maxRequestNumber;
        }

        public String getResourceIdType() {
            return resourceIdType;
        }

        public void setResourceIdType(String resourceIdType) {
            this.resourceIdType = resourceIdType;
        }
    }
}

