package mx.j2.recommend.util;

import mx.j2.recommend.thrift.Result;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhongrenli
 * @ Author     ：zhongrenli
 * @ Date       ：Created in 下午2:17 2018/12/5
 * @ Description：维护一个魔法值列表
 */
public abstract class BaseMagicValueEnum {
    public static final long LONG_INITIAL_VALUE = 0L;
    public static final int INT_INITIAL_VALUE = 0;
    public static final float FLOAT_INITIAL_VALUE = 0.0f;
    public static final double DOUBLE_INITIAL_VALUE = 0.0;
    public static final String STRING_INITIAL_VALUE = "";

    /**
     * 公用
     */
    public static final String METADATA_ID = "metadata_id";
    public static final String VIDEO_ID = "video_id";
    public static final String VIEW_COUNT = "view_count";
    public static final String SHARE_COUNT = "share_count";
    public static final String LIKE_COUNT = "like_count";
    public static final String DOWNLOAD_COUNT = "download_count";
    public static final String COMMENT_COUNT = "comment_count";
    public static final String DURATION = "duration";
    public static final String INNERDURATION = "duration";
    public static final String STATUS = "status";
    public static final String DESCRIPTION = "description";
    public static final String TITLE = "title";
    public static final String GENDER = "gender";
    public static final String APP_NAME = "app_name";
    public static final String ITEM_TYPE = "item_type";
    public static final String APP_VERSION = "app_version";
    public static final String LANGUAGE_ID = "language_id";
    public static final String LANGUAGE_ID_LIST = "language_id_list";
    public static final String SCORE = "score";
    public static final String IS_UGC_CONTENT = "is_ugc_content";
    public static final String ORDER = "order";
    public static final String HUMAN_TAGS_STRING = "tag";
    public static final String CATEGORIES = "categories";
    public static final String TAGS = "tags";
    public static final String COUNTRYS_NAME = "countries";
    public static final String HUMAN_SCORE = "human_score";
    public static final String IS_PORN = "is_porn";
    public static final String IS_TERRORISM = "is_terrorism";
    public static final String IS_POLITICAL = "is_politic";
    public static final String ONLINE_TIME = "online_time";
    public static final String CLIENT_VERSION_INFO = "client_version_info";
    public static final String HEAT_SCORE = "heat_score";
    public static final String HEAT_SCORE2 = "heat_score2";
    public static final String IS_REPORTED = "is_reported";
    public static final String HASH_TAG_STRING = "desc_tag";
    public static final String VIEW_PRIVACY = "view_privacy";
    public static final String PERMISSION = "permission";
    public static final String IS_AD_VIDEO = "is_ad_video";
    public static final String IS_ROBOT = "is_robot";
    public static final String NEXT_TOKEN = "next_token";
    public static final String HASHTAG_HEAT = "hashtag_heat";
    public static final String PRIVATE_ACCOUNT = "private_account";
    public static final String ML_TAGS = "ml_tags";
    public static final String ML_TAGS_MANUAL = "ml_tags_manual";
    public static final String POOL_TARGET = "target";
    public static final String STATISTICS = "statistics";
    public static final String PRIMARY_TAGS = "primary_tags";
    public static final String SECONDARY_TAGS = "secondary_tags";
    public static final String NAME = "name";
    public static final String STICKER_NAME = "sticker_name";
    public static final String STICKER_TYPE = "sticker_type";
    public static final String STICKER_GROUP = "sticker_group";
    public static final String UPDATE_TIME = "update_time";
    public static final String CREATE_TIME = "create_time";
    public static final String AUDIO_IDS = "audio_ids";// 音乐卡片（Card）的音乐配置字段
    public static final String TRACK_IDS = "track_ids";// 音乐播放列表（MusicPlaylist）的音乐配置字段
    public static final String PLAYLIST_IDS = "playlist_ids";// 音乐播放列表卡片（Card）的播放列表配置字段

    public static final String QQ_METADATA = "qq_metadata";
    public static final String WATERMARK = "is_delogo";
    public static final String IS_DUPLICATED = "is_duplicated";
    public static final String IS_IPL = "is_ipl";
    public static final String IS_DELETE = "is_delete";
    public static final String HUMAN_REVIEW_STATUS = "human_reviewed_status";
    public static final String NIQE_SCORE = "niqe_score";
    public static final String VIDEO_INFO_RESOLUTION_H264 = "video_info_resolution_h264";
    public static final String META = "meta";
    public static final String EXTRAINFO = "extraInfo";
    public static final String POSTFROM = "postFrom";
    public static final String UNIVERSAL = "universal";

    public static final String FEATURE30D = "feat_stat_30d";
    public static final String FEATURE7D = "feat_stat_7d";
    public static final String FEATURE3D = "feat_stat_3d";
    public static final String FEATURE1D = "feat_stat_1d";
    public static final String FEATURE0D = "feat_stat_0d";

    public static final String FINISH_RETENTION_SUM_10S_30D = "finish_retention_sum_10s_30d";
    public static final String SCORE_30D = "score_30d";
    public static final String APPEAL_STATUS = "appeal_status";
    public static final String IS_DISABLED = "is_disabled";
    public static final String LIKE_INFO = "like_info";

    public static final String BIG_HEAD = "big_head";
    public static final String STICKER_IDS = "sticker_ids";

    public static final String PUBLISHER_PAGE_WHITE = "publisher_page_white";
    public static final String FP_PUBLISHER_TRAFFIC_SUPPORT_LV2 = "fp_publisher_traffic_support_lv2";
    public static final String FP_PUBLISHER_TRAFFIC_SUPPORT_LV3 = "fp_publisher_traffic_support_lv3";
    public static final String FP_PUBLISHER_TRAFFIC_SUPPORT_LV4 = "fp_publisher_traffic_support_lv4";

    /**
     * banner
     */
    public static final String _id = "_id";
    public static final String start_time = "start_time";
    public static final String end_time = "end_time";

    /**
     * 头图信息
     */
    public static final String THUMB_URL = "thumb_s3_path";
    public static final String THUMBNAIL_WIDTH = "width";
    public static final String THUMBNAIL_HEIGHT = "height";

    /**
     * 视频原始尺寸信息
     */
    public static final String ORIGINAL_WIDTH = "original_width";
    public static final String ORIGINAL_HEIGHT = "original_height";
    public static final String WIDTH = "width";
    public static final String HEIGHT = "height";

    /**
     * 头图信息「包含width, height」
     */
    public static final String THUMBNAIL_INFO = "thumbnail_info";

    /**
     * video资源地址
     */
    public static final String SHORT_VIDEO_URL = "target_video_s3_path";
    public static final String TARGET_VIDEO_INFO = "target_video_info";

    /**
     * 池子等级标记
     */
    public static final String HIGH_LEVEL = "high";
    public static final String LOW_LEVEL = "low";
    public static final String STRATEGY_HIGH_LEVEL = "strategy_high";
    public static final String STRATEGY_LOW_LEVEL = "strategy_low";
    /**
     * video资源存在tab页面
     */
    public static final String LIVE_IN_TAB = "live_in_tab";

    /**
     * video audio original
     */
    public static final String IS_ORIGINAL_AUDIO = "is_original_audio";

    /**
     * 贴纸group图标路径
     */
    public static final String ORIGINAL_ICON_URL = "original_icon_url";

    /**
     * 贴纸原始文件路径
     */
    public static final String ORIGINAL_PACKAGE_URL = "original_package_url";

    /**
     * 贴纸缩略图路径
     */
    public static final String STICKER_THUMBNAIL_URL = "sticker_thumbnail_url";

    /**
     * 贴纸图片文件
     */
    public static final String ORIGINAL_STICKER_URL = "original_sticker_url";

    /**
     * 运营标记过资源
     */
    public static final String SPECIAL_SIGN = "special_sign";

    /**
     * 自己下载解码的视频
     */
    public static final String DECRYPT_SIGN = "decrypt_sign";

    /**
     * 自己手动上传的视频
     */
    public static final String UPLOADS_SIGN = "upload_sign";

    /**
     * 下载链接
     */
    public static final String DOWNLOAD_URL = "download_url";

    /**
     * 是否成人内容
     */
    public static final String IS_ADULT = "is_adult";

    /**
     * 爬虫数据 / UGC / 未知来源的标签
     */
    public static final String CRAWLER_TAG = "from_crawler";
    public static final String UGC_TAG = "from_ugc";
    public static final String UNKNOW_TAG = "unknown";

    /**
     * publisher
     */
    public static final String PUBLISHER_ID = "publisher_id";

    /**
     * 敏感词字段
     */
    public static final String BAD_DESCRIPTION = "bad_description";

    /**
     * 过滤属性集合
     */
    public static final String ATTRIB_FILTER = "attrb_filter";// 过滤集合 root key
    public static final String ATTRIB_FILTER_STATES = "states";// 州过滤集合 key

    /**
     * 一些空的数据
     */
    public static final List<Result> EMPTY_RES_LIST = new ArrayList<>();

    /**
     * 用来判断用户是否是新用户的相关参数
     */
    public static final Integer NewUserWatchNumber = 300;
    public static final Integer YoungUserWatchNumber = 5000;
}
