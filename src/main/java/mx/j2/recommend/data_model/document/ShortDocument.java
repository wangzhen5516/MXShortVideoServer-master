package mx.j2.recommend.data_model.document;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import lombok.Setter;
import mx.j2.recommend.recall.impl.VideosOfThePublisherRecall;
import mx.j2.recommend.util.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.concurrent.NotThreadSafe;

import static mx.j2.recommend.util.BaseMagicValueEnum.*;

/**
 * 文档类，这里可以是新闻、视频、图片等
 *
 * @author zhuowei
 */
@NotThreadSafe
@Getter
@Setter
public class ShortDocument extends BaseDocument {
    private static Logger log = LogManager.getLogger(ShortDocument.class);

    /**
     * 视频资源可以存在的tab页面
     */
    public JSONArray liveInTabArray;

    /**
     * 需要过滤的州集合
     */
    public JSONArray filterStates;

    /**
     * 构造函数
     */
    public ShortDocument() {
        init();
    }

    /**
     * 构造函数
     */
    public ShortDocument(String id) {
        this();
        this.id = id;
        this.category = DefineTool.CategoryEnum.SHORT_VIDEO;
        this.itemType = DefineTool.CategoryEnum.SHORT_VIDEO.getItemType();
    }

    /**
     * 初始化函数
     */
    private void init() {
        category = DefineTool.CategoryEnum.SHORT_VIDEO;
        liveInTabArray = new JSONArray();
        filterStates = new JSONArray();
    }

    /**
     * 解析召回结构为Document形式
     */
    public ShortDocument loadJsonObj(JSONObject source, DefineTool.CategoryEnum category, String recallName) {
        this.loadJsonObj(source, category, recallName, this);

        if (source.containsKey(BaseMagicValueEnum.SHORT_VIDEO_URL)) {
            this.contentUrl = source.getString(BaseMagicValueEnum.SHORT_VIDEO_URL);
        }

        if (source.containsKey(BaseMagicValueEnum.LIVE_IN_TAB)) {
            this.liveInTabArray = source.getJSONArray(BaseMagicValueEnum.LIVE_IN_TAB);
        }

        if (source.containsKey(BaseMagicValueEnum.ATTRIB_FILTER)) {
            JSONObject filterObject = source.getJSONObject(BaseMagicValueEnum.ATTRIB_FILTER);
            if (filterObject.containsKey(BaseMagicValueEnum.ATTRIB_FILTER_STATES)) {
                filterStates = filterObject.getJSONArray(BaseMagicValueEnum.ATTRIB_FILTER_STATES);
            }
        }

        if (source.containsKey(BaseMagicValueEnum.DURATION)) {
            this.duration = source.getLongValue(BaseMagicValueEnum.DURATION);
        }

        if (source.containsKey(BaseMagicValueEnum.META)) {
            try {
                JSONObject meta = source.getJSONObject(BaseMagicValueEnum.META);
                if (meta.containsKey(BaseMagicValueEnum.EXTRAINFO)) {
                    String extraInfo = meta.getString(BaseMagicValueEnum.EXTRAINFO);
                    if (extraInfo != null && !extraInfo.contains(publisher_id)) {
                        this.isNotOriginal = true;
                    }
                }
                if (meta.containsKey(BaseMagicValueEnum.POSTFROM)) {
                    this.postFrom = meta.getString(BaseMagicValueEnum.POSTFROM);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        if (source.containsKey(BaseMagicValueEnum.VIDEO_INFO_RESOLUTION_H264)) {
            JSONArray array = source.getJSONArray(BaseMagicValueEnum.VIDEO_INFO_RESOLUTION_H264);
            if (MXJudgeUtils.isNotEmpty(array)) {
                JSONObject obj = array.getJSONObject(0);
                if (obj.containsKey(BaseMagicValueEnum.INNERDURATION)) {
                    this.innerDuration = obj.getLongValue(BaseMagicValueEnum.INNERDURATION);
                }
            }
        }

        if (source.containsKey(BaseMagicValueEnum.SCORE)) {
            scoreDocument.baseScore = source.getFloatValue(BaseMagicValueEnum.SCORE);
        }

        if (source.containsKey(BaseMagicValueEnum.IS_ORIGINAL_AUDIO)) {
            boolean value = source.getBooleanValue(BaseMagicValueEnum.IS_ORIGINAL_AUDIO);
            if (value) {
                isOriginalAudio = 1;
            }
        }

        if (source.containsKey(BaseMagicValueEnum.IS_UGC_CONTENT)) {
            boolean isUgc = source.getBoolean(BaseMagicValueEnum.IS_UGC_CONTENT);
            if (isUgc) {
                isNoNeedWidthAndHeightFilt = true;
                videoSource = BaseMagicValueEnum.UGC_TAG;
            } else {
                videoSource = BaseMagicValueEnum.CRAWLER_TAG;
            }
        } else {
            videoSource = BaseMagicValueEnum.CRAWLER_TAG;
        }

        order = VideosOfThePublisherRecall.MAX_DEFAULT;
        if (source.containsKey(BaseMagicValueEnum.ORDER)) {
            try {
                String orderString = source.getString(BaseMagicValueEnum.ORDER);
                order = Integer.parseInt(orderString);
            } catch (Exception e) {
                order = VideosOfThePublisherRecall.MAX_DEFAULT;
            }
        }
        if (source.containsKey(BaseMagicValueEnum.HEAT_SCORE)) {
            heatScore = source.getFloatValue(BaseMagicValueEnum.HEAT_SCORE);
        }
        if (source.containsKey(BaseMagicValueEnum.HEAT_SCORE2)) {
            heatScore2 = source.getDoubleValue(BaseMagicValueEnum.HEAT_SCORE2);
        }
        if (source.containsKey(BaseMagicValueEnum.VIEW_PRIVACY)) {
            viewPrivacy = source.getIntValue(BaseMagicValueEnum.VIEW_PRIVACY);
        }
        if (source.containsKey(PERMISSION)) {
            JSONObject object = source.getJSONObject(PERMISSION);
            if (MXCollectionUtils.isNotEmpty(object)) {
                if (object.containsKey("view_privacy")) {
                    viewPrivacy = object.getIntValue("view_privacy");
                }
            }
        }
        if (source.containsKey(BaseMagicValueEnum.HASHTAG_HEAT)) {
            hashtagHeat = source.getDoubleValue(BaseMagicValueEnum.HASHTAG_HEAT);
        }
        if (source.containsKey(BaseMagicValueEnum.UNIVERSAL)) {
            universal = source.getIntValue(BaseMagicValueEnum.UNIVERSAL);
        }
        if (source.containsKey(ORIGINAL_HEIGHT)) {
            this.setOriginalHeight(source.getIntValue(ORIGINAL_HEIGHT));
        } else if (source.containsKey(META)) {
            try {
                JSONObject meta = source.getJSONObject(BaseMagicValueEnum.META);
                if (meta.containsKey(HEIGHT)) {
                    this.setOriginalHeight(meta.getIntValue(HEIGHT));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (source.containsKey(ORIGINAL_WIDTH)) {
            this.setOriginalWidth(source.getIntValue(ORIGINAL_WIDTH));
        } else if (source.containsKey(META)) {
            try {
                JSONObject meta = source.getJSONObject(BaseMagicValueEnum.META);
                if (meta.containsKey(WIDTH)) {
                    this.setOriginalWidth(meta.getIntValue(WIDTH));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        this.sageMakerVideoFeatureDocument.loadVideoFeatureInfo(source, this.innerDuration, this.appName, this.languageId);
        if (checkQuality()) {
            log.error("the doc quality is no pass, please check data");
            return null;
        }
        return this;
    }

    private boolean checkQuality() {
        return MXStringUtils.isBlank(this.id);
    }

    public static Logger getLog() {
        return log;
    }

    public static void setLog(Logger log) {
        ShortDocument.log = log;
    }
}
