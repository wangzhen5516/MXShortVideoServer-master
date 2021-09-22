package mx.j2.recommend.data_model.document;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.Data;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

import static mx.j2.recommend.util.BaseMagicValueEnum.*;

/**
 * @author liangjie.feng
 * @version 1.0
 * @date 2021/3/29 上午11:47
 * @description
 */
@Data
public class StickerDocument extends BaseDocument {

    Logger logger = LogManager.getLogger(StickerDocument.class);

    /**
     * 贴纸名称
     */
    private String stickerName;

    /**
     * 贴纸状态
     */
    private int status;

    /**
     * 贴纸类型
     * static:静态
     * dynamic:动态
     */
    private String stickerType;

    /**
     * 贴纸所属group
     */
    private String stickerGroup;

    /**
     * 贴纸原始文件路径
     */
    private String originalPackageUrl;

    /**
     * 贴纸缩略图路径
     */
    private String stickerThumbnailUrl;

    /**
     * 国家
     */
    private List<String> countries;

    /**
     * 更新时间
     */
    private long updateTime;

    /**
     * 创建时间
     */
    private long createTime;

    /**
     * 图片文件
     */
    private String originalStickerUrl;

    public StickerDocument loadJson(JSONObject jsonObject, DefineTool.CategoryEnum category, String recallName) {
        loadJsonObj(jsonObject, category, recallName, this);

        if (jsonObject.containsKey(_id)) {
            this.id = jsonObject.getString(_id);
        }

        if (jsonObject.containsKey(STICKER_NAME)) {
            this.stickerName = jsonObject.getString(STICKER_NAME);
        }

        if (jsonObject.containsKey(STATUS)) {
            this.status = jsonObject.getIntValue(STATUS);
        }

        if (jsonObject.containsKey(STICKER_TYPE)) {
            this.stickerType = jsonObject.getString(STICKER_TYPE);
        }

        if (jsonObject.containsKey(STICKER_GROUP)) {
            this.stickerGroup = jsonObject.getString(STICKER_GROUP);
        }

        if (jsonObject.containsKey(ORIGINAL_PACKAGE_URL)) {
            this.originalPackageUrl = jsonObject.getString(ORIGINAL_PACKAGE_URL);
        }

        if (jsonObject.containsKey(STICKER_THUMBNAIL_URL)) {
            this.stickerThumbnailUrl = jsonObject.getString(STICKER_THUMBNAIL_URL);
        }

        if (jsonObject.containsKey(COUNTRYS_NAME) && jsonObject.get(COUNTRYS_NAME) != null) {
            this.countries = JSONArray.parseArray(jsonObject.getJSONArray(COUNTRYS_NAME).toString(), String.class);
        }

        if (jsonObject.containsKey(UPDATE_TIME)) {
            this.updateTime = jsonObject.getLongValue(UPDATE_TIME);
        }

        if (jsonObject.containsKey(CREATE_TIME)) {
            this.createTime = jsonObject.getLongValue(CREATE_TIME);
        }

        if (jsonObject.containsKey(ORIGINAL_STICKER_URL)) {
            this.originalStickerUrl = jsonObject.getString(ORIGINAL_STICKER_URL);
        }

        if (checkQuality()) {
            logger.error("the doc quality is no pass, please check data");
            return null;
        }
        return this;
    }

    private boolean checkQuality() {
        return MXStringUtils.isBlank(this.id);
    }
}
