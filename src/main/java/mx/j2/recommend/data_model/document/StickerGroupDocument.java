package mx.j2.recommend.data_model.document;

import com.alibaba.fastjson.JSONObject;
import lombok.Data;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

import static mx.j2.recommend.util.BaseMagicValueEnum.*;

/**
 * @author liangjie.feng
 * @version 1.0
 * @date 2021/3/26 下午5:42
 * @description
 */

@Data
public class StickerGroupDocument extends BaseDocument {

    Logger logger = LogManager.getLogger(StickerGroupDocument.class);

    /**
     * 贴纸分组的名称
     */
    private String name;

    /**
     * 贴纸分组状态
     */
    private int status;

    /**
     * 贴纸分组排序
     */
    private int order;

    /**
     * 贴纸分组下的所有贴纸id
     */
    private List<String> stickerIds = new ArrayList<>();

    /**
     * 贴纸分组更新时间
     */
    private long updateTime;

    /**
     * 贴纸分组创建时间
     */
    private long createTime;

    /**
     * 贴纸group图标路径
     */
    private String originalIconUrl;

    public StickerGroupDocument loadJson(JSONObject jsonObject, DefineTool.CategoryEnum category, String recallName) {
        loadJsonObj(jsonObject, category, recallName, this);

        if (jsonObject.containsKey(_id)) {
            this.id = jsonObject.getString(_id);
        }

        if (jsonObject.containsKey(NAME)) {
            this.name = jsonObject.getString(NAME);
        }

        if (jsonObject.containsKey(STATUS)) {
            this.status = jsonObject.getIntValue(STATUS);
        }

        if (jsonObject.containsKey(ORDER)) {
            this.order = jsonObject.getIntValue(ORDER);
        }

        if (jsonObject.containsKey(STICKER_IDS) && jsonObject.getJSONArray(STICKER_IDS) != null) {
            this.stickerIds = JSONObject.parseArray(jsonObject.getJSONArray(STICKER_IDS).toString(), String.class);
        }

        if (jsonObject.containsKey(ORIGINAL_ICON_URL)) {
            this.originalIconUrl = jsonObject.getString(ORIGINAL_ICON_URL);
        }

        if (jsonObject.containsKey(UPDATE_TIME)) {
            this.updateTime = jsonObject.getLongValue(UPDATE_TIME);
        }

        if (jsonObject.containsKey(CREATE_TIME)) {
            this.createTime = jsonObject.getLongValue(CREATE_TIME);
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
