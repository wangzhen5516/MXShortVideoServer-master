package mx.j2.recommend.data_model.document;

import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import lombok.Setter;
import mx.j2.recommend.util.BaseMagicValueEnum;
import mx.j2.recommend.util.DefineTool;
import mx.j2.recommend.util.MXStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * banner
 *
 * @author xiang.zhou
 */
@NotThreadSafe
@Setter
@Getter
public class BannerDocument extends BaseDocument {
    private static Logger log = LogManager.getLogger(BannerDocument.class);

    private long startTime;

    private long endTime;

    /**
     * 构造函数
     */
    public BannerDocument() {
        init();
    }

    /**
     * 初始化函数
     */
    private void init() {

    }

    /**
     * 解析召回结构为Document形式
     */
    public BannerDocument loadJsonObj(JSONObject source, DefineTool.CategoryEnum category, String recallName) {
        this.loadJsonObj(source, category, recallName, this);

        if (source.containsKey(BaseMagicValueEnum._id)) {
            this.id = source.getString(BaseMagicValueEnum._id);
        }

        if (source.containsKey(BaseMagicValueEnum.start_time)) {
            this.startTime = source.getLongValue(BaseMagicValueEnum.start_time);
        }

        if (source.containsKey(BaseMagicValueEnum.end_time)) {
            this.endTime = source.getLongValue(BaseMagicValueEnum.end_time);
        }

        if (checkQuality()) {
            log.error("the doc quality is no pass, please check data");
            return null;
        }
        return this;
    }

    private boolean checkQuality() {
        return MXStringUtils.isBlank(this.id);
    }

    @Override
    public String toString() {
        return "ShortDocument{" +
                "id='" + id + '\'' +
                ", itemType='" + itemType + '\'' +
                ", contentUrl='" + contentUrl + '\'' +
                ", recallName=" + recallName +
                ", appName='" + appName + '\'' +
                '}';
    }

    public static Logger getLog() {
        return log;
    }

    public static void setLog(Logger log) {
        BannerDocument.log = log;
    }
}
